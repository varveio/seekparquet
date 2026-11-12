// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"slices"
	"sort"

	"github.com/parquet-go/parquet-go"
)

// ValueBatch is a positive run of consecutive selected rows in dataset
// order. It never crosses a row group, a projected column's page boundary,
// or a gap in the selection. Its maximum size is bounded by the reader.
//
// The batch, column views and value bytes are borrowed and read-only,
// valid only until the ScanValues callback returns. Copy retained values
// with ColumnView.Value(i).Clone().
type ValueBatch struct {
	ds     uint64
	start  int64
	n      int
	values [][]parquet.Value
	// kinds are the columns' physical types, KindNone for a column the
	// part lacks, as Batch's.
	kinds []Kind
}

// Len returns the number of rows in the batch.
func (b *ValueBatch) Len() int { return b.n }

// Start returns an owned position at the batch's first row.
// Row i has dataset ordinal Start's ordinal plus i.
func (b *ValueBatch) Start() RowPosition { return RowPosition{ds: b.ds, ordinal: b.start} }

// Column returns the borrowed values of projection slot c, following
// Plan.Columns, with the same null and absent semantics as Batch.Column.
// It panics when c is outside the projection.
func (b *ValueBatch) Column(c int) ColumnView {
	if k := b.kinds[c]; k != KindNone {
		return presentView(b.values[c], k)
	}
	return absentView(b.n)
}

// maxValueBatch bounds a batch of no present column: the rows of an
// absent column's view (absentView).
const maxValueBatch = len(absentValues)

// ScanValues hands fn borrowed batches of the plan's projected values,
// in dataset order, with overlapping selected rows yielded once.
//
// It executes the plan's row positions without seeking through keys,
// decoding interior key pages, or computing equal-key offsets.
// A key column explicitly included in the projection is read as requested.
//
// Selected parts and projected pages receive their normal structural
// and decoding checks. Success does not certify unread key pages.
//
// st is reset and filled on every exit. A nil fn is ErrInvalidRequest.
// Cancellation, callback errors, Stop and Dataset lifetime follow Plan.Scan.
// The caller's cache mode is honored; CacheDefault permits source-cache
// lookup and admission. Each concurrent scan has independent borrowed storage.
//
// Value pages are located by their OffsetIndex row positions; a chunk
// without one is walked by its page headers from its start. A plan with
// no columns yields batches of rows alone, at most 4,096 each and within
// a row group, checking the parts holding them and reading nothing else.
func (plan *Plan) ScanValues(ctx context.Context, st *ReadStats, fn func(*ValueBatch) error) (err error) {
	st, spare := takeStats(st)
	defer dropStats(spare)
	if fn == nil {
		return invalidRequest("a nil batch callback")
	}
	r := plan.r
	rctx, err := r.begin(ctx)
	if err != nil {
		return err
	}
	defer rctx.done(ctx, &err, st)
	v := &valueScan{
		ctx: rctx, st: st, r: r, columns: plan.columns,
		b:       ValueBatch{ds: r.id, values: make([][]parquet.Value, len(plan.columns)), kinds: make([]Kind, len(plan.columns))},
		scratch: make([]pageScratch, len(plan.columns)),
		pages:   make([]*columnPages, len(plan.columns)),
		g:       groupRef{-1, 0},
	}
	for _, sp := range plan.spans {
		if err := v.span(sp.start, sp.end, fn); err != nil {
			if err == Stop {
				return nil
			}
			return err
		}
	}
	return nil
}

// valueScan is one ScanValues call: the projected chunks of the row group
// it is in, and the batch it hands over.
type valueScan struct {
	ctx     context.Context
	st      *ReadStats
	r       *reader
	columns []string
	b       ValueBatch
	scratch []pageScratch
	pages   []*columnPages
	g       groupRef // the row group pages belong to
}

// span hands fn the rows [start, end) in batches.
func (v *valueScan) span(start, end int64, fn func(*ValueBatch) error) error {
	r := v.r
	for i := r.partAt(start); i < len(r.parts) && r.parts[i].start < end; i++ {
		if err := v.ctx.Err(); err != nil {
			return err
		}
		p, err := r.part(v.ctx, i, v.st)
		if err != nil {
			return err
		}
		j := sort.Search(len(p.groups), func(j int) bool { return p.groups[j].start+p.groups[j].rows > start })
		for ; j < len(p.groups) && p.groups[j].start < end; j++ {
			grp := p.groups[j]
			if err := v.open(p, groupRef{i, j}); err != nil {
				return err
			}
			from, to := max(start, grp.start)-grp.start, min(end, grp.start+grp.rows)-grp.start
			for row := from; row < to; {
				k, err := v.load(row, to)
				if err != nil {
					return err
				}
				b := &v.b
				b.start, b.n = grp.start+row, int(k)
				for c, cp := range v.pages {
					if cp != nil {
						at := row - cp.first
						b.values[c] = cp.values[at : at+k : at+k]
					} else {
						b.values[c] = nil
					}
				}
				if err := v.ctx.Err(); err != nil {
					return err
				}
				if err := fn(b); err != nil {
					return err
				}
				row += k
			}
		}
	}
	return nil
}

// open opens the projected chunks of row group g of part p, unless they
// are open.
func (v *valueScan) open(p *part, g groupRef) error {
	if g == v.g {
		return nil
	}
	v.g = g
	if err := p.checkProjection(v.columns); err != nil {
		return err
	}
	for c, name := range v.columns {
		cp, err := p.openColumn(v.ctx, p.groups[g.index].physical, name, v.st, &v.scratch[c])
		if err != nil {
			v.g = groupRef{-1, 0}
			return err
		}
		v.b.kinds[c] = KindNone
		if cp != nil {
			cp.planned = true
			v.b.kinds[c] = kindOf(cp.c.col.typ.Kind())
		}
		v.pages[c] = cp
	}
	return nil
}

// load loads, in every projected chunk, the page holding row of the
// current row group, and returns how many rows from row on, before to,
// they all hold.
func (v *valueScan) load(row, to int64) (int64, error) {
	k := to - row
	if len(v.pages) == 0 || slices.Contains(v.pages, nil) {
		k = min(k, int64(maxValueBatch))
	}
	for _, cp := range v.pages {
		if cp == nil {
			continue
		}
		if err := cp.load(row); err != nil {
			return 0, err
		}
		k = min(k, cp.first+int64(len(cp.values))-row)
	}
	return k, nil
}
