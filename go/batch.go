// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"slices"

	"github.com/parquet-go/parquet-go"
)

// Batch is a positive run of consecutive selected rows in dataset order.
// It never crosses a row group, a key page, a page of any projected column,
// or a selected-span gap. Batches may be smaller; maximal size is not promised.
//
// The batch object, keys, column slices and their value bytes are borrowed,
// read-only, and valid only until the ScanBatches callback returns. Do not
// retain them or use them from a goroutine that outlives the callback.
// Row and Cursor make owning copies that may be retained.
type Batch struct {
	keys    []parquet.Value
	offsets []int64
	columns []string
	values  [][]parquet.Value
}

// Len returns the number of rows in the batch.
func (b *Batch) Len() int { return len(b.keys) }

// Key returns row i's borrowed key. It panics if i is outside [0, Len()).
func (b *Batch) Key(i int) []byte {
	k := b.keys[i].ByteArray()
	return k[:len(k):len(k)]
}

// Offset returns row i's zero-based offset in its group of equal keys (PAGE-1).
// It panics if i is outside [0, Len()).
func (b *Batch) Offset(i int) int64 { return b.offsets[i] }

// Column returns the borrowed values of projection slot c, in request order,
// including duplicate projections. A present column has Len() values; nulls
// have IsNull() true. If the part lacks the column (COL-2), it returns nil,
// false. It panics if c is outside the requested projection.
func (b *Batch) Column(c int) (values []parquet.Value, ok bool) {
	v := b.values[c]
	return v, v != nil
}

// Row returns an owning copy of row i, as Scan yields. Its Columns names are
// owned by the scan and shared by its owning rows; do not modify them.
// It panics if i is outside [0, Len()).
func (b *Batch) Row(i int) Row {
	r := Row{Key: bytes.Clone(b.Key(i)), Offset: b.Offset(i), Columns: b.columns, Values: make([]Value, len(b.values))}
	for c, values := range b.values {
		if values == nil {
			r.Values[c].Absent = true
		} else {
			r.Values[c].Value = values[i].Clone()
		}
	}
	return r
}

// Cursor returns an owning cursor after row i. It panics if i is outside
// [0, Len()).
func (b *Batch) Cursor(i int) *Cursor {
	return &Cursor{Key: bytes.Clone(b.Key(i)), Offset: b.Offset(i)}
}

// ScanBatches hands fn borrowed batches of the plan's rows and projected
// values, in dataset order. Projection slots follow PlanRanges' columns.
// It stops at the first error fn returns, returning it. It positions as Range
// does and checks every key page it decodes (CHECK-2). st, when not nil,
// receives read statistics, including on failure, as Range.Stats.
// Cancellation is checked before each callback. Batch documents the boundaries
// and lifetime; use Batch.Row or Batch.Cursor to keep an owning result.
//
// The scan holds the Dataset open while fn runs: Close waits for it to return.
// fn must not call Close, nor wait for a goroutine that does: that would
// deadlock. To stop early, return an error or cancel ctx, then close the Dataset
// after ScanBatches returns. Requests started by fn run normally; once Close
// has begun they fail with ErrClosed. Concurrent or reentrant scans have
// independent borrowed storage.
func (plan *RangePlan) ScanBatches(ctx context.Context, st *ReadStats, fn func(*Batch) error) (err error) {
	st = resetStats(st)
	r := plan.r
	rctx, end, err := r.begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		err = finish(ctx, rctx, err)
		end()
	}()
	b := Batch{columns: slices.Clone(plan.columns), values: make([][]parquet.Value, len(plan.columns))}
	scratch := make([]pageScratch, len(b.columns))
	pages := make([]*columnPages, len(b.columns))
	g := groupRef{-1, 0}
	s := r.newScan(rctx, nil, st)
	for _, sp := range plan.spans {
		if sp.ordinal {
			if err := s.seekOrdinal(sp.start); err != nil {
				return err
			}
			s.groupKey, s.groupStart = s.key, sp.start
		} else if err := s.start(sp.from, sp.exclusive, sp.cursor); err != nil {
			return err
		}
		for o := sp.start; o < sp.end && s.key != nil; {
			if err := rctx.Err(); err != nil {
				return err
			}
			if s.g != g {
				g = s.g
				p := r.parts[g.part]
				if err := p.checkProjection(b.columns); err != nil {
					return err
				}
				for c, name := range b.columns {
					cp, err := p.openColumn(rctx, p.groups[g.index].physical, name, st, &scratch[c])
					if err != nil {
						return err
					}
					if cp != nil {
						cp.planned = true
					}
					pages[c] = cp
				}
			}
			// Load only the page containing the first row in each slot, then
			// intersect their ends. None may move again until fn returns.
			n := min(sp.end-o, int64(len(s.kp.values)-s.kp.next))
			for _, cp := range pages {
				if cp != nil {
					if err := cp.load(s.row); err != nil {
						return err
					}
					n = min(n, cp.first+int64(len(cp.values))-s.row)
				}
			}
			for c, cp := range pages {
				b.values[c] = nil
				if cp != nil {
					from := s.row - cp.first
					b.values[c] = cp.values[from : from+n : from+n]
				}
			}
			from := s.kp.next
			b.keys = s.kp.values[from : from+int(n) : from+int(n)]
			b.offsets = resize(b.offsets, int(n))
			for i := range b.offsets {
				b.offsets[i] = s.offset()
				if i+1 < len(b.offsets) {
					if err := s.next(); err != nil {
						return err
					}
				}
			}
			if err := rctx.Err(); err != nil {
				return err
			}
			if err := fn(&b); err != nil {
				return err
			}
			o += n
			if o < sp.end {
				if err := s.next(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
