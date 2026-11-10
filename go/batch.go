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
// read-only, and valid only until the ScanBatches or RangeBatches callback
// returns. Do not retain them or use them from a goroutine that outlives the
// callback.
// Row and Cursor make owning copies that may be retained.
type Batch struct {
	keys    pageKeys
	offsets []int64
	columns []string
	values  [][]parquet.Value
}

// Len returns the number of rows in the batch.
func (b *Batch) Len() int { return len(b.offsets) }

// Key returns row i's borrowed key. It panics if i is outside [0, Len()).
func (b *Batch) Key(i int) []byte { return b.keys.key(i) }

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
func (plan *RangePlan) ScanBatches(ctx context.Context, st *ReadStats, fn func(*Batch) error) error {
	return plan.scanBatches(ctx, st, keyReuse{}, fn)
}

// keyReuse configures the batch walker's key pages for tests. The zero
// value is ScanBatches' and RangeBatches': every key page decoded into the
// walker's one key scratch. It costs a check per call and a nil check per
// key page.
type keyReuse struct {
	// off decodes every key page into storage of its own, never reused,
	// as every other read does.
	off bool
	// retire, when set, is called with the scan once it has kept what it
	// needs of a key page about to be overwritten in its scratch.
	retire func(*scan)
}

func (plan *RangePlan) scanBatches(ctx context.Context, st *ReadStats, kr keyReuse, fn func(*Batch) error) (err error) {
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
	w := newBatcher(rctx, plan.columns, st, true)
	s := w.newScan(r, nil, kr)
	for _, sp := range plan.spans {
		if sp.ordinal {
			if err := s.seekOrdinal(sp.start); err != nil {
				return err
			}
			s.groupKey, s.groupStart = s.key, sp.start
		} else if err := s.start(sp.from, sp.exclusive, sp.cursor); err != nil {
			return err
		}
		if err := w.walk(s, sp.end-sp.start, fn); err != nil {
			return err
		}
	}
	return nil
}

// rangeBatches hands fn the rows of rng as batches; see Dataset.RangeBatches.
func (r *reader) rangeBatches(ctx context.Context, rng Range, kr keyReuse, fn func(*Batch) error) (err error) {
	st := resetStats(rng.Stats)
	if fn == nil {
		return invalidRequest("a nil batch callback")
	}
	ctx, done, err := r.request(ctx, rng.Cursor, false, rng.Limit, rng.Columns)
	if err != nil {
		return err
	}
	defer done(&err)
	from, exclusive, end := rng.bounds()
	if r.rows == 0 || end != nil && from != nil && bytes.Compare(from, end) >= 0 {
		return nil
	}
	w := newBatcher(ctx, rng.Columns, st, false)
	s := w.newScan(r, end, kr)
	if err := s.start(from, exclusive, rng.Cursor); err != nil {
		return err
	}
	return w.walk(s, int64(rng.Limit), fn)
}

// batcher cuts a scan's rows into batches, the one batch execution path
// behind ScanBatches and RangeBatches. It opens the projected chunks of each
// row group as the scan reaches it, and loads only the pages holding the
// rows it hands over. planned reads each chunk's dictionary before its first
// page, as a plan's ProjectedBytes counts it; otherwise a dictionary is read
// only for a page that uses it, as Range reads it.
//
// Its scan decodes every key page into one scratch, keys, each page
// overwriting the one before: a batch's keys are borrowed until fn returns,
// and walk hands fn no batch across a key page and moves the scan to the
// next page only after fn returns. Owning copies (Row, Cursor) are made in fn.
type batcher struct {
	ctx     context.Context
	st      *ReadStats
	planned bool
	b       Batch
	scratch []pageScratch
	keys    pageScratch
	pages   []*columnPages
	g       groupRef // the row group pages belong to
}

func newBatcher(ctx context.Context, columns []string, st *ReadStats, planned bool) *batcher {
	return &batcher{
		ctx: ctx, st: st, planned: planned,
		b:       Batch{columns: slices.Clone(columns), values: make([][]parquet.Value, len(columns))},
		scratch: make([]pageScratch, len(columns)),
		pages:   make([]*columnPages, len(columns)),
		g:       groupRef{-1, 0},
	}
}

// newScan opens the batcher's scan of r, decoding key pages into its key
// scratch unless kr.off.
func (w *batcher) newScan(r *reader, end []byte, kr keyReuse) *scan {
	s := r.newScan(w.ctx, end, w.st)
	if !kr.off {
		s.keyScratch, s.retire = &w.keys, kr.retire
	}
	return s
}

// walk hands fn batches of at most n rows from the scan's current row on,
// ending early when the scan is exhausted (at its upper bound or the end of
// the dataset). It leaves the scan on the last row it handed over, unless it
// found the upper bound inside a key page, when the scan is exhausted at the
// first row it excluded. It stops at the first error.
func (w *batcher) walk(s *scan, n int64, fn func(*Batch) error) error {
	b := &w.b
	for s.key != nil {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		if s.g != w.g {
			w.g = s.g
			p := s.r.parts[w.g.part]
			if err := p.checkProjection(b.columns); err != nil {
				return err
			}
			for c, name := range b.columns {
				cp, err := p.openColumn(w.ctx, p.groups[w.g.index].physical, name, w.st, &w.scratch[c])
				if err != nil {
					return err
				}
				if cp != nil {
					cp.planned = w.planned
				}
				w.pages[c] = cp
			}
		}
		// Load only the page containing the first row in each slot, then
		// intersect their ends. None may move again until fn returns.
		k := min(n, int64(s.kp.page.len()-s.kp.next))
		for _, cp := range w.pages {
			if cp != nil {
				if err := cp.load(s.row); err != nil {
					return err
				}
				k = min(k, cp.first+int64(len(cp.values))-s.row)
			}
		}
		first, from := s.row, s.kp.next
		b.offsets = resize(b.offsets, int(k))
		for i := range b.offsets {
			b.offsets[i] = s.offset()
			if i+1 < len(b.offsets) {
				if err := s.next(); err != nil {
					return err
				}
				if s.key == nil { // the next row is at the upper bound
					k = int64(i + 1)
					b.offsets = b.offsets[:k]
					break
				}
			}
		}
		for c, cp := range w.pages {
			b.values[c] = nil
			if cp != nil {
				at := first - cp.first
				b.values[c] = cp.values[at : at+k : at+k]
			}
		}
		b.keys = s.kp.page.slice(from, from+int(k))
		if err := w.ctx.Err(); err != nil {
			return err
		}
		if err := fn(b); err != nil {
			return err
		}
		if n -= k; n == 0 || s.key == nil {
			return nil
		}
		if err := s.next(); err != nil {
			return err
		}
	}
	return nil
}
