// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"slices"
	"sync"

	"github.com/parquet-go/parquet-go"
)

// Fold is a caller's reduction of rows into a T.
//
// The reduction must be associative and commutative: folding any division
// of a set of rows into several Ts and merging them, in any order, must
// equal folding the set into one.
//
// New and Add run on up to FoldOptions.Workers goroutines at once, never
// two on the same T. Merge runs only after every Add has returned, on
// the goroutine that called FoldRows. No function may retain a T's
// address or anything borrowed from rows.
type Fold[T any] struct {
	// Columns is the projection, in the order Add reads it, duplicates
	// included.
	Columns []string
	// New returns an empty T, owned by its caller: the identity of Merge
	// and the result of a span with no rows. Nil means the zero T.
	New func() T
	// Add folds rows into acc. rows is a run of consecutive rows that lie
	// in the same spans among those whose value work was admitted; it can
	// cross a boundary of a span that is refused or unknown. It is
	// borrowed until Add returns. An error, Stop included, fails the fold
	// and is returned with no results.
	Add func(acc *T, rows *ValueBatch) error
	// Merge folds from into into. It must not modify from, and must copy
	// any mutable state it takes from it: from may be merged into several
	// results.
	Merge func(into, from *T)
}

// empty returns a new empty T.
func (f *Fold[T]) empty() T {
	if f.New == nil {
		var zero T
		return zero
	}
	return f.New()
}

// FoldStatus describes whether a span's fold is complete.
type FoldStatus uint8

const (
	// FoldUnknown means the span's rows are not established: it covers a
	// part no read has checked, and keys are not checked.
	FoldUnknown FoldStatus = iota
	// FoldExact means Value is the reduction of every row of the span.
	FoldExact
	// FoldBudgetExceeded means the span's value work did not fit the value
	// budget.
	FoldBudgetExceeded
	// FoldUnpriced means the compressed extents of the span's value work
	// are not known, so the value budget could not admit it.
	FoldUnpriced
)

// FoldOptions controls a fold.
type FoldOptions struct {
	// ValueBudgetBytes bounds the compressed page and dictionary extents
	// of the projected columns that the fold reads, each counted once
	// however many spans share it. Spans are considered in ascending order
	// of their starts, and each is admitted whole when its value work fits
	// with that of the spans admitted before it; a span that does not fit
	// is FoldBudgetExceeded, or FoldUnpriced when its extents are not
	// known. Planning metadata and the key pages read to check keys are
	// not charged, and a fold with no columns is not budgeted. Zero is
	// unlimited; a negative budget is ErrInvalidRequest.
	ValueBudgetBytes int64
	// Workers bounds the concurrent scans and Add calls; zero or less
	// means one worker.
	Workers int
	// CheckKeys requests that every selected key is checked, including
	// the order across shards, also in spans the budget refused. Without
	// it, a span over a part no read has checked is FoldUnknown, and an
	// exact result does not certify the keys of its rows.
	CheckKeys bool
}

// FoldResult is one span's reduction. Value is the complete reduction of
// the span's rows when Status is FoldExact, and the zero T otherwise.
type FoldResult[T any] struct {
	// Value is the span's reduction when Status is FoldExact.
	Value T
	// Status describes completeness; an inexact result's Value is the
	// zero T.
	Status FoldStatus
}

// FoldRows reduces the rows of each established span with f, preserving
// input order, overlaps and empties: out has one result per span. On
// success each row of the spans whose value work was admitted is handed
// to Add exactly once, and reaches every admitted span that holds it
// through Merge. A fold with no columns still hands Add its rows. A span
// with no rows is an exact empty T, with no call to Add.
//
// Every span must have known boundaries from d and must not end before it
// starts, as PlanRows requires. An invalid span, a nil Add or Merge, or a
// negative ValueBudgetBytes is ErrInvalidRequest, before any I/O. Each
// span's count is planned without I/O; the spans whose count is exact, or
// all of them with CheckKeys, then have their value work admitted under
// the budget (FoldOptions), and the admitted spans' rows are scanned
// together, each row once. A span refused by the budget is not exact even
// where it overlaps an admitted one.
//
// st is reset and filled on every exit, including failure, with the work
// of planning, scanning and checking keys. Any error returns no results.
// The first failing Add, or a failing read, cancels the other workers, so
// an Add that would fail later may never run. FoldRows then returns, in
// this order: the first error other than Stop that was recorded, from an
// Add or from a read; the error of ctx, if ctx ended; Stop. A read's
// error that matches context.Canceled once the workers are cancelled is
// not recorded.
func FoldRows[T any](ctx context.Context, d *Dataset, spans []RowSpan, f Fold[T], opt FoldOptions, st *ReadStats) (out []FoldResult[T], err error) {
	st, spare := takeStats(st)
	defer dropStats(spare)
	if f.Add == nil || f.Merge == nil {
		return nil, invalidRequest("a nil fold callback")
	}
	if opt.ValueBudgetBytes < 0 {
		return nil, invalidRequest("a value budget must not be negative")
	}
	op, err := d.r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer op.done(ctx, &err, st)
	if err := op.Err(); err != nil {
		return nil, err
	}
	if err := d.r.checkProjection(f.Columns); err != nil {
		return nil, err
	}

	w := rowWork{
		columns:   f.Columns,
		budget:    opt.ValueBudgetBytes,
		workers:   opt.Workers,
		checkKeys: opt.CheckKeys,
	}
	a, err := d.admitRows(op, spans, w, st)
	if err != nil {
		return nil, err
	}

	out = make([]FoldResult[T], len(spans))
	for i, status := range a.status {
		out[i].Status = status
		if status == FoldExact && a.counts[i] == 0 {
			out[i].Value = f.empty()
		}
	}
	if len(a.selected) > 0 {
		values, err := foldPlan(op, a.plan, &f, opt, st)
		if err != nil {
			return nil, err
		}
		for k, i := range a.selected {
			out[i].Value = values[k]
		}
	}

	if err := d.checkRefusedKeys(op, spans, a, w, st); err != nil {
		return nil, err
	}
	if err := op.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// foldPlan folds the rows of p into one T per input of p, in input order.
// p's inputs are the admitted spans alone, so its cuts bound segments of
// rows that lie in the same admitted spans. Each worker folds the rows it
// scans into its own accumulator of each segment. Once every worker has
// returned, the workers' accumulators of a segment are merged into its
// first, and each input merges the segments it holds.
func foldPlan[T any](ctx context.Context, p *Plan, f *Fold[T], opt FoldOptions, st *ReadStats) ([]T, error) {
	cuts := p.inputCuts()
	var mu sync.Mutex
	var workers []*foldWorker[T]
	err := p.scanParallel(ctx, opt.Workers, st, opt.CheckKeys, func() shardScan {
		w := newFoldWorker(p, f, cuts, opt.CheckKeys)
		mu.Lock()
		workers = append(workers, w)
		mu.Unlock()
		return w.scan
	})
	if err != nil {
		return nil, err
	}

	segments := make([]*T, len(cuts))
	for _, w := range workers {
		for k, acc := range w.accs {
			switch {
			case acc == nil:
			case segments[k] == nil:
				segments[k] = acc
			default:
				f.Merge(segments[k], acc)
			}
		}
	}

	out := make([]T, len(p.inputs))
	for i, in := range p.inputs {
		out[i] = f.empty()
		k, _ := slices.BinarySearch(cuts, in.start)
		for ; cuts[k] < in.end; k++ {
			if segments[k] != nil {
				f.Merge(&out[i], segments[k])
			}
		}
	}
	return out, nil
}

// foldWorker is one scan worker of a fold: the accumulator of each
// segment of admitted spans it has met, and the run of a batch it hands
// Add.
type foldWorker[T any] struct {
	f         *Fold[T]
	checkKeys bool
	// cuts bound the segments; accs[k] accumulates the rows of
	// [cuts[k], cuts[k+1]) this worker has folded, nil before the first.
	cuts []int64
	accs []*T
	// keyed is a Batch of a scan that checks keys, as a ValueBatch.
	keyed ValueBatch
	// run is a batch's rows inside one segment, re-sliced from the batch.
	run ValueBatch
}

// newFoldWorker returns a worker of a fold of p, with no accumulator.
func newFoldWorker[T any](p *Plan, f *Fold[T], cuts []int64, checkKeys bool) *foldWorker[T] {
	return &foldWorker[T]{
		f:         f,
		checkKeys: checkKeys,
		cuts:      cuts,
		accs:      make([]*T, len(cuts)),
		keyed:     ValueBatch{ds: p.r.id},
		run:       ValueBatch{values: make([][]parquet.Value, len(p.columns)), kinds: make([]Kind, len(p.columns))},
	}
}

// scan folds the rows of one shard. With checkKeys it scans keys and
// values together, noting the shard's keys; otherwise it scans values
// alone.
func (w *foldWorker[T]) scan(ctx context.Context, shard *Plan, st *ReadStats, keys *shardKeys) error {
	var failed, err error
	if w.checkKeys {
		err = shard.Scan(ctx, st, func(b *Batch) error {
			keys.noteBatch(b)
			v := &w.keyed
			v.start, v.n, v.values, v.kinds = b.start, b.Len(), b.values, b.kinds
			failed = w.fold(v)
			return failed
		})
	} else {
		err = shard.scanValues(ctx, st, func(b *ValueBatch) error {
			failed = w.fold(b)
			return failed
		})
	}
	w.keyed.values, w.keyed.kinds = nil, nil
	clear(w.run.values)

	if failed != nil {
		return &shardCallbackError{failed}
	}
	return err
}

// fold hands Add the rows of b, one run per segment of admitted spans
// they lie in. A batch inside one segment is handed over whole.
func (w *foldWorker[T]) fold(b *ValueBatch) error {
	k, found := slices.BinarySearch(w.cuts, b.start)
	if !found {
		k--
	}
	end := b.start + int64(b.n)
	if w.cuts[k+1] >= end {
		return w.f.Add(w.acc(k), b)
	}

	for lo := 0; lo < b.n; k++ {
		hi := int(min(w.cuts[k+1], end) - b.start)
		w.clip(b, lo, hi)
		if err := w.f.Add(w.acc(k), &w.run); err != nil {
			return err
		}
		lo = hi
	}
	return nil
}

// acc returns the worker's accumulator of segment k, made on first use.
func (w *foldWorker[T]) acc(k int) *T {
	if w.accs[k] == nil {
		acc := w.f.empty()
		w.accs[k] = &acc
	}
	return w.accs[k]
}

// clip sets the run to rows [lo, hi) of b. It re-slices b's present
// columns and copies no value.
func (w *foldWorker[T]) clip(b *ValueBatch, lo, hi int) {
	r := &w.run
	r.ds, r.start, r.n = b.ds, b.start+int64(lo), hi-lo
	copy(r.kinds, b.kinds)
	for c, values := range b.values {
		r.values[c] = nil
		if b.kinds[c] != KindNone {
			r.values[c] = values[lo:hi:hi]
		}
	}
}

// inputCuts returns the distinct starts and ends of the plan's inputs, in
// ascending order. Consecutive cuts bound a segment: rows that lie in the
// same inputs.
func (plan *Plan) inputCuts() []int64 {
	cuts := make([]int64, 0, 2*len(plan.inputs))
	for _, in := range plan.inputs {
		cuts = append(cuts, in.start, in.end)
	}
	slices.Sort(cuts)
	return slices.Compact(cuts)
}
