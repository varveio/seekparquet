// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"slices"
	"sort"
)

// RowPosition is an immutable boundary between rows of one Dataset.
// It identifies the row following the boundary, or the end of the dataset.
//
// Positions are produced by the reader, may be copied, and remain usable
// until their Dataset closes. They are not portable between Dataset
// handles, even handles opened on the same source. The zero value is unknown.
type RowPosition struct {
	// ds is the Dataset's reader.id, zero for an unknown position.
	ds      uint64
	ordinal int64
}

// Ordinal returns the boundary's zero-based dataset row ordinal.
// known is false for the zero value.
//
// An ordinal is a coordinate in the dataset's declared row order, not
// independent certification of its row counts. Use Plan.Count for counts.
func (p RowPosition) Ordinal() (ordinal int64, known bool) { return p.ordinal, p.ds != 0 }

// RowSpan selects the rows between Start, inclusive, and End, exclusive.
// Both positions must be known and belong to the same Dataset, and End
// must not precede Start. Equal positions select no rows.
//
// A RowSpan returned by List may have an unknown boundary when the
// listing did not establish it without additional reads.
type RowSpan struct {
	Start RowPosition
	End   RowPosition
}

// pos returns the reader's position at a dataset ordinal.
func (r *reader) pos(ordinal int64) RowPosition { return RowPosition{ds: r.id, ordinal: ordinal} }

// rowSpan returns the reader's span of rows [start, end).
func (r *reader) rowSpan(start, end int64) RowSpan { return RowSpan{r.pos(start), r.pos(end)} }

// rowRanges checks spans as RowSpan requires, against the reader, and
// returns their rows.
func (r *reader) rowRanges(spans []RowSpan) ([]rowRange, error) {
	out := make([]rowRange, len(spans))
	for i, sp := range spans {
		for _, p := range [2]RowPosition{sp.Start, sp.End} {
			switch {
			case p.ds == 0:
				return nil, invalidRequest("row span %d has an unknown boundary", i)
			case p.ds != r.id:
				return nil, invalidRequest("row span %d belongs to another Dataset", i)
			case p.ordinal < 0 || p.ordinal > r.rows:
				return nil, invalidRequest("row span %d has a boundary outside the dataset", i)
			}
		}
		if sp.End.ordinal < sp.Start.ordinal {
			return nil, invalidRequest("row span %d ends before it starts", i)
		}
		out[i] = rowRange{sp.Start.ordinal, sp.End.ordinal}
	}
	return out, nil
}

// ordinalSpans returns the spans of rows of ranges, each found by its
// ordinal, possibly after rows of its key.
func (r *reader) ordinalSpans(ranges []rowRange) []span {
	spans := make([]span, 0, len(ranges))
	for _, rr := range ranges {
		spans = append(spans, span{ordinal: true, inside: !r.key.Unique, start: rr.start, end: rr.end})
	}
	return spans
}

// PlanRows plans the union of spans, projecting columns as Query.Columns
// specifies. Overlapping rows occur once, in dataset order.
//
// Every boundary must be known, belong to d, and form a non-reversed
// span. Invalid spans fail with ErrInvalidRequest before I/O.
// An empty spans slice selects no rows.
//
// With no columns, PlanRows performs no storage I/O: it neither opens
// parts nor loads indexes or decodes keys. Count reports whether the
// available row-count evidence is sufficient.
//
// With columns, planning checks selected parts and loads the metadata
// needed to locate projected pages, as Dataset.Plan does. It does not
// resolve the positions again through keys or read projected data pages.
//
// The plan owns its input descriptions. st is reset and filled on every
// exit, including failure.
func (d *Dataset) PlanRows(ctx context.Context, spans []RowSpan, columns []string, st *ReadStats) (*Plan, error) {
	return d.r.planRows(ctx, spans, columns, st)
}

func (r *reader) planRows(ctx context.Context, spans []RowSpan, columns []string, st *ReadStats) (_ *Plan, err error) {
	st, spare := takeStats(st)
	defer dropStats(spare)
	inputs, err := r.rowRanges(spans)
	if err != nil {
		return nil, err
	}
	if err := r.checkProjection(columns); err != nil {
		return nil, err
	}
	rctx, err := r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rctx.done(ctx, &err, st)
	plan := &Plan{r: r, columns: slices.Clone(columns), inputs: inputs}
	plan.union(r.ordinalSpans(inputs))
	if len(columns) > 0 {
		if err := plan.planColumns(rctx, st); err != nil {
			return nil, err
		}
	}
	plan.exact = plan.checked()
	return plan, nil
}

// RowSpans returns one span for each input selection or row span, in
// input order, before union normalization. Empty selections have equal
// boundaries. The returned slice belongs to the caller.
//
// The positions are bound to the plan's Dataset. This method performs
// no I/O and preserves overlapping inputs for independent aggregation.
//
// A shard of Plan.Split has one input per span of the plan's rows in it.
func (plan *Plan) RowSpans() []RowSpan {
	out := make([]RowSpan, len(plan.inputs))
	for i, in := range plan.inputs {
		out[i] = plan.r.rowSpan(in.start, in.end)
	}
	return out
}

// SelectRows returns a plan for the union of spans using p's projection.
// Every span must have known positions from p's Dataset and be wholly
// contained in p's selected rows; otherwise it returns ErrInvalidRequest.
//
// It performs no I/O and reuses p's immutable projection metadata.
// ProjectedBytes counts shared pages and dictionaries once for the
// selected union, retaining inexactness where page extents are unavailable.
//
// The page locations p shares are those the Dataset keeps within
// Limits.OffsetIndexMemory. Other locations are unavailable, making
// ProjectedBytes inexact. SelectRows also works after Dataset.Close;
// scanning the returned plan then fails with ErrClosed.
func (plan *Plan) SelectRows(spans []RowSpan) (*Plan, error) {
	r := plan.r
	inputs, err := r.rowRanges(spans)
	if err != nil {
		return nil, err
	}
	for i, in := range inputs {
		if in.start == in.end {
			continue
		}
		k := sort.Search(len(plan.spans), func(k int) bool { return plan.spans[k].end > in.start })
		if k == len(plan.spans) || plan.spans[k].start > in.start || plan.spans[k].end < in.end {
			return nil, invalidRequest("row span %d is not within the plan's rows", i)
		}
	}
	out := &Plan{r: r, columns: plan.columns, inputs: inputs}
	out.union(r.ordinalSpans(inputs))
	for _, gb := range plan.groups {
		k := sort.Search(len(out.spans), func(k int) bool { return out.spans[k].end > gb.start })
		if k == len(out.spans) || out.spans[k].start >= gb.start+gb.rows {
			continue
		}
		gb.price(out.spans)
		out.groups = append(out.groups, gb)
		out.bytes += gb.bytes
		out.partial = out.partial || gb.partial
	}
	out.exact = out.checked()
	return out, nil
}
