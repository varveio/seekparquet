// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"math"
)

// PageQuery requests a listing page and reductions over its complete common prefixes.
type PageQuery struct {
	// ListQuery supplies the listing bounds, projection, and result limit.
	ListQuery
	// Reductions describes the common-prefix reductions, in result order.
	Reductions []Reduction
	// Aggregates controls prefix execution, validation, and value budgeting.
	Aggregates AggregateOptions
}

// PageEntry is a listing entry with reductions for a common prefix.
type PageEntry struct {
	// ListEntry retains the listing row, prefix, Exact, and candidate Rows.
	ListEntry
	// Aggregates follows PageQuery.Reductions; nil for ordinary row entries.
	// Incomplete or unknown prefix boundaries leave every result unknown.
	Aggregates []AggregateResult
}

// Page holds listing entries and the cursor of the following page, nil at the end.
type Page struct {
	// Entries holds at most PageQuery.Limit entries in listing order.
	Entries []PageEntry
	// Next resumes after the last entry; nil when no entries follow.
	Next *Cursor
}

// Page composes List and AggregateRows. It lists Limit+1 entries to determine
// whether a following page exists, and aggregates only returned common prefixes.
// Their totals describe complete prefixes, including Exact. Unknown boundaries,
// cursors inside a prefix, or End clipping a prefix produce AggregateUnknown;
// Page never completes them with fallback seeks. Use AggregateRows explicitly
// for totals over clipped candidate rows. Limit must be positive and below
// MaxInt. Options and reductions are checked before listing. st is reset and
// includes listing and aggregation on success or failure. Errors return no page.
func (d *Dataset) Page(ctx context.Context, q PageQuery, st *ReadStats) (page Page, err error) {
	st, spare := takeStats(st)
	defer dropStats(spare)
	if q.Limit <= 0 || q.Limit == math.MaxInt {
		return Page{}, invalidRequest("a page's limit must be positive and below %d", math.MaxInt)
	}
	columns, _, err := aggregateColumns(q.Reductions, q.Aggregates)
	if err != nil {
		return Page{}, err
	}
	if err := d.r.checkProjection(columns); err != nil {
		return Page{}, err
	}
	op, err := d.r.begin(ctx)
	if err != nil {
		return Page{}, err
	}
	defer op.done(ctx, &err, st)
	list := q.ListQuery
	list.Limit++
	var listed ReadStats
	entries, err := d.List(op, list, &listed)
	st.Add(listed)
	if err != nil {
		return Page{}, err
	}
	if len(entries) > q.Limit {
		entries = entries[:q.Limit]
		page.Next = entries[len(entries)-1].Cursor()
	}
	var spans []RowSpan
	var targets []int
	page.Entries = make([]PageEntry, len(entries))
	for i, e := range entries {
		page.Entries[i].ListEntry = e
		if e.CommonPrefix == nil {
			continue
		}
		page.Entries[i].Aggregates = make([]AggregateResult, len(q.Reductions))
		_, start := e.Rows.Start.Ordinal()
		_, end := e.Rows.End.Ordinal()
		known := start && end
		if c := q.Cursor; c != nil {
			known = known && !bytes.HasPrefix(c.Key, e.CommonPrefix) && !(len(c.Prefix) > len(e.CommonPrefix) && bytes.HasPrefix(c.Prefix, e.CommonPrefix))
		}
		// Even a known ordinal at End is only the clipped candidate boundary.
		if q.End != nil {
			upper, ok := successor(e.CommonPrefix)
			known = known && ok && bytes.Compare(q.End, upper) >= 0
		}
		if known {
			spans = append(spans, e.Rows)
			targets = append(targets, i)
		}
	}
	var aggregated ReadStats
	aggs, err := d.AggregateRows(op, spans, q.Reductions, q.Aggregates, &aggregated)
	st.Add(aggregated)
	if err != nil {
		return Page{}, err
	}
	for k, i := range targets {
		page.Entries[i].Aggregates = aggs[k]
	}
	if err := op.Err(); err != nil {
		return Page{}, err
	}
	return page, nil
}
