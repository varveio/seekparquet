// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"cmp"
	"context"
	"slices"
	"sort"
)

// Plan is a set of selections planned before they are read: every row of
// the selections, its row count, and the compressed bytes that reading the
// projected columns of those rows will request. A plan is bound to the
// Dataset that made it, and its scans fail with ErrClosed once the Dataset
// is closed. It is immutable and safe for concurrent use.
//
// A plan certifies what it decoded: the key pages at the two ends of each
// selection, checked as Dataset.Rows checks them (CHECK-2). Its count is derived
// from structure, not from decoding the rows between the ends (CHECK-4):
// the positions the ends decode, the row counts of the row groups, from
// footers a part check verified (CHECK-1), and the OffsetIndex row
// positions. Scan decodes every row and checks every key page it reads.
type Plan struct {
	r       *reader
	columns []string
	// spans are the selected rows: disjoint, ascending and not adjacent.
	spans []span
	rows  int64
	exact bool
	// bytes are the projected bytes Scan requests; partial is set when a
	// projected column chunk has no OffsetIndex, whose pages bytes omits.
	bytes   int64
	partial bool
	// groups are the projected bytes counted in each row group holding a
	// row of the plan, in dataset order.
	groups []groupBytes
}

// groupBytes is the projected bytes a plan counts in one row group: its
// pages holding the plan's rows and its dictionaries. partial is set when
// a projected chunk of the group has no OffsetIndex.
type groupBytes struct {
	start   int64 // the dataset ordinal of the group's first row
	bytes   int64
	partial bool
}

// span is the dataset rows [start, end), and where a scan finds start: the
// first row at or after from (after it when exclusive), or after cursor;
// or, when ordinal is set, start itself, the first row of its key group (a
// shard boundary).
type span struct {
	from       []byte
	exclusive  bool
	cursor     *Cursor
	ordinal    bool
	start, end int64
}

// Plan plans reading the rows of selections, with the values of columns
// (as Query.Columns). Each selection's Cursor, a row cursor, starts it
// after that row, inside a group of equal keys too. Rows that several
// selections select are one row of the plan.
//
// Planning decodes what Rows starting at each end would: the key pages
// that locate the first row of the selection and the first row after it,
// through the routing and the key page index, and no part known from the
// manifest bounds to begin after the selection. With columns, it also
// checks every part holding a selected row and reads the OffsetIndex of
// each projected column chunk the rows span, unless an earlier request
// kept its page locations (Limits.OffsetIndexMemory), but no data page.
// Errors are those of Rows. st, when not nil, is reset and receives the
// reads of planning, whether it succeeds or fails.
func (d *Dataset) Plan(ctx context.Context, selections []Selection, columns []string, st *ReadStats) (*Plan, error) {
	return d.r.plan(ctx, selections, columns, st)
}

// Count returns the number of rows Scan yields. It is exact when every
// part holding one of them is checked (CHECK-1), so that the row counts it
// adds up are the parts' footers', verified against the manifest: always
// for a plan with columns, which checks those parts. Otherwise n relies on
// the manifest's row count of a part no read has touched yet, and exact is
// false.
func (plan *Plan) Count() (n int64, exact bool) { return plan.rows, plan.exact }

// ProjectedBytes returns the compressed bytes Scan requests from the
// Source for the projected columns: for each column chunk the rows span,
// the extents its OffsetIndex gives the pages holding them and, when the
// chunk has a dictionary page, the dictionary's, each counted once however
// many ranges share it. Footers, page indexes and key pages are not
// counted. Scan reads each counted extent in one request and no other
// byte of those chunks. exact is false when a projected chunk has no
// OffsetIndex (COL-4): Scan walks its page headers, and n omits it.
func (plan *Plan) ProjectedBytes() (n int64, exact bool) { return plan.bytes, !plan.partial }

// Columns returns the plan's projected column names, in order, as a copy
// the caller owns.
func (plan *Plan) Columns() []string { return slices.Clone(plan.columns) }

// plan plans selections; see Dataset.Plan. One scan positions at every
// end, so that the key pages planning decodes are checked against each
// other (CHECK-2).
func (r *reader) plan(ctx context.Context, selections []Selection, columns []string, st *ReadStats) (_ *Plan, err error) {
	st = resetStats(st)
	for i := range selections {
		if err := selections[i].Cursor.check(false); err != nil {
			return nil, err
		}
	}
	if err := r.checkProjection(columns); err != nil {
		return nil, err
	}
	rctx, end, err := r.begin(ctx, st)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = finish(ctx, rctx, err)
		end()
	}()
	plan := &Plan{r: r, columns: slices.Clone(columns)}
	s := r.newScan(rctx, nil, st)
	var spans []span
	for _, sel := range selections {
		from, exclusive, upper := sel.bounds()
		if r.rows == 0 || upper != nil && from != nil && bytes.Compare(from, upper) >= 0 {
			continue
		}
		s.end = upper
		if err := s.start(from, exclusive, sel.Cursor); err != nil {
			return nil, err
		}
		if s.key == nil {
			continue
		}
		sp := span{from: bytes.Clone(from), exclusive: exclusive, start: s.ordinal(), end: r.rows}
		if c := sel.Cursor; c != nil {
			sp.cursor = &Cursor{Key: bytes.Clone(c.Key), Offset: c.Offset}
		}
		if upper != nil {
			// The scan is before upper: it continues to the first row at or
			// after it, which exhausts it there.
			if err := s.seek(upper, true); err != nil {
				return nil, err
			}
			sp.end = s.bound
		}
		spans = append(spans, sp)
	}
	slices.SortFunc(spans, func(a, b span) int { return cmp.Compare(a.start, b.start) })
	for _, sp := range spans {
		if n := len(plan.spans); n > 0 && sp.start <= plan.spans[n-1].end {
			plan.spans[n-1].end = max(plan.spans[n-1].end, sp.end)
			continue
		}
		plan.spans = append(plan.spans, sp)
	}
	for _, sp := range plan.spans {
		plan.rows += sp.end - sp.start
	}
	if len(columns) > 0 {
		if err := plan.planColumns(rctx, st); err != nil {
			return nil, err
		}
	}
	plan.exact = plan.checked()
	return plan, nil
}

// planColumns checks every part holding a row of the plan and adds up the
// projected bytes: per column chunk, its dictionary's extent and the
// extents of the pages holding the plan's rows, each page once. It holds
// the OffsetIndexes of one row group's chunks at a time; the Dataset may
// keep their page locations across groups and requests
// (Limits.OffsetIndexMemory).
func (plan *Plan) planColumns(ctx context.Context, st *ReadStats) error {
	r := plan.r
	type planned struct {
		cp   *columnPages
		last int // the last page counted, -1 for none
	}
	current := groupRef{-1, 0}
	var chunks []planned
	for _, sp := range plan.spans {
		for i := r.partAt(sp.start); i < len(r.parts) && r.parts[i].start < sp.end; i++ {
			p, err := r.part(ctx, i, st)
			if err != nil {
				return err
			}
			if err := p.checkProjection(plan.columns); err != nil {
				return err
			}
			for j := sort.Search(len(p.groups), func(j int) bool { return p.groups[j].start+p.groups[j].rows > sp.start }); j < len(p.groups) && p.groups[j].start < sp.end; j++ {
				grp := p.groups[j]
				from, to := max(sp.start, grp.start), min(sp.end, grp.start+grp.rows)
				if g := (groupRef{i, j}); g != current {
					current, chunks = g, make([]planned, len(plan.columns))
					plan.groups = append(plan.groups, groupBytes{start: grp.start})
					for c, name := range plan.columns {
						cp, err := p.openColumn(ctx, grp.physical, name, st, nil)
						if err != nil {
							return err
						}
						chunks[c] = planned{cp: cp, last: -1}
					}
				}
				gb := &plan.groups[len(plan.groups)-1]
				for c := range chunks {
					pc := &chunks[c]
					switch {
					case pc.cp == nil: // absent (COL-2)
						continue
					case pc.cp.locs == nil:
						gb.partial = true
						continue
					case pc.last < 0:
						gb.bytes += pc.cp.dictionaryBytes()
					}
					first, last := pc.cp.pages(from-grp.start, to-grp.start)
					for k := max(first, pc.last+1); k <= last; k++ {
						gb.bytes += pc.cp.locs[k].size
					}
					pc.last = max(pc.last, last)
				}
			}
		}
	}
	for _, gb := range plan.groups {
		plan.bytes += gb.bytes
		plan.partial = plan.partial || gb.partial
	}
	return nil
}

// checked reports whether every part holding a row of the plan is
// checked.
func (plan *Plan) checked() bool {
	r := plan.r
	for _, sp := range plan.spans {
		for i := r.partAt(sp.start); i < len(r.parts) && r.parts[i].start < sp.end; i++ {
			if !r.parts[i].ready.Load() {
				return false
			}
		}
	}
	return true
}
