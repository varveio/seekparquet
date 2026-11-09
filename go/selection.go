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

// RangePlan is a selection planned before it is read: every row of a set
// of ranges, its row count, and the compressed bytes that reading the
// projected columns of those rows will request. A plan is bound to the
// Dataset that made it, and its Scan fails with ErrClosed once the Dataset
// is closed. It is immutable and safe for concurrent use.
//
// A plan certifies what it decoded: the key pages at the two ends of each
// range, checked as a Range checks them (CHECK-2). Its count is derived
// from structure, not from decoding the rows between the ends (CHECK-4):
// the positions the ends decode, the row counts of the row groups, from
// footers a part check verified (CHECK-1), and the OffsetIndex row
// positions. Scan decodes every row and checks every key page it reads.
type RangePlan struct {
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
	stats  ReadStats
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

// PlanRanges plans reading the rows of ranges, with the values of columns
// (as Range.Columns). It reads each range's bounds and Cursor: a range
// with a Limit, Columns or Stats is ErrInvalidRequest. Rows that several
// ranges select are one row of the plan.
//
// Planning decodes what a Range starting at each end would: the key pages
// that locate the first row of the range and the first row after it,
// through the routing and the key page index, and no part known from the
// manifest bounds to begin after the range. With columns, it also checks
// every part holding a selected row and reads the OffsetIndex of each
// projected column chunk the rows span, but no data page. Errors are those
// of Range.
func (d *Dataset) PlanRanges(ctx context.Context, ranges []Range, columns []string) (*RangePlan, error) {
	return d.r.planRanges(ctx, ranges, columns)
}

// Count returns the number of rows Scan yields. It is exact when every
// part holding one of them is checked (CHECK-1), so that the row counts it
// adds up are the parts' footers', verified against the manifest: always
// for a plan with columns, which checks those parts. Otherwise n relies on
// the manifest's row count of a part no read has touched yet, and exact is
// false.
func (plan *RangePlan) Count() (n int64, exact bool) { return plan.rows, plan.exact }

// ProjectedBytes returns the compressed bytes Scan requests from the
// Source for the projected columns: for each column chunk the rows span,
// the extents its OffsetIndex gives the pages holding them and, when the
// chunk has a dictionary page, the dictionary's, each counted once however
// many ranges share it. Footers, page indexes and key pages are not
// counted. Scan reads each counted extent in one request and no other
// byte of those chunks. exact is false when a projected chunk has no
// OffsetIndex (COL-4): Scan walks its page headers, and n omits it.
func (plan *RangePlan) ProjectedBytes() (n int64, exact bool) { return plan.bytes, !plan.partial }

// Stats returns the read statistics of planning, or of a shard Split
// made, the reads that placed its end.
func (plan *RangePlan) Stats() ReadStats { return plan.stats }

// Scan hands fn every row of the plan, with its projected values, in
// dataset order, and stops at the first error fn returns, returning it.
// It positions at each range as Range does and decodes each key page and
// projected page holding the plan's rows once, checking every key page it
// decodes (CHECK-2). st, when not nil, receives the scan's read
// statistics, as Range.Stats.
func (plan *RangePlan) Scan(ctx context.Context, st *ReadStats, fn func(Row) error) (err error) {
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
	columns := slices.Clone(plan.columns)
	s := r.newScan(rctx, nil, st)
	// rows are a batch of one key page's rows, at their positions; pages
	// the projected columns of row group g.
	var rows []*Row
	var at []position
	var g groupRef
	var pages []*columnPages
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		if len(columns) > 0 {
			if pages == nil || at[0].g != g {
				g = at[0].g
				p := r.parts[g.part]
				if err := p.checkProjection(columns); err != nil {
					return err
				}
				pages = make([]*columnPages, len(columns))
				for c, name := range columns {
					cp, err := p.openColumn(rctx, p.groups[g.index].physical, name, st)
					if err != nil {
						return err
					}
					if cp != nil {
						cp.planned = true
					}
					pages[c] = cp
				}
			}
			for c, cp := range pages {
				if err := projectColumn(rows, at, c, cp); err != nil {
					return err
				}
			}
		}
		for _, row := range rows {
			if err := fn(*row); err != nil {
				return err
			}
		}
		rows, at = rows[:0], at[:0]
		return nil
	}
	page := -1
	for _, sp := range plan.spans {
		if sp.ordinal {
			if err := s.seekOrdinal(sp.start); err != nil {
				return err
			}
			s.groupKey, s.groupStart = s.key, sp.start
		} else if err := s.start(sp.from, sp.exclusive, sp.cursor); err != nil {
			return err
		}
		for o := sp.start; o < sp.end && s.key != nil; o++ {
			if len(at) > 0 && (s.g != at[0].g || s.kp.at != page) {
				if err := flush(); err != nil {
					return err
				}
			}
			page = s.kp.at
			rows = append(rows, &Row{Key: bytes.Clone(s.key), Offset: s.offset(), Columns: columns, Values: make([]Value, len(columns))})
			at = append(at, s.at())
			if o+1 < sp.end {
				if err := s.next(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}

// planRanges plans ranges; see Dataset.PlanRanges. One scan positions at
// every end, so that the key pages planning decodes are checked against
// each other (CHECK-2).
func (r *reader) planRanges(ctx context.Context, ranges []Range, columns []string) (_ *RangePlan, err error) {
	for i := range ranges {
		if rng := &ranges[i]; rng.Limit != 0 || rng.Columns != nil || rng.Stats != nil {
			return nil, invalidRequest("a planned range takes no Limit, Columns or Stats")
		}
		if err := ranges[i].Cursor.check(false); err != nil {
			return nil, err
		}
	}
	if err := r.checkProjection(columns); err != nil {
		return nil, err
	}
	rctx, end, err := r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = finish(ctx, rctx, err)
		end()
	}()
	plan := &RangePlan{r: r, columns: slices.Clone(columns)}
	s := r.newScan(rctx, nil, &plan.stats)
	var spans []span
	for _, rng := range ranges {
		from, exclusive, upper := rng.bounds()
		if r.rows == 0 || upper != nil && from != nil && bytes.Compare(from, upper) >= 0 {
			continue
		}
		s.end = upper
		if err := s.start(from, exclusive, rng.Cursor); err != nil {
			return nil, err
		}
		if s.key == nil {
			continue
		}
		sp := span{from: bytes.Clone(from), exclusive: exclusive, start: s.ordinal(), end: r.rows}
		if c := rng.Cursor; c != nil {
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
		if err := plan.planColumns(rctx); err != nil {
			return nil, err
		}
	}
	plan.exact = plan.checked()
	return plan, nil
}

// planColumns checks every part holding a row of the plan and adds up the
// projected bytes: per column chunk, its dictionary's extent and the
// extents of the pages holding the plan's rows, each page once. It keeps
// the OffsetIndexes of one row group's chunks at a time.
func (plan *RangePlan) planColumns(ctx context.Context) error {
	r, st := plan.r, &plan.stats
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
						cp, err := p.openColumn(ctx, grp.physical, name, st)
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
func (plan *RangePlan) checked() bool {
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
