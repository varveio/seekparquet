// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"cmp"
	"context"
	"runtime"
	"slices"
	"sort"
)

// Plan is a set of selections planned before they are read: every row of
// the selections, its row count, and the compressed bytes that reading the
// projected columns of those rows will request. A plan is bound to the
// Dataset that made it, and its scans fail with ErrClosed once the Dataset
// is closed. It is immutable and safe for concurrent use.
//
// A plan certifies what it decoded: the key pages at the ends of each
// selection that metadata did not place, checked as Dataset.Rows checks
// them (CHECK-2). Its count is derived from structure, not from decoding
// the rows between the ends (CHECK-4): the positions the ends decode or
// metadata proves (or the row positions PlanRows is given), the row
// counts of the row groups, from footers a part check verified (CHECK-1),
// and the OffsetIndex row positions. Scan decodes every row and checks
// every key page it reads; ScanValues reads no key page.
//
// A plan with columns keeps the checked page locations of the projected
// chunks holding its rows, to price SelectRows: the Dataset's own, kept
// within Limits.OffsetIndexMemory, or, when the Dataset did not keep them,
// locations reserved in that same budget for as long as the plan, or a
// plan made from it by SelectRows or Split, is reachable; locations that
// do not fit are not kept.
type Plan struct {
	r       *reader
	columns []string
	// spans are the selected rows: disjoint, ascending and not adjacent.
	spans []span
	// inputs are the rows of each input selection or row span, in input
	// order, before the union (RowSpans).
	inputs []rowRange
	rows   int64
	exact  bool
	// bytes are the projected bytes Scan requests; partial is set when a
	// projected column chunk has no OffsetIndex, whose pages bytes omits.
	bytes   int64
	partial bool
	// groups are the projected bytes counted in each row group holding a
	// row of the plan, in dataset order, with the page locations that
	// priced them (SelectRows).
	groups []groupBytes
	// pin, when set, holds the reservation of page locations the plan's
	// groups keep privately, shared by the plans made from it.
	pin *locsPin
}

// rowRange is the dataset rows [start, end).
type rowRange struct{ start, end int64 }

// groupBytes is the projected bytes a plan counts in one row group: its
// pages holding the plan's rows and its dictionaries. partial is set when
// a projected chunk of the group has no OffsetIndex, or its locations are
// not kept.
type groupBytes struct {
	start   int64 // the dataset ordinal of the group's first row
	rows    int64 // the group's row count
	bytes   int64
	partial bool
	// chunks are the group's distinct projected chunks. Repeated projection
	// slots share physical extents and must only be priced and reserved once.
	chunks []plannedChunk
}

// plannedChunk is what a plan keeps of one projected column chunk to price
// rows of it without I/O: its checked page locations and its dictionary's
// extent.
type plannedChunk struct {
	present bool      // the part has the column (COL-2)
	private bool      // locs are not the Dataset's kept locations (keep)
	locs    []pageLoc // nil when the chunk has no OffsetIndex, or they are not kept
	dict    int64
}

// locsPin is a reservation in Limits.OffsetIndexMemory of the page
// locations plans keep privately, given back once no plan holds it.
type locsPin struct {
	// Keep the owner pointer here so the pin cannot share a tiny allocation
	// with a live pin, which could prevent its cleanup from ever running.
	r    *reader
	size int64
}

// keep reserves the private page locations of gb's chunks in the
// Dataset's OffsetIndexMemory, and drops those beyond it.
func (plan *Plan) keep(gb *groupBytes) {
	for c := range gb.chunks {
		pc := &gb.chunks[c]
		if !pc.private {
			continue
		}
		pc.private = false
		if size := locsSize(pc.locs); plan.r.reserveLocations(size) {
			if plan.pin == nil {
				plan.pin = &locsPin{r: plan.r}
			}
			plan.pin.size += size
		} else {
			pc.locs = nil
		}
	}
}

// release registers the plan's reservation to be given back once neither
// it nor any plan made from it is reachable.
func (plan *Plan) release() {
	if plan.pin != nil {
		runtime.AddCleanup(plan.pin, func(c locsCleanup) { c.r.locsMemory.Add(-c.size) }, locsCleanup{plan.pin.r, plan.pin.size})
	}
}

// locsCleanup gives back a locsPin's reservation.
type locsCleanup struct {
	r    *reader
	size int64
}

// price sets the group's bytes and partial from the plan's rows in it,
// spans being ascending and disjoint: per chunk, its dictionary's extent
// and the extents of the pages holding those rows, each page once.
func (gb *groupBytes) price(spans []span) {
	gb.bytes, gb.partial = 0, false
	lo, hi := gb.start, gb.start+gb.rows
	k := sort.Search(len(spans), func(k int) bool { return spans[k].end > lo })
	for c := range gb.chunks {
		pc := &gb.chunks[c]
		switch {
		case !pc.present:
			continue
		case pc.locs == nil:
			gb.partial = true
			continue
		}
		last := -1
		for _, sp := range spans[k:] {
			if sp.start >= hi {
				break
			}
			from, to := max(sp.start, lo)-lo, min(sp.end, hi)-lo
			if last < 0 {
				gb.bytes += pc.dict
			}
			first, end := pageAt(pc.locs, from), pageAt(pc.locs, to-1)
			for i := max(first, last+1); i <= end; i++ {
				gb.bytes += pc.locs[i].size
			}
			last = max(last, end)
		}
	}
}

// span is the dataset rows [start, end), and where a scan finds start: the
// first row at or after from (after it when exclusive), or after cursor;
// or, when ordinal is set, start itself, the first row of its key group (a
// shard boundary) unless inside is set, when it may follow rows of its key.
type span struct {
	from       []byte
	exclusive  bool
	cursor     *Cursor
	ordinal    bool
	inside     bool
	start, end int64
}

// Plan plans reading the rows of selections, with the values of columns
// (as Query.Columns). Each selection's Cursor, a row cursor, starts it
// after that row, inside a group of equal keys too. Rows that several
// selections select are one row of the plan.
//
// Planning first resolves each end from metadata already available: an
// unbounded start is the first row; the manifest bounds place an end that
// falls between parts, or past the last; when keys are unique, a row
// group's first key a read already decoded places an end equal to it. It
// decodes no key page for such an end, though it checks the part holding
// the selection's first row. Otherwise it decodes what Rows starting at
// that end would: the key pages that locate the first row of the selection
// or the first row after it, through the routing and the key page index,
// and no part known from the manifest bounds to begin after the selection.
// A selection with a Cursor always decodes its start. With columns, it also
// checks every part holding a selected row and reads the OffsetIndex of
// each projected column chunk the rows span, unless an earlier request
// kept its page locations (Limits.OffsetIndexMemory), but no data page.
// Errors are those of Rows. st, when not nil, is reset and receives the
// reads of planning, whether it succeeds or fails.
func (d *Dataset) Plan(ctx context.Context, selections []Selection, columns []string, st *ReadStats) (*Plan, error) {
	return d.r.plan(ctx, selections, columns, st)
}

// Count returns the cardinality of the plan's union, without I/O.
//
// exact is true when the selection boundaries are established and every
// part contributing rows has passed its part check, including agreement
// between manifest, footer and row-group row counts. Empty plans are exact.
//
// When exact is false, n is provisional and must not be presented as an
// exact count. Exactness does not certify keys or values not decoded
// while planning. It is a snapshot of the evidence at plan construction.
//
// A plan with columns checks the parts holding its rows, so its count is
// exact; without, a part no read has checked contributes the manifest's
// row count, and exact is false.
func (plan *Plan) Count() (n int64, exact bool) { return plan.rows, plan.exact }

// ProjectedBytes returns the unique compressed extents Scan and ScanValues
// require of the projected columns: for each column chunk the rows span,
// the extents its OffsetIndex gives the pages holding them and, when the
// chunk has a dictionary page, the dictionary's, each counted once however
// many ranges share it. Footers, page indexes and key pages are not
// counted. It does not say how many storage requests read them: windowing
// or coalescing may join or split extents. exact is false when a projected
// chunk has no OffsetIndex (COL-4), whose pages are found by walking their
// headers, and n omits it; or, for a plan of SelectRows, when the chunk's
// page locations were not kept.
func (plan *Plan) ProjectedBytes() (n int64, exact bool) { return plan.bytes, !plan.partial }

// Columns returns the plan's projected column names, in order, as a copy
// the caller owns.
func (plan *Plan) Columns() []string { return slices.Clone(plan.columns) }

// plan plans selections; see Dataset.Plan. One scan positions at every
// end it decodes, so that the key pages planning decodes are checked
// against each other (CHECK-2). An end that metadata proves (cut) is not
// decoded; the part holding a selection's first row is checked all the
// same.
func (r *reader) plan(ctx context.Context, selections []Selection, columns []string, st *ReadStats) (_ *Plan, err error) {
	st, spare := takeStats(st)
	defer dropStats(spare)
	for i := range selections {
		if err := selections[i].Cursor.check(false); err != nil {
			return nil, err
		}
	}
	if err := r.checkProjection(columns); err != nil {
		return nil, err
	}
	rctx, err := r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rctx.done(ctx, &err, st)
	plan := &Plan{r: r, columns: slices.Clone(columns), inputs: make([]rowRange, 0, len(selections))}
	s := r.newScan(rctx, nil, st)
	var spans []span
	for _, sel := range selections {
		from, exclusive, upper := sel.bounds()
		if r.rows == 0 || upper != nil && from != nil && bytes.Compare(from, upper) >= 0 {
			plan.inputs = append(plan.inputs, rowRange{})
			continue
		}
		start, ok := int64(0), false
		if sel.Cursor == nil {
			start, ok = r.cut(from, !exclusive)
		}
		cutStart := ok
		s.end = upper
		if !ok {
			if err := s.start(from, exclusive, sel.Cursor); err != nil {
				return nil, err
			}
			if s.key == nil {
				plan.inputs = append(plan.inputs, rowRange{s.bound, s.bound})
				continue
			}
			start = s.ordinal()
		}
		end := r.rows
		if upper != nil {
			if end, ok = r.cut(upper, true); !ok {
				// The scan continues to the first row at or after upper,
				// which exhausts it there.
				if err := s.seek(upper, true); err != nil {
					return nil, err
				}
				end = s.bound
			}
		}
		if end <= start {
			plan.inputs = append(plan.inputs, rowRange{start, start})
			continue
		}
		plan.inputs = append(plan.inputs, rowRange{start, end})
		// The part holding the selection's first row is checked, as
		// decoding the start would check it, so that a plan over it counts
		// its footer's rows and can split at its row groups.
		if cutStart {
			if _, err := r.part(rctx, r.partAt(start), st); err != nil {
				return nil, err
			}
		}
		sp := span{from: bytes.Clone(from), exclusive: exclusive, start: start, end: end}
		if c := sel.Cursor; c != nil {
			sp.cursor = &Cursor{Key: bytes.Clone(c.Key), Offset: c.Offset}
		}
		spans = append(spans, sp)
	}
	plan.union(spans)
	if len(columns) > 0 {
		if err := plan.planColumns(rctx, st); err != nil {
			return nil, err
		}
	}
	plan.exact = plan.checked()
	return plan, nil
}

// union sets the plan's spans and row count to the union of spans, which
// it sorts: overlapping or adjacent spans join, keeping the earlier one's
// start.
func (plan *Plan) union(spans []span) {
	slices.SortStableFunc(spans, func(a, b span) int { return cmp.Compare(a.start, b.start) })
	for _, sp := range spans {
		if sp.end <= sp.start {
			continue
		}
		if n := len(plan.spans); n > 0 && sp.start <= plan.spans[n-1].end {
			plan.spans[n-1].end = max(plan.spans[n-1].end, sp.end)
			continue
		}
		plan.spans = append(plan.spans, sp)
	}
	plan.rows = 0
	for _, sp := range plan.spans {
		plan.rows += sp.end - sp.start
	}
}

// cut returns the dataset ordinal of the first row at or after key (after
// it unless inclusive) when metadata already proves it, without I/O: a nil
// key's is the first row; the manifest bounds prove a part's first row, or
// the dataset's end; when keys are unique, a checked part's row-group first
// key decoded equal to key proves its group's first row, or the row after
// it. Otherwise ok is false, and the caller decodes the key pages.
func (r *reader) cut(key []byte, inclusive bool) (ordinal int64, ok bool) {
	switch {
	case key == nil && inclusive:
		return 0, true
	case key == nil, !r.bounded:
		return 0, false
	}
	i := sort.Search(len(r.parts), func(i int) bool {
		c := bytes.Compare(r.parts[i].maxKey, key)
		return c > 0 || c == 0 && inclusive
	})
	if i == len(r.parts) {
		return r.rows, true
	}
	p := r.parts[i]
	if c := bytes.Compare(key, p.minKey); c < 0 || c == 0 && inclusive {
		return p.start, true
	}
	if !r.key.Unique || !p.ready.Load() {
		return 0, false
	}
	// Unique first keys ascend strictly: the group whose first key is key
	// begins with key's row, and every row before it is below key.
	for _, g := range p.groups {
		if first := g.first.Load(); first != nil && bytes.Equal(*first, key) {
			if inclusive {
				return g.start, true
			}
			return g.start + 1, true
		}
	}
	return 0, false
}

// planColumns checks every part holding a row of the plan and prices the
// projected bytes (groupBytes.price), keeping each group's page locations
// for SelectRows: those the Dataset keeps (Limits.OffsetIndexMemory) are
// shared; those it does not are reserved in the same budget, pinned by the
// plan, and, past the budget, dropped, their chunk then priced as partial
// by SelectRows.
func (plan *Plan) planColumns(ctx context.Context, st *ReadStats) error {
	r := plan.r
	current := groupRef{-1, 0}
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
				if g := (groupRef{i, j}); g != current {
					current = g
					grp := p.groups[j]
					gb := groupBytes{start: grp.start, rows: grp.rows, chunks: make([]plannedChunk, 0, len(plan.columns))}
					for c, name := range plan.columns {
						if slices.Contains(plan.columns[:c], name) {
							continue
						}
						cp, err := p.openColumn(ctx, grp.physical, name, st, nil)
						if err != nil {
							return err
						}
						if cp != nil {
							pc := plannedChunk{present: true, locs: cp.locs}
							if cp.locs != nil {
								pc.dict = cp.dictionaryBytes()
								kept := p.locs[grp.physical][p.leaves[name].index].Load()
								pc.private = kept == nil || &(*kept)[0] != &cp.locs[0]
							}
							gb.chunks = append(gb.chunks, pc)
						}
					}
					plan.groups = append(plan.groups, gb)
				}
			}
		}
	}
	plan.bytes, plan.partial = 0, false
	for k := range plan.groups {
		gb := &plan.groups[k]
		gb.price(plan.spans)
		plan.bytes += gb.bytes
		plan.partial = plan.partial || gb.partial
		plan.keep(gb)
	}
	plan.release()
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
