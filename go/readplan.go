// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"sort"
)

// readPlan plans the windows of one scan over a remote Source (windows):
// the pages it will decode, from the key page it needs and the windows do
// not hold, read ahead in waves bounded by readWaveBytes. Each wave
// finishes before decoding resumes. A wave
// holds the rows the scan reads in the row group it is in and in the row
// groups after it, in checked parts: every row of a planned scan's spans,
// which the scan needs; otherwise the rows up to the scan's upper bound
// and limit, which it needs in the row group it is in, and which beyond it
// are speculation. Pages are located by the checked key page index and
// page locations the Dataset keeps, or else by the OffsetIndexes held in
// the part's retained metadata, decoded only to locate pages; a column
// chunk located by neither is read whole. Nothing a plan reads is trusted:
// every page is checked as it is decoded.
type readPlan struct {
	r       *reader
	w       *windows
	columns []string
	// spans are a planned scan's rows, nil otherwise. limit is the most
	// rows a scan without them reads from the first, 0 for no limit; end
	// the dataset ordinal past its last, once the first is known.
	spans     []span
	limit     int64
	end       int64
	speculate bool
	waveBytes int64
	// prev is the first row of the key page decoded last.
	prev int64
}

// testNoWindows, set by tests that observe every page read, plans no
// windows.
var testNoWindows bool

// newReadPlan returns the plan of a scan of ctx's operation reading the
// key column and columns, nil when r reads as suits a local file or the
// scan reads only what it needs as it needs it.
func (r *reader) newReadPlan(ctx context.Context, columns []string, spans []span, limit int64) *readPlan {
	o := operationOf(ctx)
	speculate := !r.policy.DisableSpeculation
	if r.local || testNoWindows || o == nil || o.r == nil || spans == nil && !speculate {
		return nil
	}
	return &readPlan{r: r, w: o.windows(), columns: columns, spans: spans, limit: limit, end: -1, speculate: speculate,
		waveBytes: readWaveBytes, prev: -1}
}

// page is called before the scan's walker on row group g decodes key page
// i: it drops the windows of the rows the scan has passed, and reads a wave
// from the page when no window holds it, or the next wave when the scan
// has reached the latest one.
func (rp *readPlan) page(s *scan, k *keyPages, i int) {
	ordinal := s.r.group(k.g).start + k.index.pages[i].first
	// The rows before the page decoded before this one are passed: a seek
	// may decode a page and then return to the one before it (resume).
	rp.w.release(min(rp.prev, ordinal))
	rp.prev = ordinal
	loc := k.index.pages[i].pageLoc
	if !rp.w.holds(k.part, loc.offset, loc.size) {
		rp.wave(s, k.g, ordinal)
	}
}

// wave reads ahead the rows the scan reads from the dataset ordinal from
// on, from row group g.
func (rp *readPlan) wave(s *scan, g groupRef, from int64) {
	r := s.r
	if rp.spans == nil && rp.limit > 0 && rp.end < 0 {
		rp.end = from + rp.limit // the first page the scan reads holds its first row
	}
	var need, spec []extent
	total := int64(0)
	for n := 0; ; n++ {
		if g.part < len(r.parts) && r.parts[g.part].ready.Load() && g.index >= len(r.parts[g.part].groups) {
			g = groupRef{g.part + 1, 0}
		}
		if g.part >= len(r.parts) || !r.parts[g.part].ready.Load() {
			break
		}
		if n > 0 && rp.spans == nil && (!rp.speculate || s.end != nil && r.startsAtOrAfter(g, s.end)) {
			break
		}
		p := r.parts[g.part]
		grp := p.groups[g.index]
		ranges, done := rp.rows(grp, max(from, grp.start))
		if done {
			break
		}
		units := rp.units(p, g, ranges, s.end)
		full, stop := true, false
		for _, u := range units {
			if u.stop {
				stop = true
				break
			}
			size := int64(0)
			for _, e := range u.exts {
				size += e.size
			}
			if total > 0 && total+size > rp.waveBytes {
				full = false
				break
			}
			total += size
			if n == 0 || rp.spans != nil {
				need = append(need, u.exts...)
			} else {
				spec = append(spec, u.exts...)
			}
		}
		if !full || stop {
			break
		}
		g = g.next()
	}
	sortExtents(need)
	sortExtents(spec)
	rp.w.fetchWave(need, spec)
}

// rows returns the ranges of rows of row group grp, from the dataset
// ordinal lo on, the scan reads, as dataset ordinals, and reports whether
// no later group holds any.
func (rp *readPlan) rows(grp *group, lo int64) (ranges [][2]int64, done bool) {
	ge := grp.start + grp.rows
	if rp.spans == nil {
		hi := ge
		if rp.end >= 0 {
			if lo >= rp.end {
				return nil, true
			}
			hi = min(hi, rp.end)
		}
		return [][2]int64{{lo, hi}}, false
	}
	j := sort.Search(len(rp.spans), func(j int) bool { return rp.spans[j].end > lo })
	if j == len(rp.spans) {
		return nil, true
	}
	for ; j < len(rp.spans) && rp.spans[j].start < ge; j++ {
		ranges = append(ranges, [2]int64{max(lo, rp.spans[j].start), min(ge, rp.spans[j].end)})
	}
	return ranges, false
}

// unit is the extents of a run of rows a wave reads together, from the
// dataset ordinal from: one key page's rows and the value pages holding
// them; stop is set on the last unit before the scan's upper bound.
type unit struct {
	from int64
	exts []extent
	stop bool
}

// units returns the units of row group g of part p the scan reads for the
// row ranges, in order: per key page, its extent and those of the pages of
// each projected column holding its rows not in an earlier unit, with the
// column's dictionary in the first; a column chunk no locations locate is
// read whole in the first unit, and so is the group when its key column is
// not located. A key page beginning at or after end, and the rows from it
// on, are left out.
func (rp *readPlan) units(p *part, g groupRef, ranges [][2]int64, end []byte) []unit {
	grp := p.groups[g.index]
	phys := grp.physical
	type column struct {
		locs       []pageLoc
		start, end int64
		next       int // the first page not yet in a unit
		whole      bool
	}
	var cols []column
	add := func(col leaf) bool {
		meta := &p.footer.Metadata().RowGroups[phys].Columns[col.index].MetaData
		start, stop, ok := chunkRange(meta)
		if !ok || stop > p.size {
			return false
		}
		cols = append(cols, column{locs: p.planLocs(phys, col, start, stop), start: start, end: stop})
		return true
	}
	if !add(p.key) {
		return nil
	}
	for _, name := range rp.columns {
		if col, ok := p.leaves[name]; ok && col.index != p.key.index {
			add(col)
		}
	}
	var index *groupIndex
	if end != nil {
		index = p.index[phys].Load()
	}
	keys := cols[0].locs
	var units []unit
	totalBytes, descriptors := int64(0), 0
	groupEnd := grp.start + grp.rows
	for _, rg := range ranges {
		a, b := rg[0]-grp.start, rg[1]-grp.start
		for a < b {
			u := unit{from: grp.start + a}
			hi := b
			if keys != nil {
				kp := pageAt(keys, a)
				if index != nil && bytes.Compare(index.pages[kp].lo, end) >= 0 {
					units = append(units, unit{from: u.from, stop: true})
					return units
				}
				hi = min(b, keys[kp].first+keys[kp].rows)
			}
			for c := range cols {
				col := &cols[c]
				if col.whole {
					continue
				}
				if col.locs == nil {
					col.whole = true
					u.exts = append(u.exts, extent{p, col.start, col.end - col.start, groupEnd})
					continue
				}
				if c > 0 && col.next == 0 && col.locs[0].offset > col.start {
					u.exts = append(u.exts, extent{p, col.start, col.locs[0].offset - col.start, groupEnd})
				}
				for j := max(col.next, pageAt(col.locs, a)); j < len(col.locs) && col.locs[j].first < hi; j++ {
					loc := col.locs[j]
					u.exts = append(u.exts, extent{p, loc.offset, loc.size, grp.start + loc.first + loc.rows})
					col.next = j + 1
					if descriptors+len(u.exts) >= MaxReadRanges {
						return append(units, u, unit{stop: true})
					}
				}
			}
			units = append(units, u)
			for _, e := range u.exts {
				totalBytes += e.size
			}
			descriptors += len(u.exts)
			if totalBytes >= rp.waveBytes || descriptors >= MaxReadRanges {
				return append(units, unit{stop: true})
			}
			a = hi
		}
	}
	return units
}

// planLocs returns the page locations of column col's chunk [start, end)
// in physical row group phys, to plan reads with: the key page index or
// page locations the Dataset keeps, or else those of the OffsetIndex held
// in the part's retained metadata, decoded and checked only as locations;
// nil when none are at hand.
func (p *part) planLocs(phys int, col leaf, start, end int64) []pageLoc {
	if col.index == p.key.index {
		if index := p.index[phys].Load(); index != nil {
			locs := make([]pageLoc, len(index.pages))
			for i := range index.pages {
				locs[i] = index.pages[i].pageLoc
			}
			return locs
		}
	} else if locs := p.locs[phys][col.index].Load(); locs != nil {
		return *locs
	}
	meta := &p.footer.Metadata().RowGroups[phys].Columns[col.index]
	if meta.OffsetIndexOffset <= 0 || meta.OffsetIndexLength <= 0 {
		return nil
	}
	raw := p.retainedBytes(meta.OffsetIndexOffset, int64(meta.OffsetIndexLength))
	if raw == nil {
		return nil
	}
	index, err := p.decodeOffsetIndex(raw)
	if err != nil {
		return nil
	}
	locs, err := checkLocations(index, p.footer.Metadata().RowGroups[phys].NumRows, start, end)
	if err != nil {
		return nil
	}
	return locs
}
