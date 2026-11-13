// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
)

// scan is one request's forward walk over the key column in dataset
// order, the one traversal behind Rows, Get, Scan and List. It positions at the
// first row at or after a key, or after a cursor, and steps row by row
// across row groups. It is the only place offsets are counted: a row's
// offset in its equal-key group (PAGE-1) is its dataset ordinal minus that
// of the group's first row. Every seek to a key lands on a group's first
// row, and a cursor resumes inside a group whose first row it has found.
type scan struct {
	listIndex *listIndex
	synthetic bool
	ctx       context.Context
	r         *reader
	end       []byte // the exclusive upper bound, nil for none
	log       pageLog
	stats     *ReadStats
	kp        *keyPages // the walker on row group g, nil before the first seek
	g         groupRef
	// key is the current row's key, nil once the scan is exhausted; row
	// is its position in row group g.
	key []byte
	row int64
	// groupKey and groupStart are the current key's group: its key and the
	// dataset ordinal of its first row.
	groupKey   []byte
	groupStart int64
	// bound is, once the scan is exhausted, the dataset ordinal of the
	// first row it did not reach: the first at or after the upper bound, or
	// the dataset's row count.
	bound int64
	// keyScratch, when set, is the scratch every key page of the scan's
	// own walkers is decoded into, each overwriting the one before
	// (keyPages.reuse): the batch walker's, whose callbacks borrow a key
	// page only until they return. Then key is valid until the next key
	// page is decoded, and groupKey is moved to groupOwned before it is.
	// Walkers opened by routing (reader.first) never use it.
	keyScratch *pageScratch
	groupOwned []byte
	retire     func(*scan) // keyReuse.retire
	// ahead, when set, is a listing's read-ahead of key pages (readAhead).
	ahead *readAhead
	// plan, when set, plans the scan's windows (readPlan): every walker
	// the scan opens tells it of the pages it decodes.
	plan *readPlan
}

func (r *reader) newScan(ctx context.Context, end []byte, st *ReadStats) *scan {
	return &scan{ctx: ctx, r: r, end: end, log: pageLog{unique: r.key.Unique}, stats: st, listIndex: r.listIndex.Load()}
}

// offset returns the current row's offset in its group.
func (s *scan) offset() int64 { return s.ordinal() - s.groupStart }

func (s *scan) ordinal() int64 { return s.r.group(s.g).start + s.row }

// at returns the current row's position, for reading its values.
func (s *scan) at() position { return position{s.g, s.row} }

// position is a row's row group and its row within it.
type position struct {
	g   groupRef
	row int64
}

// start positions the scan at the first row at or after from (after it
// when exclusive), or after cur when that is later.
func (s *scan) start(from []byte, exclusive bool, cur *Cursor) error {
	if cur != nil && cur.Key != nil {
		if cmp := bytes.Compare(cur.Key, from); from == nil || cmp > 0 || cmp == 0 && !exclusive {
			return s.resume(cur.Key, cur.Offset)
		}
	}
	return s.seek(from, !exclusive)
}

// seek positions the scan at the first row at or after from (after it
// unless inclusive), which is the first row of its group. It continues
// forward from the current row when no earlier row can qualify, and
// otherwise routes through groupFor.
func (s *scan) seek(from []byte, inclusive bool) error {
	if s.synthetic {
		if err := s.syntheticPages(); err != nil {
			return err
		}
		s.kp.page = pageKeys{}
		s.kp.at = s.kp.index.pageAt(s.row) - 1
		s.kp.next = 0
		s.synthetic = false
	}
	g, ok := s.forward(from, inclusive)
	if ok && g != s.g && s.kp.at+1 < len(s.kp.index.pages) {
		// Leaving the scan's group skips the rest of its pages by its
		// index, as the seek that found none there would.
		s.stats.PageIndexSeeks++
	}
	if !ok {
		var err error
		if g, err = s.r.groupFor(s.ctx, from, inclusive, &s.log, s.stats); err != nil {
			return err
		}
	}
	switch {
	case s.kp == nil || s.g.less(g):
	case s.key != nil && before(s.key, from, inclusive):
		g = s.g // continue from the current row
	case g == s.g:
		s.kp.rewind()
	}
	if ok, err := s.open(g); !ok || err != nil {
		return err
	}
	for {
		key, row, ok, err := s.kp.seek(from, inclusive)
		if err != nil {
			return err
		}
		if ok {
			s.land(key, row)
			if s.key != nil {
				s.groupKey, s.groupStart = s.key, s.ordinal()
			}
			return nil
		}
		if ok, err := s.open(s.g.next()); !ok || err != nil {
			return err
		}
	}
}

// forward returns the row group a seek to the first row at or after from
// (after it unless inclusive) decodes first, for a scan over a non-local
// Source that is on a row before it, when the key page indexes at hand
// prove it without a read: the scan's group, or one of the next few in its
// part, the first whose last page's maximum reaches from. A group whose
// maximum is below from holds no such row (RG-5), whatever the length of
// its bounds, so none is passed. A maximum is only an upper bound, so the
// group it stops at is taken only when the routing search would begin no
// later (lands): every page the seek then decodes is one the seek after
// routing decodes too, and forward saves the routing search, which decodes
// the first key page of each row group it compares, a read each over a
// remote Source, and the key page indexes of the groups passed. It reports
// false when the bounds at hand do not decide: the routing search then
// decides, as it does without them.
func (s *scan) forward(from []byte, inclusive bool) (groupRef, bool) {
	if testRouteOnly || s.r.local || s.kp == nil || s.synthetic || s.key == nil || from == nil || !before(s.key, from, inclusive) {
		return groupRef{}, false
	}
	g, index := s.g, s.kp.index
	for hops := 0; ; hops++ {
		if reaches(index.pages[len(index.pages)-1].hi, from, inclusive) {
			return g, s.lands(g, index, from, inclusive)
		}
		if g = g.next(); hops == indexAhead || g.index >= len(s.r.parts[g.part].groups) || s.end != nil && s.r.startsAtOrAfter(g, s.end) {
			return groupRef{}, false
		}
		if index = s.indexAtHand(g); index == nil {
			return groupRef{}, false
		}
	}
}

// testRouteOnly, when set, makes forward report false, so that every seek
// routes as it did before forward routing existed: tests compare the work
// of the two. It is false otherwise.
var testRouteOnly bool

// lands reports whether the routing search (groupFor) for from begins at
// or before row group g of the scan's part, by bounds at hand: whether the
// first key of the group after g is above from as the search compares
// them. The routing search begins at the group before the first whose
// first key is above from, and the scan is in no later group than g, so
// the seek would continue from the scan's row or begin at a group no later
// than g. Every key from the last page of g on is at or above that page's
// minimum, the next group's first key at or above its first page's
// minimum (RG-5); past the part's last group, the routing search chooses
// the first part whose maximum key reaches from. Bounds that do not decide
// (shortened, conservative or not monotone) report false.
func (s *scan) lands(g groupRef, index *groupIndex, from []byte, inclusive bool) bool {
	above := func(key []byte) bool { return s.r.above(key, from, inclusive) }
	if above(index.pages[len(index.pages)-1].lo) {
		return true
	}
	p := s.r.parts[g.part]
	next := g.next()
	if next.index >= len(p.groups) {
		return s.r.bounded && reaches(p.maxKey, from, inclusive)
	}
	if first := p.groups[next.index].first.Load(); first != nil {
		return above(*first)
	}
	if index = s.indexAtHand(next); index != nil {
		return above(index.pages[0].lo)
	}
	return false
}

// indexAtHand returns the key page index of row group g of a checked part
// when it is at hand without a read: the Dataset's, or one decoded and
// checked from bytes the part's retained metadata or the operation's
// windows hold (readAhead.hint, part.keyIndexAtHand). It is nil otherwise.
func (s *scan) indexAtHand(g groupRef) *groupIndex {
	p := s.r.parts[g.part]
	physical := p.groups[g.index].physical
	if index := p.index[physical].Load(); index != nil {
		return index
	}
	if s.ahead != nil {
		return s.ahead.hint(s.r, g)
	}
	return p.keyIndexAtHand(physical, windowsOf(s.ctx))
}

// reaches reports whether a maximum bound reaches from: a row at or after
// from (after it unless inclusive) can be at or below it.
func reaches(hi, from []byte, inclusive bool) bool {
	cmp := bytes.Compare(hi, from)
	return cmp > 0 || cmp == 0 && inclusive
}

// before reports whether key, and so every row up to it, precedes the
// first row at or after from (after it unless inclusive).
func before(key, from []byte, inclusive bool) bool {
	cmp := bytes.Compare(key, from)
	return cmp < 0 || cmp == 0 && !inclusive
}

// resume positions the scan after the row at offset n of key k's group:
// at the row after it when the group has one, else at the first key after
// k. It decodes the page holding the group's first row and the page
// holding the row after the cursor, through the OffsetIndex row
// positions, and none between them, so resuming deep inside a large group
// costs a few pages, not the rows before the cursor.
func (s *scan) resume(k []byte, n int64) error {
	if err := s.seek(k, true); err != nil || s.key == nil || !bytes.Equal(s.key, k) {
		return err
	}
	start := s.groupStart
	limit := s.r.rows
	if s.r.bounded { // a group never crosses parts (PART-4)
		p := s.r.parts[s.g.part]
		limit = p.start + p.rows
	}
	if n < limit-start-1 { // the row after the cursor can be in the group
		if err := s.seekOrdinal(start + n + 1); err != nil {
			return err
		}
		if s.key != nil && bytes.Equal(s.key, k) {
			s.groupKey, s.groupStart = s.key, start
			return nil
		}
	}
	// The group ends at or before the cursor's row.
	return s.seek(k, false)
}

// seekOrdinal positions the scan at a dataset row ordinal below the
// dataset's row count.
func (s *scan) seekOrdinal(ordinal int64) error {
	g, err := s.r.groupAt(s.ctx, ordinal, s.stats)
	if err != nil {
		return err
	}
	if ok, err := s.open(g); !ok || err != nil {
		return err
	}
	row := ordinal - s.r.group(s.g).start
	key, err := s.kp.seekRow(row)
	if err != nil {
		return err
	}
	s.land(key, row)
	return nil
}

// findGroupStart sets the current row's group, the scan having landed on
// it by ordinal (seekOrdinal) where rows of its key may precede it. A key
// unlike the row before it in its key page, or a row first in its part
// (PART-4), begins its group; otherwise the scan seeks the key's first row
// and returns to the row, decoding the key pages between.
func (s *scan) findGroupStart() error {
	at := s.ordinal()
	s.groupKey, s.groupStart = s.key, at
	if s.key == nil || s.r.key.Unique || at == s.r.parts[s.g.part].start {
		return nil
	}
	if n := s.kp.next; n > 0 {
		if !bytes.Equal(s.kp.page.key(n-1), s.key) {
			return nil
		}
	}
	key := bytes.Clone(s.key)
	if err := s.seek(key, true); err != nil {
		return err
	}
	start := s.ordinal()
	if err := s.seekOrdinal(at); err != nil {
		return err
	}
	s.groupKey, s.groupStart = s.key, start
	return nil
}

// Synthetic anchors need no page index until ordinary decoding resumes.
func (s *scan) syntheticPages() error {
	if s.kp != nil {
		return nil
	}
	var err error
	s.kp, err = s.keyPages(s.g)
	return err
}

// keyPages opens the scan's walker on row group g, decoding into
// keyScratch when the scan has it.
func (s *scan) keyPages(g groupRef) (*keyPages, error) {
	kp, err := s.r.keyPages(s.ctx, g, &s.log, s.stats)
	if err != nil {
		return kp, err
	}
	kp.chunk.walk = true
	kp.scan, kp.plan = s, s.plan
	if s.keyScratch == nil {
		return kp, nil
	}
	kp.chunk.scratch, kp.reuse = s.keyScratch, s.keepGroupKey
	return kp, nil
}

// keepGroupKey moves groupKey out of the key page about to be overwritten.
// key is not kept: every caller replaces it once the next page is decoded.
func (s *scan) keepGroupKey() {
	if s.groupKey != nil {
		s.groupOwned = append(s.groupOwned[:0], s.groupKey...)
		s.groupKey = s.groupOwned
	}
	if s.retire != nil {
		s.retire(s)
	}
}

// next moves to the row after the current one.
func (s *scan) next() error {
	if s.synthetic {
		if err := s.syntheticPages(); err != nil {
			return err
		}
		if _, err := s.kp.seekRow(s.row); err != nil {
			return err
		}
		s.synthetic = false
	}
	key, row, ok, err := s.kp.advance()
	for err == nil && !ok {
		if ok, err := s.open(s.g.next()); !ok || err != nil {
			return err
		}
		key, row, ok, err = s.kp.seek(nil, true)
	}
	if err != nil {
		return err
	}
	s.land(key, row)
	if s.key != nil && !bytes.Equal(s.key, s.groupKey) {
		s.groupKey, s.groupStart = s.key, s.ordinal()
	}
	return nil
}

// land makes key, at row of the current row group, the current row, or
// exhausts the scan when key reaches the upper bound.
func (s *scan) land(key []byte, row int64) {
	s.key, s.row = key, row
	if s.end != nil && bytes.Compare(key, s.end) >= 0 {
		s.key, s.bound = nil, s.ordinal()
	}
}

// open moves the scan to row group g, or to the next part's first when g
// is past its part's last, checking the part the first time a read
// touches it. It reports false, exhausting the scan, when there is no such
// group or it is known to begin at or after the upper bound; a part known
// from its bounds to begin there is not touched.
func (s *scan) open(g groupRef) (bool, error) {
	if s.kp != nil && s.g == g {
		return true, nil
	}
	for {
		if g.part >= len(s.r.parts) || s.end != nil && s.r.startsAtOrAfter(g, s.end) {
			s.key, s.bound = nil, s.r.startOf(g)
			return false, nil
		}
		if err := s.ctx.Err(); err != nil {
			return false, err
		}
		p, err := s.r.part(s.ctx, g.part, s.stats)
		if err != nil {
			return false, err
		}
		if g.index < len(p.groups) {
			break
		}
		g = groupRef{g.part + 1, 0}
	}
	kp, err := s.keyPages(g)
	if err != nil {
		return false, err
	}
	s.kp, s.g, s.synthetic = kp, g, false
	return true, nil
}

// shortcut reports the common prefix that the row group a seek to from
// lands in holds whole, and that group: its first key and the next group's share one
// common prefix, so every key between them, and so every candidate key of
// the group at or after from, rolls up to it (keys between two keys that
// begin with a prefix begin with it too), and the next group's first key
// is a candidate witness for it below the upper bound. The listing then
// emits the prefix without reading the group. It is taken once the
// group's first key is known (decoded by an earlier read, a routing probe,
// or Verify). A group, or a next group, in a part known from its bounds to
// begin at or after the upper bound is not touched.
func (s *scan) shortcut(from, prefix []byte, delim byte) ([]byte, groupRef, error) {
	g, err := s.r.groupFor(s.ctx, from, true, &s.log, s.stats)
	if err != nil || g.part >= len(s.r.parts) || s.end != nil && s.r.startsAtOrAfter(g, s.end) {
		return nil, g, err // the seek after it applies the upper bound without touching the part
	}
	if _, err := s.r.part(s.ctx, g.part, s.stats); err != nil || g.index >= len(s.r.parts[g.part].groups) {
		return nil, g, err
	}
	first := s.r.group(g).first.Load()
	if first == nil {
		return nil, g, nil
	}
	whole := commonPrefix(*first, prefix, delim)
	if whole == nil {
		return nil, g, nil
	}
	after, ok := s.r.adjacent(g, 1)
	if !ok { // the last group of its part, or of the dataset
		if g.part+1 >= len(s.r.parts) || s.end != nil && s.r.startsAtOrAfter(groupRef{g.part + 1, 0}, s.end) {
			return nil, g, nil
		}
		after = groupRef{g.part + 1, 0}
	}
	next, err := s.r.first(s.ctx, after, &s.log, s.stats)
	if err != nil {
		return nil, g, err
	}
	if !bytes.Equal(whole, commonPrefix(next, prefix, delim)) || s.end != nil && bytes.Compare(next, s.end) >= 0 {
		return nil, g, nil
	}
	return bytes.Clone(whole), g, nil
}
