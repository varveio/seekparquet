// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
)

// scan is one request's forward walk over the key column in dataset
// order, the one traversal behind Range and List. It positions at the
// first row at or after a key, or after a cursor, and steps row by row
// across row groups. It is the only place offsets are counted: a row's
// offset in its equal-key group (PAGE-1) is its dataset ordinal minus that
// of the group's first row. Every seek to a key lands on a group's first
// row, and a cursor resumes inside a group whose first row it has found.
type scan struct {
	ctx   context.Context
	r     *reader
	end   []byte // the exclusive upper bound, nil for none
	log   pageLog
	stats *ReadStats
	kp    *keyPages // the walker on row group g, nil before the first seek
	g     groupRef
	// key is the current row's key, nil once the scan is exhausted; row
	// is its position in row group g.
	key []byte
	row int64
	// groupKey and groupStart are the current key's group: its key and the
	// dataset ordinal of its first row.
	groupKey   []byte
	groupStart int64
}

func (r *reader) newScan(ctx context.Context, end []byte, st *ReadStats) *scan {
	return &scan{ctx: ctx, r: r, end: end, log: pageLog{unique: r.key.Unique}, stats: st}
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
	g, err := s.r.groupFor(s.ctx, from, inclusive, &s.log, s.stats)
	if err != nil {
		return err
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

// next moves to the row after the current one.
func (s *scan) next() error {
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
		s.key = nil
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
			s.key = nil
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
	kp, err := s.r.keyPages(s.ctx, g, &s.log, s.stats)
	if err != nil {
		return false, err
	}
	s.kp, s.g = kp, g
	return true, nil
}

// shortcut reports the common prefix that the row group a seek to from
// lands in holds whole: its first key and the next group's share one
// common prefix, so every key between them, and so every candidate key of
// the group at or after from, rolls up to it (keys between two keys that
// begin with a prefix begin with it too), and the next group's first key
// is a candidate witness for it below the upper bound. The listing then
// emits the prefix without reading the group. It is taken once the
// group's first key is known (decoded by an earlier read, a routing probe,
// or Verify). A next group in a part known from its bounds to begin at or
// after the upper bound is not touched.
func (s *scan) shortcut(from, prefix []byte, delim byte) ([]byte, error) {
	g, err := s.r.groupFor(s.ctx, from, true, &s.log, s.stats)
	if err != nil || g.part >= len(s.r.parts) {
		return nil, err
	}
	if _, err := s.r.part(s.ctx, g.part, s.stats); err != nil || g.index >= len(s.r.parts[g.part].groups) {
		return nil, err
	}
	first := s.r.group(g).first.Load()
	if first == nil {
		return nil, nil
	}
	whole := commonPrefix(*first, prefix, delim)
	if whole == nil {
		return nil, nil
	}
	after, ok := s.r.adjacent(g, 1)
	if !ok { // the last group of its part, or of the dataset
		if g.part+1 >= len(s.r.parts) || s.end != nil && s.r.startsAtOrAfter(groupRef{g.part + 1, 0}, s.end) {
			return nil, nil
		}
		after = groupRef{g.part + 1, 0}
	}
	next, err := s.r.first(s.ctx, after, &s.log, s.stats)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(whole, commonPrefix(next, prefix, delim)) || s.end != nil && bytes.Compare(next, s.end) >= 0 {
		return nil, nil
	}
	s.stats.GroupsSkipped++
	return bytes.Clone(whole), nil
}
