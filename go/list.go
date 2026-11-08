// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"slices"

	"github.com/parquet-go/parquet-go"
)

// List is a delimited listing request (spec §6.2). The candidate keys are
// those that begin with Prefix, are above After when it is set, and are
// below End when it is set. A candidate whose bytes after Prefix contain
// Delimiter yields the common prefix up to and including that delimiter;
// any other candidate yields its row. The entries are distinct, in
// ascending byte order, at most Limit of them.
type List struct {
	// Prefix scopes the listing; empty lists every key. The read is
	// confined to [Prefix, Successor(Prefix)).
	Prefix []byte
	// Delimiter is the single delimiter byte (LIST-1).
	Delimiter byte
	// After, when non-nil, excludes keys at or below it.
	After []byte
	// Continuation marks a request for the page after one that ended at
	// After: a common prefix at or before After is then not returned,
	// because that page returned it (LIST-5). To page through a listing,
	// repeat the request with After set to the last entry's bytes
	// (Entry.Bytes) and Continuation true (LIST-6).
	Continuation bool
	// End, when non-nil, excludes keys at or above it.
	End []byte
	// Limit is the most entries returned. It must be positive (QUERY-1).
	Limit int
	// Columns names the columns projected for each row entry, as
	// Range.Columns.
	Columns []string
}

// Entry is one listing entry: exactly one of CommonPrefix and Row is set.
type Entry struct {
	CommonPrefix []byte
	Row          *Row
}

// IsPrefix reports whether the entry is a common prefix.
func (e Entry) IsPrefix() bool { return e.Row == nil }

// Bytes returns the entry's bytes: the common prefix, or the row's key.
// It is what a continuation request takes as After.
func (e Entry) Bytes() []byte {
	if e.Row != nil {
		return e.Row.Key
	}
	return e.CommonPrefix
}

// List answers req as a skip-scan: after a common prefix it resumes at the
// prefix's successor through the routing index and the page index instead
// of decoding the keys under it, and a row group whose first key and the
// next group's first key share a common prefix is skipped without a read.
// Keys under a prefix that share a page with the keys around them are
// decoded. Its entries are those of the specification's definition.
func (r *Reader) List(ctx context.Context, req List, stats *Stats) ([]Entry, error) {
	if req.Limit <= 0 {
		return nil, invalidRequest("limit %d is not positive", req.Limit)
	}
	if err := r.checkProjection(req.Columns); err != nil {
		return nil, err
	}
	rctx, end, err := r.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	if stats == nil {
		stats = new(Stats)
	}
	if len(r.groups) == 0 {
		return nil, nil
	}
	s := listScan{r: r, req: req, columns: slices.Clone(req.Columns), stats: stats, cursorGroup: -1}
	// The read is the intersection of [Prefix, Successor(Prefix)), the
	// keys above After and the keys below End.
	s.from, s.inclusive = req.Prefix, true
	if len(req.Prefix) == 0 {
		s.from = nil
	}
	if req.After != nil && bytes.Compare(req.After, req.Prefix) >= 0 {
		s.from, s.inclusive = req.After, false
		s.suppress = req.Continuation
	}
	s.to = req.End
	if upper, ok := Successor(req.Prefix); ok && (s.to == nil || bytes.Compare(upper, s.to) < 0) {
		s.to = upper
	}
	if s.to != nil && s.from != nil && bytes.Compare(s.from, s.to) >= 0 {
		return nil, nil
	}
	defer s.close()
	out, err := s.run(rctx)
	return out, finish(ctx, rctx, err)
}

type listScan struct {
	r       *Reader
	req     List
	columns []string
	stats   *Stats
	// from (inclusive or not) and to bound the keys read; suppress drops
	// common prefixes at or before from, which is then After.
	from        []byte
	inclusive   bool
	to          []byte
	suppress    bool
	out         []Entry
	cursor      *keyCursor
	cursorGroup int
	seq         []byte
	values      valueRun
}

// valueRun holds the values of consecutive rows of one row group.
type valueRun struct {
	group  int
	first  int64
	values [][]parquet.Value
	n      int
}

func (s *listScan) close() {
	if s.cursor != nil {
		s.cursor.close()
	}
}

// emitPrefix adds a common prefix unless LIST-5 suppresses it.
func (s *listScan) emitPrefix(prefix []byte) {
	if s.suppress && bytes.Compare(prefix, s.from) <= 0 {
		return
	}
	s.out = append(s.out, Entry{CommonPrefix: bytes.Clone(prefix)})
}

func (s *listScan) run(ctx context.Context) ([]Entry, error) {
	r, req := s.r, s.req
	cursor, inclusive := s.from, s.inclusive
	for len(s.out) < req.Limit {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		g := r.groupFor(cursor)
		grp := r.groups[g]
		if s.to != nil && bytes.Compare(grp.first, s.to) >= 0 {
			break
		}
		// A row group whose first key and the next group's first key share
		// a common prefix holds only keys under it: keys between two keys
		// that begin with a prefix begin with it too. Where the scan lands
		// inside the group, the next group's first key is a key at or after
		// it under the prefix, so the prefix is emitted without a read.
		// When the prefix is not suppressed at the floor, that witness must
		// also lie below the upper bound.
		if g+1 < len(r.groups) {
			whole := CommonPrefix(grp.first, req.Prefix, req.Delimiter)
			next := r.groups[g+1].first
			if whole != nil && bytes.Equal(whole, CommonPrefix(next, req.Prefix, req.Delimiter)) &&
				(s.suppress || s.to == nil || bytes.Compare(next, s.to) < 0) {
				s.stats.GroupShortcuts++
				s.emitPrefix(whole)
				successor, ok := Successor(whole)
				if !ok {
					break
				}
				cursor, inclusive = successor, true
				continue
			}
		}
		if s.cursorGroup != g {
			if s.cursor != nil {
				s.cursor.close()
			}
			p := r.parts[grp.part]
			index, err := p.keyIndex(ctx, grp.physical)
			if err != nil {
				return nil, err
			}
			s.cursor = p.keyCursor(ctx, grp.physical, index, s.stats, &s.seq)
			s.cursorGroup = g
			s.stats.GroupsOpened++
		}
		key, row, ok, err := s.cursor.seek(cursor, inclusive)
		if err != nil {
			return nil, err
		}
		if !ok {
			// Nothing at or after the cursor here: continue at the next
			// group's first key.
			if g+1 >= len(r.groups) {
				break
			}
			cursor, inclusive = r.groups[g+1].first, true
			continue
		}
		if s.to != nil && bytes.Compare(key, s.to) >= 0 {
			break
		}
		if prefix := CommonPrefix(key, req.Prefix, req.Delimiter); prefix != nil {
			s.emitPrefix(prefix)
			successor, ok := Successor(prefix)
			if !ok {
				break
			}
			cursor, inclusive = successor, true
			continue
		}
		key = bytes.Clone(key)
		values, i, err := s.valuesAt(ctx, g, row, req.Limit-len(s.out))
		if err != nil {
			return nil, err
		}
		s.out = append(s.out, Entry{Row: &Row{Key: key, Columns: s.columns, Values: rowValues(values, i)}})
		cursor, inclusive = key, false
	}
	return s.out, nil
}

// valuesAt returns the run of values holding row of group g and the row's
// position in it, reading up to want rows from row when the current run
// does not hold it: rows directly under a prefix are consecutive, so a run
// serves many of them with one read per column.
func (s *listScan) valuesAt(ctx context.Context, g int, row int64, want int) ([][]parquet.Value, int, error) {
	run := &s.values
	if run.values == nil || run.group != g || row < run.first || row >= run.first+int64(run.n) {
		grp := s.r.groups[g]
		n := int(min(int64(max(want, 1)), grp.rows-row))
		values, err := s.r.parts[grp.part].readValues(ctx, grp.physical, row, n, s.columns, s.stats)
		if err != nil {
			return nil, 0, err
		}
		*run = valueRun{group: g, first: row, values: values, n: n}
	}
	return run.values, int(row - run.first), nil
}
