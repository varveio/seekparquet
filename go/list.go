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
	// Cursor resumes after a row or common prefix; mutually exclusive with After.
	Cursor *Cursor
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
	if err := req.Cursor.check(); err != nil {
		return nil, err
	}
	if req.Cursor != nil && req.After != nil {
		return nil, invalidRequest("conflicting cursors")
	}
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
	if !r.key.Unique {
		out, err := r.listRepeated(rctx, req, stats)
		return out, finish(ctx, rctx, err)
	}
	if req.Cursor != nil {
		req.After, _ = req.Cursor.floor()
		req.Continuation = true
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
	log         pageLog
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
		g, err := r.groupFor(ctx, cursor, &s.log, s.stats)
		if err != nil {
			return nil, err
		}
		if g == len(r.groups) || s.to != nil && r.startsAtOrAfter(g, s.to) {
			break
		}
		// A row group whose first key and the next group's first key share
		// a common prefix holds only keys under it: keys between two keys
		// that begin with a prefix begin with it too. Where the scan lands
		// inside the group, the next group's first key is a key at or after
		// it under the prefix, so the prefix is emitted without a read.
		// When the prefix is not suppressed at the floor, that witness must
		// also lie below the upper bound. The shortcut is taken once the
		// group's first key is known (decoded by an earlier read, a routing
		// probe, or VerifyOnOpen).
		if first := r.groups[g].first.Load(); first != nil && g+1 < len(r.groups) {
			if whole := CommonPrefix(*first, req.Prefix, req.Delimiter); whole != nil {
				next, err := r.first(ctx, g+1, &s.log, s.stats)
				if err != nil {
					return nil, err
				}
				if bytes.Equal(whole, CommonPrefix(next, req.Prefix, req.Delimiter)) &&
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
		}
		if s.cursorGroup != g {
			if s.cursor != nil {
				s.cursor.close()
				s.cursor = nil
			}
			c, err := r.cursor(ctx, g, &s.log, s.stats)
			if err != nil {
				return nil, err
			}
			s.cursor, s.cursorGroup = c, g
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
			next, err := r.first(ctx, g+1, &s.log, s.stats)
			if err != nil {
				return nil, err
			}
			cursor, inclusive = next, true
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

// listRepeated walks rows in file order so equal keys remain distinct entries.
// Common prefixes still emit once and can skip directly to their successor.
func (r *Reader) listRepeated(ctx context.Context, req List, stats *Stats) ([]Entry, error) {
	from, exclusive := req.Prefix, false
	if req.After != nil && bytes.Compare(req.After, from) >= 0 {
		from, exclusive = req.After, true
	}
	if req.Cursor != nil {
		floor, excl := req.Cursor.floor()
		if bytes.Compare(floor, from) > 0 {
			from, exclusive = floor, excl
		}
	}
	end := req.End
	if upper, ok := Successor(req.Prefix); ok && (end == nil || bytes.Compare(upper, end) < 0) {
		end = upper
	}
	var log pageLog
	g, err := r.groupFor(ctx, from, &log, stats)
	if err != nil {
		return nil, err
	}
	var position groupPosition
	var out []Entry
	var lastPrefix []byte
	columns := slices.Clone(req.Columns)
	for ; g < len(r.groups) && len(out) < req.Limit; g++ {
		if end != nil && r.startsAtOrAfter(g, end) {
			break
		}
		c, err := r.cursor(ctx, g, &log, stats)
		if err != nil {
			return nil, err
		}
		// Closing within the group bounds retained page buffers to one cursor.
		err = func() error {
			defer c.close()
			key, row, ok, err := c.seek(from, !exclusive)
			for ok && err == nil && len(out) < req.Limit {
				if end != nil && bytes.Compare(key, end) >= 0 {
					return nil
				}
				offset := position.next(key)
				if bytes.HasPrefix(key, req.Prefix) && req.Cursor.allows(key, offset) {
					prefix := CommonPrefix(key, req.Prefix, req.Delimiter)
					if prefix != nil {
						suppressed := bytes.Equal(prefix, lastPrefix) || req.Continuation && req.After != nil && bytes.Compare(prefix, req.After) <= 0 || req.Cursor != nil && req.Cursor.Prefix != nil && bytes.Compare(prefix, req.Cursor.Prefix) <= 0
						if !suppressed {
							lastPrefix = bytes.Clone(prefix)
							out = append(out, Entry{CommonPrefix: lastPrefix})
						}
					} else {
						grp := r.groups[g]
						values, err := r.parts[grp.part].readValues(ctx, grp.physical, row, 1, columns, stats)
						if err != nil {
							return err
						}
						out = append(out, Entry{Row: &Row{Key: bytes.Clone(key), Offset: offset, Columns: columns, Values: rowValues(values, 0)}})
					}
				}
				key, row, ok, err = c.advance()
			}
			return err
		}()
		if err != nil {
			return nil, err
		}
		from, exclusive = nil, false
	}
	return out, nil
}
