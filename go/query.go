// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"slices"

	"github.com/parquet-go/parquet-go"
)

// Value is one projected value of a row. Values are parquet-go values
// (parquet.Value): this package commits to parquet-go's value types rather
// than inventing its own, so a value's Kind is its column's physical type
// in the row's part.
type Value struct {
	// Absent reports that the row's part has no such column (COL-2). An
	// absent value is also null (IsNull is true), so check Absent first
	// where the two must be told apart.
	Absent bool
	parquet.Value
}

// Row is one row of a result: its key, its offset in its equal-key group,
// and the values of the projected columns, in request order. A Row owns
// its key and value bytes. Columns is the result's own copy of the
// projected names, shared by the rows of one result: do not modify it.
type Row struct {
	Key []byte
	// Offset is the row's zero-based position in its group of equal keys,
	// in file order (PAGE-1); always zero when keys are unique.
	Offset  int64
	Columns []string
	Values  []Value
}

// Value returns the value of the named column, and false when the read did
// not project it.
func (row *Row) Value(name string) (Value, bool) {
	for i, column := range row.Columns {
		if column == name {
			return row.Values[i], true
		}
	}
	return Value{}, false
}

// Cursor continues a query after one of its results: exactly one of Key
// and Prefix is non-nil. A row cursor is the row's Key and its Offset in
// its group; a prefix cursor is a common prefix a listing returned, and
// its Offset is zero. A cursor applies to the query that produced it, on
// the same dataset (PAGE-1, PAGE-2): repeating the query with the cursor
// of each page's last entry returns every result exactly once. An Offset
// beyond the end of its group continues at the next key.
type Cursor struct {
	Key    []byte
	Offset int64
	Prefix []byte
}

// Cursor returns the cursor after this row, owning its key bytes.
func (row *Row) Cursor() *Cursor { return &Cursor{Key: bytes.Clone(row.Key), Offset: row.Offset} }

// check rejects a malformed cursor, and a prefix cursor where only a row
// cursor applies.
func (c *Cursor) check(prefixes bool) error {
	switch {
	case c == nil:
	case (c.Key == nil) == (c.Prefix == nil):
		return invalidRequest("a cursor has exactly one of Key and Prefix")
	case c.Offset < 0 || c.Prefix != nil && c.Offset != 0:
		return invalidRequest("cursor offset %d is invalid", c.Offset)
	case c.Prefix != nil && !prefixes:
		return invalidRequest("a range continues only after a row")
	}
	return nil
}

// Range is a range request (spec §6.2): the rows whose keys begin with
// Prefix, are at or after Start (after it, when StartExclusive) and are
// before End, in ascending unsigned byte order of their keys, equal keys
// in file order, at most Limit of them, after Cursor when it is set.
// Bounds select whole groups of equal keys; Limit may end a page inside
// one.
type Range struct {
	// Prefix, when not empty, confines the range to the keys that begin
	// with it: it is intersected with Start and End, so the read never
	// leaves the keys that begin with it.
	Prefix []byte
	// Start is the lower bound; nil is unbounded below.
	Start []byte
	// StartExclusive excludes a key equal to Start. The default is
	// inclusive.
	StartExclusive bool
	// End is the exclusive upper bound; nil is unbounded above. A non-nil
	// empty End selects nothing, since every key is non-empty.
	End []byte
	// Cursor, when set, continues the range after a row it returned
	// (Row.Cursor): a row cursor, never a prefix cursor.
	Cursor *Cursor
	// Limit is the most rows returned. It must be positive (QUERY-1).
	Limit int
	// Columns names the columns to project for each row, in order: their
	// values, null, or absent. No other column's pages are read. Empty
	// reads keys only.
	Columns []string
}

// PrefixRange returns a prefix read: the range of the keys that begin with
// prefix, with no other bounds.
func PrefixRange(prefix []byte, limit int, columns ...string) Range {
	return Range{Prefix: bytes.Clone(prefix), Limit: limit, Columns: columns}
}

// KeyRange returns a lookup of exactly key's group of equal keys: the
// range [key, key+0x00). Its limit may split the group; continue with the
// last row's Cursor as with any range.
func KeyRange(key []byte, limit int, columns ...string) Range {
	return Range{Start: bytes.Clone(key), End: append(bytes.Clone(key), 0), Limit: limit, Columns: columns}
}

// bounds returns the range's effective lower bound (inclusive unless
// exclusive) and upper bound: Start and End intersected with the keys
// that begin with Prefix.
func (rng *Range) bounds() (start []byte, exclusive bool, end []byte) {
	start, exclusive, end = rng.Start, rng.StartExclusive, rng.End
	if len(rng.Prefix) == 0 {
		return start, exclusive, end
	}
	if start == nil || bytes.Compare(start, rng.Prefix) < 0 {
		start, exclusive = rng.Prefix, false
	}
	if upper, ok := successor(rng.Prefix); ok && (end == nil || bytes.Compare(upper, end) < 0) {
		end = upper
	}
	return start, exclusive, end
}

// List is a delimited listing request (spec §6.3). The candidate keys are
// those that begin with Prefix and are below End when it is set, after
// Cursor when it is set. A candidate whose bytes after Prefix contain
// Delimiter yields the common prefix up to and including that delimiter;
// any other candidate yields its row. Common prefixes are distinct; rows
// of equal keys are each an entry, in file order. Entries are in
// non-decreasing byte order, at most Limit of them.
type List struct {
	// Prefix scopes the listing; empty lists every key.
	Prefix []byte
	// Delimiter is the single delimiter byte (LIST-1).
	Delimiter byte
	// End, when non-nil, excludes keys at or above it.
	End []byte
	// Cursor, when set, continues the listing after an entry it returned
	// (Entry.Cursor): after a row, the rows after it; after a common
	// prefix, the keys after every key that rolls up to it (LIST-6).
	Cursor *Cursor
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

// Bytes returns the entry's bytes, by which entries are ordered: the
// common prefix, or the row's key. To continue a listing, use Cursor.
func (e Entry) Bytes() []byte {
	if e.Row != nil {
		return e.Row.Key
	}
	return e.CommonPrefix
}

// Cursor returns the cursor after this entry, owning its bytes.
func (e Entry) Cursor() *Cursor {
	if e.Row != nil {
		return e.Row.Cursor()
	}
	return &Cursor{Prefix: bytes.Clone(e.CommonPrefix)}
}

// request checks what every request shares and takes a read lease. done
// releases it and maps an error of a request ended by Close to ErrClosed.
func (r *reader) request(ctx context.Context, cur *Cursor, prefixes bool, limit int, columns []string) (context.Context, func(*error), error) {
	if err := cur.check(prefixes); err != nil {
		return nil, nil, err
	}
	if limit <= 0 {
		return nil, nil, invalidRequest("limit %d is not positive", limit)
	}
	if err := r.checkProjection(columns); err != nil {
		return nil, nil, err
	}
	rctx, end, err := r.begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	return rctx, func(err *error) {
		*err = finish(ctx, rctx, *err)
		end()
	}, nil
}

// checkProjection rejects a request for a column that some part holds as
// a nested or repeated column (COL-3).
func (r *reader) checkProjection(columns []string) error {
	for _, name := range columns {
		for _, p := range r.parts {
			if p.unsupported[name] {
				return &UnsupportedError{Code: UnsupportedProjection,
					Detail: "column " + name + " is nested or repeated in part " + p.name}
			}
		}
	}
	return nil
}

// readRange returns the rows of rng; see Dataset.Range.
func (r *reader) readRange(ctx context.Context, rng Range, st *stats) (_ []Row, err error) {
	ctx, done, err := r.request(ctx, rng.Cursor, false, rng.Limit, rng.Columns)
	if err != nil {
		return nil, err
	}
	defer done(&err)
	if st == nil {
		st = new(stats)
	}
	from, exclusive, end := rng.bounds()
	if len(r.groups) == 0 || end != nil && from != nil && bytes.Compare(from, end) >= 0 {
		return nil, nil
	}
	s := r.newScan(ctx, end, st)
	if err := s.start(from, exclusive, rng.Cursor); err != nil {
		return nil, err
	}
	var rows []Row
	var at []position
	for s.key != nil {
		rows = append(rows, Row{Key: bytes.Clone(s.key), Offset: s.offset()})
		at = append(at, s.at())
		if len(rows) == rng.Limit {
			break
		}
		if err := s.next(); err != nil {
			return nil, err
		}
	}
	out := make([]*Row, len(rows))
	for i := range rows {
		out[i] = &rows[i]
	}
	return rows, r.project(ctx, out, at, slices.Clone(rng.Columns), st)
}

// readList returns the entries of req; see Dataset.List.
func (r *reader) readList(ctx context.Context, req List, st *stats) (_ []Entry, err error) {
	ctx, done, err := r.request(ctx, req.Cursor, true, req.Limit, req.Columns)
	if err != nil {
		return nil, err
	}
	defer done(&err)
	if st == nil {
		st = new(stats)
	}
	// The read is confined to the keys that begin with Prefix, below End,
	// and after a prefix cursor's keys.
	from, end := req.Prefix, req.End
	if len(from) == 0 {
		from = nil
	}
	if upper, ok := successor(req.Prefix); ok && (end == nil || bytes.Compare(upper, end) < 0) {
		end = upper
	}
	if c := req.Cursor; c != nil && c.Prefix != nil {
		after, ok := successor(c.Prefix)
		if !ok {
			return nil, nil
		}
		if from == nil || bytes.Compare(after, from) > 0 {
			from = after
		}
	}
	if len(r.groups) == 0 || end != nil && from != nil && bytes.Compare(from, end) >= 0 {
		return nil, nil
	}
	s := r.newScan(ctx, end, st)
	var out []Entry
	var rows []*Row
	var at []position
	if req.Cursor != nil && req.Cursor.Key != nil {
		err = s.start(from, false, req.Cursor)
	} else {
		err = s.seekList(from, req, &out)
	}
	if err != nil {
		return nil, err
	}
	for s.key != nil && len(out) < req.Limit {
		if prefix := commonPrefix(s.key, req.Prefix, req.Delimiter); prefix != nil {
			out = append(out, Entry{CommonPrefix: bytes.Clone(prefix)})
			next, ok := successor(prefix)
			if !ok || len(out) == req.Limit {
				break
			}
			if err := s.seekList(next, req, &out); err != nil {
				return nil, err
			}
			continue
		}
		row := &Row{Key: bytes.Clone(s.key), Offset: s.offset()}
		out = append(out, Entry{Row: row})
		rows, at = append(rows, row), append(at, s.at())
		if len(out) == req.Limit {
			break
		}
		if err := s.next(); err != nil {
			return nil, err
		}
	}
	return out, r.project(ctx, rows, at, slices.Clone(req.Columns), st)
}

// seekList seeks to from for a listing, first adding to out the common
// prefix of each row group the seek would land in that holds only keys
// rolling up to it (scan.shortcut), and moving past it.
func (s *scan) seekList(from []byte, req List, out *[]Entry) error {
	for len(*out) < req.Limit {
		whole, err := s.shortcut(from, req.Prefix, req.Delimiter)
		if err != nil {
			return err
		}
		if whole == nil {
			return s.seek(from, true)
		}
		*out = append(*out, Entry{CommonPrefix: whole})
		next, ok := successor(whole)
		if !ok {
			break
		}
		from = next
	}
	s.key = nil
	return nil
}

// project reads the projected values of rows, which a scan found at the
// given positions in dataset order: each run of consecutive rows of one
// row group is read with one pass over each projected column, decoding
// only the pages that hold the run.
func (r *reader) project(ctx context.Context, rows []*Row, at []position, columns []string, st *stats) error {
	for _, row := range rows {
		row.Columns, row.Values = columns, make([]Value, len(columns))
	}
	for i := 0; i < len(rows) && len(columns) > 0; {
		j := i + 1
		for j < len(rows) && at[j].g == at[i].g && at[j].row == at[j-1].row+1 {
			j++
		}
		grp := r.groups[at[i].g]
		p := r.parts[grp.part]
		for c, name := range columns {
			col, ok := p.leaves[name]
			if !ok {
				for _, row := range rows[i:j] {
					row.Values[c] = Value{Absent: true}
				}
				continue
			}
			k := i
			err := p.readColumn(ctx, grp.physical, col, at[i].row, j-i, st, func(values []parquet.Value) {
				for _, v := range values {
					rows[k].Values[c] = Value{Value: v.Clone()}
					k++
				}
			})
			if err != nil {
				return err
			}
		}
		i = j
	}
	return nil
}
