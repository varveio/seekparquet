// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"math"
	"slices"

	"github.com/parquet-go/parquet-go"
)

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
// beyond the end of its group continues at the next key, so math.MaxInt64
// means after the whole group; a nonzero Offset on a dataset of unique keys
// means after the key. Encode and ParseCursor carry a cursor as text.
type Cursor struct {
	Key    []byte
	Offset int64
	Prefix []byte
}

// RowCursor returns the row cursor after the row of key at offset in its
// group, owning a copy of key; a nil key is the empty key.
func RowCursor(key []byte, offset int64) *Cursor {
	return &Cursor{Key: append([]byte{}, key...), Offset: offset}
}

// PrefixCursor returns the prefix cursor after a common prefix, owning a
// copy of prefix; a nil prefix is the empty prefix.
func PrefixCursor(prefix []byte) *Cursor { return &Cursor{Prefix: append([]byte{}, prefix...)} }

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

// Span bounds a range (spec §6.2): the keys that begin with Prefix, are at
// or after Start (after it, when StartExclusive) and are before End, in
// ascending unsigned byte order. Bounds select whole groups of equal keys.
// The zero Span is every key.
type Span struct {
	// Prefix, when not empty, confines the span to the keys that begin
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
}

// KeySpan returns the span of exactly key's group of equal keys: [key,
// key+0x00), owning copies of key. A limit may split the group; continue
// with the last row's Cursor as with any span. Dataset.Get reads one whole
// group.
func KeySpan(key []byte) Span {
	return Span{Start: bytes.Clone(key), End: append(bytes.Clone(key), 0)}
}

// PrefixSpan returns the span of the keys that begin with prefix, with no
// other bounds, owning a copy of prefix.
func PrefixSpan(prefix []byte) Span { return Span{Prefix: bytes.Clone(prefix)} }

// Selection is a span to read, from its start or, when Cursor is set, after
// a row a read of the same selection returned (Row.Cursor): a row cursor,
// never a prefix cursor.
type Selection struct {
	Span
	Cursor *Cursor
}

// Query is a range request (spec §6.2): the rows of a Selection, in
// ascending key order, equal keys in file order, at most Limit of them, with
// the values of Columns.
type Query struct {
	Selection
	// Limit is the most rows returned. Dataset.Rows requires it positive
	// (QUERY-1). Dataset.Scan also takes zero, for every row to the end of
	// the span: a streaming extension of this library, not a query of the
	// specification. A negative limit is an invalid request.
	Limit int64
	// Columns names the columns to project for each row, in order: their
	// values, null, or absent. No other column's pages are read. Empty
	// reads keys only.
	Columns []string
}

// bounds returns the span's effective lower bound (inclusive unless
// exclusive) and upper bound: Start and End intersected with the keys
// that begin with Prefix.
func (sp *Span) bounds() (start []byte, exclusive bool, end []byte) {
	start, exclusive, end = sp.Start, sp.StartExclusive, sp.End
	if len(sp.Prefix) == 0 {
		return start, exclusive, end
	}
	if start == nil || bytes.Compare(start, sp.Prefix) < 0 {
		start, exclusive = sp.Prefix, false
	}
	if upper, ok := successor(sp.Prefix); ok && (end == nil || bytes.Compare(upper, end) < 0) {
		end = upper
	}
	return start, exclusive, end
}

// ListQuery is a delimited listing request (spec §6.3). The candidate keys are
// those that begin with Prefix and are below End when it is set, after
// Cursor when it is set. A candidate whose bytes after Prefix contain
// Delimiter yields the common prefix up to and including that delimiter;
// any other candidate yields its row. Common prefixes are distinct; rows
// of equal keys are each an entry, in file order. Entries are in
// non-decreasing byte order, at most Limit of them.
type ListQuery struct {
	// Prefix scopes the listing; empty lists every key.
	Prefix []byte
	// Delimiter is the single delimiter byte (LIST-1).
	Delimiter byte
	// End, when non-nil, excludes keys at or above it.
	End []byte
	// Cursor, when set, continues the listing after an entry it returned
	// (ListEntry.Cursor): after a row, the rows after it; after a common
	// prefix, the keys after every key that rolls up to it (LIST-6).
	Cursor *Cursor
	// Limit is the most entries returned. It must be positive (QUERY-1).
	Limit int
	// Columns names the columns projected for each row entry and each
	// exact row (ListEntry.Exact), as Query.Columns.
	Columns []string
	// ReadAhead caps the future key pages considered by each listing
	// prediction. Zero selects adaptive read-ahead toward Limit+1 over a
	// remote Source; zero-policy OpenDir reads on demand. Positive values
	// cap the same predictor; negative values are invalid. Adjacent pages
	// are fetched together in bounded windows and checked on use.
	// ReadPolicy.DisableSpeculation disables this optional fetching.
	ReadAhead int
}

// ListEntry is one listing entry: exactly one of CommonPrefix and Row is set.
type ListEntry struct {
	CommonPrefix []byte
	Row          *Row
	// Exact is, for a common prefix, its exact row (LIST-8): the first
	// candidate row of this request that rolls up to the prefix, when that
	// row's key is the prefix itself, with its projected values; nil
	// otherwise, and always nil for a row entry. Without a cursor, or after
	// a prefix cursor, it is the first row stored at the prefix's exact key
	// whenever one is a candidate, so a listing pairs a prefix with the row
	// at its exact key (an S3-style "a/" folder object) without another
	// request. Its values cost the value pages a row entry's would.
	Exact *Row
}

// IsPrefix reports whether the entry is a common prefix.
func (e ListEntry) IsPrefix() bool { return e.Row == nil }

// Bytes returns the entry's bytes, by which entries are ordered: the
// common prefix, or the row's key. To continue a listing, use Cursor.
func (e ListEntry) Bytes() []byte {
	if e.Row != nil {
		return e.Row.Key
	}
	return e.CommonPrefix
}

// Cursor returns the cursor after this entry, owning its bytes.
func (e ListEntry) Cursor() *Cursor {
	if e.Row != nil {
		return e.Row.Cursor()
	}
	return &Cursor{Prefix: bytes.Clone(e.CommonPrefix)}
}

// request checks what every request shares and begins its operation
// (begin), which the caller ends with done. A limit of zero is accepted
// only when unlimited is set (Dataset.Scan).
func (r *reader) request(ctx context.Context, cur *Cursor, prefixes bool, limit int64, unlimited bool, columns []string) (*operation, error) {
	if err := cur.check(prefixes); err != nil {
		return nil, err
	}
	if limit < 0 || limit == 0 && !unlimited {
		return nil, invalidRequest("limit %d is not positive", limit)
	}
	if err := r.checkProjection(columns); err != nil {
		return nil, err
	}
	return r.begin(ctx)
}

// checkProjection rejects a request for a column that a checked part
// holds as a nested or repeated column (COL-3). A part checked later is
// held to the same rule when a projection reaches it (project).
func (r *reader) checkProjection(columns []string) error {
	for _, p := range r.parts {
		if p.ready.Load() {
			if err := p.checkProjection(columns); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkProjection rejects projecting a column the part holds as a nested
// or repeated column (COL-3).
func (p *part) checkProjection(columns []string) error {
	for _, name := range columns {
		if p.unsupported[name] {
			return &UnsupportedError{Code: UnsupportedProjection,
				Detail: "column " + name + " is nested or repeated in part " + p.name}
		}
	}
	return nil
}

// readRange returns the rows of q; see Dataset.Rows. It discovers every
// selected key before projecting any value, and returns no rows on error.
func (r *reader) readRange(ctx context.Context, q Query, st *ReadStats) (_ []Row, err error) {
	st, spare := takeStats(st)
	defer dropStats(spare)
	if q.Limit > int64(math.MaxInt) {
		return nil, invalidRequest("limit %d is beyond a slice's length", q.Limit)
	}
	op, err := r.request(ctx, q.Cursor, false, q.Limit, false, q.Columns)
	if err != nil {
		return nil, err
	}
	defer op.done(ctx, &err, st)
	ctx = op
	rows, _, err := r.collect(ctx, q.Selection, int(q.Limit), false, q.Columns, st)
	return rows, err
}

// readGet returns the group of key; see Dataset.Get.
func (r *reader) readGet(ctx context.Context, key []byte, st *ReadStats, columns []string) (_ []Row, err error) {
	st, spare := takeStats(st)
	defer dropStats(spare)
	op, err := r.request(ctx, nil, false, 1, false, columns)
	if err != nil {
		return nil, err
	}
	defer op.done(ctx, &err, st)
	ctx = op
	limit := r.limits.GetRows
	rows, more, err := r.collect(ctx, Selection{Span: KeySpan(key)}, limit, true, columns, st)
	if more {
		return nil, exceeded(LimitGetRows, -1, int64(limit),
			"the group of key %.64q has more rows than Limits.GetRows: read it in pages of Rows over KeySpan(key), continuing after each page's last row", key)
	}
	return rows, err
}

// collect returns the first rows of sel, at most limit of them, with the
// values of columns, or no rows on error. It finds every row's key before
// it projects any value. With probe, once it has limit rows it decodes the
// next key and, when the selection has another row, returns none and more
// set, having read no value page; without, it stops on the last row it
// returns, as Rows does.
func (r *reader) collect(ctx context.Context, sel Selection, limit int, probe bool, columns []string, st *ReadStats) (_ []Row, more bool, err error) {
	from, exclusive, end := sel.bounds()
	if r.rows == 0 || end != nil && from != nil && bytes.Compare(from, end) >= 0 {
		return nil, false, nil
	}
	s := r.newScan(ctx, end, st)
	if err := s.start(from, exclusive, sel.Cursor); err != nil {
		return nil, false, err
	}
	var rows []Row
	var at []position
	for s.key != nil {
		if len(rows) == limit {
			return nil, true, nil // probe found one more
		}
		rows = append(rows, Row{Key: bytes.Clone(s.key), Offset: s.offset()})
		at = append(at, s.at())
		if len(rows) == limit && !probe {
			break
		}
		if err := s.next(); err != nil {
			return nil, false, err
		}
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	out := make([]*Row, len(rows))
	for i := range rows {
		out[i] = &rows[i]
	}
	if err := r.project(ctx, out, at, slices.Clone(columns), st); err != nil {
		return nil, false, err
	}
	return rows, false, nil
}

// readList returns the entries of req; see Dataset.List.
func (r *reader) readList(ctx context.Context, req ListQuery, st *ReadStats) (_ []ListEntry, err error) {
	st, spare := takeStats(st)
	defer dropStats(spare)
	if req.ReadAhead < 0 {
		return nil, invalidRequest("read-ahead %d is negative", req.ReadAhead)
	}
	op, err := r.request(ctx, req.Cursor, true, int64(req.Limit), false, req.Columns)
	if err != nil {
		return nil, err
	}
	defer op.done(ctx, &err, st)
	ctx = op
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
	if r.rows == 0 || end != nil && from != nil && bytes.Compare(from, end) >= 0 {
		return nil, nil
	}
	s := r.newScan(ctx, end, st)
	ahead := req.ReadAhead
	if ahead == 0 {
		ahead = testListReadAhead
	}
	s.ahead = r.newReadAhead(ctx, ahead, req.Limit)
	var l listing
	if req.Cursor != nil && req.Cursor.Key != nil {
		err = s.start(from, false, req.Cursor)
	} else {
		handled := false
		if req.Cursor != nil && req.Cursor.Prefix != nil && req.Delimiter == '/' {
			handled, err = s.startListCursor(req.Cursor.Prefix, from)
		}
		if err == nil && !handled {
			err = s.seekList(from, req, &l)
		}
	}
	if err != nil {
		return nil, err
	}
	for s.key != nil && len(l.out) < req.Limit {
		if prefix := commonPrefix(s.key, req.Prefix, req.Delimiter); prefix != nil {
			l.prefix(bytes.Clone(prefix), bytes.Equal(s.key, prefix), s.offset(), s.at())
			next, ok := successor(prefix)
			if !ok || len(l.out) == req.Limit {
				break
			}
			s.readAhead(&req, req.Limit-len(l.out))
			st.PrefixSkips++
			if handled, err := s.skipList(prefix, req.Delimiter); err != nil {
				return nil, err
			} else if handled {
				continue
			}
			if err := s.seekList(next, req, &l); err != nil {
				return nil, err
			}
			continue
		}
		l.out = append(l.out, ListEntry{Row: l.row(s.key, s.offset(), s.at())})
		if len(l.out) == req.Limit {
			break
		}
		s.readAhead(&req, req.Limit-len(l.out))
		if err := s.next(); err != nil {
			return nil, err
		}
	}
	// The key scan is over: the pages read ahead are not needed any more.
	s.ahead.close()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return l.out, r.project(ctx, l.rows, l.at, slices.Clone(req.Columns), st)
}

// listing collects a listing's entries, and the rows to project with the
// positions a scan found them at, in dataset order.
type listing struct {
	out  []ListEntry
	rows []*Row
	at   []position
}

// row adds a row to project and returns it.
func (l *listing) row(key []byte, offset int64, at position) *Row {
	row := &Row{Key: bytes.Clone(key), Offset: offset}
	l.rows, l.at = append(l.rows, row), append(l.at, at)
	return row
}

// prefix adds the common prefix entry of a first candidate row at at, with
// that row as its exact row when exact (its key is the prefix).
func (l *listing) prefix(prefix []byte, exact bool, offset int64, at position) {
	e := ListEntry{CommonPrefix: prefix}
	if exact {
		e.Exact = l.row(prefix, offset, at)
	}
	l.out = append(l.out, e)
}

// seekList seeks to from for a listing, first adding to l the common
// prefix of each row group the seek would land in that holds only keys
// rolling up to it (scan.shortcut), and moving past it.
func (s *scan) seekList(from []byte, req ListQuery, l *listing) error {
	for len(l.out) < req.Limit {
		whole, g, err := s.shortcut(from, req.Prefix, req.Delimiter)
		if err != nil {
			return err
		}
		if whole == nil {
			return s.seek(from, true)
		}
		// The group's first key is its first candidate when it is at or
		// after from: no earlier group holds a key at or after from
		// (groupFor), and the row's offset is zero, since only a part's
		// first group, or a group of unique keys, can begin at or after
		// from here. A first key equal to whole is then its exact row.
		first := *s.r.group(g).first.Load()
		l.prefix(whole, bytes.Equal(first, whole) && bytes.Compare(first, from) >= 0, 0, position{g, 0})
		next, ok := successor(whole)
		if !ok {
			break
		}
		if len(l.out) == req.Limit {
			break // the page is full: no resume
		}
		s.stats.PrefixSkips++
		from = next
	}
	s.key = nil
	return nil
}

// project reads the projected values of rows, which a scan found at the
// given positions in dataset order: each row group is read with one pass
// over each projected column, decoding only pages holding selected rows.
// Sparse selections reuse the current page without retaining prior pages.
func (r *reader) project(ctx context.Context, rows []*Row, at []position, columns []string, st *ReadStats) error {
	scratch := make([]pageScratch, len(columns))
	for _, row := range rows {
		row.Columns, row.Values = columns, make([]Value, len(columns))
	}
	for i := 0; i < len(rows) && len(columns) > 0; {
		j := i + 1
		for j < len(rows) && at[j].g == at[i].g {
			j++
		}
		p := r.parts[at[i].g.part]
		if err := p.checkProjection(columns); err != nil {
			return err
		}
		for c, name := range columns {
			cp, err := p.openColumn(ctx, p.groups[at[i].g.index].physical, name, st, &scratch[c])
			if err != nil {
				return err
			}
			if err := projectColumn(rows[i:j], at[i:j], c, cp); err != nil {
				return err
			}
		}
		i = j
	}
	return nil
}

// projectColumn sets value c of rows, found at positions at in one row
// group, from the column's pages, or absent when cp is nil.
func projectColumn(rows []*Row, at []position, c int, cp *columnPages) error {
	if cp == nil {
		for _, row := range rows {
			row.Values[c] = Value{absent: true}
		}
		return nil
	}
	return cp.read(at, func(k int, v parquet.Value) {
		rows[k].Values[c] = valueOf(&v, false).Clone()
	})
}
