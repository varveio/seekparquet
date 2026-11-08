// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
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

// Row is one row of a result: its key and the values of the projected
// columns, in request order. A Row owns its key and value bytes. Columns
// is the result's own copy of the projected names, shared by the rows of
// one result: do not modify it.
type Row struct {
	// Offset is the zero-based position within this key's group in file order.
	Offset  int64
	Key     []byte
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

// Range is a range request (spec §6.2): the rows whose keys begin with
// Prefix, are at or after Start (after it, when StartExclusive) and are
// before End, in ascending unsigned byte order of their keys, at most Limit
// of them.
type Range struct {
	// Cursor resumes after a row returned by this query.
	Cursor *Cursor
	// Prefix, when not empty, confines the range to the keys that begin
	// with it: it is intersected with Start and End, so the read never
	// leaves [Prefix, Successor(Prefix)).
	Prefix []byte
	// Start is the lower bound; nil is unbounded below.
	Start []byte
	// StartExclusive excludes a key equal to Start. The default is
	// inclusive.
	StartExclusive bool
	// End is the exclusive upper bound; nil is unbounded above. A non-nil
	// empty End selects nothing, since every key is non-empty.
	End []byte
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

// bounds returns the range's effective lower bound (inclusive unless
// exclusive) and upper bound: Start and End intersected with the keys
// that begin with Prefix.
func (rng Range) bounds() (start []byte, exclusive bool, end []byte) {
	start, exclusive, end = rng.Start, rng.StartExclusive, rng.End
	if rng.Cursor != nil {
		floor, excl := rng.Cursor.floor()
		if start == nil || bytes.Compare(floor, start) > 0 {
			start, exclusive = floor, excl
		}
	}
	if len(rng.Prefix) == 0 {
		return start, exclusive, end
	}
	if start == nil || bytes.Compare(start, rng.Prefix) < 0 {
		start, exclusive = rng.Prefix, false
	}
	if upper, ok := Successor(rng.Prefix); ok && (end == nil || bytes.Compare(upper, end) < 0) {
		end = upper
	}
	return start, exclusive, end
}

// Range returns the rows of rng. It decodes key pages only from the page
// that can hold Start, and value pages only for the rows it returns. A
// limit that is not positive is ErrInvalidRequest; projecting a nested or
// repeated column is an *UnsupportedError; a key page that breaks the
// specification ends the read with a *RefusalError.
func (r *Reader) Range(ctx context.Context, rng Range, stats *Stats) ([]Row, error) {
	sink := &rowSink{columns: slices.Clone(rng.Columns)}
	if err := r.Scan(ctx, rng, stats, sink); err != nil {
		return nil, err
	}
	sink.flush()
	return sink.rows, nil
}

// ColumnSink receives the rows of a Scan column by column, one run of
// consecutive rows of a row group at a time: Keys starts a run, then each
// column of Range.Columns, in order, is given by one or more Values calls
// or by one Absent call.
//
// Calls are synchronous, on the goroutine that called Scan. A callback
// that returns an error ends the scan, and Scan returns that error as is;
// a run may then be incomplete. A sink must not call the Reader's Close
// (the scan holds the read lease Close waits for, so it would deadlock).
type ColumnSink interface {
	// Keys starts a run with its rows' keys, which the sink may keep:
	// they are newly allocated for each run.
	Keys(keys [][]byte) error
	// Values gives the next values of column c (an index into
	// Range.Columns) for the run's rows, in row order. The values are
	// borrowed: they alias page buffers and are valid only until the call
	// returns, so a sink copies (Value.Clone) what it keeps.
	Values(c int, values []parquet.Value) error
	// Absent reports that the run's part has no column c (COL-2).
	Absent(c int) error
}

// Scan reads the rows of rng as Range does and hands them to sink, without
// cloning a value or building a row.
func (r *Reader) Scan(ctx context.Context, rng Range, stats *Stats, sink ColumnSink) error {
	if err := rng.Cursor.check(); err != nil {
		return err
	}
	if rng.Limit <= 0 {
		return invalidRequest("limit %d is not positive", rng.Limit)
	}
	if err := r.checkProjection(rng.Columns); err != nil {
		return err
	}
	rctx, end, err := r.begin(ctx)
	if err != nil {
		return err
	}
	defer end()
	err = r.scan(rctx, rng, stats, sink)
	var sinkErr errSink
	if errors.As(err, &sinkErr) {
		return sinkErr.err
	}
	return finish(ctx, rctx, err)
}

// checkProjection rejects a request for a column that some part holds as
// a nested or repeated column (COL-3).
func (r *Reader) checkProjection(columns []string) error {
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

func (r *Reader) scan(ctx context.Context, rng Range, stats *Stats, sink ColumnSink) error {
	if stats == nil {
		stats = new(Stats)
	}
	start, exclusive, end := rng.bounds()
	if len(r.groups) == 0 || end != nil && start != nil && bytes.Compare(start, end) >= 0 {
		return nil
	}
	n := 0
	var position groupPosition
	from, inclusive := start, !exclusive
	var log pageLog
	g, err := r.groupFor(ctx, from, &log, stats)
	if err != nil {
		return err
	}
	for ; g < len(r.groups) && n < rng.Limit; g++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		grp := r.groups[g]
		if end != nil && r.startsAtOrAfter(g, end) {
			break
		}
		keys, first, done, err := r.readKeys(ctx, g, from, inclusive, end, rng.Limit-n, &log, stats, &position, rng.Cursor)
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if s, ok := sink.(*rowSink); ok {
				s.nextOffsets = position.offsets
			}
			if err := sink.Keys(keys); err != nil {
				return errSink{err}
			}
			p := r.parts[grp.part]
			for c, name := range rng.Columns {
				column, ok := p.leaves[name]
				if !ok {
					if err := sink.Absent(c); err != nil {
						return errSink{err}
					}
					continue
				}
				err := p.readColumnInto(ctx, grp.physical, column, first, len(keys), stats, func(values []parquet.Value) error { return sink.Values(c, values) })
				if err != nil {
					return err
				}
			}
			n += len(keys)
		}
		if done {
			break
		}
		// Later groups are read from their first row.
		from, inclusive = nil, true
	}
	return nil
}

// startsAtOrAfter reports whether row group g is known, without reading
// it, to begin at or after key: its first key, or its part's minimum key
// when it is the part's first group, is at or after key.
func (r *Reader) startsAtOrAfter(g int, key []byte) bool {
	if first := r.groups[g].first.Load(); first != nil {
		return bytes.Compare(*first, key) >= 0
	}
	p := r.parts[r.groups[g].part]
	return g == p.firstGroup && p.minKey != nil && bytes.Compare(p.minKey, key) >= 0
}

// readKeys reads the keys of up to want rows of one row group at or after
// from and below end, and the row of the first; done reports that end was
// reached. The keys share one new allocation, each capped at its own end.
func (r *Reader) readKeys(ctx context.Context, g int, from []byte, inclusive bool, end []byte, want int, log *pageLog, stats *Stats, position *groupPosition, after *Cursor) ([][]byte, int64, bool, error) {
	cursor, err := r.cursor(ctx, g, log, stats)
	if err != nil {
		return nil, 0, false, err
	}
	defer cursor.close()
	key, row, ok, err := cursor.seek(from, inclusive)
	if err != nil {
		return nil, 0, false, err
	}
	start := row
	position.offsets = nil
	var arena []byte
	var ends []int
	done := false
	for ok && len(ends) < want {
		if end != nil && bytes.Compare(key, end) >= 0 {
			done = true
			break
		}
		offset := int64(0)
		if !r.key.Unique {
			offset = position.next(key)
		}
		if !after.allows(key, offset) {
			key, row, ok, err = cursor.advance()
			if err != nil {
				return nil, 0, false, err
			}
			continue
		}
		if len(ends) == 0 {
			start = row
		}
		position.offsets = append(position.offsets, offset)
		arena = append(arena, key...)
		ends = append(ends, len(arena))
		if len(ends) == want {
			break
		}
		key, row, ok, err = cursor.advance()
		if err != nil {
			return nil, 0, false, err
		}
	}
	keys := make([][]byte, len(ends))
	for i, e := range ends {
		begin := 0
		if i > 0 {
			begin = ends[i-1]
		}
		keys[i] = arena[begin:e:e]
	}
	return keys, start, done, nil
}

// rowSink builds Range's rows, cloning each value out of its page.
type rowSink struct {
	offsets     []int64
	nextOffsets []int64
	columns     []string
	rows        []Row
	keys        [][]byte
	values      [][]parquet.Value
}

func (s *rowSink) Keys(keys [][]byte) error {
	s.flush()
	s.offsets = append(s.offsets[:0], s.nextOffsets...)
	s.keys, s.values = keys, make([][]parquet.Value, len(s.columns))
	return nil
}

func (s *rowSink) Values(c int, values []parquet.Value) error {
	if s.values[c] == nil {
		s.values[c] = make([]parquet.Value, 0, len(s.keys))
	}
	for _, v := range values {
		s.values[c] = append(s.values[c], v.Clone())
	}
	return nil
}

func (s *rowSink) Absent(int) error { return nil }

// flush adds the run's rows.
func (s *rowSink) flush() {
	for i := range s.keys {
		s.rows = append(s.rows, Row{Key: s.keys[i], Offset: s.offsets[i], Columns: s.columns, Values: rowValues(s.values, i)})
	}
	s.keys, s.values = nil, nil
}

// rowValues returns row i of column-major values, absent where a column
// was not read.
func rowValues(values [][]parquet.Value, i int) []Value {
	row := make([]Value, len(values))
	for c := range row {
		if values[c] == nil {
			row[c] = Value{Absent: true}
			continue
		}
		row[c] = Value{Value: values[c][i]}
	}
	return row
}
