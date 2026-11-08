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

// Range is a range request (spec §6.1): the rows whose keys are at or after
// Start (after it, when StartExclusive) and before End, in ascending
// unsigned byte order of their keys, at most Limit of them.
type Range struct {
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

// PrefixRange returns the range of the keys that begin with prefix: from
// prefix inclusive to Successor(prefix), or unbounded above when prefix
// has none.
func PrefixRange(prefix []byte, limit int, columns ...string) Range {
	rng := Range{Start: bytes.Clone(prefix), Limit: limit, Columns: columns}
	if end, ok := Successor(prefix); ok {
		rng.End = end
	}
	return rng
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
	if len(r.groups) == 0 || rng.End != nil && rng.Start != nil && bytes.Compare(rng.Start, rng.End) >= 0 {
		return nil
	}
	n := 0
	from, inclusive := rng.Start, !rng.StartExclusive
	var log pageLog
	for g := r.groupFor(from); g < len(r.groups) && n < rng.Limit; g++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		grp := r.groups[g]
		if rng.End != nil && bytes.Compare(grp.first, rng.End) >= 0 {
			break
		}
		keys, start, done, err := r.readKeys(ctx, g, from, inclusive, rng.End, rng.Limit-n, &log, stats)
		if err != nil {
			return err
		}
		if len(keys) > 0 {
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
				err := p.readColumnInto(ctx, grp.physical, column, start, len(keys), stats, func(values []parquet.Value) error { return sink.Values(c, values) })
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

// readKeys reads the keys of up to want rows of one row group at or after
// from and below end, and the row of the first; done reports that end was
// reached. The keys share one new allocation, each capped at its own end.
func (r *Reader) readKeys(ctx context.Context, g int, from []byte, inclusive bool, end []byte, want int, log *pageLog, stats *Stats) ([][]byte, int64, bool, error) {
	grp := r.groups[g]
	p := r.parts[grp.part]
	index, err := p.keyIndex(ctx, grp.physical)
	if err != nil {
		return nil, 0, false, err
	}
	cursor := p.keyCursor(ctx, g, grp.physical, index, stats, log)
	defer cursor.close()
	stats.GroupsOpened++
	key, row, ok, err := cursor.seek(from, inclusive)
	if err != nil {
		return nil, 0, false, err
	}
	start := row
	var arena []byte
	var ends []int
	done := false
	for ok && len(ends) < want {
		if end != nil && bytes.Compare(key, end) >= 0 {
			done = true
			break
		}
		arena = append(arena, key...)
		ends = append(ends, len(arena))
		if len(ends) == want {
			break
		}
		key, _, ok, err = cursor.advance()
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
	columns []string
	rows    []Row
	keys    [][]byte
	values  [][]parquet.Value
}

func (s *rowSink) Keys(keys [][]byte) error {
	s.flush()
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
		s.rows = append(s.rows, Row{Key: s.keys[i], Columns: s.columns, Values: rowValues(s.values, i)})
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
