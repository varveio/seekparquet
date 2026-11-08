// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/seekparquettest"
)

// typed has a column of every baseline physical type, nullable, and a
// nested and a repeated column.
type typed struct {
	Key    []byte   `parquet:"key"`
	Flag   *bool    `parquet:"flag,optional"`
	I32    *int32   `parquet:"i32,optional"`
	I64    *int64   `parquet:"i64,optional"`
	F32    *float32 `parquet:"f32,optional"`
	F64    *float64 `parquet:"f64,optional"`
	Blob   []byte   `parquet:"blob,optional"`
	Fixed  [4]byte  `parquet:"fixed"`
	Dotted int64    `parquet:"a.b"`
	Tags   []string `parquet:"tags"`
	Meta   struct {
		A int32 `parquet:"a"`
	} `parquet:"meta"`
}

// older lacks most columns: its rows report them absent.
type older struct {
	Key  []byte `parquet:"key"`
	Flag *bool  `parquet:"flag,optional"`
}

func typedDataset() seekparquettest.Dataset {
	var rows []typed
	for i := range 6 {
		r := typed{Key: []byte(fmt.Sprintf("t%d", i)), Fixed: [4]byte{byte(i), 0, 0xff, 0x7f}, Dotted: int64(i)}
		if i%2 == 0 {
			r.Flag, r.I32, r.I64 = ptr(i == 0), ptr(int32(-i)), ptr(int64(math.MaxInt64-i))
			r.F32, r.F64, r.Blob = ptr(float32(math.Copysign(0, -1))), ptr(math.Inf(1)), []byte{byte(i)}
		}
		rows = append(rows, r)
	}
	return seekparquettest.Dataset{Parts: []seekparquettest.Part{
		{Rows: []older{{Key: []byte("a"), Flag: ptr(true)}, {Key: []byte("b")}}},
		{Rows: rows, GroupRows: 4},
	}}
}

// TestProjection projects every baseline type, nulls, absent columns and a
// column whose name holds a dot (COL-6), through Range and List.
func TestProjection(t *testing.T) {
	d := open(t, write(t, typedDataset()), seekparquet.Options{})
	columns := []string{"flag", "i32", "i64", "f32", "f64", "blob", "fixed", "a.b", "nope"}
	rows, err := d.Range(ctx, seekparquet.Range{Limit: 100, Columns: columns}, nil)
	must(t, err)
	if len(rows) != 8 {
		t.Fatalf("%d rows", len(rows))
	}
	// Part 0 has only flag: the rest is absent, which is not null.
	a, b := rows[0], rows[1]
	if a.Values[0].Absent || !a.Values[0].Boolean() || b.Values[0].Absent || !b.Values[0].IsNull() {
		t.Fatalf("flag: %+v, %+v", a.Values[0], b.Values[0])
	}
	for c := 1; c < len(columns); c++ {
		if !a.Values[c].Absent || !a.Values[c].IsNull() {
			t.Fatalf("column %s of part 0 is not absent: %+v", columns[c], a.Values[c])
		}
	}
	t0, t1 := rows[2], rows[3]
	if t0.Values[1].Int32() != 0 || t0.Values[2].Int64() != math.MaxInt64 || math.Float32bits(t0.Values[3].Float()) != 0x80000000 ||
		!math.IsInf(t0.Values[4].Double(), 1) || string(t0.Values[5].ByteArray()) != "\x00" || string(t0.Values[6].ByteArray()) != "\x00\x00\xff\x7f" ||
		t0.Values[7].Int64() != 0 {
		t.Fatalf("t0 values %v", t0.Values)
	}
	for c := 0; c < 6; c++ {
		if t1.Values[c].Absent || !t1.Values[c].IsNull() {
			t.Fatalf("t1 column %s: %+v, want null", columns[c], t1.Values[c])
		}
	}
	if !t1.Values[8].Absent {
		t.Fatal("a column no part has is not absent")
	}
	if v, ok := t1.Value("a.b"); !ok || v.Int64() != 1 {
		t.Fatalf("Value(a.b) = %v, %v", v, ok)
	}
	if _, ok := t1.Value("size"); ok {
		t.Fatal("Value of an unprojected column")
	}
	entries, err := d.List(ctx, seekparquet.List{Delimiter: '/', Limit: 100, Columns: columns}, nil)
	must(t, err)
	for i, e := range entries {
		if !slices.EqualFunc(e.Row.Values, rows[i].Values, sameValue) {
			t.Fatalf("list row %d: %v, range %v", i, e.Row.Values, rows[i].Values)
		}
	}
	// Projecting a nested or repeated column is rejected (COL-3).
	for _, c := range []string{"tags", "meta"} {
		_, err := d.Range(ctx, seekparquet.Range{Limit: 1, Columns: []string{c}}, nil)
		var u *seekparquet.UnsupportedError
		if !errors.As(err, &u) || u.Code != seekparquet.UnsupportedProjection {
			t.Fatalf("%s: %v", c, err)
		}
		if _, err := d.List(ctx, seekparquet.List{Delimiter: '/', Limit: 1, Columns: []string{c}}, nil); !errors.As(err, &u) {
			t.Fatalf("list %s: %v", c, err)
		}
	}
	if got := d.Reader().Columns(); !slices.Equal(got, []string{"flag", "i32", "i64", "f32", "f64", "blob", "fixed", "a.b"}) {
		t.Fatalf("columns %q", got)
	}
}

func sameValue(a, b seekparquet.Value) bool {
	return a.Absent == b.Absent && a.IsNull() == b.IsNull() && parquet.Equal(a.Value, b.Value)
}

// TestRowsOwnTheirBytes changes everything a read was given and returned:
// nothing else changes.
func TestRowsOwnTheirBytes(t *testing.T) {
	d := open(t, write(t, parts(seekparquettest.Part{}, []string{"a", "b", "c"})), seekparquet.Options{})
	columns := []string{"size", "name"}
	rows, err := d.Range(ctx, seekparquet.Range{Limit: 3, Columns: columns}, nil)
	must(t, err)
	columns[0] = "changed"
	if rows[0].Columns[0] != "size" {
		t.Fatal("Row.Columns aliases the request")
	}
	rows[0].Key[0] = 'z'
	copy(rows[0].Values[1].ByteArray(), "XXXX")
	again, _ := d.Range(ctx, seekparquet.Range{Limit: 3, Columns: []string{"size", "name"}}, nil)
	if string(again[0].Key) != "a" || string(again[0].Values[1].ByteArray()) != "name-0" || string(rows[1].Key) != "b" {
		t.Fatalf("a result's bytes are shared: %q %q", again[0].Key, again[0].Values[1].ByteArray())
	}
}

// TestExpectations: a consumer's column expectations fail an open with an
// *ExpectationError, which is not a refusal.
func TestExpectations(t *testing.T) {
	dir := write(t, typedDataset())
	cases := map[string]seekparquet.ColumnExpectation{
		"missing in part 0": {Name: "i64", MustExist: true},
		"wrong kind":        {Name: "i64", Kinds: []parquet.Kind{parquet.Int32}},
		"nested":            {Name: "meta"},
		"check":             {Name: "flag", Check: func(parquet.Node) error { return errors.New("no") }},
		"never present":     {Name: "nope", MustExist: true},
	}
	for name, want := range cases {
		_, err := openErr(dir, seekparquet.Options{Expect: []seekparquet.ColumnExpectation{want}})
		var e *seekparquet.ExpectationError
		if !errors.As(err, &e) || e.Column != want.Name || seekparquet.CodeOf(err) != "" {
			t.Fatalf("%s: %v", name, err)
		}
	}
	open(t, dir, seekparquet.Options{Expect: []seekparquet.ColumnExpectation{
		{Name: "flag", MustExist: true, Kinds: []parquet.Kind{parquet.Boolean}},
		{Name: "i64", Kinds: []parquet.Kind{parquet.Int64}},
		{Name: "nope"},
	}})
}

// sink rebuilds rows from what Scan hands it, checking the sink contract:
// Keys, then every column in order, each given in full by Values calls or
// by one Absent call.
type sink struct {
	t       *testing.T
	columns []string
	rows    []seekparquet.Row
	run     int
	next    int
	filled  int
	failAt  int // fail the failAt-th Keys call (1-based), 0 never
	calls   int
}

var errStop = errors.New("stop")

func (s *sink) Keys(keys [][]byte) error {
	s.calls++
	if s.calls == s.failAt {
		return errStop
	}
	if s.rows != nil && s.next != len(s.columns) {
		s.t.Fatalf("a run started with column %d of %d still due", s.next, len(s.columns))
	}
	s.run, s.next, s.filled = len(s.rows), 0, 0
	for _, k := range keys {
		s.rows = append(s.rows, seekparquet.Row{Key: k, Values: make([]seekparquet.Value, len(s.columns))})
	}
	return nil
}

func (s *sink) Values(c int, values []parquet.Value) error {
	if c != s.next {
		s.t.Fatalf("values of column %d while %d is due", c, s.next)
	}
	for _, v := range values {
		s.rows[s.run+s.filled].Values[c] = seekparquet.Value{Value: v.Clone()}
		s.filled++
	}
	if s.run+s.filled == len(s.rows) {
		s.next, s.filled = s.next+1, 0
	}
	return nil
}

func (s *sink) Absent(c int) error {
	if c != s.next || s.filled != 0 {
		s.t.Fatalf("column %d absent while %d is due", c, s.next)
	}
	for i := s.run; i < len(s.rows); i++ {
		s.rows[i].Values[c] = seekparquet.Value{Absent: true}
	}
	s.next++
	return nil
}

// TestScanMatchesRange reads ranges across pages, row groups and parts,
// with a column some parts lack, through Range and Scan; and a sink error
// ends the scan with that error.
func TestScanMatchesRange(t *testing.T) {
	d := open(t, write(t, typedDataset()), seekparquet.Options{})
	columns := []string{"i64", "nope", "flag", "fixed"}
	for _, start := range []string{"", "a", "b", "t2", "t3", "t5"} {
		for _, limit := range []int{1, 2, 3, 100} {
			rng := seekparquet.Range{Start: []byte(start), Limit: limit, Columns: columns}
			want, err := d.Range(ctx, rng, nil)
			must(t, err)
			s := &sink{t: t, columns: columns}
			must(t, d.Scan(ctx, rng, nil, s))
			if len(s.rows) != len(want) {
				t.Fatalf("start %q limit %d: %d rows, want %d", start, limit, len(s.rows), len(want))
			}
			for i := range want {
				if string(s.rows[i].Key) != string(want[i].Key) || !slices.EqualFunc(s.rows[i].Values, want[i].Values, sameValue) {
					t.Fatalf("row %d: %v, want %v", i, s.rows[i].Values, want[i].Values)
				}
			}
		}
	}
	s := &sink{t: t, columns: columns, failAt: 2}
	if err := d.Scan(ctx, seekparquet.Range{Limit: 100, Columns: columns}, nil, s); err != errStop {
		t.Fatalf("a failing sink: %v", err)
	}
}

// TestStatsCountDecoding counts the pages a read decodes with no value
// column, one, and two.
func TestStatsCountDecoding(t *testing.T) {
	var keys []string
	for i := range 50 {
		keys = append(keys, fmt.Sprintf("k%03d", i))
	}
	d := open(t, write(t, parts(seekparquettest.Part{PageBytes: 1 << 20}, keys)), seekparquet.Options{})
	pages := func(columns ...string) int64 {
		var stats seekparquet.Stats
		rows, err := d.Range(ctx, seekparquet.Range{Limit: 5, Columns: columns}, &stats)
		if err != nil || len(rows) != 5 {
			t.Fatalf("read: %d rows, %v", len(rows), err)
		}
		return stats.PagesDecoded
	}
	if a, b, c := pages(), pages("size"), pages("size", "name"); a != 1 || b != 2 || c != 3 {
		t.Fatalf("pages decoded %d, %d, %d; want 1, 2, 3", a, b, c)
	}
}

// TestMetadata exposes the manifest as a copy, extensions included and
// present only when the manifest has them.
func TestMetadata(t *testing.T) {
	ds := parts(seekparquettest.Part{}, []string{"a", "b"}, []string{"c"})
	ds.SchemaID = "test/v2"
	ds.Extensions = map[string]any{"producer": map[string]any{"complete": true}}
	d := open(t, write(t, ds), seekparquet.Options{})
	m := d.Metadata()
	if m.Version != (seekparquet.Version{FormatVersion: 0, DraftRevision: 1}) || m.SchemaID != "test/v2" || len(m.Parts) != 2 ||
		string(m.Parts[0].MinKey) != "a" || string(m.Parts[1].MaxKey) != "c" || m.Parts[0].Rows != 2 || m.Parts[0].Path != seekparquettest.PartPath(0) {
		t.Fatalf("%+v", m)
	}
	raw, ok := m.Extension("producer")
	var producer map[string]bool
	if !ok || json.Unmarshal(raw, &producer) != nil || !producer["complete"] {
		t.Fatalf("extension %s, %v", raw, ok)
	}
	if _, ok := m.Extension("other"); ok {
		t.Fatal("an extension the manifest lacks")
	}
	m.Parts[0].MinKey[0] = 'z'
	m.Extensions[0] = 'x'
	if again := d.Metadata(); string(again.Parts[0].MinKey) != "a" || again.Extensions[0] != '{' {
		t.Fatal("Metadata is not a copy")
	}
	plain := open(t, write(t, parts(seekparquettest.Part{}, []string{"a"})), seekparquet.Options{}).Metadata()
	if plain.Extensions != nil {
		t.Fatalf("extensions %s on a manifest without them", plain.Extensions)
	}
}

// TestDataPageV1 reads parts written with version 1 data pages.
func TestDataPageV1(t *testing.T) {
	keys := fixtureKeys(500)
	d := open(t, write(t, parts(seekparquettest.Part{DataPageVersion: 1, PageBytes: 256, GroupRows: 100}, split(keys, 2)...)), seekparquet.Options{})
	rows, err := d.Range(ctx, seekparquet.Range{Start: []byte(keys[123]), Limit: 300, Columns: []string{"size"}}, nil)
	must(t, err)
	if !slices.Equal(keysOf(rows), keys[123:423]) || rows[0].Values[0].Int64() != 123 {
		t.Fatalf("%d rows", len(rows))
	}
}
