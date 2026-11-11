// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/seekparquet/go/internal/testutil"
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

func typedDataset() testutil.Dataset {
	var rows []typed
	for i := range 6 {
		r := typed{Key: []byte(fmt.Sprintf("t%d", i)), Fixed: [4]byte{byte(i), 0, 0xff, 0x7f}, Dotted: int64(i)}
		if i%2 == 0 {
			r.Flag, r.I32, r.I64 = ptr(i == 0), ptr(int32(-i)), ptr(int64(math.MaxInt64-i))
			r.F32, r.F64, r.Blob = ptr(float32(math.Copysign(0, -1))), ptr(math.Inf(1)), []byte{byte(i)}
		}
		rows = append(rows, r)
	}
	return testutil.Dataset{Parts: []testutil.Part{
		{Rows: []older{{Key: []byte("a"), Flag: ptr(true)}, {Key: []byte("b")}}},
		{Rows: rows, GroupRows: 4},
	}}
}

// TestProjection projects every baseline type, nulls, absent columns and a
// column whose name holds a dot (COL-6), through Query and ListQuery, from
// every start, so that runs begin inside pages and row groups.
func TestProjection(t *testing.T) {
	d := open(t, write(t, typedDataset()), OpenOptions{})
	columns := []string{"flag", "i32", "i64", "f32", "f64", "blob", "fixed", "a.b", "nope"}
	rows, err := d.Rows(ctx, Query{Limit: 100, Columns: columns}, nil)
	must(t, err)
	if len(rows) != 8 {
		t.Fatalf("%d rows", len(rows))
	}
	// Part 0 has only flag: the rest is absent, which is not null.
	a, b := rows[0], rows[1]
	if a.Values[0].Absent() || !a.Values[0].Boolean() || b.Values[0].Absent() || !b.Values[0].IsNull() {
		t.Fatalf("flag: %+v, %+v", a.Values[0], b.Values[0])
	}
	for c := 1; c < len(columns); c++ {
		if !a.Values[c].Absent() || a.Values[c].IsNull() {
			t.Fatalf("column %s of part 0 is not absent: %+v", columns[c], a.Values[c])
		}
	}
	t0, t1 := rows[2], rows[3]
	if t0.Values[1].Int32() != 0 || t0.Values[2].Int64() != math.MaxInt64 || math.Float32bits(t0.Values[3].Float()) != 0x80000000 ||
		!math.IsInf(t0.Values[4].Double(), 1) || string(t0.Values[5].Bytes()) != "\x00" || string(t0.Values[6].Bytes()) != "\x00\x00\xff\x7f" ||
		t0.Values[7].Int64() != 0 {
		t.Fatalf("t0 values %v", t0.Values)
	}
	for c := 0; c < 6; c++ {
		if t1.Values[c].Absent() || !t1.Values[c].IsNull() {
			t.Fatalf("t1 column %s: %+v, want null", columns[c], t1.Values[c])
		}
	}
	if !t1.Values[8].Absent() {
		t.Fatal("a column no part has is not absent")
	}
	if v, ok := t1.Value("a.b"); !ok || v.Int64() != 1 {
		t.Fatalf("Value(a.b) = %v, %v", v, ok)
	}
	if _, ok := t1.Value("size"); ok {
		t.Fatal("Value of an unprojected column")
	}
	entries, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 100, Columns: columns}, nil)
	must(t, err)
	for i, e := range entries {
		if !slices.EqualFunc(e.Row.Values, rows[i].Values, sameValue) {
			t.Fatalf("list row %d: %v, range %v", i, e.Row.Values, rows[i].Values)
		}
	}
	for i := range rows {
		for _, limit := range []int{1, 2, 3} {
			rng := Query{Selection: Selection{Span: Span{Start: rows[i].Key}}, Limit: int64(limit), Columns: columns}
			got, err := d.Rows(ctx, rng, nil)
			must(t, err)
			for j := range got {
				if !slices.EqualFunc(got[j].Values, rows[i+j].Values, sameValue) {
					t.Fatalf("%s: row %d: %v, want %v", describe(rng), j, got[j].Values, rows[i+j].Values)
				}
			}
		}
	}
	// Projecting a nested or repeated column is rejected (COL-3).
	for _, c := range []string{"tags", "meta"} {
		_, err := d.Rows(ctx, Query{Limit: 1, Columns: []string{c}}, nil)
		var u *UnsupportedError
		if !errors.As(err, &u) || u.Code != UnsupportedProjection {
			t.Fatalf("%s: %v", c, err)
		}
		if _, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 1, Columns: []string{c}}, nil); !errors.As(err, &u) {
			t.Fatalf("list %s: %v", c, err)
		}
	}
}

func sameValue(a, b Value) bool {
	return a.Absent() == b.Absent() && a.IsNull() == b.IsNull() && parquet.Equal(a.v, b.v)
}

// TestRowsOwnTheirBytes changes everything a read was given and returned:
// nothing else changes.
func TestRowsOwnTheirBytes(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{}, []string{"a", "b", "c"})), OpenOptions{})
	columns := []string{"size", "name"}
	rows, err := d.Rows(ctx, Query{Limit: 3, Columns: columns}, nil)
	must(t, err)
	columns[0] = "changed"
	if rows[0].Columns[0] != "size" {
		t.Fatal("Row.Columns aliases the request")
	}
	rows[0].Key[0] = 'z'
	copy(rows[0].Values[1].Bytes(), "XXXX")
	again, _ := d.Rows(ctx, Query{Limit: 3, Columns: []string{"size", "name"}}, nil)
	if string(again[0].Key) != "a" || string(again[0].Values[1].Bytes()) != "name-0" || string(rows[1].Key) != "b" {
		t.Fatalf("a result's bytes are shared: %q %q", again[0].Key, again[0].Values[1].Bytes())
	}
}

// TestStatsCountDecoding counts the pages a read decodes with no value
// column, one, and two.
func TestStatsCountDecoding(t *testing.T) {
	var keys []string
	for i := range 50 {
		keys = append(keys, fmt.Sprintf("k%03d", i))
	}
	d := open(t, write(t, parts(testutil.Part{PageBytes: 1 << 20}, keys)), OpenOptions{})
	pages := func(columns ...string) int64 {
		var st ReadStats
		rows, err := d.r.readRange(ctx, Query{Limit: 5, Columns: columns}, &st)
		if err != nil || len(rows) != 5 {
			t.Fatalf("read: %d rows, %v", len(rows), err)
		}
		return st.KeyPagesDecoded + st.ValuePagesDecoded
	}
	if a, b, c := pages(), pages("size"), pages("size", "name"); a != 1 || b != 2 || c != 3 {
		t.Fatalf("pages decoded %d, %d, %d; want 1, 2, 3", a, b, c)
	}
}

// TestMetadata exposes the manifest as a copy, extensions included and
// present only when the manifest has them.
func TestMetadata(t *testing.T) {
	ds := parts(testutil.Part{}, []string{"a", "b"}, []string{"c"})
	ds.SchemaID = "test/v2"
	ds.Extensions = map[string]any{"producer": map[string]any{"complete": true}}
	d := open(t, write(t, ds), OpenOptions{})
	m := d.Metadata()
	if m.Version != (Version{FormatVersion: 0, DraftRevision: 1}) || m.SchemaID != "test/v2" || len(m.Parts) != 2 ||
		string(m.Parts[0].MinKey) != "a" || string(m.Parts[1].MaxKey) != "c" || m.Parts[0].Rows != 2 || m.Parts[0].Path != testutil.PartPath(0) {
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
	plain := open(t, write(t, parts(testutil.Part{}, []string{"a"})), OpenOptions{}).Metadata()
	if plain.Extensions != nil {
		t.Fatalf("extensions %s on a manifest without them", plain.Extensions)
	}
}

// TestPageVersionsAndCodecs reads parts written with version 1 and 2 data
// pages under each codec parquet-go writes, from inside the parts.
func TestPageVersionsAndCodecs(t *testing.T) {
	keys := fixtureKeys(500)
	codecs := map[string]parquet.WriterOption{
		"uncompressed": parquet.Compression(&parquet.Uncompressed),
		"snappy":       parquet.Compression(&parquet.Snappy),
		"gzip":         parquet.Compression(&parquet.Gzip),
		"zstd":         parquet.Compression(&parquet.Zstd),
		"brotli":       parquet.Compression(&parquet.Brotli),
		"lz4 raw":      parquet.Compression(&parquet.Lz4Raw),
	}
	for name, codec := range codecs {
		for _, version := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s, data page v%d", name, version), func(t *testing.T) {
				geometry := testutil.Part{DataPageVersion: version, PageBytes: 256, GroupRows: 100, Compression: codec}
				d := open(t, write(t, parts(geometry, split(keys, 2)...)), OpenOptions{})
				rng := Query{Selection: Selection{Span: Span{Start: []byte(keys[123])}}, Limit: 300, Columns: []string{"size", "name"}}
				rows, err := d.Rows(ctx, rng, nil)
				must(t, err)
				if !slices.Equal(keysOf(rows), keys[123:423]) || rows[0].Values[0].Int64() != 123 || string(rows[1].Values[1].Bytes()) != "name-124" {
					t.Fatalf("%s: %d rows", describe(rng), len(rows))
				}
			})
		}
	}
}
