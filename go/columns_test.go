// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// TestColumnsAnnotated: the annotated conformance fixture's columns, as its
// generator declares them, from Columns before and after Verify and from
// InspectFooter, which agree. Columns checks the part it answers for.
func TestColumnsAnnotated(t *testing.T) {
	ms := Logical{Kind: LogicalTimestamp, Unit: TimeUnitMillis}
	us := Logical{Kind: LogicalTimestamp, AdjustedToUTC: true, Unit: TimeUnitMicros}
	decimal := func(precision, scale int32) Logical {
		return Logical{Kind: LogicalDecimal, Precision: precision, Scale: scale}
	}
	want := []Column{
		{Name: "key", Type: KindByteArray, Repetition: RepetitionRequired, Key: true},
		{Name: "d", Type: KindInt32, Logical: Logical{Kind: LogicalDate}, Repetition: RepetitionOptional},
		{Name: "ts_ms", Type: KindInt64, Logical: ms, Repetition: RepetitionOptional},
		{Name: "ts_utc", Type: KindInt64, Logical: us, Repetition: RepetitionOptional},
		{Name: "dec9", Type: KindInt32, Logical: decimal(9, 2), Repetition: RepetitionOptional},
		{Name: "dec18", Type: KindInt64, Logical: decimal(18, 4), Repetition: RepetitionOptional},
		{Name: "dec30", Type: KindFixedLenByteArray, Length: 13, Logical: decimal(30, 5), Repetition: RepetitionOptional},
		{Name: "s", Type: KindByteArray, Logical: Logical{Kind: LogicalString}, Repetition: RepetitionOptional},
	}
	strs := []string{
		"key BYTE_ARRAY REQUIRED key",
		"d INT32 OPTIONAL DATE",
		"ts_ms INT64 OPTIONAL TIMESTAMP(isAdjustedToUTC=false,unit=MILLIS)",
		"ts_utc INT64 OPTIONAL TIMESTAMP(isAdjustedToUTC=true,unit=MICROS)",
		"dec9 INT32 OPTIONAL DECIMAL(9,2)",
		"dec18 INT64 OPTIONAL DECIMAL(18,4)",
		"dec30 FIXED_LEN_BYTE_ARRAY(13) OPTIONAL DECIMAL(30,5)",
		"s BYTE_ARRAY OPTIONAL STRING",
	}
	for i, c := range want {
		if c.String() != strs[i] {
			t.Errorf("column %d: %q, want %q", i, c.String(), strs[i])
		}
	}
	dir := "../conformance/datasets/annotated"
	d := open(t, dir, OpenOptions{})
	got, err := d.Columns(ctx, 0, nil)
	must(t, err)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("before Verify:\n got %+v\nwant %+v", got, want)
	}
	if !d.r.parts[0].ready.Load() {
		t.Fatal("Columns answered without checking the part")
	}
	got[0].Name, got[1].Logical.Kind = "changed", LogicalString
	must(t, d.Verify(ctx, nil))
	if got, err = d.Columns(ctx, 0, nil); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("after Verify: %v\n got %+v\nwant %+v", err, got, want)
	}
	part := filepath.Join(dir, filepath.FromSlash(d.Metadata().Parts[0].Path))
	f, err := os.Open(part)
	must(t, err)
	defer f.Close()
	ft, err := InspectFooter(f, d.Metadata().Parts[0].Bytes)
	must(t, err)
	if !reflect.DeepEqual(ft.Schema, want) {
		t.Fatalf("InspectFooter:\n got %+v\nwant %+v", ft.Schema, want)
	}
	for _, i := range []int{-1, 1} {
		if _, err := d.Columns(ctx, i, nil); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("part %d: %v, want ErrInvalidRequest", i, err)
		}
	}
	must(t, d.Close())
	if _, err := d.Columns(ctx, 0, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("after Close: %v, want ErrClosed", err)
	}
}

// TestColumnsGroupAnnotations: a LIST group and a MAP group are reported,
// by Columns and InspectFooter, as groups with the zero Logical, so no
// Column carries LogicalList or LogicalMap.
func TestColumnsGroupAnnotations(t *testing.T) {
	type row struct {
		Key   []byte           `parquet:"key"`
		Tags  []string         `parquet:"tags,list"`
		Attrs map[string]int64 `parquet:"attrs"`
	}
	rows := make([]row, 8)
	for i := range rows {
		rows[i] = row{Key: []byte{'k', byte('0' + i)}, Tags: []string{"t"}, Attrs: map[string]int64{"a": int64(i)}}
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows}}})
	meta, err := testutil.ReadFooter(partFile(dir, 0))
	must(t, err)
	annotated := map[string]bool{}
	for _, se := range meta.Schema {
		switch se.LogicalType.Value.(type) {
		case *format.ListType:
			annotated[se.Name+"/LIST"] = se.NumChildren.V > 0
		case *format.MapType:
			annotated[se.Name+"/MAP"] = se.NumChildren.V > 0
		}
	}
	if !annotated["tags/LIST"] || !annotated["attrs/MAP"] {
		t.Fatalf("the part's schema lacks the annotated groups: %v", annotated)
	}
	want := []Column{
		{Name: "key", Type: KindByteArray, Repetition: RepetitionRequired, Key: true},
		{Name: "tags", Group: true, Repetition: RepetitionRequired},
		{Name: "attrs", Group: true, Repetition: RepetitionRequired},
	}
	d := open(t, dir, OpenOptions{})
	got, err := d.Columns(ctx, 0, nil)
	must(t, err)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Columns:\n got %+v\nwant %+v", got, want)
	}
	f, err := os.Open(partFile(dir, 0))
	must(t, err)
	defer f.Close()
	ft, err := InspectFooter(f, d.Metadata().Parts[0].Bytes)
	must(t, err)
	if !reflect.DeepEqual(ft.Schema, want) {
		t.Fatalf("InspectFooter:\n got %+v\nwant %+v", ft.Schema, want)
	}
}

// TestColumnsReadStats: Columns resets st on entry; when it is the first to
// touch the part, st receives the reads of the part's check, whether the
// check passes or refuses the part, and once the part's outcome is kept a
// later call reads nothing.
func TestColumnsReadStats(t *testing.T) {
	stale := ReadStats{ValuePagesDecoded: 9, PrefixSkips: 9} // counts a check never makes
	checked := write(t, parts(testutil.Part{}, []string{"a", "b"}))
	refused := write(t, parts(testutil.Part{}, []string{"a", "b"}))
	rewrite(t, refused, 0, func(m *format.FileMetaData) { m.Schema[1].RepetitionType = thrift.New(format.Optional) })
	for _, c := range []struct {
		name, dir string
		code      RefusalCode
	}{{"checked", checked, ""}, {"refused", refused, CodeInvalidKeyColumn}} {
		t.Run(c.name, func(t *testing.T) {
			src := &accessSource{Source: dirOf(t, c.dir)}
			d, err := OpenDataset(ctx, src, OpenOptions{})
			must(t, err)
			t.Cleanup(func() { _ = d.Close() })
			src.reset()
			st := stale
			_, err = d.Columns(ctx, 0, &st)
			if RefusalCodeOf(err) != c.code || c.code == "" && err != nil {
				t.Fatalf("cold: %v, want refusal %q", err, c.code)
			}
			if st.PartsOpened != 1 || st.SourceReads == 0 || st.SourceReads != src.reads.Load() || st.SourceBytes != src.bytes.Load() ||
				st.ValuePagesDecoded != 0 || st.PrefixSkips != 0 {
				t.Fatalf("cold: %+v; the Source saw %d reads, %d bytes", st, src.reads.Load(), src.bytes.Load())
			}
			src.reset()
			st = stale
			_, err = d.Columns(ctx, 0, &st)
			if RefusalCodeOf(err) != c.code || st != (ReadStats{}) || src.reads.Load() != 0 {
				t.Fatalf("warm: %v, %+v, %d reads", err, st, src.reads.Load())
			}
		})
	}
	st := stale
	if _, err := open(t, checked, OpenOptions{}).Columns(ctx, 1, &st); !errors.Is(err, ErrInvalidRequest) || st != (ReadStats{}) {
		t.Fatalf("invalid request: %v, %+v", err, st)
	}
}

// TestColumnsInconsistent: a second part whose column differs in physical
// type is refused by COL-5, by Columns as by a read, whichever part is
// checked first; the part checked first still answers.
func TestColumnsInconsistent(t *testing.T) {
	type narrow struct {
		Key  []byte `parquet:"key"`
		Size int32  `parquet:"size"`
	}
	type wide struct {
		Key  []byte `parquet:"key"`
		Size int64  `parquet:"size"`
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{
		{Rows: []narrow{{[]byte("a"), 1}}},
		{Rows: []wide{{[]byte("b"), 2}}},
	}})
	types := []Kind{KindInt32, KindInt64}
	for first := range 2 {
		d := open(t, dir, OpenOptions{})
		cols, err := d.Columns(ctx, first, nil)
		must(t, err)
		if len(cols) != 2 || cols[1].Name != "size" || cols[1].Type != types[first] {
			t.Fatalf("part %d: %+v", first, cols)
		}
		if _, err := d.Columns(ctx, 1-first, nil); RefusalCodeOf(err) != CodeInconsistentColumn {
			t.Fatalf("part %d after %d: %v, want inconsistent_column", 1-first, first, err)
		}
		if err := d.Verify(ctx, nil); RefusalCodeOf(err) != CodeInconsistentColumn {
			t.Fatalf("Verify: %v, want inconsistent_column", err)
		}
		if again, err := d.Columns(ctx, first, nil); err != nil || !reflect.DeepEqual(again, cols) {
			t.Fatalf("part %d again: %+v, %v", first, again, err)
		}
	}
}

// TestColumnsConcurrent races Columns with reads on unchecked parts.
func TestColumnsConcurrent(t *testing.T) {
	d := open(t, write(t, lazyParts()), OpenOptions{})
	var wg sync.WaitGroup
	for i := range 16 {
		part := i % len(d.r.parts)
		wg.Go(func() {
			cols, err := d.Columns(ctx, part, nil)
			if err != nil || len(cols) == 0 || !cols[0].Key {
				t.Errorf("part %d: %+v, %v", part, cols, err)
			}
		})
		wg.Go(func() {
			if rows, err := d.Rows(ctx, Query{Selection: Selection{Span: PrefixSpan([]byte{"abcd"[part]})}, Limit: 100}, nil); err != nil || len(rows) != 25 {
				t.Errorf("part %d: %d rows, %v", part, len(rows), err)
			}
		})
	}
	wg.Wait()
}

// TestColumnsUnrecognizedAnnotation: a value column annotated in a way the
// reader cannot describe (a logical type it does not know, a converted type
// it does not know, or INTERVAL, a converted type with no logical type)
// reports LogicalUnrecognized with its physical type, and reads, lists and
// validates as the same column unannotated does, with the same values.
func TestColumnsUnrecognizedAnnotation(t *testing.T) {
	type row struct {
		Key  []byte   `parquet:"key"`
		Size int64    `parquet:"size"`
		Span [12]byte `parquet:"span"`
	}
	// Both value columns unannotated.
	dataset := func(t *testing.T) string {
		dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: []row{
			{[]byte("a"), 1, [12]byte{1}}, {[]byte("b"), 2, [12]byte{2}},
		}}}})
		rewrite(t, dir, 0, func(m *format.FileMetaData) {
			for i := 2; i < len(m.Schema); i++ {
				m.Schema[i].LogicalType.Value = nil
				m.Schema[i].ConvertedType = thrift.Null[deprecated.ConvertedType]{}
			}
		})
		return dir
	}
	columns := []string{"size", "span"}
	read := func(t *testing.T, dir string) ([]Column, []Row, []Row) {
		t.Helper()
		d := open(t, dir, OpenOptions{})
		cols, err := d.Columns(ctx, 0, nil)
		must(t, err)
		rows, err := d.Rows(ctx, Query{Limit: 10, Columns: columns}, nil)
		must(t, err)
		entries, err := d.List(ctx, ListQuery{Limit: 10, Columns: columns}, nil)
		must(t, err)
		listed := make([]Row, len(entries))
		for i, e := range entries {
			listed[i] = *e.Row
		}
		expectLevels(t, validate(t, dir, ValidateOptions{}),
			map[ValidationLevel]string{LevelStructural: "complete", LevelProfile: "complete", LevelFull: "complete"})
		return cols, rows, listed
	}
	plainColumns, plainRows, plainEntries := read(t, dataset(t))
	for _, c := range plainColumns[1:] {
		if c.Logical != (Logical{}) {
			t.Fatalf("unannotated column %s", c)
		}
	}
	for _, c := range []struct {
		name   string
		column int // the schema element, and its index in Columns
		edit   func(t *testing.T, dir string)
	}{
		{"unknown logical type", 2, func(t *testing.T, dir string) {
			rewrite(t, dir, 0, func(m *format.FileMetaData) { m.Schema[2].LogicalType.Value = &format.NullType{} })
			raw, err := os.ReadFile(partFile(dir, 0))
			must(t, err)
			n := int(binary.LittleEndian.Uint32(raw[len(raw)-8:]))
			footer := raw[len(raw)-8-n : len(raw)-8]
			known := []byte{0xbc, 0, 0}            // LogicalType.UNKNOWN (NullType)
			unknown := []byte{0x0c, 0xc6, 1, 0, 0} // a future logical alternative, field 99
			if bytes.Count(footer, known) != 1 {
				t.Fatal("logical annotation not found exactly once")
			}
			replaceFooter(t, dir, bytes.Replace(footer, known, unknown, 1))
		}},
		{"unknown converted type", 2, func(t *testing.T, dir string) {
			rewrite(t, dir, 0, func(m *format.FileMetaData) { m.Schema[2].ConvertedType = thrift.New(deprecated.ConvertedType(99)) })
		}},
		{"INTERVAL", 3, func(t *testing.T, dir string) {
			rewrite(t, dir, 0, func(m *format.FileMetaData) { m.Schema[3].ConvertedType = thrift.New(deprecated.Interval) })
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := dataset(t)
			c.edit(t, dir)
			cols, rows, entries := read(t, dir)
			want := slices.Clone(plainColumns)
			want[c.column-1].Logical = Logical{Kind: LogicalUnrecognized}
			if !reflect.DeepEqual(cols, want) {
				t.Fatalf("columns:\n got %+v\nwant %+v", cols, want)
			}
			if got, suffix := cols[c.column-1].String(), " UNRECOGNIZED"; !strings.HasSuffix(got, suffix) {
				t.Errorf("column %q does not end with %q", got, suffix)
			}
			if !reflect.DeepEqual(rows, plainRows) || !reflect.DeepEqual(entries, plainEntries) {
				t.Fatalf("rows %+v, entries %+v; unannotated %+v, %+v", rows, entries, plainRows, plainEntries)
			}
		})
	}
}

// TestColumnsConvertedType: a leaf with only a legacy converted_type
// describes as the same leaf with the logical type it stands for, or with
// LogicalUnrecognized for INTERVAL, which stands for none; with both, the
// logical type is kept.
func TestColumnsConvertedType(t *testing.T) {
	ms := format.TimeUnit{Value: &format.MilliSeconds{}}
	us := format.TimeUnit{Value: &format.MicroSeconds{}}
	cases := []struct {
		typ       format.Type
		converted deprecated.ConvertedType
		logical   format.LogicalTypeValue
	}{
		{format.ByteArray, deprecated.UTF8, &format.StringType{}},
		{format.ByteArray, deprecated.Enum, &format.EnumType{}},
		{format.ByteArray, deprecated.Json, &format.JsonType{}},
		{format.ByteArray, deprecated.Bson, &format.BsonType{}},
		{format.Int32, deprecated.Date, &format.DateType{}},
		{format.Int32, deprecated.TimeMillis, &format.TimeType{IsAdjustedToUTC: true, Unit: ms}},
		{format.Int64, deprecated.TimeMicros, &format.TimeType{IsAdjustedToUTC: true, Unit: us}},
		{format.Int64, deprecated.TimestampMillis, &format.TimestampType{IsAdjustedToUTC: true, Unit: ms}},
		{format.Int64, deprecated.TimestampMicros, &format.TimestampType{IsAdjustedToUTC: true, Unit: us}},
		{format.Int32, deprecated.Int8, &format.IntType{BitWidth: 8, IsSigned: true}},
		{format.Int32, deprecated.Int16, &format.IntType{BitWidth: 16, IsSigned: true}},
		{format.Int32, deprecated.Int32, &format.IntType{BitWidth: 32, IsSigned: true}},
		{format.Int64, deprecated.Int64, &format.IntType{BitWidth: 64, IsSigned: true}},
		{format.Int32, deprecated.Uint8, &format.IntType{BitWidth: 8}},
		{format.Int32, deprecated.Uint16, &format.IntType{BitWidth: 16}},
		{format.Int32, deprecated.Uint32, &format.IntType{BitWidth: 32}},
		{format.Int64, deprecated.Uint64, &format.IntType{BitWidth: 64}},
		{format.Int64, deprecated.Decimal, &format.DecimalType{Precision: 18, Scale: 4}},
		{format.FixedLenByteArray, deprecated.Interval, nil},
	}
	for _, c := range cases {
		se := format.SchemaElement{Name: "v", Type: thrift.New(c.typ), RepetitionType: thrift.New(format.Optional),
			ConvertedType: thrift.New(c.converted)}
		if c.typ == format.FixedLenByteArray {
			se.TypeLength = thrift.New[int32](12)
		}
		if c.converted == deprecated.Decimal {
			se.Precision, se.Scale = thrift.New[int32](18), thrift.New[int32](4)
		}
		modern := se
		modern.ConvertedType = thrift.Null[deprecated.ConvertedType]{}
		if c.logical != nil {
			modern.LogicalType = format.LogicalType{Value: c.logical}
		}
		want := columnOf(&modern, "key")
		if c.logical == nil {
			want.Logical = Logical{Kind: LogicalUnrecognized}
		}
		if got := columnOf(&se, "key"); !reflect.DeepEqual(got, want) {
			t.Errorf("converted type %d: %+v, want %+v", c.converted, got, want)
		}
	}
	both := format.SchemaElement{Name: "v", Type: thrift.New(format.Int64), RepetitionType: thrift.New(format.Optional),
		ConvertedType: thrift.New(deprecated.TimestampMicros),
		LogicalType:   format.LogicalType{Value: &format.TimestampType{Unit: us}}}
	if got := columnOf(&both, "key").Logical; got != (Logical{Kind: LogicalTimestamp, Unit: TimeUnitMicros}) {
		t.Errorf("both: %+v, want the logical type", got)
	}
}
