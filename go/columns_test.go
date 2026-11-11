// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
	ms := Logical{Kind: LogicalTimestamp, Unit: Millis}
	us := Logical{Kind: LogicalTimestamp, AdjustedToUTC: true, Unit: Micros}
	decimal := func(precision, scale int32) Logical {
		return Logical{Kind: LogicalDecimal, Precision: precision, Scale: scale}
	}
	want := []Column{
		{Name: "key", Type: KindByteArray, Repetition: Required, Key: true},
		{Name: "d", Type: KindInt32, Logical: Logical{Kind: LogicalDate}, Repetition: Optional},
		{Name: "ts_ms", Type: KindInt64, Logical: ms, Repetition: Optional},
		{Name: "ts_utc", Type: KindInt64, Logical: us, Repetition: Optional},
		{Name: "dec9", Type: KindInt32, Logical: decimal(9, 2), Repetition: Optional},
		{Name: "dec18", Type: KindInt64, Logical: decimal(18, 4), Repetition: Optional},
		{Name: "dec30", Type: KindFixedLenByteArray, Length: 13, Logical: decimal(30, 5), Repetition: Optional},
		{Name: "s", Type: KindByteArray, Logical: Logical{Kind: LogicalString}, Repetition: Optional},
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
	d := open(t, dir, Options{})
	got, err := d.Columns(ctx, 0)
	must(t, err)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("before Verify:\n got %+v\nwant %+v", got, want)
	}
	if !d.r.parts[0].ready.Load() {
		t.Fatal("Columns answered without checking the part")
	}
	got[0].Name, got[1].Logical.Kind = "changed", LogicalString
	must(t, d.Verify(ctx))
	if got, err = d.Columns(ctx, 0); err != nil || !reflect.DeepEqual(got, want) {
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
		if _, err := d.Columns(ctx, i); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("part %d: %v, want ErrInvalidRequest", i, err)
		}
	}
	must(t, d.Close())
	if _, err := d.Columns(ctx, 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("after Close: %v, want ErrClosed", err)
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
		d := open(t, dir, Options{})
		cols, err := d.Columns(ctx, first)
		must(t, err)
		if len(cols) != 2 || cols[1].Name != "size" || cols[1].Type != types[first] {
			t.Fatalf("part %d: %+v", first, cols)
		}
		if _, err := d.Columns(ctx, 1-first); CodeOf(err) != CodeInconsistentColumn {
			t.Fatalf("part %d after %d: %v, want inconsistent_column", 1-first, first, err)
		}
		if err := d.Verify(ctx); CodeOf(err) != CodeInconsistentColumn {
			t.Fatalf("Verify: %v, want inconsistent_column", err)
		}
		if again, err := d.Columns(ctx, first); err != nil || !reflect.DeepEqual(again, cols) {
			t.Fatalf("part %d again: %+v, %v", first, again, err)
		}
	}
}

// TestColumnsConcurrent races Columns with reads on unchecked parts.
func TestColumnsConcurrent(t *testing.T) {
	d := open(t, write(t, lazyParts()), Options{})
	var wg sync.WaitGroup
	for i := range 16 {
		part := i % len(d.r.parts)
		wg.Go(func() {
			cols, err := d.Columns(ctx, part)
			if err != nil || len(cols) == 0 || !cols[0].Key {
				t.Errorf("part %d: %+v, %v", part, cols, err)
			}
		})
		wg.Go(func() {
			if rows, err := d.Range(ctx, PrefixRange([]byte{"abcd"[part]}, 100)); err != nil || len(rows) != 25 {
				t.Errorf("part %d: %d rows, %v", part, len(rows), err)
			}
		})
	}
	wg.Wait()
}

// TestColumnsConvertedType: a leaf with only a legacy converted_type
// describes as the same leaf with the logical type it stands for; with
// both, the logical type is kept.
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
		if got, want := columnOf(&se, "key"), columnOf(&modern, "key"); !reflect.DeepEqual(got, want) {
			t.Errorf("converted type %d: %+v, want %+v", c.converted, got, want)
		}
	}
	both := format.SchemaElement{Name: "v", Type: thrift.New(format.Int64), RepetitionType: thrift.New(format.Optional),
		ConvertedType: thrift.New(deprecated.TimestampMicros),
		LogicalType:   format.LogicalType{Value: &format.TimestampType{Unit: us}}}
	if got := columnOf(&both, "key").Logical; got != (Logical{Kind: LogicalTimestamp, Unit: Micros}) {
		t.Errorf("both: %+v, want the logical type", got)
	}
}
