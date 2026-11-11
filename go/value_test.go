// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"math"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// kindProbe reads every typed accessor of a value, so that a test can
// require the accessor of its kind to give the value and every other to
// give its zero value.
type kindProbe struct {
	Boolean bool
	Int32   int32
	Uint32  uint32
	Int64   int64
	Uint64  uint64
	Int96   Int96
	Float   uint32
	Double  uint64
	Bytes   []byte
}

func probeValue(v Value) kindProbe {
	return kindProbe{v.Boolean(), v.Int32(), v.Uint32(), v.Int64(), v.Uint64(), v.Int96(),
		math.Float32bits(v.Float()), math.Float64bits(v.Double()), v.Bytes()}
}

func probeView(c ColumnView, i int) kindProbe {
	return kindProbe{c.Boolean(i), c.Int32(i), c.Uint32(i), c.Int64(i), c.Uint64(i), c.Int96(i),
		math.Float32bits(c.Float(i)), math.Float64bits(c.Double(i)), c.Bytes(i)}
}

// TestValueConstructors: each constructor makes a value of its kind that
// only its own accessors read; every other accessor, and every accessor of
// a null or absent value, gives the zero value without panicking. Absent,
// null and a value are three disjoint states.
func TestValueConstructors(t *testing.T) {
	fixed := []byte{1, 2, 3, 4}
	cases := []struct {
		v    Value
		kind Kind
		want kindProbe
		str  string
	}{
		{NullValue(), KindNone, kindProbe{}, "null"},
		{Value{}, KindNone, kindProbe{}, "null"},
		{AbsentValue(), KindNone, kindProbe{}, "absent"},
		{BooleanValue(true), KindBoolean, kindProbe{Boolean: true}, "true"},
		{Int32Value(-7), KindInt32, kindProbe{Int32: -7, Uint32: math.MaxUint32 - 6}, "-7"},
		{Uint32Value(math.MaxUint32), KindInt32, kindProbe{Int32: -1, Uint32: math.MaxUint32}, "-1"},
		{Int64Value(math.MinInt64), KindInt64, kindProbe{Int64: math.MinInt64, Uint64: 1 << 63}, "-9223372036854775808"},
		{Uint64Value(math.MaxUint64), KindInt64, kindProbe{Int64: -1, Uint64: math.MaxUint64}, "-1"},
		{Int96Value(Int96{1, 2, 3}), KindInt96, kindProbe{Int96: Int96{1, 2, 3}}, "INT96(1,2,3)"},
		{FloatValue(float32(math.Copysign(0, -1))), KindFloat, kindProbe{Float: 0x80000000}, "-0"},
		{DoubleValue(math.Inf(1)), KindDouble, kindProbe{Double: 0x7ff0000000000000}, "+Inf"},
		{ByteArrayValue([]byte("abc")), KindByteArray, kindProbe{Bytes: []byte("abc")}, "abc"},
		{FixedLenByteArrayValue(fixed), KindFixedLenByteArray, kindProbe{Bytes: fixed}, "\x01\x02\x03\x04"},
	}
	for i, c := range cases {
		if c.v.Kind() != c.kind {
			t.Errorf("case %d: kind %s, want %s", i, c.v.Kind(), c.kind)
		}
		absent, null := i == 2, i < 2
		if c.v.Absent() != absent || c.v.IsNull() != null {
			t.Errorf("case %d: absent %v null %v", i, c.v.Absent(), c.v.IsNull())
		}
		if got := probeValue(c.v); !reflect.DeepEqual(got, c.want) {
			t.Errorf("case %d: %+v, want %+v", i, got, c.want)
		}
		if c.v.String() != c.str {
			t.Errorf("case %d: %q, want %q", i, c.v.String(), c.str)
		}
		clone := c.v.Clone()
		if !equalValue(clone, c.v) || clone.Absent() != c.v.Absent() {
			t.Errorf("case %d: clone %v differs", i, clone)
		}
	}
	// A clone owns its bytes; the constructor references them.
	b := []byte("xyz")
	v := ByteArrayValue(b)
	clone := v.Clone()
	b[0] = 'X'
	if string(v.Bytes()) != "Xyz" || string(clone.Bytes()) != "xyz" {
		t.Fatalf("constructor %q, clone %q", v.Bytes(), clone.Bytes())
	}
	if ByteArrayValue(nil).IsNull() || ByteArrayValue(nil).Kind() != KindByteArray {
		t.Fatal("an empty byte array is a value, not null")
	}
	for k := KindNone; k <= KindFixedLenByteArray; k++ {
		if s := k.String(); s == "" || s[0] == 'K' {
			t.Errorf("kind %d: %q", k, s)
		}
	}
	if Kind(42).String() != "Kind(42)" {
		t.Error(Kind(42).String())
	}
}

type everyKindRecord struct {
	Key     []byte           `parquet:"key"`
	Bool    bool             `parquet:"bool"`
	I32     int32            `parquet:"i32"`
	I64     int64            `parquet:"i64"`
	I96     deprecated.Int96 `parquet:"i96"`
	F32     float32          `parquet:"f32"`
	F64     float64          `parquet:"f64"`
	Bytes   []byte           `parquet:"bytes"`
	Fixed   [3]byte          `parquet:"fixed"`
	OptI64  *int64           `parquet:"opt_i64,optional"`
	OptText *string          `parquet:"opt_text,optional"`
}

type keyOnlyRecord struct {
	Key []byte `parquet:"key"`
}

// TestColumnViewEveryKind reads a column of every physical kind, a nullable
// column and a column the second part lacks through Batch.Column: each view
// reports its kind and presence, its typed accessor of the right kind gives
// the stored value and every other accessor its zero value, Value(i) gives
// the same borrowed value, nulls are null and not absent, and absent values
// are absent and not null.
func TestColumnViewEveryKind(t *testing.T) {
	var records []everyKindRecord
	for i := range 6 {
		r := everyKindRecord{Key: []byte{'a', byte('0' + i)}, Bool: i%2 == 1, I32: int32(-i), I64: int64(i) << 40,
			I96: deprecated.Int96{uint32(i), 7, math.MaxUint32}, F32: float32(i) + 0.5, F64: -float64(i),
			Bytes: bytes.Repeat([]byte{0xff}, i), Fixed: [3]byte{byte(i), 0, 0xff}}
		if i%3 != 0 {
			n, s := int64(i), "t"+string(rune('0'+i))
			r.OptI64, r.OptText = &n, &s
		}
		records = append(records, r)
	}
	d := open(t, write(t, testutil.Dataset{Parts: []testutil.Part{
		{Rows: records},
		{Rows: []keyOnlyRecord{{[]byte("b0")}, {[]byte("b1")}}},
	}}), Options{})
	columns := []string{"bool", "i32", "i64", "i96", "f32", "f64", "bytes", "fixed", "opt_i64", "opt_text"}
	kinds := []Kind{KindBoolean, KindInt32, KindInt64, KindInt96, KindFloat, KindDouble, KindByteArray, KindFixedLenByteArray, KindInt64, KindByteArray}
	want := func(r everyKindRecord, c int) kindProbe {
		switch c {
		case 0:
			return kindProbe{Boolean: r.Bool}
		case 1:
			return kindProbe{Int32: r.I32, Uint32: uint32(r.I32)}
		case 2:
			return kindProbe{Int64: r.I64, Uint64: uint64(r.I64)}
		case 3:
			return kindProbe{Int96: Int96(r.I96)}
		case 4:
			return kindProbe{Float: math.Float32bits(r.F32)}
		case 5:
			return kindProbe{Double: math.Float64bits(r.F64)}
		case 6:
			return kindProbe{Bytes: r.Bytes}
		case 7:
			return kindProbe{Bytes: r.Fixed[:]}
		case 8:
			if r.OptI64 == nil {
				return kindProbe{}
			}
			return kindProbe{Int64: *r.OptI64, Uint64: uint64(*r.OptI64)}
		default:
			if r.OptText == nil {
				return kindProbe{}
			}
			return kindProbe{Bytes: []byte(*r.OptText)}
		}
	}
	norm := func(p kindProbe) kindProbe { // nil and empty bytes are one value for the comparison
		if len(p.Bytes) == 0 {
			p.Bytes = nil
		}
		return p
	}
	n := 0
	err := d.RangeBatches(ctx, Range{Limit: 100, Columns: columns}, func(b *Batch) error {
		for c := range columns {
			col := b.Column(c)
			if col.Len() != b.Len() {
				t.Fatalf("column %d: %d values of %d", c, col.Len(), b.Len())
			}
			for i := range b.Len() {
				row := n + i
				v := col.Value(i)
				if row >= len(records) { // the second part lacks every column
					if col.Present() || col.Kind() != KindNone || !v.Absent() || v.IsNull() || col.IsNull(i) ||
						!reflect.DeepEqual(probeView(col, i), kindProbe{}) || v.Kind() != KindNone {
						t.Fatalf("row %d column %d: not absent", row, c)
					}
					continue
				}
				r := records[row]
				null := c == 8 && r.OptI64 == nil || c == 9 && r.OptText == nil
				if !col.Present() || col.Kind() != kinds[c] || v.Absent() || v.IsNull() != null || col.IsNull(i) != null {
					t.Fatalf("row %d column %d: present %v kind %s null %v", row, c, col.Present(), col.Kind(), col.IsNull(i))
				}
				if wantKind := kinds[c]; null && v.Kind() != KindNone || !null && v.Kind() != wantKind {
					t.Fatalf("row %d column %d: value kind %s", row, c, v.Kind())
				}
				w := norm(want(r, c))
				if got := norm(probeView(col, i)); !reflect.DeepEqual(got, w) {
					t.Fatalf("row %d column %d: view %+v, want %+v", row, c, got, w)
				}
				if got := norm(probeValue(v)); !reflect.DeepEqual(got, w) {
					t.Fatalf("row %d column %d: value %+v, want %+v", row, c, got, w)
				}
				if owned := b.Row(i).Values[c]; !equalValue(owned, v) {
					t.Fatalf("row %d column %d: owning %v, borrowed %v", row, c, owned, v)
				}
			}
		}
		n += b.Len()
		return nil
	})
	must(t, err)
	if n != len(records)+2 {
		t.Fatalf("%d rows", n)
	}
	// Index panics, as a slice's.
	must(t, d.RangeBatches(ctx, Range{Limit: 1, Columns: []string{"i64"}}, func(b *Batch) error {
		for _, i := range []int{-1, b.Len()} {
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("index %d did not panic", i)
					}
				}()
				_ = b.Column(0).Int64(i)
			}()
		}
		return nil
	}))
	var zero ColumnView
	if zero.Present() || zero.Len() != 0 || zero.Kind() != KindNone {
		t.Fatal("zero ColumnView")
	}
}

// TestLogicalRoundTrip: every logical annotation a footer can carry, with
// its parameters, converts to Logical and back unchanged, and prints as
// parquet-go prints it.
func TestLogicalRoundTrip(t *testing.T) {
	units := []format.TimeUnit{{Value: &format.MilliSeconds{}}, {Value: &format.MicroSeconds{}}, {Value: &format.NanoSeconds{}}}
	values := []format.LogicalTypeValue{
		&format.StringType{}, &format.MapType{}, &format.ListType{}, &format.EnumType{},
		&format.DecimalType{Precision: 38, Scale: 9}, &format.DateType{},
		&format.IntType{BitWidth: 8, IsSigned: true}, &format.IntType{BitWidth: 64},
		&format.NullType{}, &format.JsonType{}, &format.BsonType{}, &format.UUIDType{}, &format.Float16Type{},
		&format.VariantType{}, &format.VariantType{SpecificationVersion: 1},
		&format.GeometryType{}, &format.GeometryType{CRS: "EPSG:4326"},
		&format.GeographyType{}, &format.GeographyType{CRS: "OGC:CRS83", Algorithm: format.Karney},
	}
	for _, u := range units {
		for _, utc := range []bool{false, true} {
			values = append(values, &format.TimeType{IsAdjustedToUTC: utc, Unit: u}, &format.TimestampType{IsAdjustedToUTC: utc, Unit: u})
		}
	}
	seen := map[LogicalKind]bool{}
	for _, v := range values {
		l := logicalOf(v)
		seen[l.Kind] = true
		back := l.format()
		if back == nil || !reflect.DeepEqual(back.Value, v) {
			t.Errorf("%s: back %+v", v, back)
		}
		if l.String() != v.String() {
			t.Errorf("%s: printed %q", v, l.String())
		}
		if l.Kind.String() == "" {
			t.Errorf("%s: no kind name", v)
		}
	}
	for k := LogicalString; k <= LogicalGeography; k++ {
		if !seen[k] {
			t.Errorf("%s not covered", k)
		}
	}
	if (Logical{}).String() != "NONE" || (Logical{}).format() != nil || logicalOf(nil) != (Logical{}) {
		t.Error("no annotation")
	}
	for r, s := range map[Repetition]string{Required: "REQUIRED", Optional: "OPTIONAL", Repeated: "REPEATED"} {
		if r.String() != s || Repetition(r) != Repetition(format.FieldRepetitionType(r)) {
			t.Errorf("%d: %q", r, r.String())
		}
	}
}

// TestColumnsConvertedTypeExact: each legacy converted type normalizes to
// the exact Logical, parameters included, and a column's own types map
// one-for-one from the footer's physical type and repetition.
func TestColumnsConvertedTypeExact(t *testing.T) {
	cases := []struct {
		typ       format.Type
		converted deprecated.ConvertedType
		want      Logical
	}{
		{format.ByteArray, deprecated.UTF8, Logical{Kind: LogicalString}},
		{format.Int32, deprecated.TimeMillis, Logical{Kind: LogicalTime, Unit: Millis, AdjustedToUTC: true}},
		{format.Int64, deprecated.TimestampMicros, Logical{Kind: LogicalTimestamp, Unit: Micros, AdjustedToUTC: true}},
		{format.Int32, deprecated.Uint16, Logical{Kind: LogicalInt, BitWidth: 16}},
		{format.Int64, deprecated.Int64, Logical{Kind: LogicalInt, BitWidth: 64, Signed: true}},
		{format.FixedLenByteArray, deprecated.Decimal, Logical{Kind: LogicalDecimal, Precision: 18, Scale: 4}},
		{format.FixedLenByteArray, deprecated.Interval, Logical{}},
	}
	for _, c := range cases {
		se := format.SchemaElement{Name: "v", Type: thrift.New(c.typ), RepetitionType: thrift.New(format.Repeated),
			ConvertedType: thrift.New(c.converted), Precision: thrift.New[int32](18), Scale: thrift.New[int32](4)}
		if c.typ == format.FixedLenByteArray {
			se.TypeLength = thrift.New[int32](12)
		}
		col := columnOf(&se, "v")
		if col.Logical != c.want || col.Type.String() != c.typ.String() || col.Repetition != Repeated || !col.Key {
			t.Errorf("converted %d: %+v, want %+v", c.converted, col, c.want)
		}
		if c.typ == format.FixedLenByteArray && col.Length != 12 {
			t.Errorf("length %d", col.Length)
		}
	}
	group := format.SchemaElement{Name: "g", NumChildren: thrift.New[int32](1), RepetitionType: thrift.New(format.Optional)}
	if col := columnOf(&group, "key"); !col.Group || col.Type != KindNone || col.Repetition != Optional || col.String() != "g group OPTIONAL" {
		t.Errorf("group: %+v %q", col, col.String())
	}
}
