// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
)

// Kind is a Parquet physical type: of a projected value, of a batch's
// column, or of a schema's column (Column.Type). KindNone, the zero Kind,
// is no physical type: the kind of a null or absent value, and of a group
// column.
type Kind int8

// The physical types.
const (
	KindNone Kind = iota
	KindBoolean
	KindInt32
	KindInt64
	KindInt96
	KindFloat
	KindDouble
	KindByteArray
	KindFixedLenByteArray
)

// String returns the Parquet name of the kind, such as "INT64", or "NONE".
func (k Kind) String() string {
	switch k {
	case KindNone:
		return "NONE"
	case KindBoolean:
		return "BOOLEAN"
	case KindInt32:
		return "INT32"
	case KindInt64:
		return "INT64"
	case KindInt96:
		return "INT96"
	case KindFloat:
		return "FLOAT"
	case KindDouble:
		return "DOUBLE"
	case KindByteArray:
		return "BYTE_ARRAY"
	case KindFixedLenByteArray:
		return "FIXED_LEN_BYTE_ARRAY"
	}
	return "Kind(" + strconv.Itoa(int(k)) + ")"
}

// kindOf returns the Kind of a parquet-go kind: parquet-go numbers the
// physical types from 0 and reports a null value's kind as -1.
func kindOf(k parquet.Kind) Kind { return Kind(k + 1) }

// Int96 is a legacy INT96 value: three little-endian 32-bit words, the
// least significant first, as Parquet stores them.
type Int96 [3]uint32

// Value is one projected value, in exactly one of three states: absent,
// when the row's part has no such column (COL-2; Absent is true); null, a
// null stored in a present column (IsNull is true); or a physical Parquet
// value of its column's kind (Kind is not KindNone). An absent value is not
// null. The zero Value is null.
//
// Each typed accessor returns the value when its Kind is the accessor's
// physical type, and otherwise (null, absent, or another kind) the zero
// value of its result: it never panics, so a caller may check Kind before
// or after. Uint32 reads an INT32 and Uint64 an INT64 as unsigned; Bytes
// reads BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY. Floating-point values keep
// their exact bits.
//
// A Value from a Batch (ColumnView.Value) is borrowed: its bytes are valid
// only until the callback that received the batch returns. Values of an
// owning Row (Batch.Row, Dataset.Rows, Dataset.Get, Dataset.List) own their
// bytes; Clone makes an owning copy of any Value. The bytes Bytes returns
// are read-only either way.
type Value struct {
	v      parquet.Value
	absent bool
}

// Value constructors, for callers and tests that build rows. A constructor
// keeps a reference to the bytes it is given; Clone copies them.

// NullValue returns a null value.
func NullValue() Value { return Value{} }

// AbsentValue returns the value of a column the row's part lacks.
func AbsentValue() Value { return Value{absent: true} }

// BooleanValue returns a BOOLEAN value.
func BooleanValue(b bool) Value { return Value{v: parquet.BooleanValue(b)} }

// Int32Value returns an INT32 value.
func Int32Value(n int32) Value { return Value{v: parquet.Int32Value(n)} }

// Uint32Value returns an INT32 value holding n's bits.
func Uint32Value(n uint32) Value { return Value{v: parquet.Int32Value(int32(n))} }

// Int64Value returns an INT64 value.
func Int64Value(n int64) Value { return Value{v: parquet.Int64Value(n)} }

// Uint64Value returns an INT64 value holding n's bits.
func Uint64Value(n uint64) Value { return Value{v: parquet.Int64Value(int64(n))} }

// Int96Value returns an INT96 value.
func Int96Value(n Int96) Value { return Value{v: parquet.Int96Value(deprecated.Int96(n))} }

// FloatValue returns a FLOAT value.
func FloatValue(f float32) Value { return Value{v: parquet.FloatValue(f)} }

// DoubleValue returns a DOUBLE value.
func DoubleValue(f float64) Value { return Value{v: parquet.DoubleValue(f)} }

// ByteArrayValue returns a BYTE_ARRAY value referencing b; nil is an empty
// value, not a null.
func ByteArrayValue(b []byte) Value { return Value{v: parquet.ByteArrayValue(b)} }

// FixedLenByteArrayValue returns a FIXED_LEN_BYTE_ARRAY value referencing b.
func FixedLenByteArrayValue(b []byte) Value { return Value{v: parquet.FixedLenByteArrayValue(b)} }

// Kind returns the value's physical type, KindNone when it is null or
// absent.
func (v Value) Kind() Kind { return kindOf(v.v.Kind()) }

// IsNull reports whether the value is a null stored in a present column; an
// absent value is not null.
func (v Value) IsNull() bool { return !v.absent && v.v.IsNull() }

// Absent reports whether the row's part has no such column (COL-2).
func (v Value) Absent() bool { return v.absent }

// Boolean returns a BOOLEAN value, false for any other.
func (v Value) Boolean() bool { return booleanOf(&v.v) }

// Int32 returns an INT32 value, 0 for any other.
func (v Value) Int32() int32 { return int32Of(&v.v) }

// Uint32 returns an INT32 value as unsigned, 0 for any other.
func (v Value) Uint32() uint32 { return uint32(int32Of(&v.v)) }

// Int64 returns an INT64 value, 0 for any other.
func (v Value) Int64() int64 { return int64Of(&v.v) }

// Uint64 returns an INT64 value as unsigned, 0 for any other.
func (v Value) Uint64() uint64 { return uint64(int64Of(&v.v)) }

// Int96 returns an INT96 value, zero for any other.
func (v Value) Int96() Int96 { return int96Of(&v.v) }

// Float returns a FLOAT value, 0 for any other.
func (v Value) Float() float32 { return floatOf(&v.v) }

// Double returns a DOUBLE value, 0 for any other.
func (v Value) Double() float64 { return doubleOf(&v.v) }

// Bytes returns a BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY value's bytes,
// read-only, nil for any other value; an empty value's bytes may be nil
// too, so tell it from a null by IsNull.
func (v Value) Bytes() []byte { return bytesOf(&v.v) }

// Clone returns a copy of v that owns its bytes.
// (parquet-go copies byte arrays; an INT96 already holds twelve bytes of
// its own, never written after it is made, whether decoded or constructed).
func (v Value) Clone() Value { return Value{v: v.v.Clone(), absent: v.absent} }

// String formats the value for people: "absent", "null", or the value, with
// bytes as text.
func (v Value) String() string {
	switch {
	case v.absent:
		return "absent"
	case v.IsNull():
		return "null"
	}
	switch v.Kind() {
	case KindBoolean:
		return strconv.FormatBool(v.Boolean())
	case KindInt32:
		return strconv.FormatInt(int64(v.Int32()), 10)
	case KindInt64:
		return strconv.FormatInt(v.Int64(), 10)
	case KindInt96:
		n := v.Int96()
		return fmt.Sprintf("INT96(%d,%d,%d)", n[0], n[1], n[2])
	case KindFloat:
		return strconv.FormatFloat(float64(v.Float()), 'g', -1, 32)
	case KindDouble:
		return strconv.FormatFloat(v.Double(), 'g', -1, 64)
	}
	return string(v.Bytes())
}

// The typed accessors, shared by Value and ColumnView. Each tests the kind
// and reads the parquet-go value in place.

func booleanOf(v *parquet.Value) bool { return v.Kind() == parquet.Boolean && v.Boolean() }

func int32Of(v *parquet.Value) int32 {
	if v.Kind() != parquet.Int32 {
		return 0
	}
	return v.Int32()
}

func int64Of(v *parquet.Value) int64 {
	if v.Kind() != parquet.Int64 {
		return 0
	}
	return v.Int64()
}

func floatOf(v *parquet.Value) float32 {
	if v.Kind() != parquet.Float {
		return 0
	}
	return v.Float()
}

func doubleOf(v *parquet.Value) float64 {
	if v.Kind() != parquet.Double {
		return 0
	}
	return v.Double()
}

func bytesOf(v *parquet.Value) []byte {
	if k := v.Kind(); k != parquet.ByteArray && k != parquet.FixedLenByteArray {
		return nil
	}
	return v.ByteArray()
}

func int96Of(v *parquet.Value) Int96 {
	if v.Kind() != parquet.Int96 {
		return Int96{}
	}
	b := v.ByteArray()
	if len(b) != 12 {
		return Int96{}
	}
	return Int96{binary.LittleEndian.Uint32(b[0:4]), binary.LittleEndian.Uint32(b[4:8]), binary.LittleEndian.Uint32(b[8:12])}
}

// ColumnView is the borrowed values of one projection slot of a Batch, in
// row order: a small value over the batch's decoded page, copied freely and
// made without allocating. Its values, bytes and the view itself are valid
// only until the callback that received the batch returns.
//
// A column the row's part lacks (COL-2) is not Present: every value of it is
// absent, and none is null. Len is the batch's Len either way. Every indexed
// method panics when i is outside [0, Len()); the typed accessors otherwise
// follow Value's: the value of their kind, else the zero value. The zero
// ColumnView has no values and is not Present.
type ColumnView struct {
	values []parquet.Value
	kind   Kind
}

// Present reports whether the row's part has the column; when it does not,
// every value is absent (COL-2).
func (c ColumnView) Present() bool { return c.kind != KindNone }

// Kind returns the column's physical type in the batch's part, KindNone
// when it is not Present.
func (c ColumnView) Kind() Kind { return c.kind }

// Len returns the number of values: the batch's Len.
func (c ColumnView) Len() int { return len(c.values) }

// IsNull reports whether value i is a null stored in the column; the values
// of a column that is not Present are absent, not null.
func (c ColumnView) IsNull(i int) bool { return c.values[i].IsNull() && c.kind != KindNone }

// Value returns value i, borrowed, without allocating.
func (c ColumnView) Value(i int) Value { return Value{v: c.values[i], absent: c.kind == KindNone} }

// Boolean returns value i of a BOOLEAN column, false when null.
func (c ColumnView) Boolean(i int) bool { return booleanOf(&c.values[i]) }

// Int32 returns value i of an INT32 column, 0 when null.
func (c ColumnView) Int32(i int) int32 { return int32Of(&c.values[i]) }

// Uint32 returns value i of an INT32 column as unsigned, 0 when null.
func (c ColumnView) Uint32(i int) uint32 { return uint32(int32Of(&c.values[i])) }

// Int64 returns value i of an INT64 column, 0 when null.
func (c ColumnView) Int64(i int) int64 { return int64Of(&c.values[i]) }

// Uint64 returns value i of an INT64 column as unsigned, 0 when null.
func (c ColumnView) Uint64(i int) uint64 { return uint64(int64Of(&c.values[i])) }

// Int96 returns value i of an INT96 column, zero when null.
func (c ColumnView) Int96(i int) Int96 { return int96Of(&c.values[i]) }

// Float returns value i of a FLOAT column, 0 when null.
func (c ColumnView) Float(i int) float32 { return floatOf(&c.values[i]) }

// Double returns value i of a DOUBLE column, 0 when null.
func (c ColumnView) Double(i int) float64 { return doubleOf(&c.values[i]) }

// Bytes returns the borrowed, read-only bytes of value i of a BYTE_ARRAY or
// FIXED_LEN_BYTE_ARRAY column, nil when null.
func (c ColumnView) Bytes(i int) []byte { return bytesOf(&c.values[i]) }
