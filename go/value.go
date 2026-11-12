// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"unsafe"

	"github.com/parquet-go/parquet-go"
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
// A Value from a Batch or ValueBatch (ColumnView.Value) is borrowed: its bytes are valid
// only until the callback that received the batch returns. Values of an
// owning Row (Batch.Row, Dataset.Rows, Dataset.Get, Dataset.List) own their
// bytes; Clone makes an owning copy of any Value. The bytes Bytes returns
// are read-only either way.
type Value struct {
	// ptr is the bytes of a BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY or INT96, u64
	// their length or the bits of any other kind. Four plain fields keep a
	// Value in registers: it is copied freely and inlines.
	ptr    *byte
	u64    uint64
	kind   Kind
	absent bool
}

// valueOf returns the Value of a decoded parquet-go value, borrowing its
// bytes, or an absent value.
func valueOf(pv *parquet.Value, absent bool) Value {
	ptr, u64 := bitsOf(pv)
	return Value{ptr: ptr, u64: u64, kind: kindOf(pv.Kind()), absent: absent}
}

// bitsOf returns the representation of a parquet-go value that Value keeps:
// the pointer to the bytes of a BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY or INT96,
// nil for any other kind, and their length, or the bits of a BOOLEAN (0 or
// 1), INT32 (sign-extended), INT64, FLOAT or DOUBLE; zero for a null. It is
// the one place that relies on how parquet-go v0.32.0 represents a value:
// its first word is that pointer, and Uint64 returns those bits for every
// kind. TestValueRepresentation pins both for every kind, so that a
// dependency that changes them fails a test instead of corrupting values.
func bitsOf(pv *parquet.Value) (ptr *byte, u64 uint64) {
	return *(**byte)(unsafe.Pointer(pv)), pv.Uint64()
}

// Value constructors, for callers and tests that build rows. A constructor
// keeps a reference to the bytes it is given; Clone copies them.

// NullValue returns a null value.
func NullValue() Value { return Value{} }

// AbsentValue returns the value of a column the row's part lacks.
func AbsentValue() Value { return Value{absent: true} }

// BooleanValue returns a BOOLEAN value.
func BooleanValue(b bool) Value {
	v := Value{kind: KindBoolean}
	if b {
		v.u64 = 1
	}
	return v
}

// Int32Value returns an INT32 value.
func Int32Value(n int32) Value { return Value{u64: uint64(uint32(n)), kind: KindInt32} }

// Uint32Value returns an INT32 value holding n's bits.
func Uint32Value(n uint32) Value { return Value{u64: uint64(n), kind: KindInt32} }

// Int64Value returns an INT64 value.
func Int64Value(n int64) Value { return Value{u64: uint64(n), kind: KindInt64} }

// Uint64Value returns an INT64 value holding n's bits.
func Uint64Value(n uint64) Value { return Value{u64: n, kind: KindInt64} }

// Int96Value returns an INT96 value.
func Int96Value(n Int96) Value {
	b := make([]byte, 12)
	binary.LittleEndian.PutUint32(b[0:4], n[0])
	binary.LittleEndian.PutUint32(b[4:8], n[1])
	binary.LittleEndian.PutUint32(b[8:12], n[2])
	return Value{ptr: &b[0], u64: 12, kind: KindInt96}
}

// FloatValue returns a FLOAT value.
func FloatValue(f float32) Value { return Value{u64: uint64(math.Float32bits(f)), kind: KindFloat} }

// DoubleValue returns a DOUBLE value.
func DoubleValue(f float64) Value { return Value{u64: math.Float64bits(f), kind: KindDouble} }

// ByteArrayValue returns a BYTE_ARRAY value referencing b; nil is an empty
// value, not a null.
func ByteArrayValue(b []byte) Value {
	return Value{ptr: unsafe.SliceData(b), u64: uint64(len(b)), kind: KindByteArray}
}

// FixedLenByteArrayValue returns a FIXED_LEN_BYTE_ARRAY value referencing b.
func FixedLenByteArrayValue(b []byte) Value {
	return Value{ptr: unsafe.SliceData(b), u64: uint64(len(b)), kind: KindFixedLenByteArray}
}

// Kind returns the value's physical type, KindNone when it is null or
// absent.
func (v Value) Kind() Kind { return v.kind }

// IsNull reports whether the value is a null stored in a present column; an
// absent value is not null.
func (v Value) IsNull() bool { return v.kind == KindNone && !v.absent }

// Absent reports whether the row's part has no such column (COL-2).
func (v Value) Absent() bool { return v.absent }

// Boolean returns a BOOLEAN value, false for any other.
func (v Value) Boolean() bool { return v.kind == KindBoolean && v.u64 != 0 }

// Int32 returns an INT32 value, 0 for any other.
func (v Value) Int32() int32 {
	if v.kind != KindInt32 {
		return 0
	}
	return int32(v.u64)
}

// Uint32 returns an INT32 value as unsigned, 0 for any other.
func (v Value) Uint32() uint32 { return uint32(v.Int32()) }

// Int64 returns an INT64 value, 0 for any other.
func (v Value) Int64() int64 {
	if v.kind != KindInt64 {
		return 0
	}
	return int64(v.u64)
}

// Uint64 returns an INT64 value as unsigned, 0 for any other.
func (v Value) Uint64() uint64 { return uint64(v.Int64()) }

// Int96 returns an INT96 value, zero for any other.
func (v Value) Int96() Int96 {
	if v.kind != KindInt96 || v.u64 != 12 {
		return Int96{}
	}
	b := unsafe.Slice(v.ptr, 12)
	return Int96{binary.LittleEndian.Uint32(b[0:4]), binary.LittleEndian.Uint32(b[4:8]), binary.LittleEndian.Uint32(b[8:12])}
}

// Float returns a FLOAT value, 0 for any other.
func (v Value) Float() float32 {
	if v.kind != KindFloat {
		return 0
	}
	return math.Float32frombits(uint32(v.u64))
}

// Double returns a DOUBLE value, 0 for any other.
func (v Value) Double() float64 {
	if v.kind != KindDouble {
		return 0
	}
	return math.Float64frombits(v.u64)
}

// Bytes returns a BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY value's bytes,
// read-only, nil for any other value; an empty value's bytes may be nil
// too, so tell it from a null by IsNull.
func (v Value) Bytes() []byte {
	if v.kind != KindByteArray && v.kind != KindFixedLenByteArray {
		return nil
	}
	return unsafe.Slice(v.ptr, v.u64)
}

// Clone returns a copy of v that owns its bytes.
func (v Value) Clone() Value {
	switch {
	case v.ptr == nil:
	case v.u64 == 0:
		v.ptr = nil // an empty value keeps no reference to borrowed storage
	default:
		b := make([]byte, v.u64)
		copy(b, unsafe.Slice(v.ptr, v.u64))
		v.ptr = &b[0]
	}
	return v
}

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

// ColumnView is the borrowed values of one projection slot of a Batch or
// ValueBatch, in row order: a small value over the batch's decoded page,
// made without allocating. Its values, bytes and the view itself are valid
// only until the Scan or ScanValues callback that received the batch returns.
// Copying a view does not retain values; use Value(i).Clone().
//
// A column the row's part lacks (COL-2) is not Present: every value of it is
// absent, and none is null; it takes no storage. Len is the batch's Len either
// way. Every indexed method panics when i is outside [0, Len()). The typed
// accessors are for loops over a column of a known Kind: on a column of
// their kind they return the value, and the zero value for a null or an
// absent value; they never panic, but on a present column of another kind
// the numeric accessors return unspecified bits (Bytes returns no bytes, Int96
// zero), as they test no kind per value. Check Kind once per batch, or use
// Value, whose accessors return the zero value for any other kind. The zero
// ColumnView has no values and is not Present.
type ColumnView struct {
	values []parquet.Value
	kind   Kind
	// null is the parquet-go kind of a null value of the column, or
	// notNull, which no value has, for a column that is not present.
	null parquet.Kind
	// bytes is 1 for a BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY column, 0 for any
	// other: Bytes masks the length it reads by it, needing no kind test per
	// value (a numeric value's bits are not a length). A ColumnView stays
	// within four words, so that the compiler keeps it in registers.
	bytes uint8
}

// absentValues back every column that is not present: zero parquet-go
// values, null, never written, holding zero bits. A batch projecting such a
// column holds at most this many rows (batcher.walk), so a column a part
// lacks takes no storage however long the batches of other columns are.
var absentValues [4096]parquet.Value

// notNull is a parquet-go kind no value has: a null's is -1, a value's 0
// to 7.
const notNull parquet.Kind = -2

// presentView returns the view of a present column's values, of kind k.
func presentView(values []parquet.Value, k Kind) ColumnView {
	c := ColumnView{values: values, kind: k, null: parquet.Kind(-1)}
	if k == KindByteArray || k == KindFixedLenByteArray {
		c.bytes = 1
	}
	return c
}

// absentView returns the view of n values of a column the part lacks; n
// is at most len(absentValues).
func absentView(n int) ColumnView {
	return ColumnView{values: absentValues[:n:n], null: notNull}
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
func (c ColumnView) IsNull(i int) bool { return c.values[i].Kind() == c.null }

// Value returns value i, borrowed, without allocating.
func (c ColumnView) Value(i int) Value {
	pv := &c.values[i]
	ptr, u64 := bitsOf(pv)
	return Value{ptr: ptr, u64: u64, kind: kindOf(pv.Kind()), absent: c.null == notNull}
}

// The typed accessors read the decoded value in place, testing no kind per
// value: every value of a present column is of the column's kind or null,
// and a decoded null, like every value of an absent column, holds zero bits.
// Bytes masks the length by the column's kind instead: a numeric value has
// no byte pointer, and its bits are not a length.

// Boolean returns value i of a BOOLEAN column, false when null.
func (c ColumnView) Boolean(i int) bool { return c.values[i].Boolean() }

// Int32 returns value i of an INT32 column, 0 when null.
func (c ColumnView) Int32(i int) int32 { return c.values[i].Int32() }

// Uint32 returns value i of an INT32 column as unsigned, 0 when null.
func (c ColumnView) Uint32(i int) uint32 { return uint32(c.values[i].Int32()) }

// Int64 returns value i of an INT64 column, 0 when null.
func (c ColumnView) Int64(i int) int64 { return c.values[i].Int64() }

// Uint64 returns value i of an INT64 column as unsigned, 0 when null.
func (c ColumnView) Uint64(i int) uint64 { return uint64(c.values[i].Int64()) }

// Int96 returns value i of an INT96 column, zero when null.
func (c ColumnView) Int96(i int) Int96 { return c.Value(i).Int96() }

// Float returns value i of a FLOAT column, 0 when null.
func (c ColumnView) Float(i int) float32 { return c.values[i].Float() }

// Double returns value i of a DOUBLE column, 0 when null.
func (c ColumnView) Double(i int) float64 { return c.values[i].Double() }

// Bytes returns the borrowed, read-only bytes of value i of a BYTE_ARRAY or
// FIXED_LEN_BYTE_ARRAY column, nil when null, and none on a column of another kind.
func (c ColumnView) Bytes(i int) []byte {
	ptr, n := bitsOf(&c.values[i])
	return unsafe.Slice(ptr, n&-uint64(c.bytes))
}
