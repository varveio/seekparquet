// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// plainByteArray PLAIN-encodes values: a 4-byte length before each.
func plainByteArray(values ...string) []byte {
	var out []byte
	for _, v := range values {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(v)))
		out = append(out, v...)
	}
	return out
}

// TestSurplusValuesAreRefused decodes payloads that hold more values than
// the count they are decoded for. The dependency's page would expose only
// the declared prefix, so full validation would count the declared values
// and certify a page holding others (CHECK-5): each is invalid_parquet.
// The exact payloads decode, and so do the padding bits of a PLAIN Boolean
// page's final byte.
func TestSurplusValuesAreRefused(t *testing.T) {
	encode := func(b []byte, err error) []byte {
		must(t, err)
		return b
	}
	int64s := func(n int) []int64 {
		out := make([]int64, n)
		for i := range out {
			out[i] = int64(i) + 1
		}
		return out
	}
	doubles := func(n int) []float64 {
		out := make([]float64, n)
		for i := range out {
			out[i] = float64(i) + 0.5
		}
		return out
	}
	cases := []struct {
		name          string
		typ           parquet.Type
		enc           format.Encoding
		exact, excess []byte // 3 values, and 4
	}{
		{"plain int32", parquet.Int32Type, format.Plain, make([]byte, 12), make([]byte, 16)},
		{"plain int64", parquet.Int64Type, format.Plain, encode(parquet.Plain.EncodeInt64(nil, int64s(3))), encode(parquet.Plain.EncodeInt64(nil, int64s(4)))},
		{"plain double", parquet.DoubleType, format.Plain, make([]byte, 24), make([]byte, 32)},
		{"plain int96", parquet.Int96Type, format.Plain, make([]byte, 36), make([]byte, 48)},
		{"plain fixed", parquet.FixedLenByteArrayType(5), format.Plain, make([]byte, 15), make([]byte, 20)},
		{"plain byte array", parquet.ByteArrayType, format.Plain, plainByteArray("a", "bb", ""), plainByteArray("a", "bb", "", "ccc")},
		{"byte stream split", parquet.DoubleType, format.ByteStreamSplit, encode(parquet.ByteStreamSplit.EncodeDouble(nil, doubles(3))), encode(parquet.ByteStreamSplit.EncodeDouble(nil, doubles(4)))},
		{"delta", parquet.Int64Type, format.DeltaBinaryPacked, encode(parquet.DeltaBinaryPacked.EncodeInt64(nil, int64s(3))), encode(parquet.DeltaBinaryPacked.EncodeInt64(nil, int64s(4)))},
		// Three Booleans fill one byte; a second byte holds eight more.
		{"plain boolean", parquet.BooleanType, format.Plain, []byte{0b101}, []byte{0b101, 0}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			values, err := testChunk(c.typ).values(c.enc, c.exact, 3)
			must(t, err)
			if len(values) != 3 {
				t.Fatalf("%d values", len(values))
			}
			if _, err := testChunk(c.typ).values(c.enc, c.excess, 3); CodeOf(err) != CodeInvalidParquet {
				t.Fatalf("a payload of 4 values decoded as 3: %v", err)
			}
			// A dictionary page is decoded by the same check.
			if c.enc == format.Plain {
				if _, err := testChunk(c.typ).decodeValues(format.Plain, c.excess, 3); CodeOf(err) != CodeInvalidParquet {
					t.Fatalf("a dictionary of 4 values decoded as 3: %v", err)
				}
			}
		})
	}
	// PLAIN Booleans: every count from 1 to 16 fills ceil(n/8) bytes, the
	// final one padded; one more byte is refused.
	for n := 1; n <= 16; n++ {
		size := (n + 7) / 8
		if _, err := testChunk(parquet.BooleanType).values(format.Plain, bytes.Repeat([]byte{0xff}, size), n); err != nil {
			t.Fatalf("%d Booleans in %d bytes: %v", n, size, err)
		}
		if _, err := testChunk(parquet.BooleanType).values(format.Plain, make([]byte, size+1), n); CodeOf(err) != CodeInvalidParquet {
			t.Fatalf("%d Booleans in %d bytes: %v", n, size+1, err)
		}
	}
}

// TestSurplusValuesInDataPages decodes whole data pages, as the validator's
// page walk does, whose payloads hold one value more than their headers
// declare: of a required column, and of an optional one whose definition
// levels declare fewer non-null values than the payload holds.
func TestSurplusValuesInDataPages(t *testing.T) {
	page := func(n int32, payload []byte) *format.PageHeader {
		h := &format.PageHeader{Type: format.DataPage, UncompressedPageSize: int32(len(payload)), CompressedPageSize: int32(len(payload))}
		h.DataPageHeader = thrift.New(format.DataPageHeader{NumValues: n, Encoding: format.Plain, DefinitionLevelEncoding: format.RLE, RepetitionLevelEncoding: format.RLE})
		return h
	}
	required := testChunk(parquet.Int64Type)
	if _, err := required.decode(page(2, make([]byte, 16)), make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	if _, err := required.decode(page(2, make([]byte, 24)), make([]byte, 24)); CodeOf(err) != CodeInvalidParquet {
		t.Fatalf("a required page of 3 values declaring 2: %v", err)
	}
	// Definition levels: one RLE run of three values, 1, 0, 1 bit-packed.
	levels := []byte{3, 0b101}
	optional := func(values int) []byte {
		out := binary.LittleEndian.AppendUint32(nil, uint32(len(levels)))
		return append(append(out, levels...), make([]byte, 8*values)...)
	}
	c := testChunk(parquet.Int64Type)
	c.col.optional = true
	got, err := c.decode(page(3, optional(2)), optional(2))
	must(t, err)
	if len(got) != 3 || !got[1].IsNull() || got[0].IsNull() || got[2].IsNull() {
		t.Fatalf("got %v", got)
	}
	if _, err := c.decode(page(3, optional(3)), optional(3)); CodeOf(err) != CodeInvalidParquet {
		t.Fatalf("an optional page of 2 non-null values holding 3: %v", err)
	}
}
