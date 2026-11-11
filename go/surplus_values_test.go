// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
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
			for _, reuse := range []bool{false, true} {
				chunk := testChunk(c.typ)
				if reuse {
					chunk.scratch = new(pageScratch)
					// Decode a larger valid page before the smaller exact/surplus page.
					count := 4
					if c.typ.Kind() == parquet.Boolean {
						count = 9
					}
					_, err := chunk.values(c.enc, c.excess, count)
					must(t, err)
				}
				values, err := chunk.values(c.enc, c.exact, 3)
				must(t, err)
				if len(values) != 3 {
					t.Fatalf("%d values", len(values))
				}
				if _, err := chunk.values(c.enc, c.excess, 3); RefusalCodeOf(err) != CodeInvalidParquet {
					t.Fatalf("a payload of 4 values decoded as 3: %v", err)
				}
				// Dictionaries use the allocating path, even during a reusable scan.
				if c.enc == format.Plain {
					if _, err := testChunk(c.typ).decodeValues(format.Plain, c.excess, 3); RefusalCodeOf(err) != CodeInvalidParquet {
						t.Fatalf("a dictionary of 4 values decoded as 3: %v", err)
					}
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
		if _, err := testChunk(parquet.BooleanType).values(format.Plain, make([]byte, size+1), n); RefusalCodeOf(err) != CodeInvalidParquet {
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
	required.scratch = new(pageScratch)
	if _, err := required.decode(page(2, make([]byte, 16)), make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	if _, err := required.decode(page(2, make([]byte, 24)), make([]byte, 24)); RefusalCodeOf(err) != CodeInvalidParquet {
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
	c.scratch = new(pageScratch)
	got, err := c.decode(page(3, optional(2)), optional(2))
	must(t, err)
	if len(got) != 3 || !got[1].IsNull() || got[0].IsNull() || got[2].IsNull() {
		t.Fatalf("got %v", got)
	}
	if _, err := c.decode(page(3, optional(3)), optional(3)); RefusalCodeOf(err) != CodeInvalidParquet {
		t.Fatalf("an optional page of 2 non-null values holding 3: %v", err)
	}
}

// TestSurplusRunValues: an RLE/bit-packed stream of dictionary indexes or
// definition levels may not hold more values than the page through a
// repeated run, nor a bit-packed run beyond the padding of its final group,
// as Java requires; a run of no values is stepped over anywhere, and
// decodes to nothing.
func TestSurplusRunValues(t *testing.T) {
	c := testChunk(parquet.Int64Type)
	one, err := c.decodeValues(format.Plain, make([]byte, 8), 1)
	must(t, err)
	c.dict, c.dictRead = parquet.Int64Type.NewDictionary(1, 1, one), true
	for name, data := range map[string][]byte{
		"a repeated run of 2 for 1 value": {1, 4, 0},
		"a run after the last value":      {1, 2, 0, 2, 0},
		"two bit-packed groups":           {1, 5, 0, 0},
	} {
		if _, err := c.values(format.RLEDictionary, data, 1); RefusalCodeOf(err) != CodeInvalidParquet {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, data := range map[string][]byte{
		"one value":                     {1, 2, 0},
		"a padded bit-packed group":     {1, 3, 0},
		"a leading empty run":           {1, 0, 1, 2, 0},
		"an empty bit-packed run after": {1, 2, 0, 1},
		"no index, an empty run":        {0, 0},
	} {
		n := 1
		if name == "no index, an empty run" {
			n = 0
		}
		if v, err := c.values(format.RLEDictionary, data, n); err != nil || len(v) != n {
			t.Errorf("%s: %d values, %v", name, len(v), err)
		}
	}
	// Definition levels of three rows, then an RLE run of four.
	page := func(levels []byte) []byte {
		out := binary.LittleEndian.AppendUint32(nil, uint32(len(levels)))
		return append(append(out, levels...), make([]byte, 24)...)
	}
	opt := testChunk(parquet.Int64Type)
	opt.col.optional = true
	h := &format.PageHeader{Type: format.DataPage}
	h.DataPageHeader = thrift.New(format.DataPageHeader{NumValues: 3, Encoding: format.Plain, DefinitionLevelEncoding: format.RLE})
	for levels, ok := range map[string]bool{"\x06\x01": true, "\x08\x01": false, "\x00\x01\x06\x01": true} {
		data := page([]byte(levels))
		h.UncompressedPageSize, h.CompressedPageSize = int32(len(data)), int32(len(data))
		if _, err := opt.decode(h, data); (err == nil) != ok {
			t.Errorf("levels %x: %v", levels, err)
		}
	}
}

// TestFixedLengthDeltaIsExact: a DELTA_BYTE_ARRAY page of a
// FIXED_LEN_BYTE_ARRAY column must decode to whole values: five bytes are
// not one value of four, though they round down to one.
func TestFixedLengthDeltaIsExact(t *testing.T) {
	encode := func(values ...string) []byte {
		var data []byte
		var offsets []uint32
		for _, v := range values {
			offsets = append(offsets, uint32(len(data)))
			data = append(data, v...)
		}
		out, err := parquet.DeltaByteArray.EncodeByteArray(nil, data, append(offsets, uint32(len(data))))
		must(t, err)
		return out
	}
	typ := parquet.FixedLenByteArrayType(4)
	if v, err := testChunk(typ).values(format.DeltaByteArray, encode("abcd"), 1); err != nil || len(v) != 1 {
		t.Fatalf("one value of four bytes: %d values, %v", len(v), err)
	}
	if _, err := testChunk(typ).values(format.DeltaByteArray, encode("abcde"), 1); RefusalCodeOf(err) != CodeInvalidParquet {
		t.Fatalf("five bytes as one value of four: %v", err)
	}
}

// TestZeroCRCIsChecked: a page header that carries a CRC of zero is checked
// against it, as one carrying any other CRC; a header without one is not.
func TestZeroCRCIsChecked(t *testing.T) {
	type header struct { // a page header's required fields, then its crc
		Type         int32  `thrift:"1,required"`
		Uncompressed int32  `thrift:"2,required"`
		Compressed   int32  `thrift:"3,required"`
		CRC          *int32 `thrift:"4,optional"`
	}
	data := []byte{7, 0, 0, 0, 0, 0, 0, 0}
	for _, crc := range []*int32{nil, ptr(int32(0)), ptr(int32(crc32.ChecksumIEEE(data)))} {
		raw, err := thrift.Marshal(new(thrift.CompactProtocol), &header{Uncompressed: 8, Compressed: 8, CRC: crc})
		must(t, err)
		var h format.PageHeader
		n, err := decodeBounded(raw, &h, 1<<20)
		must(t, err)
		_, err = testChunk(parquet.Int64Type).payload(&h, 0, int64(n), append(raw, data...))
		if wantFail := crc != nil && *crc == 0; (RefusalCodeOf(err) == CodeInvalidParquet) != wantFail || !wantFail && err != nil {
			t.Errorf("crc %v: %v", crc, err)
		}
	}
}

// A payload read can overwrite the scratch allocation containing a partial
// header read. CRC presence must be determined before retiring those bytes.
func TestZeroCRCWithReusedHeader(t *testing.T) {
	type header struct {
		Type         int32  `thrift:"1,required"`
		Uncompressed int32  `thrift:"2,required"`
		Compressed   int32  `thrift:"3,required"`
		CRC          *int32 `thrift:"4,optional"`
	}
	data := bytes.Repeat([]byte{7, 0, 0, 0, 0, 0, 0, 0}, 1024)
	for _, crc := range []*int32{nil, ptr(int32(0)), ptr(int32(crc32.ChecksumIEEE(data)))} {
		encoded, err := thrift.Marshal(new(thrift.CompactProtocol), &header{Uncompressed: int32(len(data)), Compressed: int32(len(data)), CRC: crc})
		must(t, err)
		var h format.PageHeader
		n, err := decodeBounded(encoded, &h, 1<<20)
		must(t, err)
		file := append(bytes.Clone(encoded), data...)
		for _, reuse := range []bool{false, true} {
			c := testChunk(parquet.Int64Type)
			c.p.file = bytes.NewReader(file)
			if reuse {
				c.scratch = &pageScratch{raw: make([]byte, len(file))}
			}
			raw := c.scratch.readBuffer(4096)
			copy(raw, file[:len(raw)])
			got, err := c.payload(&h, 0, int64(n), raw)
			wantFail := crc != nil && *crc == 0
			if (RefusalCodeOf(err) == CodeInvalidParquet) != wantFail || !wantFail && (err != nil || !bytes.Equal(got, data)) {
				t.Errorf("reuse=%t crc=%v: %v", reuse, crc, err)
			}
		}
	}
}
