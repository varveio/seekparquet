// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// fuzzLimits are tight enough that a hostile file cannot make a fuzz run
// allocate much, so over-allocation shows as a failure of the run.
var fuzzLimits = Limits{
	FooterBytes:    1 << 20,
	PageIndexBytes: 1 << 20,
	MetadataMemory: 16 << 20,
	PageBytes:      1 << 20,
	PageValues:     1 << 16,
	IndexMemory:    16 << 20,
}

// seedParts writes small parts of different shapes and returns their bytes.
func seedParts(f *testing.F) [][]byte {
	var out [][]byte
	for _, d := range []testutil.Dataset{
		parts(testutil.Part{GroupRows: 3, PageBytes: 64}, []string{"a", "a/b", "b", "c/d", "e", "f"}),
		parts(testutil.Part{DataPageVersion: 1, IndexSizeLimit: 4}, []string{"\xff\xff\xff\xff\xff1", "\xff\xff\xff\xff\xff2"}),
		typedDataset(),
	} {
		dir := f.TempDir()
		if err := testutil.Write(dir, d); err != nil {
			f.Fatal(err)
		}
		for i := range d.Parts {
			raw, err := os.ReadFile(partFile(dir, i))
			if err != nil {
				f.Fatal(err)
			}
			out = append(out, raw)
		}
	}
	return out
}

// exercise opens a part from its bytes, verifying it on open, and reads
// all of it through a range, a resumed range and a listing, projecting
// every column. Any error is acceptable; a panic or an allocation beyond
// the limits is not.
func exercise(raw []byte) {
	r, err := openParts(ctx, []partInput{{name: "fuzz", file: bytes.NewReader(raw), size: int64(len(raw))}}, unique,
		Options{Limits: fuzzLimits, VerifyOnOpen: true})
	if err != nil {
		return
	}
	defer r.close()
	var columns []string
	for name := range r.parts[0].leaves {
		columns = append(columns, name)
	}
	_, _ = r.readRange(ctx, Range{Limit: 1000, Columns: columns}, nil)
	_, _ = r.readRange(ctx, Range{Cursor: &Cursor{Key: []byte("b"), Offset: 1}, Limit: 3, Columns: columns}, nil)
	_, _ = r.readList(ctx, List{Delimiter: '/', Limit: 1000, Columns: columns}, nil)
}

// FuzzPart mutates whole part files.
func FuzzPart(f *testing.F) {
	for _, raw := range seedParts(f) {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw []byte) { exercise(raw) })
}

// FuzzFooter replaces the footer of a valid part.
func FuzzFooter(f *testing.F) {
	base := seedParts(f)[0]
	n := binary.LittleEndian.Uint32(base[len(base)-8:])
	body := base[:len(base)-8-int(n)]
	f.Add(base[len(base)-8-int(n) : len(base)-8])
	f.Fuzz(func(t *testing.T, footer []byte) {
		raw := append(bytes.Clone(body), footer...)
		raw = binary.LittleEndian.AppendUint32(raw, uint32(len(footer)))
		exercise(append(raw, "PAR1"...))
	})
}

// FuzzKeyPage overwrites the key column chunk of a valid part, page
// headers included, keeping its length and so every offset.
func FuzzKeyPage(f *testing.F) {
	dir := f.TempDir()
	if err := testutil.Write(dir, parts(testutil.Part{PageBytes: 64}, []string{"a", "b", "c", "d", "e", "f", "g", "h"})); err != nil {
		f.Fatal(err)
	}
	base, err := os.ReadFile(partFile(dir, 0))
	if err != nil {
		f.Fatal(err)
	}
	meta, err := testutil.ReadFooter(partFile(dir, 0))
	if err != nil {
		f.Fatal(err)
	}
	chunk := meta.RowGroups[0].Columns[0].MetaData
	start := chunk.DataPageOffset
	if chunk.DictionaryPageOffset != 0 {
		start = chunk.DictionaryPageOffset
	}
	size := int(chunk.TotalCompressedSize)
	f.Add(base[start : int(start)+size])
	f.Fuzz(func(t *testing.T, page []byte) {
		raw := bytes.Clone(base)
		copy(raw[start:int(start)+size], page)
		exercise(raw)
	})
}

// FuzzPagePayload decodes fuzzed payloads as pages of each encoding and
// physical type: any error is acceptable, a panic or an allocation beyond
// the limits is not.
func FuzzPagePayload(f *testing.F) {
	f.Add(uint8(0), uint8(0), []byte{1, 0, 0, 0, 0, 0, 0, 0}, uint16(1))
	f.Add(uint8(2), uint8(1), []byte{0x80, 0x01, 0x04, 0x02, 0x00, 0x00, 0x00}, uint16(2))
	encodings := []format.Encoding{format.Plain, format.RLE, format.DeltaBinaryPacked, format.DeltaLengthByteArray, format.DeltaByteArray, format.ByteStreamSplit}
	types := []parquet.Type{parquet.BooleanType, parquet.Int32Type, parquet.Int64Type, parquet.DoubleType, parquet.ByteArrayType, parquet.FixedLenByteArrayType(4)}
	f.Fuzz(func(t *testing.T, enc, kind uint8, data []byte, n uint16) {
		c := testChunk(types[int(kind)%len(types)])
		c.p.r.limits = fuzzLimits.withDefaults()
		_, _ = c.values(encodings[int(enc)%len(encodings)], data, int(n))
	})
}
