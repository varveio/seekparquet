// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"

	"github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/internal/testutil"
	"github.com/varveio/seekparquet/go/seekparquettest"
)

// fuzzLimits are tight enough that a hostile file cannot make a fuzz run
// allocate much, so over-allocation shows as a failure of the run.
var fuzzLimits = seekparquet.Limits{
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
	for _, d := range []seekparquettest.Dataset{
		parts(seekparquettest.Part{GroupRows: 3, PageBytes: 64}, []string{"a", "a/b", "b", "c/d", "e", "f"}),
		parts(seekparquettest.Part{DataPageVersion: 1, IndexSizeLimit: 4}, []string{"\xff\xff\xff\xff\xff1", "\xff\xff\xff\xff\xff2"}),
		typedDataset(),
	} {
		dir := f.TempDir()
		if err := seekparquettest.Write(dir, d); err != nil {
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
// all of it through a range and a listing. Any error is acceptable; a
// panic or an allocation beyond the limits is not.
func exercise(raw []byte) {
	r, err := seekparquet.OpenParts(ctx, []seekparquet.PartSource{{Name: "fuzz", File: bytes.NewReader(raw), Size: int64(len(raw))}},
		seekparquet.PartsOptions{Options: seekparquet.Options{Limits: fuzzLimits, VerifyOnOpen: true}})
	if err != nil {
		return
	}
	defer r.Close()
	columns := append(r.Columns(), seekparquet.KeyColumn)
	_, _ = r.Range(ctx, seekparquet.Range{Limit: 1000, Columns: columns}, nil)
	_, _ = r.Range(ctx, seekparquet.Range{Start: []byte("b"), Limit: 3, Columns: columns}, nil)
	_, _ = r.List(ctx, seekparquet.List{Delimiter: '/', Limit: 1000, Columns: columns}, nil)
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

// FuzzKeyPage overwrites the first key page of a valid part, header
// included, keeping its length and so every offset.
func FuzzKeyPage(f *testing.F) {
	dir := f.TempDir()
	if err := seekparquettest.Write(dir, parts(seekparquettest.Part{PageBytes: 64}, []string{"a", "b", "c", "d", "e", "f", "g", "h"})); err != nil {
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
