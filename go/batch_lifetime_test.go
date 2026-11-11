// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

// Intercept the next physical read before it overwrites any storage. Retiring
// only the corresponding column page preserves other columns' still-live pages
// across sub-batches. No production hook, global state, or unsafe pointers.
type retiringSource struct {
	Source
	before func(string, int64)
}
type retiringFile struct {
	SourceFile
	path  string
	owner *retiringSource
}

func (s *retiringSource) Open(ctx context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	return retiringFile{f, path, s}, nil
}
func (f retiringFile) ReadAt(p []byte, off int64) (int, error) {
	if f.owner.before != nil {
		f.owner.before(f.path, off)
	}
	return f.SourceFile.ReadAt(p, off)
}

func TestScanBatchesPoisonRetiredStorage(t *testing.T) {
	// With and without fallback: dictionary bytes live for their whole chunk,
	// including while data-page scratch is reused for plain pages.
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("fallback=%t", fallback), func(t *testing.T) {
			records := transitionRecords(2048, fallback)
			dir := write(t, testutil.Dataset{Parts: []testutil.Part{
				{Rows: records[:1024], GroupRows: 512, PageBytes: 512, DictionaryMaxBytes: 1024, Compression: parquet.Compression(&zstd.Codec{})},
				{Rows: records[1024:], GroupRows: 512, PageBytes: 512, DictionaryMaxBytes: 1024, Compression: parquet.Compression(&zstd.Codec{})},
			}})
			src := &retiringSource{Source: dirOf(t, dir)}
			d, err := OpenDataset(ctx, src, Options{})
			must(t, err)
			defer d.Close()
			columns := []string{"dict", "plain", "null", "number", "key", "missing"}
			plan, err := d.PlanRanges(ctx, []Range{{}}, columns)
			must(t, err)
			type address struct {
				path string
				off  int64
			}
			type page struct{ column, chunk, index int }
			physical := map[address]page{}
			// Metadata here determines retirement only, never expected row values.
			for p := range 2 {
				for g := range 2 {
					for c, name := range columns[:4] {
						cp, err := d.r.parts[p].openColumn(ctx, g, name, new(ReadStats), nil)
						must(t, err)
						for i, loc := range cp.locs {
							physical[address{testutil.PartPath(p), loc.offset}] = page{c, p*2 + g, i}
						}
					}
				}
			}
			var kept []Row
			var cursors []*Cursor
			run := func(owning bool) {
				current := map[int]page{}
				borrowed := make([][][]parquet.Value, 4)
				dictionaryBytes := make([][]byte, 0)
				poisonedPages, poisonedChunks := 0, 0
				retirePage := func(c int) {
					if len(borrowed[c]) == 0 {
						return
					}
					for _, values := range borrowed[c] {
						for i, v := range values {
							// Dictionary byte aliases can be shared by future pages of the chunk.
							if c == 1 && !v.IsNull() {
								poisonBytes(v.ByteArray())
							}
							values[i] = parquet.Int64Value(-12345)
						}
					}
					borrowed[c] = nil
					poisonedPages++
				}
				retireDictionary := func() {
					for _, v := range dictionaryBytes {
						poisonBytes(v)
					}
					dictionaryBytes = nil
					poisonedChunks++
				}
				src.before = func(path string, off int64) {
					next, ok := physical[address{path, off}]
					if !ok {
						return
					}
					old, seen := current[next.column]
					if seen && old != next {
						retirePage(next.column)
						if next.column == 0 && old.chunk != next.chunk {
							retireDictionary()
						}
					}
					current[next.column] = next
				}
				start := len(kept)
				if !owning {
					must(t, plan.ScanBatches(ctx, nil, func(b *Batch) error {
						for c := range 4 {
							v, _ := rawColumn(b, c)
							borrowed[c] = append(borrowed[c], v)
							if c == 0 {
								for _, value := range v {
									if !value.IsNull() {
										dictionaryBytes = append(dictionaryBytes, value.ByteArray())
									}
								}
							}
						}
						for i := 0; i < b.Len(); i++ {
							kept = append(kept, b.Row(i))
							cursors = append(cursors, b.Cursor(i))
						}
						return nil
					}))
				} else {
					// The Scan adapter also retains every owning row across natural
					// page, dictionary, group and part reuse in its independent scan.
					src.before = nil
					must(t, plan.Scan(ctx, nil, func(row Row) error { kept = append(kept, row); cursors = append(cursors, row.Cursor()); return nil }))
				}
				src.before = nil
				for c := range 4 {
					retirePage(c)
				}
				retireDictionary()
				if !owning && (poisonedPages < 100 || poisonedChunks != 4) {
					t.Fatalf("insufficient retirement: pages=%d chunks=%d", poisonedPages, poisonedChunks)
				}
				for i, row := range kept[start:] {
					w := records[i]
					if !bytes.Equal(row.Key, w.Key) || row.Offset != 0 || string(row.Values[1].Bytes()) != w.Plain || !row.Values[2].IsNull() || row.Values[3].Int64() != w.Number || !bytes.Equal(row.Values[4].Bytes(), w.Key) || !row.Values[5].Absent() {
						t.Fatal("poisoned owning row", i)
					}
					if w.Dict == nil {
						if !row.Values[0].IsNull() {
							t.Fatal("null", i)
						}
					} else if string(row.Values[0].Bytes()) != *w.Dict {
						t.Fatal("poisoned dictionary", i)
					}
				}
			}
			run(false)
			run(true)
			for i, c := range cursors {
				if !bytes.Equal(c.Key, records[i%len(records)].Key) || c.Offset != 0 {
					t.Fatal("poisoned cursor", i)
				}
			}
		})
	}
}
