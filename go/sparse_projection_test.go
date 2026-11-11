// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

// A delimiter listing selects alternate rows from one value page. The
// selection must decode that page once, not once per contiguous run.
func TestSparseListingDecodesValuePageOnce(t *testing.T) {
	var keys []string
	for i := range 100 {
		key := fmt.Sprintf("f%04d", i)
		keys = append(keys, key, key+"/child")
	}
	for _, indexed := range []bool{true, false} {
		t.Run(fmt.Sprint("indexed=", indexed), func(t *testing.T) {
			dir := write(t, parts(testutil.Part{PageBytes: 1 << 20}, keys))
			if !indexed {
				dropValueOffsets(t, dir)
			}
			d := open(t, dir, Options{})
			var st ReadStats
			entries, err := d.r.readList(ctx, List{Delimiter: '/', Limit: 1000, Columns: []string{"size"}}, &st)
			must(t, err)
			if len(entries) != 200 {
				t.Fatalf("got %d entries, want 200", len(entries))
			}
			for i, entry := range entries {
				if i%2 == 0 {
					if entry.Row == nil || string(entry.Row.Key) != keys[i] || entry.Row.Values[0].Int64() != int64(i) {
						t.Fatalf("row %d: %+v", i, entry)
					}
				} else if string(entry.CommonPrefix) != keys[i-1]+"/" {
					t.Fatalf("prefix %d: %+v", i, entry)
				}
			}
			if st.KeyPagesDecoded+st.ValuePagesDecoded != 2 {
				t.Fatalf("decoded %d pages, want one key page plus one value page", st.KeyPagesDecoded+st.ValuePagesDecoded)
			}
			t.Logf("200 entries, %d decoded pages", st.KeyPagesDecoded+st.ValuePagesDecoded)
		})
	}
}

func dropValueOffsets(t *testing.T, dir string) {
	t.Helper()
	rewrite(t, dir, 0, func(meta *format.FileMetaData) {
		for g := range meta.RowGroups {
			for c := 1; c < len(meta.RowGroups[g].Columns); c++ {
				col := &meta.RowGroups[g].Columns[c]
				col.OffsetIndexOffset, col.OffsetIndexLength = 0, 0
			}
		}
	})
}

// Select multiple rows within each of two distant pages in each row group.
// Nulls, dictionaries and byte ownership must survive reuse, while untouched
// intervening pages must not be decoded, even without a value OffsetIndex.
func TestSparseProjectionSkipsPages(t *testing.T) {
	type record struct {
		Key  []byte  `parquet:"key"`
		Name *string `parquet:"name,optional,dict"`
		Size int64   `parquet:"size"`
	}
	var records []record
	for i := range 400 {
		row := record{Key: []byte(fmt.Sprintf("k%04d", i)), Size: int64(i)}
		if i%3 != 2 {
			row.Name = ptr(fmt.Sprintf("name-%d", i%7))
		}
		records = append(records, row)
	}
	for _, indexed := range []bool{true, false} {
		t.Run(fmt.Sprint("indexed=", indexed), func(t *testing.T) {
			dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: records, GroupRows: 200, PageBytes: 64}}})
			original := verified(t, dir, Options{})
			p := original.r.parts[0]
			g0, g1 := groupRef{0, 0}, groupRef{0, 1}
			selected := []position{{g: g0, row: 0}, {g: g0, row: 2}, {g: g0, row: 198}, {g: g0, row: 199}, {g: g1, row: 0}, {g: g1, row: 2}, {g: g1, row: 198}, {g: g1, row: 199}}
			columns := []string{"name", "size", "missing"}
			var wantPages int64
			for g := range 2 {
				for _, name := range columns[:2] {
					index, err := p.loadOffsetIndex(ctx, g, p.leaves[name].index, nil)
					must(t, err)
					if len(index.PageLocations) < 4 {
						t.Fatalf("fixture %s has only %d pages", name, len(index.PageLocations))
					}
					touched := map[int]bool{}
					for _, at := range selected {
						if at.g.index != g {
							continue
						}
						page := 0
						for page+1 < len(index.PageLocations) && index.PageLocations[page+1].FirstRowIndex <= at.row {
							page++
						}
						touched[page] = true
					}
					wantPages += int64(len(touched))
				}
			}
			must(t, original.Close())
			if !indexed {
				dropValueOffsets(t, dir)
			}
			d := open(t, dir, Options{})
			must(t, d.r.checkParts(ctx, new(ReadStats)))
			out := make([]*Row, len(selected))
			for i := range out {
				out[i] = &Row{}
			}
			var st ReadStats
			must(t, d.r.project(ctx, out, selected, columns, &st))
			if st.KeyPagesDecoded+st.ValuePagesDecoded != wantPages {
				t.Fatalf("decoded %d pages, want %d distinct selected pages", st.KeyPagesDecoded+st.ValuePagesDecoded, wantPages)
			}
			for i, at := range selected {
				source := records[at.g.index*200+int(at.row)]
				values := out[i].Values
				if values[1].Int64() != source.Size || !values[2].Absent() {
					t.Fatalf("row %d values: %v", i, values)
				}
				if source.Name == nil {
					if !values[0].IsNull() {
						t.Fatalf("row %d: want null", i)
					}
				} else if string(values[0].Bytes()) != *source.Name {
					t.Fatalf("row %d: wrong name", i)
				}
			}
			// A caller can mutate returned bytes without changing the dictionary
			// or another result, and the next request gets independent values.
			out[0].Values[0].Bytes()[0] = 'X'
			again := []*Row{{}}
			must(t, d.r.project(ctx, again, selected[:1], columns, &ReadStats{}))
			if string(again[0].Values[0].Bytes()) != *records[0].Name {
				t.Fatal("projected bytes alias retained dictionary data")
			}
		})
	}
}
