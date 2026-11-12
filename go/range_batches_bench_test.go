// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"fmt"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

type rangeBatchesBenchRecord struct {
	Key     []byte  `parquet:"key"`
	Size    int64   `parquet:"size"`
	Mtime   int64   `parquet:"mtime"`
	ETag    string  `parquet:"etag,plain"`
	Class   string  `parquet:"class,dict"`
	Version string  `parquet:"version,delta"`
	Owner   *string `parquet:"owner,optional"`
}

var rangeBatchesBenchColumns = []string{"size", "mtime", "etag", "class", "version", "owner"}

// BenchmarkRangeBatches compares a limited 13,013-row read of six projected
// columns, crossing a row group, through the owning Rows and through
// Scan touching every value. Warm: one Rows before timing loads the
// footers, key page indexes and OffsetIndexes both then reuse.
// Indicative only; generated data, not a real dataset.
func BenchmarkRangeBatches(b *testing.B) {
	const rows, limit = 200_000, 13_013
	records := make([]rangeBatchesBenchRecord, rows)
	classes := []string{"STANDARD", "STANDARD_IA", "GLACIER", "INTELLIGENT_TIERING"}
	for i := range records {
		r := &records[i]
		r.Key = fmt.Appendf(nil, "bucket/prefix-%03d/object-%08d.parquet", i/2000, i)
		r.Size, r.Mtime = int64(i*7919%1_000_003), 1_700_000_000+int64(i)
		r.ETag = fmt.Sprintf("%032x", uint64(i)*0x9e3779b97f4a7c15)
		r.Class, r.Version = classes[i%len(classes)], fmt.Sprintf("v%010d", i)
		if i%3 != 0 {
			owner := fmt.Sprintf("owner-%d", i%17)
			r.Owner = &owner
		}
	}
	dir := write(b, testutil.Dataset{Parts: []testutil.Part{
		{Rows: records[:rows/2], GroupRows: 40_000, PageBytes: 64 << 10},
		{Rows: records[rows/2:], GroupRows: 40_000, PageBytes: 64 << 10},
	}})
	d := open(b, dir, OpenOptions{})
	rng := Query{Selection: Selection{Span: Span{Start: records[33_333].Key}}, Limit: int64(limit), Columns: rangeBatchesBenchColumns}
	if _, err := d.Rows(ctx, rng, nil); err != nil {
		b.Fatal(err)
	}
	b.Run("Range", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			got, err := d.Rows(ctx, rng, nil)
			must(b, err)
			if len(got) != limit {
				b.Fatal(len(got))
			}
		}
	})
	b.Run("RangeBatches", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			n, sum := 0, 0
			must(b, d.Scan(ctx, rng, nil, func(batch *Batch) error {
				n += batch.Len()
				for c := range rangeBatchesBenchColumns {
					col := batch.Column(c)
					for i := range col.Len() {
						if v := col.Value(i); v.Kind() == KindByteArray {
							sum += len(v.Bytes())
						} else if !v.IsNull() {
							sum += int(v.Int64() & 1)
						}
					}
				}
				return nil
			}))
			if n != limit || sum == 0 {
				b.Fatal(n)
			}
		}
	})
}
