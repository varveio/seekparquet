// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"crypto/sha256"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

type reuseRecord struct {
	Key   []byte  `parquet:"key,delta"`
	Size  int64   `parquet:"size,delta"`
	ETag  string  `parquet:"etag,delta"`
	Class *string `parquet:"class,optional,dict"`
}

func reuseRecords(n int) []reuseRecord {
	rows := make([]reuseRecord, n)
	for i := range rows {
		rows[i] = reuseRecord{Key: []byte(fmt.Sprintf("inventory/synthetic/2026/11/object-%08d", i)), Size: int64(i), ETag: fmt.Sprintf("%032x", i)}
		if i%11 != 0 {
			rows[i].Class = ptr(fmt.Sprintf("class-%d", i%4))
		}
	}
	return rows
}

// BenchmarkScanBufferReuse measures whole owning requests, including all workers'
// allocations. Fixture generation and plan/split construction are outside the
// measured region. Use a fixed GOMAXPROCS; AllocsPerRun would force it to one.
func BenchmarkScanBufferReuse(b *testing.B) {
	const n = 32768
	dir := write(b, testutil.Dataset{Parts: []testutil.Part{{Rows: reuseRecords(n), GroupRows: 4096, PageBytes: 4096, Compression: parquet.Compression(&zstd.Codec{})}}})
	raw, err := os.ReadFile(partFile(dir, 0))
	must(b, err)
	b.Logf("fixture SHA-256 %x, %d bytes", sha256.Sum256(raw), len(raw))
	footer, err := testutil.ReadFooter(partFile(dir, 0))
	must(b, err)
	if len(footer.RowGroups) != 8 {
		b.Fatal("unexpected row-group count")
	}
	for _, column := range footer.RowGroups[0].Columns {
		b.Logf("first group: %v codec=%s encodings=%v", column.MetaData.PathInSchema, column.MetaData.Codec, column.MetaData.Encoding)
	}
	d := open(b, dir, Options{})
	columns := []string{"size", "etag", "class"}
	plan, err := d.PlanRanges(ctx, []Range{{}}, columns)
	must(b, err)
	var st ReadStats
	must(b, plan.Scan(ctx, &st, func(Row) error { return nil }))
	b.Logf("warm scan stats: %+v", st)
	for _, workers := range []int{1, 2} {
		shards, err := plan.Split(ctx, workers)
		must(b, err)
		b.Run(fmt.Sprintf("Scan/workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var wg sync.WaitGroup
				errs := make([]error, len(shards))
				for i, shard := range shards {
					wg.Go(func() { errs[i] = shard.Scan(ctx, nil, func(Row) error { return nil }) })
				}
				wg.Wait()
				for _, err := range errs {
					must(b, err)
				}
			}
			b.ReportMetric(n, "rows/op")
		})
	}
	b.Run("Range", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rows, err := d.Range(ctx, Range{Limit: n, Columns: columns})
			must(b, err)
			if len(rows) != n {
				b.Fatal(len(rows))
			}
		}
		b.ReportMetric(n, "rows/op")
	})
	b.Run("List", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rows, err := d.List(ctx, List{Limit: n, Columns: columns})
			must(b, err)
			if len(rows) != n {
				b.Fatal(len(rows))
			}
		}
		b.ReportMetric(n, "rows/op")
	})
}
