// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"
)

// crawlRows yields a listing shaped like Common Crawl's: 4,000,000 sorted keys of
// 101 to 106 bytes sharing long prefixes, such as
// crawl-data/CC-MAIN-2024-10/segments/1707947470000.17/warc/CC-MAIN-20240220211055-20240220235951-00042.warc.gz,
// with a size, a last-modified time and a storage class.
type crawlRows struct{ at int }

const (
	crawlFiles    = 4000
	crawlKinds    = 5
	crawlSegments = 100
	crawlRowCount = 2 * crawlSegments * crawlKinds * crawlFiles
)

var crawlKindNames = [crawlKinds]string{"crawldiagnostics", "robotstxt", "warc", "wat", "wet"}

func (r *crawlRows) ReadRows(rows []parquet.Row) (int, error) {
	n := 0
	for ; n < len(rows) && r.at < crawlRowCount; n, r.at = n+1, r.at+1 {
		i := r.at
		file, kind := i%crawlFiles, i/crawlFiles%crawlKinds
		segment, crawl := i/(crawlFiles*crawlKinds)%crawlSegments, i/(crawlFiles*crawlKinds*crawlSegments)
		start := 20240220211055 + int64(file)*37
		key := fmt.Appendf(nil, "crawl-data/CC-MAIN-2024-%02d/segments/1707947470000.%02d/%s/CC-MAIN-%d-%d-%05d.%s.gz",
			10+crawl, segment, crawlKindNames[kind], start, start+24896, file, crawlKindNames[kind])
		class := "STANDARD"
		if i%97 == 0 {
			class = "INTELLIGENT_TIERING"
		}
		rows[n] = parquet.Row{
			parquet.ByteArrayValue(key).Level(0, 0, 0),
			parquet.Int64Value(int64(i)*7919%1_000_000_007).Level(0, 1, 1),
			parquet.Int64Value(1_708_000_000_000_000+int64(i)*1_000_003).Level(0, 1, 2),
			parquet.ByteArrayValue([]byte(class)).Level(0, 1, 3),
		}
	}
	if r.at == crawlRowCount {
		return n, io.EOF
	}
	return n, nil
}

// crawlDataset writes the crawl listing as the module's part writer does
// (1,024-row v2 pages, ZSTD) with DELTA_BYTE_ARRAY keys, in row groups of
// 320,000 rows (as Common Crawl's 58M-object listing has about 313,000) and
// parts of about 5 MiB. SEEKPARQUET_SCAN_BENCH_DIR keeps it between runs.
func crawlDataset(b *testing.B) string {
	root := os.Getenv("SEEKPARQUET_SCAN_BENCH_DIR")
	if root == "" {
		root = b.TempDir()
	}
	dir := filepath.Join(root, "crawl-v2")
	if _, err := os.Stat(dir); err == nil {
		return dir
	}
	schema := parquet.NewSchema("listing", parquet.Group{
		"key":           parquet.Encoded(parquet.String(), &parquet.DeltaByteArray),
		"size":          parquet.Optional(parquet.Encoded(parquet.Leaf(parquet.Int64Type), &parquet.DeltaBinaryPacked)),
		"last_modified": parquet.Optional(parquet.Encoded(parquet.Timestamp(parquet.Microsecond), &parquet.DeltaBinaryPacked)),
		"storage_class": parquet.Optional(parquet.String()),
	})
	opt := local.DatasetOptions{
		Part:      write.PartOptions{Key: seekparquet.Key{Column: "key", Unique: true}, SchemaID: "bench/crawl/v1", RowGroupRows: 320_000},
		PartBytes: 5 << 20,
	}
	if err := local.WriteDir(context.Background(), dir, schema, &crawlRows{}, opt); err != nil {
		b.Fatal(err)
	}
	return dir
}

// BenchmarkScanBatchesLongKeys scans the whole crawl listing through
// RangePlan.ScanBatches (Workers shards from Split) and Dataset.RangeBatches,
// with the overview projection or keys alone, touching every key and value.
// The plan, its shards and one warm scan are outside the measured region;
// allocations count every worker. Reports ns, B and allocations per row.
// Indicative only; generated data, not a real dataset.
func BenchmarkScanBatchesLongKeys(b *testing.B) {
	ctx := context.Background()
	d, err := seekparquet.OpenDir(ctx, crawlDataset(b), seekparquet.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	touch := func(sum *int, batch *seekparquet.Batch, columns int) {
		for i := range batch.Len() {
			k := batch.Key(i)
			*sum += len(k) + int(k[len(k)-1])
		}
		for c := range columns {
			col := batch.Column(c)
			for i := range col.Len() {
				if v := col.Value(i); v.Kind() == seekparquet.KindByteArray {
					*sum += len(v.Bytes())
				} else if !v.IsNull() {
					*sum += int(v.Int64() & 1)
				}
			}
		}
	}
	measure := func(b *testing.B, scan func() (rows int, err error)) {
		if _, err := scan(); err != nil { // warm: footers, page indexes, OffsetIndexes
			b.Fatal(err)
		}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		var total int
		b.ResetTimer()
		for b.Loop() {
			rows, err := scan()
			if err != nil {
				b.Fatal(err)
			}
			if rows != crawlRowCount {
				b.Fatalf("scanned %d rows, want %d", rows, crawlRowCount)
			}
			total += rows
		}
		b.StopTimer()
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(total), "ns/row")
		b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/float64(total), "B/row")
		b.ReportMetric(float64(after.Mallocs-before.Mallocs)/float64(total), "allocs/row")
	}
	for _, columns := range [][]string{{"size", "last_modified", "storage_class"}, nil} {
		projection := "overview"
		if columns == nil {
			projection = "keys"
		}
		plan, err := d.PlanRanges(ctx, []seekparquet.Range{{}}, columns)
		if err != nil {
			b.Fatal(err)
		}
		for _, workers := range []int{1, 4} {
			shards, err := plan.Split(ctx, workers)
			if err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("ScanBatches/%s/workers=%d", projection, workers), func(b *testing.B) {
				measure(b, func() (int, error) {
					var wg sync.WaitGroup
					rows, sums, errs := make([]int, len(shards)), make([]int, len(shards)), make([]error, len(shards))
					for i, shard := range shards {
						wg.Go(func() {
							errs[i] = shard.ScanBatches(ctx, nil, func(batch *seekparquet.Batch) error {
								rows[i] += batch.Len()
								touch(&sums[i], batch, len(columns))
								return nil
							})
						})
					}
					wg.Wait()
					n := 0
					for i := range shards {
						if errs[i] != nil {
							return 0, errs[i]
						}
						n += rows[i]
					}
					return n, nil
				})
			})
		}
		b.Run(fmt.Sprintf("RangeBatches/%s", projection), func(b *testing.B) {
			measure(b, func() (int, error) {
				n, sum := 0, 0
				err := d.RangeBatches(ctx, seekparquet.Range{Limit: crawlRowCount, Columns: columns}, func(batch *seekparquet.Batch) error {
					n += batch.Len()
					touch(&sum, batch, len(columns))
					return nil
				})
				return n, err
			})
		})
	}
}
