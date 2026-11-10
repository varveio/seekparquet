// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package local_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"
)

const listBenchPrefix = "v1.1/stellar/parquet/pubnet/v1/"

type listBenchRows struct {
	at   int
	flat bool
}

func (r *listBenchRows) ReadRows(rows []parquet.Row) (int, error) {
	n := 0
	for n < len(rows) && r.at < 6_000_000 {
		key := fmt.Sprintf("%sd%06d/part-%06d.parquet", listBenchPrefix, r.at/3000, r.at%3000)
		if r.flat {
			key = fmt.Sprintf("part-%01000d.parquet", r.at)
		}
		rows[n] = parquet.Row{parquet.ByteArrayValue([]byte(key)).Level(0, 0, 0)}
		n++
		r.at++
	}
	if r.at == 6_000_000 {
		return n, io.EOF
	}
	return n, nil
}

// BenchmarkListBoundary keeps the measured six-million-key workload. Set
// SEEKPARQUET_LIST_BENCH_DIR to retain/reuse its files for the Java benchmark.
func BenchmarkListBoundary(b *testing.B) {
	for _, flat := range []bool{false, true} {
		name := "wide"
		if flat {
			name = "flat"
		}
		b.Run(name, func(b *testing.B) {
			root := os.Getenv("SEEKPARQUET_LIST_BENCH_DIR")
			if root == "" {
				root = b.TempDir()
			}
			dir := filepath.Join(root, name)
			if _, err := os.Stat(dir); os.IsNotExist(err) {
				schema := parquet.NewSchema("listing", parquet.Group{"key": parquet.Leaf(parquet.ByteArrayType)})
				must(b, local.WriteDir(context.Background(), dir, schema, &listBenchRows{flat: flat}, local.DatasetOptions{Part: write.PartOptions{Key: seekparquet.Key{Column: "key", Unique: true}, SchemaID: "list-bench/v1"}}))
			}
			for _, indexed := range []bool{false, true} {
				mode := "plain"
				if indexed {
					mode = "indexed"
				}
				b.Run(mode, func(b *testing.B) {
					d, err := seekparquet.OpenDir(context.Background(), dir, seekparquet.Options{})
					must(b, err)
					defer d.Close()
					must(b, d.Verify(context.Background()))
					req := seekparquet.List{Prefix: []byte(listBenchPrefix), Delimiter: '/', Limit: 1000}
					if flat {
						req.Prefix = nil
					}
					var build seekparquet.ListIndexStats
					if indexed {
						build, err = d.BuildListIndex(context.Background(), seekparquet.ListIndexOptions{Workers: 2})
						must(b, err)
					}
					b.ResetTimer()
					var st seekparquet.ReadStats
					for b.Loop() {
						req.Stats = &st
						_, err = d.List(context.Background(), req)
						must(b, err)
					}
					b.ReportMetric(float64(st.KeyPagesDecoded), "key-pages/op")
					b.ReportMetric(float64(st.KeysDecoded), "keys/op")
					if indexed {
						b.ReportMetric(float64(build.Duration.Nanoseconds()), "build-ns")
						b.ReportMetric(float64(build.Bytes), "index-bytes")
						b.ReportMetric(float64(build.Entries), "index-entries")
					}
				})
			}
		})
	}
}
