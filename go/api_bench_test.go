// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"fmt"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

type accessBenchRecord struct {
	Key   []byte  `parquet:"key"`
	Size  int64   `parquet:"size"`
	ETag  string  `parquet:"etag,plain"`
	Owner *string `parquet:"owner,optional"`
}

const accessBenchRows = 4096

func accessBenchDataset(b *testing.B, rows, pageBytes int) *seekparquet.Dataset {
	records := make([]accessBenchRecord, rows)
	for i := range records {
		r := &records[i]
		r.Key = fmt.Appendf(nil, "bucket/object-%08d", i)
		r.Size = int64(i * 7919 % 1_000_003)
		r.ETag = fmt.Sprintf("%032x", uint64(i)*0x9e3779b97f4a7c15)
		if i%3 != 0 {
			owner := fmt.Sprintf("owner-%d", i%17)
			r.Owner = &owner
		}
	}
	dir := b.TempDir()
	if err := testutil.Write(dir, testutil.Dataset{Parts: []testutil.Part{{Rows: records, PageBytes: pageBytes}}}); err != nil {
		b.Fatal(err)
	}
	d, err := seekparquet.OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = d.Close() })
	return d
}

// BenchmarkColumnAccess reads borrowed values of one decoded batch from
// outside the package, through ColumnView: the per-value cost of the
// column accessors, compared with direct slice access before ColumnView.
func BenchmarkColumnAccess(b *testing.B) {
	d := accessBenchDataset(b, accessBenchRows, 8<<20)
	rng := seekparquet.Query{Limit: accessBenchRows, Columns: []string{"size", "etag", "owner"}}
	run := func(name string, body func(batch *seekparquet.Batch) int) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			calls := 0
			err := d.Scan(context.Background(), rng, nil, func(batch *seekparquet.Batch) error {
				calls++
				if batch.Len() != accessBenchRows {
					b.Fatalf("batch of %d rows", batch.Len())
				}
				sum := 0
				for b.Loop() {
					sum += body(batch)
				}
				if sum == 0 {
					b.Fatal("no work")
				}
				return nil
			})
			if err != nil || calls != 1 {
				b.Fatal(err, calls)
			}
		})
	}
	run("Int64", func(batch *seekparquet.Batch) int {
		col := batch.Column(0)
		var sum int64
		for i := range col.Len() {
			sum += col.Int64(i)
		}
		return int(sum & 1023)
	})
	run("Bytes", func(batch *seekparquet.Batch) int {
		col := batch.Column(1)
		n := 0
		for i := range col.Len() {
			n += len(col.Bytes(i))
		}
		return n
	})
	run("Nullable", func(batch *seekparquet.Batch) int {
		col := batch.Column(2)
		n := 0
		for i := range col.Len() {
			if !col.IsNull(i) {
				n += len(col.Bytes(i))
			}
		}
		return n
	})
	run("Value", func(batch *seekparquet.Batch) int {
		col := batch.Column(0)
		var sum int64
		for i := range col.Len() {
			if v := col.Value(i); v.Kind() == seekparquet.KindInt64 {
				sum += v.Int64()
			}
		}
		return int(sum & 1023)
	})
}

// BenchmarkOwnedRows compares owned rows from Rows, the key-first
// collector, with the same owned rows collected through Scan's borrowed
// batches (Batch.Row).
func BenchmarkOwnedRows(b *testing.B) {
	const rows, limit = 100_000, 10_000
	d := accessBenchDataset(b, rows, 64<<10)
	rng := seekparquet.Query{Limit: limit, Columns: []string{"size", "etag", "owner"}}
	rng.Start = []byte("bucket/object-00033333")
	ctx := context.Background()
	if _, err := d.Rows(ctx, rng, nil); err != nil {
		b.Fatal(err)
	}
	b.Run("Rows", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			got, err := d.Rows(ctx, rng, nil)
			if err != nil || len(got) != limit {
				b.Fatal(err, len(got))
			}
		}
	})
	b.Run("Batches", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var got []seekparquet.Row
			err := d.Scan(ctx, rng, nil, func(batch *seekparquet.Batch) error {
				for i := range batch.Len() {
					got = append(got, batch.Row(i))
				}
				return nil
			})
			if err != nil || len(got) != limit {
				b.Fatal(err, len(got))
			}
		}
	})
}
