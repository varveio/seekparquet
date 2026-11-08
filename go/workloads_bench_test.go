// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// BenchmarkLocalWorkloads generates immutable local files outside the checkout.
// Counts are logical SourceFile.ReadAt calls and requested bytes, not disk I/O.
func BenchmarkLocalWorkloads(b *testing.B) {
	counts := os.Getenv("SEEKPARQUET_BENCH_PARTS")
	if counts == "" {
		counts = "10"
	}
	for _, raw := range strings.Split(counts, ",") {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100000 {
			b.Fatal("SEEKPARQUET_BENCH_PARTS must contain integers from 1 to 100000")
		}
		b.Run(fmt.Sprintf("parts=%d", n), func(b *testing.B) {
			spec := testutil.Dataset{}
			for p := range n {
				keys := make([]string, 128)
				for r := range keys {
					keys[r] = fmt.Sprintf("inventory/%06d/object/%06d", p, r)
				}
				spec.Parts = append(spec.Parts, testutil.Part{Rows: testutil.Rows(keys...), GroupRows: 32, PageBytes: 512})
			}
			dir := write(b, spec)
			b.Run("OpenDir", func(b *testing.B) {
				for b.Loop() {
					d, err := OpenDir(ctx, dir, Options{})
					must(b, err)
					must(b, d.Close())
				}
			})
			b.Run("OpenCounted", func(b *testing.B) {
				src := &workloadSource{Source: dirOf(b, dir)}
				b.ResetTimer()
				for b.Loop() {
					d, err := OpenDataset(ctx, src, Options{})
					must(b, err)
					must(b, d.Close())
				}
				b.ReportMetric(float64(src.reads)/float64(b.N), "reads/op")
				b.ReportMetric(float64(src.bytes)/float64(b.N), "requested-B/op")
			})
			key := []byte(fmt.Sprintf("inventory/%06d/object/000064", n-1))
			workloads := []struct {
				name string
				run  func(*Dataset) error
			}{
				{"point", func(d *Dataset) error {
					rows, err := d.Range(ctx, KeyRange(key, 1, "size"))
					if err == nil && (len(rows) != 1 || string(rows[0].Key) != string(key)) {
						return fmt.Errorf("point mismatch")
					}
					return err
				}},
				{"range100", func(d *Dataset) error {
					rows, err := d.Range(ctx, Range{Start: []byte(fmt.Sprintf("inventory/%06d/", n-1)), Limit: 100, Columns: []string{"size"}})
					if err == nil && len(rows) != 100 {
						return fmt.Errorf("range mismatch")
					}
					return err
				}},
				{"delimiter", func(d *Dataset) error {
					entries, err := d.List(ctx, List{Prefix: []byte("inventory/"), Delimiter: '/', Limit: 100})
					if err == nil && len(entries) != min(n, 100) {
						return fmt.Errorf("listing mismatch")
					}
					return err
				}},
			}
			// Obtain a real cursor near the end using the identical range request.
			initial := Range{Start: key, Limit: 1, Columns: []string{"size"}}
			d, err := OpenDir(ctx, dir, Options{})
			must(b, err)
			rows, err := d.Range(ctx, initial)
			must(b, err)
			initial.Cursor = rows[0].Cursor()
			must(b, d.Close())
			workloads = append(workloads, struct {
				name string
				run  func(*Dataset) error
			}{"late_resume", func(d *Dataset) error {
				rows, err := d.Range(ctx, initial)
				if err == nil && (len(rows) != 1 || string(rows[0].Key) != fmt.Sprintf("inventory/%06d/object/000065", n-1)) {
					return fmt.Errorf("resume mismatch")
				}
				return err
			}})
			for _, w := range workloads {
				for _, warm := range []bool{false, true} {
					b.Run(fmt.Sprintf("%s/warm=%t", w.name, warm), func(b *testing.B) {
						var reads, bytes int64
						src := &workloadSource{Source: dirOf(b, dir)}
						b.ResetTimer()
						for b.Loop() {
							b.StopTimer()
							d, err := OpenDataset(ctx, src, Options{})
							must(b, err)
							if warm {
								must(b, w.run(d))
							}
							src.reads, src.bytes = 0, 0
							b.StartTimer()
							must(b, w.run(d))
							b.StopTimer()
							reads += src.reads
							bytes += src.bytes
							must(b, d.Close())
							b.StartTimer()
						}
						b.ReportMetric(float64(reads)/float64(b.N), "reads/op")
						b.ReportMetric(float64(bytes)/float64(b.N), "requested-B/op")
					})
				}
			}
		})
	}
}

type workloadSource struct {
	Source
	reads, bytes int64
}
type workloadFile struct {
	SourceFile
	src *workloadSource
}

func (s *workloadSource) Open(ctx context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	return workloadFile{f, s}, nil
}
func (f workloadFile) ReadAt(p []byte, off int64) (int, error) {
	f.src.reads++
	f.src.bytes += int64(len(p))
	return f.SourceFile.ReadAt(p, off)
}
