// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package write

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	seekparquet "github.com/varveio/seekparquet/go"
)

// The writer benchmark reproduces swath's measurement of its writer
// (swath go/docs/measurements.md, "The seekparquet module's part writer",
// 2026-11-08): 300,000 rows of its test schema in one part, one or four
// column goroutines, best of five (-count=5), on a Xeon Platinum 8581C.
// swath's own writer, which filled each column's page from its rows,
// wrote 1,277 thousand rows/s with one column goroutine and 2,166 with
// four; through WriteRows, 64 rows at a time as swath calls it, 803 and
// 914. The pages arm is swath's intake through WritePage.

const benchRows = 300_000

var benchData = sync.OnceValue(func() []swathRow { return testRows(benchRows, 1) })

// discard is a PartFile that keeps nothing.
type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
func (discard) Sync() error                 { return nil }
func (discard) Close() error                { return nil }

func BenchmarkSwathWriter(b *testing.B) {
	rows := benchData()
	schema := swathSchema(false)
	for _, intake := range []string{"rows", "pages"} {
		for _, workers := range []int{1, 4} {
			b.Run(fmt.Sprintf("%s/workers=%d", intake, workers), func(b *testing.B) {
				b.ReportAllocs()
				start := time.Now()
				for b.Loop() {
					w, err := NewPart(discard{}, schema, PartOptions{Key: seekparquet.Key{Column: "key", Unique: true}, SchemaID: "test/all-types",
						ColumnWorkers: func() int { return workers }})
					if err != nil {
						b.Fatal(err)
					}
					if intake == "rows" {
						err = writeBatches(w, rows)
					} else {
						err = writePages(w, rows)
					}
					if err != nil {
						b.Fatal(err)
					}
					if _, err := w.Seal(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(b.N*benchRows)/time.Since(start).Seconds()/1000, "krows/s")
			})
		}
	}
}

// writeBatches is swath's intake through WriteRows: 64 rows at a time,
// their values pointing into the rows' strings.
func writeBatches(w *PartWriter, rows []swathRow) error {
	const batch = 64
	values := make([]parquet.Value, batch*swathColumns)
	buf := make([]parquet.Row, batch)
	for i := range buf {
		buf[i] = values[i*swathColumns : (i+1)*swathColumns : (i+1)*swathColumns]
	}
	for len(rows) > 0 {
		n := min(len(rows), batch)
		for i := range rows[:n] {
			for c := range buf[i] {
				buf[i][c] = rows[i].value(c)
			}
		}
		if _, err := w.WriteRows(buf[:n]); err != nil {
			return err
		}
		rows = rows[n:]
	}
	return nil
}

// writePages is swath's intake through WritePage, as its writer buffered
// rows before: each row appended to a page of rows, the page written when
// full.
func writePages(w *PartWriter, rows []swathRow) error {
	page := make([]swathRow, 0, defaultPageRows)
	for i := range rows {
		page = append(page, rows[i])
		if len(page) == defaultPageRows || i == len(rows)-1 {
			if err := w.WritePage(len(page), fillPage(page)); err != nil {
				return err
			}
			page = page[:0]
		}
	}
	return nil
}
