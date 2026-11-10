// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

type scanDeliveryRecord struct {
	Key   []byte `parquet:"key"`
	Value string `parquet:"value,plain"`
}

// Large values give several projected pages inside each key page. Read the
// geometry through parquet-go so expected error prefixes are independent of Scan.
func scanDeliveryFixture(t *testing.T) (string, []scanDeliveryRecord, parquet.OffsetIndex, parquet.OffsetIndex) {
	t.Helper()
	rows := make([]scanDeliveryRecord, 256)
	for i := range rows {
		rows[i] = scanDeliveryRecord{Key: fmt.Appendf(nil, "k%05d", i), Value: fmt.Sprintf("value-%05d-%s", i, bytes.Repeat([]byte{'x'}, 128))}
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 1024, DataPageVersion: 1}}})
	raw, err := os.ReadFile(partFile(dir, 0))
	must(t, err)
	f, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
	must(t, err)
	key, err := f.RowGroups()[0].ColumnChunks()[0].OffsetIndex()
	must(t, err)
	value, err := f.RowGroups()[0].ColumnChunks()[1].OffsetIndex()
	must(t, err)
	if key.NumPages() < 2 || value.NumPages() < 2 || value.FirstRowIndex(1) <= 0 || value.FirstRowIndex(1) >= key.FirstRowIndex(1) {
		t.Fatal("fixture needs a projected page boundary within the first key page")
	}
	return dir, rows, key, value
}

func checkScanDeliveryRows(t *testing.T, got []Row, want []scanDeliveryRecord) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("delivered %d rows, want %d", len(got), len(want))
	}
	for i, row := range got {
		if !bytes.Equal(row.Key, want[i].Key) || row.Offset != 0 || len(row.Values) != 1 || row.Values[0].Absent || string(row.Values[0].ByteArray()) != want[i].Value {
			t.Fatalf("delivered row %d: %+v", i, row)
		}
	}
}

func TestScanDeliversBatchesBeforePageError(t *testing.T) {
	for _, column := range []string{"projected", "key"} {
		t.Run(column, func(t *testing.T) {
			dir, records, keys, values := scanDeliveryFixture(t)
			index := values
			if column == "key" {
				index = keys
			}
			// The second projected page fails within the first key page; the second
			// key page fails only after every row of the first key page was delivered.
			want := records[:index.FirstRowIndex(1)]
			editPageHeader(t, partFile(dir, 0), index.Offset(1), func(h *format.PageHeader) { h.UncompressedPageSize++ })
			d := open(t, dir, Options{})
			plan, err := d.PlanRanges(ctx, []Range{{}}, []string{"value"})
			must(t, err)
			for _, batches := range []bool{false, true} {
				t.Run(fmt.Sprintf("batches=%t", batches), func(t *testing.T) {
					var got []Row
					var st ReadStats
					var err error
					if batches {
						err = plan.ScanBatches(ctx, &st, func(b *Batch) error {
							for i := 0; i < b.Len(); i++ {
								got = append(got, b.Row(i))
							}
							return nil
						})
					} else {
						err = plan.Scan(ctx, &st, func(row Row) error { got = append(got, row); return nil })
					}
					if CodeOf(err) != CodeInvalidParquet {
						t.Fatalf("error %v, want invalid_parquet", err)
					}
					checkScanDeliveryRows(t, got, want)
					if st.KeyPagesDecoded == 0 || st.ValuePagesDecoded == 0 {
						t.Fatalf("missing failure statistics: %+v", st)
					}
				})
			}
		})
	}
}

func TestScanCancellationBetweenBatches(t *testing.T) {
	dir, records, _, _ := scanDeliveryFixture(t)
	d := open(t, dir, Options{})
	plan, err := d.PlanRanges(ctx, []Range{{}}, []string{"value"})
	must(t, err)
	// Discover the first batch's actual size; the API does not promise maximal
	// batches. Cancelling in its first row must still deliver all of that batch.
	firstBatch := 0
	stop := errors.New("first batch")
	err = plan.ScanBatches(ctx, nil, func(b *Batch) error { firstBatch = b.Len(); return stop })
	if err != stop || firstBatch < 2 || firstBatch >= len(records) {
		t.Fatalf("first batch %d, error %v", firstBatch, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	defer cancel()
	var got []Row
	err = plan.Scan(cancelled, nil, func(row Row) error {
		got = append(got, row)
		if len(got) == 1 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want cancellation", err)
	}
	checkScanDeliveryRows(t, got, records[:firstBatch])
}
