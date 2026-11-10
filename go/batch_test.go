// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

// Read physical boundaries through parquet-go, independently of the scan's
// page walkers. Keep these even when the test later removes value indexes.
func batchBoundaries(t *testing.T, dir string, parts int) []int {
	t.Helper()
	cuts := []int{0}
	base := 0
	for p := range parts {
		raw, err := os.ReadFile(partFile(dir, p))
		must(t, err)
		f, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
		must(t, err)
		for _, group := range f.RowGroups() {
			for _, col := range group.ColumnChunks() {
				ix, err := col.OffsetIndex()
				must(t, err)
				for page := 0; page < ix.NumPages(); page++ {
					cuts = append(cuts, base+int(ix.FirstRowIndex(page)))
				}
			}
			base += int(group.NumRows())
			cuts = append(cuts, base)
		}
	}
	slices.Sort(cuts)
	return slices.Compact(cuts)
}

func TestScanBatchesBoundariesAndValues(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			records := transitionRecords(3072, true)
			spec := testutil.Dataset{}
			for start := 0; start < len(records); start += 1536 {
				spec.Parts = append(spec.Parts, testutil.Part{Rows: records[start : start+1536], GroupRows: 512, PageBytes: 512, DictionaryMaxBytes: 1024, Compression: parquet.Compression(&zstd.Codec{})})
			}
			dir := write(t, spec)
			cuts := batchBoundaries(t, dir, 2)
			if !indexed {
				for p := range 2 {
					rewrite(t, dir, p, func(meta *format.FileMetaData) {
						for g := range meta.RowGroups {
							for c := 1; c < len(meta.RowGroups[g].Columns); c++ {
								col := &meta.RowGroups[g].Columns[c]
								col.OffsetIndexOffset, col.OffsetIndexLength = 0, 0
							}
						}
					})
				}
			}
			d := open(t, dir, Options{})
			// Include disjoint selections within pages and across groups/parts.
			ranges := []Range{{Start: records[3].Key, End: records[7].Key}, {Start: records[9].Key, End: records[1400].Key}, {Start: records[1451].Key}}
			plan, err := d.PlanRanges(ctx, ranges, transitionColumns)
			must(t, err)
			var want []transitionRecord
			var ordinals []int
			for i, row := range records {
				if slices.ContainsFunc(ranges, func(r Range) bool { return selects(r, row.Key) }) {
					want = append(want, row)
					ordinals = append(ordinals, i)
				}
			}
			var got []Row
			var cursors []*Cursor
			lengths := map[int]int{}
			var st ReadStats
			var object *Batch
			must(t, plan.ScanBatches(ctx, &st, func(b *Batch) error {
				if b.Len() == 0 {
					t.Fatal("empty batch")
				}
				if object != nil && object != b {
					t.Fatal("batch object was not reused")
				}
				object = b
				start := len(got)
				first, last := ordinals[start], ordinals[start+b.Len()-1]
				if last-first+1 != b.Len() {
					t.Fatal("batch crossed a selected-span gap")
				}
				for _, cut := range cuts {
					if first < cut && cut <= last {
						t.Fatalf("batch [%d,%d] crossed page/group/part boundary %d", first, last, cut)
					}
				}
				lengths[b.Len()]++
				for c := range transitionColumns {
					values, ok := b.Column(c)
					if ok != (c != 6) || ok && (len(values) != b.Len() || cap(values) != len(values)) {
						t.Fatalf("column %d: len=%d cap=%d ok=%v", c, len(values), cap(values), ok)
					}
				}
				for i := 0; i < b.Len(); i++ {
					if !bytes.Equal(b.Key(i), want[start+i].Key) || b.Offset(i) != 0 {
						t.Fatal("key/offset")
					}
					got = append(got, b.Row(i))
					cursors = append(cursors, b.Cursor(i))
				}
				return nil
			}))
			checkTransitionRows(t, got, want)
			for i, c := range cursors {
				if !bytes.Equal(c.Key, want[i].Key) || c.Offset != 0 {
					t.Fatal("retained cursor", i)
				}
			}
			if len(lengths) < 3 {
				t.Fatalf("fixture needs misaligned pages: %v", lengths)
			}
			t.Logf("batch length distribution: %v; %d rows; stats %+v", lengths, len(got), st)
			var owned []Row
			var owningStats ReadStats
			must(t, plan.Scan(ctx, &owningStats, func(row Row) error { owned = append(owned, row); return nil }))
			checkTransitionRows(t, owned, want)
			must(t, plan.ScanBatches(ctx, &st, func(*Batch) error { return nil }))
			if st != owningStats {
				t.Fatalf("Scan stats %+v, batch stats %+v", owningStats, st)
			}
			checkTransitionRows(t, got, want)
		})
	}
}

func TestScanBatchesRepeatedOffsetsAndShards(t *testing.T) {
	var rows []testutil.Row
	for i := range 1380 {
		rows = append(rows, testutil.Row{Key: fmt.Appendf(nil, "k%04d", i/23), Size: ptr(int64(i))})
	}
	d := open(t, write(t, testutil.Dataset{RepeatedKeys: true, Parts: []testutil.Part{{Rows: rows[:690], GroupRows: 137, PageBytes: 128}, {Rows: rows[690:], GroupRows: 138, PageBytes: 128}}}), Options{})
	ranges := []Range{{Cursor: &Cursor{Key: rows[0].Key, Offset: 5}, End: rows[230].Key}, {Start: rows[253].Key}}
	plan, err := d.PlanRanges(ctx, ranges, []string{"size", "key", "size"})
	must(t, err)
	shards, err := plan.Split(ctx, 4)
	must(t, err)
	if len(shards) < 2 {
		t.Fatal("fixture needs shards")
	}
	var want []int64
	for i, row := range rows {
		if slices.ContainsFunc(ranges, func(r Range) bool { return selects(r, row.Key) && afterCursor(r.Cursor, row.Key, int64(i%23)) }) {
			want = append(want, int64(i))
		}
	}
	plans := append([]*RangePlan{plan, plan}, shards...)
	got := make([][]int64, len(plans))
	errs := make([]error, len(plans))
	var wg sync.WaitGroup
	for k, p := range plans {
		wg.Go(func() {
			errs[k] = p.ScanBatches(ctx, nil, func(b *Batch) error {
				values, _ := b.Column(0)
				keys, _ := b.Column(1)
				duplicate, _ := b.Column(2)
				for i, v := range values {
					n := v.Int64()
					if b.Offset(i) != n%23 || !bytes.Equal(b.Key(i), rows[n].Key) || !bytes.Equal(keys[i].ByteArray(), rows[n].Key) || duplicate[i].Int64() != n {
						return fmt.Errorf("bad repeated row %d", n)
					}
					got[k] = append(got[k], n)
				}
				return nil
			})
		})
	}
	wg.Wait()
	for _, err := range errs {
		must(t, err)
	}
	for _, actual := range [][]int64{got[0], got[1], slices.Concat(got[2:]...)} {
		if !slices.Equal(actual, want) {
			t.Fatal("independent ordinal oracle mismatch")
		}
	}
}

func TestScanBatchesAbsentAndEmptyProjection(t *testing.T) {
	type keyOnly struct {
		Key []byte `parquet:"key"`
	}
	d := open(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a", "b")}, {Rows: []keyOnly{{[]byte("c")}, {[]byte("d")}}}, {Rows: testutil.Rows("e", "f")}}}), Options{})
	for _, columns := range [][]string{nil, {"size", "missing", "size"}} {
		plan, err := d.PlanRanges(ctx, []Range{{}}, columns)
		must(t, err)
		n := 0
		must(t, plan.ScanBatches(ctx, nil, func(b *Batch) error {
			for i := 0; i < b.Len(); i++ {
				if string(b.Key(i)) != string(rune('a'+n)) {
					t.Fatal("key", n)
				}
				row := b.Row(i)
				if !slices.Equal(row.Columns, columns) || len(row.Values) != len(columns) {
					t.Fatal("projection")
				}
				for c := range columns {
					v, ok := b.Column(c)
					absent := c == 1 || n/2 == 1
					if ok == absent || row.Values[c].Absent != absent {
						t.Fatal("absent", n, c)
					}
					if !absent && v[i].Int64() != int64(n%2) {
						t.Fatal("value", n)
					}
				}
				n++
			}
			return nil
		}))
		if n != 6 {
			t.Fatal(n)
		}
	}
}

func TestScanBatchesStopAndReenter(t *testing.T) {
	records := transitionRecords(2048, true)
	d := open(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: records, GroupRows: 512, PageBytes: 128}}}), Options{})
	plan, err := d.PlanRanges(ctx, []Range{{}}, transitionColumns)
	must(t, err)
	sentinel := errors.New("callback stop")
	var st ReadStats
	calls := 0
	err = plan.ScanBatches(ctx, &st, func(b *Batch) error { calls++; return sentinel })
	if err != sentinel || calls != 1 || st.KeyPagesDecoded == 0 || st.ValuePagesDecoded == 0 {
		t.Fatalf("callback error %v, calls=%d, stats=%+v", err, calls, st)
	}
	cancelled, cancel := context.WithCancel(ctx)
	calls = 0
	err = plan.ScanBatches(cancelled, &st, func(b *Batch) error { calls++; cancel(); return nil })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancel %v, calls=%d", err, calls)
	}
	calls = 0
	err = plan.ScanBatches(cancelled, &st, func(*Batch) error { calls++; return nil })
	if !errors.Is(err, context.Canceled) || calls != 0 || st != (ReadStats{}) {
		t.Fatalf("entry cancel %v, calls=%d stats=%+v", err, calls, st)
	}
	// A reentrant request must neither overwrite nor invalidate the outer page.
	err = plan.ScanBatches(ctx, nil, func(b *Batch) error {
		saved := b.Row(0)
		err := plan.ScanBatches(ctx, nil, func(*Batch) error { return sentinel })
		if err != sentinel || !reflect.DeepEqual(saved, b.Row(0)) {
			t.Fatal("reentrant scan changed batch")
		}
		return sentinel
	})
	if err != sentinel {
		t.Fatal(err)
	}
	must(t, d.Close())
	if err := plan.ScanBatches(ctx, nil, func(*Batch) error { t.Fatal("closed callback"); return nil }); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
