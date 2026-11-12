// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

// rangeBatchesResult is what one Dataset.Scan call handed over, copied with
// Batch.Row and Batch.Cursor.
type rangeBatchesResult struct {
	rows    []Row
	cursors []*Cursor
	lengths []int
	stats   ReadStats
}

// collectRangeBatches runs Dataset.Scan on rng, checking every batch's shape:
// positive, within the limit, columns of Len() values with no spare capacity,
// borrowed keys and offsets equal to the owning copies, and, when ordinal is
// set, no batch crossing a page, row group or part boundary in cuts.
func collectRangeBatches(t *testing.T, d *Dataset, rng Query, ordinal func(key []byte, offset int64) int, cuts []int) rangeBatchesResult {
	t.Helper()
	var res rangeBatchesResult
	var object *Batch
	err := d.Scan(ctx, rng, &res.stats, func(b *Batch) error {
		if b.Len() <= 0 || int64(len(res.rows)+b.Len()) > rng.Limit {
			t.Fatalf("batch of %d rows after %d, limit %d", b.Len(), len(res.rows), rng.Limit)
		}
		if object != nil && object != b {
			t.Fatal("batch object was not reused")
		}
		object = b
		for c := range rng.Columns {
			if values, ok := rawColumn(b, c); ok && (len(values) != b.Len() || cap(values) != len(values)) {
				t.Fatalf("column %d: len %d cap %d, batch %d", c, len(values), cap(values), b.Len())
			}
		}
		for i := 0; i < b.Len(); i++ {
			row := b.Row(i)
			if !bytes.Equal(b.Key(i), row.Key) || b.Offset(i) != row.Offset {
				t.Fatal("borrowed key/offset differ from the owning row")
			}
			res.rows = append(res.rows, row)
			res.cursors = append(res.cursors, b.Cursor(i))
		}
		if ordinal != nil {
			first, last := ordinal(b.Key(0), b.Offset(0)), ordinal(b.Key(b.Len()-1), b.Offset(b.Len()-1))
			if last-first+1 != b.Len() {
				t.Fatalf("batch of %d rows spans ordinals %d..%d", b.Len(), first, last)
			}
			for _, cut := range cuts {
				if first < cut && cut <= last {
					t.Fatalf("batch [%d,%d] crossed boundary %d", first, last, cut)
				}
			}
		}
		res.lengths = append(res.lengths, b.Len())
		return nil
	})
	must(t, err)
	return res
}

// checkRangeBatches checks Scan against Rows on rng: the same rows,
// keys, values and cursors, and the same read statistics on a warm dataset
// (Rows runs first, so neither call finds the other's index reads).
func checkRangeBatches(t *testing.T, label string, d *Dataset, rng Query, ordinal func([]byte, int64) int, cuts []int) []Row {
	t.Helper()
	want, err := d.Rows(ctx, rng, nil)
	must(t, err)
	got := collectRangeBatches(t, d, rng, ordinal, cuts)
	var warm ReadStats
	again, err := d.Rows(ctx, rng, &warm)
	must(t, err)
	if len(want) == 0 {
		want = nil
	}
	if !reflect.DeepEqual(got.rows, want) || !reflect.DeepEqual(again, want) {
		t.Fatalf("%s: RangeBatches rows differ from Range: %d vs %d rows", label, len(got.rows), len(want))
	}
	for i, c := range got.cursors {
		if !reflect.DeepEqual(c, want[i].Cursor()) {
			t.Fatalf("%s: cursor %d", label, i)
		}
	}
	if memoNeutral(got.stats) != memoNeutral(warm) { // Range reuses pages its request read; a scan's scratch cannot
		t.Fatalf("%s: stats\nRangeBatches %+v\nRange        %+v", label, got.stats, warm)
	}
	if len(want) == 0 && len(got.lengths) != 0 {
		t.Fatalf("%s: an empty range called fn", label)
	}
	return want
}

// The randomized oracle fixture has three parts and repeated keys whose
// equal-key groups span row groups (parts are cut between key groups, so
// paging across a part boundary is continuation, not a split group), with
// dictionary, PLAIN, DELTA and optional columns with nulls, an absent column
// and duplicate projections, with and without value OffsetIndexes. Limits are
// sampled from the boundaries; TestRangeBatchesBoundaries picks one of each
// class exactly.
func TestRangeBatchesMatchesRange(t *testing.T) {
	projections := [][]string{batchOracleColumns, nil, {"num"}, {"missing"}, {"opt", "opt", "dict"}}
	for seed := int64(1); seed <= 3; seed++ {
		for _, indexed := range []bool{true, false} {
			t.Run(fmt.Sprintf("seed=%d/indexed=%t", seed, indexed), func(t *testing.T) {
				dir, rows, nkeys, cuts := batchOracleFixture(t, seed, indexed)
				d := open(t, dir, OpenOptions{})
				ordinals := map[string]int{}
				for _, row := range rows {
					ordinals[fmt.Sprintf("%s/%d", row.rec.Key, row.offset)] = row.ord
				}
				ordinal := func(key []byte, offset int64) int {
					o, ok := ordinals[fmt.Sprintf("%s/%d", key, offset)]
					if !ok {
						t.Fatalf("no row %s/%d", key, offset)
					}
					return o
				}
				// Limits at a sample of the page, row group and part
				// boundaries of the unbounded range, and one row either side.
				for k := 1; k < len(cuts); k += 1 + k/8 {
					for _, limit := range []int{cuts[k] - 1, cuts[k], cuts[k] + 1} {
						if limit > 0 {
							checkRangeBatches(t, fmt.Sprintf("limit %d", limit), d, Query{Limit: int64(limit), Columns: batchOracleColumns}, ordinal, cuts)
						}
					}
				}
				r := rand.New(rand.NewSource(seed * 11))
				for trial := 0; trial < 24; trial++ {
					ranges := batchOracleRanges(r, nkeys, rows)
					rng := Query{Selection: ranges[r.Intn(len(ranges))]}
					rng.Columns = projections[r.Intn(len(projections))]
					rng.Limit = int64(1 + r.Intn(len(rows)/3))
					if r.Intn(4) == 0 {
						rng.Limit = int64(len(rows))
					}
					label := fmt.Sprintf("trial %d %s", trial, describe(rng))
					// Page through the range with each page's last cursor,
					// against the oracle.
					oracle := batchOracleExpected([]Selection{rng.Selection}, rows)
					var all []Row
					for page := 0; ; page++ {
						// Cuts hold every column's pages: only a full projection
						// is bounded by all of them.
						var bounds []int
						if slices.Equal(rng.Columns, batchOracleColumns) {
							bounds = cuts
						}
						got := checkRangeBatches(t, fmt.Sprintf("%s page %d", label, page), d, rng, ordinal, bounds)
						all = append(all, got...)
						if int64(len(got)) < rng.Limit {
							break
						}
						rng.Cursor = got[len(got)-1].Cursor()
					}
					if len(all) != len(oracle) {
						t.Fatalf("%s: paged %d rows, oracle %d", label, len(all), len(oracle))
					}
					for i, row := range all {
						if !bytes.Equal(row.Key, oracle[i].rec.Key) || row.Offset != oracle[i].offset {
							t.Fatalf("%s: row %d is %s/%d, oracle ordinal %d", label, i, row.Key, row.Offset, oracle[i].ord)
						}
					}
				}
			})
		}
	}
}

// A small limit on a long range reads what Rows reads, cold: no OffsetIndex
// or dictionary of a row group the rows do not reach, unlike a plan.
func TestRangeBatchesDoesNotPlan(t *testing.T) {
	records := transitionRecords(4096, true)
	spec := testutil.Dataset{}
	for start := 0; start < len(records); start += 1024 {
		spec.Parts = append(spec.Parts, testutil.Part{Rows: records[start : start+1024], GroupRows: 256, PageBytes: 512, DictionaryMaxBytes: 1024})
	}
	dir := write(t, spec)
	rng := Query{Selection: Selection{Span: Span{Start: records[100].Key}}, Limit: 50, Columns: transitionColumns}
	var cold, batched ReadStats
	// Index reads from the Source, not retained metadata: planning's are
	// what make it cost more.
	opt := OpenOptions{Read: ReadPolicy{DisableSpeculation: true}}
	want, err := open(t, dir, opt).Rows(ctx, rng, &cold)
	must(t, err)
	got := collectRangeBatches(t, open(t, dir, opt), rng, nil, nil)
	batched = got.stats
	if !reflect.DeepEqual(got.rows, want) {
		t.Fatal("rows differ from Range")
	}
	if batched != cold {
		t.Fatalf("cold stats\nRangeBatches %+v\nRange        %+v", batched, cold)
	}
	var planned ReadStats
	_, err = open(t, dir, opt).Plan(ctx, []Selection{{Span: Span{Start: records[100].Key}}}, transitionColumns, &planned)
	must(t, err)
	if planned.SourceReads <= batched.SourceReads || planned.PartsOpened <= batched.PartsOpened {
		t.Fatalf("planning read %+v, RangeBatches %+v: the fixture needs planning to cost more", planned, batched)
	}
}

func TestRangeBatchesEmptyAndInvalid(t *testing.T) {
	keys := fixtureKeys(300)
	d := open(t, write(t, parts(testutil.Part{GroupRows: 50, PageBytes: 128}, split(keys, 2)...)), OpenOptions{})
	empty := []Query{{Selection: Selection{Span: Span{Start: []byte(keys[10]), End: []byte(keys[10])}}, Limit: 5}, {Selection: Selection{Span: Span{Start: []byte(keys[20]), End: []byte(keys[10])}}, Limit: 5}, {Selection: Selection{Span: Span{Start: []byte(keys[299]), StartExclusive: true}}, Limit: 5}, {Selection: Selection{Span: Span{Prefix: []byte("~nothing")}}, Limit: 5}, {Selection: Selection{Span: Span{End: []byte{}}}, Limit: 5}, {Selection: Selection{Cursor: &Cursor{Key: []byte(keys[299])}}, Limit: 5}}
	for _, rng := range empty {
		got := checkRangeBatches(t, describe(rng), d, rng, nil, nil)
		if len(got) != 0 {
			t.Fatalf("%s: %d rows", describe(rng), len(got))
		}
	}
	noRows := open(t, write(t, testutil.Dataset{}), OpenOptions{})
	must(t, noRows.Scan(ctx, Query{Limit: 1}, nil, func(*Batch) error { t.Fatal("fn called"); return nil }))

	fn := func(*Batch) error { t.Fatal("fn called"); return nil }
	invalid := []struct {
		rng Query
		fn  func(*Batch) error
	}{
		{Query{Limit: -1}, fn},
		{Query{Limit: math.MinInt64}, fn},
		{Query{Selection: Selection{Cursor: &Cursor{}}, Limit: 1}, fn},
		{Query{Selection: Selection{Cursor: &Cursor{Key: []byte("a"), Prefix: []byte("a")}}, Limit: 1}, fn},
		{Query{Selection: Selection{Cursor: &Cursor{Key: []byte("a"), Offset: -1}}, Limit: 1}, fn},
		{Query{Selection: Selection{Cursor: &Cursor{Prefix: []byte("a")}}, Limit: 1}, fn},
		{Query{Limit: 1}, nil},
	}
	for i, c := range invalid {
		st := ReadStats{KeyPagesDecoded: 7}
		if err := d.Scan(ctx, c.rng, &st, c.fn); !errors.Is(err, ErrInvalidRequest) || st != (ReadStats{}) {
			t.Fatalf("case %d: %v, stats %+v", i, err, st)
		}
	}
}

func TestRangeBatchesStopCancelAndClose(t *testing.T) {
	records := transitionRecords(2048, true)
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: records, GroupRows: 512, PageBytes: 128}}})
	d := open(t, dir, OpenOptions{})
	rng := Query{Limit: int64(len(records)), Columns: transitionColumns}

	// A callback error is returned unchanged, with the reads made so far.
	sentinel := errors.New("callback stop")
	var st ReadStats
	calls := 0
	err := d.Scan(ctx, rng, &st, func(*Batch) error { calls++; return sentinel })
	if err != sentinel || calls != 1 || st.KeyPagesDecoded == 0 || st.ValuePagesDecoded == 0 {
		t.Fatalf("callback error %v, calls %d, stats %+v", err, calls, st)
	}

	// Cancelled before any read: no callback and no read, on a cold dataset.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	cold := open(t, dir, OpenOptions{})
	err = cold.Scan(cancelled, rng, &st, func(*Batch) error { calls++; return nil })
	if !errors.Is(err, context.Canceled) || st.SourceReads != 0 || st.KeyPagesDecoded != 0 {
		t.Fatalf("entry cancel %v, stats %+v", err, st)
	}

	// Cancelled in a callback: the next callback does not run.
	cancelled, cancel = context.WithCancel(ctx)
	defer cancel()
	calls = 0
	err = d.Scan(cancelled, rng, nil, func(*Batch) error { calls++; cancel(); return nil })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancel between callbacks %v, calls %d", err, calls)
	}

	// Owning copies outlive the callback and a reentrant call; the borrowed
	// batch is unchanged by a reentrant Scan.
	var kept []Row
	var cursor *Cursor
	err = d.Scan(ctx, rng, nil, func(b *Batch) error {
		saved := b.Row(0)
		if err := d.Scan(ctx, Query{Limit: 7, Columns: transitionColumns}, nil, func(*Batch) error { return nil }); err != nil {
			return err
		}
		if !reflect.DeepEqual(saved, b.Row(0)) {
			t.Fatal("reentrant scan changed the batch")
		}
		for i := 0; i < b.Len(); i++ {
			kept = append(kept, b.Row(i))
		}
		cursor = b.Cursor(b.Len() - 1)
		if len(kept) > 600 {
			return sentinel
		}
		return nil
	})
	if err != sentinel {
		t.Fatal(err)
	}
	checkTransitionRows(t, kept, records[:len(kept)])
	if !bytes.Equal(cursor.Key, records[len(kept)-1].Key) {
		t.Fatal("retained cursor")
	}

	// Close during a callback waits for it, then the scan ends with ErrClosed.
	inside, release := make(chan struct{}), make(chan struct{})
	scanned := make(chan error, 1)
	go func() {
		first := true
		scanned <- d.Scan(ctx, rng, nil, func(*Batch) error {
			if first {
				first = false
				close(inside)
				<-release
			}
			return nil
		})
	}()
	<-inside
	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	for d.r.closing.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned %v during a callback", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-scanned; !errors.Is(err, ErrClosed) {
		t.Fatalf("scan after Close: %v", err)
	}
	must(t, <-closed)
	if err := d.Scan(ctx, rng, nil, func(*Batch) error { t.Fatal("closed callback"); return nil }); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

// Concurrent calls on one Dataset, cold and then warm, each have their own
// scratch: run under -race.
func TestRangeBatchesConcurrent(t *testing.T) {
	dir, rows, nkeys, _ := batchOracleFixture(t, 5, true)
	d := open(t, dir, OpenOptions{})
	r := rand.New(rand.NewSource(5))
	var reqs []Query
	for range 12 {
		ranges := batchOracleRanges(r, nkeys, rows)
		reqs = append(reqs, Query{Selection: ranges[0], Limit: int64(1 + r.Intn(len(rows)/2)), Columns: batchOracleColumns})
	}
	want := make([][]Row, len(reqs))
	for i, rng := range reqs {
		var err error
		want[i], err = open(t, dir, OpenOptions{}).Rows(ctx, rng, nil)
		must(t, err)
	}
	for range 2 {
		got := make([][]Row, len(reqs))
		errs := make([]error, len(reqs))
		var wg sync.WaitGroup
		for i, rng := range reqs {
			wg.Go(func() {
				errs[i] = d.Scan(ctx, rng, nil, func(b *Batch) error {
					for k := 0; k < b.Len(); k++ {
						got[i] = append(got[i], b.Row(k))
					}
					return nil
				})
			})
		}
		wg.Wait()
		for i := range reqs {
			must(t, errs[i])
			if !reflect.DeepEqual(got[i], want[i]) && (len(got[i]) != 0 || len(want[i]) != 0) {
				t.Fatalf("request %d: %d rows, want %d", i, len(got[i]), len(want[i]))
			}
		}
	}
	if !slices.ContainsFunc(want, func(rows []Row) bool { return len(rows) > 100 }) {
		t.Fatal("fixture needs long reads")
	}
}

// checkRangeBatchesCold compares Scan with Rows on fresh Datasets,
// so that every footer, index and page either reads counts: an extra
// advance past the last row (another key page, row group or part) fails it
// even when the rows are the same.
func checkRangeBatchesCold(t *testing.T, label, dir string, rng Query) {
	t.Helper()
	var cold ReadStats
	want, err := open(t, dir, OpenOptions{}).Rows(ctx, rng, &cold)
	must(t, err)
	got := collectRangeBatches(t, open(t, dir, OpenOptions{}), rng, nil, nil)
	if len(want) == 0 {
		want = nil
	}
	if !reflect.DeepEqual(got.rows, want) {
		t.Fatalf("%s: %d rows, Query %d", label, len(got.rows), len(want))
	}
	if memoNeutral(got.stats) != memoNeutral(cold) {
		t.Fatalf("%s: cold stats\nRangeBatches %+v\nRange        %+v", label, got.stats, cold)
	}
}

// boundaryFixture writes two parts of two row groups each, with several key
// pages per group and smaller value pages, and returns one dataset ordinal
// of each boundary class, read through parquet-go: a key page boundary that
// is not a value page boundary, a value page boundary inside a key page, a
// row group boundary inside a part, and the part boundary.
func boundaryFixture(t *testing.T) (dir string, records []scanDeliveryRecord, cuts map[string]int, keyPages [][2]int) {
	t.Helper()
	records = make([]scanDeliveryRecord, 512)
	for i := range records {
		records[i] = scanDeliveryRecord{Key: fmt.Appendf(nil, "k%05d", i), Value: fmt.Sprintf("value-%05d-%s", i, bytes.Repeat([]byte{'x'}, 128))}
	}
	dir = write(t, testutil.Dataset{Parts: []testutil.Part{
		{Rows: records[:256], GroupRows: 128, PageBytes: 1024, DataPageVersion: 1},
		{Rows: records[256:], GroupRows: 128, PageBytes: 1024, DataPageVersion: 1},
	}})
	keyCuts, valueCuts, groupCuts := map[int]bool{}, map[int]bool{}, map[int]bool{}
	base := 0
	for p := range 2 {
		raw, err := os.ReadFile(partFile(dir, p))
		must(t, err)
		f, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
		must(t, err)
		for _, group := range f.RowGroups() {
			groupCuts[base] = true
			for c, cuts := range []map[int]bool{keyCuts, valueCuts} {
				ix, err := group.ColumnChunks()[c].OffsetIndex()
				must(t, err)
				for page := 0; page < ix.NumPages(); page++ {
					first := base + int(ix.FirstRowIndex(page))
					if page > 0 {
						cuts[first] = true
					}
					if c == 0 {
						last := base + int(group.NumRows())
						if page+1 < ix.NumPages() {
							last = base + int(ix.FirstRowIndex(page+1))
						}
						keyPages = append(keyPages, [2]int{first, last})
					}
				}
			}
			base += int(group.NumRows())
		}
	}
	cuts = map[string]int{"part": 256, "group": 128}
	for o := 1; o < len(records); o++ {
		if _, ok := cuts["key page"]; !ok && keyCuts[o] && !valueCuts[o] && !groupCuts[o] {
			cuts["key page"] = o
		}
		if _, ok := cuts["value page"]; !ok && valueCuts[o] && !keyCuts[o] && !groupCuts[o] {
			cuts["value page"] = o
		}
	}
	if len(cuts) != 4 || !groupCuts[128] || !groupCuts[256] || len(keyPages) < 4 {
		t.Fatalf("fixture lacks a boundary class: %v, key pages %v", cuts, keyPages)
	}
	return dir, records, cuts, keyPages
}

func TestRangeBatchesBoundaries(t *testing.T) {
	dir, records, cuts, keyPages := boundaryFixture(t)
	columns := []string{"value"}
	d := open(t, dir, OpenOptions{})
	for class, cut := range cuts {
		for _, limit := range []int{cut - 1, cut, cut + 1} {
			for _, start := range []int{0, cut - 3} {
				rng := Query{Selection: Selection{Span: Span{Start: records[start].Key}}, Limit: int64(limit - start), Columns: columns}
				label := fmt.Sprintf("%s boundary %d: rows [%d, %d)", class, cut, start, limit)
				got := checkRangeBatches(t, label, d, rng, nil, nil)
				checkRangeBatchesCold(t, label, dir, rng)
				if len(got) != limit-start || !bytes.Equal(got[len(got)-1].Key, records[limit-1].Key) {
					t.Fatalf("%s: %d rows", label, len(got))
				}
			}
		}
	}
	// End inside a key page, one row into it, and on its first row.
	page := keyPages[1]
	if page[1]-page[0] < 3 {
		t.Fatalf("key page %v too short", page)
	}
	for _, end := range []int{page[0] + 2, page[0] + 1, page[0], page[1]} {
		for _, start := range []int{0, page[0]} {
			if start >= end {
				continue
			}
			rng := Query{Selection: Selection{Span: Span{Start: records[start].Key, End: records[end].Key}}, Limit: int64(len(records)), Columns: columns}
			label := fmt.Sprintf("End %d (key page %v) from %d", end, page, start)
			got := checkRangeBatches(t, label, d, rng, nil, nil)
			checkRangeBatchesCold(t, label, dir, rng)
			if len(got) != end-start {
				t.Fatalf("%s: %d rows", label, len(got))
			}
		}
	}
}

// Completion wins over a cancellation or Close that arrives in the callback
// that reaches the limit, as for Rows, which checks nothing after its last
// read; an empty request returns nil even with a cancelled context.
func TestRangeBatchesCompletion(t *testing.T) {
	dir, records, cuts, _ := boundaryFixture(t)
	d := open(t, dir, OpenOptions{})
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	for _, rng := range []Query{{Selection: Selection{Span: Span{Start: records[10].Key, End: records[10].Key}}, Limit: 5}, {Selection: Selection{Span: Span{Prefix: []byte("~")}}, Limit: 5}} {
		rows, err := d.Rows(cancelled, rng, nil)
		if err != nil || rows != nil {
			t.Fatalf("Query on a cancelled empty request: %v", err)
		}
		if err := d.Scan(cancelled, rng, nil, func(*Batch) error { t.Fatal("fn called"); return nil }); err != nil {
			t.Fatalf("RangeBatches on a cancelled empty request: %v", err)
		}
	}

	// Limits that end one batch and two batches in.
	rng := Query{Limit: int64(len(records)), Columns: []string{"value"}}
	var lengths []int
	must(t, d.Scan(ctx, rng, nil, func(b *Batch) error { lengths = append(lengths, b.Len()); return nil }))
	if len(lengths) < 3 || lengths[0]+lengths[1] > cuts["group"] {
		t.Fatalf("fixture needs small batches: %v", lengths)
	}
	for _, limit := range []int{lengths[0], lengths[0] + lengths[1]} {
		rng.Limit = int64(limit)
		cancelled, cancel := context.WithCancel(ctx)
		n := 0
		err := d.Scan(cancelled, rng, nil, func(b *Batch) error {
			if n += b.Len(); n == limit {
				cancel()
			}
			return nil
		})
		cancel()
		if err != nil || n != limit {
			t.Fatalf("cancel in the limit-completing callback: %v after %d of %d rows", err, n, limit)
		}
	}

	// Close during the final callback waits for it; the scan then returns
	// either nil (completion) or ErrClosed, never another error.
	rng.Limit = int64(lengths[0])
	inside, release := make(chan struct{}), make(chan struct{})
	scanned := make(chan error, 1)
	go func() {
		scanned <- d.Scan(ctx, rng, nil, func(*Batch) error {
			close(inside)
			<-release
			return nil
		})
	}()
	<-inside
	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	for d.r.closing.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned %v during the final callback", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-scanned; err != nil && !errors.Is(err, ErrClosed) {
		t.Fatalf("scan after Close in its final callback: %v", err)
	}
	must(t, <-closed)
}

// A corrupt projected page or a corrupt dictionary is refused as Rows
// refuses it, after the batches before it; a dictionary is read only when
// a page needs it, so a limit that ends before the corrupt group succeeds.
func TestRangeBatchesRefusals(t *testing.T) {
	t.Run("value page", func(t *testing.T) {
		dir, records, _, values := scanDeliveryFixture(t)
		editPageHeader(t, partFile(dir, 0), values.Offset(1), func(h *format.PageHeader) { h.UncompressedPageSize++ })
		checkRangeBatchesRefusal(t, dir, Query{Limit: int64(len(records)), Columns: []string{"value"}}, int(values.FirstRowIndex(1)))
		checkRangeBatchesCold(t, "before the corrupt page", dir, Query{Limit: int64(int(values.FirstRowIndex(1))), Columns: []string{"value"}})
	})
	t.Run("dictionary", func(t *testing.T) {
		records := transitionRecords(1024, false)
		dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: records, GroupRows: 512, PageBytes: 256}}})
		raw, err := os.ReadFile(partFile(dir, 0))
		must(t, err)
		f, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
		must(t, err)
		meta := f.Metadata().RowGroups[1].Columns[1].MetaData // dict
		if meta.PathInSchema[0] != "dict" || meta.DictionaryPageOffset == 0 {
			t.Fatalf("fixture needs a dictionary in group 1: %+v", meta.PathInSchema)
		}
		editPageHeader(t, partFile(dir, 0), meta.DictionaryPageOffset, func(h *format.PageHeader) { h.UncompressedPageSize++ })
		checkRangeBatchesRefusal(t, dir, Query{Limit: int64(len(records)), Columns: []string{"dict"}}, 512)
		checkRangeBatchesCold(t, "before the corrupt dictionary", dir, Query{Limit: 512, Columns: []string{"dict"}})
	})
}

func checkRangeBatchesRefusal(t *testing.T, dir string, rng Query, delivered int) {
	t.Helper()
	if _, err := open(t, dir, OpenOptions{}).Rows(ctx, rng, nil); RefusalCodeOf(err) != CodeInvalidParquet {
		t.Fatalf("Range: %v, want invalid_parquet", err)
	}
	var st ReadStats
	n := 0
	err := open(t, dir, OpenOptions{}).Scan(ctx, rng, &st, func(b *Batch) error { n += b.Len(); return nil })
	if RefusalCodeOf(err) != CodeInvalidParquet || n != delivered || st.ValuePagesDecoded == 0 {
		t.Fatalf("RangeBatches: %v after %d rows (want %d), stats %+v", err, n, delivered, st)
	}
}
