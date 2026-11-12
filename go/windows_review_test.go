// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

type panicWaveSource struct {
	Source
	armed atomic.Bool
	calls atomic.Int64
}
type panicWaveFile struct {
	SourceFile
	owner *panicWaveSource
}

func (s *panicWaveSource) Open(ctx context.Context, name string) (SourceFile, error) {
	f, err := s.Source.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return &panicWaveFile{f, s}, nil
}
func (f *panicWaveFile) ReadRanges(ctx context.Context, ranges []ReadRange) error {
	if f.owner.armed.Load() {
		f.owner.calls.Add(1)
		panic("source")
	}
	for _, rg := range ranges {
		if _, err := f.SourceFile.ReadAt(rg.Buf, rg.Off); err != nil {
			return err
		}
	}
	return nil
}

func TestDemandedWindowSourcePanic(t *testing.T) {
	dir := write(t, parts(testutil.Part{PageBytes: 128}, fixtureKeys(100)))
	for _, path := range []string{"get", "rows", "list", "scan", "planned", "build"} {
		t.Run(path, func(t *testing.T) {
			src := &panicWaveSource{Source: dirOf(t, dir)}
			d := openWith(t, ctx, src, OpenOptions{}, false)
			must(t, d.Verify(ctx, nil))
			// Warm both projected indexes, so the failure is a demanded page wave.
			_, err := d.Get(ctx, []byte(fixtureKeys(100)[0]), nil, "size", "name")
			must(t, err)
			plan, err := d.Plan(ctx, []Selection{{}}, []string{"size", "name"}, nil)
			must(t, err)
			src.armed.Store(true)
			switch path {
			case "get":
				_, err = d.Get(ctx, []byte(fixtureKeys(100)[0]), nil, "size", "name")
			case "rows":
				_, err = d.Rows(ctx, Query{Limit: 1, Columns: []string{"size", "name"}}, nil)
			case "list":
				_, err = d.List(ctx, ListQuery{Limit: 1, Columns: []string{"size", "name"}}, nil)
			case "scan":
				err = d.Scan(ctx, Query{Limit: 1, Columns: []string{"size", "name"}}, nil, func(*Batch) error { return nil })
			case "planned":
				err = plan.Scan(ctx, nil, func(*Batch) error { return nil })
			case "build":
				_, err = d.BuildListIndex(ctx, ListIndexOptions{})
			}
			var refusal *RefusalError
			if src.calls.Load() == 0 || !errors.Is(err, errSourcePanic) || !strings.Contains(err.Error(), "the source panicked: source") || !isEnvironment(ctx, err) || errors.As(err, &refusal) {
				t.Fatalf("calls %d, error %T %v", src.calls.Load(), err, err)
			}
		})
	}
}

func TestReadPlanPageLimitBytes(t *testing.T) {
	type item struct {
		Key  string `parquet:"key"`
		Blob []byte `parquet:"blob"`
	}
	rows := make([]item, 600)
	for i := range rows {
		rows[i] = item{fmt.Sprintf("k%04d", i), make([]byte, 10<<10)}
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 1 << 20}}})
	for _, metadata := range []int64{0, 128 << 10} {
		t.Run(fmt.Sprint(metadata), func(t *testing.T) {
			var counts [2]int64
			for mode := range counts {
				src := &extentSource{Source: dirOf(t, dir)}
				d := openWith(t, ctx, &recordingWaveSource{Source: src}, OpenOptions{Limits: Limits{PageBytes: 64 << 10, MetadataMemory: metadata}, Read: ReadPolicy{DisableSpeculation: mode == 0}}, false)
				must(t, d.Verify(ctx, nil))
				src.take()
				err := d.Scan(ctx, Query{Columns: []string{"blob"}}, nil, func(*Batch) error { t.Fatal("decoded oversized page"); return nil })
				if !errors.Is(err, ErrLimitExceeded) {
					t.Fatal(err)
				}
				for _, rd := range src.take() {
					counts[mode] += rd.len
				}
			}
			if counts[1] > counts[0] {
				t.Fatalf("window transfer %d exceeds demand %d", counts[1], counts[0])
			}
			t.Logf("demand %d bytes, windows %d bytes", counts[0], counts[1])
		})
	}
}

func TestListIndexBudgetLookahead(t *testing.T) {
	// Many small parts used to all open before the first budget decision.
	keys := make([][]string, 100)
	for i := range keys {
		keys[i] = []string{fmt.Sprintf("d%04d/f", i)}
	}
	dir := write(t, parts(testutil.Part{}, keys...))
	d := openWith(t, ctx, dirOf(t, dir), OpenOptions{}, false)
	st, err := d.BuildListIndex(ctx, ListIndexOptions{MaxBytes: 1})
	must(t, err)
	ready := 0
	for _, p := range d.r.parts {
		if p.ready.Load() {
			ready++
		}
	}
	if !st.BudgetExhausted || st.GroupsIndexed != 0 || ready != 1 {
		t.Fatalf("%d parts checked: %+v", ready, st)
	}

	// Already checked parts must not cause windows far beyond the stop either.
	dir = write(t, parts(testutil.Part{GroupRows: 10, PageBytes: 64}, fixtureKeys(1000)))
	src := &recordingWaveSource{Source: dirOf(t, dir)}
	d = openWith(t, ctx, src, OpenOptions{}, false)
	must(t, d.Verify(ctx, nil))
	src.batches = nil
	st, err = d.BuildListIndex(ctx, ListIndexOptions{MaxBytes: 1})
	must(t, err)
	if !st.BudgetExhausted || len(src.batches) != 1 {
		t.Fatalf("batches %v, %+v", src.batches, st)
	}
	p := d.r.parts[0]
	meta := p.footer.Metadata().RowGroups[0].Columns[p.key.index].MetaData
	start, end, ok := chunkRange(&meta)
	if !ok {
		t.Fatal("bad fixture")
	}
	for _, rg := range src.batches[0] {
		if rg[0] < start || rg[0]+rg[1] > end {
			t.Fatalf("prefetched beyond first group [%d,%d): %v", start, end, src.batches)
		}
	}
}

func TestSourceErrorWalkBudget(t *testing.T) {
	// A broken pin beyond the walk budget cannot override a missing-part
	// classification already found. Reversing their order does expose it.
	unreached := error(ErrObjectChanged)
	for range maxEOFCauses {
		unreached = fmt.Errorf("retry: %w", unreached)
	}
	if got := sourceError("p", errors.Join(fs.ErrNotExist, unreached)); RefusalCodeOf(got) != CodeMissingPart {
		t.Fatal(got)
	}
	if got := sourceError("p", errors.Join(ErrObjectChanged, fs.ErrNotExist, unreached)); !isEnvironment(ctx, got) {
		t.Fatal(got)
	}
	for _, n := range []int{31, 32, 64, 100} {
		for _, mismatch := range []bool{false, true} {
			causes := make([]error, n)
			for i := range causes {
				cause := error(fs.ErrNotExist)
				if mismatch {
					cause = &SizeMismatchError{Path: "p", Expected: 1, Actual: 2}
				}
				causes[i] = fmt.Errorf("retry %d: %w", i, cause)
			}
			code := CodeMissingPart
			if mismatch {
				code = CodePartMismatch
			}
			if err := sourceError("p", errors.Join(causes...)); RefusalCodeOf(err) != code {
				t.Fatalf("%d causes: %v", n, err)
			}
		}
	}
	// Exactly 64 visited nodes, with no skipped non-nil node.
	err := error(fs.ErrNotExist)
	for range 63 {
		err = fmt.Errorf("retry: %w", err)
	}
	if got := sourceError("p", err); RefusalCodeOf(got) != CodeMissingPart {
		t.Fatal(got)
	}
}

type heldWaveFile struct{ started, finish chan struct{} }

func (f *heldWaveFile) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }
func (f *heldWaveFile) ReadRanges(context.Context, []ReadRange) error {
	close(f.started)
	<-f.finish
	return nil
}

func TestWindowsFetchJoinsOwnWave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &heldWaveFile{make(chan struct{}), make(chan struct{})}
		_, w, p := testWindows(t, f)
		defer close(f.finish)
		first := make(chan struct{})
		go func() { w.fetch([]extent{{p, 0, 1, 1}}, false); close(first) }()
		<-f.started
		_, _, other := testWindows(t, &windowFile{data: []byte("x")})
		second := make(chan struct{})
		go func() { w.fetchWave([]extent{{other, 0, 1, 1}}, nil); close(second) }()
		synctest.Wait()
		select {
		case <-second:
		default:
			t.Error("second fetch waited for unrelated wave")
		}
		select {
		case <-first:
			t.Error("first fetch did not wait for its source")
		default:
		}
	})
}

func TestScanReadChunkBounds(t *testing.T) {
	chunks := []chunkExtent{{"p", 10, 20}, {"p", 20, 30}, {"p", 40, 50}, {"other", 30, 40}}
	for _, tc := range []struct {
		off, size int64
		valid     bool
	}{
		{10, 10, true}, {10, 20, true}, {15, 15, true},
		{10, 21, false}, {15, 16, false}, {9, 11, false}, {25, 20, false},
	} {
		if got := insideChunks(extentRead{"p", tc.off, tc.size}, chunks); got != tc.valid {
			t.Fatalf("%d+%d: valid %v", tc.off, tc.size, got)
		}
	}
}

func TestReadPlanKeyDictionaryPrefix(t *testing.T) {
	type item struct {
		Key string `parquet:"key,dict"`
	}
	rows := make([]item, 500)
	for i := range rows {
		rows[i].Key = fmt.Sprintf("k%04d", i)
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 128}}})
	d := openWith(t, ctx, dirOf(t, dir), OpenOptions{}, false)
	must(t, d.Verify(ctx, nil))
	p := d.r.parts[0]
	meta := &p.footer.Metadata().RowGroups[0].Columns[p.key.index].MetaData
	start, end, ok := chunkRange(meta)
	locs := p.planLocs(0, p.key, start, end)
	if !ok || len(locs) == 0 || locs[0].offset == start {
		t.Fatal("fixture lacks dictionary prefix")
	}
	op, err := d.r.begin(ctx)
	must(t, err)
	defer op.end(nil)
	rp := d.r.newReadPlan(op, nil, nil, 0)
	units := rp.units(p, groupRef{}, [][2]int64{{0, 500}}, nil)
	found := false
	for _, u := range units {
		for _, e := range u.exts {
			found = found || e.off == start && e.size == locs[0].offset-start
		}
	}
	if !found {
		t.Fatal("key dictionary left out of scan wave")
	}
}

func TestWindowPlanningExtentLimits(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 100, PageBytes: 128}, fixtureKeys(800)))
	d := openWith(t, ctx, dirOf(t, dir), OpenOptions{}, false)
	op, err := d.r.begin(ctx)
	must(t, err)
	var st ReadStats
	defer op.end(&st)
	p, err := d.r.part(op, 0, &st)
	must(t, err)
	// Make every routing extent too large for windows. Routing must defer
	// the header check to demand instead of prefetching search candidates.
	d.r.limits.PageBytes = 1
	d.r.routeWindows(op, func(i int) groupRef { return groupRef{0, i} }, 0, len(p.groups))
	if w := op.win.Load(); w != nil && w.held != 0 {
		t.Fatal("oversized routing extent fetched")
	}
	rp := d.r.newReadPlan(op, []string{"size"}, nil, 0)
	for _, u := range rp.units(p, groupRef{}, [][2]int64{{0, 100}}, nil) {
		for _, e := range u.exts {
			if e.size > 1 {
				t.Fatal("oversized page planned", e)
			}
		}
	}
	// With no OffsetIndex, the whole chunk also must stay on demand.
	for i := range p.footer.Metadata().RowGroups[0].Columns {
		p.footer.Metadata().RowGroups[0].Columns[i].OffsetIndexLength = 0
	}
	for _, u := range rp.units(p, groupRef{}, [][2]int64{{0, 100}}, nil) {
		for _, e := range u.exts {
			if e.size > 1 {
				t.Fatal("oversized whole chunk planned", e)
			}
		}
	}
	d.r.buildWindows(op.windows(), groupRef{}, 1)
	if op.win.Load().held != 0 {
		t.Fatal("oversized build extent fetched")
	}
}

func TestReadRangesScalarSourcePanic(t *testing.T) {
	for _, width := range []int{1, 4} {
		op := newOperation(ctx, width)
		var st ReadStats
		errs := readRanges(op, rangeFile{file: panickingWindowReader{}, name: "p", concurrency: width}, testRanges(3, 10), &st)
		if len(errs) != 3 {
			t.Fatalf("%d errors", len(errs))
		}
		for _, err := range errs {
			if !errors.Is(err, errSourcePanic) || !strings.Contains(err.Error(), "the source panicked: source") || !isEnvironment(ctx, err) || RefusalCodeOf(err) != "" {
				t.Fatal(err)
			}
		}
		if st.SourceReads != 3 || st.SourceBytes != 30 || st.ScalarFallbackReads != 3 || op.limit.free != width {
			t.Fatalf("width %d: %+v", width, st)
		}
	}
}

func TestProjectedWindowsKeepEarlierColumns(t *testing.T) {
	type item struct {
		Key  string `parquet:"key"`
		A    int64  `parquet:"a,plain"`
		B    int64  `parquet:"b,plain"`
		Blob string `parquet:"blob,plain"`
		Dict string `parquet:"dict,dict"`
		Last int64  `parquet:"last,plain"`
	}
	rows := make([]item, 100)
	for i := range rows {
		rows[i] = item{fmt.Sprintf("k%04d", i), int64(i), int64(i), strings.Repeat("x", 2048), fmt.Sprintf("%04d-%s", i, strings.Repeat("d", 128)), int64(i)}
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 256}}})
	for _, oversized := range []string{"blob", "dict"} {
		t.Run(oversized, func(t *testing.T) {
			src := &recordingWaveSource{Source: dirOf(t, dir)}
			d := openWith(t, ctx, src, OpenOptions{}, false)
			cols := []string{"a", "b", oversized, "last"}
			_, err := d.Plan(ctx, []Selection{{}}, cols, nil)
			must(t, err)
			d.r.limits.PageBytes = 1000
			p := d.r.parts[0]
			col := p.leaves[oversized]
			loc := (*p.locs[0][col.index].Load())[0]
			start, _, ok := chunkRange(&p.footer.Metadata().RowGroups[0].Columns[col.index].MetaData)
			if !ok || oversized == "blob" && loc.size <= 1000 || oversized == "dict" && loc.offset-start <= 1000 {
				t.Fatal("fixture has no oversized extent")
			}
			op, err := d.r.begin(ctx)
			must(t, err)
			var st ReadStats
			defer op.end(&st)
			w := op.windows()
			src.batches = nil
			d.r.projectWindows(w, p, []position{{groupRef{}, 0}}, cols)
			for _, name := range cols {
				loc := (*p.locs[0][p.leaves[name].index].Load())[0]
				held := w.holds(p, loc.offset, loc.size)
				want := name == "a" || name == "b"
				if held != want {
					t.Fatalf("%s held %v, want %v; batches %v", name, held, want, src.batches)
				}
			}
			if len(src.batches) != 1 {
				t.Fatalf("earlier columns need one batch: %v", src.batches)
			}
		})
	}
}

// Both landing/following and the prediction after an emitted row leave
// pages larger than PageBytes to demand, without fetching later pages.
func TestListReadAheadPageLimit(t *testing.T) {
	dir := write(t, parts(testutil.Part{PageBytes: 128}, fixtureKeys(1000)))
	for _, path := range []string{"landing", "predict"} {
		t.Run(path, func(t *testing.T) {
			src := &recordingWaveSource{Source: dirOf(t, dir)}
			d := openWith(t, ctx, src, OpenOptions{}, false)
			must(t, d.Verify(ctx, nil))
			op, err := d.r.begin(ctx)
			must(t, err)
			var st ReadStats
			defer op.end(&st)
			s := d.r.newScan(op, nil, &st)
			must(t, s.seek(nil, true))
			if len(s.kp.index.pages) < 2 {
				t.Fatal("fixture needs multiple pages")
			}
			d.r.limits.PageBytes = 1
			s.ahead = d.r.newReadAhead(op, 8, 8)
			src.batches = nil
			if path == "landing" {
				s.ahead.landing(s, s.kp, 0)
			} else {
				s.readAhead(&ListQuery{}, 8)
			}
			if len(src.batches) != 0 {
				t.Fatalf("oversized read-ahead fetched: %v", src.batches)
			}
		})
	}
}

func TestProjectedWindowPageLimitBytes(t *testing.T) {
	type item struct {
		Key   string `parquet:"key"`
		Blob  []byte `parquet:"blob"`
		Other []byte `parquet:"other"`
	}
	rows := make([]item, 600)
	for i := range rows {
		rows[i] = item{fmt.Sprintf("k%04d", i), make([]byte, 10<<10), make([]byte, 10<<10)}
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 1 << 20}}})
	for _, path := range []string{"get", "rows", "list"} {
		t.Run(path, func(t *testing.T) {
			var counts [2]int64
			for mode := range counts {
				src := &extentSource{Source: dirOf(t, dir)}
				var source Source = src // the demand baseline has no RangeReader
				if mode == 1 {
					source = &recordingWaveSource{Source: src}
				}
				d := openWith(t, ctx, source, OpenOptions{Limits: Limits{PageBytes: 64 << 10, MetadataMemory: 128 << 10}}, false)
				must(t, d.Verify(ctx, nil))
				src.take()
				var err error
				switch path {
				case "get":
					_, err = d.Get(ctx, []byte(rows[0].Key), nil, "blob", "other")
				case "rows":
					_, err = d.Rows(ctx, Query{Limit: 1, Columns: []string{"blob", "other"}}, nil)
				case "list":
					_, err = d.List(ctx, ListQuery{Limit: 1, Columns: []string{"blob", "other"}}, nil)
				}
				if !errors.Is(err, ErrLimitExceeded) {
					t.Fatal(err)
				}
				for _, rd := range src.take() {
					counts[mode] += rd.len
				}
			}
			if counts[1] > counts[0] {
				t.Fatalf("projected windows transferred %d bytes, demand %d", counts[1], counts[0])
			}
		})
	}
}
