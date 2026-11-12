// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/parquet-go/parquet-go"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

// windowFile publishes successful members only. A failed member deliberately
// fills its buffer too: callers must use the outcome, never its contents.
type windowFile struct {
	data           []byte
	failOff        int64
	failure        error
	short          bool
	calls, batches atomic.Int64
}

func (f *windowFile) ReadAt(buf []byte, off int64) (int, error) {
	f.calls.Add(1)
	return bytes.NewReader(f.data).ReadAt(buf, off)
}
func (f *windowFile) ReadRanges(ctx context.Context, ranges []ReadRange) error {
	f.batches.Add(1)
	errs := make([]error, len(ranges))
	failed := false
	for i, rg := range ranges {
		_, errs[i] = bytes.NewReader(f.data).ReadAt(rg.Buf, rg.Off)
		if rg.Off == f.failOff && f.failure != nil {
			errs[i] = f.failure
		}
		if f.short && rg.Off == f.failOff {
			errs[i] = io.ErrUnexpectedEOF
		}
		if err := ctx.Err(); err != nil {
			errs[i] = err
		}
		failed = failed || errs[i] != nil
	}
	if failed {
		return &BatchError{Errs: errs}
	}
	return nil
}
func testWindows(t *testing.T, f io.ReaderAt) (*operation, *windows, *part) {
	t.Helper()
	op := newOperation(context.Background(), 2)
	op.r = &reader{policy: ReadPolicy{Concurrency: 2}}
	w := op.windows()
	p := &part{r: op.r, name: "p", file: f}
	t.Cleanup(func() { w.close(nil) })
	return op, w, p
}

func TestWindowsCompleteCoverage(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefghijk"), int(2*readWindowBytes/11)+11)
	for _, tc := range []struct {
		name        string
		spec, short bool
		failure     error
	}{
		{name: "complete"},
		{name: "short", short: true, spec: true},
		{name: "failed speculative", spec: true, failure: ErrObjectChanged},
		{name: "failed demanded", failure: ErrObjectChanged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &windowFile{data: data, failOff: readWindowBytes, failure: tc.failure, short: tc.short}
			op, w, p := testWindows(t, f)
			if tc.spec {
				w.fetchAhead([]extent{{p, 0, int64(len(data)), 1}})
			} else if n := w.fetch([]extent{{p, 0, int64(len(data)), 1}}, false); n != 1 {
				t.Fatal(n)
			}
			if len(w.list) != 3 {
				t.Fatalf("%d windows, want 3", len(w.list))
			}
			buf := bytes.Repeat([]byte{0xff}, len(data)-2)
			pristine := bytes.Clone(buf)
			hit, err := w.copy(p, buf, 1)
			failed := tc.failure != nil || tc.short
			if failed {
				if hit || !bytes.Equal(buf, pristine) {
					t.Fatal("exposed bytes from incomplete coverage")
				}
				if !tc.spec && !errors.Is(err, ErrObjectChanged) {
					t.Fatal(err)
				}
				if tc.spec && err != nil {
					t.Fatal(err)
				}
			} else if !hit || err != nil || !bytes.Equal(buf, data[1:len(data)-1]) {
				t.Fatalf("complete three-window copy: hit %v, err %v", hit, err)
			}
			var st ReadStats
			err = p.read(op, &st, buf, 1)
			if failed && !tc.spec {
				if !errors.Is(err, ErrObjectChanged) || f.calls.Load() != 0 {
					t.Fatal("retried demanded failure", err)
				}
			} else if err != nil || !bytes.Equal(buf, data[1:len(data)-1]) {
				t.Fatal("bad data or fallback", err)
			}
			w.close(&st)
			fallback := int64(0)
			if failed && tc.spec {
				fallback = 1
			}
			if st.SourceReads != 3+fallback || st.SourceBatches != 1 || f.calls.Load() != fallback ||
				st.SourceBytes != int64(len(data))+fallback*int64(len(buf)) || op.limit.free != op.limit.size || w.held != 0 {
				t.Fatalf("counters or ownership: %+v, held %d, calls %d", st, w.held, f.calls.Load())
			}
			if tc.spec && (st.ReadAheadReads != 1 || st.ReadAheadUnused != fallback || st.ReadAheadUnusedBytes != int64(len(data))*fallback) {
				t.Fatalf("read-ahead counters: %+v", st)
			}
		})
	}
	t.Run("gap and identity", func(t *testing.T) {
		f := &windowFile{data: []byte("0123456789")}
		op, w, p := testWindows(t, f)
		w.fetchAhead([]extent{{p, 0, 4, 1}, {p, 5, 5, 1}})
		buf := bytes.Repeat([]byte{0xff}, 10)
		if hit, err := w.copy(p, buf, 0); hit || err != nil || !bytes.Equal(buf, bytes.Repeat([]byte{0xff}, 10)) {
			t.Fatal("served a gap")
		}
		other := &part{r: p.r, name: p.name, file: bytes.NewReader([]byte("xxxxxxxxxx"))}
		if hit, _ := w.copy(other, make([]byte, 4), 0); hit {
			t.Fatal("served another handle")
		}
		var st ReadStats
		must(t, p.read(op, &st, buf, 0))
		w.close(&st)
		if string(buf) != "0123456789" || st.SourceReads != 3 || st.ReadAheadUnused != 2 || st.ReadAheadUnusedBytes != 9 {
			t.Fatalf("%q %+v", buf, st)
		}
	})
	t.Run("cancelled", func(t *testing.T) {
		f := &windowFile{data: []byte("abc")}
		op, w, p := testWindows(t, f)
		w.fetch([]extent{{p, 0, 3, 1}}, true)
		w.cancel()
		buf := []byte("xxx")
		if hit, err := w.copy(p, buf, 0); hit || err != nil || string(buf) != "xxx" {
			t.Fatal(hit, err, string(buf))
		}
		_ = op
	})
}

func TestWindowsBoundAndRefill(t *testing.T) {
	f := &windowFile{data: []byte("abcdefgh")}
	_, w, p := testWindows(t, f)
	// Even demanded read-ahead is optional when the next allocation cannot fit.
	w.held = readWaveBytes
	if n := w.fetch([]extent{{p, 0, 4, 1}}, false); n != 0 || f.batches.Load() != 0 {
		t.Fatal("admitted beyond fixed bound")
	}
	w.held = 0
	if n := w.fetch([]extent{{p, 0, readWaveBytes + 1, 1}}, false); n != 0 {
		t.Fatal("admitted oversized run")
	}
	w.fetch([]extent{{p, 0, 4, 1}}, false)
	w.release(1)
	if w.held != 0 {
		t.Fatal("passed rows still held")
	}
	w.fetch([]extent{{p, 4, 4, 2}}, false)
	buf := make([]byte, 4)
	if hit, err := w.copy(p, buf, 4); !hit || err != nil || string(buf) != "efgh" {
		t.Fatal("refill", hit, err)
	}
}

// Actual encoded headers and dictionaries are split at every byte. Both the
// dictionary path and the located page path must assemble complete coverage.
func TestWindowsHeaderAndDictionary(t *testing.T) {
	type row struct {
		Key  string `parquet:"key,dict"`
		Name string `parquet:"name,dict"`
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: []row{{"a", "red"}, {"b", "blue"}, {"c", "red"}}}}})
	d := open(t, dir, OpenOptions{})
	must(t, d.Verify(ctx, nil))
	p := d.r.parts[0]
	op, err := d.r.begin(ctx)
	must(t, err)
	var st ReadStats
	defer op.end(&st)
	w := op.windows()
	phys := p.groups[0].physical
	col := p.leaves["name"]
	meta := &p.footer.Metadata().RowGroups[phys].Columns[col.index].MetaData
	start, end, ok := chunkRange(meta)
	if !ok || meta.DictionaryPageOffset == 0 {
		t.Fatal("fixture lacks dictionary")
	}
	// Install independently completed, one-byte reads of the encoded chunk.
	for off := start; off < end; off++ {
		x := &window{p: p, off: off, buf: make([]byte, 1), rowEnd: 3, done: make(chan struct{})}
		must(t, p.at(op, &st).readFull(x.buf, off))
		close(x.done)
		w.list = append(w.list, x)
		w.held++
	}
	reads := st.SourceReads
	cp, err := p.openColumn(op, phys, "name", &st, nil)
	must(t, err)
	// The index itself is not in the chunk. Only page/header/dictionary reads
	// below are expected to be served from windows.
	reads = st.SourceReads
	var values []string
	must(t, cp.read([]position{{groupRef{}, 0}, {groupRef{}, 1}, {groupRef{}, 2}}, func(_ int, v parquet.Value) { values = append(values, v.String()) }))
	if st.SourceReads != reads || len(values) != 3 || values[0] != "red" || values[1] != "blue" || values[2] != "red" {
		t.Fatalf("%v, %+v", values, st)
	}
}

type blockedWindowFile struct {
	io.ReaderAt
	started, cancelled, finish chan struct{}
	closed                     atomic.Bool
	afterClose                 atomic.Bool
	inFlight                   atomic.Int64
}

func (f *blockedWindowFile) ReadRanges(ctx context.Context, ranges []ReadRange) error {
	f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	if observer := IOObserverFromContext(ctx); observer != nil {
		done := observer.Begin(IORequest{Kind: IORange, Path: "p", Offset: ranges[0].Off, Length: int64(len(ranges[0].Buf))})
		defer func() { done(IOResult{Err: ctx.Err()}) }()
	}
	close(f.started)
	<-ctx.Done()
	close(f.cancelled)
	<-f.finish
	if f.closed.Load() {
		f.afterClose.Store(true)
	}
	// A source owns its buffers until it returns, even after cancellation.
	for _, rg := range ranges {
		clear(rg.Buf)
	}
	return ctx.Err()
}
func (f *blockedWindowFile) Close() error { f.closed.Store(true); return closeFile(f.ReaderAt) }

func TestWindowsCloseJoinsWave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := write(t, parts(testutil.Part{}, []string{"a", "b", "c"}))
		d := open(t, dir, OpenOptions{})
		must(t, d.Verify(ctx, nil))
		p := d.r.parts[0]
		original := p.file
		defer func() { p.file = original }()
		f := &blockedWindowFile{ReaderAt: original, started: make(chan struct{}), cancelled: make(chan struct{}), finish: make(chan struct{})}
		p.file = f
		op, err := d.r.begin(ctx)
		must(t, err)
		w := op.windows()
		ended := make(chan struct{})
		var completed ReadStats
		go func() {
			st, spare := takeStats(nil)
			w.fetch([]extent{{p, 4, 3, 3}}, true)
			op.end(st)
			completed = *st
			dropStats(spare)
			close(ended)
		}()
		<-f.started
		closed := make(chan error, 1)
		go func() { closed <- d.Close() }()
		<-f.cancelled
		synctest.Wait()
		if f.closed.Load() || w.held != 3 {
			t.Fatal("freed buffer or handle while source still owned it")
		}
		select {
		case <-ended:
			t.Fatal("operation returned before source")
		default:
		}
		close(f.finish)
		<-ended
		must(t, <-closed)
		if !f.closed.Load() || f.afterClose.Load() || f.inFlight.Load() != 0 || w.held != 0 || op.limit.free != op.limit.size ||
			completed.SourceReads != 1 || completed.SourceBatches != 1 || completed.PhysicalRequests != 1 || completed.PhysicalFailures != 1 {
			t.Fatal("wave leaked resources or used closed handle")
		}
	})
}

func TestWindowsOperationExit(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 30, PageBytes: 64}, fixtureKeys(200)))
	for _, tc := range []struct {
		name       string
		result     error
		panicValue bool
	}{
		{name: "stop", result: Stop}, {name: "error", result: io.ErrClosedPipe}, {name: "panic", panicValue: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := openWith(t, ctx, dirOf(t, dir), OpenOptions{}, false)
			must(t, d.Verify(ctx, nil))
			callbacks := 0
			func() {
				defer func() {
					if got := recover(); tc.panicValue != (got != nil) {
						t.Fatalf("panic %v", got)
					}
				}()
				err := d.Scan(ctx, Query{Limit: math.MaxInt64}, nil, func(b *Batch) error {
					callbacks++
					if tc.panicValue {
						panic("callback")
					}
					return tc.result
				})
				if tc.result == Stop {
					must(t, err)
				} else if !errors.Is(err, tc.result) {
					t.Fatal(err)
				}
			}()
			if callbacks != 1 || d.r.active != 0 {
				t.Fatal("operation survived callback", callbacks, d.r.active)
			}
			// Immediately reuse pooled nil statistics and the same adopted handle.
			rows, err := d.Rows(ctx, Query{Limit: 1}, nil)
			must(t, err)
			if len(rows) != 1 {
				t.Fatal(rows)
			}
			must(t, d.Close())
		})
	}
}

// recordingWaveSource records descriptors, never the caller-owned buffers.
type recordingWaveSource struct {
	Source
	batches [][][2]int64
}
type recordingWaveFile struct {
	SourceFile
	owner *recordingWaveSource
}

func (s *recordingWaveSource) Open(ctx context.Context, name string) (SourceFile, error) {
	f, err := s.Source.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return &recordingWaveFile{f, s}, nil
}
func (f *recordingWaveFile) ReadRanges(ctx context.Context, ranges []ReadRange) error {
	batch := make([][2]int64, len(ranges))
	errs := make([]error, len(ranges))
	failed := false
	for i, rg := range ranges {
		batch[i] = [2]int64{rg.Off, int64(len(rg.Buf))}
		if err := ctx.Err(); err != nil {
			errs[i] = err
		} else {
			_, errs[i] = f.SourceFile.ReadAt(rg.Buf, rg.Off)
		}
		failed = failed || errs[i] != nil
	}
	// Tests use one handle and synchronous waves, so this recorder needs no lock.
	f.owner.batches = append(f.owner.batches, batch)
	if failed {
		return &BatchError{Errs: errs}
	}
	return nil
}

func TestProjectedPageWave(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 1000, PageBytes: 128}, fixtureKeys(1000)))
	for _, prefetch := range []int64{64, 2 << 20} {
		t.Run(fmt.Sprint(prefetch), func(t *testing.T) {
			src := &recordingWaveSource{Source: dirOf(t, dir)}
			d := openWith(t, ctx, src, OpenOptions{Read: ReadPolicy{MetadataPrefetchBytes: prefetch}}, false)
			must(t, d.Verify(ctx, nil))
			src.batches = nil
			var st ReadStats
			rows, err := d.Rows(ctx, Query{Limit: 2, Columns: []string{"size", "name"}}, &st)
			must(t, err)
			if len(rows) != 2 || rows[0].Values[0].Int64() != 0 || rows[1].Values[0].Int64() != 1 {
				t.Fatal(rows)
			}
			wantCalls := 1
			if prefetch == 64 {
				wantCalls = 2
			} // Missing indexes together, then their selected pages.
			if len(src.batches) != wantCalls {
				t.Fatalf("%d batches, want %d: %v", len(src.batches), wantCalls, src.batches)
			}
			p := d.r.parts[0]
			batch := src.batches[len(src.batches)-1]
			if len(batch) != 2 {
				t.Fatalf("selected pages should be fetched together: %v", batch)
			}
			for _, name := range []string{"size", "name"} {
				col := p.leaves[name]
				locs := *p.locs[0][col.index].Load()
				found := false
				for _, rg := range batch {
					found = found || rg == [2]int64{locs[0].offset, locs[0].size}
				}
				if !found {
					t.Fatalf("missing selected page of %s: %v", name, batch)
				}
			}
			// A subsequent operation with nil stats must get clean state and reuse
			// the adopted handle; it still batches only the selected projected pages.
			src.batches = nil
			_, err = d.Get(ctx, rows[0].Key, nil, "size", "name")
			must(t, err)
			if len(src.batches) != 1 || len(src.batches[0]) != 2 {
				t.Fatal(src.batches)
			}
		})
	}
}

func TestRoutingProbeWave(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 100, PageBytes: 128}, fixtureKeys(800)))
	src := &recordingWaveSource{Source: dirOf(t, dir)}
	d := openWith(t, ctx, src, OpenOptions{}, false)
	// Shared part checks read only metadata: no unconditional key probes.
	op, err := d.r.begin(ctx)
	must(t, err)
	var st ReadStats
	_, err = d.r.part(op, 0, &st)
	must(t, err)
	op.end(&st)
	if len(src.batches) != 0 {
		t.Fatal("part check read key candidates", src.batches)
	}
	p := d.r.parts[0]
	op, err = d.r.begin(ctx)
	must(t, err)
	defer op.end(&st)
	var log pageLog
	_, err = d.r.groupFor(op, []byte(fixtureKeys(800)[550]), true, &log, &st)
	must(t, err)
	if len(src.batches) != 1 || len(src.batches[0]) != 7 {
		t.Fatalf("three search levels should form one seven-probe batch: %v", src.batches)
	}
	// The batch must not decode the candidates outside the actual route.
	known := 0
	for _, grp := range p.groups {
		if grp.first.Load() != nil {
			known++
		}
	}
	if known >= 7 {
		t.Fatal("decoded unused probes", known)
	}
}

func TestReadPlanRefillsWithinGroup(t *testing.T) {
	dir := write(t, parts(testutil.Part{PageBytes: 128}, fixtureKeys(1000)))
	src := &recordingWaveSource{Source: dirOf(t, dir)}
	d := openWith(t, ctx, src, OpenOptions{}, false)
	must(t, d.Verify(ctx, nil))
	op, err := d.r.begin(ctx)
	must(t, err)
	var st ReadStats
	defer op.end(&st)
	s := d.r.newScan(op, nil, &st)
	s.plan = d.r.newReadPlan(op, nil, nil, 0)
	s.plan.waveBytes = 2048 // Exercise refill inside a single group without a large fixture.
	must(t, s.start(nil, false, nil))
	rows := 0
	for s.key != nil {
		rows++
		must(t, s.next())
	}
	if rows != 1000 || st.SourceReads != 0 {
		t.Fatalf("%d rows, demand reads %+v", rows, st)
	}
	if len(src.batches) < 2 {
		t.Fatalf("did not refill inside group: %d batches", len(src.batches))
	}
	if op.win.Load().held > readWaveBytes {
		t.Fatal("exceeded fixed wave bound")
	}
}

func TestReadAheadPageCounters(t *testing.T) {
	f := &windowFile{data: bytes.Repeat([]byte("a"), 30)}
	_, w, p := testWindows(t, f)
	w.fetchAhead([]extent{{p, 0, 10, 1}, {p, 10, 10, 1}, {p, 20, 10, 1}})
	if len(w.list) != 1 {
		t.Fatal("adjacent pages did not merge")
	}
	if hit, err := w.copy(p, make([]byte, 10), 10); !hit || err != nil {
		t.Fatal(hit, err)
	}
	var st ReadStats
	w.close(&st)
	if st.SourceReads != 1 || st.ReadAheadReads != 3 || st.ReadAheadUnused != 2 || st.ReadAheadUnusedBytes != 20 {
		t.Fatalf("merged pages: %+v", st)
	}
	// Scan and routing speculation do not count as listing page read-ahead.
	_, w, p = testWindows(t, f)
	w.fetch([]extent{{p, 0, 10, 1}}, true)
	st = ReadStats{}
	w.close(&st)
	if st.ReadAheadReads != 0 || st.ReadAheadUnused != 0 || st.ReadAheadUnusedBytes != 0 {
		t.Fatalf("non-listing wave: %+v", st)
	}
}

type panickingWindowReader struct{}

func (panickingWindowReader) ReadAt([]byte, int64) (int, error) { panic("source") }
func TestWindowsPanicCountsInvocation(t *testing.T) {
	op, w, p := testWindows(t, panickingWindowReader{})
	w.fetch([]extent{{p, 0, 10, 1}}, false)
	hit, err := w.copy(p, make([]byte, 10), 0)
	if hit || !errors.Is(err, errSourcePanic) || !isEnvironment(ctx, err) || RefusalCodeOf(err) != "" {
		t.Fatal(hit, err)
	}
	var st ReadStats
	w.close(&st)
	if st.SourceReads != 1 || st.SourceBytes != 10 || st.ScalarFallbackReads != 1 || op.limit.free != op.limit.size {
		t.Fatalf("lost panicking invocation: %+v", st)
	}
}

func TestZeroPolicyOpenDirReadsOnDemand(t *testing.T) {
	dir := write(t, parts(testutil.Part{PageBytes: 128}, fixtureKeys(100)))
	d := open(t, dir, OpenOptions{})
	must(t, d.Verify(ctx, nil))
	// Warm the projected locations, as on main: the next read is exactly
	// one key page and one page of each projected column.
	_, err := d.Rows(ctx, Query{Limit: 1, Columns: []string{"size", "name"}}, nil)
	must(t, err)
	op, err := d.r.begin(ctx)
	must(t, err)
	var st ReadStats
	defer op.end(&st)
	s := d.r.newScan(op, nil, &st)
	s.plan = d.r.newReadPlan(op, []string{"size", "name"}, nil, 1)
	s.ahead = d.r.newReadAhead(op, 0, 1)
	if s.plan != nil || s.ahead != nil {
		t.Fatal("zero-policy OpenDir started planner")
	}
	must(t, s.start(nil, false, nil))
	row := new(Row)
	must(t, d.r.project(op, []*Row{row}, []position{s.at()}, []string{"size", "name"}, &st))
	if !d.r.local || op.win.Load() != nil || st.SourceReads != 3 || st.SourceBatches != 0 || st.RequestMemoHitBytes != 0 || st.MetadataHitBytes != 0 {
		t.Fatalf("local direct path changed: %+v", st)
	}
}

func TestProjectedDictionaryWave(t *testing.T) {
	type item struct {
		Key   string `parquet:"key"`
		Name  string `parquet:"name,dict"`
		Color string `parquet:"color,dict"`
	}
	var rows []item
	for i := range 500 {
		rows = append(rows, item{fmt.Sprintf("k%04d", i), "name", "red"})
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 64}}})
	src := &recordingWaveSource{Source: dirOf(t, dir)}
	d := openWith(t, ctx, src, OpenOptions{}, false)
	must(t, d.Verify(ctx, nil))
	src.batches = nil
	var st ReadStats
	got, err := d.Rows(ctx, Query{Limit: 2, Columns: []string{"name", "color"}}, &st)
	must(t, err)
	if len(got) != 2 || got[0].Values[0].String() != "name" || got[1].Values[1].String() != "red" {
		t.Fatal(got)
	}
	if len(src.batches) != 1 || st.SourceReads != 1+int64(len(src.batches[0])) {
		t.Fatalf("dictionary not fetched with selected pages: %v, %+v", src.batches, st)
	}
}

func TestProjectedMixedDictionaryWave(t *testing.T) {
	rows := make([]fallbackRow, 700)
	for i := range rows {
		rows[i] = fallbackRow{Key: fmt.Appendf(nil, "k%04d", i), Size: int64(i) * 1000}
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: rows, PageBytes: 256, DictionaryMaxBytes: 2048}}})
	extents := &extentSource{Source: dirOf(t, dir)}
	src := &recordingWaveSource{Source: extents}
	d := openWith(t, ctx, src, OpenOptions{Limits: Limits{PageBytes: 1024}}, false)
	must(t, d.Verify(ctx, nil))
	meta := d.r.parts[0].footer.Metadata().RowGroups[0].Columns[1].MetaData
	if meta.DictionaryPageOffset == 0 {
		t.Fatal("fixture lacks dictionary")
	}
	extents.take()
	got, err := d.Rows(ctx, Query{Selection: Selection{Span: KeySpan(rows[len(rows)-1].Key)}, Limit: 1, Columns: []string{"size"}}, nil)
	must(t, err)
	if len(got) != 1 || got[0].Values[0].Int64() != rows[len(rows)-1].Size {
		t.Fatal(got)
	}
	dict := chunkExtent{testutil.PartPath(0), meta.DictionaryPageOffset, meta.DataPageOffset}
	if n := chunkBytes(t, "batched mixed dictionary", extents.take(), []chunkExtent{dict}); n != 0 {
		t.Fatalf("PLAIN selection fetched %d dictionary bytes", n)
	}
}
