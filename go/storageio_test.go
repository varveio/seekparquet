// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// rangeTestFile serves data through ReadAt and WithContext, each read
// waiting a virtual millisecond, counting reads in flight and failing the
// offsets in fail.
type rangeTestFile struct {
	data              []byte
	fail              map[int64]error
	inFlight, maxSeen atomic.Int64
	calls             atomic.Int64
}

func (f *rangeTestFile) ReadAt(p []byte, off int64) (int, error) {
	n := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for {
		m := f.maxSeen.Load()
		if n <= m || f.maxSeen.CompareAndSwap(m, n) {
			break
		}
	}
	f.calls.Add(1)
	time.Sleep(time.Millisecond)
	if err := f.fail[off]; err != nil {
		return 0, err
	}
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n2 := copy(p, f.data[off:])
	if n2 < len(p) {
		return n2, io.EOF
	}
	return n2, nil
}

// batchTestFile adds ReadRanges, recording each call's length.
type batchTestFile struct {
	*rangeTestFile
	mu    sync.Mutex
	sizes []int
	err   error // returned whole, when set
}

func (f *batchTestFile) ReadRanges(ctx context.Context, ranges []ReadRange) error {
	f.mu.Lock()
	f.sizes = append(f.sizes, len(ranges))
	f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	errs := make([]error, len(ranges))
	failed := false
	for i, rg := range ranges {
		if _, err := f.rangeTestFile.ReadAt(rg.Buf, rg.Off); err != nil && !errors.Is(err, io.EOF) {
			errs[i], failed = err, true
		}
	}
	if failed {
		return &BatchError{Errs: errs}
	}
	return nil
}

func testRanges(n, size int) []ReadRange {
	out := make([]ReadRange, n)
	for i := range out {
		out[i] = ReadRange{Off: int64(i * size), Buf: make([]byte, size)}
	}
	return out
}

// TestReadRangesFallback: without RangeReader, ranges are read by at most
// Concurrency concurrent ReadAt calls, all filled, counted as fallback
// reads; per-range failures keep their positions and classification.
func TestReadRangesFallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := bytes.Repeat([]byte("0123456789"), 100)
		f := &rangeTestFile{data: data, fail: map[int64]error{30: errors.New("boom"), 50: fs.ErrNotExist}}
		ranges := testRanges(10, 10)
		ranges[2].Buf = nil // empty: no read
		var st ReadStats
		errs := readRanges(context.Background(), rangeFile{file: f, name: "p", concurrency: 4}, ranges, &st)
		if f.maxSeen.Load() != 4 || f.calls.Load() != 9 {
			t.Fatalf("%d at once, %d calls", f.maxSeen.Load(), f.calls.Load())
		}
		var se *storageError
		if len(errs) != 10 || !errors.As(errs[3], &se) || RefusalCodeOf(errs[5]) != CodeMissingPart || errs[0] != nil || errs[2] != nil {
			t.Fatalf("errors %v", errs)
		}
		if !bytes.Equal(ranges[9].Buf, data[90:100]) {
			t.Fatal("range 9 not filled")
		}
		if st.SourceReads != 9 || st.SourceBytes != 90 || st.ScalarFallbackReads != 9 || st.UnreportedIOCalls != 9 || st.SourceBatches != 0 {
			t.Fatalf("%+v", st)
		}
	})
}

// TestReadRangesBatched: through RangeReader, ranges go in calls of at
// most MaxReadRanges ranges, irrespective of Concurrency. A *BatchError
// keeps positions, another error
// fails every range of its call, a short range is ErrUnexpectedEOF.
func TestReadRangesBatched(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := bytes.Repeat([]byte("x"), 1000)
		f := &batchTestFile{rangeTestFile: &rangeTestFile{data: data, fail: map[int64]error{70: errors.New("boom")}}}
		var st ReadStats
		ranges := testRanges(10, 10)
		observed := newOperation(context.Background(), 2) // a reporting file observed by an operation
		errs := readRanges(observed, rangeFile{file: f, name: "p", reports: true, concurrency: 4}, ranges, &st)
		if fmt.Sprint(f.sizes) != "[10]" || st.SourceBatches != 1 || st.SourceReads != 10 || st.UnreportedIOCalls != 0 || st.ScalarFallbackReads != 0 {
			t.Fatalf("calls %v, %+v", f.sizes, st)
		}
		for i, err := range errs {
			if (err != nil) != (i == 7) {
				t.Fatalf("range %d: %v", i, err)
			}
		}
		f.err, f.sizes = errors.New("all"), nil
		errs = readRanges(context.Background(), rangeFile{file: f, name: "p", concurrency: 64}, testRanges(3, 10), nil)
		for _, err := range errs {
			if err == nil || err.Error() != "all" {
				t.Fatalf("whole-call error: %v", errs)
			}
		}
		f.err = &BatchError{Errs: []error{nil, io.EOF}}
		errs = readRanges(context.Background(), rangeFile{file: f, name: "p", concurrency: 64}, testRanges(2, 10), nil)
		var se *storageError
		if errs[0] != nil || !errors.As(errs[1], &se) || RefusalCodeOf(errs[1]) != "" {
			t.Fatalf("short range: %v, want the environment's error", errs)
		}
	})
}

// TestReadRangesCancelled: a cancelled context reads nothing and fails
// every non-empty range with its error.
func TestReadRangesCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := &rangeTestFile{data: make([]byte, 100)}
	errs := readRanges(ctx, rangeFile{file: f, concurrency: 8}, testRanges(3, 10), nil)
	if f.calls.Load() != 0 || len(errs) != 3 || !errors.Is(errs[2], context.Canceled) {
		t.Fatalf("%d calls, %v", f.calls.Load(), errs)
	}
}

// TestOpLimit: an operation's limit bounds the ranges in flight across
// concurrent callers, and a waiter whose context ends gives up without
// taking slots.
func TestOpLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		op := newOperation(context.Background(), 3)
		ctx := context.Context(op)
		f := &rangeTestFile{data: make([]byte, 1000)}
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				readRanges(ctx, rangeFile{file: f, concurrency: 3}, testRanges(5, 10), nil)
			})
		}
		wg.Wait()
		if f.maxSeen.Load() != 3 {
			t.Fatalf("%d in flight, limit 3", f.maxSeen.Load())
		}
		l := &op.limit
		if err := l.acquire(context.Background(), 3); err != nil {
			t.Fatal(err)
		}
		short, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := l.acquire(short, 1); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		l.release(3)
		if err := l.acquire(context.Background(), 3); err != nil || l.free != 0 {
			t.Fatalf("%v, %d free", err, l.free)
		}
	})
}

// TestSourceErrorClassification: the source's errors map to refusals
// only for a resized or missing part; a broken pin stays the environment's.
func TestSourceErrorClassification(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		err  error
		code RefusalCode
	}{
		{&SizeMismatchError{Path: "p", Expected: 1, Actual: 2}, CodePartMismatch},
		{fmt.Errorf("open: %w", fs.ErrNotExist), CodeMissingPart},
		{fmt.Errorf("%w: gone: %w", ErrObjectChanged, fs.ErrNotExist), ""},
		{errors.New("reset"), ""},
	} {
		got := sourceError("p", tc.err)
		if RefusalCodeOf(got) != tc.code || tc.code == "" && (!isEnvironment(ctx, got) || !errors.Is(got, tc.err)) {
			t.Errorf("%v: %v (%q)", tc.err, got, RefusalCodeOf(got))
		}
	}
	if err := checkCacheMode(WithCacheMode(ctx, CacheMode(7))); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if err := checkCacheMode(WithCacheMode(ctx, CacheBypass)); err != nil {
		t.Fatal(err)
	}
}

// TestThrowawayStats: an operation whose caller passes no statistics
// counts into a pooled ReadStats, zeroed when taken, and allocates none
// once the pool holds one; a caller's statistics are zeroed and never
// returned to the pool.
func TestThrowawayStats(t *testing.T) {
	st, spare := takeStats(nil)
	if st == nil || spare != st {
		t.Fatalf("takeStats(nil) = %p, %p", st, spare)
	}
	st.SourceReads = 7
	dropStats(spare)
	if allocs := testing.AllocsPerRun(100, func() {
		st, spare := takeStats(nil)
		if *st != (ReadStats{}) {
			t.Fatalf("a pooled ReadStats is not zeroed: %+v", *st)
		}
		st.SourceReads++
		dropStats(spare)
	}); allocs != 0 {
		t.Errorf("%v allocations per operation without statistics", allocs)
	}
	mine := &ReadStats{SourceReads: 3}
	if got, spare := takeStats(mine); got != mine || spare != nil || *mine != (ReadStats{}) {
		t.Errorf("takeStats(mine) = %p, %p, %+v", got, spare, *mine)
	}
}

// gatedFile counts its ReadAt calls, each waiting for gate.
type gatedFile struct {
	gate  chan struct{}
	calls atomic.Int64
}

func (f *gatedFile) ReadAt(p []byte, off int64) (int, error) {
	f.calls.Add(1)
	<-f.gate
	clear(p)
	return len(p), nil
}

// TestReadRangesCountsCalls: a range counts once its read is admitted. With
// Concurrency 2 and one slot held (as by a page read ahead), a pair of
// ranges on scalar fallback reads one, the other queued for a slot; the
// operation cancelled then, the queued range is never read, and the counts
// are the one ReadAt call made.
func TestReadRangesCountsCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, cancel := context.WithCancel(context.Background())
		op := newOperation(c, 2)
		if !op.limit.tryAcquire(1, 0) { // the read ahead's slot
			t.Fatal("no slot")
		}
		f := &gatedFile{gate: make(chan struct{})}
		var st ReadStats
		done := make(chan []error, 1)
		go func() {
			done <- readRanges(op, rangeFile{file: f, name: "p", concurrency: 2}, testRanges(2, 10), &st)
		}()
		synctest.Wait() // one range in ReadAt, the other waiting for a slot
		if f.calls.Load() != 1 {
			t.Fatalf("%d calls made", f.calls.Load())
		}
		cancel()
		synctest.Wait()
		close(f.gate)
		errs := <-done
		failed := 0
		for _, err := range errs {
			if err != nil {
				failed++
			}
		}
		if failed != 1 || f.calls.Load() != 1 ||
			st.SourceReads != 1 || st.SourceBytes != 10 || st.ScalarFallbackReads != 1 || st.UnreportedIOCalls != 1 {
			t.Fatalf("%d calls, errors %v, %+v", f.calls.Load(), errs, st)
		}
		op.limit.release(1)
		if op.limit.free != 2 {
			t.Fatalf("%d slots free", op.limit.free)
		}
	})
}

// panicBatchFile is a RangeReader whose ReadRanges panics.
type panicBatchFile struct{ rangeTestFile }

func (*panicBatchFile) ReadRanges(context.Context, []ReadRange) error { panic("source") }

// TestBatchPanicReleasesSlots: a source panicking in ReadRanges gives the
// operation's slots back.
func TestBatchPanicReleasesSlots(t *testing.T) {
	op := newOperation(context.Background(), 4)
	errs := readRanges(op, rangeFile{file: &panicBatchFile{}, name: "p", concurrency: 4}, testRanges(3, 10), nil)
	for _, err := range errs {
		if !errors.Is(err, errSourcePanic) || !isEnvironment(ctx, err) || RefusalCodeOf(err) != "" {
			t.Fatal(err)
		}
	}
	if op.limit.free != 4 {
		t.Fatalf("%d of 4 slots free after the panic", op.limit.free)
	}
}

// loopError is its own cause.
type loopError struct{}

func (e *loopError) Error() string { return "loop" }
func (e *loopError) Unwrap() error { return e }

// TestOnlyEOFBounded: the walk of an error's causes ends on a cyclic error,
// and on one with more causes than it visits, failing closed.
func TestOnlyEOFBounded(t *testing.T) {
	if onlyEOF(&loopError{}) || onlyEOF(errors.Join(io.EOF, &loopError{})) {
		t.Fatal("a cyclic error is io.EOF")
	}
	wide := make([]error, 2*maxEOFCauses)
	for i := range wide {
		wide[i] = io.EOF
	}
	if onlyEOF(errors.Join(wide...)) {
		t.Fatal("an error with more causes than the walk visits is io.EOF")
	}
	if !onlyEOF(errors.Join(wide[:4]...)) || !onlyEOF(fmt.Errorf("read: %w", io.EOF)) {
		t.Fatal("io.EOF's own causes refused")
	}
}

// A source error can unwrap to itself. Exercise the actual ReadAt boundary:
// onlyEOF rejecting it must not hand it to another unbounded classifier.
type cyclicSourceError struct{}

func (*cyclicSourceError) Error() string   { return "cyclic source failure" }
func (e *cyclicSourceError) Unwrap() error { return e }

type errorReader struct{ err error }

func (r errorReader) ReadAt(p []byte, off int64) (int, error) { return len(p), r.err }
func TestStorageReaderCyclicError(t *testing.T) {
	cycle := new(cyclicSourceError)
	for _, err := range []error{
		cycle,
		errors.Join(io.EOF, cycle),
		errors.Join(&SizeMismatchError{Expected: 1, Actual: 2}, cycle, ErrObjectChanged),
	} {
		var st ReadStats
		_, got := (storageReader{r: errorReader{err}, st: &st, name: "p"}).ReadAt(make([]byte, 3), 0)
		source, ok := got.(*storageError)
		// Avoid errors.Is/As on the returned cyclic cause in the assertion too.
		if !ok || source.err != err || st.SourceReads != 1 || st.SourceBytes != 3 {
			t.Fatalf("classification %T, counts %+v", got, st)
		}
	}
}
