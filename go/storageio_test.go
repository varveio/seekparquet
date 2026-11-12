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
// most Concurrency ranges; a *BatchError keeps positions, another error
// fails every range of its call, a short range is ErrUnexpectedEOF.
func TestReadRangesBatched(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := bytes.Repeat([]byte("x"), 1000)
		f := &batchTestFile{rangeTestFile: &rangeTestFile{data: data, fail: map[int64]error{70: errors.New("boom")}}}
		var st ReadStats
		ranges := testRanges(10, 10)
		errs := readRanges(context.Background(), rangeFile{file: f, name: "p", reports: true, concurrency: 4}, ranges, &st)
		if fmt.Sprint(f.sizes) != "[4 4 2]" || st.SourceBatches != 3 || st.SourceReads != 10 || st.UnreportedIOCalls != 0 || st.ScalarFallbackReads != 0 {
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
		if errs[0] != nil || !errors.Is(errs[1], io.ErrUnexpectedEOF) {
			t.Fatalf("short range: %v", errs)
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
		ctx := withOpLimit(context.Background(), 3)
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
		l := opLimitOf(ctx)
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
