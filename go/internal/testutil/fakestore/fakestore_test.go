// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package fakestore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func data(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 7)
	}
	return b
}

func newSource(opt Options) *Source {
	return New(map[string]Object{"a": {Data: data(1000)}, "b": {Data: data(500)}}, opt)
}

func open(t *testing.T, s *Source, ctx context.Context, name string) *File {
	t.Helper()
	f, err := s.Open(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// TestRoundsSerialAndConcurrent: dependent requests take one round each;
// requests in flight together share one; the virtual times are exact.
func TestRoundsSerialAndConcurrent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s := newSource(Options{Latency: 10 * time.Millisecond, MetadataOnOpen: true})
		op := s.Begin("mixed")
		f := open(t, s, ctx, "a")
		r := f.WithContext(ctx)
		buf := make([]byte, 100)
		for _, off := range []int64{0, 100} { // adjacent, serial
			if _, err := r.ReadAt(buf, off); err != nil {
				t.Fatal(err)
			}
		}
		var wg sync.WaitGroup
		for _, off := range []int64{0, 300, 600, 800} { // a repeat and three more, together
			wg.Go(func() {
				if _, err := r.ReadAt(make([]byte, 100), off); err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		rep := op.End()
		got := rep.Summary()
		want := Summary{Requests: 7, Metadata: 1, Ranges: 6, Bytes: 600, Rounds: 4, MaxInFlight: 4, Repeats: 1, Adjacent: 1, Near: 4}
		if got != want {
			t.Fatalf("summary\n got %+v\nwant %+v\n%s", got, want, rep.Trace(0))
		}
		for _, q := range rep.Requests {
			if q.End-q.Start != 10*time.Millisecond || q.Start != time.Duration(q.Round-1)*10*time.Millisecond {
				t.Fatalf("request %d: %v-%v in round %d", q.Seq, q.Start, q.End, q.Round)
			}
		}
	})
}

// TestMaxInFlight: a source bound to two requests in flight runs eight
// concurrent reads in four rounds, two at a time.
func TestMaxInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s := newSource(Options{Latency: time.Millisecond, MaxInFlight: 2})
		f := open(t, s, ctx, "a")
		op := s.Begin("limited")
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Go(func() { _, _ = f.WithContext(ctx).ReadAt(make([]byte, 10), int64(i*10)) })
		}
		wg.Wait()
		if got := op.End().Summary(); got.MaxInFlight != 2 || got.Rounds != 4 || got.Requests != 8 || got.Adjacent != 7 {
			t.Fatalf("%+v", got)
		}
	})
}

// TestThroughput: a request takes its latency plus its bytes at the rate,
// so a large request and a small one issued together end apart, and the
// one after the small one starts while the large one is in flight.
func TestThroughput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s := newSource(Options{Latency: 10 * time.Millisecond, BytesPerSecond: 10_000})
		f := open(t, s, ctx, "a")
		op := s.Begin("rate")
		var wg sync.WaitGroup
		wg.Go(func() { _, _ = f.WithContext(ctx).ReadAt(make([]byte, 1000), 0) }) // 110 ms
		wg.Go(func() {
			_, _ = f.WithContext(ctx).ReadAt(make([]byte, 100), 0)   // 20 ms
			_, _ = f.WithContext(ctx).ReadAt(make([]byte, 100), 100) // 20-40 ms
		})
		wg.Wait()
		rep := op.End()
		if got := rep.Summary(); got.Rounds != 2 || got.MaxInFlight != 2 || got.Repeats != 0 || got.Overlaps != 2 {
			t.Fatalf("%+v\n%s", got, rep.Trace(0))
		}
	})
}

// TestFaults: each fault does what it says to the request it targets and
// to no other.
func TestFaults(t *testing.T) {
	want := data(1000)
	for _, tc := range []struct {
		fault Fault
		n     int
		err   error
	}{
		{Fault{At: 2, Kind: Fail}, 0, ErrInjected},
		{Fault{At: 2, Kind: ShortRead}, 50, nil},
		{Fault{At: 2, Kind: ShortRead, Bytes: 7}, 7, nil},
		{Fault{At: 2, Kind: FailAfter, Bytes: 30}, 30, ErrInjected},
		{Fault{At: 2, Kind: Hang}, 0, context.DeadlineExceeded},
	} {
		t.Run(tc.fault.Kind.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				s := newSource(Options{Latency: time.Millisecond})
				f := open(t, s, ctx, "a")
				op := s.Begin("fault", tc.fault)
				r := f.WithContext(ctx)
				for i := range 3 {
					buf := make([]byte, 100)
					n, err := r.ReadAt(buf, 100)
					if i == 2 && tc.fault.Kind == Hang { // the hang spent the context
						if !errors.Is(err, context.DeadlineExceeded) {
							t.Fatalf("read after the hang: %v", err)
						}
						continue
					}
					if i != 1 {
						if n != 100 || err != nil || !bytes.Equal(buf, want[100:200]) {
							t.Fatalf("read %d: %d, %v", i+1, n, err)
						}
						continue
					}
					if n != tc.n || !errors.Is(err, tc.err) || err == nil && tc.err != nil || !bytes.Equal(buf[:n], want[100:100+n]) {
						t.Fatalf("faulted read: %d, %v; want %d, %v", n, err, tc.n, tc.err)
					}
				}
				rep := op.End()
				if q := rep.Requests[1]; q.Seq != 2 || q.Fault != tc.fault.Kind || q.Got != int64(tc.n) {
					t.Fatalf("recorded %+v", q)
				}
				if tc.fault.Kind == Hang && rep.Requests[2].Ctx != Expired {
					t.Fatalf("the read after the hang: context %v", rep.Requests[2].Ctx)
				}
			})
		})
	}
}

// TestMetadataFaultAndMissing: Open makes one metadata request, which an
// injected fault fails; a missing object wraps fs.ErrNotExist after it.
// Without metadata requests, Open makes none.
func TestMetadataFaultAndMissing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s := newSource(Options{Latency: time.Millisecond, MetadataOnOpen: true})
		op := s.Begin("open", Fault{At: 1, Kind: Fail})
		if _, err := s.Open(ctx, "a"); !errors.Is(err, ErrInjected) {
			t.Fatalf("faulted open: %v", err)
		}
		if _, err := s.Open(ctx, "missing"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("missing: %v", err)
		}
		if got := op.End().Summary(); got.Metadata != 2 || got.Failed != 2 {
			t.Fatalf("%+v", got)
		}
		s = newSource(Options{Latency: time.Millisecond})
		op = s.Begin("sized")
		open(t, s, ctx, "a")
		if got := op.End().Summary(); got.Requests != 0 {
			t.Fatalf("%+v", got)
		}
	})
}

// TestCancelInFlight: cancelling a context ends its request at once,
// recorded with its error; the source has nothing in flight after.
func TestCancelInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSource(Options{Latency: time.Second})
		f := open(t, s, context.Background(), "b")
		ctx, cancel := context.WithCancel(context.Background())
		op := s.Begin("cancel")
		var err error
		var wg sync.WaitGroup
		wg.Go(func() { _, err = f.WithContext(ctx).ReadAt(make([]byte, 10), 0) })
		time.Sleep(10 * time.Millisecond)
		cancel()
		wg.Wait()
		rep := op.End()
		if !errors.Is(err, context.Canceled) || rep.Requests[0].End != 10*time.Millisecond || s.InFlight() != 0 {
			t.Fatalf("%v\n%s", err, rep.Trace(0))
		}
		// A read whose context already ended starts, and is recorded so.
		op = s.Begin("ended")
		_, _ = f.WithContext(ctx).ReadAt(make([]byte, 10), 0)
		if q := op.End().Requests[0]; q.Ctx != Canceled {
			t.Fatalf("%+v", q)
		}
	})
}

// TestReadAtEOFAndOnDisk: reads past the end follow io.ReaderAt, from
// memory and from disk alike, and unbound reads are recorded as such.
func TestReadAtEOFAndOnDisk(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "d", "x"), data(100), 0o644); err != nil {
		t.Fatal(err)
	}
	mem, err := Load(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	for name, s := range map[string]*Source{"memory": mem, "disk": OnDisk(root, Options{})} {
		op := s.Begin(name)
		f, err := s.Open(context.Background(), "d/x")
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 20)
		if n, err := f.ReadAt(buf, 90); n != 10 || err != io.EOF || !bytes.Equal(buf[:10], data(100)[90:]) {
			t.Fatalf("%s: %d, %v", name, n, err)
		}
		if err := f.Close(); err != nil || s.OpenFiles() != 0 {
			t.Fatalf("%s: close %v, %d open", name, err, s.OpenFiles())
		}
		if got := op.End().Summary(); got.Requests != 1 || got.Unbound != 1 {
			t.Fatalf("%s: %+v", name, got)
		}
	}
}

// TestDeterministic: concurrent workloads report the same summary and
// trace on every run.
func TestDeterministic(t *testing.T) {
	var first string
	for range 10 {
		synctest.Test(t, func(t *testing.T) {
			ctx := context.Background()
			s := newSource(Options{Latency: 3 * time.Millisecond, BytesPerSecond: 1 << 20, MaxInFlight: 3})
			f := open(t, s, ctx, "a")
			op := s.Begin("det")
			var wg sync.WaitGroup
			for g := range 5 {
				wg.Go(func() {
					for i := range 4 {
						_, _ = f.WithContext(ctx).ReadAt(make([]byte, 10*(g+1)), int64(100*i+g))
					}
				})
			}
			wg.Wait()
			tr := op.End().Trace(0)
			if first == "" {
				first = tr
			} else if tr != first {
				t.Fatalf("trace differs:\n%s\nfirst:\n%s", tr, first)
			}
		})
	}
}

// TestCapabilities: a sized open makes no request and its first read pins
// the object, failing on another size or a missing object; a replaced
// object fails a pinned handle's next read; a whole-object read is one
// request within its limit; a batch is one round of requests together.
func TestCapabilities(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s := newSource(Options{Latency: 10 * time.Millisecond, MetadataOnOpen: true})
		op := s.Begin("sized")
		f, err := s.OpenSized(ctx, "a", 1000)
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 10)
		if _, err := f.ReadAt(buf, 0); err != nil || !bytes.Equal(buf, data(1000)[:10]) {
			t.Fatal(err)
		}
		if got := op.End().Summary(); got.Requests != 1 || got.Metadata != 0 {
			t.Fatalf("%+v", got)
		}
		wrong, _ := s.OpenSized(ctx, "a", 999)
		var mismatch *SizeMismatchError
		if _, err := wrong.ReadAt(buf, 0); !errors.As(err, &mismatch) || mismatch.Actual != 1000 || mismatch.Expected != 999 {
			t.Fatalf("wrong size: %v", err)
		}
		missing, _ := s.OpenSized(ctx, "nope", 5)
		if _, err := missing.ReadAt(buf[:1], 0); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("missing: %v", err)
		}
		s.Replace("a", Object{Data: data(1000)})
		if _, err := f.ReadAt(buf, 0); !errors.Is(err, ErrObjectChanged) {
			t.Fatalf("replaced: %v", err)
		}
		op = s.Begin("whole")
		got, id, err := s.ReadObject(ctx, "b", 500)
		if err != nil || !bytes.Equal(got, data(500)) || id != "b#1" {
			t.Fatalf("whole: %v %q", err, id)
		}
		if _, _, err := s.ReadObject(ctx, "b", 499); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("too large: %v", err)
		}
		if got := op.End().Summary(); got.Whole != 2 || got.Bytes != 500 {
			t.Fatalf("%+v", got)
		}
		g, _ := s.Open(ctx, "b")
		op = s.Begin("batch")
		exts := []Extent{{Off: 0, Buf: make([]byte, 10)}, {Off: 100, Buf: make([]byte, 10)}, {Off: 200, Buf: nil}}
		if err := g.ReadRanges(ctx, exts); err != nil || !bytes.Equal(exts[1].Buf, data(500)[100:110]) {
			t.Fatal(err)
		}
		rep := op.End()
		if got := rep.Summary(); got.Requests != 2 || got.Rounds != 1 || got.MaxInFlight != 2 || rep.Requests[0].Batch == 0 {
			t.Fatalf("%+v", got)
		}
		op = s.Begin("batch fault", Fault{At: 2, Kind: Fail})
		err = g.ReadRanges(ctx, exts)
		var be *BatchError
		if !errors.As(err, &be) || (be.Errs[0] == nil) == (be.Errs[1] == nil) || be.Errs[2] != nil {
			t.Fatalf("batch fault: %v", err)
		}
		op.End()
	})
}
