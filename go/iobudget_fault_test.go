// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/internal/testutil/fakestore"
)

// The fault matrix: every operation of the budgets, on the small fault
// fixture, run with each fault injected at each of its requests (at most
// faultPositions of them, spread evenly and always the first and last).
// The operation must return an error or the correct result, never a
// wrong one; must end, by its context's deadline at the latest, on the
// virtual clock; must leave no request in flight and no goroutine behind
// when it returns. A failed request (Fail, FailAfter) must surface as the
// source's error, never as a refusal (CHECK-5), and a hung one as the
// deadline. A short read with no error breaks io.ReaderAt's contract: it
// is the source's failure, an environment error carrying
// io.ErrUnexpectedEOF, never a refusal; so is a read that returned every
// byte with an error (FullThenFail). The one exception is a whole manifest
// read short with no error (ManifestReader), which the reader cannot tell
// from a short manifest. A retried request (RetryOnce) gives the correct
// result.
// Today's reader passes every cell; the later scheduling work must keep
// it so.

// faultPositions bounds the request positions tried per operation and
// fault.
const faultPositions = 12

// raceFaultPositions is faultPositions under the race detector, which
// slows every cell tenfold: the first, the last and two between still
// cover every phase, within the race shard's time; the full matrix runs
// without it.
const raceFaultPositions = 4

// faultDeadline ends a hung operation on the virtual clock.
const faultDeadline = time.Hour

var faultKinds = []fakestore.FaultKind{fakestore.Fail, fakestore.ShortRead, fakestore.FailAfter, fakestore.Hang, fakestore.FullThenFail, fakestore.RetryOnce}

// faultOp is one operation: setup prepares it on a dataset, unrecorded
// and fault-free, and returns the run that is faulted.
type faultOp struct {
	name  string
	setup func(ctx context.Context, d *seekparquet.Dataset) func(ctx context.Context) (string, error)
}

func faultOps(fx *ioFixture, cursor *seekparquet.Cursor) []faultOp {
	partSpan := seekparquet.Span{Start: fx.first, End: succ(fx.last)}
	warm := func(run func(ctx context.Context) (string, error)) func(ctx context.Context) (string, error) {
		_, _ = run(context.Background())
		return run
	}
	rows := func(d *seekparquet.Dataset, q seekparquet.Query) func(ctx context.Context) (string, error) {
		return func(ctx context.Context) (string, error) { return rowsText(d.Rows(ctx, q, nil)) }
	}
	list := func(d *seekparquet.Dataset, q seekparquet.ListQuery) func(ctx context.Context) (string, error) {
		return func(ctx context.Context) (string, error) { return listText(d.List(ctx, q, nil)) }
	}
	listQ := seekparquet.ListQuery{Prefix: fx.listPrefix, Delimiter: '/', Limit: 1000}
	ra8 := listQ
	ra8.ReadAhead = 8
	page2 := listQ
	page2.Cursor = cursor
	scan := func(d *seekparquet.Dataset, q seekparquet.Query) func(ctx context.Context) (string, error) {
		return func(ctx context.Context) (string, error) {
			h := newBatchHash()
			return h.text(d.Scan(ctx, q, nil, h.add))
		}
	}
	plan := func(ctx context.Context, d *seekparquet.Dataset, sels []seekparquet.Selection, cols []string) *seekparquet.Plan {
		p, err := d.Plan(ctx, sels, cols, nil)
		if err != nil {
			panic(err)
		}
		return p
	}
	var twenty []seekparquet.Selection
	for _, k := range fx.twenty {
		twenty = append(twenty, seekparquet.Selection{Span: seekparquet.KeySpan(k)})
	}
	return []faultOp{
		{"rows-key/mid/cold", func(_ context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			return rows(d, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.KeySpan(fx.mid)}, Limit: 1, Columns: ioColumns})
		}},
		{"rows-key/mid/warm", func(_ context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			return warm(rows(d, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.KeySpan(fx.mid)}, Limit: 1, Columns: ioColumns}))
		}},
		{"get/last/cold", func(_ context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			return func(ctx context.Context) (string, error) { return rowsText(d.Get(ctx, fx.last, nil, ioColumns...)) }
		}},
		{"rows-100/2-cols/cold", func(_ context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			return rows(d, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.Span{Start: fx.mid}}, Limit: 100, Columns: ioColumns})
		}},
		{"list-ra0/page1-cold", func(_ context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			return list(d, listQ)
		}},
		{"list-ra0/page2", func(_ context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			return list(d, page2)
		}},
		{"list-ra8/page1-cold", func(_ context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			return list(d, ra8)
		}},
		{"list-ra8/page1-warm", func(_ context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			return warm(list(d, ra8))
		}},
		{"build-list-index", func(_ context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			return func(ctx context.Context) (string, error) {
				// A build that leaves failed groups unindexed succeeds
				// (ListIndexStats.GroupsFailed): its result is the
				// listing after it, which must be the right one.
				if _, err := d.BuildListIndex(ctx, seekparquet.ListIndexOptions{}); err != nil {
					return "", err
				}
				return listText(d.List(ctx, listQ, nil))
			}
		}},
		{"list-indexed-ra0/page1-cold", func(ctx context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			if _, err := d.BuildListIndex(ctx, seekparquet.ListIndexOptions{}); err != nil {
				panic(err)
			}
			return list(d, listQ)
		}},
		{"scan-part/cold", func(_ context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			return scan(d, seekparquet.Query{Selection: seekparquet.Selection{Span: partSpan}, Columns: []string{"size"}})
		}},
		{"plan-20/plan-cold", func(_ context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			return func(ctx context.Context) (string, error) { return planText(d.Plan(ctx, twenty, ioColumns, nil)) }
		}},
		{"plan-20/scan-cold", func(ctx context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			p := plan(ctx, d, twenty, ioColumns)
			return func(ctx context.Context) (string, error) {
				h := newBatchHash()
				return h.text(p.Scan(ctx, nil, h.add))
			}
		}},
		{"split/shards-concurrent", func(ctx context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			shards, err := plan(ctx, d, []seekparquet.Selection{{Span: partSpan}}, []string{"size"}).Split(ctx, 4, nil)
			if err != nil {
				panic(err)
			}
			return func(ctx context.Context) (string, error) { return shardsText(ctx, shards) }
		}},
		{"verify/cold", func(_ context.Context, d *seekparquet.Dataset) func(context.Context) (string, error) {
			return func(ctx context.Context) (string, error) { return "", d.Verify(ctx, nil) }
		}},
	}
}

// positions returns at most k request positions of n: all of them, or
// the first, the last and evenly spaced ones between.
func positions(n, k int) []int {
	var out []int
	if n <= k {
		for i := 1; i <= n; i++ {
			out = append(out, i)
		}
		return out
	}
	for i := range k {
		p := 1 + i*(n-1)/(k-1)
		if len(out) == 0 || out[len(out)-1] != p {
			out = append(out, p)
		}
	}
	return out
}

// faultRun is the outcome of one run of an operation.
type faultRun struct {
	text     string
	err      error
	report   fakestore.Report
	inFlight int64
	leaked   int // goroutines more than before the operation
}

// runFaulted runs op on a fresh store, in its own bubble, with faults
// injected into the operation (none: the clean run). The open is the
// operation when op is nil.
func runFaulted(t *testing.T, fx *ioFixture, store ioStore, op *faultOp, faults ...fakestore.Fault) faultRun {
	t.Helper()
	var out faultRun
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), faultDeadline)
		defer cancel()
		src := fakestore.New(fx.objects, ioOptions)
		var d *seekparquet.Dataset
		var run func(context.Context) (string, error)
		if op == nil {
			run = func(ctx context.Context) (string, error) {
				var err error
				d, err = seekparquet.OpenDataset(ctx, store.wrap(src), store.opt)
				if err != nil {
					return "", err
				}
				return fmt.Sprint(len(d.Metadata().Parts)), nil
			}
		} else {
			var err error
			d, err = seekparquet.OpenDataset(ctx, store.wrap(src), store.opt)
			must(t, err)
			run = op.setup(ctx, d)
		}
		synctest.Wait()
		before := bubbleGoroutines()
		rec := src.Begin("faulted", faults...)
		out.text, out.err = run(ctx)
		out.report = rec.End()
		out.inFlight = src.InFlight()
		out.leaked = goroutinesAbove(before)
		if d != nil {
			must(t, d.Close())
		}
		if n := src.OpenFiles(); n != 0 {
			t.Errorf("%d files open after Close", n)
		}
	})
	return out
}

// goroutinesAbove returns how many more goroutines of this bubble exist
// than before, once every other one of them is durably blocked or gone.
func goroutinesAbove(before int) int {
	synctest.Wait()
	return bubbleGoroutines() - before
}

// bubbleGoroutines counts the goroutines of the calling goroutine's
// synctest bubble, from their stacks: runtime.NumGoroutine counts the
// whole process, whose other goroutines come and go on the wall clock.
func bubbleGoroutines() int {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	// The calling goroutine's header comes first: "goroutine N [running,
	// synctest bubble B]:".
	header, _, _ := strings.Cut(string(buf), "\n")
	i := strings.Index(header, "synctest bubble ")
	if i < 0 {
		panic("bubbleGoroutines outside a synctest bubble")
	}
	tag := strings.TrimSuffix(header[i:], ":")
	count := 0
	for _, line := range strings.Split(string(buf), "\n") {
		if strings.HasPrefix(line, "goroutine ") && strings.Contains(line, tag) {
			count++
		}
	}
	return count
}

// TestFaultMatrix runs the matrix.
func TestFaultMatrix(t *testing.T) {
	seekparquet.NoTestReadAhead(t)
	fx := ioFixtureOf(t, ioTiny)
	local, err := seekparquet.OpenDir(context.Background(), fx.dir, seekparquet.OpenOptions{})
	must(t, err)
	ents, err := local.List(context.Background(), seekparquet.ListQuery{Prefix: fx.listPrefix, Delimiter: '/', Limit: 1000}, nil)
	must(t, err)
	must(t, local.Close())
	ops := faultOps(fx, ents[len(ents)-1].Cursor())
	cells := 0
	outcomes := map[string]int{}
	tried := faultPositions
	if raceEnabled {
		tried = raceFaultPositions
	}
	for _, store := range ioStores {
		for i := -1; i < len(ops); i++ {
			var op *faultOp
			name := "open"
			if i >= 0 {
				op, name = &ops[i], ops[i].name
			}
			t.Run(store.name+"/"+name, func(t *testing.T) {
				clean := runFaulted(t, fx, store, op)
				if clean.err != nil || clean.inFlight != 0 || clean.leaked != 0 {
					t.Fatalf("clean run: %v, %d in flight, %d goroutines left\n%s", clean.err, clean.inFlight, clean.leaked, clean.report.Trace(40))
				}
				n := len(clean.report.Requests)
				for _, kind := range faultKinds {
					for _, at := range positions(n, tried) {
						cells++
						got := runFaulted(t, fx, store, op, fakestore.Fault{At: at, Kind: kind})
						where := fmt.Sprintf("%s at request %d of %d", kind, at, n)
						outcomes[fmt.Sprintf("%s: %s", kind, outcome(got.err))]++
						switch {
						case got.err == nil && got.text != clean.text:
							t.Errorf("%s: a wrong result and no error:\n%.600s\nwant\n%.600s\n%s", where, got.text, clean.text, got.report.Trace(40))
						case got.inFlight != 0:
							t.Errorf("%s: %d requests in flight after the operation returned (%v)", where, got.inFlight, got.err)
						case got.leaked != 0:
							t.Errorf("%s: %d goroutines left after the operation returned (%v)", where, got.leaked, got.err)
						case (kind == fakestore.Fail || kind == fakestore.FailAfter || kind == fakestore.FullThenFail) && got.err != nil && (!errors.Is(got.err, fakestore.ErrInjected) || seekparquet.RefusalCodeOf(got.err) != ""):
							t.Errorf("%s: %v (refusal %q), want the source's error", where, got.err, seekparquet.RefusalCodeOf(got.err))
						case kind == fakestore.ShortRead && got.err != nil && !(op == nil && store.name != "plain") &&
							(!errors.Is(got.err, io.ErrUnexpectedEOF) && !errors.Is(got.err, fakestore.ErrInjected) || seekparquet.RefusalCodeOf(got.err) != ""): // a metadata request's fault fails the open
							t.Errorf("%s: %v (refusal %q), want the environment's short read", where, got.err, seekparquet.RefusalCodeOf(got.err))
						case kind == fakestore.RetryOnce && (got.err != nil || got.text != clean.text):
							t.Errorf("%s: a retried request: %v", where, got.err)
						case kind == fakestore.Hang && got.err != nil && !errors.Is(got.err, context.DeadlineExceeded):
							t.Errorf("%s: %v, want the context's deadline", where, got.err)
						}
					}
				}
			})
		}
	}
	t.Logf("%d cells: %v", cells, outcomes)
}

// outcome classifies a faulted operation's error.
func outcome(err error) string {
	switch {
	case err == nil:
		return "correct result"
	case seekparquet.RefusalCodeOf(err) != "":
		return "refusal"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, fakestore.ErrInjected):
		return "source error"
	}
	return "other error"
}

// TestStorageFaults covers the storage paths the capabilities add, through
// the plain and the capable store alike, with the same outcome on both: a
// part of another size than its manifest entry is refused part_mismatch,
// whether Open's size or a sized open's first read finds it; a missing
// part missing_part, found by Open's metadata request or by the first
// read; a part replaced after it was pinned fails the next read with
// ErrObjectChanged, an environment error and never a refusal, and is not
// refused later; a manifest beyond Limits.ManifestBytes fails with a
// *LimitError naming ManifestBytes; a short manifest fails the open.
func TestStorageFaults(t *testing.T) {
	fx := ioFixtureOf(t, ioTiny)
	part := "data/part-00001.parquet"
	key := fx.mid
	get := func(ctx context.Context, d *seekparquet.Dataset) error {
		_, err := d.Get(ctx, key, nil, ioColumns...)
		return err
	}
	type outcome struct {
		code   seekparquet.RefusalCode
		part   string
		errStr string
	}
	run := func(t *testing.T, store ioStore, opt seekparquet.OpenOptions, setup func(*fakestore.Source), op func(context.Context, *seekparquet.Dataset, *fakestore.Source) error) (openErr, opErr error) {
		synctest.Test(t, func(t *testing.T) {
			ctx := context.Background()
			src := fakestore.New(fx.objects, ioOptions)
			if setup != nil {
				setup(src)
			}
			store.opt = opt
			d, err := seekparquet.OpenDataset(ctx, store.wrap(src), store.opt)
			if err != nil {
				openErr = err
				return
			}
			defer d.Close()
			opErr = op(ctx, d, src)
		})
		return openErr, opErr
	}
	refusal := func(err error) outcome {
		var re *seekparquet.RefusalError
		if errors.As(err, &re) {
			return outcome{code: re.Code, part: re.Part}
		}
		return outcome{errStr: fmt.Sprint(err)}
	}
	for _, store := range ioStores {
		t.Run(store.name, func(t *testing.T) {
			// Another size: the object is 10 bytes longer than its entry.
			_, err := run(t, store, seekparquet.OpenOptions{}, func(s *fakestore.Source) {
				o := fx.objects[part]
				s.Replace(part, fakestore.Object{Data: append(bytes.Clone(o.Data), make([]byte, 10)...)})
			}, func(ctx context.Context, d *seekparquet.Dataset, _ *fakestore.Source) error {
				if err := get(ctx, d); err == nil {
					return errors.New("no error")
				}
				return d.Verify(ctx, nil) // the refusal is kept
			})
			if got := refusal(err); got != (outcome{code: seekparquet.CodePartMismatch, part: part}) {
				t.Errorf("resized part: %v", err)
			}
			// Missing.
			_, err = run(t, store, seekparquet.OpenOptions{}, func(s *fakestore.Source) { s.Remove(part) },
				func(ctx context.Context, d *seekparquet.Dataset, _ *fakestore.Source) error { return get(ctx, d) })
			if got := refusal(err); got != (outcome{code: seekparquet.CodeMissingPart, part: part}) {
				t.Errorf("missing part: %v", err)
			}
			// Replaced after it was pinned: the first read pins it.
			_, err = run(t, store, seekparquet.OpenOptions{}, nil, func(ctx context.Context, d *seekparquet.Dataset, s *fakestore.Source) error {
				if err := get(ctx, d); err != nil {
					return fmt.Errorf("before the replacement: %w", err)
				}
				s.Replace(part, fx.objects[part])
				_, err := d.Get(ctx, fx.first, nil, ioColumns...)
				if !errors.Is(err, seekparquet.ErrObjectChanged) || seekparquet.RefusalCodeOf(err) != "" {
					return fmt.Errorf("after the replacement: %v", err)
				}
				_, err = d.Get(ctx, fx.last, nil, ioColumns...)
				if !errors.Is(err, seekparquet.ErrObjectChanged) {
					return fmt.Errorf("again after the replacement: %v", err)
				}
				return nil
			})
			if err != nil {
				t.Error(err)
			}
			// A manifest one byte beyond the limit.
			limit := int64(len(fx.objects[seekparquet.ManifestName].Data)) - 1
			openErr, _ := run(t, store, seekparquet.OpenOptions{Limits: seekparquet.Limits{ManifestBytes: limit}}, nil, nil)
			var le *seekparquet.LimitError
			if !errors.As(openErr, &le) || le.Limit != seekparquet.LimitManifestBytes || le.Max != limit {
				t.Errorf("oversized manifest: %v", openErr)
			}
			// A short manifest: the read returns half of it.
			synctest.Test(t, func(t *testing.T) {
				src := fakestore.New(fx.objects, ioOptions)
				op := src.Begin("open", fakestore.Fault{At: 1 + boolInt(store.name == "plain"), Kind: fakestore.ShortRead})
				_, err := seekparquet.OpenDataset(context.Background(), store.wrap(src), store.opt)
				op.End()
				if err == nil {
					t.Error("a short manifest opened")
				}
			})
		})
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// countingObserver counts the physical requests reported to it.
type countingObserver struct{ begun, ended, bytes, hits, failures atomic.Int64 }

func (o *countingObserver) Begin(_ fakestore.Kind, _ string, _, _ int64, _ bool) func(int64, error) {
	o.begun.Add(1)
	return func(n int64, err error) {
		o.ended.Add(1)
		o.bytes.Add(n)
		if err != nil {
			o.failures.Add(1)
		}
	}
}
func (o *countingObserver) CacheHit(n int64)                 { o.hits.Add(n) }
func (o *countingObserver) BatchPlanned(int64, int64, int64) {}

// TestStorageObserver: storage observes physical requests through the
// caller's context, including opening, independently of logical ReadStats.
func TestStorageObserver(t *testing.T) {
	fx := ioFixtureOf(t, ioTiny)
	for _, store := range ioStores {
		t.Run(store.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				src := fakestore.New(fx.objects, ioOptions)
				var open seekparquet.ReadStats
				opt := store.opt
				opt.OpenStats = &open
				var openObs countingObserver
				rec := src.Begin("open")
				d, err := seekparquet.OpenDataset(fakestore.WithObserver(context.Background(), &openObs), store.wrap(src), opt)
				must(t, err)
				defer d.Close()
				want := rec.End().Summary()
				if openObs.begun.Load() != int64(want.Requests) || openObs.ended.Load() != int64(want.Requests) || want.MaxInFlight != 1 {
					t.Fatalf("open: %+v, the store made %+v", open, want)
				}
				var obs countingObserver
				ctx := fakestore.WithObserver(context.Background(), &obs)
				var st seekparquet.ReadStats
				rec = src.Begin("get")
				_, err = d.Get(ctx, fx.mid, &st, ioColumns...)
				must(t, err)
				got := rec.End().Summary()
				if obs.begun.Load() != int64(got.Requests) || obs.ended.Load() != int64(got.Requests) || obs.bytes.Load() != got.Bytes || obs.failures.Load() != 0 {
					t.Fatalf("get: %+v, the store made %+v, the observer saw %d/%d", st, got, obs.begun.Load(), obs.ended.Load())
				}
			})
		})
	}
}

// TestStorageRequestCounting covers open, cold and warm lookup, listing
// read-ahead, and failed first checks with observation owned by storage.
func TestStorageRequestCounting(t *testing.T) {
	fx := ioFixtureOf(t, ioTiny)
	synctest.Test(t, func(t *testing.T) {
		src := fakestore.New(fx.objects, ioOptions)
		// opens is the Source's own calls the operation made: the
		// manifest's open, or its parts'.
		var obs countingObserver
		ctx := fakestore.WithObserver(context.Background(), &obs)
		check := func(what string, st seekparquet.ReadStats, rec *fakestore.Operation, opens int64) {
			t.Helper()
			rep := rec.End()
			made := int64(len(rep.Requests))
			var consumed, failures int64
			for _, request := range rep.Requests {
				consumed += request.Got
				if request.Err != nil {
					failures++
				}
			}
			if made == 0 || obs.begun.Load() != made || obs.ended.Load() != made || obs.bytes.Load() != consumed || obs.failures.Load() != failures {
				t.Fatalf("%s: store made %d requests, observer saw %d/%d (%d bytes)", what, made, obs.begun.Load(), obs.ended.Load(), obs.bytes.Load())
			}
			if made != st.SourceReads+opens && what != "open" {
				t.Fatalf("%s: %d requests, %d logical reads + %d opens", what, made, st.SourceReads, opens)
			}
			obs = countingObserver{}
		}
		source := fakeSource{src}
		var open seekparquet.ReadStats
		rec := src.Begin("open")
		d, err := seekparquet.OpenDataset(ctx, source, seekparquet.OpenOptions{OpenStats: &open})
		must(t, err)
		defer d.Close()
		check("open", open, rec, 1)
		for _, temp := range []string{"cold", "warm"} {
			var st seekparquet.ReadStats
			rec := src.Begin(temp)
			_, err := d.Get(ctx, fx.mid, &st, ioColumns...)
			must(t, err)
			check(temp, st, rec, st.PartsOpened)
		}
		listed, err := seekparquet.OpenDataset(ctx, source, seekparquet.OpenOptions{})
		must(t, err)
		defer listed.Close()
		var st seekparquet.ReadStats
		obs = countingObserver{}
		rec = src.Begin("list")
		_, err = listed.List(ctx, seekparquet.ListQuery{Prefix: fx.listPrefix, Limit: 300, ReadAhead: 4}, &st)
		must(t, err)
		if st.ReadAheadReads == 0 {
			t.Fatalf("list: nothing read ahead: %+v", st)
		}
		check("cold list", st, rec, st.PartsOpened)
		failing, err := seekparquet.OpenDataset(ctx, source, seekparquet.OpenOptions{})
		must(t, err)
		defer failing.Close()
		st = seekparquet.ReadStats{}
		obs = countingObserver{}
		rec = src.Begin("failed", fakestore.Fault{At: 2, Kind: fakestore.Fail})
		if _, err := failing.Get(ctx, fx.mid, &st, ioColumns...); err == nil {
			t.Fatal("the failed check succeeded")
		}
		check("failed check", st, rec, st.PartsOpened)
	})
}

// TestScanSourceCache: the caller controls byte caching at the store.
func TestScanSourceCache(t *testing.T) {
	fx := ioFixtureOf(t, ioTiny)
	synctest.Test(t, func(t *testing.T) {
		opt := ioOptions
		opt.Cache = true
		var obs countingObserver
		ctx := fakestore.WithObserver(context.Background(), &obs)
		src := fakestore.New(fx.objects, opt)
		d, err := seekparquet.OpenDataset(context.Background(), capableSource{fakeSource{src}}, seekparquet.OpenOptions{})
		must(t, err)
		defer d.Close()
		var st seekparquet.ReadStats
		for range 2 {
			_, err = d.Get(ctx, fx.mid, &st, ioColumns...)
			must(t, err)
		}
		if obs.hits.Load() == 0 {
			t.Fatal("a repeated lookup hit no cached byte")
		}
		before := obs.hits.Load()
		q := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.Span{Start: fx.first, End: succ(fx.last)}}, Columns: []string{"size"}}
		for range 2 {
			must(t, d.Scan(fakestore.WithBypass(ctx), q, &st, func(*seekparquet.Batch) error { return nil }))
		}
		if obs.hits.Load() != before {
			t.Fatalf("a scan hit %d cached bytes", obs.hits.Load()-before)
		}
	})
}

// TestVerifyPhysicalPeak: storage limits and records physical concurrency
// independently of the reader's internal logical read limit.
func TestVerifyPhysicalPeak(t *testing.T) {
	fx := ioFixtureOf(t, ioTiny)
	for _, n := range []int{1, 3} {
		synctest.Test(t, func(t *testing.T) {
			opt := ioOptions
			opt.MaxInFlight = n
			src := fakestore.New(fx.objects, opt)
			d, err := seekparquet.OpenDataset(context.Background(), fakeSource{src}, seekparquet.OpenOptions{})
			must(t, err)
			defer d.Close()
			var st seekparquet.ReadStats
			var obs countingObserver
			ctx := fakestore.WithObserver(context.Background(), &obs)
			rec := src.Begin("verify")
			must(t, d.Verify(ctx, &st))
			made := rec.End().Summary()
			if made.MaxInFlight != n || obs.begun.Load() != int64(made.Requests) || obs.ended.Load() != int64(made.Requests) ||
				st.PartsOpened != int64(fx.spec.parts) {
				t.Fatalf("Concurrency %d: %+v, the store made %+v", n, st, made)
			}
		})
	}
}
