// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
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
// deadline. A short read with no error breaks io.ReaderAt's contract, so
// the data is taken to have lied: a refusal is a correct answer to it.
// Today's reader passes every cell; the later scheduling work must keep
// it so.

// faultPositions bounds the request positions tried per operation and
// fault.
const faultPositions = 12

// faultDeadline ends a hung operation on the virtual clock.
const faultDeadline = time.Hour

var faultKinds = []fakestore.FaultKind{fakestore.Fail, fakestore.ShortRead, fakestore.FailAfter, fakestore.Hang}

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
			return func(ctx context.Context) (string, error) { return "", d.Verify(ctx) }
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
func runFaulted(t *testing.T, fx *ioFixture, op *faultOp, faults ...fakestore.Fault) faultRun {
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
				d, err = seekparquet.OpenDataset(ctx, fakeSource{src}, seekparquet.OpenOptions{})
				if err != nil {
					return "", err
				}
				return fmt.Sprint(len(d.Metadata().Parts)), nil
			}
		} else {
			var err error
			d, err = seekparquet.OpenDataset(ctx, fakeSource{src}, seekparquet.OpenOptions{})
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
	for i := -1; i < len(ops); i++ {
		var op *faultOp
		name := "open"
		if i >= 0 {
			op, name = &ops[i], ops[i].name
		}
		t.Run(name, func(t *testing.T) {
			clean := runFaulted(t, fx, op)
			if clean.err != nil || clean.inFlight != 0 || clean.leaked != 0 {
				t.Fatalf("clean run: %v, %d in flight, %d goroutines left\n%s", clean.err, clean.inFlight, clean.leaked, clean.report.Trace(40))
			}
			n := len(clean.report.Requests)
			for _, kind := range faultKinds {
				for _, at := range positions(n, faultPositions) {
					cells++
					got := runFaulted(t, fx, op, fakestore.Fault{At: at, Kind: kind})
					where := fmt.Sprintf("%s at request %d of %d", kind, at, n)
					outcomes[fmt.Sprintf("%s: %s", kind, outcome(got.err))]++
					switch {
					case got.err == nil && got.text != clean.text:
						t.Errorf("%s: a wrong result and no error:\n%.600s\nwant\n%.600s\n%s", where, got.text, clean.text, got.report.Trace(40))
					case got.inFlight != 0:
						t.Errorf("%s: %d requests in flight after the operation returned (%v)", where, got.inFlight, got.err)
					case got.leaked != 0:
						t.Errorf("%s: %d goroutines left after the operation returned (%v)", where, got.leaked, got.err)
					case (kind == fakestore.Fail || kind == fakestore.FailAfter) && got.err != nil && (!errors.Is(got.err, fakestore.ErrInjected) || seekparquet.RefusalCodeOf(got.err) != ""):
						t.Errorf("%s: %v (refusal %q), want the source's error", where, got.err, seekparquet.RefusalCodeOf(got.err))
					case kind == fakestore.Hang && got.err != nil && !errors.Is(got.err, context.DeadlineExceeded):
						t.Errorf("%s: %v, want the context's deadline", where, got.err)
					}
				}
			}
		})
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
