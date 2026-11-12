// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// Ported from swath scan_test.go: scanDir and TestScanShardsPerWorker.
func parallelFixture(t *testing.T, n, group int, repeated bool) (*Dataset, *Plan) {
	t.Helper()
	keys := make([]string, n)
	for i := range keys {
		k := i
		if repeated {
			k -= i % 3
		}
		keys[i] = fmt.Sprintf("a/%07d", k)
	}
	d := open(t, write(t, testutil.Dataset{RepeatedKeys: repeated, Parts: []testutil.Part{{Rows: testutil.Rows(keys...), GroupRows: group, PageBytes: 256}}}), OpenOptions{})
	p, err := d.Plan(ctx, []Selection{{}}, []string{"size"}, nil)
	must(t, err)
	return d, p
}
func TestScanParallelShards(t *testing.T) {
	for _, repeated := range []bool{false, true} {
		_, p := parallelFixture(t, 1200, 30, repeated)
		var want []Row
		must(t, p.Scan(ctx, nil, func(b *Batch) error {
			for i := range b.Len() {
				want = append(want, b.Row(i))
			}
			return nil
		}))
		for _, per := range []int{1, 2, 8, 16} {
			old := shardsPerWorker
			shardsPerWorker = per
			for _, workers := range []int{0, 1, 2, 7} {
				var now, most atomic.Int64
				var mu sync.Mutex
				var got []Row
				var st ReadStats
				must(t, p.ScanParallel(ctx, workers, &st, func(b *Batch) error {
					n := now.Add(1)
					defer now.Add(-1)
					for m := most.Load(); n > m && !most.CompareAndSwap(m, n); m = most.Load() {
					}
					runtime.Gosched()
					for i := 1; i < b.Len(); i++ {
						if bytes.Compare(b.Key(i-1), b.Key(i)) > 0 {
							return errors.New("unordered batch")
						}
					}
					mu.Lock()
					defer mu.Unlock()
					for i := range b.Len() {
						got = append(got, b.Row(i))
					}
					return nil
				}))
				slices.SortFunc(got, func(a, b Row) int {
					if c := bytes.Compare(a.Key, b.Key); c != 0 {
						return c
					}
					if a.Offset < b.Offset {
						return -1
					}
					if a.Offset > b.Offset {
						return 1
					}
					return 0
				})
				if !reflect.DeepEqual(got, want) || most.Load() > int64(max(workers, 1)) || st.KeysDecoded == 0 {
					t.Fatalf("repeated=%v workers=%d per=%d rows=%d stats=%+v", repeated, workers, per, len(got), st)
				}
			}
			shardsPerWorker = old
		}
	}
	if scanShards(1) != 1 || scanShards(math.MaxInt) < 1 {
		t.Fatal("shard sizing overflow")
	}
}

// Ported from swath TestScanSlowShardDoesNotHoldTheRest.
func TestScanParallelSlowShard(t *testing.T) {
	_, p := parallelFixture(t, 1600, 100, false)
	c, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var others atomic.Int64
	done := make(chan struct{})
	var once sync.Once
	must(t, p.ScanParallel(c, 2, nil, func(b *Batch) error {
		if string(b.Key(0)) != "a/0000000" {
			if others.Add(int64(b.Len())) >= 1200 {
				once.Do(func() { close(done) })
			}
			return nil
		}
		select {
		case <-done:
			return nil
		case <-c.Done():
			return fmt.Errorf("only %d rows while first shard held", others.Load())
		}
	}))
}

// swath TestScanCallbackStopIsAnError adapted to SeekParquet's successful Stop.
func TestScanParallelStopErrorsStats(t *testing.T) {
	_, p := parallelFixture(t, 1600, 100, false)
	failure := errors.New("callback identity")
	for _, workers := range []int{1, 4} {
		for _, e := range []error{Stop, failure} {
			var calls atomic.Int64
			var st ReadStats
			err := p.ScanParallel(ctx, workers, &st, func(*Batch) error { calls.Add(1); return e })
			want := e
			if e == Stop {
				want = nil
			}
			if err != want || calls.Load() < 1 || calls.Load() > int64(workers) || st.KeyPagesDecoded == 0 {
				t.Fatalf("%d workers err=%v calls=%d stats=%+v", workers, err, calls.Load(), st)
			}
		}
	}
	c, cancel := context.WithCancel(ctx)
	cancel()
	if err := p.ScanParallel(c, 4, nil, func(*Batch) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	c, cancel = context.WithCancel(ctx)
	if err := p.ScanParallel(c, 4, nil, func(*Batch) error { cancel(); return Stop }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := p.ScanParallel(ctx, 4, nil, nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
}

// Ported from swath TestShardBoundaryOrder and TestTotalsShardErrors. Each
// individual shard is ordered; corruption appears only across its boundary.
func TestScanParallelBoundaryCorruption(t *testing.T) {
	for _, keys := range [][]string{
		{"a", "b", "c", "z", "d", "e", "f", "zz"},
		{"a", "b", "c", "d", "d", "e", "f", "g"},
		{"a", "b", "c", "d", "e", "f", "g", "h", "g", "i", "j", "k"},
	} {
		d := open(t, write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows(keys...), GroupRows: 4, PageBytes: 32}}}), OpenOptions{})
		p, err := d.Plan(ctx, []Selection{{}}, []string{"size"}, nil)
		must(t, err)
		for _, workers := range []int{1, 2, 4, 8} {
			var st ReadStats
			err := p.ScanParallel(ctx, workers, &st, func(*Batch) error { return nil })
			if RefusalCodeOf(err) != CodeKeyOrder || st.KeyPagesDecoded == 0 {
				t.Fatalf("keys=%v workers=%d: %v (%+v)", keys, workers, err, st)
			}
			out, err := d.Aggregate(ctx, []Selection{{}, {Span: KeySpan([]byte("b"))}}, []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}}, AggregateOptions{ValueBudgetBytes: -1, Workers: workers, CheckKeys: true}, nil)
			if RefusalCodeOf(err) != CodeKeyOrder || out != nil {
				t.Fatalf("aggregate workers=%d: %v %+v", workers, err, out)
			}
		}
	}
}

func TestScanParallelCloseJoins(t *testing.T) {
	d, p := parallelFixture(t, 300, 30, false)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	scanned := make(chan error, 1)
	go func() {
		scanned <- p.ScanParallel(ctx, 2, nil, func(*Batch) error { once.Do(func() { close(entered) }); <-release; return nil })
	}()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned inside callback: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	err := <-scanned
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("scan: %v", err)
	}
	must(t, <-closed)
	if err := p.ScanParallel(ctx, 1, nil, func(*Batch) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

// Ported unchanged from swath TestShardOrder.
func TestScanParallelShardOrder(t *testing.T) {
	k := func(first, last string) shardKeys {
		return shardKeys{first: []byte(first), last: []byte(last), seen: true}
	}
	for _, c := range []struct {
		keys []shardKeys
		bad  string
	}{
		{nil, ""},
		{[]shardKeys{{}, k("a", "c"), {}, k("d", "f"), k("g", "g")}, ""},
		{[]shardKeys{k("a", "c"), {}, k("c", "f")}, "c"},
		{[]shardKeys{k("a", "c"), k("b", "f")}, "b"},
		{[]shardKeys{k("a", "c"), k("d", "f"), k("e", "z")}, "e"},
	} {
		err := shardOrder(c.keys)
		var r *RefusalError
		switch {
		case c.bad == "" && err != nil:
			t.Errorf("%v: %v", c.keys, err)
		case c.bad != "" && (!errors.As(err, &r) || r.Code != CodeKeyOrder || string(r.Key) != c.bad):
			t.Errorf("%v: %v, want key_order at %q", c.keys, err, c.bad)
		}
	}
}

func TestScanParallelStopKeepsRealErrors(t *testing.T) {
	_, p := parallelFixture(t, 300, 10, false)
	for _, failure := range []error{context.Canceled, errors.New("real error wins over Stop")} {
		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		var calls atomic.Int64
		done := make(chan error, 1)
		go func() {
			done <- p.ScanParallel(ctx, 2, nil, func(*Batch) error {
				n := calls.Add(1)
				entered <- struct{}{}
				<-release
				if n == 1 {
					return Stop
				}
				return failure
			})
		}()
		<-entered
		<-entered
		close(release)
		if err := <-done; err != failure {
			t.Fatalf("got %v want identity %v", err, failure)
		}
	}
}

func TestScanParallelSplitFailureStats(t *testing.T) {
	// A repeated-key split reads boundary key pages. Fail those reads after
	// planning; split failure must keep the work and callback must not run.
	dir := write(t, testutil.Dataset{RepeatedKeys: true, Parts: []testutil.Part{{Rows: testutil.Rows("a", "b", "c", "d", "e", "f"), GroupRows: 2, PageBytes: 32}}})
	failure := errors.New("split storage failure")
	src := &readHookSource{Source: dirOf(t, dir)}
	ds, err := OpenDataset(ctx, src, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}})
	must(t, err)
	defer ds.Close()
	plan, err := ds.Plan(ctx, []Selection{{}}, nil, nil)
	must(t, err)
	src.read = func(string, int, int64) error { return failure }
	var st ReadStats
	err = plan.ScanParallel(ctx, 3, &st, func(*Batch) error { t.Fatal("callback after split failure"); return nil })
	if !errors.Is(err, failure) || st.SourceReads == 0 {
		t.Fatal(err, st)
	}
}

func TestScanParallelDisjointRepeatedPositions(t *testing.T) {
	d := verified(t, write(t, testutil.Dataset{RepeatedKeys: true, Parts: []testutil.Part{{Rows: testutil.Rows("a", "a", "a", "a", "b", "b"), GroupRows: 2, PageBytes: 32}}}), OpenOptions{})
	spans := []RowSpan{d.r.rowSpan(0, 1), d.r.rowSpan(2, 3), d.r.rowSpan(5, 6)}
	p, err := d.PlanRows(ctx, spans, []string{"size"}, nil)
	must(t, err)
	for _, workers := range []int{1, 2, 4} {
		var mu sync.Mutex
		var offsets []int64
		must(t, p.ScanParallel(ctx, workers, nil, func(b *Batch) error {
			mu.Lock()
			defer mu.Unlock()
			for i := range b.Len() {
				offsets = append(offsets, b.Offset(i))
			}
			return nil
		}))
		slices.Sort(offsets)
		if !slices.Equal(offsets, []int64{0, 1, 2}) {
			t.Fatal(offsets)
		}
		out, err := d.AggregateRows(ctx, spans, []Reduction{{Op: CountRows}, {Op: Sum, Column: "size"}}, AggregateOptions{ValueBudgetBytes: -1, Workers: workers, CheckKeys: true}, nil)
		must(t, err)
		for i, n := range []int64{0, 2, 5} {
			if out[i][0].Value.Int64() != 1 || out[i][1].Value.Int64() != n {
				t.Fatal(out)
			}
		}
	}
}

// Ported from swath 0d204aa4 TestShardBoundaryRepeatedKeys: four equal
// keys cross three-row groups and must remain together at every worker count.
func TestScanParallelShardBoundaryRepeatedKeys(t *testing.T) {
	d := open(t, write(t, testutil.Dataset{RepeatedKeys: true, Parts: []testutil.Part{{Rows: testutil.Rows("a", "b", "c", "c", "c", "c", "d", "e"), GroupRows: 3}}}), OpenOptions{})
	p, err := d.Plan(ctx, []Selection{{}}, []string{"size"}, nil)
	must(t, err)
	var want []Row
	must(t, p.Scan(ctx, nil, func(b *Batch) error {
		for i := range b.Len() {
			want = append(want, b.Row(i))
		}
		return nil
	}))
	if len(want) != 8 {
		t.Fatal(want)
	}
	for _, workers := range []int{1, 2, 4, 8} {
		var mu sync.Mutex
		var got []Row
		must(t, p.ScanParallel(ctx, workers, nil, func(b *Batch) error {
			mu.Lock()
			defer mu.Unlock()
			for i := range b.Len() {
				got = append(got, b.Row(i))
			}
			return nil
		}))
		slices.SortStableFunc(got, func(a, b Row) int { return bytes.Compare(a.Key, b.Key) })
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("workers=%d got=%+v want=%+v", workers, got, want)
		}
	}
}
