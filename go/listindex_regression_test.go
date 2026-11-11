// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

func TestListIndexRegressionArbitraryPrefixCursor(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 100, PageBytes: 200}, []string{"a/x", "a/y", "b/z"}))
	plain := open(t, dir, Options{})
	indexed := open(t, dir, Options{})
	st, err := indexed.BuildListIndex(ctx, ListIndexOptions{})
	must(t, err)
	q := List{Prefix: []byte("a/"), Delimiter: '/', Limit: 100, Cursor: &Cursor{Prefix: []byte("a/x")}}
	want, e1 := plain.List(ctx, q)
	got, e2 := indexed.List(ctx, q)
	if e1 != nil || e2 != nil || !reflect.DeepEqual(want, got) {
		t.Fatalf("build=%+v want=%v got=%v err=%v/%v", st, entriesOf(want), entriesOf(got), e1, e2)
	}
}
func TestListIndexRegressionDescendingFirstKeys(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 2, PageBytes: 200}, []string{"a/0", "a/1", "c/0", "c/1", "b/0", "d/0"}))
	d := open(t, dir, Options{})
	st, err := d.BuildListIndex(ctx, ListIndexOptions{Workers: 1})
	must(t, err)
	t.Logf("build: %+v", st)
	plain := open(t, dir, Options{})
	q := List{Delimiter: '/', Limit: 2}
	want, e1 := plain.List(ctx, q)
	got, e2 := d.List(ctx, q)
	if CodeOf(e1) != CodeKeyOrder || CodeOf(e2) != CodeKeyOrder {
		t.Fatalf("plain=%v/%v indexed=%v/%v", entriesOf(want), e1, entriesOf(got), e2)
	}
	if st.GroupsFailed != 2 || st.GroupsIndexed != 1 {
		t.Fatalf("both sides must be omitted: %+v", st)
	}
}
func TestListIndexRegressionCancelledBuildPages(t *testing.T) {
	keys := dirKeys(map[string]int{"a": 100, "b": 100}, "a", "b")
	dir := write(t, parts(testutil.Part{GroupRows: 200, PageBytes: 200}, keys))
	d := open(t, dir, Options{})
	must(t, d.r.checkParts(ctx, new(ReadStats)))
	p := d.r.parts[0]
	idx, err := p.keyIndex(ctx, 0, new(ReadStats))
	must(t, err)
	original := p.file
	c, cancel := context.WithCancel(ctx)
	defer cancel()
	p.file = &listFaultReader{ReaderAt: original, offset: idx.pages[2].offset, cancel: cancel}
	defer func() { p.file = original }()
	st, err := d.BuildListIndex(c, ListIndexOptions{Workers: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if st.Pages < 2 {
		t.Fatalf("two earlier pages decoded but stats=%+v", st)
	}
}

func compareListIndex(t *testing.T, plain, indexed *Dataset, q List) ReadStats {
	t.Helper()
	q.Stats = nil
	want, e1 := plain.List(ctx, q)
	var st ReadStats
	q.Stats = &st
	got, e2 := indexed.List(ctx, q)
	if e1 != nil || e2 != nil || !reflect.DeepEqual(want, got) {
		t.Fatalf("query=%+v plain=%v/%v indexed=%v/%v", q, entriesOf(want), e1, entriesOf(got), e2)
	}
	return st
}

func TestListIndexRegressionCursorMatrix(t *testing.T) {
	keys := []string{"/", "//", "//x", "a/", "a/", "a/", "a/", "a/", "a/x", "a/y", "b/z", "\xff/", "\xff/x", "\xff/y", "\xff\xff/x"}
	fixture := parts(testutil.Part{GroupRows: 3, PageBytes: 16}, keys)
	fixture.RepeatedKeys = true
	dir := write(t, fixture)
	plain := open(t, dir, Options{})
	indexed := open(t, dir, Options{})
	_, err := indexed.BuildListIndex(ctx, ListIndexOptions{Workers: 3})
	must(t, err)
	cursors := []*Cursor{nil}
	for _, p := range []string{"", "a", "a/", "a/x", "a/w", "a/x/", "a/\xff", "missing/", "\xff", "\xff/", "\xff/x", "\xff\xff", "/", "//"} {
		cursors = append(cursors, &Cursor{Prefix: []byte(p)})
	}
	entries, err := plain.List(ctx, List{Prefix: []byte("a"), Delimiter: '/', Limit: 10})
	must(t, err)
	if entries[0].Exact == nil {
		t.Fatal("missing exact-row fixture")
	}
	for _, offset := range []int64{0, 2, 4, 50} {
		c := entries[0].Exact.Cursor()
		c.Offset = offset
		cursors = append(cursors, c)
	}
	for _, prefix := range []string{"", "a", "a/", "//", "\xff"} {
		for _, c := range cursors {
			for _, end := range [][]byte{nil, []byte("a/"), []byte("a/x"), []byte("a/x\x00"), []byte("\xff/y")} {
				for _, limit := range []int{1, 2, 4, 1000} {
					compareListIndex(t, plain, indexed, List{Prefix: []byte(prefix), Delimiter: '/', Cursor: c, End: end, Limit: limit, Columns: []string{"size", "name"}})
				}
			}
		}
	}
}

func TestListIndexRegressionBoundaryCertification(t *testing.T) {
	layouts := [][]string{
		{"a/0", "a/1", "c/0", "c/1", "b/0", "d/0"},
		{"a/0", "a/1", "d/0", "d/1", "c/0", "c/1", "b/0", "z/0"},
	}
	for li, keys := range layouts {
		dir := write(t, parts(testutil.Part{GroupRows: 2, PageBytes: 200}, keys))
		for _, workers := range []int{1, 2, 4} {
			for _, order := range [][]int{nil, {0, 1, 2}, {2, 1, 0}, {3, 2, 1, 0}} {
				d := open(t, dir, Options{})
				must(t, d.r.checkParts(ctx, new(ReadStats)))
				for _, i := range order {
					if i < len(d.r.parts[0].groups) {
						_, _ = d.r.first(ctx, groupRef{0, i}, &pageLog{unique: true}, new(ReadStats))
					}
				}
				st, err := d.BuildListIndex(ctx, ListIndexOptions{Workers: workers})
				must(t, err)
				failed := int64(len(keys)/2 - 1)
				if st.GroupsFailed != failed || st.GroupsSkipped != failed || st.GroupsIndexed != 1 || st.Pages != int64(len(keys)/2) {
					t.Fatalf("layout=%d workers=%d warm=%v: %+v", li, workers, order, st)
				}
				x := d.r.listIndex.Load()
				if len(x.groups) != 1 || x.groups[0].g != (groupRef{0, 0}) {
					t.Fatal("certified a disordered neighbor")
				}
				_, err = d.List(ctx, List{Delimiter: '/', Limit: 2})
				if CodeOf(err) != CodeKeyOrder {
					t.Fatalf("suppressed refusal: %v", err)
				}
			}
		}
	}
}

func TestListIndexRegressionSparseDenseSparse(t *testing.T) {
	var keys []string
	for _, dir := range []string{"a", "b", "c"} {
		for j := 0; j < 4; j++ {
			keys = append(keys, fmt.Sprintf("%s/%04d", dir, j))
		}
	}
	keys = append(keys, "c/0004", "d/0000", "e/0000", "f/0000")
	for j := 1; j <= 4; j++ {
		keys = append(keys, fmt.Sprintf("f/%04d", j))
	}
	for _, dir := range []string{"g", "h"} {
		for j := 0; j < 4; j++ {
			if dir == "g" && j == 0 {
				keys = append(keys, "g/")
			} else if dir == "g" && j == 1 {
				// Balance the shorter folder key to keep four rows in this page.
				keys = append(keys, "g/00010000")
			} else {
				keys = append(keys, fmt.Sprintf("%s/%04d", dir, j))
			}
		}
	}
	dir := write(t, parts(testutil.Part{GroupRows: 100, PageBytes: 40}, keys))
	plain := open(t, dir, Options{})
	indexed := open(t, dir, Options{})
	_, err := indexed.BuildListIndex(ctx, ListIndexOptions{MaxChangesPerPage: 1})
	must(t, err)
	g := indexed.r.listIndex.Load().group(groupRef{0, 0})
	if !g.dense {
		t.Fatal("fixture needs a dense page")
	}
	found := false
	for i, b := range g.runs {
		if b.row == 16 {
			found = string(g.key(i)) == "f/0001"
		}
	}
	if !found {
		t.Fatalf("missing coverage-resumption anchor: %+v", g.runs)
	}
	var projected ReadStats
	entries, err := indexed.List(ctx, List{Delimiter: '/', Limit: 100, Columns: []string{"size"}, Stats: &projected})
	must(t, err)
	if len(entries) != 8 || string(entries[6].CommonPrefix) != "g/" || entries[6].Exact == nil {
		t.Fatalf("missing exact folder after dense transition: %+v", entries)
	}
	exact := entries[6].Exact
	if string(exact.Key) != "g/" || exact.Offset != 0 || len(exact.Values) != 1 || exact.Values[0].Absent() || exact.Values[0].IsNull() || exact.Values[0].Int64() != 20 {
		t.Fatalf("wrong projected folder: %+v", exact)
	}
	if projected.ListIndexSkips < 3 || projected.ListIndexDensePage == 0 || projected.ValuePagesDecoded == 0 {
		t.Fatalf("did not project after synthetic/dense transitions: %+v", projected)
	}
	for _, cursor := range []*Cursor{nil, {Prefix: []byte("b/")}, {Prefix: []byte("f/0000")}, {Key: []byte("f/0001"), Offset: 0}} {
		for _, end := range [][]byte{nil, []byte("c/0002"), []byte("f/0003")} {
			st := compareListIndex(t, plain, indexed, List{Delimiter: '/', Limit: 100, Cursor: cursor, End: end, Columns: []string{"size", "name"}})
			if cursor == nil && end == nil && (st.ListIndexSkips < 3 || st.ListIndexDensePage == 0) {
				t.Fatalf("did not exercise hit/fallback/resumption: %+v", st)
			}
		}
	}
}

func TestListIndexRegressionFlatFirstGroupBudget(t *testing.T) {
	flat := "a" + strings.Repeat("x", 5000)
	for _, pageBytes := range []int{20000, 4000} {
		t.Run(fmt.Sprint(pageBytes), func(t *testing.T) {
			dir := write(t, parts(testutil.Part{GroupRows: 2, PageBytes: pageBytes}, []string{flat + "0", flat + "1", "b/x", "b/y"}))
			plain := open(t, dir, Options{})
			d := open(t, dir, Options{})
			st, err := d.BuildListIndex(ctx, ListIndexOptions{MaxBytes: 600})
			must(t, err)
			pages := int64(2)
			if pageBytes == 4000 {
				pages++
			}
			if st.GroupsIndexed != 1 || st.GroupsSkipped != 1 || st.GroupsFailed != 0 || st.BudgetExhausted || st.Pages != pages || st.Bytes > 600 {
				t.Fatalf("flat group must be fully decoded and omitted before admission: %+v", st)
			}
			if x := d.r.listIndex.Load(); len(x.groups) != 1 || x.groups[0].g != (groupRef{0, 1}) {
				t.Fatal("expected only the following slash group")
			}
			compareListIndex(t, plain, d, List{Delimiter: '/', Limit: 100, Columns: []string{"size"}})
		})
	}
	// A flat first page does not establish that the complete group is flat.
	dir := write(t, parts(testutil.Part{GroupRows: 2, PageBytes: 4000}, []string{flat + "0", flat + "1/x", "b/x", "b/y"}))
	d := open(t, dir, Options{})
	st, err := d.BuildListIndex(ctx, ListIndexOptions{MaxBytes: 600})
	must(t, err)
	if st.GroupsIndexed != 0 || !st.BudgetExhausted || st.Pages != 2 {
		t.Fatalf("later slash must prevent flat elimination: %+v", st)
	}
}

func TestListIndexRegressionMultiPart(t *testing.T) {
	layout := [][]string{{"/", "//", "//a", "//b"}, {"//c", "a/", "a//x", "a/a"}, {"a/b", "a/c", "b/", "b/x"}, {"\xff/", "\xff/a", "\xff/b", "\xff\xff/x"}}
	dir := write(t, parts(testutil.Part{GroupRows: 4, PageBytes: 30}, layout...))
	plain := open(t, dir, Options{})
	full := open(t, dir, Options{})
	_, err := full.BuildListIndex(ctx, ListIndexOptions{})
	must(t, err)
	firstCost := int64(256) + full.r.listIndex.Load().groups[0].memoryBytes()
	partial := open(t, dir, Options{})
	st, err := partial.BuildListIndex(ctx, ListIndexOptions{MaxBytes: firstCost, Workers: 4})
	must(t, err)
	if !st.BudgetExhausted || st.GroupsIndexed != 1 {
		t.Fatalf("part boundary budget: %+v", st)
	}
	for _, d := range []*Dataset{full, partial} {
		for _, prefix := range []string{"", "/", "//", "a", "a/", "\xff"} {
			for _, cursor := range []*Cursor{nil, {Prefix: []byte("//")}, {Prefix: []byte("a/x")}, {Prefix: []byte("\xff")}} {
				compareListIndex(t, plain, d, List{Prefix: []byte(prefix), Delimiter: '/', Limit: 100, Cursor: cursor, Columns: []string{"size", "name"}})
			}
		}
	}
	// A missing middle part remains a touch-time refusal, while a later part is usable.
	bad := write(t, parts(testutil.Part{GroupRows: 2}, []string{"a/0", "a/1"}, []string{"a/2", "b/0"}, []string{"c/0", "c/1"}))
	must(t, os.Remove(filepath.Join(bad, testutil.PartPath(1))))
	before := open(t, bad, Options{})
	after := open(t, bad, Options{})
	st, err = after.BuildListIndex(ctx, ListIndexOptions{})
	must(t, err)
	if st.PartsFailed != 1 {
		t.Fatalf("%+v", st)
	}
	for _, d := range []*Dataset{before, after} {
		_, e := d.List(ctx, List{Delimiter: '/', Limit: 100})
		if CodeOf(e) != CodeMissingPart {
			t.Fatal(e)
		}
	}
	compareListIndex(t, before, after, List{Prefix: []byte("c/"), Delimiter: '/', Limit: 100, Columns: []string{"size"}})
	// A flat middle part is deliberately unindexed, not a read failure.
	flat := write(t, parts(testutil.Part{GroupRows: 2}, []string{"a/0", "a/1"}, []string{"b", "b0"}, []string{"c/0", "c/1"}))
	before = open(t, flat, Options{})
	after = open(t, flat, Options{})
	st, err = after.BuildListIndex(ctx, ListIndexOptions{})
	must(t, err)
	if st.GroupsIndexed != 2 || st.GroupsSkipped != 1 || st.GroupsFailed != 0 {
		t.Fatalf("flat middle part: %+v", st)
	}
	read := compareListIndex(t, before, after, List{Delimiter: '/', Limit: 100, Columns: []string{"size", "name"}})
	if read.ListIndexMissingGroup == 0 {
		t.Fatal("did not exercise missing-group fallback")
	}
}

// Reading page 1 proves that page 0 has finished decoding. Release each
// worker's first page in a chosen order, awaiting that proof before the next.
type listBoundaryGate struct {
	io.ReaderAt
	first, second    []int64
	entered, decoded []chan struct{}
	release          []chan struct{}
}

func (g *listBoundaryGate) WithContext(c context.Context) io.ReaderAt {
	return readerFunc(func(p []byte, off int64) (int, error) {
		for i := range g.first {
			if off == g.first[i] {
				g.entered[i] <- struct{}{}
				select {
				case <-g.release[i]:
				case <-c.Done():
					return 0, c.Err()
				}
			}
			if off == g.second[i] {
				g.decoded[i] <- struct{}{}
			}
		}
		return g.ReaderAt.ReadAt(p, off)
	})
}

func TestListIndexRegressionWorkerBoundaryOrder(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 2, PageBytes: 8}, []string{"a/0", "a/1", "c/0", "c/1", "b/0", "d/0"}))
	for _, order := range [][]int{{0, 1, 2}, {2, 1, 0}, {1, 0, 2}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			d := open(t, dir, Options{})
			must(t, d.r.checkParts(ctx, new(ReadStats)))
			p := d.r.parts[0]
			original := p.file
			gate := &listBoundaryGate{ReaderAt: original}
			for i := 0; i < 3; i++ {
				idx, err := p.keyIndex(ctx, i, new(ReadStats))
				must(t, err)
				if len(idx.pages) != 2 {
					t.Fatalf("need two pages: %d", len(idx.pages))
				}
				gate.first = append(gate.first, idx.pages[0].offset)
				gate.second = append(gate.second, idx.pages[1].offset)
				gate.entered = append(gate.entered, make(chan struct{}, 1))
				gate.decoded = append(gate.decoded, make(chan struct{}, 1))
				gate.release = append(gate.release, make(chan struct{}))
			}
			p.file = gate
			c, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			defer func() { cancel(); <-done; p.file = original }()
			var st ListIndexStats
			var err error
			go func() { defer close(done); st, err = d.BuildListIndex(c, ListIndexOptions{Workers: 3}) }()
			for _, ch := range gate.entered {
				awaitListTest(t, ch)
			}
			for _, i := range order {
				close(gate.release[i])
				awaitListTest(t, gate.decoded[i])
			}
			awaitListTest(t, done)
			must(t, err)
			if st.GroupsIndexed != 1 || st.GroupsFailed != 2 || st.GroupsSkipped != 2 || st.Pages != 6 {
				t.Fatalf("decode order %v: %+v", order, st)
			}
		})
	}
}

func TestListIndexRegressionBudgetAccounting(t *testing.T) {
	var keys []string
	for i := 0; i < 30; i++ {
		keys = append(keys, fmt.Sprintf("d%02d/%s", i, string(bytes.Repeat([]byte{'x'}, i+1))))
	}
	dir := write(t, parts(testutil.Part{GroupRows: 10, PageBytes: 200}, keys))
	full := open(t, dir, Options{})
	all, err := full.BuildListIndex(ctx, ListIndexOptions{MaxChangesPerPage: 100})
	must(t, err)
	x := full.r.listIndex.Load()
	first := int64(256) + x.groups[0].memoryBytes()
	for _, budget := range []int64{1, first - 1, first, first + 1, all.Bytes - 1, all.Bytes, all.Bytes + 1} {
		d := open(t, dir, Options{})
		st, e := d.BuildListIndex(ctx, ListIndexOptions{MaxBytes: budget, Workers: 3, MaxChangesPerPage: 100})
		must(t, e)
		if st.Bytes > budget {
			t.Fatalf("%+v", st)
		}
		if budget < first && st.GroupsIndexed != 0 || budget >= first && budget < all.Bytes && st.GroupsIndexed == 0 || budget >= all.Bytes && st.GroupsIndexed != 3 {
			t.Fatalf("budget %d: %+v", budget, st)
		}
		if st.Pages < 3 || budget >= all.Bytes && st.Pages != all.Pages {
			t.Fatalf("lost worker-batch work: %+v / %+v", st, all)
		}
		again, e := d.BuildListIndex(ctx, ListIndexOptions{MaxBytes: all.Bytes, MaxChangesPerPage: 100})
		must(t, e)
		if st.GroupsIndexed == 0 {
			if again.GroupsIndexed != 3 {
				t.Fatal(again)
			}
		} else {
			again.OptionsMismatch = false
			if again != st {
				t.Fatal("nonempty build was replaced")
			}
		}
	}
	// Independent retained-layout accounting, using actual capacities and type sizes.
	raw := int64(unsafe.Sizeof(*x)) + int64(cap(x.groups))*int64(unsafe.Sizeof(listGroup{}))
	for _, g := range x.groups {
		if g.last != nil {
			t.Fatal("retained build-only boundary")
		}
		raw += int64(cap(g.base)+cap(g.data)) + int64(cap(g.runs))*int64(unsafe.Sizeof(listBoundary{}))
	}
	if raw > all.Bytes {
		t.Fatalf("retained layout %d exceeds charge %d", raw, all.Bytes)
	}
	// Measure allocator rounding independently by retaining isolated deep copies.
	copies := make([]*listIndex, 100)
	oldGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGC)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := range copies {
		y := &listIndex{stats: x.stats, groups: make([]listGroup, len(x.groups))}
		for j, g := range x.groups {
			y.groups[j] = g
			y.groups[j].base = bytes.Clone(g.base)
			y.groups[j].data = bytes.Clone(g.data)
			y.groups[j].runs = slices.Clone(g.runs)
		}
		copies[i] = y
	}
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(copies)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > uint64(len(copies))*uint64(all.Bytes) {
		t.Fatalf("allocator retained copies: %d > %d", allocated, int64(len(copies))*all.Bytes)
	}
}

// A context-bound gate blocks only builds carrying its marker, so ordinary
// lists can run while publication is pending. It never changes shared state.
type listBuildGate struct {
	io.ReaderAt
	entered, release chan struct{}
	once             sync.Once
}

func (g *listBuildGate) Close() error {
	if c, ok := g.ReaderAt.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

type listBuildMarker struct{}

func (g *listBuildGate) WithContext(c context.Context) io.ReaderAt {
	return readerFunc(func(p []byte, off int64) (int, error) {
		if c.Value(listBuildMarker{}) != nil {
			g.once.Do(func() { close(g.entered) })
			select {
			case <-g.release:
			case <-c.Done():
				return 0, c.Err()
			}
		}
		return g.ReaderAt.ReadAt(p, off)
	})
}
func awaitListTest[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for interleaving")
		var zero T
		return zero
	}
}
func TestListIndexRegressionLifecycle(t *testing.T) {
	for _, action := range []string{"publish", "cancel", "close"} {
		t.Run(action, func(t *testing.T) {
			dir := write(t, parts(testutil.Part{GroupRows: 4}, []string{"a/0", "a/1", "b/0", "b/1"}))
			d := open(t, dir, Options{})
			must(t, d.r.checkParts(ctx, new(ReadStats)))
			p := d.r.parts[0]
			original := p.file
			gate := &listBuildGate{ReaderAt: original, entered: make(chan struct{}), release: make(chan struct{})}
			p.file = gate
			defer func() { p.file = original }()
			marked, cancel := context.WithCancel(context.WithValue(ctx, listBuildMarker{}, true))
			defer cancel()
			type outcome struct {
				stats ListIndexStats
				err   error
			}
			done := make(chan outcome, 1)
			go func() { st, e := d.BuildListIndex(marked, ListIndexOptions{}); done <- outcome{st, e} }()
			awaitListTest(t, gate.entered)
			if d.r.listIndex.Load() != nil {
				t.Fatal("published incomplete snapshot")
			}
			if action == "close" {
				closed := make(chan error, 1)
				go func() { closed <- d.Close() }()
				out := awaitListTest(t, done)
				if !errors.Is(out.err, ErrClosed) || out.stats.Bytes != 0 {
					t.Fatalf("%+v", out)
				}
				must(t, awaitListTest(t, closed))
				return
			}
			// A second builder is cancelled while the first holds the build lock.
			waiting, stop := context.WithTimeout(ctx, 20*time.Millisecond)
			_, waitErr := d.BuildListIndex(waiting, ListIndexOptions{MaxBytes: 1})
			stop()
			if !errors.Is(waitErr, context.DeadlineExceeded) {
				t.Fatalf("lock waiter: %v", waitErr)
			}
			second := make(chan outcome, 1)
			if action == "publish" {
				go func() {
					st, e := d.BuildListIndex(ctx, ListIndexOptions{MaxBytes: 1, Workers: 3})
					second <- outcome{st, e}
				}()
			}

			got, e := d.List(ctx, List{Delimiter: '/', Limit: 100})
			must(t, e)
			if len(got) != 2 {
				t.Fatal(got)
			}
			if action == "cancel" {
				cancel()
			} else {
				close(gate.release)
			}
			out := awaitListTest(t, done)
			if action == "cancel" {
				if !errors.Is(out.err, context.Canceled) || d.r.listIndex.Load() != nil {
					t.Fatalf("%+v", out)
				}
				close(gate.release)
				_, e = d.BuildListIndex(ctx, ListIndexOptions{})
				must(t, e)
			} else {
				must(t, out.err)
				concurrent := awaitListTest(t, second)
				must(t, concurrent.err)
				if !concurrent.stats.OptionsMismatch {
					t.Fatal("missing options mismatch")
				}
				concurrent.stats.OptionsMismatch = false
				if concurrent.stats != out.stats {
					t.Fatal("concurrent build replaced snapshot")
				}
				again, e := d.BuildListIndex(ctx, ListIndexOptions{MaxBytes: 1, Workers: 3})
				must(t, e)
				if !again.OptionsMismatch {
					t.Fatal("missing options mismatch")
				}
				again.OptionsMismatch = false
				if again != out.stats {
					t.Fatal("changed completed build")
				}
			}
		})
	}
}
