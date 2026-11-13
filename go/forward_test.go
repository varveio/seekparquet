// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

// A forward seek over a non-local Source (scan.forward) routes by the key
// page indexes at hand instead of the routing search. These tests hold it
// to the routing search: the same results as a local open, and no key page
// decoded, no source read made and no failure met that a seek routed by
// the routing search alone (testRouteOnly) would not meet.

// routeOnly runs fn with every seek routed by the routing search.
func routeOnly(t testing.TB, fn func()) {
	t.Helper()
	testRouteOnly = true
	defer func() { testRouteOnly = false }()
	fn()
}

// overSource opens the dataset at dir through a Source, as a non-local open.
func overSource(t testing.TB, dir string, opt OpenOptions) *Dataset {
	t.Helper()
	d, err := OpenDataset(ctx, dirOf(t, dir), opt)
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// noMoreWork requires a forward-routed request to have decoded no more key
// pages than the same request routed by the routing search, and, when
// reads counts, to have made no more source reads.
func noMoreWork(t testing.TB, what string, forward, routed *ReadStats, reads bool) {
	t.Helper()
	if forward.KeyPagesDecoded > routed.KeyPagesDecoded || reads && forward.SourceReads > routed.SourceReads {
		t.Errorf("%s: %d key pages decoded and %d source reads; routed by the routing search, %d and %d",
			what, forward.KeyPagesDecoded, forward.SourceReads, routed.KeyPagesDecoded, routed.SourceReads)
	}
}

// fullyValid requires the dataset at dir to pass full validation: the
// bounds the tests write are valid, only conservative.
func fullyValid(t testing.TB, dir string) {
	t.Helper()
	report, err := ValidateDir(ctx, dir, ValidateOptions{Level: LevelFull})
	must(t, err)
	if report.Result() != Complete {
		t.Fatalf("the fixture is not valid: %+v", report.Levels)
	}
}

// maximaAbove writes max as every key page maximum of row group group of
// part 0: a valid bound when it is above every key (RG-5), and an upper
// bound that says nothing of where the group's keys end.
func maximaAbove(t testing.TB, dir string, group int, max string) {
	t.Helper()
	editColumnIndexes(t, dir, 0, func(g int, index *format.ColumnIndex) {
		if g == group {
			for i := range index.MaxValues {
				index.MaxValues[i] = []byte(max)
			}
			index.BoundaryOrder = format.Unordered
		}
	})
}

// TestForwardSeekDecodesNoSkippedPage: a seek past a common prefix whose
// row group's maxima are written above its keys lands in the next row
// group. Forward routing once accepted the first group because its last
// maximum reached the target, and decoded there a page the listing had
// skipped, over the page limit: the listing, Page, Plan and Aggregate
// failed with limit_exceeded where a local open and the routing search
// succeed (issue 79).
func TestForwardSeekDecodesNoSkippedPage(t *testing.T) {
	noTestReadAhead(t)
	big := "a/" + strings.Repeat("x", 4096)
	for _, fx := range []struct {
		name      string
		keys      []string
		groupRows int
		list      []string
		span      Span
		count     int64
	}{
		{"prefix then row", []string{"a/0", big, "a0"}, 2, []string{"p:a/", "r:a0"}, Span{Start: []byte("a/0"), End: []byte("a0\x00")}, 3},
		{"span end past the prefix", []string{"a/!", "a/0", big, "a0", "b0"}, 3, []string{"p:a/", "r:a0", "r:b0"}, Span{Start: []byte("a/0"), End: []byte("a0\x00")}, 3},
	} {
		t.Run(fx.name, func(t *testing.T) {
			dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows(fx.keys...), GroupRows: fx.groupRows, PageBytes: 1}}})
			maximaAbove(t, dir, 0, "z")
			fullyValid(t, dir)
			limits := Limits{PageBytes: 512}
			local := open(t, dir, OpenOptions{Limits: limits})
			q := ListQuery{Delimiter: '/', Limit: 10}
			want, err := local.List(ctx, q, nil)
			must(t, err)
			if got := entriesOf(want); !slices.Equal(got, fx.list) {
				t.Fatalf("local listing %q, want %q", got, fx.list)
			}
			wantPage, err := local.Page(ctx, PageQuery{ListQuery: q, Reductions: []Reduction{{Op: CountRows}}}, nil)
			must(t, err)
			sel := []Selection{{Span: fx.span}}
			wantAgg, err := local.Aggregate(ctx, sel, []Reduction{{Op: CountRows}}, AggregateOptions{}, nil)
			must(t, err)
			for _, speculate := range []bool{true, false} {
				opt := OpenOptions{Limits: limits, Read: ReadPolicy{DisableSpeculation: !speculate}}
				what := fmt.Sprintf("speculation %v", speculate)
				var fst, rst ReadStats
				got, err := overSource(t, dir, opt).List(ctx, q, &fst)
				if err != nil || !sameEntries(want, got) {
					t.Fatalf("%s: listing %q, %v; want %q", what, entriesOf(got), err, fx.list)
				}
				routeOnly(t, func() { _, err = overSource(t, dir, opt).List(ctx, q, &rst) })
				must(t, err)
				noMoreWork(t, what+": listing", &fst, &rst, !speculate)

				page, err := overSource(t, dir, opt).Page(ctx, PageQuery{ListQuery: q, Reductions: []Reduction{{Op: CountRows}}}, nil)
				must(t, err)
				if len(page.Entries) != len(wantPage.Entries) {
					t.Fatalf("%s: page of %d entries, want %d", what, len(page.Entries), len(wantPage.Entries))
				}
				for i, e := range page.Entries {
					if !sameEntries([]ListEntry{wantPage.Entries[i].ListEntry}, []ListEntry{e.ListEntry}) || !reflect.DeepEqual(e.Aggregates, wantPage.Entries[i].Aggregates) {
						t.Fatalf("%s: page entry %d is %+v, want %+v", what, i, e, wantPage.Entries[i])
					}
				}

				fst, rst = ReadStats{}, ReadStats{}
				plan, err := overSource(t, dir, opt).Plan(ctx, sel, nil, &fst)
				must(t, err)
				if n, exact := plan.Count(); n != fx.count || !exact {
					t.Fatalf("%s: plan counts %d (exact %v), want %d", what, n, exact, fx.count)
				}
				routeOnly(t, func() { _, err = overSource(t, dir, opt).Plan(ctx, sel, nil, &rst) })
				must(t, err)
				noMoreWork(t, what+": plan", &fst, &rst, !speculate)

				agg, err := overSource(t, dir, opt).Aggregate(ctx, sel, []Reduction{{Op: CountRows}}, AggregateOptions{}, nil)
				if err != nil || !reflect.DeepEqual(agg, wantAgg) {
					t.Fatalf("%s: aggregate %v, %v; want %v", what, agg, err, wantAgg)
				}
			}
		})
	}
}

// TestForwardSeekBroadMaxima plans a span from a row group of a thousand
// one-row key pages whose maxima are all written above its keys into the
// next row group. The end of the span is in that next group: routing by
// the maxima alone decoded every page after the start (1,002 key pages,
// against 4 by the routing search, issue 79).
func TestForwardSeekBroadMaxima(t *testing.T) {
	keys := make([]string, 1000, 1002)
	for i := range keys {
		keys[i] = fmt.Sprintf("a%04d", i)
	}
	keys = append(keys, "m0000", "m0001")
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows(keys...), GroupRows: 1000, PageBytes: 1}}})
	maximaAbove(t, dir, 0, "z")
	sel := []Selection{{Span: Span{Start: []byte("a0001"), End: []byte("m0000\x00")}}}
	want, err := open(t, dir, OpenOptions{}).Plan(ctx, sel, nil, nil)
	must(t, err)
	wantN, wantExact := want.Count()
	opt := OpenOptions{Read: ReadPolicy{DisableSpeculation: true}}
	var fst, rst ReadStats
	got, err := overSource(t, dir, opt).Plan(ctx, sel, nil, &fst)
	must(t, err)
	if n, exact := got.Count(); n != wantN || exact != wantExact {
		t.Fatalf("plan counts %d (exact %v), want %d (exact %v)", n, exact, wantN, wantExact)
	}
	routeOnly(t, func() { _, err = overSource(t, dir, opt).Plan(ctx, sel, nil, &rst) })
	must(t, err)
	t.Logf("%d key pages decoded and %d source reads; routed by the routing search, %d and %d",
		fst.KeyPagesDecoded, fst.SourceReads, rst.KeyPagesDecoded, rst.SourceReads)
	noMoreWork(t, "plan", &fst, &rst, true)
}

// TestForwardSeekPageEvidence lists the root of four one-row parts, the
// first part's maxima written above its key, with a row count of each
// common prefix. Forward routing once walked the first part's group and
// then the next parts to the target instead of routing to the part that
// holds it, checking the parts it passed: the count of "a/" became exact
// where a local open, which checks only the parts it routes to, leaves it
// unknown (issue 79). The answer must be the local open's.
func TestForwardSeekPageEvidence(t *testing.T) {
	noTestReadAhead(t)
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{{Rows: testutil.Rows("a/x")}, {Rows: testutil.Rows("a/y")}, {Rows: testutil.Rows("a/z")}, {Rows: testutil.Rows("a0")}}})
	maximaAbove(t, dir, 0, "z")
	fullyValid(t, dir)
	q := PageQuery{ListQuery: ListQuery{Delimiter: '/', Limit: 10}, Reductions: []Reduction{{Op: CountRows}}}
	want, err := open(t, dir, OpenOptions{}).Page(ctx, q, nil)
	must(t, err)
	if len(want.Entries) != 2 || want.Entries[0].Aggregates[0].Status != AggregateUnknown {
		t.Fatalf("local page %+v: want the count of a/ unknown", want.Entries)
	}
	for _, speculate := range []bool{true, false} {
		got, err := overSource(t, dir, OpenOptions{Read: ReadPolicy{DisableSpeculation: !speculate}}).Page(ctx, q, nil)
		must(t, err)
		if len(got.Entries) != len(want.Entries) {
			t.Fatalf("speculation %v: %d entries, want %d", speculate, len(got.Entries), len(want.Entries))
		}
		for i, e := range got.Entries {
			if !sameEntries([]ListEntry{want.Entries[i].ListEntry}, []ListEntry{e.ListEntry}) || !reflect.DeepEqual(e.Aggregates, want.Entries[i].Aggregates) {
				t.Fatalf("speculation %v: entry %d is %+v, want %+v", speculate, i, e, want.Entries[i])
			}
		}
	}
}

// loosenBounds rewrites each part's key page bounds at random into other
// valid bounds (RG-5): a maximum widened to all 0xFF bytes or shortened
// with its last kept byte incremented, a minimum shortened or empty. The
// bounds then overlap and stop being monotone (RG-6).
func loosenBounds(t testing.TB, dir string, parts int, rng *rand.Rand, p float64) {
	t.Helper()
	for part := range parts {
		editColumnIndexes(t, dir, part, func(_ int, index *format.ColumnIndex) {
			for i := range index.MaxValues {
				if rng.Float64() < p {
					hi := index.MaxValues[i]
					if k := 1 + rng.IntN(len(hi)); rng.IntN(3) == 0 {
						hi = bytes.Repeat([]byte{0xff}, 8)
					} else if hi[k-1] < 0xff {
						hi = append(bytes.Clone(hi[:k-1]), hi[k-1]+1)
					}
					index.MaxValues[i] = hi
				}
				if rng.Float64() < p {
					index.MinValues[i] = bytes.Clone(index.MinValues[i][:rng.IntN(len(index.MinValues[i])+1)])
				}
			}
			index.BoundaryOrder = format.Unordered
		})
	}
}

// TestForwardSeekDifferential runs random listings, pages, range reads and
// plans over random datasets, some with conservative, shortened and
// non-monotone key page bounds, unique and repeated keys, through a
// non-local open with and without speculation, some under a page limit
// that some of their pages exceed. Each answer, or error, must be a local
// open's, and each request must decode no more key pages, and
// without speculation make no more source reads, than the same request on
// a fresh handle with every seek routed by the routing search.
func TestForwardSeekDifferential(t *testing.T) {
	noTestReadAhead(t)
	seeds := 24
	if testing.Short() {
		seeds = 6
	}
	for seed := range uint64(seeds) {
		rng := rand.New(rand.NewPCG(seed, 79))
		repeated := seed%3 == 2
		keys := fixtureKeys(30 + rng.IntN(150))
		var rows []string
		for _, k := range keys {
			rows = append(rows, k)
			for repeated && rng.IntN(2) == 0 {
				rows = append(rows, k)
			}
		}
		geometry := testutil.Part{GroupRows: 2 + rng.IntN(20), PageBytes: []int{1, 1, 64, 300}[rng.IntN(4)], IndexSizeLimit: []int{0, 6, 1 << 10}[rng.IntN(3)]}
		chunks := split(keys, 1+rng.IntN(3))
		var d testutil.Dataset
		d.RepeatedKeys = repeated
		at := 0
		for _, chunk := range chunks {
			p := geometry
			n := 0
			for at+n < len(rows) && rows[at+n] <= chunk[len(chunk)-1] {
				n++
			}
			p.Rows = testutil.Rows(rows[at : at+n]...)
			at += n
			d.Parts = append(d.Parts, p)
		}
		dir := write(t, d)
		loosenBounds(t, dir, len(chunks), rng, []float64{0, 0.2, 0.7}[rng.IntN(3)])
		fullyValid(t, dir)
		// A page limit below the largest pages makes a request fail when
		// it decodes one: forward routing must fail where routing fails.
		limits := Limits{}
		if rng.IntN(3) == 0 {
			limits.PageBytes = 200
		}
		local := open(t, dir, OpenOptions{Limits: limits})
		bounds := probes(keys, 5)
		bound := func() []byte { return bounds[rng.IntN(len(bounds))] }
		for op := range 30 {
			what := fmt.Sprintf("seed %d op %d", seed, op)
			speculate := rng.IntN(2) == 0
			opt := OpenOptions{Limits: limits, Read: ReadPolicy{DisableSpeculation: !speculate}}
			start, end := bound(), bound()
			if rng.IntN(3) == 0 || bytes.Compare(end, start) <= 0 {
				end = nil
			}
			var fst, rst ReadStats
			var want, got, routed any
			var werr, gerr, rerr error
			var request any
			switch rng.IntN(4) {
			case 0:
				q := ListQuery{Delimiter: '/', Limit: 1 + rng.IntN(12), End: end}
				if rng.IntN(4) > 0 {
					q.Prefix = bound()
				}
				if rng.IntN(5) == 0 {
					q.Delimiter = 0
				}
				request = q
				want, werr = local.List(ctx, q, nil)
				got, gerr = overSource(t, dir, opt).List(ctx, q, &fst)
				routeOnly(t, func() { routed, rerr = overSource(t, dir, opt).List(ctx, q, &rst) })
			case 1:
				q := PageQuery{ListQuery: ListQuery{Delimiter: '/', Limit: 1 + rng.IntN(6), Prefix: bound()}, Reductions: []Reduction{{Op: CountRows}}}
				request = q.ListQuery
				page := func(d *Dataset, st *ReadStats) ([]PageEntry, error) {
					first, err := d.Page(ctx, q, st)
					if err != nil || first.Next == nil {
						return first.Entries, err
					}
					next := q
					next.Cursor = first.Next
					second, err := d.Page(ctx, next, nil)
					return append(first.Entries, second.Entries...), err
				}
				want, werr = page(local, nil)
				got, gerr = page(overSource(t, dir, opt), &fst)
				routeOnly(t, func() { routed, rerr = page(overSource(t, dir, opt), &rst) })
			case 2:
				q := Query{Selection: Selection{Span: Span{Start: start, End: end, StartExclusive: rng.IntN(2) == 0}}, Limit: int64(1 + rng.IntN(20))}
				request = q
				want, werr = local.Rows(ctx, q, nil)
				got, gerr = overSource(t, dir, opt).Rows(ctx, q, &fst)
				routeOnly(t, func() { routed, rerr = overSource(t, dir, opt).Rows(ctx, q, &rst) })
			default:
				sel := []Selection{{Span: Span{Start: start, End: end}}}
				request = sel[0]
				plan := func(d *Dataset, st *ReadStats) (count, error) {
					p, err := d.Plan(ctx, sel, nil, st)
					if err != nil {
						return count{}, err
					}
					n, exact := p.Count()
					return count{n, exact}, nil
				}
				want, werr = plan(local, nil)
				got, gerr = plan(overSource(t, dir, opt), &fst)
				routeOnly(t, func() { routed, rerr = plan(overSource(t, dir, opt), &rst) })
			}
			if (werr == nil) != (gerr == nil) || (werr == nil) != (rerr == nil) {
				t.Fatalf("%s: %s: errors local %v, remote %v, routed %v", what, describe(request), werr, gerr, rerr)
			}
			if werr != nil {
				continue
			}
			if c, ok := want.(count); ok {
				// Whether a count is exact follows the parts the handle
				// has checked, which a local open checks as it opens: a
				// forward-routed count must be exact as the routed one is.
				c.exact = routed.(count).exact
				want = c
			}
			if !sameAnswer(want, got) || !sameAnswer(want, routed) {
				t.Fatalf("%s: %s (speculation %v):\nlocal  %+v\nremote %+v\nrouted %+v", what, describe(request), speculate, want, got, routed)
			}
			noMoreWork(t, fmt.Sprintf("%s: %s", what, describe(request)), &fst, &rst, !speculate)
		}
	}
}

// count is a plan's row count (Plan.Count).
type count struct {
	n     int64
	exact bool
}

// sameAnswer is sameResults, also for the entries of pages.
func sameAnswer(want, got any) bool {
	w, ok := want.([]PageEntry)
	if !ok {
		return sameResults(want, got)
	}
	g := got.([]PageEntry)
	if len(w) != len(g) {
		return false
	}
	for i := range w {
		if !sameEntries([]ListEntry{w[i].ListEntry}, []ListEntry{g[i].ListEntry}) || !reflect.DeepEqual(w[i].Aggregates, g[i].Aggregates) {
			return false
		}
	}
	return true
}
