// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/format"
	"github.com/varveio/seekparquet/go/internal/testutil"
)

// A forward seek over a non-local Source (scan.forward) routes by the key
// page indexes at hand instead of the routing search. These tests compare
// it with requests routed by the routing search alone (testRouteOnly): the
// same results as a local open, and no failure where the routed request
// succeeds. A forward-routed seek decodes no key page the routed seek
// would not, from the same state. A whole request's key pages and source
// reads can differ from the routed request's either way, without a bound,
// since the two leave different state for its later seeks
// (TestForwardSeekKnownDifferences); the tests require no more work only
// in the cases they name and over the differential's requests.

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
// pages, and made no more source reads, than the same request routed by
// the routing search.
func noMoreWork(t testing.TB, what string, forward, routed *ReadStats) {
	t.Helper()
	if forward.KeyPagesDecoded > routed.KeyPagesDecoded || forward.SourceReads > routed.SourceReads {
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
// skipped, over the page limit: the listing, a page of it with the count
// of each common prefix, and Plan failed with limit_exceeded where a local open and the routing search
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
			wantPage, err := countedPages(local, q, nil)
			must(t, err)
			sel := []Selection{{Span: fx.span}}
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
				noMoreWork(t, what+": listing", &fst, &rst)

				page, err := countedPages(overSource(t, dir, opt), q, nil)
				must(t, err)
				if !sameAnswer(wantPage, page) {
					t.Fatalf("%s: page %+v, want %+v", what, page, wantPage)
				}

				fst, rst = ReadStats{}, ReadStats{}
				plan, err := overSource(t, dir, opt).Plan(ctx, sel, nil, &fst)
				must(t, err)
				if n, exact := plan.Count(); n != fx.count || !exact {
					t.Fatalf("%s: plan counts %d (exact %v), want %d", what, n, exact, fx.count)
				}
				routeOnly(t, func() { _, err = overSource(t, dir, opt).Plan(ctx, sel, nil, &rst) })
				must(t, err)
				noMoreWork(t, what+": plan", &fst, &rst)
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
	noMoreWork(t, "plan", &fst, &rst)
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
	q := ListQuery{Delimiter: '/', Limit: 10}
	want, err := countedPages(open(t, dir, OpenOptions{}), q, nil)
	must(t, err)
	if len(want.entries) != 2 || want.counts[0] != -1 {
		t.Fatalf("local page %+v: want the count of a/ unknown", want)
	}
	for _, speculate := range []bool{true, false} {
		got, err := countedPages(overSource(t, dir, OpenOptions{Read: ReadPolicy{DisableSpeculation: !speculate}}), q, nil)
		must(t, err)
		if !sameAnswer(want, got) {
			t.Fatalf("speculation %v: %+v, want %+v", speculate, got, want)
		}
	}
}

// TestForwardSeekCursorPages pages listings a few entries at a time, each
// page resumed from the last one's cursor on the same handle, decoding no
// more key pages than routing alone, and pins their source reads with
// speculation, forward-routed and routed, which the #80 reviews found
// regressed:
//
//   - a later page's routing search read a page the read-ahead then read
//     a second time: a prediction now skips the pages the request has read
//     (pageMemo);
//   - that skip let a prediction run past a page the windows already held,
//     where it stops, and read a page it did not use.
//
// Since a prediction reads the few bytes between row groups too
// (readGapBytes), each listing reads 5 ranges forward-routed (7 and 8
// before) and 4 routed (7 and 8): routing's first page decodes the next
// row group's first key from a read it makes anyway, which forward
// routing's later page reads alone (TestForwardSeekKnownDifferences).
func TestForwardSeekCursorPages(t *testing.T) {
	noTestReadAhead(t)
	for _, fx := range []struct {
		name  string
		fx    cursorFixture
		reads [2]int64 // with speculation: forward-routed, routed
	}{
		{"probe read ahead again", cursorFixture{testutil.Part{Rows: testutil.Rows("c/f", "c0f", "f/a"), GroupRows: 2, PageBytes: 64}, 1, nil}, [2]int64{5, 4}},
		{"prediction past a held page", cursorFixture{testutil.Part{Rows: testutil.Rows("b/c", "c/e", "d/d", "d0d", "e/b", "e/c", "e0d", "f/c", "f/e"), Groups: []int{1, 2, 2, 2, 2}, PageBytes: 1}, 4,
			func(group int, index *format.ColumnIndex) {
				switch group {
				case 2: // "e/b"
					index.MaxValues[1] = []byte("z")
				case 4: // "f/c"
					index.MaxValues[0] = []byte("z")
				}
				index.BoundaryOrder = format.Unordered
			}}, [2]int64{5, 4}},
	} {
		t.Run(fx.name, func(t *testing.T) {
			for _, speculate := range []bool{true, false} {
				fst, rst := fx.fx.work(t, speculate)
				t.Logf("speculation %v: %d key pages decoded and %d source reads; routed by the routing search, %d and %d",
					speculate, fst.KeyPagesDecoded, fst.SourceReads, rst.KeyPagesDecoded, rst.SourceReads)
				if fst.KeyPagesDecoded > rst.KeyPagesDecoded {
					t.Errorf("speculation %v: %d key pages decoded; routed by the routing search, %d", speculate, fst.KeyPagesDecoded, rst.KeyPagesDecoded)
				}
				if reads := [2]int64{fst.SourceReads, rst.SourceReads}; speculate && reads != fx.reads {
					t.Errorf("source reads with speculation %v (forward-routed, routed), want %v", reads, fx.reads)
				}
			}
		})
	}
}

// cursorFixture is a one-part dataset, its key ColumnIndex edited when
// edit is set, listed Limit entries a page.
type cursorFixture struct {
	part  testutil.Part
	limit int
	edit  func(group int, index *format.ColumnIndex)
}

// work lists the fixture's root page by page with the count of each common
// prefix (countedPages), following
// each page's cursor on one non-local handle, forward-routed and routed by
// the routing search alone, and returns the work of each over all the
// pages, after requiring both to answer as a local open does.
func (fx cursorFixture) work(t *testing.T, speculate bool) (forward, routed ReadStats) {
	t.Helper()
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{fx.part}})
	if fx.edit != nil {
		editColumnIndexes(t, dir, 0, fx.edit)
	}
	fullyValid(t, dir)
	q := ListQuery{Delimiter: '/', Limit: fx.limit}
	pages := func(d *Dataset) (countedListing, ReadStats) {
		var total ReadStats
		got, err := countedPages(d, q, &total)
		must(t, err)
		return got, total
	}
	want, _ := pages(open(t, dir, OpenOptions{}))
	opt := OpenOptions{Read: ReadPolicy{DisableSpeculation: !speculate}}
	got, forward := pages(overSource(t, dir, opt))
	var answer countedListing
	routeOnly(t, func() { answer, routed = pages(overSource(t, dir, opt)) })
	if !sameAnswer(want, got) || !sameAnswer(want, answer) {
		t.Fatalf("speculation %v: pages %+v and, routed, %+v; want %+v", speculate, got, answer, want)
	}
	return forward, routed
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
// non-monotone key page bounds, unique and repeated keys, plain keys over
// a Source read range by range and dictionary-encoded keys over one that
// reads ranges together (RangeReader), with and without speculation, some
// under a page limit that some of their pages exceed (forwardSeed: every
// request answers as a local open does, and fails only where routing alone
// fails). Over these seeds, no request decodes more key pages than routing
// alone, and all of them together make no more source reads. Neither holds
// for every request: TestForwardSeekKnownDifferences pins requests of other
// seeds that decode or read more.
func TestForwardSeekDifferential(t *testing.T) {
	noTestReadAhead(t)
	seeds := 24
	if testing.Short() {
		seeds = 6
	}
	for _, dict := range []bool{false, true} {
		var forward, routed ReadStats
		for seed := range uint64(seeds) {
			forwardSeed(t, seed, dict, func(_ int, what string, fst, rst *ReadStats) {
				if fst.KeyPagesDecoded > rst.KeyPagesDecoded {
					t.Errorf("%s: %d key pages decoded; routed by the routing search, %d", what, fst.KeyPagesDecoded, rst.KeyPagesDecoded)
				}
				forward.Add(*fst)
				routed.Add(*rst)
			})
		}
		t.Logf("dictionary %v: %d key pages decoded and %d source reads; routed by the routing search, %d and %d",
			dict, forward.KeyPagesDecoded, forward.SourceReads, routed.KeyPagesDecoded, routed.SourceReads)
		noMoreWork(t, fmt.Sprintf("dictionary %v, all requests", dict), &forward, &routed)
	}
}

// TestForwardSeekKnownDifferences pins requests that forward routing makes
// with more work than the routing search alone. Each seek forward routes
// decodes no key page the routed seek would not, from the same state; but
// the two leave different state for the request's later seeks and the
// pages that follow it: the routing search's probes decode row groups'
// first keys, which later seeks use to route, to stop at an upper bound,
// or to emit a common prefix without reading its group, and forward
// routing skips them, so its read-ahead also reads other ranges. No bound
// is claimed, either way: in 1,000-seed runs of the differential a few
// dozen of the 60,000 requests read more, with plain keys and with
// dictionary keys, the most by 6 ranges (seed 551: 98 against 92), and
// seed 831's is the only one found to decode more. Should a change move a
// row, update it.
func TestForwardSeekKnownDifferences(t *testing.T) {
	noTestReadAhead(t)
	for _, k := range []struct {
		seed            uint64
		dict            bool
		op              int
		forward, routed [2]int64 // key pages decoded, source reads
	}{
		{831, false, 24, [2]int64{12, 4}, [2]int64{11, 5}}, // the routed listing stops at End by a probe's first key
		{551, false, 18, [2]int64{164, 98}, [2]int64{174, 92}},
		{704, true, 22, [2]int64{72, 95}, [2]int64{73, 91}},
	} {
		found := false
		forwardSeed(t, k.seed, k.dict, func(op int, what string, fst, rst *ReadStats) {
			if op != k.op {
				return
			}
			found = true
			got, routed := [2]int64{fst.KeyPagesDecoded, fst.SourceReads}, [2]int64{rst.KeyPagesDecoded, rst.SourceReads}
			if got != k.forward || routed != k.routed {
				t.Errorf("%s: key pages decoded and source reads %v, routed by the routing search %v; pinned %v and %v", what, got, routed, k.forward, k.routed)
			}
		})
		if !found {
			t.Errorf("seed %d: no successful operation %d", k.seed, k.op)
		}
	}
	// Paged by cursor: the first page leaves the next row group's first key
	// undecoded, though the read-ahead read its page; the next page's
	// routing search reads it again (6 source reads against 4).
	fx := cursorFixture{testutil.Part{Rows: testutil.Rows("a/f", "a0a", "a0b", "a0f", "d0b", "d0c", "d0e", "e/d"), GroupRows: 4, PageBytes: 16}, 3, nil}
	fst, rst := fx.work(t, true)
	if got, routed := [2]int64{fst.KeyPagesDecoded, fst.SourceReads}, [2]int64{rst.KeyPagesDecoded, rst.SourceReads}; got != [2]int64{10, 6} || routed != [2]int64{10, 4} {
		t.Errorf("cursor pages: key pages decoded and source reads %v, routed by the routing search %v; pinned [10 6] and [10 4]", got, routed)
	}
}

// forwardSeed writes the random dataset of seed and runs 30 random
// requests on it: listings, pages following every cursor on one handle,
// range reads and plans, each on fresh non-local handles with and without
// speculation. Each request must fail only where the same request routed
// by the routing search alone (testRouteOnly) fails, and otherwise answer
// as a local open does; work is called with the work of each request
// both succeed in.
func forwardSeed(t *testing.T, seed uint64, dict bool, work func(op int, what string, forward, routed *ReadStats)) {
	t.Helper()
	nonLocal := func(dir string, opt OpenOptions) *Dataset {
		if dict { // ranges read together, as an object store's batches
			return openWith(t, ctx, &recordingWaveSource{Source: dirOf(t, dir)}, opt, false)
		}
		return overSource(t, dir, opt)
	}
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
		if dict {
			keys := make([]dictKey, n)
			for i, k := range rows[at : at+n] {
				keys[i].Key = []byte(k)
			}
			p.Rows = keys
		}
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
		what := fmt.Sprintf("seed %d op %d (dictionary %v)", seed, op, dict)
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
			got, gerr = nonLocal(dir, opt).List(ctx, q, &fst)
			routeOnly(t, func() { routed, rerr = nonLocal(dir, opt).List(ctx, q, &rst) })
		case 1:
			q := ListQuery{Delimiter: '/', Limit: 1 + rng.IntN(6), Prefix: bound()}
			request = q
			// Follow the cursors to the end on one handle, adding up
			// the work of every page: what one page leaves cached is
			// what the next one reads.
			want, werr = countedPages(local, q, nil)
			got, gerr = countedPages(nonLocal(dir, opt), q, &fst)
			routeOnly(t, func() { routed, rerr = countedPages(nonLocal(dir, opt), q, &rst) })
		case 2:
			q := Query{Selection: Selection{Span: Span{Start: start, End: end, StartExclusive: rng.IntN(2) == 0}}, Limit: int64(1 + rng.IntN(20))}
			request = q
			want, werr = local.Rows(ctx, q, nil)
			got, gerr = nonLocal(dir, opt).Rows(ctx, q, &fst)
			routeOnly(t, func() { routed, rerr = nonLocal(dir, opt).Rows(ctx, q, &rst) })
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
			got, gerr = plan(nonLocal(dir, opt), &fst)
			routeOnly(t, func() { routed, rerr = plan(nonLocal(dir, opt), &rst) })
		}
		// Forward routing may succeed where routing fails (it loads
		// fewer indexes, under IndexMemory); it must not fail where
		// routing succeeds.
		if gerr != nil && rerr == nil {
			t.Fatalf("%s: %s (speculation %v): forward-routed %v; routed by the routing search, no error", what, describe(request), speculate, gerr)
		}
		if gerr != nil || rerr != nil {
			continue
		}
		if c, ok := want.(count); ok {
			// Whether a count is exact follows the parts the handle
			// has checked, which a local open checks as it opens: a
			// forward-routed count must be exact as the routed one is.
			c.exact = routed.(count).exact
			want = c
		}
		if werr == nil && (!sameAnswer(want, got) || !sameAnswer(want, routed)) {
			t.Fatalf("%s: %s (speculation %v):\nlocal  %+v\nremote %+v\nrouted %+v", what, describe(request), speculate, want, got, routed)
		}
		work(op, fmt.Sprintf("%s: %s (speculation %v)", what, describe(request), speculate), &fst, &rst)
	}
}

// dictKey is a row of a dictionary-encoded key alone.
type dictKey struct {
	Key []byte `parquet:"key,dict"`
}

// count is a plan's row count (Plan.Count).
type count struct {
	n     int64
	exact bool
}

// countedListing is the entries of a listing's pages and the count of
// each (prefixCounts).
type countedListing struct {
	entries []ListEntry
	counts  []int64
}

// countedPages lists q a page at a time (listPage), following each page's
// cursor to the end on d, with the count of each common prefix of each
// page. st, when not nil, receives the work of every page.
func countedPages(d *Dataset, q ListQuery, st *ReadStats) (countedListing, error) {
	var out countedListing
	for {
		var one ReadStats
		entries, next, err := listPage(ctx, d, q, &one)
		if st != nil {
			st.Add(one)
		}
		if err != nil {
			return out, err
		}
		counts, err := prefixCounts(ctx, d, entries)
		if err != nil {
			return out, err
		}
		out.entries = append(out.entries, entries...)
		out.counts = append(out.counts, counts...)
		if next == nil {
			return out, nil
		}
		q.Cursor = next
	}
}

// sameAnswer is sameResults, also for counted listings.
func sameAnswer(want, got any) bool {
	w, ok := want.(countedListing)
	if !ok {
		return sameResults(want, got)
	}
	g := got.(countedListing)
	return sameEntries(w.entries, g.entries) && slices.Equal(w.counts, g.counts)
}
