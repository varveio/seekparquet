// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// treeDataset is fixtureKeys(n) in three parts of row groups of 400 rows,
// small pages, and bounds shortened to 8 bytes, so that adjacent pages'
// bounds overlap on the long shared stems.
func treeDataset(t testing.TB, n int) (*Dataset, []string) {
	keys := fixtureKeys(n)
	d := parts(testutil.Part{GroupRows: 400, PageBytes: 512, IndexSizeLimit: 8}, split(keys, 3)...)
	return open(t, write(t, d), OpenOptions{}), keys
}

// TestRangeMatchesOracle reads from and up to keys, the bytes around them
// and their parents (row group, page and part boundaries among them),
// inclusive and exclusive, at several limits, and compares the keys with
// the definition.
func TestRangeMatchesOracle(t *testing.T) {
	d, keys := treeDataset(t, 3000)
	every := 7
	if testing.Short() {
		every = 41
	}
	bounds := probes(keys, every)
	check := func(rng Query) {
		t.Helper()
		got, err := d.Rows(ctx, rng, nil)
		if err != nil {
			t.Fatalf("%s: %v", describe(rng), err)
		}
		if want := rangeOracle(keys, rng); !slices.Equal(keysOf(got), want) {
			t.Fatalf("%s: %d keys, want %d", describe(rng), len(got), len(want))
		}
	}
	for i, b := range bounds {
		for _, exclusive := range []bool{false, true} {
			check(Query{Selection: Selection{Span: Span{Start: b, StartExclusive: exclusive}}, Limit: 2})
			if i%5 == 0 {
				check(Query{Selection: Selection{Span: Span{Start: b, StartExclusive: exclusive}}, Limit: 1001})
			}
		}
		check(Query{Selection: Selection{Span: Span{End: b}}, Limit: 1})
		check(Query{Selection: Selection{Cursor: &Cursor{Key: b}}, Limit: 3})
		for j := i; j < min(i+4, len(bounds)); j++ {
			check(Query{Selection: Selection{Span: Span{Start: b, End: bounds[j]}}, Limit: 1001})
			check(Query{Selection: Selection{Span: Span{Start: b, StartExclusive: true, End: bounds[j]}}, Limit: 3})
			check(Query{Selection: Selection{Span: Span{Start: b}, Cursor: &Cursor{Key: bounds[j]}}, Limit: 3})
		}
	}
	// A prefix intersected with the start and the end (RANGE-1).
	for _, prefix := range []string{"d01/", "d01/s01/", "long/", "\xff", "zz", "nothing/"} {
		for i := 0; i < len(bounds); i += 9 {
			for _, exclusive := range []bool{false, true} {
				check(Query{Selection: Selection{Span: Span{Prefix: []byte(prefix), Start: bounds[i], StartExclusive: exclusive}}, Limit: 50})
				check(Query{Selection: Selection{Span: Span{Prefix: []byte(prefix), End: bounds[i]}}, Limit: 50})
				check(Query{Selection: Selection{Span: Span{Prefix: []byte(prefix), Start: bounds[i], End: bounds[min(i+30, len(bounds)-1)]}}, Limit: 1000})
			}
		}
		check(Query{Selection: Selection{Span: PrefixSpan([]byte(prefix))}, Limit: 1000})
	}
	check(Query{Limit: int64(math.MaxInt)})
	check(Query{Selection: Selection{Span: Span{Start: []byte{}}}, Limit: 5})                       // an empty start includes every key
	check(Query{Selection: Selection{Span: Span{Start: []byte{}, StartExclusive: true}}, Limit: 5}) // and so does an exclusive one
	if got, _ := d.Rows(ctx, Query{Selection: Selection{Span: Span{End: []byte{}}}, Limit: 5}, nil); len(got) != 0 {
		t.Fatalf("an empty end returned %d rows", len(got)) // QUERY-2
	}
}

// TestRangePagesToCompletion pages through the dataset at several limits
// with each page's last cursor: exactly the keys, in order, with values.
func TestRangePagesToCompletion(t *testing.T) {
	d, keys := treeDataset(t, 3000)
	for _, limit := range []int{1, 7, 999, 1000, 1001, 5000} {
		t.Run(fmt.Sprint("limit ", limit), func(t *testing.T) {
			var got []string
			rng := Query{Limit: int64(limit), Columns: []string{"size"}}
			for range len(keys) + 1 {
				page, err := d.Rows(ctx, rng, nil)
				if err != nil {
					t.Fatalf("%s: %v", describe(rng), err)
				}
				if len(page) > limit {
					t.Fatalf("%s: %d rows", describe(rng), len(page))
				}
				if len(page) == 0 {
					break
				}
				for i, row := range page {
					if row.Offset != 0 || row.Values[0].Absent() || row.Values[0].IsNull() {
						t.Fatalf("%s: row %d: offset %d, size %v", describe(rng), i, row.Offset, row.Values[0])
					}
				}
				got = append(got, keysOf(page)...)
				rng.Cursor = page[len(page)-1].Cursor()
			}
			if !slices.Equal(got, keys) {
				t.Fatalf("%d keys, want %d", len(got), len(keys))
			}
		})
	}
}

// TestListMatchesOracle lists prefixes at every level from many cursors,
// row and prefix, with and without an end, and compares the entries with
// the definition. Under -short it samples fewer points.
func TestListMatchesOracle(t *testing.T) {
	d, keys := treeDataset(t, 3000)
	prefixes := []string{"", "d01/", "d01/s01/", "long/", "é/", "zzz/", "a/b/", "\xff", "long/" + strings.Repeat("x", 90) + "/"}
	every := 13
	if testing.Short() {
		every = 97
	}
	bounds := probes(keys, every)
	var skipped, seeks int64
	for _, prefix := range prefixes {
		cursors := []*Cursor{nil}
		for _, b := range bounds {
			if bytes.HasPrefix(b, []byte(prefix)) || len(cursors) < 8 {
				cursors = append(cursors, &Cursor{Key: b}, &Cursor{Prefix: b})
			}
		}
		if len(cursors) > 80 {
			step := len(cursors) / 80
			var kept []*Cursor
			for i := 0; i < len(cursors); i += step {
				kept = append(kept, cursors[i])
			}
			cursors = kept
		}
		for _, cur := range cursors {
			for _, end := range [][]byte{nil, []byte("d05/"), []byte(prefix + "s03/")} {
				for _, limit := range []int{1, 3, 1000} {
					req := ListQuery{Prefix: []byte(prefix), Delimiter: '/', Cursor: cur, End: end, Limit: limit}
					var st ReadStats
					got, err := d.r.readList(ctx, req, &st)
					skipped, seeks = skipped+st.GroupsSkipped, seeks+st.PageIndexSeeks
					if err != nil {
						t.Fatalf("%s: %v", describe(req), err)
					}
					if want := listOracle(keys, req); !slices.Equal(entriesOf(got), want) {
						t.Fatalf("%s:\n got %q\nwant %q", describe(req), entriesOf(got), want)
					}
				}
			}
		}
	}
	// The truncated-bound fixture above often starts inside a group. Such
	// a shortcut now seeks its first candidate to establish Rows.Start and
	// must not count the group as skipped. Also exercise a whole-group
	// shortcut with an aligned start and a previously decoded witness.
	alignedKeys := []string{"a/0", "a/1", "a/2", "a/3", "a/4", "a/5", "b/0"}
	aligned := open(t, write(t, parts(testutil.Part{GroupRows: 3, PageBytes: 16}, alignedKeys)), OpenOptions{})
	_, err := aligned.Rows(ctx, Query{Limit: 1}, nil)
	must(t, err)
	var st ReadStats
	req := ListQuery{Delimiter: '/', Limit: 1}
	got, err := aligned.r.readList(ctx, req, &st)
	must(t, err)
	if want := listOracle(alignedKeys, req); !slices.Equal(entriesOf(got), want) {
		t.Fatalf("aligned shortcut: got %q, want %q", entriesOf(got), want)
	}
	skipped += st.GroupsSkipped
	if skipped == 0 || seeks == 0 {
		t.Fatalf("the listing never skipped: %d groups, %d seeks", skipped, seeks)
	}
}

// TestListPagination concatenates the pages of listings (PAGE-2) and
// requires exactly the entries of one unlimited request.
func TestListPagination(t *testing.T) {
	d, keys := treeDataset(t, 2000)
	for _, prefix := range []string{"", "d03/", "long/", "\xff"} {
		for _, delim := range []byte{'/', 's', 0xff} {
			want := listOracle(keys, ListQuery{Prefix: []byte(prefix), Delimiter: delim, Limit: math.MaxInt})
			for _, limit := range []int{1, 2, 5, 100} {
				req := ListQuery{Prefix: []byte(prefix), Delimiter: delim, Limit: limit}
				var got []string
				for range len(keys) + 1 {
					page, err := d.List(ctx, req, nil)
					if err != nil {
						t.Fatalf("%s: %v", describe(req), err)
					}
					if len(page) == 0 {
						break
					}
					got = append(got, entriesOf(page)...)
					req.Cursor = page[len(page)-1].Cursor()
				}
				if !slices.Equal(got, want) {
					t.Fatalf("prefix %q delimiter %q limit %d: %d entries, want %d", prefix, delim, limit, len(got), len(want))
				}
			}
		}
	}
}

// TestDelimiterEdgeCases lists a dataset of awkward keys under every
// combination of prefix, delimiter, cursor and end drawn from its own keys
// and compares with the definition: keys equal to prefixes, trailing and
// repeated delimiters, a non-ASCII delimiter byte, prefixes without a
// successor, and cursors inside a common prefix.
func TestDelimiterEdgeCases(t *testing.T) {
	keys := []string{"/", "//x", "a", "a/", "a//", "a//c", "a/b", "a/b/", "a/b//", "a/b/c", "a/b/c/d", "a/c", "a\xffb", "a\xffb\xffc",
		"b", "\xff", "\xff/", "\xff/a", "\xff\xff", "\xff\xff/x", "\xff\xff\xff"}
	d := open(t, write(t, parts(testutil.Part{GroupRows: 3, PageBytes: 64}, split(keys, 3)...)), OpenOptions{})
	points := [][]byte{nil, {}, []byte("a/b"), []byte("a/b/"), []byte("a/b/c"), []byte("a/"), []byte("a//c"), []byte("\xff"), []byte("\xff\xff"), []byte("zz")}
	prefixes := []string{"", "a", "a/", "a/b/", "\xff", "\xff\xff", "a\xff", "nothing"}
	for _, prefix := range prefixes {
		for _, delim := range []byte{'/', 0xff, 'b'} {
			for _, at := range points {
				cursors := []*Cursor{nil}
				if at != nil {
					cursors = append(cursors, &Cursor{Key: at}, &Cursor{Prefix: at})
				}
				for _, cur := range cursors {
					for _, end := range points {
						for _, limit := range []int{1, 2, 100} {
							req := ListQuery{Prefix: []byte(prefix), Delimiter: delim, Cursor: cur, End: end, Limit: limit}
							got, err := d.List(ctx, req, nil)
							if err != nil {
								t.Fatalf("%s: %v", describe(req), err)
							}
							if want := listOracle(keys, req); !slices.Equal(entriesOf(got), want) {
								t.Fatalf("%s:\n got %q\nwant %q", describe(req), entriesOf(got), want)
							}
						}
					}
				}
			}
		}
	}
	// The cases named in the specification.
	for _, c := range []struct {
		req  ListQuery
		want []string
	}{
		// a/ is a row; a/b/ rolls up to itself (LIST-3).
		{ListQuery{Prefix: []byte("a/"), Delimiter: '/', Limit: 100}, []string{"r:a/", "P:a//", "r:a/b", "P:a/b/", "r:a/c"}},
		// After the common prefix a/b/, the keys after every key under it.
		{ListQuery{Prefix: []byte("a/"), Delimiter: '/', Cursor: &Cursor{Prefix: []byte("a/b/")}, Limit: 100}, []string{"r:a/c"}},
		// After a row cursor inside a/b/, a/b/ rolls up again (LIST-5).
		{ListQuery{Prefix: []byte("a/"), Delimiter: '/', Cursor: &Cursor{Key: []byte("a/b/c")}, Limit: 100}, []string{"p:a/b/", "r:a/c"}},
	} {
		got, err := d.List(ctx, c.req, nil)
		if err != nil || !slices.Equal(entriesOf(got), c.want) {
			t.Fatalf("%s: %q, %v; want %q", describe(c.req), entriesOf(got), err, c.want)
		}
	}
}

// TestListExact: a common prefix carries its exact row (LIST-8), from the
// scan and from the group shortcut, at every page size, after prefix and
// row cursors, with its projected values; it changes no key-side read.
func TestListExact(t *testing.T) {
	keys := []string{"a/", "a/1", "a/2", "a/3", "a/4", "a/5", "b/1", "b/2", "b/3", "b/4", "c", "d/", "d/1"}
	dir := write(t, parts(testutil.Part{GroupRows: 2}, keys))
	spell := func(entries []ListEntry) []string {
		var out []string
		for _, e := range entries {
			switch {
			case e.Row != nil:
				out = append(out, fmt.Sprintf("r:%s=%d", e.Row.Key, e.Row.Values[0].Int64()))
			case e.Exact != nil:
				out = append(out, fmt.Sprintf("p:%s=%s/%d=%d", e.CommonPrefix, e.Exact.Key, e.Exact.Offset, e.Exact.Values[0].Int64()))
			default:
				out = append(out, "p:"+string(e.CommonPrefix))
			}
		}
		return out
	}
	for _, verify := range []bool{false, true} {
		d := open(t, dir, OpenOptions{})
		if verify {
			must(t, d.Verify(ctx, nil))
		}
		for _, c := range []struct {
			cur  *Cursor
			want []string
		}{
			{nil, []string{"p:a/=a//0=0", "p:b/", "r:c=10", "p:d/=d//0=11"}},
			{&Cursor{Prefix: []byte("a/")}, []string{"p:b/", "r:c=10", "p:d/=d//0=11"}},
			// After a row cursor inside a/, a/ rolls up again without the
			// row at its exact key, which precedes the cursor.
			{&Cursor{Key: []byte("a/")}, []string{"p:a/", "p:b/", "r:c=10", "p:d/=d//0=11"}},
			{&Cursor{Key: []byte("a/2")}, []string{"p:a/", "p:b/", "r:c=10", "p:d/=d//0=11"}},
			{&Cursor{Key: []byte("c")}, []string{"p:d/=d//0=11"}},
		} {
			for _, limit := range []int{1, 2, 3, 100} {
				req := ListQuery{Delimiter: '/', Cursor: c.cur, Limit: limit, Columns: []string{"size"}}
				var got []string
				for range len(keys) + 1 {
					page, err := d.List(ctx, req, nil)
					if err != nil {
						t.Fatalf("%s: %v", describe(req), err)
					}
					got = append(got, spell(page)...)
					if len(page) < limit {
						break
					}
					req.Cursor = page[len(page)-1].Cursor()
				}
				if !slices.Equal(got, c.want) {
					t.Fatalf("verified %v, after %s, limit %d:\n got %q\nwant %q", verify, cursorString(c.cur), limit, got, c.want)
				}
			}
		}
		// Projecting the exact rows reads their value pages and nothing
		// else: the key-side counts are those of a keys-only listing.
		var keysOnly, projected ReadStats
		_, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 100}, &keysOnly)
		must(t, err)
		_, err = d.List(ctx, ListQuery{Delimiter: '/', Limit: 100, Columns: []string{"size"}}, &projected)
		must(t, err)
		if keysOnly.ValuePagesDecoded != 0 || projected.ValuePagesDecoded == 0 || verify && keysOnly.GroupsSkipped == 0 ||
			keysOnly.KeyPagesDecoded != projected.KeyPagesDecoded || keysOnly.KeysDecoded != projected.KeysDecoded ||
			keysOnly.GroupsSkipped != projected.GroupsSkipped || keysOnly.PrefixSkips != projected.PrefixSkips {
			t.Fatalf("verified %v: keys only %+v, projected %+v", verify, keysOnly, projected)
		}
		var none ReadStats
		_, err = d.List(ctx, ListQuery{Delimiter: '/', Cursor: &Cursor{Prefix: []byte("a/")}, End: []byte("c"), Limit: 100, Columns: []string{"size"}}, &none)
		if err != nil || none.ValuePagesDecoded != 0 {
			t.Fatalf("a listing without exact rows decoded %d value pages, %v", none.ValuePagesDecoded, err)
		}
	}
}

// TestListNarrowUpperBound lists after a/5 below a/8 over row groups [a/1]
// and [a/9] with prefix a/: no key lies in the range, so neither the scan
// nor the group shortcut may produce an entry (LIST-7).
func TestListNarrowUpperBound(t *testing.T) {
	d := verified(t, write(t, parts(testutil.Part{GroupRows: 1}, []string{"a/1", "a/9"})), OpenOptions{})
	for _, prefix := range []string{"a/", ""} {
		for _, cur := range []*Cursor{{Key: []byte("a/5")}, {Prefix: []byte("a/4")}} {
			req := ListQuery{Prefix: []byte(prefix), Delimiter: '/', Cursor: cur, End: []byte("a/8"), Limit: 10}
			if got, err := d.List(ctx, req, nil); err != nil || len(got) != 0 {
				t.Fatalf("%s: %q, %v", describe(req), entriesOf(got), err)
			}
		}
	}
	// The specification's LIST-7 example.
	d = open(t, write(t, parts(testutil.Part{GroupRows: 1}, []string{"a/a", "a/z"})), OpenOptions{})
	got, _ := d.List(ctx, ListQuery{Delimiter: '/', End: []byte("a/b"), Limit: 10}, nil)
	if want := []string{"p:a/"}; !slices.Equal(entriesOf(got), want) {
		t.Fatalf("%q, want %q", entriesOf(got), want)
	}
	got, _ = d.List(ctx, ListQuery{Delimiter: '/', Cursor: &Cursor{Key: []byte("a/a")}, End: []byte("a/b"), Limit: 10}, nil)
	if len(got) != 0 {
		t.Fatalf("a/z alone made %q appear", entriesOf(got))
	}
}

// TestListMaxIntLimit lists with limit math.MaxInt.
func TestListMaxIntLimit(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{}, []string{"a", "b/c"})), OpenOptions{})
	got, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: math.MaxInt}, nil)
	if want := []string{"r:a", "p:b/"}; err != nil || !slices.Equal(entriesOf(got), want) {
		t.Fatalf("%q, %v", entriesOf(got), err)
	}
}

// TestInvalidRequests: a limit that is not positive, or a malformed
// cursor, is an invalid request, never an empty result (QUERY-1); a range
// takes only a row cursor.
func TestInvalidRequests(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{}, []string{"a"})), OpenOptions{})
	empty := open(t, write(t, testutil.Dataset{}), OpenOptions{})
	bad := []*Cursor{{}, {Key: []byte("a"), Offset: -1}, {Key: []byte("a"), Prefix: []byte("a/")}, {Prefix: []byte("a/"), Offset: 1}}
	for name, ds := range map[string]*Dataset{"dataset": d, "empty dataset": empty} {
		t.Run(name, func(t *testing.T) {
			for _, limit := range []int{0, -1, math.MinInt} {
				if _, err := ds.Rows(ctx, Query{Limit: int64(limit)}, nil); !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("range limit %d: %v", limit, err)
				}
				if _, err := ds.List(ctx, ListQuery{Delimiter: '/', Limit: limit}, nil); !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("list limit %d: %v", limit, err)
				}
			}
			for _, cur := range bad {
				if _, err := ds.Rows(ctx, Query{Selection: Selection{Cursor: cur}, Limit: 1}, nil); !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("range cursor %s: %v", cursorString(cur), err)
				}
				if _, err := ds.List(ctx, ListQuery{Cursor: cur, Limit: 1}, nil); !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("list cursor %s: %v", cursorString(cur), err)
				}
			}
			if _, err := ds.Rows(ctx, Query{Selection: Selection{Cursor: &Cursor{Prefix: []byte("a/")}}, Limit: 1}, nil); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("range with a prefix cursor: %v", err)
			}
		})
	}
}

// TestUniqueCursors: with unique keys a row cursor (k, 0) resumes after k
// like an exclusive start, a larger offset means the same, and offsets
// returned are zero.
func TestUniqueCursors(t *testing.T) {
	d, keys := treeDataset(t, 1000)
	for _, i := range []int{0, 1, 399, 400, 401, 666, 667, 998, 999} {
		k := []byte(keys[i])
		want := keys[i+1 : min(i+4, len(keys))]
		for _, offset := range []int64{0, 1, math.MaxInt64} {
			rng := Query{Selection: Selection{Cursor: &Cursor{Key: k, Offset: offset}}, Limit: 3}
			got, err := d.Rows(ctx, rng, nil)
			if err != nil || !slices.Equal(keysOf(got), want) {
				t.Fatalf("%s: %q, %v; want %q", describe(rng), keysOf(got), err, want)
			}
			for _, row := range got {
				if row.Offset != 0 {
					t.Fatalf("%s: offset %d with unique keys", describe(rng), row.Offset)
				}
			}
		}
	}
}

// TestPrefixRange reads prefixes, including ones without a successor.
func TestPrefixRange(t *testing.T) {
	keys := []string{"a", "a/1", "a/2", "b", "\xff", "\xff\x00", "\xff\xff"}
	d := open(t, write(t, parts(testutil.Part{}, keys)), OpenOptions{})
	for prefix, want := range map[string][]string{"a/": {"a/1", "a/2"}, "a": {"a", "a/1", "a/2"}, "\xff": {"\xff", "\xff\x00", "\xff\xff"}, "\xff\xff": {"\xff\xff"}, "": keys, "c": nil} {
		got, err := d.Rows(ctx, Query{Selection: Selection{Span: PrefixSpan([]byte(prefix))}, Limit: 100}, nil)
		if err != nil || !slices.Equal(keysOf(got), want) {
			t.Fatalf("prefix %q: %q, %v", prefix, keysOf(got), err)
		}
	}
}

// TestEmptyGroupsAndOneRowParts reads a part whose physical row groups are
// [empty, a, empty, m, empty] followed by a one-row part holding z, from
// at, before and after every key, inclusive and exclusive, with and
// without an end, at limits 1 and 2.
func TestEmptyGroupsAndOneRowParts(t *testing.T) {
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{
		{Rows: testutil.Rows("a", "m"), Groups: []int{0, 1, 0, 1, 0}},
		{Rows: testutil.Rows("z")},
	}})
	meta, err := testutil.ReadFooter(partFile(dir, 0))
	if err != nil || len(meta.RowGroups) != 5 {
		t.Fatalf("physical row groups: %v", err)
	}
	d := open(t, dir, OpenOptions{})
	must(t, d.Verify(ctx, nil))
	if groups := len(d.r.parts[0].groups) + len(d.r.parts[1].groups); groups != 3 || d.r.rows != 3 || len(d.r.parts) != 2 {
		t.Fatalf("%d groups, %d rows", groups, d.r.rows)
	}
	keys := []string{"a", "m", "z"}
	points := [][]byte{nil, []byte("\x00"), []byte("a"), []byte("a\x00"), []byte("b"), []byte("m"), []byte("m\x00"), []byte("z"), []byte("z\x00")}
	for _, start := range points {
		for _, end := range points {
			for _, exclusive := range []bool{false, true} {
				for _, limit := range []int{1, 2} {
					rng := Query{Selection: Selection{Span: Span{Start: start, StartExclusive: exclusive, End: end}}, Limit: int64(limit)}
					got, err := d.Rows(ctx, rng, nil)
					if err != nil || !slices.Equal(keysOf(got), rangeOracle(keys, rng)) {
						t.Fatalf("%s: %q, %v", describe(rng), keysOf(got), err)
					}
				}
			}
		}
	}
}

// TestEmptyDataset opens parts: [] and reads nothing.
func TestEmptyDataset(t *testing.T) {
	d := open(t, write(t, testutil.Dataset{}), OpenOptions{})
	rows, err := d.Rows(ctx, Query{Limit: 10, Columns: []string{"x"}}, nil)
	if err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	entries, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 10}, nil)
	if err != nil || len(entries) != 0 {
		t.Fatal(entries, err)
	}
	if m := d.Metadata(); len(m.Parts) != 0 || m.SchemaID != testutil.SchemaID {
		t.Fatalf("%+v", m)
	}
}

// TestLongKeys reads keys far longer than the index bound, whose shortened
// bounds overlap from page to page (RG-6), from every key.
func TestLongKeys(t *testing.T) {
	var keys []string
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("%s/%s%04d", strings.Repeat("p", 1100), strings.Repeat("q", i%3*400), i))
	}
	slices.Sort(keys)
	d := open(t, write(t, parts(testutil.Part{PageBytes: 4096, GroupRows: 100, IndexSizeLimit: 64}, split(keys, 2)...)), OpenOptions{})
	for i, k := range keys {
		got, err := d.Rows(ctx, Query{Selection: Selection{Span: Span{Start: []byte(k)}}, Limit: 2}, nil)
		if err != nil || len(got) == 0 || string(got[0].Key) != k {
			t.Fatalf("from key %d: %d rows, %v", i, len(got), err)
		}
	}
}
