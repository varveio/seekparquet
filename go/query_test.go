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
	return open(t, write(t, d), Options{}), keys
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
	check := func(rng Range) {
		t.Helper()
		got, err := d.Range(ctx, rng)
		if err != nil {
			t.Fatalf("%s: %v", describe(rng), err)
		}
		if want := rangeOracle(keys, rng); !slices.Equal(keysOf(got), want) {
			t.Fatalf("%s: %d keys, want %d", describe(rng), len(got), len(want))
		}
	}
	for i, b := range bounds {
		for _, exclusive := range []bool{false, true} {
			check(Range{Start: b, StartExclusive: exclusive, Limit: 2})
			if i%5 == 0 {
				check(Range{Start: b, StartExclusive: exclusive, Limit: 1001})
			}
		}
		check(Range{End: b, Limit: 1})
		check(Range{Cursor: &Cursor{Key: b}, Limit: 3})
		for j := i; j < min(i+4, len(bounds)); j++ {
			check(Range{Start: b, End: bounds[j], Limit: 1001})
			check(Range{Start: b, StartExclusive: true, End: bounds[j], Limit: 3})
			check(Range{Start: b, Cursor: &Cursor{Key: bounds[j]}, Limit: 3})
		}
	}
	// A prefix intersected with the start and the end (RANGE-1).
	for _, prefix := range []string{"d01/", "d01/s01/", "long/", "\xff", "zz", "nothing/"} {
		for i := 0; i < len(bounds); i += 9 {
			for _, exclusive := range []bool{false, true} {
				check(Range{Prefix: []byte(prefix), Start: bounds[i], StartExclusive: exclusive, Limit: 50})
				check(Range{Prefix: []byte(prefix), End: bounds[i], Limit: 50})
				check(Range{Prefix: []byte(prefix), Start: bounds[i], End: bounds[min(i+30, len(bounds)-1)], Limit: 1000})
			}
		}
		check(PrefixRange([]byte(prefix), 1000))
	}
	check(Range{Limit: math.MaxInt})
	check(Range{Start: []byte{}, Limit: 5})                       // an empty start includes every key
	check(Range{Start: []byte{}, StartExclusive: true, Limit: 5}) // and so does an exclusive one
	if got, _ := d.Range(ctx, Range{End: []byte{}, Limit: 5}); len(got) != 0 {
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
			rng := Range{Limit: limit, Columns: []string{"size"}}
			for range len(keys) + 1 {
				page, err := d.Range(ctx, rng)
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
					if row.Offset != 0 || row.Values[0].Absent || row.Values[0].IsNull() {
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
					req := List{Prefix: []byte(prefix), Delimiter: '/', Cursor: cur, End: end, Limit: limit}
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
			want := listOracle(keys, List{Prefix: []byte(prefix), Delimiter: delim, Limit: math.MaxInt})
			for _, limit := range []int{1, 2, 5, 100} {
				req := List{Prefix: []byte(prefix), Delimiter: delim, Limit: limit}
				var got []string
				for range len(keys) + 1 {
					page, err := d.List(ctx, req)
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
	d := open(t, write(t, parts(testutil.Part{GroupRows: 3, PageBytes: 64}, split(keys, 3)...)), Options{})
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
							req := List{Prefix: []byte(prefix), Delimiter: delim, Cursor: cur, End: end, Limit: limit}
							got, err := d.List(ctx, req)
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
		req  List
		want []string
	}{
		// a/ is a row; a/b/ rolls up to itself (LIST-3).
		{List{Prefix: []byte("a/"), Delimiter: '/', Limit: 100}, []string{"r:a/", "p:a//", "r:a/b", "p:a/b/", "r:a/c"}},
		// After the common prefix a/b/, the keys after every key under it.
		{List{Prefix: []byte("a/"), Delimiter: '/', Cursor: &Cursor{Prefix: []byte("a/b/")}, Limit: 100}, []string{"r:a/c"}},
		// After a row cursor inside a/b/, a/b/ rolls up again (LIST-5).
		{List{Prefix: []byte("a/"), Delimiter: '/', Cursor: &Cursor{Key: []byte("a/b/c")}, Limit: 100}, []string{"p:a/b/", "r:a/c"}},
	} {
		got, err := d.List(ctx, c.req)
		if err != nil || !slices.Equal(entriesOf(got), c.want) {
			t.Fatalf("%s: %q, %v; want %q", describe(c.req), entriesOf(got), err, c.want)
		}
	}
}

// TestListNarrowUpperBound lists after a/5 below a/8 over row groups [a/1]
// and [a/9] with prefix a/: no key lies in the range, so neither the scan
// nor the group shortcut may produce an entry (LIST-7).
func TestListNarrowUpperBound(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{GroupRows: 1}, []string{"a/1", "a/9"})), Options{VerifyOnOpen: true})
	for _, prefix := range []string{"a/", ""} {
		for _, cur := range []*Cursor{{Key: []byte("a/5")}, {Prefix: []byte("a/4")}} {
			req := List{Prefix: []byte(prefix), Delimiter: '/', Cursor: cur, End: []byte("a/8"), Limit: 10}
			if got, err := d.List(ctx, req); err != nil || len(got) != 0 {
				t.Fatalf("%s: %q, %v", describe(req), entriesOf(got), err)
			}
		}
	}
	// The specification's LIST-7 example.
	d = open(t, write(t, parts(testutil.Part{GroupRows: 1}, []string{"a/a", "a/z"})), Options{})
	got, _ := d.List(ctx, List{Delimiter: '/', End: []byte("a/b"), Limit: 10})
	if want := []string{"p:a/"}; !slices.Equal(entriesOf(got), want) {
		t.Fatalf("%q, want %q", entriesOf(got), want)
	}
	got, _ = d.List(ctx, List{Delimiter: '/', Cursor: &Cursor{Key: []byte("a/a")}, End: []byte("a/b"), Limit: 10})
	if len(got) != 0 {
		t.Fatalf("a/z alone made %q appear", entriesOf(got))
	}
}

// TestListMaxIntLimit lists with limit math.MaxInt.
func TestListMaxIntLimit(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{}, []string{"a", "b/c"})), Options{})
	got, err := d.List(ctx, List{Delimiter: '/', Limit: math.MaxInt})
	if want := []string{"r:a", "p:b/"}; err != nil || !slices.Equal(entriesOf(got), want) {
		t.Fatalf("%q, %v", entriesOf(got), err)
	}
}

// TestInvalidRequests: a limit that is not positive, or a malformed
// cursor, is an invalid request, never an empty result (QUERY-1); a range
// takes only a row cursor.
func TestInvalidRequests(t *testing.T) {
	d := open(t, write(t, parts(testutil.Part{}, []string{"a"})), Options{})
	empty := open(t, write(t, testutil.Dataset{}), Options{})
	bad := []*Cursor{{}, {Key: []byte("a"), Offset: -1}, {Key: []byte("a"), Prefix: []byte("a/")}, {Prefix: []byte("a/"), Offset: 1}}
	for name, ds := range map[string]*Dataset{"dataset": d, "empty dataset": empty} {
		t.Run(name, func(t *testing.T) {
			for _, limit := range []int{0, -1, math.MinInt} {
				if _, err := ds.Range(ctx, Range{Limit: limit}); !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("range limit %d: %v", limit, err)
				}
				if _, err := ds.List(ctx, List{Delimiter: '/', Limit: limit}); !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("list limit %d: %v", limit, err)
				}
			}
			for _, cur := range bad {
				if _, err := ds.Range(ctx, Range{Cursor: cur, Limit: 1}); !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("range cursor %s: %v", cursorString(cur), err)
				}
				if _, err := ds.List(ctx, List{Cursor: cur, Limit: 1}); !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("list cursor %s: %v", cursorString(cur), err)
				}
			}
			if _, err := ds.Range(ctx, Range{Cursor: &Cursor{Prefix: []byte("a/")}, Limit: 1}); !errors.Is(err, ErrInvalidRequest) {
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
			rng := Range{Cursor: &Cursor{Key: k, Offset: offset}, Limit: 3}
			got, err := d.Range(ctx, rng)
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
	d := open(t, write(t, parts(testutil.Part{}, keys)), Options{})
	for prefix, want := range map[string][]string{"a/": {"a/1", "a/2"}, "a": {"a", "a/1", "a/2"}, "\xff": {"\xff", "\xff\x00", "\xff\xff"}, "\xff\xff": {"\xff\xff"}, "": keys, "c": nil} {
		got, err := d.Range(ctx, PrefixRange([]byte(prefix), 100))
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
	d := open(t, dir, Options{})
	if len(d.r.groups) != 3 || d.r.rows != 3 || len(d.r.parts) != 2 {
		t.Fatalf("%d groups, %d rows", len(d.r.groups), d.r.rows)
	}
	keys := []string{"a", "m", "z"}
	points := [][]byte{nil, []byte("\x00"), []byte("a"), []byte("a\x00"), []byte("b"), []byte("m"), []byte("m\x00"), []byte("z"), []byte("z\x00")}
	for _, start := range points {
		for _, end := range points {
			for _, exclusive := range []bool{false, true} {
				for _, limit := range []int{1, 2} {
					rng := Range{Start: start, StartExclusive: exclusive, End: end, Limit: limit}
					got, err := d.Range(ctx, rng)
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
	d := open(t, write(t, testutil.Dataset{}), Options{})
	rows, err := d.Range(ctx, Range{Limit: 10, Columns: []string{"x"}})
	if err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	entries, err := d.List(ctx, List{Delimiter: '/', Limit: 10})
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
	d := open(t, write(t, parts(testutil.Part{PageBytes: 4096, GroupRows: 100, IndexSizeLimit: 64}, split(keys, 2)...)), Options{})
	for i, k := range keys {
		got, err := d.Range(ctx, Range{Start: []byte(k), Limit: 2})
		if err != nil || len(got) == 0 || string(got[0].Key) != k {
			t.Fatalf("from key %d: %d rows, %v", i, len(got), err)
		}
	}
}
