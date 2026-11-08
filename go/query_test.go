// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/seekparquettest"
)

// treeDataset is fixtureKeys(n) in three parts of row groups of 400 rows,
// small pages, and bounds shortened to 8 bytes, so that adjacent pages'
// bounds overlap on the long shared stems.
func treeDataset(t testing.TB, n int) (*seekparquet.Dataset, []string) {
	keys := fixtureKeys(n)
	d := parts(seekparquettest.Part{GroupRows: 400, PageBytes: 512, IndexSizeLimit: 8}, split(keys, 3)...)
	return open(t, write(t, d), seekparquet.Options{}), keys
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
	check := func(rng seekparquet.Range) {
		t.Helper()
		got, err := d.Range(ctx, rng, nil)
		if err != nil {
			t.Fatal(err)
		}
		if want := rangeOracle(keys, rng); !slices.Equal(keysOf(got), want) {
			t.Fatalf("start %s (exclusive %v) end %s limit %d: %d keys, want %d", short(rng.Start), rng.StartExclusive, short(rng.End), rng.Limit, len(got), len(want))
		}
	}
	for i, b := range bounds {
		for _, exclusive := range []bool{false, true} {
			check(seekparquet.Range{Start: b, StartExclusive: exclusive, Limit: 2})
			if i%5 == 0 {
				check(seekparquet.Range{Start: b, StartExclusive: exclusive, Limit: 1001})
			}
		}
		check(seekparquet.Range{End: b, Limit: 1})
		for j := i; j < min(i+4, len(bounds)); j++ {
			check(seekparquet.Range{Start: b, End: bounds[j], Limit: 1001})
			check(seekparquet.Range{Start: b, StartExclusive: true, End: bounds[j], Limit: 3})
		}
	}
	check(seekparquet.Range{Limit: math.MaxInt})
	check(seekparquet.Range{Start: []byte{}, Limit: 5})                       // an empty start includes every key
	check(seekparquet.Range{Start: []byte{}, StartExclusive: true, Limit: 5}) // and so does an exclusive one
	if got, _ := d.Range(ctx, seekparquet.Range{End: []byte{}, Limit: 5}, nil); len(got) != 0 {
		t.Fatalf("an empty end returned %d rows", len(got)) // QUERY-2
	}
}

// TestRangePagesToCompletion pages through the dataset at several limits,
// resuming after the last key exclusively: exactly the keys, in order.
func TestRangePagesToCompletion(t *testing.T) {
	d, keys := treeDataset(t, 3000)
	for _, limit := range []int{1, 7, 999, 1000, 1001, 5000} {
		var got []string
		rng := seekparquet.Range{Limit: limit, Columns: []string{"size"}}
		for {
			page, err := d.Range(ctx, rng, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(page) > limit {
				t.Fatalf("limit %d: %d rows", limit, len(page))
			}
			if len(page) == 0 {
				break
			}
			got = append(got, keysOf(page)...)
			rng.Start, rng.StartExclusive = page[len(page)-1].Key, true
		}
		if !slices.Equal(got, keys) {
			t.Fatalf("limit %d: %d keys, want %d", limit, len(got), len(keys))
		}
	}
}

// TestListMatchesOracle lists prefixes at every level from many resume
// points, with and without continuation and an end, and compares the
// entries with the definition. Under -short it samples fewer points.
func TestListMatchesOracle(t *testing.T) {
	d, keys := treeDataset(t, 3000)
	prefixes := []string{"", "d01/", "d01/s01/", "long/", "é/", "zzz/", "a/b/", "\xff", "long/" + strings.Repeat("x", 90) + "/"}
	every := 13
	if testing.Short() {
		every = 97
	}
	bounds := probes(keys, every)
	var stats seekparquet.Stats
	for _, prefix := range prefixes {
		afters := [][]byte{nil}
		for _, b := range bounds {
			if bytes.HasPrefix(b, []byte(prefix)) || len(afters) < 4 {
				afters = append(afters, b)
			}
		}
		if len(afters) > 40 {
			step := len(afters) / 40
			var kept [][]byte
			for i := 0; i < len(afters); i += step {
				kept = append(kept, afters[i])
			}
			afters = kept
		}
		for _, after := range afters {
			for _, continuation := range []bool{false, true} {
				for _, end := range [][]byte{nil, []byte("d05/"), []byte(prefix + "s03/")} {
					for _, limit := range []int{1, 3, 1000} {
						req := seekparquet.List{Prefix: []byte(prefix), Delimiter: '/', After: after, Continuation: continuation, End: end, Limit: limit}
						got, err := d.List(ctx, req, &stats)
						if err != nil {
							t.Fatal(err)
						}
						if want := listOracle(keys, req); !slices.Equal(entriesOf(got), want) {
							t.Fatalf("prefix %q after %s continuation %v end %q limit %d:\n got %q\nwant %q", prefix, short(after), continuation, end, limit, entriesOf(got), want)
						}
					}
				}
			}
		}
	}
	if stats.GroupShortcuts == 0 || stats.Reseeks == 0 {
		t.Fatalf("the listing never skipped: %+v", stats)
	}
}

// TestListPagination concatenates the pages of listings (LIST-6) and
// requires exactly the entries of one unlimited request.
func TestListPagination(t *testing.T) {
	d, keys := treeDataset(t, 2000)
	for _, prefix := range []string{"", "d03/", "long/", "\xff"} {
		for _, delim := range []byte{'/', 's', 0xff} {
			whole := seekparquet.List{Prefix: []byte(prefix), Delimiter: delim, Limit: math.MaxInt}
			want := listOracle(keys, whole)
			for _, limit := range []int{1, 2, 5, 100} {
				req := seekparquet.List{Prefix: []byte(prefix), Delimiter: delim, Limit: limit}
				var got []string
				for range len(keys) + 1 {
					page, err := d.List(ctx, req, nil)
					if err != nil {
						t.Fatal(err)
					}
					if len(page) == 0 {
						break
					}
					got = append(got, entriesOf(page)...)
					req.After, req.Continuation = page[len(page)-1].Bytes(), true
				}
				if !slices.Equal(got, want) {
					t.Fatalf("prefix %q delimiter %q limit %d: %d entries, want %d", prefix, delim, limit, len(got), len(want))
				}
			}
		}
	}
}

// TestDelimiterEdgeCases lists a dataset of awkward keys under every
// combination of prefix, delimiter, after, continuation and end drawn from
// its own keys and compares with the definition: keys equal to prefixes,
// trailing and repeated delimiters, a non-ASCII delimiter byte, prefixes
// without a successor, and resumes inside a common prefix.
func TestDelimiterEdgeCases(t *testing.T) {
	keys := []string{"/", "//x", "a", "a/", "a//", "a//c", "a/b", "a/b/", "a/b//", "a/b/c", "a/b/c/d", "a/c", "a\xffb", "a\xffb\xffc",
		"b", "\xff", "\xff/", "\xff/a", "\xff\xff", "\xff\xff/x", "\xff\xff\xff"}
	d := open(t, write(t, parts(seekparquettest.Part{GroupRows: 3, PageBytes: 64}, split(keys, 3)...)), seekparquet.Options{})
	points := [][]byte{nil, {}, []byte("a/b"), []byte("a/b/"), []byte("a/b/c"), []byte("a/"), []byte("a//c"), []byte("\xff"), []byte("\xff\xff"), []byte("zz")}
	prefixes := []string{"", "a", "a/", "a/b/", "\xff", "\xff\xff", "a\xff", "nothing"}
	for _, prefix := range prefixes {
		for _, delim := range []byte{'/', 0xff, 'b'} {
			for _, after := range points {
				for _, end := range points {
					for _, continuation := range []bool{false, true} {
						for _, limit := range []int{1, 2, 100} {
							req := seekparquet.List{Prefix: []byte(prefix), Delimiter: delim, After: after, Continuation: continuation, End: end, Limit: limit}
							got, err := d.List(ctx, req, nil)
							if err != nil {
								t.Fatal(err)
							}
							if want := listOracle(keys, req); !slices.Equal(entriesOf(got), want) {
								t.Fatalf("prefix %q delimiter %q after %q continuation %v end %q limit %d:\n got %q\nwant %q",
									prefix, delim, after, continuation, end, limit, entriesOf(got), want)
							}
						}
					}
				}
			}
		}
	}
	// The cases named in the specification.
	got, _ := d.List(ctx, seekparquet.List{Prefix: []byte("a/"), Delimiter: '/', Limit: 100}, nil)
	if want := []string{"r:a/", "p:a//", "r:a/b", "p:a/b/", "r:a/c"}; !slices.Equal(entriesOf(got), want) {
		t.Fatalf("a/: %q, want %q", entriesOf(got), want) // a/ is a row; a/b/ rolls up to itself
	}
	got, _ = d.List(ctx, seekparquet.List{Prefix: []byte("a/"), Delimiter: '/', After: []byte("a/b/c"), Continuation: true, Limit: 100}, nil)
	if want := []string{"r:a/c"}; !slices.Equal(entriesOf(got), want) {
		t.Fatalf("continuation inside a/b/: %q, want %q", entriesOf(got), want)
	}
	got, _ = d.List(ctx, seekparquet.List{Prefix: []byte("a/"), Delimiter: '/', After: []byte("a/b/c"), Limit: 100}, nil)
	if want := []string{"p:a/b/", "r:a/c"}; !slices.Equal(entriesOf(got), want) {
		t.Fatalf("no continuation inside a/b/: %q, want %q", entriesOf(got), want)
	}
}

// TestListNarrowUpperBound lists [a/5, a/8) over row groups [a/1] and
// [a/9] with prefix a/: no key lies in the range, so neither rule may
// produce an entry (LIST-7).
func TestListNarrowUpperBound(t *testing.T) {
	d := open(t, write(t, parts(seekparquettest.Part{GroupRows: 1}, []string{"a/1", "a/9"})), seekparquet.Options{})
	for _, prefix := range []string{"a/", ""} {
		for _, continuation := range []bool{false, true} {
			got, err := d.List(ctx, seekparquet.List{Prefix: []byte(prefix), Delimiter: '/', After: []byte("a/5"), Continuation: continuation, End: []byte("a/8"), Limit: 10}, nil)
			if err != nil || len(got) != 0 {
				t.Fatalf("prefix %q continuation %v: %q, %v", prefix, continuation, entriesOf(got), err)
			}
		}
	}
	// The specification's LIST-7 example.
	d = open(t, write(t, parts(seekparquettest.Part{GroupRows: 1}, []string{"a/a", "a/z"})), seekparquet.Options{})
	got, _ := d.List(ctx, seekparquet.List{Delimiter: '/', End: []byte("a/b"), Limit: 10}, nil)
	if want := []string{"p:a/"}; !slices.Equal(entriesOf(got), want) {
		t.Fatalf("%q, want %q", entriesOf(got), want)
	}
	got, _ = d.List(ctx, seekparquet.List{Delimiter: '/', After: []byte("a/a"), End: []byte("a/b"), Limit: 10}, nil)
	if len(got) != 0 {
		t.Fatalf("a/z alone made %q appear", entriesOf(got))
	}
}

// TestListMaxIntLimit lists with limit math.MaxInt.
func TestListMaxIntLimit(t *testing.T) {
	d := open(t, write(t, parts(seekparquettest.Part{}, []string{"a", "b/c"})), seekparquet.Options{})
	got, err := d.List(ctx, seekparquet.List{Delimiter: '/', Limit: math.MaxInt}, nil)
	if want := []string{"r:a", "p:b/"}; err != nil || !slices.Equal(entriesOf(got), want) {
		t.Fatalf("%q, %v", entriesOf(got), err)
	}
}

// TestInvalidRequests: a limit that is not positive is an invalid request,
// never an empty result (QUERY-1).
func TestInvalidRequests(t *testing.T) {
	d := open(t, write(t, parts(seekparquettest.Part{}, []string{"a"})), seekparquet.Options{})
	empty := open(t, write(t, seekparquettest.Dataset{}), seekparquet.Options{})
	for _, ds := range []*seekparquet.Dataset{d, empty} {
		for _, limit := range []int{0, -1, math.MinInt} {
			if _, err := ds.Range(ctx, seekparquet.Range{Limit: limit}, nil); !errors.Is(err, seekparquet.ErrInvalidRequest) {
				t.Fatalf("range limit %d: %v", limit, err)
			}
			if _, err := ds.List(ctx, seekparquet.List{Delimiter: '/', Limit: limit}, nil); !errors.Is(err, seekparquet.ErrInvalidRequest) {
				t.Fatalf("list limit %d: %v", limit, err)
			}
			if err := ds.Scan(ctx, seekparquet.Range{Limit: limit}, nil, nil); !errors.Is(err, seekparquet.ErrInvalidRequest) {
				t.Fatalf("scan limit %d: %v", limit, err)
			}
		}
	}
}

// TestPrefixRange reads prefixes, including ones without a successor.
func TestPrefixRange(t *testing.T) {
	keys := []string{"a", "a/1", "a/2", "b", "\xff", "\xff\x00", "\xff\xff"}
	d := open(t, write(t, parts(seekparquettest.Part{}, keys)), seekparquet.Options{})
	for prefix, want := range map[string][]string{"a/": {"a/1", "a/2"}, "a": {"a", "a/1", "a/2"}, "\xff": {"\xff", "\xff\x00", "\xff\xff"}, "\xff\xff": {"\xff\xff"}, "": keys, "c": nil} {
		got, err := d.Range(ctx, seekparquet.PrefixRange([]byte(prefix), 100), nil)
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
	dir := write(t, seekparquettest.Dataset{Parts: []seekparquettest.Part{
		{Rows: seekparquettest.Rows("a", "m"), Groups: []int{0, 1, 0, 1, 0}},
		{Rows: seekparquettest.Rows("z")},
	}})
	meta, err := seekparquettest.ReadFooter(dir + "/" + seekparquettest.PartPath(0))
	if err != nil || len(meta.RowGroups) != 5 {
		t.Fatalf("physical row groups: %v", err)
	}
	d := open(t, dir, seekparquet.Options{})
	if d.Reader().RowGroups() != 3 || d.Reader().Rows() != 3 || d.Reader().Parts() != 2 {
		t.Fatalf("%d groups, %d rows", d.Reader().RowGroups(), d.Reader().Rows())
	}
	keys := []string{"a", "m", "z"}
	points := [][]byte{nil, []byte("\x00"), []byte("a"), []byte("a\x00"), []byte("b"), []byte("m"), []byte("m\x00"), []byte("z"), []byte("z\x00")}
	for _, start := range points {
		for _, end := range points {
			for _, exclusive := range []bool{false, true} {
				for _, limit := range []int{1, 2} {
					rng := seekparquet.Range{Start: start, StartExclusive: exclusive, End: end, Limit: limit}
					got, err := d.Range(ctx, rng, nil)
					if err != nil || !slices.Equal(keysOf(got), rangeOracle(keys, rng)) {
						t.Fatalf("%+v: %q, %v", rng, keysOf(got), err)
					}
				}
			}
		}
	}
}

// TestEmptyDataset opens parts: [] and reads nothing.
func TestEmptyDataset(t *testing.T) {
	d := open(t, write(t, seekparquettest.Dataset{}), seekparquet.Options{})
	rows, err := d.Range(ctx, seekparquet.Range{Limit: 10, Columns: []string{"x"}}, nil)
	if err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	entries, err := d.List(ctx, seekparquet.List{Delimiter: '/', Limit: 10}, nil)
	if err != nil || len(entries) != 0 {
		t.Fatal(entries, err)
	}
	if m := d.Metadata(); len(m.Parts) != 0 || m.SchemaID != seekparquettest.SchemaID {
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
	d := open(t, write(t, parts(seekparquettest.Part{PageBytes: 4096, GroupRows: 100, IndexSizeLimit: 64}, split(keys, 2)...)), seekparquet.Options{})
	for i, k := range keys {
		got, err := d.Range(ctx, seekparquet.Range{Start: []byte(k), Limit: 2}, nil)
		if err != nil || len(got) == 0 || string(got[0].Key) != k {
			t.Fatalf("from key %d: %d rows, %v", i, len(got), err)
		}
	}
}
