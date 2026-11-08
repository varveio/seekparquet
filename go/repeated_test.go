// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// positioned is a source row of a repeated-key dataset with its offset in
// its group, counted over the stored rows before any filtering.
type positioned struct {
	key    []byte
	offset int64
	size   int64 // the row's ordinal within its part, as testutil.Rows writes it
}

func positions(parts [][]string) []positioned {
	var out []positioned
	for _, part := range parts {
		for i, k := range part {
			offset := int64(0)
			if n := len(out); n > 0 && string(out[n-1].key) == k {
				offset = out[n-1].offset + 1
			}
			out = append(out, positioned{key: []byte(k), offset: offset, size: int64(i)})
		}
	}
	return out
}

// rangeExpect answers a range over the rows by the definition, walking
// every row with no routing or page access; entries are spelled
// "key/offset=size".
func rangeExpect(rows []positioned, rng Range) []string {
	var out []string
	from, exclusive, end := rng.bounds()
	for _, r := range rows {
		if len(out) == rng.Limit {
			break
		}
		if from != nil && (bytes.Compare(r.key, from) < 0 || exclusive && bytes.Equal(r.key, from)) ||
			end != nil && bytes.Compare(r.key, end) >= 0 || !afterCursor(rng.Cursor, r.key, r.offset) {
			continue
		}
		out = append(out, fmt.Sprintf("%s/%d=%d", r.key, r.offset, r.size))
	}
	return out
}

// listExpect answers a listing over the rows by the definition; rows are
// spelled "r:key/offset=size" and prefixes "p:prefix".
func listExpect(rows []positioned, req List) []string {
	var out []string
	seen := map[string]bool{}
	for _, r := range rows {
		if len(out) == req.Limit {
			break
		}
		if !bytes.HasPrefix(r.key, req.Prefix) || req.End != nil && bytes.Compare(r.key, req.End) >= 0 || !afterCursor(req.Cursor, r.key, r.offset) {
			continue
		}
		if cp := commonPrefix(r.key, req.Prefix, req.Delimiter); cp != nil {
			if !seen[string(cp)] {
				seen[string(cp)] = true
				out = append(out, "p:"+string(cp))
			}
			continue
		}
		out = append(out, fmt.Sprintf("r:%s/%d=%d", r.key, r.offset, r.size))
	}
	return out
}

func rowsString(rows []Row) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = fmt.Sprintf("%s/%d=%d", r.Key, r.Offset, r.Values[0].Int64())
	}
	return out
}

func listString(entries []Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		if e.IsPrefix() {
			out[i] = "p:" + string(e.CommonPrefix)
		} else {
			out[i] = fmt.Sprintf("r:%s/%d=%d", e.Row.Key, e.Row.Offset, e.Row.Values[0].Int64())
		}
	}
	return out
}

// repeatedParts is two parts of runs of equal keys of random lengths, split
// at a group boundary (a group never crosses parts).
func repeatedParts() [][]string {
	random := rand.New(rand.NewPCG(91, 8))
	var keys []string
	for _, k := range []string{"a", "a/", "a/b", "b", "c", "c/d", "d", "\xff"} {
		for n := random.IntN(45) + 1; n > 0; n-- {
			keys = append(keys, k)
		}
	}
	at := slices.Index(keys, "c")
	return [][]string{keys[:at], keys[at:]}
}

// TestRepeatedQueriesMatchSequentialOracle reads ranges and listings of a
// two-part repeated-key dataset under several page and row-group
// geometries, opened lazily and verified, with a projected column, and
// compares rows, offsets and values with the sequential definition; then
// pages through every result with each page's last cursor.
func TestRepeatedQueriesMatchSequentialOracle(t *testing.T) {
	keyParts := repeatedParts()
	rows := positions(keyParts)
	starts := []string{"", "a", "a/", "a/b", "b", "c", "c/d", "d", "\xff", "\xff\x00"}
	for _, geometry := range []testutil.Part{{GroupRows: 7, PageBytes: 16}, {GroupRows: 19, PageBytes: 32}, {GroupRows: 1}} {
		for _, verify := range []bool{false, true} {
			name := fmt.Sprintf("groups of %d, pages of %d bytes, verified %v", geometry.GroupRows, geometry.PageBytes, verify)
			t.Run(name, func(t *testing.T) {
				doc := parts(geometry, keyParts...)
				doc.RepeatedKeys = true
				d := open(t, write(t, doc), Options{VerifyOnOpen: verify})
				for _, start := range starts {
					for _, exclusive := range []bool{false, true} {
						for _, end := range [][]byte{nil, []byte("b"), []byte("d")} {
							for _, limit := range []int{1, 5, 1000} {
								rng := Range{Start: []byte(start), StartExclusive: exclusive, End: end, Limit: limit, Columns: []string{"size"}}
								got, err := d.Range(ctx, rng)
								if err != nil {
									t.Fatalf("%s: %v", describe(rng), err)
								}
								if want := rangeExpect(rows, rng); !slices.Equal(rowsString(got), want) {
									t.Fatalf("%s:\n got %q\nwant %q", describe(rng), rowsString(got), want)
								}
							}
						}
					}
					for _, delimiter := range []byte{'/', 0xff, 0} {
						for _, cur := range []*Cursor{nil, {Key: []byte(start)}, {Key: []byte(start), Offset: 3}, {Prefix: []byte(start)}} {
							req := List{Cursor: cur, Delimiter: delimiter, Limit: 1000, Columns: []string{"size"}}
							got, err := d.List(ctx, req)
							if err != nil {
								t.Fatalf("%s: %v", describe(req), err)
							}
							if want := listExpect(rows, req); !slices.Equal(listString(got), want) {
								t.Fatalf("%s:\n got %q\nwant %q", describe(req), listString(got), want)
							}
						}
					}
				}
				for _, prefix := range []string{"", "a", "a/", "c", "\xff"} {
					for _, limit := range []int{1, 2, 7, 50} {
						rng := PrefixRange([]byte(prefix), limit, "size")
						var joined []string
						for range len(rows) + 1 {
							page, err := d.Range(ctx, rng)
							if err != nil {
								t.Fatalf("%s: %v", describe(rng), err)
							}
							joined = append(joined, rowsString(page)...)
							if len(page) < limit {
								break
							}
							rng.Cursor = page[len(page)-1].Cursor()
						}
						if want := rangeExpect(rows, PrefixRange([]byte(prefix), math.MaxInt)); !slices.Equal(joined, want) {
							t.Fatalf("paged %s: %d rows, want %d", describe(rng), len(joined), len(want))
						}
						req := List{Prefix: []byte(prefix), Delimiter: '/', Limit: limit, Columns: []string{"size"}}
						var entries []string
						for range len(rows) + 1 {
							page, err := d.List(ctx, req)
							if err != nil {
								t.Fatalf("%s: %v", describe(req), err)
							}
							entries = append(entries, listString(page)...)
							if len(page) < limit {
								break
							}
							req.Cursor = page[len(page)-1].Cursor()
						}
						req.Cursor, req.Limit = nil, math.MaxInt
						if want := listExpect(rows, req); !slices.Equal(entries, want) {
							t.Fatalf("paged %s: %q, want %q", describe(req), entries, want)
						}
					}
				}
			})
		}
	}
}

// TestCursorAtEveryRow resumes after every row of a repeated-key dataset,
// with offsets inside groups and past their ends, across page, row-group
// and part boundaries.
func TestCursorAtEveryRow(t *testing.T) {
	keyParts := repeatedParts()
	rows := positions(keyParts)
	doc := parts(testutil.Part{GroupRows: 11, PageBytes: 24}, keyParts...)
	doc.RepeatedKeys = true
	d := open(t, write(t, doc), Options{})
	for _, r := range rows {
		for _, offset := range []int64{r.offset, r.offset + 100, math.MaxInt64} {
			rng := Range{Cursor: &Cursor{Key: r.key, Offset: offset}, Limit: 4, Columns: []string{"size"}}
			got, err := d.Range(ctx, rng)
			if err != nil {
				t.Fatalf("%s: %v", describe(rng), err)
			}
			if want := rangeExpect(rows, rng); !slices.Equal(rowsString(got), want) {
				t.Fatalf("%s:\n got %q\nwant %q", describe(rng), rowsString(got), want)
			}
		}
	}
}

// TestLargeGroupResumeIsPageBounded pages through one group of 5,000 equal
// keys spanning several row groups, ten rows at a time: every page
// decodes a bounded number of key pages, however deep in the group its
// cursor lies, because a cursor resumes through the OffsetIndex row
// positions instead of counting the group again from its first row.
func TestLargeGroupResumeIsPageBounded(t *testing.T) {
	keys := []string{"a"}
	for range 5000 {
		keys = append(keys, "k")
	}
	keys = append(keys, "z")
	doc := parts(testutil.Part{GroupRows: 1000, PageBytes: 64}, keys)
	doc.RepeatedKeys = true
	d := open(t, write(t, doc), Options{})
	rng := KeyRange([]byte("k"), 10)
	var maxPages, total int64
	seen := 0
	for range 600 {
		var st stats
		page, err := d.r.readRange(ctx, rng, &st)
		if err != nil {
			t.Fatalf("%s: %v", describe(rng), err)
		}
		for i, row := range page {
			if row.Offset != int64(seen+i) {
				t.Fatalf("%s: row %d has offset %d, want %d", describe(rng), i, row.Offset, seen+i)
			}
		}
		seen += len(page)
		maxPages, total = max(maxPages, st.pages), total+st.pages
		if len(page) < rng.Limit {
			break
		}
		rng.Cursor = page[len(page)-1].Cursor()
	}
	if seen != 5000 {
		t.Fatalf("paged %d rows of the group, want 5000", seen)
	}
	// Each request decodes the group's first page, the page after the
	// cursor and the pages its ten rows span, and probes row groups' first
	// pages on the way; counting the group again would decode every page
	// before the cursor.
	if maxPages > 8 {
		t.Fatalf("a page of ten rows decoded up to %d pages (%d in all)", maxPages, total)
	}
	t.Logf("500 pages of ten rows decoded at most %d and %d in all", maxPages, total)
}

// TestNamedRepeatedKeyAndLookup reads a dotted key column with equal keys:
// a lookup returns the whole group in file order.
func TestNamedRepeatedKeyAndLookup(t *testing.T) {
	type namedRow struct {
		ID    []byte `parquet:"object.id"`
		Value int64  `parquet:"value"`
	}
	d := open(t, write(t, testutil.Dataset{KeyColumn: "object.id", RepeatedKeys: true,
		Parts: []testutil.Part{{Rows: []namedRow{{[]byte("a"), 9}, {[]byte("a"), 8}, {[]byte("a\x00"), 7}}, GroupRows: 1}}}), Options{VerifyOnOpen: true})
	if got := d.Metadata().Key; got != (Key{Column: "object.id", Unique: false}) {
		t.Fatal(got)
	}
	rng := KeyRange([]byte("a"), 10, "object.id", "value", "key")
	rows, err := d.Range(ctx, rng)
	if err != nil {
		t.Fatalf("%s: %v", describe(rng), err)
	}
	if len(rows) != 2 || rows[1].Offset != 1 || rows[1].Values[1].Int64() != 8 || !rows[0].Values[2].Absent {
		t.Fatalf("%s: %+v", describe(rng), rows)
	}
}

// TestNamedKeyAlongsideOrdinaryKeyColumn keeps an ordinary column named
// key beside the declared key column: the declared one orders the rows.
func TestNamedKeyAlongsideOrdinaryKeyColumn(t *testing.T) {
	type record struct {
		Payload string `parquet:"key"`
		ID      []byte `parquet:"object.id"`
	}
	d := open(t, write(t, testutil.Dataset{KeyColumn: "object.id", RepeatedKeys: true,
		Parts: []testutil.Part{{Rows: []record{{"z", []byte("a")}, {"y", []byte("a")}, {"x", []byte("b")}}, GroupRows: 1}}}), Options{VerifyOnOpen: true})
	rows, err := d.Range(ctx, KeyRange([]byte("a"), 10, "key", "object.id"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[1].Offset != 1 || string(rows[0].Values[0].ByteArray()) != "z" || string(rows[1].Values[0].ByteArray()) != "y" {
		t.Fatalf("wrong key column or file order: %+v", rows)
	}
}

// TestOpenPartsKey opens parts without a manifest under a declared key:
// the named column orders and routes the rows, and equal keys are allowed
// only when the key is declared repeated.
func TestOpenPartsKey(t *testing.T) {
	type record struct {
		ID   []byte `parquet:"id"`
		Size int64  `parquet:"size"`
	}
	dir := write(t, testutil.Dataset{KeyColumn: "id", RepeatedKeys: true, Parts: []testutil.Part{
		{Rows: []record{{[]byte("a"), 0}, {[]byte("b"), 1}, {[]byte("b"), 2}}, GroupRows: 2},
	}})
	r, err := openPartsOf(t, dir, 1, Key{Column: "id"}, false, Options{})
	must(t, err)
	rows, err := r.readRange(ctx, Range{Cursor: &Cursor{Key: []byte("b")}, Limit: 10, Columns: []string{"size"}}, nil)
	if err != nil || len(rows) != 1 || rows[0].Offset != 1 || rows[0].Values[0].Int64() != 2 {
		t.Fatalf("%+v, %v", rows, err)
	}
	r, err = openPartsOf(t, dir, 1, Key{Column: "id", Unique: true}, false, Options{})
	must(t, err)
	if _, err := r.readRange(ctx, Range{Limit: 10}, nil); CodeOf(err) != CodeKeyOrder {
		t.Fatalf("equal keys declared unique: %v", err)
	}
	if _, err := openPartsOf(t, dir, 1, Key{Column: "key", Unique: true}, false, Options{}); CodeOf(err) != CodeInvalidKeyColumn {
		t.Fatalf("a key column the parts lack: %v", err)
	}
}

// TestRepeatedRefusals: in repeated mode a decreasing key inside or between
// groups is key_order, and a group may not cross parts (PART-4), whether
// parts carry bounds or not.
func TestRepeatedRefusals(t *testing.T) {
	repeated := func(geometry testutil.Part, keys ...[]string) string {
		doc := parts(geometry, keys...)
		doc.RepeatedKeys = true
		return write(t, doc)
	}
	t.Run("decrease inside a page", func(t *testing.T) {
		d := open(t, repeated(testutil.Part{}, []string{"a", "b", "b", "a", "c"}), Options{})
		if _, err := d.Range(ctx, Range{Limit: 10}); CodeOf(err) != CodeKeyOrder {
			t.Fatalf("got %v, want key_order", err)
		}
	})
	t.Run("decrease between row groups", func(t *testing.T) {
		d := open(t, repeated(testutil.Part{GroupRows: 2}, []string{"a", "b", "b", "a", "c", "c"}), Options{})
		if _, err := d.Range(ctx, Range{Limit: 10}); CodeOf(err) != CodeKeyOrder {
			t.Fatalf("got %v, want key_order", err)
		}
	})
	// The group b spans both parts.
	split := repeated(testutil.Part{}, []string{"a", "b"}, []string{"b", "c"})
	t.Run("group across parts, manifest bounds", func(t *testing.T) {
		if err := openErr(split, Options{}); CodeOf(err) != CodeInvalidManifest {
			t.Fatalf("got %v, want invalid_manifest", err)
		}
	})
	t.Run("group across parts, part bounds", func(t *testing.T) {
		var inputs []partInput
		for i, b := range [][2]string{{"a", "b"}, {"b", "c"}} {
			raw, err := os.ReadFile(partFile(split, i))
			must(t, err)
			inputs = append(inputs, partInput{name: testutil.PartPath(i), file: bytes.NewReader(raw), size: int64(len(raw)),
				minKey: []byte(b[0]), maxKey: []byte(b[1])})
		}
		if _, err := openParts(ctx, inputs, Key{Column: "key"}, Options{}); CodeOf(err) != CodeBoundsMismatch {
			t.Fatalf("got %v, want bounds_mismatch", err)
		}
	})
	t.Run("group across parts, no bounds", func(t *testing.T) {
		r, err := openPartsOf(t, split, 2, Key{Column: "key"}, false, Options{})
		must(t, err)
		if _, err := r.readRange(ctx, Range{Limit: 10}, nil); CodeOf(err) != CodeKeyOrder {
			t.Fatalf("range: got %v, want key_order", err)
		}
		if _, err := openPartsOf(t, split, 2, Key{Column: "key"}, false, Options{VerifyOnOpen: true}); CodeOf(err) != CodeKeyOrder {
			t.Fatalf("verified open: got %v, want key_order", err)
		}
	})
}

// FuzzRepeatedPagination pages through groups of equal keys of fuzzed
// sizes, row groups and limits with each page's last cursor.
func FuzzRepeatedPagination(f *testing.F) {
	f.Add(uint8(24), uint8(4), uint8(3))
	f.Add(uint8(1), uint8(1), uint8(1))
	f.Add(uint8(65), uint8(7), uint8(13))
	f.Fuzz(func(t *testing.T, count, group, limit uint8) {
		n, geometry, size := int(count%70)+1, int(group%19)+1, int(limit%15)+1
		keys := make([]string, n)
		for i := range keys {
			keys[i] = fmt.Sprintf("k%d", i/(n/3+1))
		}
		doc := parts(testutil.Part{GroupRows: geometry, PageBytes: 16}, keys)
		doc.RepeatedKeys = true
		d := open(t, write(t, doc), Options{})
		rng := Range{Limit: size, Columns: []string{"size"}}
		var got []string
		for range n + 1 {
			page, err := d.Range(ctx, rng)
			if err != nil {
				t.Fatalf("%s: %v", describe(rng), err)
			}
			got = append(got, rowsString(page)...)
			if len(page) < size {
				break
			}
			rng.Cursor = page[len(page)-1].Cursor()
		}
		if want := rangeExpect(positions([][]string{keys}), Range{Limit: n}); !slices.Equal(got, want) {
			t.Fatalf("paged %q,\nwant %q", strings.Join(got, " "), strings.Join(want, " "))
		}
	})
}
