// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/seekparquettest"
)

// repeatedOracle enumerates source rows, counts offsets before filtering,
// and performs no routing or page access. Values identify the physical row.
func repeatedOracle(keys []string, rng seekparquet.Range, list *seekparquet.List) []seekparquet.Entry {
	var out []seekparquet.Entry
	seen := map[string]bool{}
	var previous string
	offset := int64(0)
	for i, k := range keys {
		if i > 0 && k == previous {
			offset++
		} else {
			offset = 0
		}
		previous = k
		key := []byte(k)
		prefix, end, limit, cursor := rng.Prefix, rng.End, rng.Limit, rng.Cursor
		if list != nil {
			prefix, end, limit, cursor = list.Prefix, list.End, list.Limit, list.Cursor
		}
		if !bytes.HasPrefix(key, prefix) || end != nil && bytes.Compare(key, end) >= 0 {
			continue
		}
		if list == nil && rng.Start != nil && (bytes.Compare(key, rng.Start) < 0 || rng.StartExclusive && bytes.Equal(key, rng.Start)) {
			continue
		}
		if list != nil && list.After != nil && bytes.Compare(key, list.After) <= 0 {
			continue
		}
		if cursor != nil {
			floor := cursor.Key
			if cursor.Prefix != nil {
				floor = cursor.Prefix
			}
			cmp := bytes.Compare(key, floor)
			if cmp < 0 || cmp == 0 && (cursor.Prefix != nil || offset <= cursor.Offset) {
				continue
			}
		}
		if list != nil {
			if at := bytes.IndexByte(key[len(prefix):], list.Delimiter); at >= 0 {
				cp := k[:len(prefix)+at+1]
				if seen[cp] || list.Continuation && list.After != nil && bytes.Compare([]byte(cp), list.After) <= 0 || cursor != nil && cursor.Prefix != nil && bytes.Compare([]byte(cp), cursor.Prefix) <= 0 {
					continue
				}
				seen[cp] = true
				out = append(out, seekparquet.Entry{CommonPrefix: []byte(cp)})
			} else {
				out = append(out, seekparquet.Entry{Row: &seekparquet.Row{Key: key, Offset: offset}})
			}
		} else {
			out = append(out, seekparquet.Entry{Row: &seekparquet.Row{Key: key, Offset: offset}})
		}
		if len(out) == limit {
			break
		}
	}
	return out
}

func compareRepeated(t testing.TB, got, want []seekparquet.Entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i, e := range got {
		w := want[i]
		if e.IsPrefix() != w.IsPrefix() || !bytes.Equal(e.Bytes(), w.Bytes()) || e.Row != nil && e.Row.Offset != w.Row.Offset {
			t.Fatalf("entry %d: got %+v, want %+v", i, e, w)
		}
	}
}

func TestRepeatedQueriesMatchSequentialOracle(t *testing.T) {
	random := rand.New(rand.NewPCG(91, 8))
	var keys []string
	for _, k := range []string{"a", "a/", "a/b", "b", "c", "c/d", "d", "\xff"} {
		for n := random.IntN(45) + 1; n > 0; n-- {
			keys = append(keys, k)
		}
	}
	for _, geometry := range []seekparquettest.Part{{GroupRows: 7, PageBytes: 16}, {GroupRows: 19, PageBytes: 32}, {GroupRows: 1}} {
		doc := parts(geometry, keys)
		doc.RepeatedKeys = true
		d := open(t, write(t, doc), seekparquet.Options{VerifyOnOpen: true})
		for _, start := range []string{"", "a", "a/", "a/b", "b", "c", "c/d", "d", "\xff", "\xff\x00"} {
			for _, exclusive := range []bool{false, true} {
				for _, end := range [][]byte{nil, []byte("b"), []byte("d")} {
					for _, limit := range []int{1, 5, 1000} {
						rng := seekparquet.Range{Start: []byte(start), StartExclusive: exclusive, End: end, Limit: limit}
						rows, err := d.Range(ctx, rng, nil)
						if err != nil {
							t.Fatal(err)
						}
						got := make([]seekparquet.Entry, len(rows))
						for i := range rows {
							got[i].Row = &rows[i]
						}
						compareRepeated(t, got, repeatedOracle(keys, rng, nil))
					}
				}
			}
			for _, continuation := range []bool{false, true} {
				for _, delimiter := range []byte{'/', 0xff, 0} {
					req := seekparquet.List{After: []byte(start), Continuation: continuation, Delimiter: delimiter, Limit: 1000}
					got, err := d.List(ctx, req, nil)
					if err != nil {
						t.Fatal(err)
					}
					compareRepeated(t, got, repeatedOracle(keys, seekparquet.Range{}, &req))
				}
			}
		}
		for _, prefix := range []string{"", "a", "a/", "c", "\xff"} {
			for _, limit := range []int{1, 2, 7, 50} {
				rng := seekparquet.PrefixRange([]byte(prefix), limit, "size")
				var joined []seekparquet.Row
				for {
					page, err := d.Range(ctx, rng, nil)
					if err != nil {
						t.Fatal(err)
					}
					joined = append(joined, page...)
					if len(page) < limit {
						break
					}
					rng.Cursor = page[len(page)-1].Cursor()
					if len(joined) > len(keys) {
						t.Fatal("range pagination failed to advance")
					}
				}
				full, err := d.Range(ctx, seekparquet.PrefixRange([]byte(prefix), 1000, "size"), nil)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(joined, full) {
					t.Fatal("range pagination changed rows or values")
				}
				req := seekparquet.List{Prefix: []byte(prefix), Delimiter: '/', Limit: limit, Columns: []string{"size"}}
				var entries []seekparquet.Entry
				for {
					page, err := d.List(ctx, req, nil)
					if err != nil {
						t.Fatal(err)
					}
					entries = append(entries, page...)
					if len(page) < limit {
						break
					}
					req.Cursor = page[len(page)-1].Cursor()
					if len(entries) > len(keys) {
						t.Fatal("listing pagination failed to advance")
					}
				}
				req.Cursor, req.Limit = nil, 1000
				fullEntries, err := d.List(ctx, req, nil)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(entries, fullEntries) {
					t.Fatal("listing pagination changed rows or values")
				}
			}
		}
	}
}

func TestNamedRepeatedKeyAndLookup(t *testing.T) {
	type namedRow struct {
		ID    []byte `parquet:"object.id"`
		Value int64  `parquet:"value"`
	}
	d := open(t, write(t, seekparquettest.Dataset{KeyColumn: "object.id", RepeatedKeys: true,
		Parts: []seekparquettest.Part{{Rows: []namedRow{{[]byte("a"), 9}, {[]byte("a"), 8}, {[]byte("a\x00"), 7}}, GroupRows: 1}}}), seekparquet.Options{VerifyOnOpen: true})
	if got := d.Metadata().Key; got != (seekparquet.Key{Column: "object.id", Unique: false}) {
		t.Fatal(got)
	}
	rows, err := d.Range(ctx, seekparquet.KeyRange([]byte("a"), 10, "object.id", "value", "key"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[1].Offset != 1 || rows[1].Values[1].Int64() != 8 || !rows[0].Values[2].Absent {
		t.Fatalf("%+v", rows)
	}
	for _, cursor := range []*seekparquet.Cursor{{}, {Key: []byte("a"), Offset: -1}, {Key: []byte("a"), Prefix: []byte("a/")}, {Prefix: []byte("a/"), Offset: 1}} {
		if _, err := d.Range(ctx, seekparquet.Range{Cursor: cursor, Limit: 1}, nil); !errors.Is(err, seekparquet.ErrInvalidRequest) {
			t.Fatal(err)
		}
		if _, err := d.List(ctx, seekparquet.List{Cursor: cursor, Limit: 1}, nil); !errors.Is(err, seekparquet.ErrInvalidRequest) {
			t.Fatal(err)
		}
	}
}

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
		doc := parts(seekparquettest.Part{GroupRows: geometry, PageBytes: 16}, keys)
		doc.RepeatedKeys = true
		d := open(t, write(t, doc), seekparquet.Options{})
		req := seekparquet.Range{Limit: size}
		var got []seekparquet.Entry
		for {
			page, err := d.Range(ctx, req, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := range page {
				got = append(got, seekparquet.Entry{Row: &page[i]})
			}
			if len(page) < size {
				break
			}
			req.Cursor = page[len(page)-1].Cursor()
			if len(got) > n {
				t.Fatal("cursor did not advance")
			}
		}
		compareRepeated(t, got, repeatedOracle(keys, seekparquet.Range{Limit: n + 1}, nil))
	})
}
