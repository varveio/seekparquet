// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// statsFixture writes keys as one part of 100-row row groups, and opens it
// through a counting Source, with its part checked (neither is counted).
// The group index is checked against the geometry the expected counts are
// derived from.
func statsFixture(t *testing.T, keys []string, pageRows int) (*Dataset, *accessSource) {
	t.Helper()
	src := &accessSource{Source: dirOf(t, write(t, parts(testutil.Part{GroupRows: 100, PageBytes: 200}, keys)))}
	d, err := OpenDataset(ctx, src, Options{})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	probe := new(ReadStats)
	must(t, d.r.checkParts(ctx, probe))
	for _, grp := range d.r.parts[0].groups {
		g := grp.physical
		index, err := d.r.parts[0].keyIndex(ctx, grp.physical, probe)
		must(t, err)
		for i, page := range index.pages {
			if page.first != int64(i*pageRows) {
				t.Fatalf("group %d page %d starts at row %d, want %d: the derivations below assume %d-row key pages", g, i, page.first, i*pageRows, pageRows)
			}
		}
	}
	// Forget what the geometry check loaded.
	d, err = OpenDataset(ctx, src, Options{})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	must(t, d.r.checkParts(ctx, probe))
	src.reset()
	return d, src
}

// dirKeys returns n keys "<dir>/f000"... for each dir.
func dirKeys(dirs map[string]int, order ...string) []string {
	var keys []string
	for _, dir := range order {
		for i := range dirs[dir] {
			keys = append(keys, fmt.Sprintf("%s/f%03d", dir, i))
		}
	}
	return keys
}

// checkStats compares st with want and with what the counting Source saw.
func checkStats(t *testing.T, name string, st, want ReadStats, src *accessSource) {
	t.Helper()
	if src.reads.Load() != st.SourceReads || src.bytes.Load() != st.SourceBytes {
		t.Errorf("%s: Source saw %d reads, %d bytes; stats say %d, %d", name, src.reads.Load(), src.bytes.Load(), st.SourceReads, st.SourceBytes)
	}
	// Bytes follow from the encoding, not from the geometry.
	want.SourceBytes = st.SourceBytes
	if st != want {
		t.Errorf("%s:\n got %+v\nwant %+v", name, st, want)
	}
	src.reset()
}

// Eight directories of 50 keys "d0/f000"... in 800 rows: four row groups of
// 100 rows (group g holds directories 2g and 2g+1), each with key pages of
// 18 rows, the last of 10 (rows 0, 18, 36, 54, 72 and 90).
func TestReadStatsLookupAndRange(t *testing.T) {
	order := []string{"d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7"}
	sizes := map[string]int{}
	for _, dir := range order {
		sizes[dir] = 50
	}
	keys := dirKeys(sizes, order...)

	// A cold point lookup of d3/f010 (row 60 of group 1, page 3). Routing
	// searches the four groups' first keys by binary search: group 2
	// ("d4/f000" is above the key), then group 1 ("d2/f000" is not), each
	// loading its index (a ColumnIndex and an OffsetIndex read) and decoding
	// its page 0. The seek then opens group 1 again (a reuse) and goes to
	// page 3, the first whose maximum ("d3/f021") reaches the key; as the
	// walker has decoded nothing yet, that is not a skip. The one value is in
	// the "size" column's one page, read through that column's OffsetIndex.
	// Pages: 2 probes + 1 = 3 key pages of 18 keys; reads: 2 loads x 2 + 3
	// key pages + 1 OffsetIndex + 1 value page.
	d, src := statsFixture(t, keys, 18)
	var st ReadStats
	rows, err := d.Range(ctx, withStats(KeyRange([]byte("d3/f010"), 5, "size"), &st))
	must(t, err)
	if len(rows) != 1 || string(rows[0].Key) != "d3/f010" || rows[0].Values[0].Int64() != 160 {
		t.Fatalf("lookup: %+v", rows)
	}
	checkStats(t, "point lookup", st, ReadStats{KeyPagesDecoded: 3, ValuePagesDecoded: 1, KeysDecoded: 54,
		RowGroupsOpened: 3, IndexLoads: 2, SourceReads: 9}, src)

	// The same lookup warm: the first keys and both indexes are kept, so the
	// routing probes decode nothing; only the seek's page 3, and the value.
	st = ReadStats{}
	_, err = d.Range(ctx, withStats(KeyRange([]byte("d3/f010"), 5, "size"), &st))
	must(t, err)
	checkStats(t, "warm point lookup", st, ReadStats{KeyPagesDecoded: 1, ValuePagesDecoded: 1, KeysDecoded: 18,
		RowGroupsOpened: 1, SourceReads: 3}, src)

	// A cold short range, d3/f015 up to d3/f030 (rows 65 to 79 of group 1):
	// the same routing, then key pages 3 (rows 54 to 71) and 4 (72 to 89),
	// where the next key, d3/f030, is at the end and stops the scan.
	d, src = statsFixture(t, keys, 18)
	st = ReadStats{}
	rows, err = d.Range(ctx, Range{Start: []byte("d3/f015"), End: []byte("d3/f030"), Limit: 100, Stats: &st})
	must(t, err)
	if len(rows) != 15 {
		t.Fatalf("range: %d rows", len(rows))
	}
	checkStats(t, "short range", st, ReadStats{KeyPagesDecoded: 4, KeysDecoded: 72, RowGroupsOpened: 3, IndexLoads: 2, SourceReads: 8}, src)
}

// withStats returns rng with its Stats set.
func withStats(rng Range, st *ReadStats) Range {
	rng.Stats = st
	return rng
}

// Directories a (350 keys "a/f000"...), b (50), c (100) and d (100): 600
// rows in six row groups of 100 rows, each with key pages of 20 rows. Group
// 0 to 2 hold only a; group 3 holds a/f300 to a/f349 then b/; group 4 holds
// c and group 5 d.
func listFixture(t *testing.T) (*Dataset, *accessSource) {
	keys := dirKeys(map[string]int{"a": 350, "b": 50, "c": 100, "d": 100}, "a", "b", "c", "d")
	return statsFixture(t, keys, 20)
}

func TestReadStatsDelimitedListing(t *testing.T) {
	d, src := listFixture(t)
	list := func(st *ReadStats, limit int, cursor *Cursor) []Entry {
		t.Helper()
		*st = ReadStats{}
		entries, err := d.List(ctx, List{Delimiter: '/', Limit: limit, Cursor: cursor, Stats: st})
		must(t, err)
		return entries
	}
	var st ReadStats
	// Cold. The listing opens group 0 and decodes its page 0 (20 keys): "a/",
	// resumed at its successor "a0". Routing for "a0" searches the first keys
	// of groups 3, 5 and 4, each loaded and decoded cold (3 pages); it lands
	// in group 3, where page 2 (rows 40 to 59, "a/f340" to "b/f009") is the
	// first whose maximum reaches "a0": "b/", resumed at "b0". No page of
	// group 3 holds a key at or after "b0", a move that skips pages 3 and 4
	// (seek 1) into group 4, whose page 0 gives "c/", resumed at "c0": the
	// rest of group 4 is skipped (seek 2), group 5's page 0 gives "d/", and
	// "d0" is above every part's maximum key. Pages: 1+3+1+1+1 = 7 of 20 keys;
	// groups opened: 0, 3, 5, 4 (4 loads), then 3 again (routing ended in
	// it), 4 and 5 again (reuses), 7 in all; four resumes at a successor;
	// reads: 4 loads x 2 + 7 pages.
	entries := list(&st, 100, nil)
	if len(entries) != 4 {
		t.Fatalf("%d entries", len(entries))
	}
	checkStats(t, "cold listing", st, ReadStats{KeyPagesDecoded: 7, KeysDecoded: 140, RowGroupsOpened: 7, IndexLoads: 4,
		PageIndexSeeks: 2, PrefixSkips: 4, SourceReads: 15}, src)

	// The same listing warm: groups 0, 3, 4 and 5 are known (first keys
	// decoded, indexes loaded), group 1 is not. From the start, group 0's
	// first key and group 1's share "a/", so "a/" is emitted without reading
	// group 0 (GroupsSkipped 1), at the price of loading group 1's index and
	// decoding its page 0 for its first key. Routing for "a0" then lands in
	// group 3 from the cached first keys, and the walk is the cold one:
	// pages of groups 1, 3, 4 and 5 (4 of 20 keys), reuses of 3, 4 and 5
	// after the one load, two skips, four resumes (the last at "d0", above
	// every key), reads: 1 load x 2 + 4 pages.
	entries = list(&st, 100, nil)
	if len(entries) != 4 {
		t.Fatalf("%d entries", len(entries))
	}
	checkStats(t, "warm listing", st, ReadStats{KeyPagesDecoded: 4, KeysDecoded: 80, RowGroupsOpened: 4, IndexLoads: 1,
		PageIndexSeeks: 2, PrefixSkips: 4, GroupsSkipped: 1, SourceReads: 6}, src)

	// A page filled by a group shortcut does not resume. Warm as above,
	// Limit 1 emits "a/" for groups 0 and 1 sharing it, and stops: the page
	// is full, so no resume at "a0" happened (PrefixSkips 0).
	entries = list(&st, 1, nil)
	if len(entries) != 1 || string(entries[0].CommonPrefix) != "a/" || st.GroupsSkipped != 1 || st.PrefixSkips != 0 {
		t.Fatalf("limit 1: %q %+v", entriesOf(entries), st)
	}
	src.reset()

	// A paginated listing resumed by cursor. Page 1, limit 2, is the cold
	// listing up to "b/": 5 pages, 5 openings (groups 0, 3, 5, 4, 3) and 4
	// loads, one resume (none after the entry that fills the page), reads:
	// 4 loads x 2 + 5 pages.
	d, src = listFixture(t)
	entries = list(&st, 2, nil)
	checkStats(t, "page 1", st, ReadStats{KeyPagesDecoded: 5, KeysDecoded: 100, RowGroupsOpened: 5, IndexLoads: 4,
		PrefixSkips: 1, SourceReads: 13}, src)
	if len(entries) != 2 || string(entries[1].CommonPrefix) != "b/" {
		t.Fatalf("page 1: %q", entriesOf(entries))
	}
	// Page 2 continues after the prefix cursor "b/", at "b0": all routing is
	// warm, so it lands in group 3 and finds no page holding a key at or
	// after "b0" (not a skip: nothing was decoded in the group yet), opens
	// group 4, whose page 0 gives "c/", resumed at "c0": its pages are
	// skipped (seek 1) into group 5's page 0, "d/", which fills the page.
	entries = list(&st, 2, entries[1].Cursor())
	checkStats(t, "page 2", st, ReadStats{KeyPagesDecoded: 2, KeysDecoded: 40, RowGroupsOpened: 3,
		PageIndexSeeks: 1, PrefixSkips: 1, SourceReads: 2}, src)
	if len(entries) != 2 || string(entries[0].CommonPrefix) != "c/" || string(entries[1].CommonPrefix) != "d/" {
		t.Fatalf("page 2: %q", entriesOf(entries))
	}
}

// A request that fails still fills Stats with what it did up to the failure.
func TestReadStatsFailingRequest(t *testing.T) {
	keys := dirKeys(map[string]int{"k": 100}, "k")
	keys[60], keys[61] = keys[61], keys[60] // inside key page 3 of 20 rows
	d, src := statsFixture(t, keys, 20)
	// From the start of the one group: no routing, its index loads (2 reads),
	// then pages 0, 1, 2 and 3 are decoded in turn, and page 3 breaks the key
	// order. The page counts when decoded, before it is checked.
	var st ReadStats
	_, err := d.Range(ctx, Range{Limit: 1000, Stats: &st})
	if CodeOf(err) != CodeKeyOrder {
		t.Fatalf("got %v, want key_order", err)
	}
	checkStats(t, "refused range", st, ReadStats{KeyPagesDecoded: 4, KeysDecoded: 80, RowGroupsOpened: 1, IndexLoads: 1, SourceReads: 6}, src)

	// A request refused before it reads anything leaves Stats zero, whatever
	// it held.
	st = ReadStats{KeyPagesDecoded: 9, SourceBytes: 9}
	_, err = d.Range(ctx, Range{Limit: 0, Stats: &st})
	if err == nil || st != (ReadStats{}) {
		t.Fatalf("invalid request: %v, %+v", err, st)
	}
}

// Requests with their own Stats do not interfere, and a nil Stats changes
// nothing.
func TestReadStatsConcurrentAndNil(t *testing.T) {
	d, _ := listFixture(t)
	type request struct {
		rng  *Range
		list *List
	}
	requests := []request{
		{rng: &Range{Start: []byte("a/f150"), Limit: 40, Columns: []string{"size", "name"}}},
		{rng: &Range{Prefix: []byte("c/"), Limit: 500}},
		{rng: &Range{Cursor: &Cursor{Key: []byte("b/f010")}, Limit: 7, Columns: []string{"size"}}},
		{list: &List{Delimiter: '/', Limit: 100}},
		{list: &List{Prefix: []byte("a/f2"), Delimiter: '/', Limit: 100}},
	}
	run := func(r request, st *ReadStats) (any, error) {
		if r.rng != nil {
			rng := *r.rng
			rng.Stats = st
			return d.Range(ctx, rng)
		}
		list := *r.list
		list.Stats = st
		return d.List(ctx, list)
	}
	// Warm every request until its counts settle, then record them.
	want := make([]ReadStats, len(requests))
	results := make([]any, len(requests))
	for round := range 3 {
		for i, r := range requests {
			got, err := run(r, &want[i])
			must(t, err)
			if round == 0 {
				plain, err := run(r, nil)
				must(t, err)
				if !reflect.DeepEqual(plain, got) {
					t.Fatalf("request %d: results differ with and without Stats", i)
				}
			}
			results[i] = got
		}
	}
	var wg sync.WaitGroup
	for i, r := range requests {
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 50 {
					var st ReadStats
					got, err := run(r, &st)
					if err != nil {
						t.Error(err)
						return
					}
					if st != want[i] || !reflect.DeepEqual(got, results[i]) {
						t.Errorf("request %d concurrent: %+v, want %+v", i, st, want[i])
						return
					}
				}
			}()
		}
	}
	wg.Wait()
}

// A page whose values do not decode is not a decoded page.
func TestReadStatsFailedDecodeNotCounted(t *testing.T) {
	c := testChunk(parquet.Int64Type)
	data := binary.AppendUvarint(binary.AppendUvarint(binary.AppendUvarint(nil, 128), 4), 1<<31-1)
	h := &format.PageHeader{Type: format.DataPage, UncompressedPageSize: int32(len(data)) + 1}
	h.DataPageHeader.V = format.DataPageHeader{NumValues: 10, Encoding: format.DeltaBinaryPacked}
	h.DataPageHeader.Valid = true
	if _, err := c.decode(h, append(data, 0)); CodeOf(err) != CodeInvalidParquet {
		t.Fatalf("got %v", err)
	}
	if *c.pages != 0 {
		t.Fatalf("counted %d decoded pages for a page that failed to decode", *c.pages)
	}
}

// An index refused by Limits.IndexMemory is neither loaded nor opened, so
// RowGroupsOpened-IndexLoads, the reuses, is never negative.
func TestReadStatsIndexRefused(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 100, PageBytes: 200}, dirKeys(map[string]int{"k": 100}, "k")))
	d := open(t, dir, Options{Limits: Limits{IndexMemory: 1}})
	must(t, d.r.checkParts(ctx, new(ReadStats)))
	var st ReadStats
	_, err := d.Range(ctx, Range{Limit: 5, Stats: &st})
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("got %v", err)
	}
	if st.RowGroupsOpened != 0 || st.IndexLoads != 0 || st.KeyPagesDecoded != 0 || st.SourceReads != 2 {
		t.Fatalf("refused index: %+v", st)
	}
}
