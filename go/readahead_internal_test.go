// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// readAheadFixture is a dataset directory and its keys in order (with
// repeats when the keys are not unique).
type readAheadFixture struct {
	name   string
	dir    string
	keys   []string
	unique bool
}

// readAheadFixtures returns datasets whose key page bounds make the
// read-ahead's predictions wrong in different ways: bounds shortened to 8
// bytes that overlap from page to page on long shared stems (RG-6), keys
// far longer than their 64-byte bounds, broad bounds out of order (every
// minimum "a", every other maximum "z"), and repeated keys.
func readAheadFixtures(t *testing.T) []readAheadFixture {
	var out []readAheadFixture
	keys := fixtureKeys(3000)
	out = append(out, readAheadFixture{"shortened bounds", write(t, parts(testutil.Part{GroupRows: 400, PageBytes: 512, IndexSizeLimit: 8}, split(keys, 3)...)), keys, true})

	var long []string
	for i := range 300 {
		long = append(long, fmt.Sprintf("%s/%s%04d/f", strings.Repeat("p", 300), strings.Repeat("q", i%3*100), i/4))
	}
	slices.Sort(long)
	long = slices.Compact(long)
	out = append(out, readAheadFixture{"long keys", write(t, parts(testutil.Part{PageBytes: 2048, GroupRows: 100, IndexSizeLimit: 64}, split(long, 2)...)), long, true})

	var dirs []string
	for i := range 640 {
		dirs = append(dirs, fmt.Sprintf("d%02d/f%04d", i/16, i))
	}
	broad := write(t, parts(testutil.Part{GroupRows: 320, PageBytes: 64}, dirs))
	broadNonMonotoneIndexes(t, broad)
	out = append(out, readAheadFixture{"broad non-monotone bounds", broad, dirs, true})

	var repeated []string
	for i := range 900 {
		for range 1 + i%3 {
			repeated = append(repeated, fmt.Sprintf("r%02d/s%d/k%04d", i/30, i%2, i))
		}
	}
	// Parts must not share a key (PART-4): the boundary is moved past a
	// group of equal keys.
	half := len(repeated) / 2
	for half < len(repeated) && repeated[half] == repeated[half-1] {
		half++
	}
	spec := parts(testutil.Part{GroupRows: 300, PageBytes: 128}, repeated[:half], repeated[half:])
	spec.RepeatedKeys = true
	out = append(out, readAheadFixture{"repeated keys", write(t, spec), repeated, false})
	return out
}

// TestListReadAheadSameResults lists every fixture with prefixes, cursors,
// upper bounds and limits, on one Dataset per read-ahead setting, so that
// the caches evolve alike: the entries, the key pages decoded and every
// other count are those of a listing without read-ahead, the entries
// those of the oracle, and the reads and bytes beyond it exactly the pages
// read ahead and not used. Refusals are compared as well.
func TestListReadAheadSameResults(t *testing.T) {
	noTestReadAhead(t)
	var predicted, wasted int64
	for _, fx := range readAheadFixtures(t) {
		t.Run(fx.name, func(t *testing.T) {
			modes := []int{0, 1, 3, 16}
			ds := make([]*Dataset, len(modes))
			for i := range modes {
				ds[i] = open(t, fx.dir, Options{})
			}
			bounds := probes(fx.keys, 37)
			prefixes := [][]byte{nil}
			for _, b := range bounds {
				if i := bytes.IndexByte(b, '/'); i >= 0 && !slices.ContainsFunc(prefixes, func(p []byte) bool { return bytes.Equal(p, b[:i+1]) }) {
					prefixes = append(prefixes, b[:i+1])
				}
			}
			var reqs []List
			for _, prefix := range prefixes {
				for i, b := range bounds {
					var cur *Cursor
					switch i % 3 {
					case 1:
						cur = &Cursor{Key: b}
					case 2:
						cur = &Cursor{Prefix: b}
					}
					if i%5 != 0 && cur != nil {
						continue
					}
					for _, limit := range []int{1, 4, 1000} {
						reqs = append(reqs, List{Prefix: prefix, Delimiter: '/', Cursor: cur, Limit: limit})
					}
					if i%11 == 0 {
						reqs = append(reqs, List{Prefix: prefix, Delimiter: '/', Cursor: cur, End: b, Limit: 1000},
							List{Prefix: prefix, Delimiter: 's', Cursor: cur, Limit: 50})
					}
				}
			}
			for _, req := range reqs {
				var base ReadStats
				want, wantErr := ds[0].r.readList(ctx, req, &base)
				if wantErr == nil && fx.unique {
					if oracle := listOracle(fx.keys, req); !slices.Equal(entriesOf(want), oracle) {
						t.Fatalf("%s: %q, oracle %q", describe(req), entriesOf(want), oracle)
					}
				}
				for i, n := range modes[1:] {
					req := req
					req.ReadAhead = n
					var st ReadStats
					got, err := ds[i+1].r.readList(ctx, req, &st)
					if fmt.Sprint(err) != fmt.Sprint(wantErr) {
						t.Fatalf("%s, ReadAhead %d: %v, without %v", describe(req), n, err, wantErr)
					}
					if !slices.Equal(entriesOf(got), entriesOf(want)) || !slices.EqualFunc(got, want, func(a, b Entry) bool {
						return a.Row == nil && b.Row == nil || a.Row != nil && b.Row != nil && a.Row.Offset == b.Row.Offset
					}) {
						t.Fatalf("%s, ReadAhead %d:\n got %q\nwant %q", describe(req), n, entriesOf(got), entriesOf(want))
					}
					extra := st
					extra.SourceReads -= st.ReadAheadUnused
					extra.SourceBytes -= st.ReadAheadUnusedBytes
					extra.ReadAheadReads, extra.ReadAheadUnused, extra.ReadAheadUnusedBytes = 0, 0, 0
					if extra != base {
						t.Fatalf("%s, ReadAhead %d: counts beyond the unused reads ahead differ:\n got %+v\nwant %+v\n(%+v)", describe(req), n, extra, base, st)
					}
					if st.ReadAheadUnused > st.ReadAheadReads || st.ReadAheadReads > st.SourceReads {
						t.Fatalf("%s, ReadAhead %d: %+v", describe(req), n, st)
					}
					predicted += st.ReadAheadReads
					wasted += st.ReadAheadUnused
				}
			}
		})
	}
	t.Logf("%d pages read ahead, %d of them unused", predicted, wasted)
	if predicted == 0 || wasted == 0 {
		t.Fatalf("the fixtures exercised no read-ahead (%d) or no wrong prediction (%d)", predicted, wasted)
	}
}

// TestListReadAheadRefusals: a key page that breaks the format is refused
// the same whether it was read ahead or not: its bytes are checked as any
// page's (CHECK-2).
func TestListReadAheadRefusals(t *testing.T) {
	noTestReadAhead(t)
	var keys []string
	for i := range 400 {
		keys = append(keys, fmt.Sprintf("d%02d/f%04d", i/8, i))
	}
	for _, flaw := range []struct {
		name   string
		mutate func([]string) []string
	}{
		{"out of order", func(k []string) []string { k[250], k[251] = k[251], k[250]; return k }},
		{"empty key", func(k []string) []string { k[300] = ""; return k }},
	} {
		keys := flaw.mutate(slices.Clone(keys))
		dir := write(t, parts(testutil.Part{PageBytes: 128}, keys))
		var errs []string
		for _, n := range []int{0, 2, 16} {
			d := open(t, dir, Options{})
			_, err := d.List(ctx, List{Delimiter: '/', Limit: 1000, ReadAhead: n})
			if CodeOf(err) == "" {
				t.Fatalf("%s, ReadAhead %d: %v, want a refusal", flaw.name, n, err)
			}
			errs = append(errs, err.Error())
		}
		if errs[0] != errs[1] || errs[0] != errs[2] {
			t.Fatalf("%s: %q", flaw.name, errs)
		}
	}
}

// TestListReadAheadListIndex: a '/' listing the list index serves reads no
// page ahead, and reads what it reads without one; another delimiter,
// which the index declines, reads ahead as usual.
func TestListReadAheadListIndex(t *testing.T) {
	noTestReadAhead(t)
	var keys []string
	for i := range 2000 {
		keys = append(keys, fmt.Sprintf("seg%03d/kind%d/f%04d", i/40, i/10%4, i))
	}
	dir := write(t, parts(testutil.Part{PageBytes: 256}, keys))
	var counts [2]ReadStats
	var entries [2][]string
	for mode, n := range []int{0, 8} {
		d := open(t, dir, Options{})
		if _, err := d.BuildListIndex(ctx, ListIndexOptions{}); err != nil {
			t.Fatal(err)
		}
		got, err := d.List(ctx, List{Delimiter: '/', Limit: 1000, ReadAhead: n, Stats: &counts[mode]})
		must(t, err)
		entries[mode] = entriesOf(got)
		var st ReadStats
		_, err = d.List(ctx, List{Prefix: []byte("seg001/"), Delimiter: 'k', Limit: 1000, ReadAhead: n, Stats: &st})
		must(t, err)
		if n > 0 && st.ReadAheadReads == 0 {
			t.Errorf("a 'k' listing with the index built read nothing ahead: %+v", st)
		}
	}
	if !slices.Equal(entries[0], entries[1]) || len(entries[0]) != 50 {
		t.Fatalf("%d and %d entries", len(entries[0]), len(entries[1]))
	}
	if counts[1] != counts[0] || counts[0].ListIndexSkips == 0 {
		t.Fatalf("with the index: ReadAhead 8 %+v, without %+v", counts[1], counts[0])
	}
}
