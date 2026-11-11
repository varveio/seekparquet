// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

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
				ds[i] = open(t, fx.dir, OpenOptions{})
			}
			bounds := probes(fx.keys, 37)
			prefixes := [][]byte{nil}
			for _, b := range bounds {
				if i := bytes.IndexByte(b, '/'); i >= 0 && !slices.ContainsFunc(prefixes, func(p []byte) bool { return bytes.Equal(p, b[:i+1]) }) {
					prefixes = append(prefixes, b[:i+1])
				}
			}
			var reqs []ListQuery
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
						reqs = append(reqs, ListQuery{Prefix: prefix, Delimiter: '/', Cursor: cur, Limit: limit})
					}
					if i%11 == 0 {
						reqs = append(reqs, ListQuery{Prefix: prefix, Delimiter: '/', Cursor: cur, End: b, Limit: 1000},
							ListQuery{Prefix: prefix, Delimiter: 's', Cursor: cur, Limit: 50})
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
					if !slices.Equal(entriesOf(got), entriesOf(want)) || !slices.EqualFunc(got, want, func(a, b ListEntry) bool {
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
			d := open(t, dir, OpenOptions{})
			_, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 1000, ReadAhead: n}, nil)
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
		d := open(t, dir, OpenOptions{})
		if _, err := d.BuildListIndex(ctx, ListIndexOptions{}); err != nil {
			t.Fatal(err)
		}
		got, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 1000, ReadAhead: n}, &counts[mode])
		must(t, err)
		entries[mode] = entriesOf(got)
		var st ReadStats
		_, err = d.List(ctx, ListQuery{Prefix: []byte("seg001/"), Delimiter: 'k', Limit: 1000, ReadAhead: n}, &st)
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

// TestListReadAheadManyPages: a listing over one row group of about 30,000
// key pages (60,000 keys, 16-byte pages) costs no more with read-ahead than
// without: the read-ahead's bookkeeping does not grow with the pages it
// has been through. The entries, reads and bytes are identical in both
// modes (the pages here are all used: every key is its own entry).
func TestListReadAheadManyPages(t *testing.T) {
	noTestReadAhead(t)
	keys := make([]string, 60000)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%08d", i)
	}
	dir := write(t, parts(testutil.Part{PageBytes: 16}, keys))
	var took [2]time.Duration
	var stats [2]ReadStats
	var entries [2][]string
	for mode, n := range []int{0, 64} {
		d := open(t, dir, OpenOptions{})
		// Warm the Dataset's caches (footer, indexes) so the comparison is the scan's.
		_, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 1, ReadAhead: n}, nil)
		must(t, err)
		start := time.Now()
		got, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 100000, ReadAhead: n}, &stats[mode])
		took[mode] = time.Since(start)
		must(t, err)
		entries[mode] = entriesOf(got)
	}
	t.Logf("ReadAhead 0: %v, ReadAhead 64: %v; %+v", took[0], took[1], stats[1])
	if len(entries[0]) != len(keys) || !slices.Equal(entries[0], entries[1]) {
		t.Fatalf("%d and %d entries, want %d alike", len(entries[0]), len(entries[1]), len(keys))
	}
	if stats[1].ReadAheadReads == 0 {
		t.Fatalf("nothing was read ahead: %+v", stats[1])
	}
	extra := stats[1]
	extra.SourceReads -= extra.ReadAheadUnused
	extra.SourceBytes -= extra.ReadAheadUnusedBytes
	extra.ReadAheadReads, extra.ReadAheadUnused, extra.ReadAheadUnusedBytes = 0, 0, 0
	if extra != stats[0] {
		t.Fatalf("counts differ beyond the unused reads ahead:\n got %+v\nwant %+v", extra, stats[0])
	}
	if limit := 5*took[0] + time.Second; took[1] > limit {
		t.Fatalf("ReadAhead 64 took %v, ReadAhead 0 %v (limit %v)", took[1], took[0], limit)
	}
}

// TestListReadAheadByteFlips: whatever a single flipped byte does to a part
// (its header, a page, an index, the footer), a listing reports the same
// error and returns the same entries with read-ahead as without it: what
// is read ahead is checked as any page read is, and a page read ahead and
// never decoded changes nothing. Every stride-th offset of the part is
// flipped.
func TestListReadAheadByteFlips(t *testing.T) {
	noTestReadAhead(t)
	var keys []string
	for i := range 400 {
		keys = append(keys, fmt.Sprintf("d%02d/f%04d", i/8, i))
	}
	dir := write(t, parts(testutil.Part{PageBytes: 128}, keys))
	file := partFile(dir, 0)
	orig, err := os.ReadFile(file)
	must(t, err)
	stride := 11
	var failed, ahead int
	for off := 0; off < len(orig); off += stride {
		flipped := bytes.Clone(orig)
		flipped[off] ^= 0x55
		must(t, os.WriteFile(file, flipped, 0o644))
		var texts, got []string
		for _, n := range []int{0, 3, 40} {
			var st ReadStats
			text, entries := "", ""
			d, err := OpenDir(ctx, dir, OpenOptions{})
			if err == nil {
				var out []ListEntry
				out, err = d.List(ctx, ListQuery{Delimiter: '/', Limit: 1000, ReadAhead: n}, &st)
				entries = fmt.Sprint(entriesOf(out))
				_ = d.Close()
			}
			if err != nil {
				text = err.Error()
			}
			texts, got = append(texts, text), append(got, entries)
			ahead += int(st.ReadAheadReads)
		}
		if texts[0] != texts[1] || texts[0] != texts[2] || got[0] != got[1] || got[0] != got[2] {
			t.Fatalf("byte %d flipped: errors %q, entries differ: %v", off, texts, got[0] != got[1] || got[0] != got[2])
		}
		if texts[0] != "" {
			failed++
		}
	}
	must(t, os.WriteFile(file, orig, 0o644))
	t.Logf("%d bytes, every %dth flipped: %d refused or failed, %d pages read ahead", len(orig), stride, failed, ahead)
	if failed == 0 || ahead == 0 {
		t.Fatalf("%d flips failed, %d pages read ahead", failed, ahead)
	}
}

// panicSource panics in ReadAt when it is called from a read-ahead's
// goroutine, once per extent.
type panicSource struct {
	Source
	mu     sync.Mutex
	seen   map[int64]bool
	panics int
}

type panicFile struct {
	SourceFile
	s *panicSource
}

func (s *panicSource) Open(ctx context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	return panicFile{f, s}, nil
}

func (f panicFile) ReadAt(p []byte, off int64) (int, error) {
	var stack [4096]byte
	if strings.Contains(string(stack[:runtime.Stack(stack[:], false)]), "(*readAhead).fetch") {
		f.s.mu.Lock()
		first := !f.s.seen[off]
		f.s.seen[off] = true
		if first {
			f.s.panics++
		}
		f.s.mu.Unlock()
		if first {
			panic("the source failed")
		}
	}
	return f.SourceFile.ReadAt(p, off)
}

// TestListReadAheadSourcePanic: a panic in the Source's ReadAt in a
// read-ahead's goroutine does not end the process: the page is read again
// by the scan, and the listing returns what it returns without read-ahead.
func TestListReadAheadSourcePanic(t *testing.T) {
	noTestReadAhead(t)
	var keys []string
	for i := range 600 {
		keys = append(keys, fmt.Sprintf("d%02d/f%04d", i/6, i))
	}
	dir := write(t, parts(testutil.Part{PageBytes: 128}, keys))
	src := &panicSource{Source: dirOf(t, dir), seen: map[int64]bool{}}
	d, err := OpenDataset(ctx, src, OpenOptions{})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	want, err := open(t, dir, OpenOptions{}).List(ctx, ListQuery{Delimiter: '/', Limit: 1000}, nil)
	must(t, err)
	var st ReadStats
	got, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 1000, ReadAhead: 4}, &st)
	must(t, err)
	if !slices.Equal(entriesOf(got), entriesOf(want)) {
		t.Fatalf("entries %q, want %q", entriesOf(got), entriesOf(want))
	}
	if src.panics == 0 || st.ReadAheadReads < int64(src.panics) || st.ReadAheadUnused < int64(src.panics) {
		t.Fatalf("%d panics, stats %+v", src.panics, st)
	}
}
