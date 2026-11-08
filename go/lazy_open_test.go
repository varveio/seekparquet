// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// lazyParts is four parts of 25 keys each: a000..a024, b000..b024, ...
func lazyParts() testutil.Dataset {
	var keys [][]string
	for _, dir := range []string{"a", "b", "c", "d"} {
		keys = append(keys, dirKeys(map[string]int{dir: 25}, dir))
	}
	return parts(testutil.Part{GroupRows: 10}, keys...)
}

// TestLazyOpenCounts counts the Source's work: opening reads the manifest
// and opens no part (CHECK-1); a prefix read routed to one part opens and
// checks exactly that part, and a part known from its bounds to begin at
// the read's upper bound is not touched; Verify opens the rest (CHECK-3).
func TestLazyOpenCounts(t *testing.T) {
	reads := &readCounter{Source: dirOf(t, write(t, lazyParts()))}
	src := &countingSource{Source: reads}
	d, err := OpenDataset(ctx, src, Options{})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	if src.opened.Load() != 1 || reads.reads.Load() != 1 {
		t.Fatalf("open: %d files opened, %d reads; want the manifest's one each", src.opened.Load(), reads.reads.Load())
	}
	var st ReadStats
	rows, err := d.Range(ctx, Range{Prefix: []byte("c/"), Limit: 100, Stats: &st})
	must(t, err)
	if len(rows) != 25 || string(rows[0].Key) != "c/f000" {
		t.Fatalf("prefix read: %d rows, first %q", len(rows), keysOf(rows[:1]))
	}
	if src.opened.Load() != 2 || st.PartsOpened != 1 {
		t.Fatalf("prefix read: %d files opened in all, %d parts by the read; want the manifest and part 2", src.opened.Load(), st.PartsOpened)
	}
	if !d.r.parts[2].ready.Load() || d.r.parts[1].ready.Load() || d.r.parts[3].ready.Load() {
		t.Fatal("the prefix read checked a part it was not routed to")
	}
	if _, err := d.Range(ctx, Range{Prefix: []byte("c/"), Limit: 100, Stats: &st}); err != nil || st.PartsOpened != 0 {
		t.Fatalf("repeated read: %v, %d parts opened", err, st.PartsOpened)
	}
	must(t, d.Verify(ctx))
	if src.opened.Load() != 5 {
		t.Fatalf("Verify: %d files opened in all, want the manifest and 4 parts", src.opened.Load())
	}
	must(t, d.Close())
	if src.closed.Load() != 5 {
		t.Fatalf("Close: %d of 5 files closed", src.closed.Load())
	}
}

// TestLazyPartRefusal breaks one part's stamps: open succeeds, a read
// routed elsewhere succeeds, every read that touches the part is refused
// with the same code, and so is Verify. The part is opened, refused and
// closed once.
func TestLazyPartRefusal(t *testing.T) {
	dir := write(t, lazyParts())
	rewrite(t, dir, 3, func(m *format.FileMetaData) { m.KeyValueMetadata = nil })
	src := &countingSource{Source: dirOf(t, dir)}
	d, err := OpenDataset(ctx, src, Options{})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	if rows, err := d.Range(ctx, PrefixRange([]byte("a/"), 100)); err != nil || len(rows) != 25 {
		t.Fatalf("read of a good part: %d rows, %v", len(rows), err)
	}
	for _, rng := range []Range{PrefixRange([]byte("d/"), 1), {Start: []byte("c/f020"), Limit: 10}, PrefixRange([]byte("d/"), 1)} {
		if _, err := d.Range(ctx, rng); CodeOf(err) != CodeStampMismatch {
			t.Fatalf("%s: %v, want stamp_mismatch", describe(rng), err)
		}
	}
	if _, err := d.List(ctx, List{Prefix: []byte("d/"), Limit: 5}); CodeOf(err) != CodeStampMismatch {
		t.Fatalf("list: %v, want stamp_mismatch", err)
	}
	if err := d.Verify(ctx); CodeOf(err) != CodeStampMismatch {
		t.Fatalf("Verify: %v, want stamp_mismatch", err)
	}
	if src.opened.Load() != 5 || src.closed.Load() != 2 {
		t.Fatalf("opened %d, closed %d: want the manifest and part 3 closed, part 3 opened once", src.opened.Load(), src.closed.Load())
	}
}

// TestLazyColumnConsistency checks COL-5 incrementally: a part whose
// column differs from a part checked before it is refused when touched,
// whichever is touched first, and the part checked first stays readable.
func TestLazyColumnConsistency(t *testing.T) {
	type narrow struct {
		Key  []byte `parquet:"key"`
		Size int32  `parquet:"size"`
	}
	type wide struct {
		Key  []byte `parquet:"key"`
		Size int64  `parquet:"size"`
	}
	dir := write(t, testutil.Dataset{Parts: []testutil.Part{
		{Rows: []narrow{{[]byte("a"), 1}}},
		{Rows: []wide{{[]byte("b"), 2}}},
	}})
	for _, first := range []string{"a", "b"} {
		d := open(t, dir, Options{})
		if _, err := d.Range(ctx, KeyRange([]byte(first), 1, "size")); err != nil {
			t.Fatalf("first touch of %s: %v", first, err)
		}
		other := map[string]string{"a": "b", "b": "a"}[first]
		if _, err := d.Range(ctx, KeyRange([]byte(other), 1)); CodeOf(err) != CodeInconsistentColumn {
			t.Fatalf("%s after %s: %v, want inconsistent_column", other, first, err)
		}
		if _, err := d.Range(ctx, KeyRange([]byte(first), 1, "size")); err != nil {
			t.Fatalf("%s again: %v", first, err)
		}
	}
	if err := openErr(dir, Options{}); CodeOf(err) != CodeInconsistentColumn {
		t.Fatalf("every part: %v, want inconsistent_column", err)
	}
}

// TestLazyConcurrentFirstTouch races reads to an unchecked part: it is
// opened and checked once.
func TestLazyConcurrentFirstTouch(t *testing.T) {
	src := &countingSource{Source: dirOf(t, write(t, lazyParts()))}
	d, err := OpenDataset(ctx, src, Options{})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if rows, err := d.Range(ctx, PrefixRange([]byte("b/"), 100)); err != nil || len(rows) != 25 {
				t.Errorf("%d rows, %v", len(rows), err)
			}
		})
	}
	wg.Wait()
	if src.opened.Load() != 2 {
		t.Fatalf("%d files opened, want the manifest and part 1", src.opened.Load())
	}
}
