// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"os"
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
	d, err := OpenDataset(ctx, src, OpenOptions{})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	if src.opened.Load() != 1 || reads.reads.Load() != 1 {
		t.Fatalf("open: %d files opened, %d reads; want the manifest's one each", src.opened.Load(), reads.reads.Load())
	}
	var st ReadStats
	rows, err := d.Rows(ctx, Query{Selection: Selection{Span: Span{Prefix: []byte("c/")}}, Limit: 100}, &st)
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
	if _, err := d.Rows(ctx, Query{Selection: Selection{Span: Span{Prefix: []byte("c/")}}, Limit: 100}, &st); err != nil || st.PartsOpened != 0 {
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
	d, err := OpenDataset(ctx, src, OpenOptions{})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	if rows, err := d.Rows(ctx, Query{Selection: Selection{Span: PrefixSpan([]byte("a/"))}, Limit: 100}, nil); err != nil || len(rows) != 25 {
		t.Fatalf("read of a good part: %d rows, %v", len(rows), err)
	}
	for _, rng := range []Query{Query{Selection: Selection{Span: PrefixSpan([]byte("d/"))}, Limit: 1}, {Selection: Selection{Span: Span{Start: []byte("c/f020")}}, Limit: 10}, Query{Selection: Selection{Span: PrefixSpan([]byte("d/"))}, Limit: 1}} {
		if _, err := d.Rows(ctx, rng, nil); RefusalCodeOf(err) != CodeStampMismatch {
			t.Fatalf("%s: %v, want stamp_mismatch", describe(rng), err)
		}
	}
	var st ReadStats
	if _, err := d.List(ctx, ListQuery{Prefix: []byte("d/"), Limit: 5}, &st); RefusalCodeOf(err) != CodeStampMismatch || st.PartsOpened != 0 {
		t.Fatalf("list after the refusal: %v, %d parts opened", err, st.PartsOpened)
	}
	if _, err := d.List(ctx, ListQuery{Prefix: []byte("d/"), Limit: 5}, nil); RefusalCodeOf(err) != CodeStampMismatch {
		t.Fatalf("list: %v, want stamp_mismatch", err)
	}
	if err := d.Verify(ctx); RefusalCodeOf(err) != CodeStampMismatch {
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
		d := open(t, dir, OpenOptions{})
		if _, err := d.Rows(ctx, Query{Selection: Selection{Span: KeySpan([]byte(first))}, Limit: 1, Columns: []string{"size"}}, nil); err != nil {
			t.Fatalf("first touch of %s: %v", first, err)
		}
		other := map[string]string{"a": "b", "b": "a"}[first]
		if _, err := d.Rows(ctx, Query{Selection: Selection{Span: KeySpan([]byte(other))}, Limit: 1}, nil); RefusalCodeOf(err) != CodeInconsistentColumn {
			t.Fatalf("%s after %s: %v, want inconsistent_column", other, first, err)
		}
		if _, err := d.Rows(ctx, Query{Selection: Selection{Span: KeySpan([]byte(first))}, Limit: 1, Columns: []string{"size"}}, nil); err != nil {
			t.Fatalf("%s again: %v", first, err)
		}
	}
	if err := openErr(dir, OpenOptions{}); RefusalCodeOf(err) != CodeInconsistentColumn {
		t.Fatalf("every part: %v, want inconsistent_column", err)
	}
}

// TestLazyConcurrentFirstTouch races reads to an unchecked part: it is
// opened and checked once.
func TestLazyConcurrentFirstTouch(t *testing.T) {
	src := &countingSource{Source: dirOf(t, write(t, lazyParts()))}
	d, err := OpenDataset(ctx, src, OpenOptions{})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if rows, err := d.Rows(ctx, Query{Selection: Selection{Span: PrefixSpan([]byte("b/"))}, Limit: 100}, nil); err != nil || len(rows) != 25 {
				t.Errorf("%d rows, %v", len(rows), err)
			}
		})
	}
	wg.Wait()
	if src.opened.Load() != 2 {
		t.Fatalf("%d files opened, want the manifest and part 1", src.opened.Load())
	}
}

// TestLazyRefusedPartCounted counts a part whose check fails: the read
// opened and read it, so PartsOpened and SourceReads include it.
func TestLazyRefusedPartCounted(t *testing.T) {
	dir := write(t, lazyParts())
	rewrite(t, dir, 1, func(m *format.FileMetaData) { m.KeyValueMetadata = nil })
	d := open(t, dir, OpenOptions{})
	var st ReadStats
	if _, err := d.Rows(ctx, Query{Selection: Selection{Span: Span{Prefix: []byte("b/")}}, Limit: 5}, &st); RefusalCodeOf(err) != CodeStampMismatch {
		t.Fatalf("%v, want stamp_mismatch", err)
	}
	if st.PartsOpened != 1 || st.SourceReads == 0 {
		t.Fatalf("refused part: %+v, want it counted as opened and read", st)
	}
}

// TestLazyFirstTouchRacesResume resumes a cursor inside a repeated-key
// group of part 0, which locates the row after the cursor by dataset
// ordinal across every part's row count, while other reads touch part 1
// for the first time. Run with -race: a part's row count, from the
// manifest, is never rewritten by its check.
func TestLazyFirstTouchRacesResume(t *testing.T) {
	doc := parts(testutil.Part{GroupRows: 1}, []string{"a", "a", "a", "b"}, dirKeys(map[string]int{"c": 20}, "c"))
	doc.RepeatedKeys = true
	dir := write(t, doc)
	for range 20 {
		d := open(t, dir, OpenOptions{})
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Go(func() {
				if i%2 == 0 {
					rows, err := d.Rows(ctx, Query{Selection: Selection{Cursor: &Cursor{Key: []byte("a"), Offset: 0}}, Limit: 1}, nil)
					if err != nil || len(rows) != 1 || string(rows[0].Key) != "a" || rows[0].Offset != 1 {
						t.Errorf("resume: %+v, %v", rows, err)
					}
				} else if rows, err := d.Rows(ctx, Query{Selection: Selection{Span: PrefixSpan([]byte("c/"))}, Limit: 100}, nil); err != nil || len(rows) != 20 {
					t.Errorf("part 1: %d rows, %v", len(rows), err)
				}
			})
		}
		wg.Wait()
	}
}

// cancellingSource cancels a context on the first read of one file.
type cancellingSource struct {
	Source
	path   string
	cancel context.CancelFunc
	opened int
}

type cancellingFile struct {
	SourceFile
	cancel context.CancelFunc
}

func (f cancellingFile) ReadAt(p []byte, off int64) (int, error) {
	f.cancel()
	return f.SourceFile.ReadAt(p, off)
}

func (s *cancellingSource) Open(c context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(c, path)
	if err != nil || path != s.path {
		return f, err
	}
	s.opened++
	if s.opened == 1 {
		return cancellingFile{f, s.cancel}, nil
	}
	return f, nil
}

// TestLazyCancelledCheckNotKept cancels a read during its first touch of a
// part with bad stamps: the read fails with the context's error, and the
// refusal it reached is not kept, so the next read checks the part again
// and is refused.
func TestLazyCancelledCheckNotKept(t *testing.T) {
	dir := write(t, lazyParts())
	rewrite(t, dir, 1, func(m *format.FileMetaData) { m.KeyValueMetadata = nil })
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	src := &cancellingSource{Source: dirOf(t, dir), path: testutil.PartPath(1), cancel: cancel}
	d, err := OpenDataset(ctx, src, OpenOptions{})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	if _, err := d.Rows(cctx, Query{Selection: Selection{Span: PrefixSpan([]byte("b/"))}, Limit: 5}, nil); !errors.Is(err, context.Canceled) || RefusalCodeOf(err) != "" {
		t.Fatalf("cancelled first touch: %v, want context.Canceled", err)
	}
	if _, err := d.Rows(ctx, Query{Selection: Selection{Span: PrefixSpan([]byte("b/"))}, Limit: 5}, nil); RefusalCodeOf(err) != CodeStampMismatch {
		t.Fatalf("second touch: %v, want stamp_mismatch", err)
	}
	if src.opened != 2 {
		t.Fatalf("part opened %d times, want 2: the cancelled check was kept", src.opened)
	}
}

// TestLazyListBoundsBeforeTouch lists a prefix that the manifest's bounds
// place before the only part, whose file is missing: the bounds prove the
// result empty, so neither a listing nor a range touches the part.
func TestLazyListBoundsBeforeTouch(t *testing.T) {
	dir := write(t, parts(testutil.Part{}, []string{"b/x", "b/y"}))
	must(t, os.Remove(partFile(dir, 0)))
	d := open(t, dir, OpenOptions{})
	if out, err := d.List(ctx, ListQuery{Prefix: []byte("a/"), Delimiter: '/', Limit: 10}, nil); err != nil || len(out) != 0 {
		t.Fatalf("list: %v, %v", out, err)
	}
	if rows, err := d.Rows(ctx, Query{Selection: Selection{Span: PrefixSpan([]byte("a/"))}, Limit: 10}, nil); err != nil || len(rows) != 0 {
		t.Fatalf("range: %v, %v", rows, err)
	}
	if _, err := d.Rows(ctx, Query{Selection: Selection{Span: PrefixSpan([]byte("b/"))}, Limit: 10}, nil); RefusalCodeOf(err) != CodeMissingPart {
		t.Fatalf("touching the part: %v, want missing_part", err)
	}
}
