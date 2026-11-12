// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// locsFixture writes 200 keys as two 100-row row groups of small pages, and
// opens it through a counting Source with its part checked.
func locsFixture(t *testing.T, dir string, lim Limits) (*Dataset, *accessSource) {
	t.Helper()
	src := &accessSource{Source: dirOf(t, dir)}
	// The OffsetIndexes are read from the Source, not retained metadata.
	d, err := OpenDataset(ctx, src, OpenOptions{Limits: lim, Read: ReadPolicy{DisableSpeculation: true}})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	must(t, d.r.checkParts(ctx, new(ReadStats)))
	src.reset()
	return d, src
}

func locsKeys() []string {
	var keys []string
	for i := range 200 {
		keys = append(keys, fmt.Sprintf("k%03d", i))
	}
	return keys
}

// keptLocs returns the bytes charged for the page locations kept for the
// column chunks of p, recomputed from the slots.
func keptLocs(p *part) (chunks int, size int64) {
	for _, group := range p.locs {
		for i := range group {
			if locs := group[i].Load(); locs != nil {
				chunks++
				size += int64(len(*locs))*int64(unsafe.Sizeof(pageLoc{})) + 64
			}
		}
	}
	return chunks, size
}

// TestOffsetIndexCacheReadStats pins what a kept OffsetIndex saves: a later
// request reads neither it nor its bytes, and decodes the same pages. Past
// Limits.OffsetIndexMemory nothing is kept and every request reads the
// indexes, with the same rows.
func TestOffsetIndexCacheReadStats(t *testing.T) {
	dir := write(t, parts(testutil.Part{GroupRows: 100, PageBytes: 64}, locsKeys()))
	meta, err := testutil.ReadFooter(partFile(dir, 0))
	must(t, err)
	columns := []string{"size", "name"}
	var indexBytes int64
	for _, c := range meta.RowGroups[0].Columns[1:] {
		indexBytes += int64(c.OffsetIndexLength)
	}
	req := func(d *Dataset, st *ReadStats) []Row {
		rows, err := d.Rows(ctx, Query{Selection: Selection{Span: Span{Start: []byte("k010")}}, Limit: 50, Columns: columns}, st)
		must(t, err)
		return rows
	}

	cached, csrc := locsFixture(t, dir, Limits{})
	uncached, usrc := locsFixture(t, dir, Limits{OffsetIndexMemory: 1})
	var c1, c2, u1, u2 ReadStats
	want := req(uncached, &u1)
	usrc.reset()
	if got := req(uncached, &u2); !reflect.DeepEqual(got, want) {
		t.Fatal("uncached rows differ between requests")
	}
	checkStats(t, "uncached repeat", u2, u2, usrc)
	var u3 ReadStats
	req(uncached, &u3)
	if u2.IndexLoads != 0 || u3 != u2 {
		t.Fatalf("uncached repeats: %+v, then %+v", u2, u3)
	}
	if n, size := keptLocs(uncached.r.parts[0]); n != 0 || size != 0 || uncached.r.locsMemory.Load() != 0 {
		t.Fatalf("over budget: %d chunks kept, %d bytes charged", n, uncached.r.locsMemory.Load())
	}

	if got := req(cached, &c1); !reflect.DeepEqual(got, want) {
		t.Fatal("cached rows differ")
	}
	if c1 != u1 {
		t.Fatalf("a cold request: %+v, uncached %+v", c1, u1)
	}
	n, size := keptLocs(cached.r.parts[0])
	if n != len(columns) || cached.r.locsMemory.Load() != size {
		t.Fatalf("%d chunks kept, %d bytes charged, want %d chunks, %d bytes", n, cached.r.locsMemory.Load(), len(columns), size)
	}
	csrc.reset()
	if got := req(cached, &c2); !reflect.DeepEqual(got, want) {
		t.Fatal("cached rows differ on a repeat")
	}
	// The repeat skips one read per projected column chunk: its OffsetIndex.
	w := u2
	w.SourceReads -= int64(len(columns))
	w.UnreportedIOCalls -= int64(len(columns))
	checkStats(t, "cached repeat", c2, w, csrc)
	if u2.SourceBytes-c2.SourceBytes != indexBytes {
		t.Fatalf("the repeat read %d fewer bytes, want the OffsetIndexes' %d", u2.SourceBytes-c2.SourceBytes, indexBytes)
	}
}

// TestOffsetIndexCacheMalformed corrupts a value column's OffsetIndex.
// One that fails its own check (checkLocations) is never kept and is
// refused on every request. One that passes it but misdescribes a page is
// kept, and still refused on every request, by the page checks each
// request makes against it.
func TestOffsetIndexCacheMalformed(t *testing.T) {
	edits := map[string]struct {
		edit func(index *format.OffsetIndex)
		kept bool
	}{
		"check": {func(index *format.OffsetIndex) { index.PageLocations[1].FirstRowIndex = 0 }, false},
		"page":  {func(index *format.OffsetIndex) { index.PageLocations[1].FirstRowIndex++ }, true},
	}
	for name, e := range edits {
		t.Run(name, func(t *testing.T) {
			dir := write(t, parts(testutil.Part{PageBytes: 64}, locsKeys()))
			meta, err := testutil.ReadFooter(partFile(dir, 0))
			must(t, err)
			chunk := meta.RowGroups[0].Columns[1] // size
			raw, err := os.ReadFile(partFile(dir, 0))
			must(t, err)
			at := raw[chunk.OffsetIndexOffset : chunk.OffsetIndexOffset+int64(chunk.OffsetIndexLength)]
			var index format.OffsetIndex
			must(t, thrift.Unmarshal(new(thrift.CompactProtocol), at, &index))
			e.edit(&index)
			encoded, err := thrift.Marshal(new(thrift.CompactProtocol), &index)
			must(t, err)
			if len(encoded) != len(at) {
				t.Fatalf("the edited index is %d bytes, the original %d", len(encoded), len(at))
			}
			copy(at, encoded)
			must(t, os.WriteFile(partFile(dir, 0), raw, 0o644))
			d := open(t, dir, OpenOptions{})
			for i := range 3 {
				if _, err := d.Rows(ctx, Query{Limit: 200, Columns: []string{"size"}}, nil); RefusalCodeOf(err) != CodeInvalidParquet {
					t.Fatalf("request %d: got %v, want invalid_parquet", i, err)
				}
			}
			if kept := d.r.parts[0].locs[0][1].Load() != nil; kept != e.kept {
				t.Fatalf("size locations kept: %v, want %v", kept, e.kept)
			}
			// The intact column is kept and served as before.
			for range 2 {
				rows, err := d.Rows(ctx, Query{Limit: 200, Columns: []string{"name"}}, nil)
				must(t, err)
				if len(rows) != 200 {
					t.Fatalf("%d rows", len(rows))
				}
			}
			if d.r.parts[0].locs[0][2].Load() == nil {
				t.Fatal("the name column's locations were not kept")
			}
			if _, size := keptLocs(d.r.parts[0]); d.r.locsMemory.Load() != size {
				t.Fatalf("%d bytes charged, %d kept", d.r.locsMemory.Load(), size)
			}
		})
	}
}

// TestOffsetIndexCacheConcurrent runs concurrent cold and warm requests
// (run with -race) at several budgets, from one that keeps nothing to the
// default: rows are those of a dataset that keeps nothing, and the charge
// matches what was published, each chunk once.
func TestOffsetIndexCacheConcurrent(t *testing.T) {
	keys := locsKeys()
	dir := write(t, parts(testutil.Part{GroupRows: 50, PageBytes: 64}, keys))
	columns := []string{"size", "name"}
	ref := open(t, dir, OpenOptions{Limits: Limits{OffsetIndexMemory: 1}})
	var starts []int
	want := map[int][]Row{}
	for s := 0; s < len(keys); s += 7 {
		rows, err := ref.Rows(ctx, Query{Selection: Selection{Span: Span{Start: []byte(keys[s])}}, Limit: 30, Columns: columns}, nil)
		must(t, err)
		starts, want[s] = append(starts, s), rows
	}
	for _, budget := range []int64{1, 300, 1000, 0} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			d := open(t, dir, OpenOptions{Limits: Limits{OffsetIndexMemory: budget}})
			var wg sync.WaitGroup
			errs := make(chan error, 16)
			for w := range 16 {
				wg.Go(func() {
					for i := range starts {
						s := starts[(i+w)%len(starts)]
						rows, err := d.Rows(ctx, Query{Selection: Selection{Span: Span{Start: []byte(keys[s])}}, Limit: 30, Columns: columns}, nil)
						if err == nil && !reflect.DeepEqual(rows, want[s]) {
							err = fmt.Errorf("start %d: rows differ", s)
						}
						if err != nil {
							errs <- err
							return
						}
					}
				})
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
			_, size := keptLocs(d.r.parts[0])
			if got := d.r.locsMemory.Load(); got != size || got > d.r.limits.OffsetIndexMemory {
				t.Fatalf("charged %d bytes, kept %d, budget %d", got, size, d.r.limits.OffsetIndexMemory)
			}
		})
	}
}

// failAtSource fails reads at one offset of part files while fail is set.
type failAtSource struct {
	Source
	off  int64
	fail atomic.Bool
}

type failAtFile struct {
	SourceFile
	owner *failAtSource
}

func (s *failAtSource) Open(c context.Context, path string) (SourceFile, error) {
	f, err := s.Source.Open(c, path)
	if err != nil {
		return nil, err
	}
	return failAtFile{f, s}, nil
}

func (f failAtFile) ReadAt(p []byte, off int64) (int, error) {
	if off == f.owner.off && f.owner.fail.Load() {
		return 0, errors.New("transient read failure")
	}
	return f.SourceFile.ReadAt(p, off)
}

// TestOffsetIndexCacheTransientFailure fails the read of a value column's
// OffsetIndex: the request fails and nothing is kept; once the Source
// recovers, the next request reads the index and keeps it.
func TestOffsetIndexCacheTransientFailure(t *testing.T) {
	dir := write(t, parts(testutil.Part{PageBytes: 64}, locsKeys()))
	meta, err := testutil.ReadFooter(partFile(dir, 0))
	must(t, err)
	src := &failAtSource{Source: dirOf(t, dir), off: meta.RowGroups[0].Columns[1].OffsetIndexOffset}
	d, err := OpenDataset(ctx, src, OpenOptions{Read: ReadPolicy{DisableSpeculation: true}}) // the OffsetIndex is read from the Source
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	req := Query{Limit: 200, Columns: []string{"size"}}
	src.fail.Store(true)
	if _, err := d.Rows(ctx, req, nil); err == nil {
		t.Fatal("a failed OffsetIndex read went unreported")
	}
	if d.r.parts[0].locs[0][1].Load() != nil || d.r.locsMemory.Load() != 0 {
		t.Fatal("locations kept after a failed read")
	}
	src.fail.Store(false)
	for range 2 {
		rows, err := d.Rows(ctx, req, nil)
		must(t, err)
		if len(rows) != 200 {
			t.Fatalf("%d rows", len(rows))
		}
	}
	if d.r.parts[0].locs[0][1].Load() == nil {
		t.Fatal("locations not kept after recovery")
	}
	if _, size := keptLocs(d.r.parts[0]); d.r.locsMemory.Load() != size {
		t.Fatalf("%d bytes charged, %d kept", d.r.locsMemory.Load(), size)
	}
}
