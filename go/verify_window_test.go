// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// versionSource is a Source over dir whose part opens and reads call hooks:
// open with the part's path and how many opens of it came before, read
// with the file's open and the extent.
type versionSource struct {
	Source
	mu     sync.Mutex
	opens  map[string]int
	open   func(path string, nth int) (Source, string) // where the nth open of path is served from
	read   func(path string, nth int, p []byte, off int64)
	opened atomic.Int64
}

type versionFile struct {
	SourceFile
	s    *versionSource
	path string
	nth  int
}

func (s *versionSource) Open(c context.Context, name string) (SourceFile, error) {
	if !strings.HasPrefix(name, "data/") {
		return s.Source.Open(c, name)
	}
	s.mu.Lock()
	nth := s.opens[name]
	s.opens[name]++
	s.mu.Unlock()
	from, as := s.Source, name
	if s.open != nil {
		if alt, altName := s.open(name, nth); alt != nil {
			from, as = alt, altName
		}
	}
	f, err := from.Open(c, as)
	if err != nil {
		return nil, err
	}
	s.opened.Add(1)
	return versionFile{f, s, name, nth}, nil
}

func (f versionFile) ReadAt(p []byte, off int64) (int, error) {
	if f.s.read != nil {
		f.s.read(f.path, f.nth, p, off)
	}
	return f.SourceFile.ReadAt(p, off)
}

func newVersionSource(t testing.TB, dir string) *versionSource {
	return &versionSource{Source: dirOf(t, dir), opens: map[string]int{}}
}

// openWith opens the dataset of src with opt as OpenDataset does, reading
// as a local directory when local is set (OpenDir's zero policy).
func openWith(t testing.TB, c context.Context, src Source, opt OpenOptions, local bool) *Dataset {
	t.Helper()
	must(t, prepareOpen(&opt))
	d, err := openDataset(c, src, opt, local)
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// TestVerifyLocalLimit: Verify's parts take no slot of the operation's
// limit while they are fetched; only their reads do, each its own. With
// every slot's part opened before any of its footer reads proceeds, as
// OpenDir's zero policy reads a part larger than its end's window (its
// head and end, one at a time), Verify completes instead of each part
// waiting for a slot another holds.
func TestVerifyLocalLimit(t *testing.T) {
	keys := fixtureKeys(4000)
	dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, split(keys, 8)...))
	const slots = 2
	src := newVersionSource(t, dir)
	arrived := make(chan struct{})
	var n atomic.Int64
	src.open = func(string, int) (Source, string) {
		if n.Add(1) == slots {
			close(arrived)
		}
		<-arrived // every slot's part is open before any reads
		return nil, ""
	}
	c, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	d := openWith(t, c, src, OpenOptions{Read: ReadPolicy{MetadataPrefetchBytes: 1 << 10}}, true)
	d.r.concurrency = slots
	if !d.r.local || d.r.width() != 1 {
		t.Fatal("not a local reader")
	}
	must(t, d.Verify(c, nil))
}

// swapKeys returns the bytes of the part file at name with keys a and b,
// of equal length, swapped where they first occur: in their key page.
func swapKeys(t testing.TB, name, a, b string) []byte {
	t.Helper()
	raw, err := os.ReadFile(name)
	must(t, err)
	i, j := bytes.Index(raw, []byte(a)), bytes.Index(raw, []byte(b))
	if len(a) != len(b) || i < 0 || j < 0 {
		t.Fatalf("keys %q and %q not found alike", a, b)
	}
	copy(raw[i:], b)
	copy(raw[j:], a)
	return raw
}

// TestVerifyAdoptsHandle: the pages Verify fetches with a part it opens
// are its handle's, and a Source's immutability holds per handle: when a
// concurrent read checks the part first, with its own handle of another
// version of the same size, bounds and page extents, Verify decodes that
// handle's pages, never the ones it fetched from the handle it discards.
func TestVerifyAdoptsHandle(t *testing.T) {
	keys := make([]string, 2400)
	for i := range keys {
		keys[i] = fmt.Sprintf("key/%06d", i)
	}
	dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, split(keys, 4)...))
	// Another version of part 1: its last key page holds two keys
	// swapped, out of order, at the same extents.
	last := split(keys, 4)[1]
	other := t.TempDir()
	alt := filepath.Join(other, filepath.FromSlash(testutil.PartPath(1)))
	must(t, os.MkdirAll(filepath.Dir(alt), 0o755))
	must(t, os.WriteFile(alt, swapKeys(t, partFile(dir, 1), last[len(last)-3], last[len(last)-2]), 0o644))
	altSrc := dirOf(t, other)
	// The other version alone is refused.
	src := newVersionSource(t, dir)
	src.open = func(name string, _ int) (Source, string) {
		if name == testutil.PartPath(1) {
			return altSrc, name
		}
		return nil, ""
	}
	if err := openWith(t, ctx, src, OpenOptions{}, false).Verify(ctx, nil); RefusalCodeOf(err) == "" {
		t.Fatalf("the other version: %v, want a refusal", err)
	}

	src = newVersionSource(t, dir)
	src.open = func(name string, nth int) (Source, string) {
		if name == testutil.PartPath(1) && nth == 0 { // Verify's open
			return altSrc, name
		}
		return nil, ""
	}
	fetching, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	src.read = func(name string, nth int, p []byte, off int64) {
		// Verify's reads of its handle after the part's end: its pages.
		if name == testutil.PartPath(1) && nth == 0 && off > 0 && len(p) < 4096 {
			once.Do(func() { close(fetching) })
			<-release
		}
	}
	d := openWith(t, ctx, src, OpenOptions{}, false)
	verified := make(chan error, 1)
	go func() { verified <- d.Verify(ctx, nil) }()
	<-fetching
	// A read checks part 1 first, through its own handle, which is
	// published; Verify's is then discarded.
	rows, err := d.Rows(ctx, Query{Selection: Selection{Span: KeySpan([]byte(last[0]))}, Limit: 1}, nil)
	must(t, err)
	if len(rows) != 1 || string(rows[0].Key) != last[0] {
		t.Fatalf("rows %v", keysOf(rows))
	}
	close(release)
	if err := <-verified; err != nil {
		t.Fatalf("Verify decoded the discarded handle's pages: %v", err)
	}
	rows, err = d.Rows(ctx, Query{Selection: Selection{Span: Span{Start: []byte(last[len(last)-3])}}, Limit: 3}, nil)
	must(t, err)
	if got := keysOf(rows); fmt.Sprint(got) != fmt.Sprint(last[len(last)-3:]) {
		t.Fatalf("rows %q", got)
	}
}

// TestVerifyWindow: Verify fetches at most Concurrency parts ahead of the
// one it checks, so that the parts' ends it holds stay bounded however
// many parts there are: while the first part's read waits, no more parts
// than that are opened.
func TestVerifyWindow(t *testing.T) {
	keys := fixtureKeys(4000)
	dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, split(keys, 20)...))
	verified(t, dir, OpenOptions{}) // decoders made outside the bubble
	for _, slots := range []int{1, 2, 4} {
		synctest.Test(t, func(t *testing.T) {
			src := newVersionSource(t, dir)
			gate := make(chan struct{})
			src.read = func(name string, _ int, _ []byte, _ int64) {
				if name == testutil.PartPath(0) {
					<-gate
				}
			}
			d := openWith(t, ctx, src, OpenOptions{Read: ReadPolicy{}}, false)
			d.r.concurrency = slots
			done := make(chan error, 1)
			go func() { done <- d.Verify(ctx, nil) }()
			synctest.Wait()
			if got := src.opened.Load(); got != int64(slots) {
				t.Errorf("Concurrency %d: %d parts opened while the first waits", slots, got)
			}
			close(gate)
			must(t, <-done)
			if got := src.opened.Load(); got != 20 {
				t.Errorf("Concurrency %d: %d parts opened", slots, got)
			}
		})
	}
}

// slowSource is limitSource whose every read takes a millisecond, on the
// bubble's clock, so that reads that may overlap do.
func slowSource(t testing.TB, dir string) *limitSource {
	return &limitSource{Source: dirOf(t, dir), delay: time.Millisecond}
}

// TestOperationLimit: the internal concurrency limit bounds the ranges an
// operation has in flight wherever they come from: a list-index build's
// workers, and a listing's read-ahead beside its own reads: a listing
// that skips common prefixes leaves pages read ahead in flight as it
// reads the next row group's key index as a pair of ranges.
func TestOperationLimit(t *testing.T) {
	keys := fixtureKeys(6000)
	dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, split(keys, 4)...))
	verified(t, dir, OpenOptions{})
	synctest.Test(t, func(t *testing.T) {
		src := slowSource(t, dir)
		d := openWith(t, ctx, src, OpenOptions{Read: ReadPolicy{}}, false)
		d.r.concurrency = 1
		_, err := d.BuildListIndex(ctx, ListIndexOptions{Workers: 8})
		must(t, err)
		if got := src.most.Load(); got != 1 {
			t.Errorf("list index: %d reads at once, Concurrency 1", got)
		}
	})
	synctest.Test(t, func(t *testing.T) {
		src := slowSource(t, dir)
		// A window holding no page index: each row group's key index is
		// read as a pair of ranges.
		d := openWith(t, ctx, src, OpenOptions{Read: ReadPolicy{MetadataPrefetchBytes: 64}}, false)
		d.r.concurrency = 2
		var st ReadStats
		entries, err := d.List(ctx, ListQuery{Delimiter: '/', Limit: 100, ReadAhead: 8}, &st)
		must(t, err)
		if len(entries) == 0 || st.ReadAheadReads == 0 || st.ReadAheadUnusedBytes == 0 {
			t.Fatalf("%d entries, %+v", len(entries), st)
		}
		if got := src.most.Load(); got > 2 {
			t.Errorf("listing: %d reads at once, Concurrency 2", got)
		}
	})
}

// TestVerifyStats: Verify(ctx, &st) resets st and fills it on success, on
// a refusal and on cancellation.
func TestVerifyStats(t *testing.T) {
	keys := fixtureKeys(2400)
	dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, split(keys, 4)...))
	st := ReadStats{SourceReads: -1}
	d := open(t, dir, OpenOptions{})
	must(t, d.Verify(ctx, &st))
	if st.PartsOpened != 4 || st.SourceReads <= 0 || st.KeyPagesDecoded <= 0 {
		t.Fatalf("success: %+v", st)
	}
	must(t, d.Verify(ctx, &st))
	if st != (ReadStats{}) {
		t.Fatalf("a second Verify: %+v", st)
	}
	setHead(t, dir, 2, "XAR1")
	d = open(t, dir, OpenOptions{})
	if err := d.Verify(ctx, &st); RefusalCodeOf(err) != CodeInvalidFooter {
		t.Fatalf("refusal: %v", err)
	}
	if st.PartsOpened < 3 || st.SourceReads <= 0 { // and the part after it, when its fetch began
		t.Fatalf("refusal: %+v", st)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	st = ReadStats{SourceReads: -1}
	if err := open(t, dir, OpenOptions{}).Verify(cancelled, &st); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	if st.SourceReads != 0 {
		t.Fatalf("cancelled: %+v", st)
	}
}

// TestVerifyConcurrentReads: Verify alongside reads of the same parts, and
// cancelled at every point of its ordered checks, leaves every read's
// answer and every later Verify's unchanged, and no goroutine behind.
func TestVerifyConcurrentReads(t *testing.T) {
	keys := fixtureKeys(4000)
	dir := write(t, parts(testutil.Part{GroupRows: 150, PageBytes: 400}, split(keys, 10)...))
	verified(t, dir, OpenOptions{})
	for step := range 40 {
		synctest.Test(t, func(t *testing.T) {
			src := slowSource(t, dir)
			d := openWith(t, ctx, src, OpenOptions{Read: ReadPolicy{}}, false)
			d.r.concurrency = 3
			vctx, cancel := context.WithCancel(ctx)
			verified := make(chan error, 1)
			go func() { verified <- d.Verify(vctx, nil) }()
			var wg sync.WaitGroup
			for q := range 6 {
				wg.Go(func() {
					key := keys[(step*97+q*631)%len(keys)]
					rows, err := d.Rows(ctx, Query{Selection: Selection{Span: Span{Start: []byte(key)}}, Limit: 3}, nil)
					if err != nil || len(rows) == 0 || string(rows[0].Key) != key {
						t.Errorf("step %d: rows from %q: %q, %v", step, key, keysOf(rows), err)
					}
				})
			}
			time.Sleep(time.Duration(step) * time.Millisecond)
			cancel()
			if err := <-verified; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("step %d: Verify: %v", step, err)
			}
			wg.Wait()
			must(t, d.Verify(ctx, nil))
			must(t, d.Close())
		})
	}
}
