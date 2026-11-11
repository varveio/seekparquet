// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"
)

// slowSource opens the files of a local dataset directory as an object
// store would serve them: every ReadAt waits delay before it reads, as a
// round trip would. It counts the reads, their bytes, the reads in flight
// and the most in flight at once. Its files implement ContextBinder: a
// read through a bound view stops waiting when its context ends.
type slowSource struct {
	root  *os.Root
	delay atomic.Int64 // a time.Duration
	// started is signalled (without blocking) as each read begins.
	started chan struct{}

	reads, bytes, inFlight, maxInFlight atomic.Int64
}

func newSlowSource(t testing.TB, dir string, delay time.Duration) *slowSource {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	s := &slowSource{root: root, started: make(chan struct{}, 1)}
	s.delay.Store(int64(delay))
	return s
}

func (s *slowSource) Open(ctx context.Context, path string) (seekparquet.SourceFile, error) {
	f, err := s.root.Open(filepath.FromSlash(path))
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &slowFile{f: f, size: info.Size(), s: s}, nil
}

// reset zeroes the counts, for a measurement after opening.
func (s *slowSource) reset() {
	s.reads.Store(0)
	s.bytes.Store(0)
	s.maxInFlight.Store(0)
}

type slowFile struct {
	f    *os.File
	size int64
	s    *slowSource
}

func (f *slowFile) Size() int64  { return f.size }
func (f *slowFile) Close() error { return f.f.Close() }

func (f *slowFile) ReadAt(p []byte, off int64) (int, error) {
	return f.read(context.Background(), p, off)
}

func (f *slowFile) WithContext(ctx context.Context) io.ReaderAt { return slowView{f, ctx} }

type slowView struct {
	f   *slowFile
	ctx context.Context
}

func (v slowView) ReadAt(p []byte, off int64) (int, error) { return v.f.read(v.ctx, p, off) }

func (f *slowFile) read(ctx context.Context, p []byte, off int64) (int, error) {
	s := f.s
	s.reads.Add(1)
	s.bytes.Add(int64(len(p)))
	n := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	for {
		m := s.maxInFlight.Load()
		if n <= m || s.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	select {
	case s.started <- struct{}{}:
	default:
	}
	timer := time.NewTimer(time.Duration(s.delay.Load()))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return f.f.ReadAt(p, off)
}

// folderRows yields keys shaped like Common Crawl's listing,
// crawl-data/CC-MAIN-2024-18/segments/<segment>/<kind>/<file>, folders
// segments with perFolder keys each (5 kinds of perFolder/5 files).
type folderRows struct {
	folders, perFolder, at int
	repeat                 int // each key is written repeat times (0 or 1: once)
}

var folderKinds = crawlKindNames

func (r *folderRows) key(i int) []byte {
	files := r.perFolder / len(folderKinds)
	file, kind, segment := i%files, i/files%len(folderKinds), i/r.perFolder
	return fmt.Appendf(nil, "crawl-data/CC-MAIN-2024-18/segments/1712296817%03d.%02d/%s/CC-MAIN-20240412-%05d.%s.gz",
		segment, segment%100, folderKinds[kind], file, folderKinds[kind])
}

func (r *folderRows) ReadRows(rows []parquet.Row) (int, error) {
	repeat := max(r.repeat, 1)
	total := r.folders * r.perFolder * repeat
	n := 0
	for ; n < len(rows) && r.at < total; n, r.at = n+1, r.at+1 {
		rows[n] = parquet.Row{parquet.ByteArrayValue(r.key(r.at/repeat)).Level(0, 0, 0)}
	}
	if r.at == total {
		return n, io.EOF
	}
	return n, nil
}

// folderDataset writes folders folders of perFolder keys, each written
// repeat times, as one key-only part of one row group with pageRows-row
// pages, and returns its directory.
func folderDataset(t testing.TB, folders, perFolder, pageRows, repeat int) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "crawl")
	schema := parquet.NewSchema("listing", parquet.Group{"key": parquet.String()})
	opt := local.DatasetOptions{
		Part: write.PartOptions{Key: seekparquet.Key{Column: "key", Unique: repeat <= 1}, SchemaID: "test/readahead/v1", PageRows: pageRows},
		// Windows cannot sync a directory; the tests only read what they wrote.
		AllowUnsynced: runtime.GOOS == "windows",
	}
	if err := local.WriteDir(context.Background(), dir, schema, &folderRows{folders: folders, perFolder: perFolder, repeat: repeat}, opt); err != nil {
		t.Fatal(err)
	}
	return dir
}

// listing is one measured ListQuery on a fresh Dataset over a slow source.
type listing struct {
	entries                   []seekparquet.ListEntry
	stats                     seekparquet.ReadStats
	wall                      time.Duration
	reads, bytes, maxInFlight int64
}

func measureList(t *testing.T, dir string, delay time.Duration, req seekparquet.ListQuery) listing {
	t.Helper()
	ctx := context.Background()
	src := newSlowSource(t, dir, delay)
	d, err := seekparquet.OpenDataset(ctx, src, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	src.reset()
	var l listing
	start := time.Now()
	l.entries, err = d.List(ctx, req, &l.stats)
	l.wall = time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	l.reads, l.bytes, l.maxInFlight = src.reads.Load(), src.bytes.Load(), src.maxInFlight.Load()
	if l.reads != l.stats.SourceReads || l.bytes != l.stats.SourceBytes {
		t.Fatalf("the source saw %d reads of %d bytes, ReadStats %d of %d", l.reads, l.bytes, l.stats.SourceReads, l.stats.SourceBytes)
	}
	if n := src.inFlight.Load(); n != 0 {
		t.Fatalf("%d reads still in flight after ListQuery returned", n)
	}
	return l
}

func sameEntries(t *testing.T, what string, got, want []seekparquet.ListEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d entries, want %d", what, len(got), len(want))
	}
	for i := range got {
		g, w := got[i], want[i]
		if !bytes.Equal(g.CommonPrefix, w.CommonPrefix) || (g.Row == nil) != (w.Row == nil) || (g.Exact == nil) != (w.Exact == nil) ||
			g.Row != nil && (!bytes.Equal(g.Row.Key, w.Row.Key) || g.Row.Offset != w.Row.Offset) {
			t.Fatalf("%s: entry %d is %q, want %q", what, i, g.Bytes(), w.Bytes())
		}
	}
}

// TestListReadAheadLatency lists 100 folders of 1,000 keys (4 key pages of
// 256 rows each, as the 4,000 keys of 1,024-row pages of a Common Crawl
// segment) with a 5 ms wait per read: without read-ahead the listing makes
// its page reads one after another; with ReadAhead 8 they overlap, at most
// 8 read ahead (and the scan's own read) at once, for the same entries.
func TestListReadAheadLatency(t *testing.T) {
	seekparquet.NoTestReadAhead(t)
	const delay = 5 * time.Millisecond
	dir := folderDataset(t, 100, 1000, 256, 1)
	req := seekparquet.ListQuery{Prefix: []byte("crawl-data/CC-MAIN-2024-18/segments/"), Delimiter: '/', Limit: 1000}
	off := measureList(t, dir, delay, req)
	if len(off.entries) != 100 {
		t.Fatalf("%d entries, want the 100 folders", len(off.entries))
	}
	report := func(name string, l listing) {
		t.Logf("%-14s %3d entries  %4d reads  %8d bytes  %4d key pages  wall %6.1f ms (%5.1f delays)  max in flight %d  read ahead %d, unused %d (%d bytes)",
			name, len(l.entries), l.reads, l.bytes, l.stats.KeyPagesDecoded, float64(l.wall)/1e6, float64(l.wall)/float64(delay), l.maxInFlight,
			l.stats.ReadAheadReads, l.stats.ReadAheadUnused, l.stats.ReadAheadUnusedBytes)
	}
	report("ReadAhead 0", off)
	if off.maxInFlight != 1 || off.stats.ReadAheadReads != 0 {
		t.Fatalf("without read-ahead: %d reads at once, %d read ahead", off.maxInFlight, off.stats.ReadAheadReads)
	}
	for _, n := range []int{2, 8, 32} {
		req.ReadAhead = n
		on := measureList(t, dir, delay, req)
		report(fmt.Sprintf("ReadAhead %d", n), on)
		sameEntries(t, fmt.Sprintf("ReadAhead %d", n), on.entries, off.entries)
		if on.stats.KeyPagesDecoded != off.stats.KeyPagesDecoded {
			t.Errorf("ReadAhead %d decoded %d key pages, without %d", n, on.stats.KeyPagesDecoded, off.stats.KeyPagesDecoded)
		}
		if on.maxInFlight > int64(n)+1 {
			t.Errorf("ReadAhead %d: %d reads at once", n, on.maxInFlight)
		}
		// Exact bounds predict every page; the pages read ahead and not
		// used are at most those past the listing's end.
		if on.stats.ReadAheadUnused > int64(n) || on.reads > off.reads+int64(n) {
			t.Errorf("ReadAhead %d: %d of %d pages read ahead unused, %d reads (without: %d)", n,
				on.stats.ReadAheadUnused, on.stats.ReadAheadReads, on.reads, off.reads)
		}
		if n == 8 && on.wall*2 > off.wall {
			t.Errorf("ReadAhead 8 took %v, without %v: less than twice as fast", on.wall, off.wall)
		}
	}
}

// TestListReadAheadPagination pages through the listing with cursors, and
// lists rows across pages (a delimiter no key holds, and a prefix of one
// folder), with and without read-ahead, over keys written once and keys
// written three times.
func TestListReadAheadPagination(t *testing.T) {
	seekparquet.NoTestReadAhead(t)
	for _, repeat := range []int{1, 3} {
		dir := folderDataset(t, 20, 200, 64, repeat)
		var readAhead int64
		reqs := []seekparquet.ListQuery{
			{Prefix: []byte("crawl-data/CC-MAIN-2024-18/segments/"), Delimiter: '/', Limit: 7},
			{Prefix: []byte("crawl-data/CC-MAIN-2024-18/segments/1712296817003.03/"), Delimiter: '/', Limit: 2},
			{Prefix: []byte("crawl-data/CC-MAIN-2024-18/segments/1712296817003.03/warc/"), Delimiter: '/', Limit: 50},
			{Prefix: []byte("crawl-data/"), Delimiter: '|', Limit: 333},
			{Delimiter: '.', Limit: 5},
		}
		for _, base := range reqs {
			var pages [2][]seekparquet.ListEntry
			for mode, ahead := range []int{0, 4} {
				src := newSlowSource(t, dir, 0)
				d, err := seekparquet.OpenDataset(context.Background(), src, seekparquet.OpenOptions{})
				if err != nil {
					t.Fatal(err)
				}
				req := base
				req.ReadAhead = ahead
				var st seekparquet.ReadStats
				for range 1000 {
					entries, err := d.List(context.Background(), req, &st)
					readAhead += st.ReadAheadReads
					if err != nil {
						t.Fatal(err)
					}
					pages[mode] = append(pages[mode], entries...)
					if len(entries) < req.Limit {
						break
					}
					req.Cursor = entries[len(entries)-1].Cursor()
				}
				_ = d.Close()
			}
			if len(pages[0]) == 0 {
				t.Fatalf("repeat %d, prefix %q: no entries", repeat, base.Prefix)
			}
			sameEntries(t, fmt.Sprintf("repeat %d, prefix %q delimiter %q", repeat, base.Prefix, base.Delimiter), pages[1], pages[0])
		}
		if readAhead == 0 {
			t.Fatalf("repeat %d: no page was read ahead", repeat)
		}
	}
}

// waitStarted waits for n reads to begin on src.
func waitStarted(t *testing.T, src *slowSource, n int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for src.inFlight.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("reads in flight: %d, want %d", src.inFlight.Load(), n)
		}
		select {
		case <-src.started:
		case <-time.After(time.Millisecond):
		}
	}
}

// TestListReadAheadCancelAndClose: a listing whose reads ahead are in
// flight ends when its context is cancelled, or when the Dataset closes,
// and no read, and no goroutine, outlives it.
func TestListReadAheadCancelAndClose(t *testing.T) {
	dir := folderDataset(t, 40, 500, 64, 1)
	req := seekparquet.ListQuery{Prefix: []byte("crawl-data/CC-MAIN-2024-18/segments/"), Delimiter: '/', Limit: 1000, ReadAhead: 8}
	for _, how := range []string{"cancel", "close"} {
		t.Run(how, func(t *testing.T) {
			goroutines := runtime.NumGoroutine()
			src := newSlowSource(t, dir, 0)
			d, err := seekparquet.OpenDataset(context.Background(), src, seekparquet.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// Open the part and its row group's index, then make every read wait
			// 200 ms, or until its context ends.
			if _, err := d.List(ctx, seekparquet.ListQuery{Prefix: req.Prefix, Delimiter: '/', Limit: 1}, nil); err != nil {
				t.Fatal(err)
			}
			src.delay.Store(int64(200 * time.Millisecond))
			var wg sync.WaitGroup
			var listErr error
			wg.Go(func() {
				r := req
				r.Cursor = &seekparquet.Cursor{Prefix: []byte("crawl-data/CC-MAIN-2024-18/segments/1712296817001.01/")}
				_, listErr = d.List(ctx, r, nil)
			})
			waitStarted(t, src, 2) // reads ahead in flight
			var closeErr error
			if how == "cancel" {
				cancel()
			} else {
				closeErr = d.Close()
			}
			wg.Wait()
			if n := src.inFlight.Load(); n != 0 {
				t.Fatalf("%d reads in flight after ListQuery returned", n)
			}
			switch {
			case how == "cancel" && !errors.Is(listErr, context.Canceled):
				t.Fatalf("ListQuery after cancel: %v", listErr)
			case how == "close" && !errors.Is(listErr, seekparquet.ErrClosed):
				t.Fatalf("ListQuery during Close: %v (Close: %v)", listErr, closeErr)
			}
			_ = d.Close()
			deadline := time.Now().Add(5 * time.Second)
			for runtime.NumGoroutine() > goroutines && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if n := runtime.NumGoroutine(); n > goroutines {
				t.Fatalf("%d goroutines after the listing, %d before", n, goroutines)
			}
		})
	}
}

// TestListReadAheadInvalid: a negative ReadAhead is an invalid request.
func TestListReadAheadInvalid(t *testing.T) {
	dir := folderDataset(t, 2, 10, 64, 1)
	d, err := seekparquet.OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	_, err = d.List(context.Background(), seekparquet.ListQuery{Delimiter: '/', Limit: 1, ReadAhead: -1}, nil)
	if !errors.Is(err, seekparquet.ErrInvalidRequest) {
		t.Fatalf("ReadAhead -1: %v", err)
	}
}
