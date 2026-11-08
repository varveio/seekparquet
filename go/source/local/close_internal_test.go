// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/seekparquettest"
)

// openTwoParts writes a two-part dataset and opens it through a
// one-descriptor cache, returning the dataset, the cache and its files.
func openTwoParts(t *testing.T) (*seekparquet.Dataset, *FileCache, []*cachedFile) {
	t.Helper()
	dir := t.TempDir()
	if err := seekparquettest.Write(dir, seekparquettest.Dataset{Parts: []seekparquettest.Part{
		{Rows: seekparquettest.Rows("a", "b")}, {Rows: seekparquettest.Rows("m", "n")},
	}}); err != nil {
		t.Fatal(err)
	}
	cache := NewFileCache(1)
	var files []*cachedFile
	src := recordingSource{Dir: NewDir(dir, cache), files: &files}
	d, err := seekparquet.OpenDataset(context.Background(), src, seekparquet.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(dir, "data", "part-00000.parquet"); len(files) != 3 || files[1].Name() != want {
		t.Fatalf("files %d, %v", len(files), files)
	}
	return d, cache, files[1:]
}

type recordingSource struct {
	*Dir
	files *[]*cachedFile
}

func (s recordingSource) Open(ctx context.Context, path string) (seekparquet.SourceFile, error) {
	p, err := s.Dir.Open(ctx, path)
	if err == nil {
		*s.files = append(*s.files, p.(*cachedFile))
	}
	return p, err
}

// TestWaitEndsWithContextAndClose occupies the one descriptor slot with a
// lease on part A, then reads part B: the read waits for the slot and
// returns its context's error as soon as it is cancelled. A second waiting
// read fails with ErrClosed as soon as the dataset is closed, and Close
// returns while A's lease is still held; releasing it closes the last
// descriptor.
func TestWaitEndsWithContextAndClose(t *testing.T) {
	d, cache, files := openTwoParts(t)
	waited := waiters(cache)
	if _, err := files[0].lease(context.Background()); err != nil {
		t.Fatal(err)
	}
	read := func(ctx context.Context) chan error {
		done := make(chan error, 1)
		go func() {
			_, err := d.Range(ctx, seekparquet.Range{Start: []byte("m"), Limit: 1}, nil)
			done <- err
		}()
		return done
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancelled := read(ctx)
	awaitWaiter(t, waited)
	cancel()
	select {
	case err := <-cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled read: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled read still waits for a descriptor")
	}

	queued := read(context.Background())
	awaitWaiter(t, waited)
	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	select {
	case err := <-queued:
		if !errors.Is(err, seekparquet.ErrClosed) {
			t.Fatalf("read queued across Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a read waiting for a descriptor held Close")
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if st := cache.Stats(); st.Open != 1 || st.Waits < 2 {
		t.Fatalf("stats %+v while A is leased: want 1 open and both reads counted as waits", st)
	}
	files[0].release()
	if st := cache.Stats(); st.Open != 0 || st.Peak > 1 {
		t.Fatalf("stats %+v after the last lease ended", st)
	}
}

// waiters returns a channel that receives each time a read of cache is
// about to wait for a descriptor.
func waiters(cache *FileCache) <-chan struct{} {
	ch := make(chan struct{}, 16)
	cache.mu.Lock()
	cache.waiting = func() {
		select {
		case ch <- struct{}{}:
		default: // never block under the cache's lock
		}
	}
	cache.mu.Unlock()
	return ch
}

func awaitWaiter(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("no read waited for a descriptor")
	}
}
