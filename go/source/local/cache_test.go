// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package local_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/source/local"
)

// TestFileCacheBoundsHandles reads ten files concurrently through a cache
// of three handles: every read sees its own file's bytes, no more than
// three handles are ever open, a file replaced after its handle was
// evicted is refused rather than read, and a closed file reads no more.
func TestFileCacheBoundsHandles(t *testing.T) {
	dir := t.TempDir()
	cache := local.NewFileCache(3)
	var files []*local.CachedFile
	for i := range 10 {
		path := filepath.Join(dir, fmt.Sprintf("f%d", i))
		if err := os.WriteFile(path, bytes.Repeat([]byte{byte('a' + i)}, 64), 0o644); err != nil {
			t.Fatal(err)
		}
		f, err := cache.Open(context.Background(), path, nil)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			buf := make([]byte, 8)
			for n := range 200 {
				i := (g*7 + n) % len(files)
				if _, err := files[i].ReadAt(buf, int64(n%56)); err != nil {
					t.Error(err)
					return
				}
				if !bytes.Equal(buf, bytes.Repeat([]byte{byte('a' + i)}, 8)) {
					t.Errorf("file %d read %q", i, buf)
					return
				}
			}
		})
	}
	wg.Wait()
	if st := cache.Stats(); st.Peak > 3 || st.Open > 3 || st.Opens <= 10 || st.Evictions != st.Opens-st.Open {
		t.Fatalf("stats %+v: at most 3 open, reopens after eviction, an eviction for every descriptor closed", st)
	}
	// Evict f0 by reading three others, then replace it.
	for _, i := range []int{1, 2, 3} {
		if _, err := files[i].ReadAt(make([]byte, 1), 0); err != nil {
			t.Fatal(err)
		}
	}
	path := files[0].Name()
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, bytes.Repeat([]byte{'z'}, 64), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := files[0].ReadAt(make([]byte, 1), 0); err == nil {
		t.Fatal("a replaced file was read")
	}
	_ = files[1].Close()
	if _, err := files[1].ReadAt(make([]byte, 1), 0); !errors.Is(err, os.ErrClosed) || !errors.Is(err, seekparquet.ErrClosed) {
		t.Fatalf("read after close: %v", err)
	}
	for _, f := range files {
		_ = f.Close()
	}
	if st := cache.Stats(); st.Open != 0 {
		t.Fatalf("%d handles open after every file closed", st.Open)
	}
}

// TestDirConfinesPaths: a symbolic link inside the dataset root that points
// outside it is not followed, and a directory is not a file.
func TestDirConfinesPaths(t *testing.T) {
	outside := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Skip(err)
	}
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "ok"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := local.NewDir(root, nil)
	if _, err := dir.Open(context.Background(), "link"); err == nil {
		t.Fatal("a link out of the root was followed")
	}
	if _, err := dir.Open(context.Background(), "sub"); !errors.Is(err, fs.ErrNotExist) || !errors.Is(err, local.ErrNotRegular) {
		t.Fatalf("a directory: %v", err)
	}
	if _, err := dir.Open(context.Background(), "missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a missing file: %v", err)
	}
	f, err := dir.Open(context.Background(), "sub/ok")
	if err != nil || f.Size() != 2 {
		t.Fatalf("sub/ok: %v", err)
	}
	_ = f.Close()
}
