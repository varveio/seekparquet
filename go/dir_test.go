// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/varveio/seekparquet/go/internal/testutil"
)

// TestDirConfinement: OpenDir reads only regular files inside its
// directory. A part that is a symbolic link out of the directory is not
// followed, and a manifest that is a directory is not a manifest.
func TestDirConfinement(t *testing.T) {
	outside := write(t, twoParts())
	dir := write(t, twoParts())
	must(t, os.Remove(partFile(dir, 1)))
	must(t, os.Symlink(partFile(outside, 1), partFile(dir, 1)))
	if err := openErr(dir, Options{}); err == nil {
		t.Fatal("opened a part outside the directory")
	}
	// A link inside the directory is followed.
	inside := write(t, twoParts())
	must(t, os.Rename(partFile(inside, 1), filepath.Join(inside, "data", "real.parquet")))
	must(t, os.Symlink("real.parquet", partFile(inside, 1)))
	open(t, inside, Options{})

	src := dirOf(t, dir)
	if _, err := src.Open(ctx, "data"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a directory: %v, want fs.ErrNotExist", err)
	}
	if _, err := src.Open(ctx, testutil.PartPath(7)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a missing file: %v, want fs.ErrNotExist", err)
	}
	f, err := src.Open(ctx, testutil.PartPath(0))
	must(t, err)
	info, _ := os.Stat(partFile(dir, 0))
	if f.Size() != info.Size() {
		t.Fatalf("size %d, want %d", f.Size(), info.Size())
	}
	must(t, f.Close())
}
