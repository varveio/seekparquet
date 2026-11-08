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
