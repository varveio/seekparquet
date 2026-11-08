// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package local_test

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/seekparquet/go/source/local"
)

// TestFIFOIsRefusedAtOnce: opening a FIFO put where a file should be does
// not wait for a writer; it is refused as not a regular file.
func TestFIFOIsRefusedAtOnce(t *testing.T) {
	root := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(root, "manifest.json"), 0o644); err != nil {
		t.Skip(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := local.NewDir(root, nil).Open(context.Background(), "manifest.json")
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, local.ErrNotRegular) {
			t.Fatalf("FIFO: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("opening a FIFO blocked")
	}
}
