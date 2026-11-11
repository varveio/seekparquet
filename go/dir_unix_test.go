// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package seekparquet

import (
	"os"
	"syscall"
	"testing"
	"time"
)

// TestDirFIFO: a FIFO at a part's path is refused as a missing part at
// once, instead of blocking the open until a writer appears.
func TestDirFIFO(t *testing.T) {
	dir := write(t, twoParts())
	must(t, os.Remove(partFile(dir, 1)))
	must(t, syscall.Mkfifo(partFile(dir, 1), 0o644))
	done := make(chan error, 1)
	go func() { done <- openErr(dir, OpenOptions{}) }()
	select {
	case err := <-done:
		if RefusalCodeOf(err) != CodeMissingPart {
			t.Fatalf("got %v, want missing_part", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("opening a FIFO blocked")
	}
}
