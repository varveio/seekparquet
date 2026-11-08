// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package local

import (
	"fmt"
	"os"
)

// DefaultMaxOpenFiles is 4,096 where the process has no descriptor limit
// to derive it from (Windows handles are not bounded that way).
func DefaultMaxOpenFiles() int { return 4096 }

// openFlags are the flags a file is opened with: read-only (an open does
// not block on a special file here).
const openFlags = os.O_RDONLY

// openFile opens path read-only and requires a regular file
// (ErrNotRegular).
func openFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, openFlags, 0)
	if err != nil {
		return nil, err
	}
	if err := blocking(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// blocking requires f to be a regular file.
func blocking(f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s: %w", f.Name(), ErrNotRegular)
	}
	return nil
}
