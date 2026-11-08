// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package local

import (
	"fmt"
	"os"
	"syscall"
)

// DefaultMaxOpenFiles is half the process's soft descriptor limit, leaving
// the other half to the rest of the program, at most 4,096 (more open
// parts buy little: a read touches one part at a time) and at least one.
func DefaultMaxOpenFiles() int {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return 32
	}
	return int(min(max(uint64(limit.Cur)/2, 1), 4096))
}

// openFlags are the flags a file is opened with: read-only and
// non-blocking, so opening a FIFO or a device put at a part's path returns
// at once instead of waiting for a writer; the caller then requires a
// regular file and calls blocking.
const openFlags = os.O_RDONLY | syscall.O_NONBLOCK

// OpenFile opens path read-only without blocking on a special file,
// requires a regular file (ErrNotRegular) and makes its descriptor
// blocking again.
func OpenFile(path string) (*os.File, error) {
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

// blocking requires f to be a regular file and clears the non-blocking
// flag openFlags set on it.
func blocking(f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s: %w", f.Name(), ErrNotRegular)
	}
	raw, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var setErr error
	if err := raw.Control(func(fd uintptr) { setErr = syscall.SetNonblock(int(fd), false) }); err != nil {
		return err
	}
	return setErr
}
