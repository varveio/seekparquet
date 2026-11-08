// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package seekparquet

import (
	"os"
	"syscall"
)

// openFlags open a file read-only and without blocking, so that opening a
// FIFO or a device put at a part's path returns at once instead of
// waiting for a writer; dirSource then requires a regular file, for which
// O_NONBLOCK has no effect.
const openFlags = os.O_RDONLY | syscall.O_NONBLOCK
