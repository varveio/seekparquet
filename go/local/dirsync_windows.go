// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package local

// dirSync is false on Windows: Go has no directory fsync there (a
// directory handle cannot be flushed). A process crash loses nothing, since
// the system cache survives it; after a power loss or a system crash, NTFS
// replays its metadata log to a consistent state, which need not include
// the latest creations and renames even when the files' data was synced.
const dirSync = false
