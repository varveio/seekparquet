// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package seekparquet

import "os"

// openFlags open a file read-only; opening does not block on a special
// file here.
const openFlags = os.O_RDONLY
