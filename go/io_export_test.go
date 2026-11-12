// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

// SetTestReadConcurrency selects a private reader limit before any operations
// start, for external tests comparing deterministic ordered storage traces.
func SetTestReadConcurrency(d *Dataset, n int) {
	d.r.concurrency = n
}
