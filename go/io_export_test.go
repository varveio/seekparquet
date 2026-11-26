// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import "context"

// SetTestReadConcurrency selects a private reader limit before any operations
// start, for external tests comparing deterministic ordered storage traces.
func SetTestReadConcurrency(d *Dataset, n int) {
	d.r.concurrency = n
}

// ListPage is listPage, for external tests.
func ListPage(ctx context.Context, d *Dataset, q ListQuery, st *ReadStats) ([]ListEntry, *Cursor, error) {
	return listPage(ctx, d, q, st)
}

// PrefixCounts is prefixCounts, for external tests.
func PrefixCounts(ctx context.Context, d *Dataset, entries []ListEntry) ([]int64, error) {
	return prefixCounts(ctx, d, entries)
}

// ScanValues is plan.scanValues, for external tests.
func ScanValues(ctx context.Context, plan *Plan, st *ReadStats, fn func(*ValueBatch) error) error {
	return plan.scanValues(ctx, st, fn)
}

// SelectRows is plan.selectRows, for external tests.
func SelectRows(plan *Plan, spans []RowSpan) (*Plan, error) {
	return plan.selectRows(spans)
}
