// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import "fmt"

// Limits bounds the resources a dataset can make the reader use (§11).
// Every limit is checked against what the files declare before anything
// is allocated for it, and exceeding one fails the operation with an error
// wrapping ErrLimitExceeded: a policy of the caller, not a refusal of the
// data. A zero field takes its default.
type Limits struct {
	// ManifestBytes bounds the manifest's size. Default 64 MiB.
	ManifestBytes int64
	// Parts bounds the number of parts in a manifest. Default 1,000,000.
	Parts int
	// FooterBytes bounds the encoded size of one part's footer. Default
	// 16 MiB.
	FooterBytes int64
	// PageIndexBytes bounds the encoded size of one key column chunk's
	// ColumnIndex, and of its OffsetIndex. Default 16 MiB.
	PageIndexBytes int64
	// MetadataMemory bounds the estimated decoded size of one footer, page
	// index or page header. Default 256 MiB.
	MetadataMemory int64
	// PageBytes bounds a page's compressed and uncompressed sizes,
	// dictionary pages included. Default 64 MiB.
	PageBytes int64
	// PageValues bounds the number of values in one page or dictionary.
	// Default 16,777,216.
	PageValues int64
	// IndexMemory bounds the memory one Reader retains for key page
	// indexes, which it keeps once loaded. Default 1 GiB.
	IndexMemory int64
}

// withDefaults returns l with its zero fields set to their defaults.
func (l Limits) withDefaults() Limits {
	set := func(v *int64, d int64) {
		if *v <= 0 {
			*v = d
		}
	}
	set(&l.ManifestBytes, 64<<20)
	set(&l.FooterBytes, 16<<20)
	set(&l.PageIndexBytes, 16<<20)
	set(&l.MetadataMemory, 256<<20)
	set(&l.PageBytes, 64<<20)
	set(&l.PageValues, 1<<24)
	set(&l.IndexMemory, 1<<30)
	if l.Parts <= 0 {
		l.Parts = 1_000_000
	}
	return l
}

// exceeded returns an error wrapping ErrLimitExceeded.
func exceeded(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrLimitExceeded}, args...)...)
}
