// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import "fmt"

// Limits bounds the resources a dataset can make the reader use (§11).
// Every limit is checked against what the files declare before anything
// is allocated for it, and exceeding one fails the operation with an error
// wrapping ErrLimitExceeded: a policy of the caller, not a refusal of the
// data. A zero field takes its default. Within a page, the counts and
// lengths its encodings declare are checked against the page header before
// decoding, and a page that decompresses to more or fewer bytes than its
// header declares is refused, so a small page cannot allocate beyond them.
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
	// PageBytes bounds a page's compressed and uncompressed sizes, and the
	// size of its decoded values' bytes, dictionary pages included. Default
	// 64 MiB. It does not bound the memory of decoding a page: each decoded
	// value is also held in a parquet.Value (24 bytes on 64-bit platforms),
	// which PageValues bounds.
	PageBytes int64
	// PageValues bounds the number of values in one page or dictionary.
	// Default 16,777,216. It also bounds the parquet.Value slices a page
	// decodes to, besides its PageBytes: up to 24 bytes per value on 64-bit
	// platforms, twice that for an optional column (its non-null values,
	// then every row), so 384 to 768 MiB for a page at the defaults. Each
	// request retains projected data-page scratch at its high-water capacity
	// until the call ends. Concurrent requests and Scan shards add up. Lower
	// this limit and bound concurrency to reduce transient memory; IndexMemory
	// does not account for it.
	PageValues int64
	// IndexMemory bounds the memory one Dataset retains for key page
	// indexes and decoded row-group first-key bytes, which it keeps once
	// loaded. This accounts for page entries and their bound bytes, plus
	// first-key bytes; not allocator overhead, footers, transient decoding
	// buffers or query results. Default 1 GiB.
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

// headerBytes is the most bytes a page header is read in: 16 MiB, far
// beyond any real header, or MetadataMemory when that is less.
func (l *Limits) headerBytes() int64 { return min(16<<20, l.MetadataMemory) }
