// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"fmt"
	"strconv"
)

// Limits bounds the resources a dataset can make the reader use (§11).
// Every limit is checked against what the files declare before anything
// is allocated for it, and exceeding one fails the operation with an error
// wrapping ErrLimitExceeded: a policy of the caller, not a refusal of the
// data, reported as a *LimitError. OffsetIndexMemory and
// PageIndexCacheMemory are the exceptions: they bound caches, and exceeding
// them never fails an operation. A zero field
// takes its default; a negative field is an invalid request when the
// dataset is opened. Within a page, the counts and
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
	// value page also holds a parquet.Value per value (24 bytes on 64-bit
	// platforms), which PageMemory bounds. Key pages are decoded into their
	// bytes with a 4-byte offset or dictionary index per key.
	PageBytes int64
	// PageValues bounds the number of values in one page or dictionary.
	// Default 16,777,216. Each request retains projected data-page scratch
	// at its high-water capacity until the call ends. Besides PageBytes,
	// header buffers can retain up to headerBytes (16 MiB, bounded by
	// MetadataMemory); indexed raw extents include header and payload.
	// IndexMemory does not account for any of it.
	PageValues int64
	// PageMemory bounds the memory decoding one page holds, data and
	// dictionary pages alike, key pages included, estimated from the page's
	// header before anything is allocated for its values (pageMemory): 24
	// bytes per value for its parquet.Value (64-bit platforms), twice that
	// for an optional column (its non-null values, then every row), plus
	// the bytes its decoded values hold, at most PageBytes. A key page is
	// estimated the same way, though it holds a 4-byte offset or index per
	// key instead of a parquet.Value: the estimate is conservative for it.
	// A page over it fails with an error wrapping ErrLimitExceeded, naming
	// PageMemory. It bounds one page's decoding, not a call's total memory
	// or the high-water scratch a call retains.
	//
	// Default: the most PageValues and PageBytes allow, 2 × 24 × PageValues
	// + PageBytes, so that the default refuses no page those limits admit:
	// 832 MiB at their defaults. Set it lower to bound decoding memory; a
	// page within PageValues and PageBytes may then be refused. The bound is
	// per page and per request: concurrent requests, and the shards of a
	// split plan scanned at once, each decode their own pages, so their
	// memory adds up; bound concurrency as well.
	PageMemory int64
	// IndexMemory bounds the memory one Dataset retains for key page
	// indexes and decoded row-group first-key bytes, which it keeps once
	// loaded. This accounts for page entries and their bound bytes, plus
	// first-key bytes; not allocator overhead, footers, transient decoding
	// buffers or query results. Default 1 GiB.
	IndexMemory int64
	// OffsetIndexMemory bounds the memory one Dataset retains for the
	// checked page locations of projected column chunks' OffsetIndexes,
	// which it keeps once loaded so that later requests do not read and
	// decode them again. It is a cache, not a limit on reads: past it,
	// each request loads the locations it needs and drops them, as if no
	// cache were kept, and never with ErrLimitExceeded. It counts page
	// entries plus a fixed per-chunk overhead, apart from IndexMemory. It
	// does not count allocator overhead, the locations a request loads
	// without keeping (including concurrent first loads of one chunk, all
	// but one of which are dropped), or the cache's slots: every opened
	// part allocates about 24 bytes per physical row group and 8 per
	// column chunk on 64-bit platforms when it is checked, whatever the
	// budget and even if no request projects a value column. Default
	// 256 MiB.
	OffsetIndexMemory int64
	// PageIndexCacheMemory bounds the raw page-index bytes one Dataset
	// keeps from the ends of the parts it checked
	// (ReadPolicy.MetadataPrefetchBytes), to serve later ColumnIndex and
	// OffsetIndex loads without a read. It is a cache, and only of those
	// raw bytes: past it, the part used least recently gives up its bytes,
	// or a part's are not kept, and later loads read the indexes from the
	// Source; it never fails an operation. Footers, which every checked
	// part keeps, decoded key indexes (IndexMemory) and page locations
	// (OffsetIndexMemory) are outside it: no aggregate bound on a
	// Dataset's metadata exists yet. Default 128 MiB.
	PageIndexCacheMemory int64
	// GetRows bounds the rows Dataset.Get returns: a group of more rows
	// fails with a *LimitError naming GetRows, and is read by paging
	// KeySpan(key) with Rows instead. It bounds a convenience call's row
	// count, not its memory: a few rows can hold large keys and values.
	// Default 1,000.
	GetRows int
}

// check rejects a negative limit, which would otherwise take its default
// unnoticed.
func (l *Limits) check() error {
	for _, f := range []struct {
		name  LimitName // or the name of a cache field, which no LimitError names
		value int64
	}{
		{LimitManifestBytes, l.ManifestBytes}, {LimitParts, int64(l.Parts)}, {LimitFooterBytes, l.FooterBytes},
		{LimitPageIndexBytes, l.PageIndexBytes}, {LimitMetadataMemory, l.MetadataMemory}, {LimitPageBytes, l.PageBytes},
		{LimitPageValues, l.PageValues}, {LimitPageMemory, l.PageMemory}, {LimitIndexMemory, l.IndexMemory},
		{LimitOffsetIndexMemory, l.OffsetIndexMemory}, {LimitGetRows, int64(l.GetRows)}, {"PageIndexCacheMemory", l.PageIndexCacheMemory},
	} {
		if f.value < 0 {
			return invalidRequest("Limits.%s is negative (%d)", f.name, f.value)
		}
	}
	return nil
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
	set(&l.PageMemory, worstPageMemory(l.PageValues, l.PageBytes))
	set(&l.IndexMemory, 1<<30)
	set(&l.OffsetIndexMemory, 256<<20)
	set(&l.PageIndexCacheMemory, 128<<20)
	if l.Parts <= 0 {
		l.Parts = 1_000_000
	}
	if l.GetRows <= 0 {
		l.GetRows = 1000
	}
	return l
}

// LimitName names the limit a *LimitError reports: a field of Limits, or
// LimitDatasetRows.
type LimitName string

// The limits.
const (
	LimitManifestBytes     LimitName = "ManifestBytes"
	LimitParts             LimitName = "Parts"
	LimitFooterBytes       LimitName = "FooterBytes"
	LimitPageIndexBytes    LimitName = "PageIndexBytes"
	LimitMetadataMemory    LimitName = "MetadataMemory"
	LimitPageBytes         LimitName = "PageBytes"
	LimitPageValues        LimitName = "PageValues"
	LimitPageMemory        LimitName = "PageMemory"
	LimitIndexMemory       LimitName = "IndexMemory"
	LimitOffsetIndexMemory LimitName = "OffsetIndexMemory"
	LimitGetRows           LimitName = "GetRows"
	// LimitDatasetRows is the reader's capacity of signed 64-bit dataset
	// ordinals: a total row count above 2^63-1.
	LimitDatasetRows LimitName = "DatasetRows"
)

// LimitError reports input or a result beyond one of the Limits (§11), or
// beyond the reader's dataset row capacity. It wraps ErrLimitExceeded: a
// policy of the caller or a capacity of this reader, not a refusal of the
// data.
type LimitError struct {
	Limit LimitName
	// Value is the amount the input declares or the read found, -1 when it
	// is known only to exceed Max.
	Value int64
	// Max is the limit in force.
	Max int64
	// Detail says where, for people.
	Detail string
}

func (e *LimitError) Error() string {
	s := "seekparquet: limit exceeded: " + string(e.Limit)
	if e.Value >= 0 {
		s += " " + strconv.FormatInt(e.Value, 10) + " >"
	}
	s += " " + strconv.FormatInt(e.Max, 10)
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// Unwrap returns ErrLimitExceeded.
func (e *LimitError) Unwrap() error { return ErrLimitExceeded }

// exceeded returns a *LimitError.
func exceeded(limit LimitName, value, max int64, format string, args ...any) error {
	return &LimitError{Limit: limit, Value: value, Max: max, Detail: fmt.Sprintf(format, args...)}
}

// headerBytes is the most bytes a page header is read in: 16 MiB, far
// beyond any real header, or MetadataMemory when that is less.
func (l *Limits) headerBytes() int64 { return min(16<<20, l.MetadataMemory) }

// The page memory estimate (Limits.PageMemory), in bytes, from a page header.
const (
	// valueBytes is the size of a parquet.Value on 64-bit platforms
	// (unsafe.Sizeof, checked by a test): a page decodes to one per value.
	valueBytes = 24
	// optionalValueSlices is the number of parquet.Value slices a page of an
	// optional column holds at once: its non-null values, then one per row.
	optionalValueSlices = 2
)

// worstPageMemory is the most memory pageMemory estimates for a page within
// pageValues and pageBytes: an optional column's two Value slices for every
// value, and decoded values of pageBytes. It saturates instead of
// overflowing.
func worstPageMemory(pageValues, pageBytes int64) int64 {
	per := int64(optionalValueSlices * valueBytes)
	if pageValues > (maxInt64-pageBytes)/per {
		return maxInt64
	}
	return pageValues*per + pageBytes
}
