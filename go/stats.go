// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

// ReadStats counts the work of one Range or List request, one PlanRanges
// (RangePlan.Stats) or one RangePlan.Scan. Set it as the request's Stats
// field, or pass it to Scan, to receive it. The counts are exact for the request
// and are filled whether it succeeds or fails, up to the point it stopped.
//
// They are diagnostics of this reader's access path, not part of the
// SeekParquet format contract: another reader, or a later version of this
// one, may do the same read with different counts. All fields are plain
// integers; a ReadStats must not be shared between concurrent requests.
//
// The manifest read when the Dataset was opened is not counted. Work shared
// with earlier requests is counted only where a field says so: a part
// checked by an earlier request, or by Verify, is not checked again, a key
// page index loaded by an earlier request is reused, not loaded, and a
// decoded first key is reused without decoding.
type ReadStats struct {
	// PartsOpened is the number of parts the request was the first to
	// touch and opened from the Source to check (CHECK-1), whether or not
	// the check passed. The footer reads are in SourceReads.
	PartsOpened int64
	// KeyPagesDecoded is the number of key column data pages the request
	// decoded successfully (a page whose values do not decode is not
	// counted, one that then fails a key check is): pages walked, probed for a row group's first key, or
	// positioned on by a cursor. A page decoded twice counts twice.
	KeyPagesDecoded int64
	// ValuePagesDecoded is the number of data pages of projected columns
	// the request decoded, whichever column they belong to.
	ValuePagesDecoded int64
	// KeysDecoded is the number of keys decoded from key pages: the total
	// number of rows in KeyPagesDecoded.
	KeysDecoded int64
	// RowGroupsOpened is the number of times the request opened a row
	// group's key pages: it loaded the group's key page index or reused one
	// an earlier request, or an earlier step of this one, had loaded.
	RowGroupsOpened int64
	// IndexLoads is the number of those openings that read the key page
	// index from the Source and retained it; RowGroupsOpened-IndexLoads were
	// reuses, including an index a concurrent request published first. An
	// index read but refused (Limits.IndexMemory) is in neither count.
	IndexLoads int64
	// PageIndexSeeks is the number of moves within a row group's key pages,
	// through its ColumnIndex or OffsetIndex, that skipped at least one
	// page.
	PageIndexSeeks int64
	// PrefixSkips is the number of times a listing, having emitted a
	// common prefix, resumed at the prefix's successor instead of reading
	// the keys under it.
	PrefixSkips int64
	// GroupsSkipped is the number of row groups a listing emitted as a
	// common prefix without reading them, because their first key and the
	// next group's first key share it.
	GroupsSkipped int64
	// SourceReads is the number of ReadAt calls the request made on part
	// files, and SourceBytes the total of their lengths, the bytes
	// requested. They include the footer, page index and page header reads
	// made on behalf of the request.
	SourceReads, SourceBytes int64
}

// resetStats zeroes st for a new request, or returns a throwaway one when
// the caller asked for none, so that counting never tests for nil.
func resetStats(st *ReadStats) *ReadStats {
	if st == nil {
		return new(ReadStats)
	}
	*st = ReadStats{}
	return st
}
