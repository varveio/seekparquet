// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import "sync"

// ReadStats counts the work of one operation: Dataset.Rows, Get, Scan or
// List, Dataset.Plan, Plan.Split or Plan.Scan. Pass a *ReadStats as the
// operation's statistics argument to receive it: it is reset when the
// operation starts and filled as it runs. The counts are exact for the
// operation and are filled whether it succeeds or fails, up to the point it
// stopped. Add sums the counts of several operations.
//
// They are diagnostics of this reader's access path, not part of the
// SeekParquet format contract: another reader, or a later version of this
// one, may do the same read with different counts. All fields are plain
// integers; a ReadStats must not be shared between concurrent requests.
//
// The manifest read when the Dataset was opened is not counted here: pass
// OpenOptions.OpenStats for it. Work shared
// with earlier requests is counted only where a field says so: a part
// checked by an earlier request, or by Verify, is not checked again, a key
// page index loaded by an earlier request is reused, not loaded, a
// projected column chunk's OffsetIndex kept by an earlier request
// (Limits.OffsetIndexMemory) is not read again, and a decoded first key is
// reused without decoding.
type ReadStats struct {
	// ListIndexSkips counts skips answered by the optional slash index.
	ListIndexSkips int64
	// Declines after an index was built: another delimiter, an omitted group,
	// a dense page, or a run that cannot establish an exact successor.
	ListIndexDelimiter, ListIndexMissingGroup, ListIndexDensePage, ListIndexUnsafeRun int64
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
	// IndexLoads counts successful key-index loads not satisfied by an
	// already decoded index, including loads from retained raw metadata
	// and successful build-only transient loads;
	// RowGroupsOpened-IndexLoads were reuses, including an index a concurrent
	// request published first. An
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
	// SourceReads counts non-empty logical part ranges submitted to
	// storage through ReadAt or ReadRanges. SourceBytes counts their
	// requested bytes. Neither counts physical requests or wire bytes.
	// Manifest reads are excluded.
	SourceReads, SourceBytes int64
	// ReadAheadReads is the number of key pages a listing read ahead
	// (ListQuery.ReadAhead), each in one read counted in SourceReads and
	// SourceBytes; ReadAheadUnused and ReadAheadUnusedBytes are the pages
	// of them, and their bytes, that it did not use: wrong predictions,
	// pages past where the listing stopped, and reads that failed (the
	// page is then read again).
	ReadAheadReads, ReadAheadUnused, ReadAheadUnusedBytes int64

	// SourceBatches counts calls actually made to RangeReader.ReadRanges.
	// ScalarFallbackReads counts non-empty ReadAt calls actually made
	// to execute a logical batch without RangeReader.
	SourceBatches, ScalarFallbackReads int64

	// The physical counters come from a Source that reports its requests
	// (IOReporter): the requests it made for the operation, opens and
	// metadata requests included, their payload bytes, the retries among
	// them and the attempts that failed. They are exact only when
	// UnreportedIOCalls is zero.
	PhysicalRequests, PhysicalBytes, PhysicalRetries, PhysicalFailures int64
	// PlannedRanges, PlannedRequests and PlannedOverReadBytes are what a
	// reporting source said it planned for the batches it was given
	// (IOObserver.BatchPlanned); do not subtract them to infer requests.
	PlannedRanges, PlannedRequests, PlannedOverReadBytes int64
	// ByteCacheHitBytes are the bytes a reporting source served from its
	// own byte cache.
	ByteCacheHitBytes int64
	// MetadataHitBytes counts raw page-index bytes supplied from retained
	// metadata instead of storage, counted on each use.
	MetadataHitBytes int64
	// RequestMemoHitBytes counts the bytes of complete key-page extents
	// reused from the request's bounded memo instead of read again: a
	// routing probe's page that the walk reached, a cursor's page, a page
	// Verify fetched together with others. Extents not admitted to the
	// memo may be read again; its capacity and admission policy are
	// implementation details.
	RequestMemoHitBytes int64
	// UnreportedIOCalls is the number of storage calls (ReadAt,
	// ReadRanges, Open, OpenSized, ReadManifest) whose physical requests
	// the physical counters leave out: made on a source or file that does
	// not report them, or, for a file that does, by an operation that
	// began before the Dataset knew any of its files reported (the first
	// operation to open such a part, when the Source itself does not).
	UnreportedIOCalls int64
	// PeakPhysicalRequests is the most physical requests a reporting
	// source had in flight at once for the operation.
	PeakPhysicalRequests int64
}

// takeStats zeroes st for a new operation. When the caller asked for no
// statistics (st is nil) it returns a zeroed throwaway from spareStats as
// both the statistics and spare, so that counting never tests for nil; the
// operation hands spare back with dropStats once it has ended and nothing
// writes the throwaway any more. A throwaway is taken from a pool, not
// allocated, because it cannot live on the stack: the operation's scans,
// chunks and storage readers keep a pointer to it.
func takeStats(st *ReadStats) (_, spare *ReadStats) {
	if st == nil {
		spare = spareStats.Get().(*ReadStats)
		*spare = ReadStats{}
		return spare, spare
	}
	*st = ReadStats{}
	return st, nil
}

// dropStats returns the throwaway takeStats handed out, if any.
func dropStats(spare *ReadStats) {
	if spare != nil {
		spareStats.Put(spare)
	}
}

var spareStats = sync.Pool{New: func() any { return new(ReadStats) }}

// Add sums counters and takes the maximum of each peak: the work of several
// operations, such as a plan, its split and the scans of its shards.
// The resulting peak is the largest constituent operation peak,
// not the peak of those operations running concurrently.
func (st *ReadStats) Add(o ReadStats) {
	st.ListIndexSkips += o.ListIndexSkips
	st.ListIndexDelimiter += o.ListIndexDelimiter
	st.ListIndexMissingGroup += o.ListIndexMissingGroup
	st.ListIndexDensePage += o.ListIndexDensePage
	st.ListIndexUnsafeRun += o.ListIndexUnsafeRun
	st.PartsOpened += o.PartsOpened
	st.KeyPagesDecoded += o.KeyPagesDecoded
	st.ValuePagesDecoded += o.ValuePagesDecoded
	st.KeysDecoded += o.KeysDecoded
	st.RowGroupsOpened += o.RowGroupsOpened
	st.IndexLoads += o.IndexLoads
	st.PageIndexSeeks += o.PageIndexSeeks
	st.PrefixSkips += o.PrefixSkips
	st.GroupsSkipped += o.GroupsSkipped
	st.SourceReads += o.SourceReads
	st.SourceBytes += o.SourceBytes
	st.ReadAheadReads += o.ReadAheadReads
	st.ReadAheadUnused += o.ReadAheadUnused
	st.ReadAheadUnusedBytes += o.ReadAheadUnusedBytes
	st.SourceBatches += o.SourceBatches
	st.ScalarFallbackReads += o.ScalarFallbackReads
	st.PhysicalRequests += o.PhysicalRequests
	st.PhysicalBytes += o.PhysicalBytes
	st.PhysicalRetries += o.PhysicalRetries
	st.PhysicalFailures += o.PhysicalFailures
	st.PlannedRanges += o.PlannedRanges
	st.PlannedRequests += o.PlannedRequests
	st.PlannedOverReadBytes += o.PlannedOverReadBytes
	st.ByteCacheHitBytes += o.ByteCacheHitBytes
	st.MetadataHitBytes += o.MetadataHitBytes
	st.RequestMemoHitBytes += o.RequestMemoHitBytes
	st.UnreportedIOCalls += o.UnreportedIOCalls
	// A peak of several operations is the largest of theirs, not a
	// measurement of their concurrent peak.
	st.PeakPhysicalRequests = max(st.PeakPhysicalRequests, o.PeakPhysicalRequests)
}
