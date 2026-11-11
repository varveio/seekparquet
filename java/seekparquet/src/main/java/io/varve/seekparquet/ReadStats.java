// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * The work of one range or list request, one {@link Dataset#planRanges planning} ({@link
 * RangePlan#stats()}), the placing of one shard's end by {@link RangePlan#split split}, or one {@link
 * RangePlan#scan scan}. Pass a consumer to the request to receive
 * it: the counts are exact for the request and are delivered whether it succeeds or fails, up to the
 * point it stopped.
 *
 * <p>They are diagnostics of this reader's access path, not part of the SeekParquet format contract:
 * another reader, or a later version of this one, may do the same read with different counts. The
 * Go reader's {@code ReadStats} has the same fields and counting rules, but its access path differs
 * (its listing skips whole row groups, for one), so the two can count the same request differently.
 *
 * <p>The manifest read when the dataset was opened is not counted. Work shared with earlier requests
 * is counted only where a component says so: a part checked by an earlier request, or by {@link
 * Dataset#verify()}, is not checked again, a key page index loaded by an earlier request is reused,
 * not loaded, a projected column chunk's OffsetIndex kept by an earlier request ({@link
 * Limits#maxOffsetIndexMemoryBytes()}) is not read again, and a decoded first key is reused without
 * decoding.
 *
 * @param partsOpened the parts the request was the first to touch and opened from storage to check
 *     (CHECK-1), whether or not the check passed; the footer reads are in {@code sourceReads}
 * @param keyPagesDecoded the key column data pages the request decoded (a page whose values do not
 *     decode is not counted, one that then fails a key check is): pages walked, probed for a row
 *     group's first key, or positioned on by a cursor; a page decoded twice counts twice
 * @param valuePagesDecoded the data pages of projected columns the request decoded, whichever column
 *     they belong to; dictionary pages are not counted
 * @param keysDecoded the keys decoded from key pages: the total number of rows in {@code
 *     keyPagesDecoded}
 * @param rowGroupsOpened the times the request took a row group's key page index, to probe its bounds
 *     or walk its pages: loading it, or reusing one an earlier request, or an earlier step of this
 *     one, had loaded
 * @param indexLoads how many of those read the key page index from storage, including successful build-only transient loads; {@code
 *     rowGroupsOpened - indexLoads} were reuses, including an index a concurrent request published
 *     first; an index read but refused by {@link Limits#maxIndexMemoryBytes()} is in neither count
 * @param pageIndexSeeks the moves within a row group's key pages, through its ColumnIndex or
 *     OffsetIndex, that skipped at least one page
 * @param prefixSkips the times a listing, having emitted a common prefix, resumed at the prefix's
 *     successor instead of reading the keys under it
 * @param groupsSkipped the row groups a listing emitted as a common prefix without reading them; this
 *     reader has no such shortcut, so it is always zero (the Go reader's is not)
 * @param listIndexSkips skips answered by the optional slash index
 * @param listIndexDelimiter declines because the delimiter is not slash
 * @param listIndexMissingGroup declines because a group was omitted
 * @param listIndexDensePage declines because a dense page must be decoded
 * @param listIndexUnsafeRun declines because the run cannot establish an exact successor
 * @param sourceReads the {@link io.varve.seekparquet.storage.PartSource#read} calls the request made
 *     on part files, footer, page index and page header reads included
 * @param sourceBytes the total length of those reads, the bytes requested
 * @param readAheadReads the key pages a listing read ahead ({@link ListQuery#readAhead()}), each in
 *     one read that is also counted in {@code sourceReads} and {@code sourceBytes}
 * @param readAheadUnused how many of those the listing did not use: wrong predictions, pages past
 *     where the listing stopped, and reads that failed (the page is then read again)
 * @param readAheadUnusedBytes the bytes of the pages read ahead and not used
 */
public record ReadStats(
        long partsOpened,
        long keyPagesDecoded,
        long valuePagesDecoded,
        long keysDecoded,
        long rowGroupsOpened,
        long indexLoads,
        long pageIndexSeeks,
        long prefixSkips,
        long groupsSkipped,
        long sourceReads,
        long sourceBytes,
        long listIndexSkips,
        long listIndexDelimiter,
        long listIndexMissingGroup,
        long listIndexDensePage,
        long listIndexUnsafeRun,
        long readAheadReads,
        long readAheadUnused,
        long readAheadUnusedBytes) {

    /**
     * Constructs counts without read-ahead counts.
     * @param partsOpened parts opened
     * @param keyPagesDecoded key pages decoded
     * @param valuePagesDecoded value pages decoded
     * @param keysDecoded keys decoded
     * @param rowGroupsOpened row groups opened
     * @param indexLoads indexes loaded
     * @param pageIndexSeeks page index seeks
     * @param prefixSkips prefix skips
     * @param groupsSkipped groups skipped
     * @param sourceReads source reads
     * @param sourceBytes source bytes
     * @param listIndexSkips list index skips
     * @param listIndexDelimiter list index delimiter declines
     * @param listIndexMissingGroup list index missing-group declines
     * @param listIndexDensePage list index dense-page declines
     * @param listIndexUnsafeRun list index unsafe-run declines
     */
    public ReadStats(long partsOpened, long keyPagesDecoded, long valuePagesDecoded, long keysDecoded,
            long rowGroupsOpened, long indexLoads, long pageIndexSeeks, long prefixSkips,
            long groupsSkipped, long sourceReads, long sourceBytes, long listIndexSkips, long listIndexDelimiter,
            long listIndexMissingGroup, long listIndexDensePage, long listIndexUnsafeRun) {
        this(partsOpened, keyPagesDecoded, valuePagesDecoded, keysDecoded, rowGroupsOpened, indexLoads,
                pageIndexSeeks, prefixSkips, groupsSkipped, sourceReads, sourceBytes, listIndexSkips, listIndexDelimiter,
                listIndexMissingGroup, listIndexDensePage, listIndexUnsafeRun, 0, 0, 0);
    }

    /**
     * Constructs counts from the original access paths, with zero optional-index counts.
     * @param partsOpened parts opened
     * @param keyPagesDecoded key pages decoded
     * @param valuePagesDecoded value pages decoded
     * @param keysDecoded keys decoded
     * @param rowGroupsOpened row groups opened
     * @param indexLoads indexes loaded
     * @param pageIndexSeeks page index seeks
     * @param prefixSkips prefix skips
     * @param groupsSkipped groups skipped
     * @param sourceReads source reads
     * @param sourceBytes source bytes
     */
    public ReadStats(long partsOpened, long keyPagesDecoded, long valuePagesDecoded, long keysDecoded,
            long rowGroupsOpened, long indexLoads, long pageIndexSeeks, long prefixSkips,
            long groupsSkipped, long sourceReads, long sourceBytes) {
        this(partsOpened, keyPagesDecoded, valuePagesDecoded, keysDecoded, rowGroupsOpened, indexLoads,
                pageIndexSeeks, prefixSkips, groupsSkipped, sourceReads, sourceBytes, 0, 0, 0, 0, 0);
    }

    /** Zero counts: a request that failed before it read. */
    static final ReadStats NONE = new ReadStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    /** {@return the counts beyond {@code o}, for a request that reports the reads of its steps apart} */
    ReadStats minus(ReadStats o) {
        return new ReadStats(partsOpened - o.partsOpened, keyPagesDecoded - o.keyPagesDecoded, valuePagesDecoded - o.valuePagesDecoded,
                keysDecoded - o.keysDecoded, rowGroupsOpened - o.rowGroupsOpened, indexLoads - o.indexLoads,
                pageIndexSeeks - o.pageIndexSeeks, prefixSkips - o.prefixSkips, groupsSkipped - o.groupsSkipped,
                sourceReads - o.sourceReads, sourceBytes - o.sourceBytes, listIndexSkips - o.listIndexSkips,
                listIndexDelimiter - o.listIndexDelimiter, listIndexMissingGroup - o.listIndexMissingGroup,
                listIndexDensePage - o.listIndexDensePage, listIndexUnsafeRun - o.listIndexUnsafeRun,
                readAheadReads - o.readAheadReads, readAheadUnused - o.readAheadUnused,
                readAheadUnusedBytes - o.readAheadUnusedBytes);
    }

    /** The counts of a request in progress. Confined to the request's thread. */
    static final class Counter {
        long listIndexSkips, listIndexDelimiter, listIndexMissingGroup, listIndexDensePage, listIndexUnsafeRun;
        long partsOpened;
        long keyPagesDecoded;
        long valuePagesDecoded;
        long keysDecoded;
        long rowGroupsOpened;
        long indexLoads;
        long pageIndexSeeks;
        long prefixSkips;
        long sourceReads;
        long sourceBytes;
        long readAheadReads, readAheadUnused, readAheadUnusedBytes;

        ReadStats snapshot() {
            return new ReadStats(partsOpened, keyPagesDecoded, valuePagesDecoded, keysDecoded, rowGroupsOpened, indexLoads,
                    pageIndexSeeks, prefixSkips, 0, sourceReads, sourceBytes, listIndexSkips, listIndexDelimiter,
                    listIndexMissingGroup, listIndexDensePage, listIndexUnsafeRun, readAheadReads, readAheadUnused,
                    readAheadUnusedBytes);
        }
    }
}
