// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * Limits on the sizes a reader accepts from the data before it allocates (spec §11). A size above a
 * limit fails with {@link LimitExceededException}, which is not a refusal: the data may be valid.
 * {@code maxOffsetIndexMemoryBytes} is the exception: it bounds a cache, and nothing fails past it.
 *
 * <p>{@code maxPageBytes} bounds a page's bytes, not the memory of decoding it: a page's values are
 * also held in primitive, offset/length or reference arrays of one slot per value, which
 * {@code maxPageValues} bounds. Keys and dictionary entries can additionally hold one byte-array
 * object per value. Each request retains its projected-column scratch at its high-water capacities
 * until the call ends, separately from dictionary storage and codec state. Concurrent requests add
 * up; lower {@code maxPageValues} and caller concurrency to bound decoding memory. The shared index
 * budget does not account for request scratch or owning results.
 * No limit bounds the part sources and footers a dataset keeps open for the parts it has touched (see
 * {@link Dataset}).
 *
 * @param maxManifestBytes the largest {@code manifest.json}
 * @param maxParts the most parts in a manifest
 * @param maxFooterBytes the largest Parquet footer
 * @param maxIndexBytes the largest ColumnIndex or OffsetIndex of one column chunk
 * @param maxPageBytes the largest page, compressed or uncompressed, dictionary pages included
 * @param maxPageValues the most values in one page
 * @param maxResultEntries the most entries requested in one result
 * @param maxIndexMemoryBytes the dataset-wide retained decoded-index and exact-first-key budget
 * @param maxOffsetIndexMemoryBytes the dataset-wide budget for the checked page locations of projected
 *     column chunks' OffsetIndexes, which a dataset keeps once loaded so that later requests do not read
 *     and decode them again. It is a cache, not a limit on reads: past it, each request loads the
 *     locations it needs and drops them, as if no cache were kept. It bounds an accounting charge, not
 *     heap: 32 bytes per kept page plus 64 per kept chunk, as the Go reader's {@code
 *     Limits.OffsetIndexMemory} counts, apart from {@code maxIndexMemoryBytes}; JVM headers and
 *     alignment can exceed it. It does not count the slot array each opened part allocates, one
 *     reference per physical row group and leaf even for key-only requests or a 1-byte budget, nor
 *     transient Thrift data, duplicate loads by concurrent first uses, or the locations a request holds
 *     for itself past the budget. Default 256 MiB.
 */
public record Limits(
        int maxManifestBytes, int maxParts, int maxFooterBytes, int maxIndexBytes, int maxPageBytes, int maxPageValues, int maxResultEntries, long maxIndexMemoryBytes,
        long maxOffsetIndexMemoryBytes) {

    private static final int MIB = 1 << 20;

    /**
     * The defaults: a 64 MiB manifest, 1,000,000 parts, a 64 MiB footer, a 16 MiB page index, a
     * 256 MiB page, 4 Mi (4,194,304) values per page and 1,000,000 entries per result.
     */
    public static final Limits DEFAULT = new Limits(64 * MIB, 1_000_000, 64 * MIB, 16 * MIB, 256 * MIB, 4 * MIB, 1_000_000);

    /** Creates limits with a 256 MiB OffsetIndex location budget.
     * @param maxManifestBytes the largest manifest
     * @param maxParts the most parts
     * @param maxFooterBytes the largest footer
     * @param maxIndexBytes the largest page index
     * @param maxPageBytes the largest page
     * @param maxPageValues the most values in a page
     * @param maxResultEntries the largest request limit
     * @param maxIndexMemoryBytes the retained decoded-index and exact-first-key budget
     */
    public Limits(int maxManifestBytes, int maxParts, int maxFooterBytes, int maxIndexBytes, int maxPageBytes,
            int maxPageValues, int maxResultEntries, long maxIndexMemoryBytes) {
        this(maxManifestBytes, maxParts, maxFooterBytes, maxIndexBytes, maxPageBytes, maxPageValues, maxResultEntries, maxIndexMemoryBytes,
                256L * MIB);
    }

    /** Creates limits with 256 MiB retained-index and OffsetIndex location budgets.
     * @param maxManifestBytes the largest manifest
     * @param maxParts the most parts
     * @param maxFooterBytes the largest footer
     * @param maxIndexBytes the largest page index
     * @param maxPageBytes the largest page
     * @param maxPageValues the most values in a page
     * @param maxResultEntries the largest request limit
     */
    public Limits(int maxManifestBytes, int maxParts, int maxFooterBytes, int maxIndexBytes, int maxPageBytes,
            int maxPageValues, int maxResultEntries) {
        this(maxManifestBytes, maxParts, maxFooterBytes, maxIndexBytes, maxPageBytes, maxPageValues, maxResultEntries, 256L * MIB);
    }

    /** Returns these limits with a different dataset-wide retained-index budget.
     * @param bytes the budget, in bytes; positive
     * @return the new limits
     */
    public Limits withMaxIndexMemoryBytes(long bytes) {
        return new Limits(maxManifestBytes, maxParts, maxFooterBytes, maxIndexBytes, maxPageBytes, maxPageValues, maxResultEntries, bytes,
                maxOffsetIndexMemoryBytes);
    }

    /** Returns these limits with a different dataset-wide OffsetIndex location budget.
     * @param bytes the budget, in bytes; positive
     * @return the new limits
     */
    public Limits withMaxOffsetIndexMemoryBytes(long bytes) {
        return new Limits(maxManifestBytes, maxParts, maxFooterBytes, maxIndexBytes, maxPageBytes, maxPageValues, maxResultEntries,
                maxIndexMemoryBytes, bytes);
    }

    /** Creates limits with at most one million entries per result.
     * @param maxManifestBytes the largest manifest
     * @param maxParts the most parts
     * @param maxFooterBytes the largest footer
     * @param maxIndexBytes the largest page index
     * @param maxPageBytes the largest page
     * @param maxPageValues the most values in a page
     */
    public Limits(int maxManifestBytes, int maxParts, int maxFooterBytes, int maxIndexBytes, int maxPageBytes, int maxPageValues) {
        this(maxManifestBytes, maxParts, maxFooterBytes, maxIndexBytes, maxPageBytes, maxPageValues, 1_000_000);
    }

    /**
     * Creates limits; every limit must be positive.
     *
     * @param maxManifestBytes the largest manifest
     * @param maxParts the most parts
     * @param maxFooterBytes the largest footer
     * @param maxIndexBytes the largest page index
     * @param maxPageBytes the largest page
     * @param maxPageValues the most values in a page
     * @param maxResultEntries the largest request limit
     * @param maxIndexMemoryBytes the retained decoded-index and exact-first-key budget
     * @param maxOffsetIndexMemoryBytes the kept OffsetIndex page-location budget
     */
    public Limits {
        if (maxManifestBytes <= 0 || maxParts <= 0 || maxFooterBytes <= 0 || maxIndexBytes <= 0 || maxPageBytes <= 0
                || maxPageValues <= 0 || maxResultEntries <= 0 || maxIndexMemoryBytes <= 0 || maxOffsetIndexMemoryBytes <= 0) {
            throw new IllegalArgumentException("every limit must be positive");
        }
    }
}
