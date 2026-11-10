// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * Limits on the sizes a reader accepts from the data before it allocates (spec §11). A size above a
 * limit fails with {@link LimitExceededException}, which is not a refusal: the data may be valid.
 *
 * <p>{@code maxPageBytes} bounds a page's bytes, not the memory of decoding it: a page's values are
 * also held in primitive, offset/length or reference arrays of one slot per value, and keys and
 * dictionary entries in one byte-array object per value. {@code maxPageMemory} bounds that memory
 * per page, data and dictionary pages alike, key pages included: it is estimated from the page's
 * header before anything is allocated for its values (see {@code PageDecoder.pageMemory}), and a
 * page over it fails with {@link LimitExceededException} naming {@code maxPageMemory}. Its default
 * (when given as 0, or by a constructor without it) is the most {@code maxPageValues} and
 * {@code maxPageBytes} allow, so that the default refuses no page those limits admit: 832 MiB at
 * their defaults. Set it lower ({@link #withMaxPageMemory}) to bound decoding memory; a page within
 * the other limits may then be refused. The bound is per page and per request: concurrent requests
 * and the shards of a split plan scanned at once each decode their own pages, so their memory adds
 * up; bound concurrency as well. Each request retains its projected-column scratch at its high-water
 * capacities until the call ends, separately from dictionary storage and codec state. The shared index
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
 * @param maxPageMemory the estimated memory decoding one page may hold; 0 for the default
 */
public record Limits(
        int maxManifestBytes, int maxParts, int maxFooterBytes, int maxIndexBytes, int maxPageBytes, int maxPageValues, int maxResultEntries, long maxIndexMemoryBytes,
        long maxPageMemory) {

    private static final int MIB = 1 << 20;

    /**
     * The defaults: a 64 MiB manifest, 1,000,000 parts, a 64 MiB footer, a 16 MiB page index, a
     * 256 MiB page, 4 Mi (4,194,304) values per page and 1,000,000 entries per result.
     */
    public static final Limits DEFAULT = new Limits(64 * MIB, 1_000_000, 64 * MIB, 16 * MIB, 256 * MIB, 4 * MIB, 1_000_000);

    /** Creates limits with a 256 MiB retained-index budget.
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
                maxPageMemory);
    }

    /** Returns these limits with a different per-page decoding-memory bound.
     * @param bytes the bound, in bytes; positive, or 0 for the default from {@code maxPageValues} and
     *     {@code maxPageBytes}
     * @return the new limits
     */
    public Limits withMaxPageMemory(long bytes) {
        return new Limits(maxManifestBytes, maxParts, maxFooterBytes, maxIndexBytes, maxPageBytes, maxPageValues, maxResultEntries,
                maxIndexMemoryBytes, bytes);
    }

    /** Creates limits with the default per-page decoding-memory bound.
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
        this(maxManifestBytes, maxParts, maxFooterBytes, maxIndexBytes, maxPageBytes, maxPageValues, maxResultEntries, maxIndexMemoryBytes, 0);
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
     * @param maxPageMemory the estimated memory decoding one page may hold; 0 for the default, the most
     *     {@code maxPageValues} and {@code maxPageBytes} allow
     */
    public Limits {
        if (maxManifestBytes <= 0 || maxParts <= 0 || maxFooterBytes <= 0 || maxIndexBytes <= 0 || maxPageBytes <= 0
                || maxPageValues <= 0 || maxResultEntries <= 0 || maxIndexMemoryBytes <= 0 || maxPageMemory < 0) {
            throw new IllegalArgumentException("every limit must be positive");
        }
        if (maxPageMemory == 0) {
            maxPageMemory = PageDecoder.worstPageMemory(maxPageValues, maxPageBytes);
        }
    }
}
