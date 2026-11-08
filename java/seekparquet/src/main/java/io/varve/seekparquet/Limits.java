// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * Limits on the sizes a reader accepts from the data before it allocates (spec §11). A size above a
 * limit fails with {@link LimitExceededException}, which is not a refusal: the data may be valid.
 *
 * @param maxManifestBytes the largest {@code manifest.json}
 * @param maxParts the most parts in a manifest
 * @param maxFooterBytes the largest Parquet footer
 * @param maxIndexBytes the largest ColumnIndex or OffsetIndex of one column chunk
 * @param maxPageBytes the largest page, compressed or uncompressed, dictionary pages included
 * @param maxPageValues the most values in one page
 * @param maxResultEntries the most entries requested in one result
 * @param maxIndexMemoryBytes the dataset-wide retained decoded-index and exact-first-key budget
 */
public record Limits(
        int maxManifestBytes, int maxParts, int maxFooterBytes, int maxIndexBytes, int maxPageBytes, int maxPageValues, int maxResultEntries, long maxIndexMemoryBytes) {

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
        return new Limits(maxManifestBytes, maxParts, maxFooterBytes, maxIndexBytes, maxPageBytes, maxPageValues, maxResultEntries, bytes);
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
     */
    public Limits {
        if (maxManifestBytes <= 0 || maxParts <= 0 || maxFooterBytes <= 0 || maxIndexBytes <= 0 || maxPageBytes <= 0
                || maxPageValues <= 0 || maxResultEntries <= 0 || maxIndexMemoryBytes <= 0) {
            throw new IllegalArgumentException("every limit must be positive");
        }
    }
}
