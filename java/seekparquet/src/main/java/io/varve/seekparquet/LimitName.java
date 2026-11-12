// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * Names the limit a {@link LimitExceededException} reports: a setting of {@link Limits}, or a fixed
 * capacity of this reader. The constants correspond one to one to the Go reader's {@code LimitName}
 * constants ({@code MANIFEST_BYTES} is {@code LimitManifestBytes}, and so on).
 */
public enum LimitName {
    /** {@link Limits#manifestBytes()}. */
    MANIFEST_BYTES,
    /** {@link Limits#parts()}. */
    PARTS,
    /** {@link Limits#footerBytes()}. */
    FOOTER_BYTES,
    /** {@link Limits#pageIndexBytes()}. */
    PAGE_INDEX_BYTES,
    /** {@link Limits#metadataMemory()}. */
    METADATA_MEMORY,
    /** {@link Limits#pageBytes()}. */
    PAGE_BYTES,
    /** {@link Limits#pageValues()}. */
    PAGE_VALUES,
    /** {@link Limits#pageMemory()}. */
    PAGE_MEMORY,
    /** {@link Limits#indexMemory()}. */
    INDEX_MEMORY,
    /** {@link Limits#offsetIndexMemory()}; never exceeded, since it bounds a cache. */
    OFFSET_INDEX_MEMORY,
    /** {@link Limits#getRows()}: a group {@link Dataset#get} would return more rows of. */
    GET_ROWS,
    /**
     * The reader's fixed capacity of nested JSON objects and arrays in a manifest: 512, the manifest
     * object itself counting 1, as in the Go reader.
     */
    MANIFEST_NESTING,
    /** The reader's capacity of signed 64-bit dataset ordinals: a manifest row total above 2^63-1. */
    DATASET_ROWS
}
