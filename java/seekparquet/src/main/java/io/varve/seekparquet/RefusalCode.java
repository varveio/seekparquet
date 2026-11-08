// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * The stable refusal codes of SeekParquet format 1, draft revision 1 ({@code
 * spec/refusal-codes.json}). A refusal says that the data breaks the specification. Codes are
 * stable; diagnostic messages are not.
 */
public enum RefusalCode {
    /** The dataset root has no {@code manifest.json}. */
    NO_MANIFEST("no_manifest"),
    /** The manifest breaks §3: syntax, members, types, base64, paths, or part bounds. */
    INVALID_MANIFEST("invalid_manifest"),
    /** The manifest names a (format_version, draft_revision) pair this reader does not support. */
    UNSUPPORTED_VERSION("unsupported_version"),
    /** A part the manifest lists does not exist. */
    MISSING_PART("missing_part"),
    /** A part's size in bytes, or its footer's row count, differs from its manifest entry. */
    PART_MISMATCH("part_mismatch"),
    /** A part's footer cannot be read, its row counts do not add up, or it refers to another file. */
    INVALID_FOOTER("invalid_footer"),
    /** A part's {@code seekparquet.*} footer metadata is missing, duplicated, forbidden or wrong. */
    STAMP_MISMATCH("stamp_mismatch"),
    /** A part's {@code key} column breaks KEY-1 or KEY-5. */
    INVALID_KEY_COLUMN("invalid_key_column"),
    /** A column differs in physical type, repetition or nesting between parts (COL-5). */
    INCONSISTENT_COLUMN("inconsistent_column"),
    /** A non-empty row group does not declare {@code key} ascending as its first sorting column. */
    MISSING_SORTING_COLUMNS("missing_sorting_columns"),
    /** A non-empty row group's key column lacks a ColumnIndex or an OffsetIndex. */
    MISSING_PAGE_INDEX("missing_page_index"),
    /** A key column's page index is inconsistent with itself, the pages, or the decoded keys. */
    INVALID_PAGE_INDEX("invalid_page_index"),
    /** An empty key, or a key that is not valid UTF-8 in a key column annotated as a string. */
    INVALID_KEY("invalid_key"),
    /** Keys are not unique and strictly ascending. */
    KEY_ORDER("key_order"),
    /** Corruption in the Parquet data itself (CHECK-5). */
    INVALID_PARQUET("invalid_parquet"),
    /** A part's actual first or last key differs from its manifest bounds. */
    BOUNDS_MISMATCH("bounds_mismatch"),
    /** A part's SHA-256 differs from the manifest's. */
    DIGEST_MISMATCH("digest_mismatch");

    private final String id;

    RefusalCode(String id) {
        this.id = id;
    }

    /** {@return the code as the specification spells it, for example {@code key_order}} */
    public String id() {
        return id;
    }
}
