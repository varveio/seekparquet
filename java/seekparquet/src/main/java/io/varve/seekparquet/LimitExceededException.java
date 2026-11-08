// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * A size the data declares (a manifest, a footer, a page, a number of parts or values) exceeds a
 * configured {@link Limits limit}. The data may be valid; the reader declined to allocate for it.
 * Raise the limit in {@link OpenOptions} to read it.
 */
public final class LimitExceededException extends SeekParquetException {

    private static final long serialVersionUID = 1L;

    /** The name of the limit. */
    private final String limit;

    /** The size the data declared. */
    private final long declared;

    /** The configured maximum. */
    private final long maximum;

    LimitExceededException(String limit, long declared, long maximum, String where) {
        super(where + ": " + limit + " " + declared + " exceeds the configured limit " + maximum, null);
        this.limit = limit;
        this.declared = declared;
        this.maximum = maximum;
    }

    /** {@return the name of the limit, as the {@link Limits} component is named} */
    public String limit() {
        return limit;
    }

    /** {@return the size the data declared} */
    public long declared() {
        return declared;
    }

    /** {@return the configured maximum} */
    public long maximum() {
        return maximum;
    }
}
