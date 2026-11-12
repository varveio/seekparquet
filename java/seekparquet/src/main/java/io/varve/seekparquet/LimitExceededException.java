// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * Input or a result beyond one of the {@link Limits} (spec §11), or beyond a fixed capacity of this
 * reader: a policy of the caller or a capacity of the reader, not a refusal of the data, which may be
 * valid. Raise the limit in {@link OpenOptions} to read it. The Go reader's {@code *LimitError}.
 */
public final class LimitExceededException extends SeekParquetException {

    private static final long serialVersionUID = 1L;

    /** The limit. */
    private final LimitName limit;

    /** The amount declared or found, -1 when known only to exceed the maximum. */
    private final long declared;

    /** The limit in force. */
    private final long maximum;

    LimitExceededException(LimitName limit, long declared, long maximum, String where) {
        super(where + ": limit " + limit + (declared >= 0 ? " " + declared + " exceeds " : " exceeds ") + maximum, null);
        this.limit = limit;
        this.declared = declared;
        this.maximum = maximum;
    }

    /** {@return which limit was exceeded} */
    public LimitName limit() {
        return limit;
    }

    /** {@return the amount the input declared or the read found, or -1 when it is known only to exceed {@link #maximum()}} */
    public long declared() {
        return declared;
    }

    /** {@return the limit in force} */
    public long maximum() {
        return maximum;
    }
}
