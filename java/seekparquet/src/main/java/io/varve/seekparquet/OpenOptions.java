// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;

/**
 * Options for {@link Dataset#open(io.varve.seekparquet.storage.DatasetStorage, OpenOptions,
 * Cancellation)}.
 *
 * @param limits size limits on what the data declares
 * @param verifyPartBounds whether opening also decodes each part's first and last key and compares
 *     them with the manifest's bounds (CHECK-3), refusing a difference with {@link
 *     RefusalCode#BOUNDS_MISMATCH}. This reads the page index and two key pages of every part, so it
 *     is off by default: opening then reads only the manifest and each part's footer, and a part's
 *     key indexes are loaded when a request first needs them.
 * @param openConcurrency how many parts are opened at once (their footers read concurrently, on
 *     virtual threads); 1 opens them one after another
 */
public record OpenOptions(Limits limits, boolean verifyPartBounds, int openConcurrency) {

    /** The defaults: {@link Limits#DEFAULT}, no bound verification, 8 parts opened at once. */
    public static final OpenOptions DEFAULT = new OpenOptions(Limits.DEFAULT, false, 8);

    /**
     * Creates options.
     *
     * @param limits the limits
     * @param verifyPartBounds whether to verify part bounds on open
     * @param openConcurrency how many parts to open at once, at least 1
     */
    public OpenOptions {
        Objects.requireNonNull(limits, "limits");
        if (openConcurrency < 1) {
            throw new IllegalArgumentException("openConcurrency must be at least 1");
        }
    }

    /**
     * Returns these options with other limits.
     *
     * @param limits the limits
     * @return the options
     */
    public OpenOptions withLimits(Limits limits) {
        return new OpenOptions(limits, verifyPartBounds, openConcurrency);
    }

    /**
     * Returns these options with part bound verification on or off.
     *
     * @param verify whether to verify
     * @return the options
     */
    public OpenOptions withVerifyPartBounds(boolean verify) {
        return new OpenOptions(limits, verify, openConcurrency);
    }

    /**
     * Returns these options with another open concurrency.
     *
     * @param concurrency how many parts to open at once
     * @return the options
     */
    public OpenOptions withOpenConcurrency(int concurrency) {
        return new OpenOptions(limits, verifyPartBounds, concurrency);
    }
}
