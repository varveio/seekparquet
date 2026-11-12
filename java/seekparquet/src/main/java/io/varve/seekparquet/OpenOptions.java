// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;

/**
 * Options for {@link Dataset#open(io.varve.seekparquet.storage.DatasetStorage, OpenOptions,
 * Cancellation)}. The Go reader's {@code OpenOptions}, which has the limits only: it verifies one part at
 * a time.
 *
 * @param limits size limits on what the data declares
 * @param verifyConcurrency how many parts {@link Dataset#verify()} checks at once (their footers
 *     read concurrently, on virtual threads); 1 checks them one after another
 */
public record OpenOptions(Limits limits, int verifyConcurrency) {

    /**
     * The defaults: {@link Limits#DEFAULT}, and one part checked at a time by {@link Dataset#verify()}, as
     * the Go reader verifies.
     */
    public static final OpenOptions DEFAULT = new OpenOptions(Limits.DEFAULT, 1);

    /**
     * Creates options.
     *
     * @param limits the limits
     * @param verifyConcurrency how many parts verification checks at once, at least 1
     */
    public OpenOptions {
        Objects.requireNonNull(limits, "limits");
        if (verifyConcurrency < 1) {
            throw new IllegalArgumentException("verifyConcurrency must be at least 1");
        }
    }

    /**
     * Returns these options with other limits.
     *
     * @param limits the limits
     * @return the options
     */
    public OpenOptions withLimits(Limits limits) {
        return new OpenOptions(limits, verifyConcurrency);
    }

    /**
     * Returns these options with another verification concurrency.
     *
     * @param concurrency how many parts {@link Dataset#verify()} checks at once
     * @return the options
     */
    public OpenOptions withVerifyConcurrency(int concurrency) {
        return new OpenOptions(limits, concurrency);
    }
}
