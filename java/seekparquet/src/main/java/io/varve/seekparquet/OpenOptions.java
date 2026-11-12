// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;

/**
 * Options for {@link Dataset#open(io.varve.seekparquet.storage.DatasetStorage, OpenOptions, Cancellation)}.
 *
 * @param limits size limits on what the data declares
 */
public record OpenOptions(Limits limits) {
    /** The default size limits. */
    public static final OpenOptions DEFAULT = new OpenOptions(Limits.DEFAULT);

    /** Creates options with the given limits. */
    public OpenOptions {
        Objects.requireNonNull(limits, "limits");
    }

    /**
     * Returns these options with other limits.
     * @param limits the limits
     * @return the options
     */
    public OpenOptions withLimits(Limits limits) {
        return new OpenOptions(limits);
    }
}
