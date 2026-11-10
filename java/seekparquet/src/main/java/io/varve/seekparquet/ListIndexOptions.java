// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * Options for the optional slash listing index. Zero fields select the defaults.
 *
 * @param maxBytes hard retained index budget, including containers and anchors; zero is 64 MiB.
 *     Existing footer, routing and Parquet page-index caches retain their own limits.
 * @param workers maximum concurrently decoded row groups; zero is one
 * @param maxChangesPerPage omit boundaries from pages with more directory changes; zero is eight
 */
public record ListIndexOptions(long maxBytes, int workers, int maxChangesPerPage) {
    /** Default settings. */
    public static final ListIndexOptions DEFAULT = new ListIndexOptions(0, 0, 0);

    /** Validates options and applies defaults. */
    public ListIndexOptions {
        if (maxBytes < 0 || workers < 0 || maxChangesPerPage < 0) {
            throw new IllegalArgumentException("negative list index option");
        }
        if (maxBytes == 0) maxBytes = 64L << 20;
        if (workers == 0) workers = 1;
        if (maxChangesPerPage == 0) maxChangesPerPage = 8;
    }
}
