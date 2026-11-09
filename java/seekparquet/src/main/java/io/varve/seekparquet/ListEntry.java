// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;
import java.util.Optional;

/**
 * One entry of a delimited listing (spec §6.3): a row, or a common prefix that rolls up the keys
 * under it. The alternatives are a closed set, for use with {@code switch} patterns.
 */
public sealed interface ListEntry {

    /**
     * {@return the entry's bytes: the row's key or the common prefix} This is what the next page of
     * a listing takes as its {@code after} bound ({@link ListQuery#next(ListEntry)}).
     */
    Bytes bytes();

    /**
     * A candidate key whose remainder after the prefix holds no delimiter (LIST-3).
     *
     * @param row the row, with its projected values
     */
    record RowEntry(Row row) implements ListEntry {
        /**
         * Creates the entry.
         *
         * @param row the row
         */
        public RowEntry {
            Objects.requireNonNull(row, "row");
        }

        @Override
        public Bytes bytes() {
            return row.key();
        }
    }

    /**
     * A common prefix: a key up to and including the first delimiter after the listing's prefix
     * (LIST-2), with its exact row when it has one (LIST-8): the first candidate row of the request
     * that rolls up to the prefix, when that row's key is the prefix itself, with its projected
     * values. Without a cursor, or after a prefix cursor, that is the first row stored at the
     * prefix's exact key whenever one is a candidate, so a listing pairs a folder with its folder
     * object without another request.
     *
     * @param prefix the common prefix
     * @param exact the exact row, or empty
     */
    record CommonPrefix(Bytes prefix, Optional<Row> exact) implements ListEntry {
        /**
         * Creates the entry.
         *
         * @param prefix the common prefix
         * @param exact the exact row, or empty
         */
        public CommonPrefix {
            Objects.requireNonNull(prefix, "prefix");
            Objects.requireNonNull(exact, "exact");
        }

        /**
         * Creates the entry of a common prefix without an exact row.
         *
         * @param prefix the common prefix
         */
        public CommonPrefix(Bytes prefix) {
            this(prefix, Optional.empty());
        }

        @Override
        public Bytes bytes() {
            return prefix;
        }
    }
}
