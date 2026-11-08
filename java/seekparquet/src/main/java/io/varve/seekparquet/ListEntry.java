// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;

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
     * (LIST-2).
     *
     * @param prefix the common prefix
     */
    record CommonPrefix(Bytes prefix) implements ListEntry {
        /**
         * Creates the entry.
         *
         * @param prefix the common prefix
         */
        public CommonPrefix {
            Objects.requireNonNull(prefix, "prefix");
        }

        @Override
        public Bytes bytes() {
            return prefix;
        }
    }
}
