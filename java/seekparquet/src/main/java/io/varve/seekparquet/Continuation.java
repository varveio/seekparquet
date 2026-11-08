// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;

/** A position after a row or a common prefix in an immutable dataset (PAGE-1). */
public sealed interface Continuation permits Continuation.RowPosition, Continuation.CommonPrefix {
    /** {@return the key or common prefix bytes} */
    Bytes bytes();

    /** A row's zero-based position within its equal-key group.
     * @param bytes the row's key
     * @param offset the position in file order, at least zero
     */
    record RowPosition(Bytes bytes, long offset) implements Continuation {
        /** Creates a row position.
         * @param bytes the key
         * @param offset the non-negative position within its group
         */
        public RowPosition {
            Objects.requireNonNull(bytes, "bytes");
            if (offset < 0) {
                throw new IllegalArgumentException("offset must be non-negative");
            }
        }
    }

    /** A common prefix whose entire subtree has been returned.
     * @param bytes the common prefix
     */
    record CommonPrefix(Bytes bytes) implements Continuation {
        /** Creates a prefix position.
         * @param bytes the prefix
         */
        public CommonPrefix {
            Objects.requireNonNull(bytes, "bytes");
        }
    }
}
