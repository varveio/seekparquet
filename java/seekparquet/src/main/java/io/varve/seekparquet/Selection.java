// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;
import java.util.Optional;

/**
 * A span to read, from its start or, when {@code cursor} is present, after a row a read of the same
 * selection returned ({@link Row#cursor()}): a row cursor, never a prefix cursor, which may fall inside a
 * group of equal keys. {@link Dataset#plan} plans a list of selections; a {@link Query} adds a limit and
 * a projection to one. An immutable value. The Go reader's {@code Selection}.
 *
 * @param span the key bounds
 * @param cursor the row cursor to continue after, when present
 */
public record Selection(Span span, Optional<Cursor.RowPosition> cursor) {

    /**
     * Creates a selection.
     *
     * @param span the key bounds
     * @param cursor the row cursor, or empty
     */
    public Selection {
        Objects.requireNonNull(span, "span");
        Objects.requireNonNull(cursor, "cursor");
    }

    /**
     * Returns the selection of {@code span} from its start.
     *
     * @param span the key bounds
     * @return the selection
     */
    public static Selection of(Span span) {
        return new Selection(span, Optional.empty());
    }

    /**
     * Returns the selection of {@code span} after the row {@code cursor} names.
     *
     * @param span the key bounds
     * @param cursor the row cursor
     * @return the selection
     */
    public static Selection after(Span span, Cursor.RowPosition cursor) {
        return new Selection(span, Optional.of(cursor));
    }
}
