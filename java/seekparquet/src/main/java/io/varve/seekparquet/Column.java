// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;

/**
 * A top-level column of a dataset, as its parts declare it. COL-5 makes the declaration the same in
 * every part that has the column.
 *
 * @param name the top-level field name (a {@code .} in it is a literal character)
 * @param type the physical type, or {@link Type#GROUP} for a nested column
 * @param typeLength the type length of a {@code FIXED_LEN_BYTE_ARRAY} column, 0 otherwise
 * @param repetition the field's repetition
 * @param parts how many parts have the column
 */
public record Column(String name, Type type, int typeLength, Repetition repetition, int parts) {

    /**
     * Creates a column description.
     *
     * @param name the name
     * @param type the physical type
     * @param typeLength the type length
     * @param repetition the repetition
     * @param parts the number of parts with the column
     */
    public Column {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(repetition, "repetition");
    }

    /**
     * {@return whether this reader can project the column (COL-3)} It can when the column is not
     * nested, not repeated, and of a baseline physical type.
     */
    public boolean projectable() {
        return type != Type.GROUP && type != Type.INT96 && repetition != Repetition.REPEATED;
    }

    /** A column's physical type. */
    public enum Type {
        /** {@code BOOLEAN}. */
        BOOLEAN,
        /** {@code INT32}. */
        INT32,
        /** {@code INT64}. */
        INT64,
        /** {@code INT96}, which this reader does not project. */
        INT96,
        /** {@code FLOAT}. */
        FLOAT,
        /** {@code DOUBLE}. */
        DOUBLE,
        /** {@code BYTE_ARRAY}. */
        BYTE_ARRAY,
        /** {@code FIXED_LEN_BYTE_ARRAY}. */
        FIXED_LEN_BYTE_ARRAY,
        /** A group: a nested column, which this reader does not project. */
        GROUP
    }

    /** A field's repetition. */
    public enum Repetition {
        /** Exactly one value per row. */
        REQUIRED,
        /** At most one value per row. */
        OPTIONAL,
        /** Any number of values per row. */
        REPEATED
    }
}
