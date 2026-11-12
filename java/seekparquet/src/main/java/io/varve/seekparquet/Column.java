// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;
import java.util.Optional;

/**
 * A top-level column of one part, as its footer declares it ({@link Dataset#columns(int)}); the Go
 * reader's {@code Column}. COL-5 makes the physical declaration (type,
 * type length, repetition, nesting) the same in every part that has the column; logical annotations are
 * not compared, so they may differ between parts.
 *
 * @param name the top-level field name (a {@code .} in it is a literal character)
 * @param type the physical type, or {@link Type#GROUP} for a nested column
 * @param typeLength the type length of a {@code FIXED_LEN_BYTE_ARRAY} column, 0 otherwise
 * @param logicalType the logical type of a leaf column: its {@code logical_type}, or else its legacy
 *     {@code converted_type} normalized into one (INTERVAL has none); always empty for a group
 * @param repetition the field's repetition
 * @param key whether it is the dataset's key column
 */
public record Column(String name, Type type, int typeLength, Optional<LogicalType> logicalType, Repetition repetition, boolean key) {

    /**
     * Creates a column description.
     *
     * @param name the name
     * @param type the physical type
     * @param typeLength the type length
     * @param logicalType the logical type
     * @param repetition the repetition
     * @param key whether it is the key column
     */
    public Column {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(logicalType, "logicalType");
        Objects.requireNonNull(repetition, "repetition");
    }

    /**
     * {@return whether this reader can project the column (COL-3)} It can when the column is not
     * nested and not repeated; an {@code INT96} column is projected as its raw bits ({@link Value.Int96}),
     * as in the Go reader.
     */
    public boolean projectable() {
        return type != Type.GROUP && repetition != Repetition.REPEATED;
    }

    /** A column's physical type. */
    public enum Type {
        /** {@code BOOLEAN}. */
        BOOLEAN,
        /** {@code INT32}. */
        INT32,
        /** {@code INT64}. */
        INT64,
        /** {@code INT96}, projected as raw bits ({@link Value.Int96}), not interpreted. */
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
