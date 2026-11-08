// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * One row of a result: its key and the values of the projected columns, in request order (RANGE-2).
 * Rows are immutable.
 *
 * @param key the row's key
 * @param columns the projected column names, in request order (shared by the rows of one result)
 * @param values one value per projected column, in the same order
 */
public record Row(Bytes key, List<String> columns, List<Value> values) {

    /**
     * Creates a row.
     *
     * @param key the key
     * @param columns the projected column names
     * @param values the values, one per column
     */
    public Row {
        Objects.requireNonNull(key, "key");
        columns = List.copyOf(columns);
        values = List.copyOf(values);
        if (columns.size() != values.size()) {
            throw new IllegalArgumentException(columns.size() + " columns but " + values.size() + " values");
        }
    }

    /**
     * Returns the value of the named column, or empty when the request did not project it. When a
     * name was projected more than once, the first occurrence answers.
     *
     * @param column the column name
     * @return the value, when projected
     */
    public Optional<Value> value(String column) {
        int i = columns.indexOf(column);
        return i < 0 ? Optional.empty() : Optional.of(values.get(i));
    }
}
