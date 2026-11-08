// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;

/** The manifest's named key column and whether equal keys are forbidden.
 * @param column the exact top-level column name
 * @param unique whether keys must be strictly ascending
 */
public record KeyDefinition(String column, boolean unique) {
    /** Creates a key declaration.
     * @param column the exact column name, non-empty and without control characters
     * @param unique whether keys must be unique
     */
    public KeyDefinition {
        Objects.requireNonNull(column, "column");
        if (column.isEmpty() || column.chars().anyMatch(c -> c < 32 || c == 127)) {
            throw new IllegalArgumentException("key column must be non-empty and contain no control characters");
        }
    }
}
