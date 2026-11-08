// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * The decoded values of one data page of a non-repeated column: one entry per row. Values are held
 * in primitive arrays and turned into {@link Value}s only for the rows a result returns.
 */
final class ColumnPage {

    final Column.Type type;
    final int rows;
    /** Which rows are null; {@code null} when none is. */
    final boolean[] nulls;
    final boolean[] bools;
    /** INT32 values, or FLOAT bits. */
    final int[] ints;
    /** INT64 values, or DOUBLE bits. */
    final long[] longs;
    /** BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY values; never modified. */
    final byte[][] binaries;

    ColumnPage(Column.Type type, int rows, boolean[] nulls, boolean[] bools, int[] ints, long[] longs, byte[][] binaries) {
        this.type = type;
        this.rows = rows;
        this.nulls = nulls;
        this.bools = bools;
        this.ints = ints;
        this.longs = longs;
        this.binaries = binaries;
    }

    /** {@return the value of row {@code i} of the page} */
    Value value(int i) {
        if (nulls != null && nulls[i]) {
            return Value.Null.INSTANCE;
        }
        return switch (type) {
            case BOOLEAN -> new Value.Bool(bools[i]);
            case INT32 -> new Value.Int32(ints[i]);
            case INT64 -> new Value.Int64(longs[i]);
            case FLOAT -> new Value.Float32(ints[i]);
            case DOUBLE -> new Value.Float64(longs[i]);
            case BYTE_ARRAY -> new Value.ByteArray(Bytes.wrap(binaries[i]));
            case FIXED_LEN_BYTE_ARRAY -> new Value.FixedLenByteArray(Bytes.wrap(binaries[i]));
            case INT96, GROUP -> throw new IllegalStateException("not projectable: " + type);
        };
    }
}
