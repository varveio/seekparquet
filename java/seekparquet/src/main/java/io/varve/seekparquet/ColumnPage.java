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
    /** Fresh key arrays or dictionary entries; value-page reference arrays may be reused. */
    final byte[][] binaries;
    /** Binary values in reusable page storage, addressed by row. Null for keys/dictionary entries. */
    final byte[] arena;
    final int[] offsets;
    final int[] lengths;

    ColumnPage(Column.Type type, int rows, boolean[] nulls, boolean[] bools, int[] ints, long[] longs, byte[][] binaries) {
        this(type, rows, nulls, bools, ints, longs, binaries, null, null, null);
    }

    ColumnPage(Column.Type type, int rows, boolean[] nulls, boolean[] bools, int[] ints, long[] longs,
            byte[][] binaries, byte[] arena, int[] offsets, int[] lengths) {
        this.arena = arena;
        this.offsets = offsets;
        this.lengths = lengths;
        this.type = type;
        this.rows = rows;
        this.nulls = nulls;
        this.bools = bools;
        this.ints = ints;
        this.longs = longs;
        this.binaries = binaries;
    }

    /** {@return an owning value of row {@code i}, safe after this page is overwritten} */
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
            case BYTE_ARRAY -> new Value.ByteArray(ownBytes(i));
            case FIXED_LEN_BYTE_ARRAY -> new Value.FixedLenByteArray(ownBytes(i));
            case INT96, GROUP -> throw new IllegalStateException("not projectable: " + type);
        };
    }

    private Bytes ownBytes(int i) {
        return arena == null ? Bytes.of(binaries[i]) : Bytes.of(arena, offsets[i], lengths[i]);
    }
}
