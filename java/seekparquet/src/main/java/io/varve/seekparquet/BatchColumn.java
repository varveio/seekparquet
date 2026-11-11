// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.nio.ByteBuffer;

/**
 * The values of one projection slot in a {@link Batch}, read by row without a {@link Value} object per
 * row. It is borrowed exactly as its batch is: valid only until the action that received the batch
 * returns, after which every method throws {@link IllegalStateException}.
 *
 * <p>A slot is {@linkplain #isAbsent() absent} when the batch's part has no such column (COL-2), which is
 * distinct from a null value. A present slot has a value or a null for every row of the batch, of its
 * {@linkplain #type() physical type} in the batch's part; values are physical, as {@link Value}'s are,
 * and floating-point values keep their exact bits. A projected key column is a present {@code
 * BYTE_ARRAY} slot holding each row's key.
 *
 * <p>Reading a value of another type than the slot's, of a null row, or of an absent slot throws {@link
 * IllegalStateException}; an index outside the batch's rows throws {@link IndexOutOfBoundsException}.
 */
public final class BatchColumn {

    /**
     * Where a slot's values are: absent, the batch's keys, or a decoded page whose row {@code base} is the
     * batch's first row.
     */
    record Source(Column.Type type, ColumnPage page, int base) {
        static final Source ABSENT = new Source(null, null, 0);
        static final Source KEY = new Source(Column.Type.BYTE_ARRAY, null, 0);
    }

    private final Batch batch;
    private final Column.Type type;
    private final ColumnPage page;
    private final int base;

    BatchColumn(Batch batch, Source source) {
        this.batch = batch;
        this.type = source.type();
        this.page = source.page();
        this.base = source.base();
    }

    /** {@return whether the batch's part has no such column (COL-2)} */
    public boolean isAbsent() {
        batch.checkLive();
        return type == null;
    }

    /**
     * Returns the slot's physical type in the batch's part.
     *
     * @return the type: {@code BOOLEAN}, {@code INT32}, {@code INT64}, {@code FLOAT}, {@code DOUBLE},
     *     {@code BYTE_ARRAY} or {@code FIXED_LEN_BYTE_ARRAY}
     * @throws IllegalStateException if the slot is absent
     */
    public Column.Type type() {
        batch.checkLive();
        if (type == null) {
            throw new IllegalStateException("the batch's part has no such column");
        }
        return type;
    }

    /**
     * Returns whether row {@code i} holds no value. An absent slot is not null, as {@link Value.Absent} is
     * not.
     *
     * @param i the row
     * @return whether the row's value is null
     */
    public boolean isNull(int i) {
        int r = batch.checkRow(i);
        return page != null && page.nulls != null && page.nulls[base + r];
    }

    /**
     * Returns row {@code i}'s {@code BOOLEAN} value.
     *
     * @param i the row
     * @return the value
     */
    public boolean getBoolean(int i) {
        int at = row(i, Column.Type.BOOLEAN);
        return page.bools[at];
    }

    /**
     * Returns row {@code i}'s {@code INT32} value.
     *
     * @param i the row
     * @return the value
     */
    public int getInt(int i) {
        int at = row(i, Column.Type.INT32);
        return page.ints[at];
    }

    /**
     * Returns row {@code i}'s {@code INT64} value.
     *
     * @param i the row
     * @return the value
     */
    public long getLong(int i) {
        int at = row(i, Column.Type.INT64);
        return page.longs[at];
    }

    /**
     * Returns row {@code i}'s {@code FLOAT} value, as its exact bits.
     *
     * @param i the row
     * @return the bits, as {@link Float#floatToRawIntBits}
     */
    public int getFloatBits(int i) {
        int at = row(i, Column.Type.FLOAT);
        return page.ints[at];
    }

    /**
     * Returns row {@code i}'s {@code FLOAT} value.
     *
     * @param i the row
     * @return the value
     */
    public float getFloat(int i) {
        return Float.intBitsToFloat(getFloatBits(i));
    }

    /**
     * Returns row {@code i}'s {@code DOUBLE} value, as its exact bits.
     *
     * @param i the row
     * @return the bits, as {@link Double#doubleToRawLongBits}
     */
    public long getDoubleBits(int i) {
        int at = row(i, Column.Type.DOUBLE);
        return page.longs[at];
    }

    /**
     * Returns row {@code i}'s {@code DOUBLE} value.
     *
     * @param i the row
     * @return the value
     */
    public double getDouble(int i) {
        return Double.longBitsToDouble(getDoubleBits(i));
    }

    /**
     * Returns the length of row {@code i}'s {@code BYTE_ARRAY} or {@code FIXED_LEN_BYTE_ARRAY} value.
     *
     * @param i the row
     * @return the number of bytes
     */
    public int length(int i) {
        int r = binary(i);
        return page == null ? batch.keyArray(i).length : page.length(r);
    }

    /**
     * Copies row {@code i}'s {@code BYTE_ARRAY} or {@code FIXED_LEN_BYTE_ARRAY} value into {@code dst} at
     * {@code dstOffset}; it must fit.
     *
     * @param i the row
     * @param dst the destination
     * @param dstOffset where the value begins in {@code dst}
     * @return the number of bytes copied, {@link #length(int)}
     * @throws IndexOutOfBoundsException if the value does not fit
     */
    public int copyBytes(int i, byte[] dst, int dstOffset) {
        int r = binary(i);
        if (page == null) {
            return batch.copyKey(i, dst, dstOffset);
        }
        int n = page.length(r);
        java.util.Objects.checkFromIndexSize(dstOffset, n, dst.length);
        page.copy(r, dst, dstOffset);
        return n;
    }

    /**
     * Returns a read-only view of row {@code i}'s {@code BYTE_ARRAY} or {@code FIXED_LEN_BYTE_ARRAY}
     * value, borrowed as the batch is: the scan reuses its storage for later pages. It allocates the view.
     *
     * @param i the row
     * @return the view, positioned at the value's first byte
     */
    public ByteBuffer byteBuffer(int i) {
        int r = binary(i);
        return page == null ? batch.keyBuffer(i) : page.view(r);
    }

    /**
     * Returns row {@code i}'s {@code BYTE_ARRAY} or {@code FIXED_LEN_BYTE_ARRAY} value as owning bytes,
     * which may be kept.
     *
     * @param i the row
     * @return the value
     */
    public Bytes bytes(int i) {
        int r = binary(i);
        return page == null ? batch.key(i) : page.ownBytes(r);
    }

    /**
     * Returns an owning copy of row {@code i}'s value, the value a range read returns for it: {@link
     * Value.Absent} for an absent slot, {@link Value.Null} for a null row.
     *
     * @param i the row
     * @return the value
     */
    public Value value(int i) {
        int r = batch.checkRow(i);
        if (type == null) {
            return Value.Absent.INSTANCE;
        }
        if (page == null) {
            return new Value.ByteArray(batch.key(i));
        }
        return page.value(base + r);
    }

    /** {@return row {@code i}'s index in the page, after checking it holds a non-null value of {@code want}} */
    private int row(int i, Column.Type want) {
        int r = batch.checkRow(i);
        if (type != want) {
            throw new IllegalStateException(type == null ? "the batch's part has no such column" : "a " + type + " column, not " + want);
        }
        int at = base + r;
        if (page.nulls != null && page.nulls[at]) {
            throw new IllegalStateException("row " + i + " is null");
        }
        return at;
    }

    /** {@return row {@code i}'s index in the page, after checking it holds a non-null byte array} */
    private int binary(int i) {
        int r = batch.checkRow(i);
        if (type != Column.Type.BYTE_ARRAY && type != Column.Type.FIXED_LEN_BYTE_ARRAY) {
            throw new IllegalStateException(type == null ? "the batch's part has no such column" : "a " + type + " column, not a byte array");
        }
        int at = base + r;
        if (page != null && page.nulls != null && page.nulls[at]) {
            throw new IllegalStateException("row " + i + " is null");
        }
        return at;
    }

    @Override
    public String toString() {
        return "BatchColumn[" + (type == null ? "absent" : type) + "]";
    }
}
