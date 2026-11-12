// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A positive run of consecutive selected rows, in dataset order, handed to the action of {@link
 * RangePlan#scanBatches} or {@link Dataset#rangeBatches}: their keys, group offsets and the values of the
 * projected columns, read without a {@link Row} or {@link Value} object per row.
 *
 * <p>A batch never crosses a row group, a key page, a page of any projected column, or a gap between the
 * selected rows of a plan. Batches may be smaller than that; their size is not promised.
 *
 * <p><b>A batch is borrowed.</b> It, its {@link BatchColumn}s, and the buffers {@link #keyBuffer} and
 * {@link BatchColumn#byteBuffer} return view storage the scan reuses for later pages: they are read-only
 * and valid only until the action returns. Using the batch or its columns after that throws {@link
 * IllegalStateException}; a buffer view kept past it may show other bytes. Do not hand them to work that
 * outlives the action. {@link #row}, {@link #key}, {@link #cursor}, {@link BatchColumn#value} and
 * {@link BatchColumn#bytes} make owning copies, which may be kept. An index outside {@code [0, size())}
 * throws {@link IndexOutOfBoundsException}.
 *
 * <p>The action may have other threads read the batch while it runs, but it must wait for them before it
 * returns. The batch's end of life is published with a volatile write, so a thread that reads it after
 * the action returns sees it retired.
 */
public final class Batch {

    private final List<String> columns;
    private final byte[][] keys;
    private final long[] offsets;
    private final int from;
    private final int size;
    private final BatchColumn[] values;
    private volatile boolean live = true;

    Batch(List<String> columns, byte[][] keys, long[] offsets, int from, int size, BatchColumn.Source[] sources) {
        this.columns = columns;
        this.keys = keys;
        this.offsets = offsets;
        this.from = from;
        this.size = size;
        this.values = new BatchColumn[sources.length];
        for (int c = 0; c < sources.length; c++) {
            values[c] = new BatchColumn(this, sources[c]);
        }
    }

    /** Ends the batch's lifetime: its accessors throw from now on. */
    void retire() {
        live = false;
    }

    /** Throws {@link IllegalStateException} once the batch is retired. */
    void checkLive() {
        if (!live) {
            throw new IllegalStateException("a batch is valid only until the action it was handed to returns");
        }
    }

    /** {@return {@code i}}, after checking that the batch is live and {@code i} is one of its rows */
    int checkRow(int i) {
        checkLive();
        return Objects.checkIndex(i, size);
    }

    /** {@return the index of row {@code i} in the key page}, after checking it as {@link #checkRow} does */
    private int at(int i) {
        return from + checkRow(i);
    }

    /** {@return the number of rows, at least 1} */
    public int size() {
        checkLive();
        return size;
    }

    /** {@return the projected column names, in request order, duplicates included} */
    public List<String> columns() {
        checkLive();
        return columns;
    }

    /**
     * Returns the values of projection slot {@code c}, in request order, duplicate projections included.
     *
     * @param c the slot, an index into {@link #columns()}
     * @return the slot's values in this batch, borrowed as the batch is
     * @throws IndexOutOfBoundsException if {@code c} is not a slot
     */
    public BatchColumn column(int c) {
        checkLive();
        return values[Objects.checkIndex(c, values.length)];
    }

    /**
     * Returns row {@code i}'s zero-based offset in its group of equal keys (PAGE-1); always zero when keys
     * are unique.
     *
     * @param i the row
     * @return the offset
     */
    public long offset(int i) {
        return offsets[at(i)];
    }

    /**
     * Returns the length of row {@code i}'s key.
     *
     * @param i the row
     * @return the number of bytes
     */
    public int keyLength(int i) {
        return keys[at(i)].length;
    }

    /**
     * Copies row {@code i}'s key into {@code dst} at {@code dstOffset}; it must fit.
     *
     * @param i the row
     * @param dst the destination
     * @param dstOffset where the key begins in {@code dst}
     * @return the number of bytes copied, {@link #keyLength(int)}
     * @throws IndexOutOfBoundsException if the key does not fit
     */
    public int copyKey(int i, byte[] dst, int dstOffset) {
        byte[] key = keys[at(i)];
        System.arraycopy(key, 0, dst, dstOffset, key.length);
        return key.length;
    }

    /**
     * Compares row {@code i}'s key with {@code other} in unsigned lexicographic order, as {@link
     * Bytes#compareTo} does, without copying it.
     *
     * @param i the row
     * @param other the bytes to compare with
     * @return negative, zero or positive as the key is below, equal to or above {@code other}
     */
    public int compareKey(int i, Bytes other) {
        return Arrays.compareUnsigned(keys[at(i)], other.unsafeArray());
    }

    /**
     * Returns a read-only view of row {@code i}'s key, borrowed as the batch is. It allocates the view.
     *
     * @param i the row
     * @return the view, positioned at the key's first byte
     */
    public ByteBuffer keyBuffer(int i) {
        return ByteBuffer.wrap(keys[at(i)]).asReadOnlyBuffer();
    }

    /**
     * Returns row {@code i}'s key as owning bytes, which may be kept.
     *
     * @param i the row
     * @return the key
     */
    public Bytes key(int i) {
        // Key pages are decoded into arrays of their own, never reused or modified.
        return Bytes.wrap(keys[at(i)]);
    }

    /**
     * Returns an owning copy of row {@code i}, the row a range read or {@link RangePlan#scan} returns for it.
     *
     * @param i the row
     * @return the row
     */
    public Row row(int i) {
        Bytes key = key(i);
        Value[] row = new Value[values.length];
        for (int c = 0; c < row.length; c++) {
            row[c] = values[c].value(i);
        }
        return new Row(key, offset(i), columns, Arrays.asList(row));
    }

    /**
     * Returns an owning cursor after row {@code i}, as {@link Row#cursor()} of {@link #row(int)}.
     *
     * @param i the row
     * @return the cursor, which may be kept
     */
    public Cursor.RowPosition cursor(int i) {
        return new Cursor.RowPosition(key(i), offset(i));
    }

    /** {@return the key bytes of row {@code i}, borrowed} */
    byte[] keyArray(int i) {
        return keys[at(i)];
    }

    @Override
    public String toString() {
        return "Batch[" + size + " rows]";
    }
}
