// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Arrays;
import java.util.List;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageLocation;

/**
 * The checked page index of one row group's key column chunk (RG-2 to RG-4). Immutable once built,
 * and shared by every request on the dataset.
 *
 * <p>Each page's bounds are Parquet bounds whatever their length (RG-5): {@code lo} is at or below
 * the page's first key and {@code hi} at or above its last. They may be shortened, so adjacent pages'
 * bounds may overlap and need not be monotone (RG-6); {@code boundary_order} is never read.
 */
final class KeyIndex {

    final byte[][] lo;
    final byte[][] hi;
    final long[] firstRow;
    final long[] rows;
    final long[] offset;
    final int[] size;

    private KeyIndex(byte[][] lo, byte[][] hi, long[] firstRow, long[] rows, long[] offset, int[] size) {
        this.lo = lo;
        this.hi = hi;
        this.firstRow = firstRow;
        this.rows = rows;
        this.offset = offset;
        this.size = size;
    }

    int pages() {
        return lo.length;
    }

    /**
     * Builds the index after checking RG-3 and RG-4 as far as the indexes alone allow: a non-empty
     * list, parallel lists of one length, no null page, first rows from 0 strictly ascending below
     * the row count, every page inside the column chunk {@code [start, end)}, in order and without
     * overlap, computed without overflow. A page whose minimum is above its maximum bounds no key, so
     * it contradicts RG-5; for a string key every bound must be valid UTF-8 (KEY-6).
     *
     * @throws DataException describing the inconsistency (invalid_page_index)
     */
    static KeyIndex check(ColumnIndex ci, OffsetIndex oi, long groupRows, long start, long end, boolean utf8Key) throws DataException {
        List<PageLocation> locations = oi.getPage_locations();
        int n = locations == null ? 0 : locations.size();
        if (n == 0) {
            throw new DataException("the key OffsetIndex is empty");
        }
        List<Boolean> nullPages = ci.getNull_pages();
        List<java.nio.ByteBuffer> mins = ci.getMin_values();
        List<java.nio.ByteBuffer> maxs = ci.getMax_values();
        if (nullPages == null || mins == null || maxs == null || nullPages.size() != n || mins.size() != n || maxs.size() != n) {
            throw new DataException("the key ColumnIndex lists and the OffsetIndex disagree on the page count");
        }
        byte[][] lo = new byte[n][];
        byte[][] hi = new byte[n][];
        long[] firstRow = new long[n];
        long[] rows = new long[n];
        long[] offset = new long[n];
        int[] size = new int[n];
        long previousEnd = start;
        for (int i = 0; i < n; i++) {
            if (Boolean.TRUE.equals(nullPages.get(i))) {
                throw new DataException("key page " + i + " is marked null");
            }
            PageLocation loc = locations.get(i);
            long first = loc.getFirst_row_index();
            long last = i + 1 < n ? locations.get(i + 1).getFirst_row_index() : groupRows;
            if ((i == 0 && first != 0) || first < 0 || last <= first || last > groupRows) {
                throw new DataException("the key OffsetIndex row positions are invalid at page " + i);
            }
            long off = loc.getOffset();
            int sz = loc.getCompressed_page_size();
            if (sz <= 0 || off < previousEnd || off > end - sz) {
                throw new DataException("key page " + i + " lies outside its column chunk, out of order or overlapping");
            }
            previousEnd = off + sz;
            lo[i] = bytes(mins.get(i));
            hi[i] = bytes(maxs.get(i));
            if (Arrays.compareUnsigned(lo[i], hi[i]) > 0) {
                throw new DataException("key page " + i + " has a minimum above its maximum");
            }
            if (utf8Key && (!Utf8.isValid(lo[i]) || !Utf8.isValid(hi[i]))) {
                throw new DataException("a bound of key page " + i + " is not valid UTF-8");
            }
            firstRow[i] = first;
            rows[i] = last - first;
            offset[i] = off;
            size[i] = sz;
        }
        return new KeyIndex(lo, hi, firstRow, rows, offset, size);
    }

    private static byte[] bytes(java.nio.ByteBuffer b) throws DataException {
        if (b == null) {
            throw new DataException("a key page bound is missing");
        }
        byte[] out = new byte[b.remaining()];
        b.duplicate().get(out);
        return out;
    }

    /** Returns the earliest candidate; upper bounds may be non-monotone (ROUTE-1). */
    int pageFor(int from, byte[] target) {
        for (int i = from; i < pages(); i++) {
            if (Arrays.compareUnsigned(hi[i], target) >= 0) {
                return i;
            }
        }
        return pages();
    }

    /** {@return the maximum of the last page: an upper bound on every key of the group} */
    byte[] upper() {
        return hi[hi.length - 1];
    }
}
