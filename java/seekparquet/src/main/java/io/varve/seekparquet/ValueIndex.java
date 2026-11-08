// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.List;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageLocation;

/** The checked OffsetIndex of a value column chunk (COL-4). Immutable. */
final class ValueIndex {

    final long[] firstRow;
    final long[] offset;
    final int[] size;

    private ValueIndex(long[] firstRow, long[] offset, int[] size) {
        this.firstRow = firstRow;
        this.offset = offset;
        this.size = size;
    }

    /** Conservative retained size, including arrays and the map entry. */
    long memoryBytes() {
        return 192L + 24L * firstRow.length;
    }

    static ValueIndex check(OffsetIndex oi, long groupRows, long start, long end) throws DataException {
        List<PageLocation> locations = oi.getPage_locations();
        int n = locations == null ? 0 : locations.size();
        if (n == 0) {
            throw new DataException("an empty OffsetIndex");
        }
        long[] firstRow = new long[n];
        long[] offset = new long[n];
        int[] size = new int[n];
        long previousEnd = start;
        for (int i = 0; i < n; i++) {
            PageLocation loc = locations.get(i);
            long first = loc.getFirst_row_index();
            long last = i + 1 < n ? locations.get(i + 1).getFirst_row_index() : groupRows;
            if ((i == 0 && first != 0) || first < 0 || last <= first || last > groupRows) {
                throw new DataException("the OffsetIndex row positions are invalid at page " + i);
            }
            long off = loc.getOffset();
            int sz = loc.getCompressed_page_size();
            if (sz <= 0 || off < previousEnd || off > end - sz) {
                throw new DataException("page " + i + " of the OffsetIndex lies outside its column chunk");
            }
            previousEnd = off + sz;
            firstRow[i] = first;
            offset[i] = off;
            size[i] = sz;
        }
        return new ValueIndex(firstRow, offset, size);
    }

    /** {@return the page holding row {@code row} of the group} */
    int pageOf(long row) {
        int lo = 0;
        int hi = firstRow.length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (firstRow[mid] <= row) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    long rowsOf(int page, long groupRows) {
        return (page + 1 < firstRow.length ? firstRow[page + 1] : groupRows) - firstRow[page];
    }
}
