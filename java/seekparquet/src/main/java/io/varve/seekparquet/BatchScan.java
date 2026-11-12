// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Arrays;
import java.util.List;

/**
 * Cuts a key cursor's rows into {@link Batch}es, the one batch path behind {@link Plan#scan}
 * and {@link Dataset#scan}. It loads, for each batch, only the page of each projected column holding
 * its first row, and ends the batch where the first of those pages, or the key page, ends: every page a
 * batch borrows stays decoded until its action returns, and the cursor moves past the key page only after
 * it. Value pages are decoded into the request's per-column scratch, so the batch's values are valid only
 * until the next page of their column is decoded.
 */
final class BatchScan {

    private final List<String> columns;
    /** The request's context: its cancellation, statistics and the key cursor's checks. */
    private final ReadContext ctx;
    /** The context the value columns are read through, whose row-group caches routing probes leave alone. */
    private final ReadContext values;
    private PartFile part;
    private int physical = -1;
    /** The projection slots' columns in the current part: null when absent. */
    private final ColumnSpec[] specs;

    BatchScan(List<String> columns, ReadContext ctx, ReadContext values) {
        this.columns = columns;
        this.ctx = ctx;
        this.values = values;
        this.specs = new ColumnSpec[columns.size()];
    }

    /**
     * Hands {@code visitor} batches of at most {@code n} rows from the cursor's current row on, until a key
     * at or above {@code end} or not beginning with {@code prefix} (each null for none), or the end of the
     * dataset. The cursor is left on the last row handed over, or on the first row excluded by a bound.
     * {@return false when the visitor stopped the scan}
     */
    <E extends Exception> boolean walk(KeyCursor cursor, long n, byte[] end, byte[] prefix, BatchVisitor<E> visitor) throws E {
        while (n > 0) {
            ctx.checkCancelled();
            byte[][] keys = cursor.pageKeys();
            int from = cursor.pagePosition();
            if (outside(keys[from], end, prefix)) {
                return true;
            }
            enter(cursor.partFile(), cursor.physical());
            int k = (int) Math.min(n, keys.length - from);
            long row = cursor.row();
            BatchColumn.Source[] sources = new BatchColumn.Source[specs.length];
            for (int c = 0; c < specs.length; c++) {
                ColumnSpec spec = specs[c];
                if (spec == null) {
                    sources[c] = BatchColumn.Source.ABSENT;
                } else if (spec.leaf() == part.keyLeaf) {
                    sources[c] = BatchColumn.Source.KEY;
                } else {
                    ReadContext.DecodedPage page = part.page(physical, spec, row, values);
                    int base = (int) (row - page.firstRow());
                    k = Math.min(k, page.page().rows - base);
                    sources[c] = new BatchColumn.Source(page.page().type, page.page(), base);
                }
            }
            // The rows stay in the key page: moving to the next is a step, never a decode.
            boolean bounded = false;
            for (int i = 1; i < k; i++) {
                cursor.next();
                if (outside(cursor.key(), end, prefix)) {
                    k = i;
                    bounded = true;
                    break;
                }
            }
            Batch batch = new Batch(columns, keys, cursor.pageOffsets(), from, k, sources);
            ctx.checkCancelled();
            boolean more;
            try {
                more = visitor.visit(batch);
            } finally {
                batch.retire();
            }
            if (!more) {
                return false;
            }
            n -= k;
            if (n == 0 || bounded || !cursor.next()) {
                return true;
            }
        }
        return true;
    }

    /** Enters the cursor's row group: checks the projection against its part (COL-3) and resolves the slots. */
    private void enter(PartFile p, int g) {
        if (p == part && g == physical) {
            return;
        }
        values.enterGroup(p.ordinal, g);
        if (p != part) {
            p.checkProjection(columns);
            for (int c = 0; c < specs.length; c++) {
                specs[c] = p.projectable.get(columns.get(c));
            }
        }
        part = p;
        physical = g;
    }

    private static boolean outside(byte[] key, byte[] end, byte[] prefix) {
        return (end != null && Arrays.compareUnsigned(key, end) >= 0)
                || (prefix != null && (key.length < prefix.length || !Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length)));
    }
}
