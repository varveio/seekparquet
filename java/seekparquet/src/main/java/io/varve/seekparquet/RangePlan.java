// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;

/**
 * A selection planned before it is read: every row of a set of ranges, its row count, and the
 * compressed bytes that reading the projected columns of those rows will request. Made by {@link
 * Dataset#planRanges}. A plan is bound to the dataset that made it, and its {@link #scan scan} fails
 * with {@link IllegalStateException} once the dataset is closed. It is immutable and safe for
 * concurrent use.
 *
 * <p>A plan certifies what it decoded: the key pages at the two ends of each range, checked as a range
 * read checks them (CHECK-2). Its count is derived from structure, not from decoding the rows between
 * the ends (CHECK-4): the positions the ends decode, the row counts of the row groups, from footers a
 * part check verified (CHECK-1), and the OffsetIndex row positions. A scan decodes every row and checks
 * every key page it reads.
 */
public final class RangePlan {

    /**
     * The selected dataset rows {@code [start, end)}, and where a scan finds {@code start}: the first row
     * within {@code bounds}' start and cursor.
     */
    private record Span(Queries.Bounds bounds, long start, long end) {}

    private final Dataset dataset;
    private final List<String> columns;
    /** Disjoint, ascending and not adjacent. */
    private final List<Span> spans;
    private final long rows;
    private final boolean exact;
    private final long bytes;
    private final boolean partial;
    private final ReadStats stats;

    private RangePlan(Dataset dataset, List<String> columns, List<Span> spans, long rows, boolean exact, long bytes, boolean partial,
            ReadStats stats) {
        this.dataset = dataset;
        this.columns = columns;
        this.spans = spans;
        this.rows = rows;
        this.exact = exact;
        this.bytes = bytes;
        this.partial = partial;
        this.stats = stats;
    }

    /**
     * {@return the number of rows a scan yields} It is exact when every part holding one of them is
     * checked (CHECK-1), so that the row counts it adds up are the parts' footers', verified against the
     * manifest: always for a plan with columns, which checks those parts. Otherwise it relies on the
     * manifest's row count of a part no read has touched yet, and {@link #countExact()} is false.
     */
    public long count() {
        return rows;
    }

    /** {@return whether {@link #count()} is exact: every part holding a row of the plan was checked} */
    public boolean countExact() {
        return exact;
    }

    /**
     * {@return the compressed bytes a scan requests from storage for the projected columns} For each
     * column chunk the rows span, the extents its OffsetIndex gives the pages holding them and, when the
     * chunk has a dictionary page, the dictionary's (the bytes before the first data page), each counted
     * once however many ranges share it. Footers, page indexes and key pages are not counted, nor is the
     * key column when projected: its values come from the key pages. A scan reads each counted extent in
     * one request and no other byte of those chunks. A chunk without an OffsetIndex (COL-4) is walked
     * through its page headers by a scan and omitted here; {@link #projectedBytesExact()} is then false.
     */
    public long projectedBytes() {
        return bytes;
    }

    /** {@return whether {@link #projectedBytes()} is exact: every projected chunk the rows span has an OffsetIndex} */
    public boolean projectedBytesExact() {
        return !partial;
    }

    /** {@return the projected columns, in order} */
    public List<String> columns() {
        return columns;
    }

    /** {@return the read statistics of planning} */
    public ReadStats stats() {
        return stats;
    }

    /**
     * Hands {@code action} every row of the plan, with its projected values, in dataset order; see {@link
     * #scan(Cancellation, Consumer, Consumer)}.
     *
     * @param action receives each row
     * @throws IllegalStateException if the dataset is closed
     */
    public void scan(Consumer<? super Row> action) {
        scan(Cancellation.none(), stats -> {}, action);
    }

    /**
     * Hands {@code action} every row of the plan, ending early when {@code cancellation} is cancelled;
     * see {@link #scan(Cancellation, Consumer, Consumer)}.
     *
     * @param cancellation the scan's cancellation
     * @param action receives each row
     * @throws IllegalStateException if the dataset is closed
     */
    public void scan(Cancellation cancellation, Consumer<? super Row> action) {
        scan(cancellation, stats -> {}, action);
    }

    /**
     * Hands {@code action} every row of the plan, with its projected values, in dataset order. An
     * exception {@code action} throws ends the scan and propagates. The scan positions at each range as a
     * range read does and decodes each key page and projected page holding the plan's rows once,
     * checking every key page it decodes (CHECK-2).
     *
     * @param cancellation the scan's cancellation
     * @param stats receives the scan's read statistics when it ends, whether it succeeds or fails
     * @param action receives each row
     * @throws RefusalException if data the scan decodes breaks the specification
     * @throws UnsupportedFeatureException if a page uses a codec or encoding this reader does not implement
     * @throws LimitExceededException if a page declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public void scan(Cancellation cancellation, Consumer<? super ReadStats> stats, Consumer<? super Row> action) {
        try (Dataset.Request request = dataset.begin(cancellation)) {
            ReadContext ctx = request.ctx;
            try {
                ReadContext values = ctx.forPlannedValues();
                KeyCursor cursor = new KeyCursor(dataset.parts(), null, ctx);
                for (Span span : spans) {
                    boolean ok = span.bounds().position(cursor);
                    for (long o = span.start(); ok && o < span.end(); o++) {
                        values.enterGroup(cursor.partFile().ordinal, cursor.physical());
                        action.accept(Queries.row(cursor, columns, values));
                        if (o + 1 < span.end()) {
                            ok = cursor.next();
                        }
                    }
                }
            } finally {
                stats.accept(ctx.stats.snapshot());
            }
        }
    }

    /**
     * Plans {@code ranges}; see {@link Dataset#planRanges}. Each range's ends are found by one cursor, and
     * every key page planning decodes is checked against the others through the request's context.
     */
    static RangePlan plan(Dataset dataset, Parts parts, List<RangeQuery> ranges, List<String> columns, ReadContext ctx) {
        List<Span> found = new ArrayList<>();
        for (RangeQuery q : ranges) {
            Queries.Bounds b = Queries.Bounds.of(q);
            if (parts.size() == 0 || b.empty()) {
                continue;
            }
            KeyCursor cursor = new KeyCursor(parts, b.end(), ctx);
            if (!b.position(cursor)) {
                continue;
            }
            if (b.end() != null && java.util.Arrays.compareUnsigned(cursor.key(), b.end()) >= 0) {
                continue;
            }
            long start = cursor.ordinal();
            long end = parts.start(parts.size());
            if (b.end() != null) {
                // The first row at or after the upper bound, or where the cursor stopped short of it.
                end = cursor.seek(b.end(), false) ? cursor.ordinal() : cursor.bound();
            }
            found.add(new Span(b, start, end));
        }
        found.sort(Comparator.comparingLong(Span::start));
        List<Span> spans = new ArrayList<>();
        for (Span s : found) {
            int n = spans.size();
            if (n > 0 && s.start() <= spans.get(n - 1).end()) {
                Span last = spans.get(n - 1);
                spans.set(n - 1, new Span(last.bounds(), last.start(), Math.max(last.end(), s.end())));
            } else {
                spans.add(s);
            }
        }
        long rows = 0;
        for (Span s : spans) {
            rows += s.end() - s.start();
        }
        long[] bytes = {0};
        boolean partial = !columns.isEmpty() && planColumns(parts, spans, columns, ctx, bytes);
        return new RangePlan(dataset, columns, List.copyOf(spans), rows, checked(parts, spans), bytes[0], partial,
                ctx.stats.snapshot());
    }

    /** A projected column chunk being counted: its OffsetIndex (null without one) and the last page counted. */
    private static final class Chunk {
        final ValueIndex index;
        int last = -1;

        Chunk(ValueIndex index) {
            this.index = index;
        }
    }

    /**
     * Checks every part holding a row of the plan and adds up the projected bytes into {@code total[0]}:
     * per column chunk, its dictionary's extent and the extents of the pages holding the plan's rows, each
     * page once. It keeps the OffsetIndexes of one row group's chunks at a time. {@return whether a
     * projected chunk lacks an OffsetIndex}
     */
    private static boolean planColumns(Parts parts, List<Span> spans, List<String> columns, ReadContext ctx, long[] total) {
        boolean partial = false;
        int currentPart = -1;
        int currentGroup = -1;
        List<Chunk> chunks = List.of();
        for (Span span : spans) {
            for (int i = parts.partAt(span.start()); i < parts.size() && parts.start(i) < span.end(); i++) {
                PartFile p = parts.get(i, ctx);
                p.checkProjection(columns);
                long partStart = parts.start(i);
                for (int physical : p.groups) {
                    long groupStart = partStart + p.groupStart[physical];
                    long groupEnd = groupStart + p.groupRows[physical];
                    if (groupEnd <= span.start()) {
                        continue;
                    }
                    if (groupStart >= span.end()) {
                        break;
                    }
                    ctx.checkCancelled();
                    long from = Math.max(span.start(), groupStart) - groupStart;
                    long to = Math.min(span.end(), groupEnd) - groupStart;
                    if (i != currentPart || physical != currentGroup) {
                        currentPart = i;
                        currentGroup = physical;
                        chunks = new ArrayList<>();
                        List<Integer> leaves = new ArrayList<>();
                        for (String name : columns) {
                            ColumnSpec spec = p.projectable.get(name);
                            // Absent (COL-2), the key (decoded from its key pages), or a repeated name: no bytes of its own.
                            if (spec == null || spec.leaf() == p.keyLeaf || leaves.contains(spec.leaf())) {
                                continue;
                            }
                            leaves.add(spec.leaf());
                            p.checkCodec(physical, spec.leaf());
                            Optional<ValueIndex> index = p.valueIndex(physical, spec.leaf(), ctx);
                            Chunk c = new Chunk(index.orElse(null));
                            if (c.index != null) {
                                total[0] += p.dictionaryBytes(physical, spec.leaf(), c.index);
                            }
                            chunks.add(c);
                        }
                    }
                    for (Chunk c : chunks) {
                        if (c.index == null) {
                            partial = true;
                            continue;
                        }
                        int first = c.index.pageOf(from);
                        int last = c.index.pageOf(to - 1);
                        for (int k = Math.max(first, c.last + 1); k <= last; k++) {
                            total[0] += c.index.size[k];
                        }
                        c.last = Math.max(c.last, last);
                    }
                }
            }
        }
        return partial;
    }

    /** {@return whether every part holding a row of the plan is checked} */
    private static boolean checked(Parts parts, List<Span> spans) {
        for (Span span : spans) {
            for (int i = parts.partAt(span.start()); i < parts.size() && parts.start(i) < span.end(); i++) {
                if (parts.checked(i) == null) {
                    return false;
                }
            }
        }
        return true;
    }
}
