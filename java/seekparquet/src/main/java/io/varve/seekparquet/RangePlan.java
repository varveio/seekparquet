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
     * within {@code bounds}' start and cursor; or, when {@code bounds} is null, {@code start} itself, the
     * first row of its group of equal keys (a shard boundary).
     */
    private record Span(Queries.Bounds bounds, long start, long end) {}

    /**
     * The projected bytes a plan counts in one row group holding a row of it: its pages holding the plan's
     * rows and its dictionaries; {@code partial} when a projected chunk of the group has no OffsetIndex.
     *
     * @param start the dataset ordinal of the group's first row
     */
    private record GroupBytes(long start, long bytes, boolean partial) {}

    private final Dataset dataset;
    private final List<String> columns;
    /** Disjoint, ascending and not adjacent. */
    private final List<Span> spans;
    private final long rows;
    private final boolean exact;
    private final long bytes;
    private final boolean partial;
    /** The projected bytes of each row group holding a row of the plan, in dataset order. */
    private final List<GroupBytes> groups;
    private final ReadStats stats;

    private RangePlan(Dataset dataset, List<String> columns, List<Span> spans, long rows, boolean exact, List<GroupBytes> groups,
            ReadStats stats) {
        this.dataset = dataset;
        this.columns = columns;
        this.spans = spans;
        this.rows = rows;
        this.exact = exact;
        this.groups = groups;
        long b = 0;
        boolean p = false;
        for (GroupBytes g : groups) {
            b += g.bytes();
            p |= g.partial();
        }
        this.bytes = b;
        this.partial = p;
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

    /** {@return the read statistics of planning, or of a shard {@link #split} made, the reads that placed its end} */
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
     * <p>The scan holds the dataset open while {@code action} runs: {@link Dataset#close()} waits for it.
     * To stop early, throw from {@code action} (or cancel); close the dataset after the scan returns.
     * Closing it from inside {@code action} fails with {@link IllegalStateException}, and {@code action}
     * must not wait for another thread that is closing it.
     *
     * @param cancellation the scan's cancellation
     * @param stats receives the scan's read statistics when it ends, whether it succeeds or fails (zero
     *     counts when it fails to start)
     * @param action receives each row
     * @throws RefusalException if data the scan decodes breaks the specification
     * @throws UnsupportedFeatureException if a page uses a codec or encoding this reader does not implement
     * @throws LimitExceededException if a page declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public void scan(Cancellation cancellation, Consumer<? super ReadStats> stats, Consumer<? super Row> action) {
        try (Dataset.Request request = dataset.begin(cancellation, stats)) {
            ReadContext ctx = request.ctx;
            try {
                ReadContext values = ctx.forPlannedValues();
                KeyCursor cursor = new KeyCursor(dataset.parts(), null, ctx);
                for (Span span : spans) {
                    boolean ok = span.bounds() == null ? cursor.seekOrdinal(span.start()) : span.bounds().position(cursor);
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
     * Divides the plan into at most {@code n} shards; see {@link #split(int, Cancellation)}.
     *
     * @param n the most shards
     * @return the shards, in dataset order
     * @throws RefusalException if a key page decoded at a boundary breaks the specification
     * @throws IllegalStateException if the dataset is closed
     */
    public List<RangePlan> split(int n) {
        return split(n, Cancellation.none());
    }

    /**
     * Divides the plan into at most {@code n} shards, for scans run concurrently: plans over consecutive
     * sub-ranges of its rows, bound to the same dataset, in dataset order. Scanning every shard yields
     * exactly the rows of the plan, each once, with the same offsets and values; within a shard in dataset
     * order, and across shards by shard index. A plan with no rows, or {@code n} below 1, has no shard; a
     * plan with fewer row groups than {@code n} has fewer shards. The library starts no thread: the caller
     * scans the shards as it chooses.
     *
     * <p>A boundary between two shards is a selected row that is the first row of a row group (or of a part
     * no read has checked yet, whose row groups are not known), never inside a group of equal keys: when
     * keys are not unique and a group continues from the row group before, the boundary moves to the next
     * row group that begins a group. The first shard begins and the last ends where the plan does, at its
     * ranges' bounds and cursors. Boundaries are placed to balance {@link #projectedBytes()} when the plan
     * has columns and its bytes are exact, and the row count otherwise.
     *
     * <p>The shards' counts add up to the plan's. Their projected bytes add up to the plan's, since each
     * row group's pages and dictionaries are counted in the one shard holding the group; a shard's bytes
     * are exact when the plan's are. Shards share the dataset's checked parts and page indexes as any
     * concurrent requests do; each scan takes its own cancellation and statistics, and a part a read
     * refuses is refused to every shard touching it.
     *
     * <p>Splitting reads nothing when keys are unique. When they are not, it decodes the two key pages
     * around each boundary inside a part, checking them as a range read does (CHECK-2), to keep groups
     * whole; a shard's {@link #stats()} are the reads that placed its end. Splitting is a convenience of
     * this reader, not a rule of the format.
     *
     * @param n the most shards
     * @param cancellation the split's cancellation
     * @return the shards, in dataset order
     * @throws RefusalException if a key page decoded at a boundary breaks the specification
     * @throws UnsupportedFeatureException if such a page uses a codec or encoding this reader does not implement
     * @throws LimitExceededException if such a page declares a size above the configured limits
     * @throws StorageException if reading storage fails
     * @throws CancellationException if cancelled, interrupted, or the dataset is closed meanwhile
     * @throws IllegalStateException if the dataset is closed
     */
    public List<RangePlan> split(int n, Cancellation cancellation) {
        if (n < 1 || spans.isEmpty()) {
            return List.of();
        }
        try (Dataset.Request request = dataset.begin(cancellation)) {
            Parts parts = dataset.parts();
            List<Long> cuts = new ArrayList<>();
            List<ReadStats> stats = new ArrayList<>();
            cuts(parts, request.ctx, n, cuts, stats);
            List<RangePlan> shards = new ArrayList<>(cuts.size() + 1);
            long lo = 0;
            for (int i = 0; i <= cuts.size(); i++) {
                request.ctx.checkCancelled();
                long hi = i < cuts.size() ? cuts.get(i) : parts.start(parts.size());
                shards.add(shard(parts, lo, hi, stats.get(i)));
                lo = hi;
            }
            return List.copyOf(shards);
        }
    }

    /**
     * Finds at most {@code n - 1} shard boundaries, ascending dataset ordinals each with rows of the plan on
     * both sides, into {@code cuts}, and the read statistics of the shards they divide the plan into, into
     * {@code stats}.
     */
    private void cuts(Parts parts, ReadContext ctx, int n, List<Long> cuts, List<ReadStats> stats) {
        long first = spans.getFirst().start();
        // The candidates are the selected rows that begin a row group, or a part no read has checked, after
        // the plan's first row.
        List<Long> found = new ArrayList<>();
        for (Span span : spans) {
            for (int i = parts.partAt(span.start()); i < parts.size() && parts.start(i) < span.end(); i++) {
                ctx.checkCancelled();
                PartFile p = parts.checked(i);
                long partStart = parts.start(i);
                if (p == null) {
                    if (partStart > first && partStart >= span.start()) {
                        found.add(partStart);
                    }
                    continue;
                }
                long from = Math.max(span.start(), first + 1);
                for (int j = firstGroupAtOrAfter(p, partStart, from); j < p.groups.length; j++) {
                    long g = partStart + p.groupStart[p.groups[j]];
                    if (g >= span.end()) {
                        break;
                    }
                    found.add(g);
                }
            }
        }
        long[] candidates = found.stream().mapToLong(Long::longValue).toArray();
        int m = candidates.length;
        // rows[j] is the plan's rows before candidate j; weight[j] what the boundaries balance: the
        // projected bytes of the row groups before it, or rows.
        long[] rowsBefore = new long[m];
        long whole = 0;
        for (int j = 0, k = 0; j < m; j++) {
            long c = candidates[j];
            for (; k < spans.size() && spans.get(k).end() <= c; k++) {
                whole += spans.get(k).end() - spans.get(k).start();
            }
            rowsBefore[j] = whole;
            if (k < spans.size() && spans.get(k).start() < c) {
                rowsBefore[j] += c - spans.get(k).start();
            }
        }
        long[] weight = rowsBefore;
        long total = rows;
        if (!columns.isEmpty() && !partial && bytes > 0) {
            weight = new long[m];
            total = bytes;
            long before = 0;
            for (int j = 0, k = 0; j < m; j++) {
                for (; k < groups.size() && groups.get(k).start() < candidates[j]; k++) {
                    before += groups.get(k).bytes();
                }
                weight[j] = before;
            }
        }
        // chosen are the candidates nearest the even shares of the weight.
        List<Integer> chosen = new ArrayList<>();
        int shards = Math.min(n, m + 1);
        for (int k = 1; k < shards; k++) {
            double target = (double) total * k / shards;
            int j = firstAtLeast(weight, target);
            if (j > 0 && (j == m || target - weight[j - 1] < weight[j] - target)) {
                j--;
            }
            if (!chosen.isEmpty() && rowsBefore[j] <= rowsBefore[chosen.getLast()]) {
                continue;
            }
            chosen.add(j);
        }
        // Each boundary inside a group of equal keys moves to the next candidate that is not, before the
        // next boundary, or is dropped. The reads of placing a boundary are the stats of the shard it ends:
        // one cursor counts them all, and each accepted boundary takes the reads since the one before.
        KeyCursor cursor = new KeyCursor(parts, null, ctx);
        ReadStats mark = ctx.stats.snapshot();
        for (int i = 0; i < chosen.size(); i++) {
            ctx.checkCancelled();
            int next = i + 1 < chosen.size() ? chosen.get(i + 1) : m;
            int j = chosen.get(i);
            while (before(j, next, m, rowsBefore) && insideGroup(parts, cursor, candidates[j])) {
                j++;
            }
            if (before(j, next, m, rowsBefore)) {
                cuts.add(candidates[j]);
                ReadStats all = ctx.stats.snapshot();
                stats.add(all.minus(mark));
                mark = all;
            }
        }
        stats.add(ctx.stats.snapshot().minus(mark));
    }

    /** {@return the index in {@code p.groups} of the first row group starting at or after dataset row {@code from}} */
    private static int firstGroupAtOrAfter(PartFile p, long partStart, long from) {
        int lo = 0;
        int hi = p.groups.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (partStart + p.groupStart[p.groups[mid]] >= from) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    /** {@return the first index of the non-decreasing {@code weight} at or above {@code target}, or its length} */
    private static int firstAtLeast(long[] weight, double target) {
        int lo = 0;
        int hi = weight.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (weight[mid] >= target) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    /** {@return the index of the first span ending after dataset row {@code row}, or the span count} */
    private int firstSpanEndingAfter(long row) {
        int lo = 0;
        int hi = spans.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (spans.get(mid).end() > row) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    /** {@return the index of the first row group of the plan starting at or after dataset row {@code row}} */
    private int firstGroupFrom(long row) {
        int lo = 0;
        int hi = groups.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (groups.get(mid).start() >= row) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    /** {@return whether candidate j comes before the next chosen boundary, with rows of the plan between} */
    private static boolean before(int j, int next, int m, long[] rowsBefore) {
        return j < next && (next == m || rowsBefore[j] < rowsBefore[next]);
    }

    /**
     * {@return whether row {@code c}, the first of its row group, and the row before it are rows of the plan
     * with equal keys}, so that a boundary at {@code c} would divide their group. Only when keys are not
     * unique and the two rows are in one part (PART-4) does it decode the key pages holding them.
     */
    private boolean insideGroup(Parts parts, KeyCursor cursor, long c) {
        if (dataset.manifest().key().unique() || parts.start(parts.partAt(c)) == c) {
            return false;
        }
        int k = firstSpanEndingAfter(c);
        if (k == spans.size() || spans.get(k).start() >= c) {
            return false;
        }
        if (!cursor.seekOrdinal(c - 1)) {
            return false;
        }
        byte[] before = cursor.key();
        return cursor.next() && java.util.Arrays.equals(before, cursor.key());
    }

    /**
     * {@return the plan's rows in {@code [lo, hi)}}, which are the first rows of row groups (or {@code lo}
     * is 0 and {@code hi} the dataset's row count), with the projected bytes of the row groups holding them.
     * The plan's spans and row groups are ascending and disjoint, so it visits only those in {@code [lo,
     * hi)}, after a binary search: many shards cost no more than few.
     */
    private RangePlan shard(Parts parts, long lo, long hi, ReadStats stats) {
        List<Span> in = new ArrayList<>();
        long count = 0;
        for (int k = firstSpanEndingAfter(lo); k < spans.size(); k++) {
            Span span = spans.get(k);
            if (span.start() >= hi) {
                break;
            }
            Span s = span.start() < lo ? new Span(null, lo, span.end()) : span;
            s = new Span(s.bounds(), s.start(), Math.min(s.end(), hi));
            in.add(s);
            count += s.end() - s.start();
        }
        List<GroupBytes> held = new ArrayList<>();
        for (int g = firstGroupFrom(lo); g < groups.size() && groups.get(g).start() < hi; g++) {
            held.add(groups.get(g));
        }
        return new RangePlan(dataset, columns, List.copyOf(in), count, checked(parts, in), List.copyOf(held), stats);
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
        List<GroupBytes> groups = columns.isEmpty() ? List.of() : planColumns(parts, spans, columns, ctx);
        return new RangePlan(dataset, columns, List.copyOf(spans), rows, checked(parts, spans), groups, ctx.stats.snapshot());
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
     * Checks every part holding a row of the plan and {@return the projected bytes of each row group holding
     * a row}: per column chunk, its dictionary's extent and the extents of the pages holding the plan's rows,
     * each page once. It keeps the OffsetIndexes of one row group's chunks at a time.
     */
    private static List<GroupBytes> planColumns(Parts parts, List<Span> spans, List<String> columns, ReadContext ctx) {
        List<GroupBytes> groups = new ArrayList<>();
        int currentPart = -1;
        int currentGroup = -1;
        long groupFirst = 0;
        long total = 0;
        boolean partial = false;
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
                        if (currentPart >= 0) {
                            groups.add(new GroupBytes(groupFirst, total, partial));
                        }
                        currentPart = i;
                        currentGroup = physical;
                        groupFirst = groupStart;
                        total = 0;
                        partial = false;
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
                                total += p.dictionaryBytes(physical, spec.leaf(), c.index);
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
                            total += c.index.size[k];
                        }
                        c.last = Math.max(c.last, last);
                    }
                }
            }
        }
        if (currentPart >= 0) {
            groups.add(new GroupBytes(groupFirst, total, partial));
        }
        return List.copyOf(groups);
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
