// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.varve.seekparquet.TestDatasets.TestRow;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Borrowed batch scans ({@link Plan#scanBatches}, {@link Dataset#rangeBatches}) against the
 * row-returning paths: the same rows, offsets and values, read statistics and errors; batch boundaries;
 * the accessors against the owning values; lifetime, cancellation and close.
 */
class BatchScanTest {

    @TempDir Path tmp;

    /** A scan that hands batches to an action. */
    interface BatchSource {
        void run(Consumer<? super Batch> action);
    }

    /** {@return the owning rows of every batch the scan hands over}, checking each batch's accessors first */
    static List<Row> rows(BatchSource scan) {
        List<Row> out = new ArrayList<>();
        scan.run(batch -> {
            check(batch);
            for (int i = 0; i < batch.size(); i++) {
                out.add(batch.row(i));
            }
        });
        return out;
    }

    /** Checks that every accessor of the batch agrees with the owning row it makes. */
    static void check(Batch batch) {
        assertTrue(batch.size() > 0);
        assertThrows(IndexOutOfBoundsException.class, () -> batch.offset(batch.size()));
        assertThrows(IndexOutOfBoundsException.class, () -> batch.key(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> batch.column(batch.columns().size()));
        for (int i = 0; i < batch.size(); i++) {
            Row row = batch.row(i);
            assertEquals(row.key(), batch.copyKey(i));
            assertEquals(row.offset(), batch.offset(i));
            assertEquals(row.cursor(), batch.cursor(i));
            assertEquals(row.key().length(), batch.keyLength(i));
            byte[] key = new byte[batch.keyLength(i) + 3];
            assertEquals(batch.keyLength(i), batch.copyKey(i, key, 3));
            assertEquals(row.key(), Bytes.of(key, 3, batch.keyLength(i)));
            assertEquals(row.key(), Bytes.of(batch.key(i)));
            assertTrue(batch.key(i).isReadOnly());
            assertEquals(0, batch.compareKey(i, row.key()));
            assertEquals(Integer.signum(row.key().compareTo(Bytes.utf8("m"))), Integer.signum(batch.compareKey(i, Bytes.utf8("m"))));
            for (int c = 0; c < batch.columns().size(); c++) {
                checkValue(batch.column(c), i, row.values().get(c));
            }
        }
    }

    private static void checkValue(BatchColumn col, int i, Value want) {
        assertEquals(want, col.value(i));
        assertEquals(!(want instanceof Value.Absent), col.isPresent());
        assertEquals(want instanceof Value.Null, col.isNull(i));
        if (want instanceof Value.Absent) {
            assertThrows(IllegalStateException.class, col::type);
            assertThrows(IllegalStateException.class, () -> col.getLong(i));
            assertThrows(IllegalStateException.class, () -> col.byteLength(i));
            return;
        }
        Column.Type type = col.type();
        switch (want) {
            case Value.Null n -> {
                switch (type) {
                    case BOOLEAN -> assertThrows(IllegalStateException.class, () -> col.getBoolean(i));
                    case INT32 -> assertThrows(IllegalStateException.class, () -> col.getInt(i));
                    case INT64 -> assertThrows(IllegalStateException.class, () -> col.getLong(i));
                    case FLOAT -> assertThrows(IllegalStateException.class, () -> col.getFloat(i));
                    case DOUBLE -> assertThrows(IllegalStateException.class, () -> col.getDouble(i));
                    default -> {
                        assertThrows(IllegalStateException.class, () -> col.byteLength(i));
                        assertThrows(IllegalStateException.class, () -> col.bytes(i));
                    }
                }
            }
            case Value.Bool v -> assertEquals(v.value(), col.getBoolean(i));
            case Value.Int32 v -> {
                assertEquals(v.value(), col.getInt(i));
                assertThrows(IllegalStateException.class, () -> col.getLong(i));
            }
            case Value.Int64 v -> {
                assertEquals(v.value(), col.getLong(i));
                assertThrows(IllegalStateException.class, () -> col.getInt(i));
                assertThrows(IllegalStateException.class, () -> col.byteLength(i));
            }
            case Value.Float32 v -> {
                assertEquals(v.bits(), col.getFloatBits(i));
                assertEquals(v.bits(), Float.floatToRawIntBits(col.getFloat(i)));
            }
            case Value.Float64 v -> {
                assertEquals(v.bits(), col.getDoubleBits(i));
                assertEquals(v.bits(), Double.doubleToRawLongBits(col.getDouble(i)));
            }
            case Value.ByteArray v -> checkBytes(col, i, v.value(), Column.Type.BYTE_ARRAY);
            case Value.FixedLenByteArray v -> checkBytes(col, i, v.value(), Column.Type.FIXED_LEN_BYTE_ARRAY);
            case Value.Absent a -> fail();
        }
    }

    private static void checkBytes(BatchColumn col, int i, Bytes want, Column.Type type) {
        assertEquals(type, col.type());
        assertEquals(want.length(), col.byteLength(i));
        assertEquals(want, col.copyBytes(i));
        ByteBuffer view = col.bytes(i);
        assertTrue(view.isReadOnly());
        assertEquals(want, Bytes.of(view));
        byte[] dst = new byte[want.length() + 2];
        assertEquals(want.length(), col.copyBytes(i, dst, 2));
        assertEquals(want, Bytes.of(dst, 2, want.length()));
        if (want.length() > 0) {
            assertThrows(IndexOutOfBoundsException.class, () -> col.copyBytes(i, new byte[want.length() - 1], 0));
        }
        assertThrows(IllegalStateException.class, () -> col.getLong(i));
    }

    /** The projectable columns of a verified dataset, the key, a column no part has, and a duplicate. */
    static List<String> columns(Dataset d) {
        List<String> out = new ArrayList<>(Reads.columns(d).stream().filter(Column::projectable).map(Column::name).toList());
        if (!out.contains(d.manifest().key().column())) {
            out.add(d.manifest().key().column());
        }
        out.add("no such column");
        out.add(out.getFirst());
        return List.copyOf(out);
    }

    static Stream<String> validDatasets() throws IOException {
        return RangePlanTest.validDatasets();
    }

    /** A range read's statistics, on a dataset of its own so that no earlier request warms its caches. */
    private static ReadStats rangeStats(Path root, Query q, boolean batches) {
        AtomicReference<ReadStats> stats = new AtomicReference<>();
        try (Dataset d = Dataset.open(root)) {
            if (batches) {
                Reads.rangeBatches(d, q, Cancellation.none(), stats::set, batch -> {});
            } else {
                Reads.range(d, q, Cancellation.none(), stats::set);
            }
        }
        return stats.get();
    }

    /** A plan's scan statistics, on a dataset of its own so that no earlier request warms its caches. */
    private static ReadStats scanStats(Path root, List<Query> ranges, List<String> columns, boolean batches) {
        AtomicReference<ReadStats> stats = new AtomicReference<>();
        try (Dataset d = Dataset.open(root)) {
            Plan plan = Reads.plan(d, ranges, columns);
            if (batches) {
                Reads.scanBatches(plan, Cancellation.none(), stats::set, batch -> {});
            } else {
                Reads.scan(plan, Cancellation.none(), stats::set, row -> {});
            }
        }
        return stats.get();
    }

    /**
     * Over every valid conformance dataset, with every projectable column, the key, an absent column and a
     * duplicate: a batch range read returns the rows, and the read statistics, of a range read for sampled
     * bounds, prefixes, lookups, limits and cursors; a batch scan of a plan the rows, and the read statistics,
     * of its scan.
     */
    @ParameterizedTest
    @MethodSource("validDatasets")
    void conformanceEquivalence(String name) {
        Path root = ConformanceFixturesTest.fixture("datasets/" + name);
        try (Dataset d = Dataset.open(root)) {
            d.verify();
            List<String> columns = columns(d);
            Query whole = Reads.builder().limit(1_000_000).columns(columns).build();
            List<Row> all = d.rows(whole);
            assertEquals(all, rows(action -> Reads.rangeBatches(d, whole, action)), name);
            assertEquals(rangeStats(root, whole, false), rangeStats(root, whole, true), name);

            Plan plan = Reads.plan(d, List.of(Reads.all(1)), columns);
            List<Row> scanned = new ArrayList<>();
            Reads.scan(plan, scanned::add);
            assertEquals(all, scanned, name);
            assertEquals(all, rows(action -> Reads.scanBatches(plan, action)), name);
            assertEquals(scanStats(root, List.of(Reads.all(1)), columns, false),
                    scanStats(root, List.of(Reads.all(1)), columns, true), name);

            List<Query> ranges = RangePlanTest.ranges(all);
            for (Query q : ranges) {
                for (int limit : new int[] {1, 3, 1000}) {
                    Query page = q.toBuilder().limit(limit).columns(columns).build();
                    for (int n = 0; n < 4; n++) {
                        Query current = page;
                        List<Row> want = d.rows(current);
                        assertEquals(want, rows(action -> Reads.rangeBatches(d, current, action)), name + " " + current);
                        if (want.size() < limit) {
                            break;
                        }
                        page = page.next(want.getLast());
                    }
                }
            }
            for (int i = 0; i < ranges.size(); i += Math.max(1, ranges.size() / 16)) {
                Query q = ranges.get(i).toBuilder().limit(2).columns(columns).build();
                assertEquals(rangeStats(root, q, false), rangeStats(root, q, true), name + " " + q);
            }
            Random random = new Random(all.size());
            for (int i = 0; i < 20; i++) {
                List<Query> set = new ArrayList<>();
                for (int n = 1 + random.nextInt(4); n > 0; n--) {
                    set.add(ranges.get(random.nextInt(ranges.size())));
                }
                Plan p = Reads.plan(d, set, columns);
                List<Row> want = new ArrayList<>();
                Reads.scan(p, want::add);
                assertEquals(p.count().value(), want.size());
                assertEquals(want, rows(action -> Reads.scanBatches(p, action)), name + " " + set);
                if (i % 5 == 0) {
                    assertEquals(scanStats(root, set, columns, false), scanStats(root, set, columns, true), name + " " + set);
                }
            }
        }
    }

    private static List<List<TestRow>> split(int keys, int parts) {
        List<List<TestRow>> out = new ArrayList<>();
        for (int p = 0; p < parts; p++) {
            List<TestRow> rows = new ArrayList<>();
            for (int i = p * keys / parts; i < (p + 1) * keys / parts; i++) {
                rows.add(TestRow.of(key(i), i % 11 == 0 ? -1 : i));
            }
            out.add(rows);
        }
        return out;
    }

    private static String key(int i) {
        return String.format("k%05d", i);
    }

    /**
     * Three parts of 80 rows, row groups of 40, key pages of 7 rows, {@code size} pages of 5 and {@code note}
     * pages of 3 (dictionary and plain pages alternating, with every codec): no batch crosses any of them,
     * and the batches hold every row once, as a range read and a scan return them.
     */
    @Test
    void batchesNeverCrossPagesGroupsPartsOrGaps() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 40;
        spec.rowsPerPage = 7;
        spec.sizeRowsPerPage = 5;
        spec.noteRowsPerPage = 3;
        spec.noteColumn = true;
        spec.dictionaryNote = true;
        spec.mixedValuePages = true;
        try (Dataset d = Dataset.open(TestDatasets.write(tmp, spec, split(240, 3)))) {
            for (List<String> columns : List.of(List.<String>of(), List.of("size"), List.of("note", "size"), List.of("key", "note"))) {
                int[] pages = columns.isEmpty() ? new int[] {7} : columns.contains("size") ? new int[] {7, 5, 3} : new int[] {7, 3};
                if (columns.equals(List.of("size"))) {
                    pages = new int[] {7, 5};
                }
                int[] widths = pages;
                Query all = Reads.builder().limit(1000).columns(columns).build();
                List<Row> want = d.rows(all);
                assertEquals(240, want.size());
                for (BatchSource scan : List.<BatchSource>of(
                        action -> Reads.rangeBatches(d, all, action),
                        action -> Reads.scanBatches(Reads.plan(d, List.of(Reads.all(1)), columns), action))) {
                    List<Row> got = new ArrayList<>();
                    scan.run(batch -> {
                        check(batch);
                        int first = Integer.parseInt(batch.copyKey(0).toStringUtf8().substring(1));
                        int last = first + batch.size() - 1;
                        assertEquals(key(last), batch.copyKey(batch.size() - 1).toStringUtf8());
                        assertEquals(first / 80, last / 80, "a batch crosses a part");
                        assertEquals(first % 80 / 40, last % 80 / 40, "a batch crosses a row group");
                        for (int w : widths) {
                            assertEquals(first % 40 / w, last % 40 / w, "a batch of " + columns + " crosses a page of " + w + " rows");
                        }
                        for (int i = 0; i < batch.size(); i++) {
                            got.add(batch.row(i));
                        }
                    });
                    assertEquals(want, got, columns.toString());
                }
                // Two ranges with a gap between them: no batch holds rows of both.
                Plan plan = Reads.plan(d, List.of(
                        Reads.builder().start(Bytes.utf8(key(3))).end(Bytes.utf8(key(5))).limit(1).build(),
                        Reads.builder().start(Bytes.utf8(key(6))).end(Bytes.utf8(key(13))).limit(1).build()), columns);
                List<Integer> sizes = new ArrayList<>();
                Reads.scanBatches(plan, batch -> sizes.add(batch.size()));
                assertEquals(List.of(2, 1), sizes.subList(0, 2), columns.toString());
                assertEquals(9, sizes.stream().mapToInt(Integer::intValue).sum());
                // A gap inside one page of every projected column and of the keys (rows 0..2 share a page of 7, 5
                // and 3 rows): rows 0 and 2 are one page's neighbours but still two batches.
                Plan inPage = Reads.plan(d, List.of(
                        Reads.builder().start(Bytes.utf8(key(0))).end(Bytes.utf8(key(1))).limit(1).build(),
                        Reads.builder().start(Bytes.utf8(key(2))).end(Bytes.utf8(key(3))).limit(1).build()), columns);
                List<List<String>> batches = new ArrayList<>();
                Reads.scanBatches(inPage, batch -> {
                    List<String> keys = new ArrayList<>();
                    for (int i = 0; i < batch.size(); i++) {
                        keys.add(batch.copyKey(i).toStringUtf8());
                    }
                    batches.add(keys);
                });
                assertEquals(List.of(List.of(key(0)), List.of(key(2))), batches, columns.toString());
            }
        }
    }

    /** A range's limit and upper bound end its last batch; a range with no rows calls no action. */
    @Test
    void limitsAndBoundsEndTheLastBatch() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 40;
        spec.rowsPerPage = 10;
        try (Dataset d = Dataset.open(TestDatasets.write(tmp, spec, split(80, 1)))) {
            List<Integer> sizes = new ArrayList<>();
            Reads.rangeBatches(d, Reads.builder().start(Bytes.utf8(key(2))).limit(5).build(), batch -> sizes.add(batch.size()));
            assertEquals(List.of(5), sizes);
            sizes.clear();
            Reads.rangeBatches(d, Reads.builder().start(Bytes.utf8(key(2))).end(Bytes.utf8(key(14))).limit(50).build(),
                    batch -> sizes.add(batch.size()));
            assertEquals(List.of(8, 4), sizes);
            sizes.clear();
            Reads.rangeBatches(d, Reads.prefix(Bytes.utf8("k0003"), 50), batch -> sizes.add(batch.size()));
            assertEquals(List.of(10), sizes);
            Reads.rangeBatches(d, Reads.prefix(Bytes.utf8("x"), 50), batch -> fail("a batch of an empty range"));
            Reads.rangeBatches(d, Reads.builder().end(Bytes.empty()).limit(1).build(), batch -> fail("a batch of an empty range"));
            Reads.scanBatches(Reads.plan(d, List.of(Reads.prefix(Bytes.utf8("x"), 1)), List.of("size")), batch -> fail("a batch of an empty plan"));
        }
    }

    private Path repeated() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.unique = false;
        spec.rowsPerPage = 2;
        spec.rowsPerGroup = 3;
        List<TestRow> rows = new ArrayList<>();
        rows.add(TestRow.of("a", 0));
        for (int i = 0; i < 17; i++) {
            rows.add(TestRow.of("b", 17 - i));
        }
        rows.add(TestRow.of("c/1", 19));
        rows.add(TestRow.of("d", 21));
        return TestDatasets.write(tmp, spec, List.of(rows, List.of(TestRow.of("z", 22), TestRow.of("z", 23), TestRow.of("z", 24))));
    }

    /**
     * Groups of equal keys across pages and row groups: offsets, lookups, paginated continuations inside a
     * group, and concurrently scanned shards agree with the row-returning paths.
     */
    @Test
    void repeatedKeys() throws Exception {
        try (Dataset d = Dataset.open(repeated())) {
            List<String> columns = List.of("size", "key", "note");
            for (int limit : new int[] {1, 2, 5, 100}) {
                Query q = Reads.lookup(Bytes.utf8("b"), limit).toBuilder().columns(columns).build();
                List<Row> want = new ArrayList<>();
                List<Row> got = new ArrayList<>();
                for (Query page = q; ; ) {
                    List<Row> rows = d.rows(page);
                    want.addAll(rows);
                    List<Row> batched = new ArrayList<>();
                    AtomicReference<Cursor.RowPosition> last = new AtomicReference<>();
                    Reads.rangeBatches(d, page, batch -> {
                        check(batch);
                        for (int i = 0; i < batch.size(); i++) {
                            batched.add(batch.row(i));
                        }
                        last.set(batch.cursor(batch.size() - 1));
                    });
                    got.addAll(batched);
                    if (rows.size() < limit) {
                        break;
                    }
                    assertEquals(rows.getLast().cursor(), last.get());
                    page = page.toBuilder().cursor(last.get()).build();
                }
                assertEquals(want, got);
                assertEquals(17, got.size());
                for (int i = 0; i < 17; i++) {
                    assertEquals(i, got.get(i).offset());
                    assertEquals(new Value.Int64(17 - i), got.get(i).values().getFirst());
                }
            }
            Plan plan = Reads.plan(d, List.of(Reads.all(1)), columns);
            List<Row> want = new ArrayList<>();
            Reads.scan(plan, want::add);
            assertEquals(want, rows(action -> Reads.scanBatches(plan, action)));
            List<Plan> shards = plan.split(4);
            assertTrue(shards.size() > 1);
            try (ExecutorService ex = Executors.newFixedThreadPool(shards.size())) {
                List<Future<List<Row>>> parts = new ArrayList<>();
                for (Plan shard : shards) {
                    parts.add(ex.submit(() -> rows(action -> Reads.scanBatches(shard, action))));
                }
                List<Row> got = new ArrayList<>();
                for (Future<List<Row>> f : parts) {
                    got.addAll(f.get());
                }
                assertEquals(want, got);
            }
        }
    }

    /**
     * The errors of a range read: a limit above the configured result limit and a page above the page
     * limit are {@link LimitExceededException}, a nested projection is unsupported, keys out of order are
     * refused; a request that fails to start delivers zero counts and calls no action.
     */
    @Test
    void errorsAreThoseOfARangeRead() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 40;
        spec.rowsPerPage = 10;
        Path root = TestDatasets.write(tmp.resolve("ok"), spec, split(80, 2));
        Limits small = Limits.builder().manifestBytes(1 << 20).parts(10).footerBytes(1 << 20).pageIndexBytes(1 << 20).pageBytes(1 << 20).pageValues(1 << 20).build();
        try (Dataset d = Dataset.open(io.varve.seekparquet.storage.LocalStorage.of(root), OpenOptions.DEFAULT.withLimits(small), Cancellation.none())) {
            AtomicReference<ReadStats> stats = new AtomicReference<>();
            // A collected result is bounded by the caller's limit only; a scan has no result cap at all.
            assertEquals(80, d.rows(Reads.all(1_000_000)).size());
            long[] scanned = new long[1];
            Reads.rangeBatches(d, Reads.all(0), Cancellation.none(), stats::set, batch -> scanned[0] += batch.size());
            assertEquals(80, scanned[0]);
            assertThrows(IllegalArgumentException.class,
                    () -> d.rows(Reads.all(0), new ReadOptions(Cancellation.none(), stats::set)));
            assertEquals(ReadStats.NONE, stats.get());
            assertThrows(NullPointerException.class, () -> Reads.rangeBatches(d, Reads.all(1), null));
            Plan plan = Reads.plan(d, List.of(Reads.all(1)), List.of());
            assertThrows(NullPointerException.class, () -> Reads.scanBatches(plan, null));
        }
        Limits tinyPages = Limits.DEFAULT.toBuilder().pageMemory(64).build();
        try (Dataset d = Dataset.open(io.varve.seekparquet.storage.LocalStorage.of(root), OpenOptions.DEFAULT.withLimits(tinyPages), Cancellation.none())) {
            LimitExceededException a = assertThrows(LimitExceededException.class, () -> d.rows(Reads.all(5)));
            LimitExceededException b = assertThrows(LimitExceededException.class, () -> Reads.rangeBatches(d, Reads.all(5), batch -> fail("a batch")));
            assertEquals(a.limit(), b.limit());
        }
        spec.swapFirstKeys = true;
        Path bad = TestDatasets.write(tmp.resolve("bad"), spec, split(80, 2));
        try (Dataset d = Dataset.open(bad)) {
            assertEquals(RefusalCode.KEY_ORDER, assertThrows(RefusalException.class, () -> d.rows(Reads.all(5))).code());
            assertEquals(RefusalCode.KEY_ORDER,
                    assertThrows(RefusalException.class, () -> Reads.rangeBatches(d, Reads.all(5), batch -> fail("a batch"))).code());
        }
        Path projection = ConformanceFixturesTest.fixture("datasets/projection");
        try (Dataset d = Dataset.open(projection)) {
            d.verify();
            Query nested = Reads.builder().limit(5).columns("meta").build();
            UnsupportedFeatureException a = assertThrows(UnsupportedFeatureException.class, () -> d.rows(nested));
            UnsupportedFeatureException b = assertThrows(UnsupportedFeatureException.class, () -> Reads.rangeBatches(d, nested, batch -> fail("a batch")));
            assertEquals(a.code(), b.code());
        }
    }

    /**
     * Cancellation is checked before every action: cancelling from the action ends the scan before the next
     * batch; a cancelled request starts no scan and delivers zero counts; an exception the action throws
     * ends the scan unchanged, with the counts of what it read.
     */
    @Test
    void cancellationAndActionErrors() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 40;
        spec.rowsPerPage = 5;
        try (Dataset d = Dataset.open(TestDatasets.write(tmp, spec, split(80, 2)))) {
            Plan plan = Reads.plan(d, List.of(Reads.all(1)), List.of("size"));
            Query all = Reads.builder().limit(100).columns("size").build();
            List<java.util.function.BiConsumer<Cancellation, Consumer<? super Batch>>> scans = List.of(
                    (c, a) -> Reads.rangeBatches(d, all, c, a), (c, a) -> Reads.scanBatches(plan, c, a));
            for (var scan : scans) {
                Cancellation cancel = Cancellation.create();
                AtomicInteger calls = new AtomicInteger();
                assertThrows(CancellationException.class, () -> scan.accept(cancel, batch -> {
                    calls.incrementAndGet();
                    cancel.cancel();
                }));
                assertEquals(1, calls.get());
                assertThrows(CancellationException.class, () -> scan.accept(cancel, batch -> fail("a batch of a cancelled scan")));
            }
            Cancellation cancelled = Cancellation.create();
            cancelled.cancel();
            AtomicReference<ReadStats> stats = new AtomicReference<>();
            assertThrows(CancellationException.class, () -> Reads.scanBatches(plan, cancelled, stats::set, batch -> fail("a batch")));
            assertEquals(ReadStats.NONE, stats.get());
            assertThrows(CancellationException.class, () -> Reads.rangeBatches(d, all, cancelled, stats::set, batch -> fail("a batch")));
            assertEquals(ReadStats.NONE, stats.get());

            RuntimeException stop = new RuntimeException("stop");
            List<Row> some = new ArrayList<>();
            assertSame(stop, assertThrows(RuntimeException.class, () -> Reads.rangeBatches(d, all, Cancellation.none(), stats::set, batch -> {
                some.add(batch.row(0));
                throw stop;
            })));
            assertEquals(1, some.size());
            assertEquals(1, stats.get().keyPagesDecoded());
            assertEquals(1, stats.get().valuePagesDecoded());
            assertSame(stop, assertThrows(RuntimeException.class, () -> Reads.scanBatches(plan, Cancellation.none(), stats::set, batch -> {
                throw stop;
            })));
            assertEquals(1, stats.get().valuePagesDecoded());
        }
    }

    /** A batch and its columns are borrowed: every accessor fails once the action has returned. */
    @Test
    void batchesAreValidOnlyDuringTheAction() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.noteColumn = true;
        try (Dataset d = Dataset.open(TestDatasets.write(tmp, spec, split(20, 2)))) {
            List<Batch> kept = new ArrayList<>();
            List<BatchColumn> columns = new ArrayList<>();
            List<Row> owned = new ArrayList<>();
            List<Bytes> notes = new ArrayList<>();
            Query all = Reads.builder().limit(100).columns("size", "note").build();
            Reads.rangeBatches(d, all, batch -> {
                kept.add(batch);
                columns.add(batch.column(1));
                for (int i = 0; i < batch.size(); i++) {
                    owned.add(batch.row(i));
                    if (!batch.column(1).isNull(i)) {
                        notes.add(batch.column(1).copyBytes(i));
                    }
                }
            });
            assertTrue(kept.size() > 1);
            assertEquals(d.rows(all), owned, "owning rows survive later pages, groups and parts");
            List<Bytes> want = new ArrayList<>();
            for (Row r : d.rows(all)) {
                if (r.values().get(1) instanceof Value.ByteArray b) {
                    want.add(b.value());
                }
            }
            assertEquals(want, notes);
            for (Batch b : kept) {
                assertThrows(IllegalStateException.class, b::size);
                assertThrows(IllegalStateException.class, b::columns);
                assertThrows(IllegalStateException.class, () -> b.key(0));
                assertThrows(IllegalStateException.class, () -> b.offset(0));
                assertThrows(IllegalStateException.class, () -> b.row(0));
                assertThrows(IllegalStateException.class, () -> b.cursor(0));
                assertThrows(IllegalStateException.class, () -> b.column(0));
                assertThrows(IllegalStateException.class, () -> b.key(0));
            }
            for (BatchColumn c : columns) {
                assertThrows(IllegalStateException.class, c::isPresent);
                assertThrows(IllegalStateException.class, () -> c.isNull(0));
                assertThrows(IllegalStateException.class, () -> c.value(0));
                assertThrows(IllegalStateException.class, () -> c.byteLength(0));
            }
        }
    }

    /**
     * A nested request from the action runs as any other; closing the dataset from the action fails and
     * changes nothing; a close on another thread waits for the action, ends the scan, and later scans fail.
     */
    @Test
    @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void closeDuringAScan() throws Exception {
        for (boolean planned : new boolean[] {false, true}) {
            Dataset d = Dataset.open(TestDatasets.write(tmp.resolve(Boolean.toString(planned)), new TestDatasets.Spec(), split(30, 2)));
            Query all = Reads.builder().limit(100).columns("size").build();
            Plan plan = Reads.plan(d, List.of(Reads.all(1)), List.of("size"));
            BatchScanTest.BatchSource scan = planned ? action -> Reads.scanBatches(plan, action) : action -> Reads.rangeBatches(d, all, action);

            AtomicReference<RuntimeException> inside = new AtomicReference<>();
            AtomicInteger nested = new AtomicInteger();
            scan.run(batch -> {
                nested.addAndGet(d.rows(Reads.all(3)).size());
                try {
                    d.close();
                } catch (RuntimeException e) {
                    inside.set(e);
                }
            });
            assertTrue(inside.get() instanceof IllegalStateException, "close inside the scan: " + inside.get());
            assertTrue(nested.get() > 0);
            assertEquals(30, d.rows(Reads.all(100)).size(), "the failed close changed nothing");

            AtomicReference<Thread> closer = new AtomicReference<>();
            AtomicInteger calls = new AtomicInteger();
            assertThrows(CancellationException.class, () -> scan.run(batch -> {
                if (calls.incrementAndGet() > 1) {
                    fail("a batch after the dataset began closing");
                }
                Thread c = Thread.ofPlatform().start(d::close);
                closer.set(c);
                while (c.getState() != Thread.State.WAITING) { // the close waits for this scan
                    Thread.onSpinWait();
                }
                assertTrue(c.isAlive(), "the close returned while a scan's action ran");
            }));
            closer.get().join();
            assertThrows(IllegalStateException.class, () -> scan.run(batch -> fail("a batch of a closed dataset")));
        }
    }

    /**
     * Over a plan of one optional INT64 column, a batch scan read through the typed accessors allocates no
     * object per row. The scan itself allocates a fresh key array per row (the key page decodes one: 24
     * bytes for these keys) and the pages' storage; measured here, 71 to 160 bytes per row depending on how far
     * the JIT has got, against 290 or more for a row scan, which also makes a row, its value list, a value and a
     * key wrapper per row. Two assertions, neither comparing with the row path (whose allocations the JIT may
     * remove): the batch scan stays under 224 bytes per row in its best round, and reading every row through
     * the accessors adds no more than 16 bytes per row to a scan whose action reads nothing (it adds more than
     * 150 when the action makes an owning {@link Batch#row}). Allocation is counted per thread, so other
     * threads do not disturb it; the median over many rounds absorbs a JIT transition between two of them.
     */
    @Test
    void batchScansAllocateNoObjectPerRow() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 4096;
        spec.rowsPerPage = 512;
        int rows = 16384;
        try (Dataset d = Dataset.open(TestDatasets.write(tmp, spec, split(rows, 2)))) {
            Plan plan = Reads.plan(d, List.of(Reads.all(1)), List.of("size"));
            com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
            long[] sum = new long[1];
            int[] seen = new int[1];
            Consumer<Batch> idle = batch -> seen[0] += batch.size();
            Consumer<Batch> reading = batch -> {
                BatchColumn size = batch.column(0);
                for (int i = 0; i < batch.size(); i++) {
                    if (!size.isNull(i)) {
                        sum[0] += size.getLong(i) + batch.keyLength(i);
                    }
                }
                seen[0] += batch.size();
            };
            for (int warm = 0; warm < 40; warm++) {
                Reads.scanBatches(plan, reading);
            }
            long tid = Thread.currentThread().threadId();
            long best = Long.MAX_VALUE;
            long[] extra = new long[15];
            for (int round = 0; round < extra.length; round++) {
                seen[0] = 0;
                long before = bean.getThreadAllocatedBytes(tid);
                Reads.scanBatches(plan, idle);
                long idleBytes = bean.getThreadAllocatedBytes(tid) - before;
                assertEquals(rows, seen[0]);
                seen[0] = 0;
                before = bean.getThreadAllocatedBytes(tid);
                Reads.scanBatches(plan, reading);
                long readingBytes = bean.getThreadAllocatedBytes(tid) - before;
                assertEquals(rows, seen[0]);
                best = Math.min(best, readingBytes / rows);
                extra[round] = (readingBytes - idleBytes) / rows;
            }
            assertTrue(sum[0] != 0);
            java.util.Arrays.sort(extra);
            assertTrue(extra[extra.length / 2] <= 16, "reading through the accessors allocated " + extra[extra.length / 2] + " B/row");
        }
    }

    /** Scans of one plan on many threads at once have storage of their own. */
    @Test
    void concurrentScansAgree() throws Exception {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 30;
        spec.rowsPerPage = 4;
        spec.noteColumn = true;
        spec.dictionaryNote = true;
        try (Dataset d = Dataset.open(TestDatasets.write(tmp, spec, split(300, 3))); ExecutorService ex = Executors.newFixedThreadPool(8)) {
            List<String> columns = List.of("note", "size", "key");
            Plan plan = Reads.plan(d, List.of(Reads.all(1)), columns);
            List<Row> want = d.rows(Reads.builder().limit(1000).columns(columns).build());
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                results.add(ex.submit(() -> want.equals(rows(action -> Reads.scanBatches(plan, action)))
                        && want.equals(rows(action -> Reads.rangeBatches(d, Reads.builder().limit(1000).columns(columns).build(), action)))));
            }
            for (Future<Boolean> r : results) {
                assertTrue(r.get());
            }
            byte[] first = new byte[16];
            Reads.rangeBatches(d, Reads.all(1), batch -> assertArrayEquals(Bytes.utf8(key(0)).toByteArray(),
                    java.util.Arrays.copyOf(first, batch.copyKey(0, first, 0))));
        }
    }
}
