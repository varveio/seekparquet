// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.varve.seekparquet.TestDatasets.TestRow;
import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.LocalStorage;
import io.varve.seekparquet.storage.PartSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.parquet.format.ColumnMetaData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Selection planning against an oracle: every valid conformance dataset (but optional-codecs and brotli-key,
 * whose BROTLI this reader lacks) read whole by a range read on a separate handle, with the byte extents of the
 * projected column's chunks from its footers. A plan must count the rows its ranges select, scan exactly
 * those rows with their values, and ask storage for exactly {@link Plan#projectedBytes()} bytes of
 * the column's chunks, each read within one chunk.
 */
class RangePlanTest {

    @TempDir Path tmp;

    /** Records every part read, so that a test can attribute requested bytes to column chunks. */
    static final class RecordingStorage implements DatasetStorage {
        /** A read; {@code tail} marks the first read of a part, its footer window, which may span chunks. */
        record Read(String path, long offset, int length, boolean tail) {}

        final LocalStorage local;
        final List<Read> reads = new ArrayList<>();

        RecordingStorage(Path root) {
            local = LocalStorage.of(root);
        }

        synchronized List<Read> take() {
            List<Read> out = List.copyOf(reads);
            reads.clear();
            return out;
        }

        @Override
        public byte[] readManifest(int maxBytes, Cancellation cancellation) throws IOException {
            return local.readManifest(maxBytes, cancellation);
        }

        @Override
        public PartSource openPart(String path, long expectedSize, Cancellation cancellation) throws IOException {
            PartSource inner = local.openPart(path, expectedSize, cancellation);
            return new PartSource() {
                @Override public String path() { return inner.path(); }
                @Override public long size() { return inner.size(); }
                @Override public Optional<String> version() { return inner.version(); }
                @Override public void close() throws IOException { inner.close(); }

                @Override
                public ByteBuffer read(long position, int length, Cancellation c) throws IOException {
                    synchronized (RecordingStorage.this) {
                        long window = Math.min(inner.size(), PartFile.TAIL_WINDOW);
                        reads.add(new Read(path, position, length, position == inner.size() - window && length == window));
                    }
                    return inner.read(position, length, c);
                }
            };
        }
    }

    /** A column chunk's bytes [start, end) in a part file. */
    record Extent(String path, long start, long end) {}

    /** The rows of a dataset, read whole on its own handle, and the extents of {@code column}'s chunks. */
    record Oracle(List<Row> rows, List<Extent> extents) {
        static Oracle of(Path root, String column) {
            try (Dataset d = Dataset.open(root)) {
                d.verify();
                List<Row> rows = d.rows(Reads.builder().limit(1_000_000).columns(column).build());
                List<Extent> extents = new ArrayList<>();
                ReadContext ctx = new ReadContext(Cancellation.none(), Limits.DEFAULT);
                for (int i = 0; i < d.parts().size(); i++) {
                    PartFile p = d.parts().get(i, ctx);
                    ColumnSpec spec = p.projectable.get(column);
                    if (spec == null) {
                        continue;
                    }
                    for (int g : p.groups) {
                        ColumnMetaData md = p.rowGroup(g).getColumns().get(spec.leaf()).getMeta_data();
                        long[] range = PartFile.chunkRange(md);
                        extents.add(new Extent(p.name, range[0], range[1]));
                    }
                }
                return new Oracle(rows, extents);
            }
        }

        /**
         * {@return the bytes of {@code reads} inside the column's chunks}: any read overlapping a chunk
         * must lie within it, but for a part's footer window, which counts nothing.
         */
        long bytes(String name, List<RecordingStorage.Read> reads) {
            long n = 0;
            for (RecordingStorage.Read read : reads) {
                if (read.tail()) {
                    continue;
                }
                long end = read.offset() + read.length();
                for (Extent e : extents) {
                    if (read.path().equals(e.path()) && read.offset() < e.end() && end > e.start()) {
                        assertTrue(read.offset() >= e.start() && end <= e.end(),
                                name + ": a read crosses its chunk's boundary: " + read + " " + e + " " + extents);
                        n += read.length();
                    }
                }
            }
            return n;
        }
    }

    /** {@return whether the range's bounds select key, by the definition} */
    static boolean selects(Query query, Bytes key) {
        Span q = query.span();
        if (!key.startsWith(q.prefix())) {
            return false;
        }
        if (q.start().isPresent()) {
            int c = key.compareTo(q.start().get());
            if (c < 0 || (c == 0 && q.startExclusive())) {
                return false;
            }
        }
        return q.end().isEmpty() || key.compareTo(q.end().get()) < 0;
    }

    /** Every pair of a sample of bounds, an empty end, prefixes of several lengths and lookups. */
    static List<Query> ranges(List<Row> rows) {
        List<Bytes> distinct = rows.stream().map(Row::key).distinct().toList();
        List<Bytes> bounds = new ArrayList<>();
        bounds.add(null);
        int step = Math.max(1, distinct.size() / 9);
        for (int i = 0; i < distinct.size(); i += step) {
            byte[] k = distinct.get(i).toByteArray();
            bounds.add(Bytes.wrap(k));
            bounds.add(Bytes.wrap(Arrays.copyOf(k, k.length + 1)));
            bounds.add(Bytes.wrap(Arrays.copyOf(k, (k.length + 1) / 2)));
        }
        if (!distinct.isEmpty()) {
            byte[] last = distinct.getLast().toByteArray();
            bounds.add(Bytes.wrap(last));
            byte[] above = Arrays.copyOf(last, last.length + 1);
            above[last.length] = (byte) 0xff;
            bounds.add(Bytes.wrap(above));
        }
        List<Query> out = new ArrayList<>();
        for (Bytes a : bounds) {
            for (Bytes b : bounds) {
                Reads.Builder in = Reads.builder().limit(1);
                Reads.Builder ex = Reads.builder().limit(1);
                if (a != null) {
                    in.start(a);
                    ex.startAfter(a);
                }
                if (b != null) {
                    in.end(b);
                    ex.end(b);
                }
                out.add(in.build());
                out.add(ex.build());
            }
        }
        out.add(Reads.builder().end(Bytes.empty()).limit(1).build());
        for (int i = 0; i < distinct.size(); i += step) {
            byte[] k = distinct.get(i).toByteArray();
            for (int n : new int[] {1, (k.length + 1) / 2, k.length}) {
                Bytes prefix = Bytes.wrap(Arrays.copyOf(k, n));
                byte[] end = Arrays.copyOf(k, k.length + 1);
                end[k.length] = 1;
                out.add(Reads.prefix(prefix, 1));
                out.add(Reads.builder().prefix(prefix).start(Bytes.wrap(k)).end(Bytes.wrap(end)).limit(1).build());
            }
            out.add(Reads.lookup(Bytes.wrap(k), 1));
        }
        return out;
    }

    static void checkPlan(Dataset d, RecordingStorage storage, Oracle oracle, String column, List<Query> ranges) {
        String name = column + " over " + ranges;
        List<Row> want = oracle.rows().stream().filter(r -> ranges.stream().anyMatch(q -> selects(q, r.key()))).toList();
        storage.take();
        AtomicReference<ReadStats> planned = new AtomicReference<>();
        Plan plan = d.plan(Reads.selections(ranges), List.of(column), ReadOptions.of(planned::set));
        assertEquals(want.size(), plan.count().value(), name);
        assertTrue(plan.count().exact(), name);
        assertEquals(0, planned.get().valuePagesDecoded(), name);
        assertEquals(0, oracle.bytes(name, storage.take()), name + ": planning read value pages");
        List<Row> got = new ArrayList<>();
        AtomicReference<ReadStats> stats = new AtomicReference<>();
        Reads.scan(plan, Cancellation.none(), stats::set, got::add);
        long served = oracle.bytes(name, storage.take());
        assertEquals(want, got, name);
        assertEquals(got.isEmpty() ? 0 : 1, Math.min(1, stats.get().keyPagesDecoded()), name);
        if (plan.projectedBytes().exact()) {
            assertEquals(served, plan.projectedBytes().value(), name + ": projected bytes against the bytes the scan asked for");
        } else {
            assertEquals(0, plan.projectedBytes().value(), name + ": bytes of a chunk without an OffsetIndex");
        }
        assertEquals(want.size(), Reads.plan(d, ranges, List.of()).count().value(), name + " without columns");
    }

    static Stream<String> validDatasets() throws IOException {
        Path datasets = ConformanceFixturesTest.fixture("datasets");
        List<String> out = new ArrayList<>();
        try (Stream<Path> dirs = Files.list(datasets)) {
            for (Path dir : dirs.sorted().toList()) {
                Path structure = dir.resolve("STRUCTURE.txt");
                if (Files.exists(structure) && Files.readString(structure).contains("\nvalid: yes\n")
                        && !List.of("optional-codecs", "brotli-key").contains(dir.getFileName().toString())) {
                    out.add(dir.getFileName().toString());
                }
            }
        }
        return out.stream();
    }

    @ParameterizedTest
    @MethodSource("validDatasets")
    void conformanceOracle(String name) throws IOException {
        Path root = ConformanceFixturesTest.fixture("datasets/" + name);
        String column = Files.readString(root.resolve("STRUCTURE.txt")).contains("column i64: INT64") ? "i64" : "size";
        Oracle oracle = Oracle.of(root, column);
        RecordingStorage storage = new RecordingStorage(root);
        try (Dataset d = Dataset.open(storage)) {
            List<Query> ranges = ranges(oracle.rows());
            for (Query q : ranges) {
                checkPlan(d, storage, oracle, column, List.of(q));
            }
            Random random = new Random(oracle.rows().size());
            for (int i = 0; i < 40; i++) {
                List<Query> set = new ArrayList<>();
                for (int n = 2 + random.nextInt(3); n > 0; n--) {
                    set.add(ranges.get(random.nextInt(ranges.size())));
                }
                checkPlan(d, storage, oracle, column, set);
            }
        }
    }

    /**
     * Two lookups in one dictionary-encoded row group of the encodings fixture cost its dictionary once,
     * and three lookups in one page cost that page once.
     */
    @Test
    void sharedDictionary() {
        Path root = ConformanceFixturesTest.fixture("datasets/encodings");
        Oracle oracle = Oracle.of(root, "i64");
        RecordingStorage storage = new RecordingStorage(root);
        try (Dataset d = Dataset.open(storage)) {
            ReadContext ctx = new ReadContext(Cancellation.none(), Limits.DEFAULT);
            PartFile p = d.parts().get(0, ctx);
            ColumnMetaData md = p.rowGroup(0).getColumns().get(p.projectable.get("i64").leaf()).getMeta_data();
            assertTrue(md.isSetDictionary_page_offset(), "the fixture's i64 chunk has no dictionary");
            long dictionary = md.getData_page_offset() - md.getDictionary_page_offset();
            java.util.function.Function<List<String>, Long> cost = keys -> {
                List<Query> ranges = keys.stream().map(k -> Reads.lookup(Bytes.utf8(k), 1)).toList();
                checkPlan(d, storage, oracle, "i64", ranges);
                Plan plan = Reads.plan(d, ranges, List.of("i64"));
                assertTrue(plan.projectedBytes().exact());
                return plan.projectedBytes().value();
            };
            long a = cost.apply(List.of("enc/000"));
            long b = cost.apply(List.of("enc/004"));
            long both = cost.apply(List.of("enc/000", "enc/004"));
            assertEquals(dictionary, a + b - both, "the dictionary is shared");
            assertTrue(both > dictionary);
            assertEquals(a, cost.apply(List.of("enc/000", "enc/001", "enc/002")), "three rows of one page cost it once");
        }
    }

    private static List<List<TestRow>> split(int keys, int parts) {
        List<List<TestRow>> out = new ArrayList<>();
        for (int p = 0; p < parts; p++) {
            List<TestRow> rows = new ArrayList<>();
            for (int i = p * keys / parts; i < (p + 1) * keys / parts; i++) {
                rows.add(TestRow.of(key(i), i));
            }
            out.add(rows);
        }
        return out;
    }

    private static String key(int i) {
        return String.format("k%05d", i);
    }

    /**
     * Ends at every key of three parts of several row groups and pages: inside a page, at a page's first
     * key, at a row group's and at a part's; and empty ranges.
     */
    @Test
    void boundaries() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 40;
        spec.rowsPerPage = 7;
        Path root = TestDatasets.write(tmp, spec, split(240, 3));
        Oracle oracle = Oracle.of(root, "size");
        RecordingStorage storage = new RecordingStorage(root);
        try (Dataset d = Dataset.open(storage)) {
            for (int i = 0; i < 240; i++) {
                Bytes k = Bytes.utf8(key(i));
                for (Query q : List.of(
                        Reads.builder().end(k).limit(1).build(),
                        Reads.builder().start(k).limit(1).build(),
                        Reads.builder().startAfter(k).limit(1).build(),
                        Reads.lookup(k, 1),
                        Reads.builder().start(k).end(Bytes.utf8(key(Math.min(i + 7, 239)))).limit(1).build())) {
                    checkPlan(d, storage, oracle, "size", List.of(q));
                }
            }
            for (Query q : List.of(
                    Reads.builder().start(Bytes.utf8(key(9))).end(Bytes.utf8(key(9))).limit(1).build(),
                    Reads.builder().start(Bytes.utf8(key(90))).end(Bytes.utf8(key(10))).limit(1).build(),
                    Reads.prefix(Bytes.utf8("nothing"), 1),
                    Reads.builder().startAfter(Bytes.utf8(key(239))).limit(1).build())) {
                Plan plan = Reads.plan(d, List.of(q), List.of("size"));
                assertEquals(0, plan.count().value(), q.toString());
                assertTrue(plan.count().exact());
                assertEquals(0, plan.projectedBytes().value());
            }
        }
    }

    /**
     * A scan's action that closes the dataset fails at once instead of waiting forever for the scan it
     * runs in; throwing from the action ends the scan, and the dataset then closes.
     */
    @Test
    // In a thread of its own, so that a close waiting for its own scan fails the test instead of hanging it.
    @org.junit.jupiter.api.Timeout(value = 20, threadMode = org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD)
    void closeInsideScanFails() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        Dataset d = Dataset.open(TestDatasets.write(tmp, spec, split(30, 2)));
        Plan plan = Reads.plan(d, List.of(Reads.all(1)), List.of("size"));
        AtomicReference<RuntimeException> inside = new AtomicReference<>();
        RuntimeException stop = new RuntimeException("stop");
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> Reads.scan(plan, row -> {
            try {
                d.close();
            } catch (RuntimeException e) {
                inside.set(e);
            }
            throw stop;
        }));
        assertSame(stop, thrown);
        assertTrue(inside.get() instanceof IllegalStateException, "close inside the scan: " + inside.get());
        // The failed close changed nothing: the dataset is open, and closes once the scan has returned.
        assertEquals(30, Reads.plan(d, List.of(Reads.all(1)), List.of()).count().value());
        d.close();
        assertThrows(IllegalStateException.class, () -> Reads.scan(plan, row -> {}));
    }

    /**
     * A request a scan's action starts on another thread, while a close waits for the scan, fails at once
     * instead of waiting behind the close (which waits for the scan, which waits for the action).
     */
    @Test
    @org.junit.jupiter.api.Timeout(value = 20, threadMode = org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD)
    void requestInsideScanWhileClosing() throws Exception {
        Dataset d = Dataset.open(TestDatasets.write(tmp, new TestDatasets.Spec(), split(30, 2)));
        Plan plan = Reads.plan(d, List.of(Reads.all(1)), List.of("size"));
        AtomicReference<Throwable> nested = new AtomicReference<>();
        AtomicReference<Thread> closer = new AtomicReference<>();
        RuntimeException stop = new RuntimeException("stop");
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> Reads.scan(plan, row -> {
            Thread c = Thread.ofPlatform().start(d::close);
            closer.set(c);
            while (c.getState() != Thread.State.WAITING) { // the close waits for this scan
                Thread.onSpinWait();
            }
            Thread request = Thread.ofPlatform().start(() -> {
                try {
                    d.rows(Reads.all(1));
                } catch (Throwable e) {
                    nested.set(e);
                }
            });
            try {
                request.join();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            throw stop;
        }));
        assertSame(stop, thrown);
        assertTrue(nested.get() instanceof IllegalStateException || nested.get() instanceof java.util.concurrent.CancellationException,
                "the nested request: " + nested.get());
        closer.get().join();
        assertThrows(IllegalStateException.class, () -> Reads.scan(plan, row -> {}));
    }

    /**
     * A count across a part no read has checked relies on its manifest row count and is not exact;
     * planning without columns opens only the parts at the ends, and not a part the manifest bounds place
     * after the range; with columns it checks the parts between, and the count is exact.
     */
    @Test
    void countsUseOnlyCheckedParts() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 25;
        spec.rowsPerPage = 5;
        AtomicReference<ReadStats> planned = new AtomicReference<>();
        try (Dataset d = Dataset.open(TestDatasets.write(tmp, spec, split(300, 3)))) {
            Query span = Reads.builder().start(Bytes.utf8(key(50))).end(Bytes.utf8(key(250))).limit(1).build();
            Plan plan = d.plan(Reads.selections(List.of(span)), List.of(), ReadOptions.of(planned::set));
            assertEquals(200, plan.count().value());
            assertFalse(plan.count().exact());
            assertEquals(2, planned.get().partsOpened());
            plan = d.plan(Reads.selections(List.of(Reads.builder().start(Bytes.utf8(key(10))).end(Bytes.utf8(key(200))).limit(1).build())), List.of(), ReadOptions.of(planned::set));
            assertEquals(190, plan.count().value());
            assertFalse(plan.count().exact());
            assertEquals(0, planned.get().partsOpened());
            plan = d.plan(Reads.selections(List.of(Reads.builder().start(Bytes.utf8(key(210))).end(Bytes.utf8(key(290))).limit(1).build())), List.of(), ReadOptions.of(planned::set));
            assertEquals(80, plan.count().value());
            assertTrue(plan.count().exact());
            plan = d.plan(Reads.selections(List.of(span)), List.of("size"), ReadOptions.of(planned::set));
            assertEquals(200, plan.count().value());
            assertTrue(plan.count().exact());
            assertEquals(1, planned.get().partsOpened());
            assertEquals(0, planned.get().valuePagesDecoded());
            plan = d.plan(Reads.selections(List.of(span)), List.of(), ReadOptions.of(planned::set));
            assertTrue(plan.count().exact());
            assertEquals(0, planned.get().partsOpened());
        }
    }

    /** What planning rejects, a cursor, a scan ended by its action, and a plan of a closed dataset. */
    @Test
    void requests() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 20;
        Dataset d = Dataset.open(TestDatasets.write(tmp, spec, split(60, 2)));
        List<Row> all = d.rows(Reads.all(100).toBuilder().columns("size").build());
        // A limit is not applied, and a cursor resumes after its row.
        Plan plan = Reads.plan(d, List.of(Reads.all(1).next(all.get(29))), List.of("size"));
        List<Row> got = new ArrayList<>();
        Reads.scan(plan, got::add);
        assertEquals(all.subList(30, 60), got);
        RuntimeException stop = new RuntimeException("stop");
        List<Row> some = new ArrayList<>();
        assertSame(stop, assertThrows(RuntimeException.class, () -> Reads.scan(plan, row -> {
            some.add(row);
            if (some.size() == 3) {
                throw stop;
            }
        })));
        assertEquals(all.subList(30, 33), some);
        d.close();
        assertThrows(IllegalStateException.class, () -> Reads.scan(plan, row -> fail("scanned a closed dataset")));
        assertThrows(IllegalStateException.class, () -> Reads.plan(d, List.of(Reads.all(1)), List.of()));
    }

    /** A cursor inside a group of equal keys that crosses pages and row groups. */
    @Test
    void cursorInEqualKeys() {
        Path root = ConformanceFixturesTest.fixture("datasets/repeated-keys");
        try (Dataset d = Dataset.open(root)) {
            List<Row> all = d.rows(Reads.builder().limit(1_000_000).columns("size").build());
            for (int i = 0; i < all.size(); i++) {
                Plan plan = Reads.plan(d, List.of(Reads.all(1).next(all.get(i))), List.of("size"));
                List<Row> got = new ArrayList<>();
                Reads.scan(plan, got::add);
                assertEquals(all.subList(i + 1, all.size()), got, "after row " + i);
                assertEquals(all.size() - i - 1, plan.count().value());
            }
        }
    }

    /** The statistics a range read delivers agree with the reads storage served. */
    @Test
    void rangeStatsCountStorageReads() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 20;
        spec.rowsPerPage = 5;
        RecordingStorage storage = new RecordingStorage(TestDatasets.write(tmp, spec, split(60, 2)));
        try (Dataset d = Dataset.open(storage)) {
            AtomicReference<ReadStats> stats = new AtomicReference<>();
            List<Row> rows = Reads.range(d, Reads.builder().start(Bytes.utf8(key(33))).limit(4).columns("size").build(), Cancellation.none(), stats::set);
            assertEquals(4, rows.size());
            List<RecordingStorage.Read> reads = storage.take();
            ReadStats s = stats.get();
            assertEquals(reads.size(), s.sourceReads());
            assertEquals(reads.stream().mapToLong(RecordingStorage.Read::length).sum(), s.sourceBytes());
            assertEquals(1, s.partsOpened());
            assertEquals(1, s.indexLoads(), s.toString());
            assertTrue(s.rowGroupsOpened() >= s.indexLoads());
            assertEquals(s.keyPagesDecoded() * 5, s.keysDecoded());
            assertEquals(2, s.valuePagesDecoded(), "rows 33 and 34, then 35 and 36");
            assertEquals(0, s.groupsSkipped());
            // A warm repeat reads no footer and loads no index.
            Reads.range(d, Reads.builder().start(Bytes.utf8(key(33))).limit(4).columns("size").build(), Cancellation.none(), stats::set);
            assertEquals(0, stats.get().partsOpened());
            assertEquals(0, stats.get().indexLoads());
            // A listing that rolls keys up skips to each prefix's successor.
            Reads.list(d, ListQuery.builder((byte) '0').limit(10).build(), Cancellation.none(), stats::set);
            assertTrue(stats.get().prefixSkips() > 0);
        }
    }

    /**
     * A request that fails to start, cancelled already or on a closed dataset, delivers zero counts in
     * place of whatever the consumer held, and throws its own exception.
     */
    @Test
    void statsOfARequestThatFailsToStart() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        Dataset d = Dataset.open(TestDatasets.write(tmp, spec, split(8, 1)));
        Plan plan = Reads.plan(d, List.of(Reads.all(1)), List.of("size"));
        ReadStats earlier = Reads.planStats(d, List.of(Reads.all(1)), List.of("size"));
        ReadStats zero = ReadStats.NONE;
        Cancellation cancelled = Cancellation.create();
        cancelled.cancel();
        for (boolean closed : new boolean[] {false, true}) {
            if (closed) {
                d.close();
            }
            Cancellation c = closed ? Cancellation.none() : cancelled;
            Class<? extends RuntimeException> failure = closed ? IllegalStateException.class : java.util.concurrent.CancellationException.class;
            AtomicReference<ReadStats> stats = new AtomicReference<>(earlier);
            assertThrows(failure, () -> Reads.range(d, Reads.all(10), c, stats::set));
            assertEquals(zero, stats.get(), "range, closed " + closed);
            stats.set(earlier);
            assertThrows(failure, () -> Reads.list(d, ListQuery.builder((byte) '/').limit(10).build(), c, stats::set));
            assertEquals(zero, stats.get(), "list, closed " + closed);
            stats.set(earlier);
            assertThrows(failure, () -> Reads.scan(plan, c, stats::set, row -> {}));
            assertEquals(zero, stats.get(), "scan, closed " + closed);
            stats.set(earlier);
            assertThrows(failure, () -> d.plan(List.of(Selection.of(Span.all())), List.of("size"), new ReadOptions(c, stats::set)));
            assertEquals(zero, stats.get(), "plan, closed " + closed);
            stats.set(earlier);
            assertThrows(failure, () -> plan.split(2, new ReadOptions(c, stats::set)));
            assertEquals(zero, stats.get(), "split, closed " + closed);
            stats.set(earlier);
            assertThrows(failure, () -> d.get(Bytes.utf8(key(1)), List.of("size"), new ReadOptions(c, stats::set)));
            assertEquals(zero, stats.get(), "get, closed " + closed);
            stats.set(earlier);
            assertThrows(failure, () -> d.scan(Query.builder().build(), new ReadOptions(c, stats::set), batch -> true));
            assertEquals(zero, stats.get(), "dataset scan, closed " + closed);
            stats.set(earlier);
            assertThrows(failure, () -> d.verify(new ReadOptions(c, stats::set)));
            assertEquals(zero, stats.get(), "verify, closed " + closed);
        }
    }

    /** A failing request still delivers what it read. */
    @Test
    void statsOfAFailingRequest() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.swapFirstKeys = true;
        try (Dataset d = Dataset.open(TestDatasets.write(tmp, spec, split(8, 1)))) {
            AtomicReference<ReadStats> stats = new AtomicReference<>();
            assertThrows(RefusalException.class, () -> Reads.range(d, Reads.all(10), Cancellation.none(), stats::set));
            assertEquals(1, stats.get().partsOpened());
            assertEquals(1, stats.get().keyPagesDecoded());
        }
    }
}
