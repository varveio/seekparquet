// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.varve.seekparquet.RangePlanTest.Oracle;
import io.varve.seekparquet.RangePlanTest.RecordingStorage;
import io.varve.seekparquet.TestDatasets.TestRow;
import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.LocalStorage;
import io.varve.seekparquet.storage.PartSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * {@link RangePlan#split}: the shards' scans together are the plan's scan, which is the oracle's rows;
 * every shard is non-empty; counts and projected bytes add up to the plan's, and a shard's scan asks for
 * exactly its projected bytes; no group of equal keys crosses two shards.
 */
class SplitTest {

    /** The shard counts the tests ask for. */
    static final int[] COUNTS = {1, 2, 3, 7, 64};

    @TempDir Path tmp;

    /** {@return whether {@code q} selects {@code row}, by the definition, its cursor included} */
    static boolean admits(RangeQuery q, Row row) {
        if (!RangePlanTest.selects(q, row.key())) {
            return false;
        }
        if (q.cursor().orElse(null) instanceof Continuation.RowPosition c) {
            int cmp = row.key().compareTo(c.bytes());
            return cmp > 0 || (cmp == 0 && row.offset() > c.offset());
        }
        return true;
    }

    static List<Row> scan(RangePlan plan) {
        List<Row> rows = new ArrayList<>();
        plan.scan(rows::add);
        return rows;
    }

    static void checkSplit(Dataset d, RecordingStorage storage, Oracle oracle, String column, List<RangeQuery> ranges) {
        List<Row> want = oracle.rows().stream().filter(r -> ranges.stream().anyMatch(q -> admits(q, r))).toList();
        for (List<String> columns : List.of(List.of(column), List.<String>of())) {
            String name = columns + " over " + ranges;
            RangePlan plan = d.planRanges(ranges, columns);
            for (int n : COUNTS) {
                List<RangePlan> shards = plan.split(n);
                assertTrue(shards.size() <= n && (want.isEmpty() == shards.isEmpty()),
                        name + ": " + want.size() + " rows split " + n + " ways into " + shards.size());
                List<Row> got = new ArrayList<>();
                long rows = 0;
                long bytes = 0;
                Bytes lastKey = null;
                for (int i = 0; i < shards.size(); i++) {
                    RangePlan shard = shards.get(i);
                    storage.take();
                    List<Row> shardRows = scan(shard);
                    long served = oracle.bytes(name, storage.take());
                    String where = name + ": shard " + i + " of " + n;
                    assertFalse(shardRows.isEmpty(), where);
                    assertEquals(shardRows.size(), shard.count(), where);
                    assertTrue(shard.countExact() || !plan.countExact(), where);
                    assertEquals(plan.projectedBytesExact(), shard.projectedBytesExact(), where);
                    if (columns.isEmpty()) {
                        assertEquals(0, shard.projectedBytes(), where);
                    } else if (shard.projectedBytesExact()) {
                        assertEquals(served, shard.projectedBytes(), where + ": projected bytes against its scan");
                    }
                    assertNotEquals(lastKey, shardRows.getFirst().key(), where + ": a group crosses shards");
                    lastKey = shardRows.getLast().key();
                    got.addAll(shardRows);
                    rows += shard.count();
                    bytes += shard.projectedBytes();
                }
                List<Row> w = columns.isEmpty() ? want.stream().map(r -> new Row(r.key(), r.offset(), List.of(), List.of())).toList() : want;
                assertEquals(w, got, name + " in " + n);
                assertEquals(plan.count(), rows, name + ": counts add up");
                assertEquals(plan.projectedBytes(), bytes, name + ": projected bytes add up");
            }
        }
    }

    static List<String> validDatasets() throws IOException {
        return RangePlanTest.validDatasets().toList();
    }

    /** The whole dataset, a sample of the plan oracle's ranges, and random sets of them. */
    @ParameterizedTest
    @MethodSource("validDatasets")
    void conformanceOracle(String name) throws IOException {
        Path root = ConformanceFixturesTest.fixture("datasets/" + name);
        String column = Files.readString(root.resolve("STRUCTURE.txt")).contains("column i64: INT64") ? "i64" : "size";
        Oracle oracle = Oracle.of(root, column);
        RecordingStorage storage = new RecordingStorage(root);
        try (Dataset d = Dataset.open(storage)) {
            List<RangeQuery> ranges = RangePlanTest.ranges(oracle.rows());
            checkSplit(d, storage, oracle, column, List.of(RangeQuery.all(1)));
            for (int i = 0; i < ranges.size(); i += 11) {
                checkSplit(d, storage, oracle, column, List.of(ranges.get(i)));
            }
            Random random = new Random(2L * oracle.rows().size() + 1);
            for (int i = 0; i < 10; i++) {
                List<RangeQuery> set = new ArrayList<>();
                for (int k = 2 + random.nextInt(3); k > 0; k--) {
                    set.add(ranges.get(random.nextInt(ranges.size())));
                }
                checkSplit(d, storage, oracle, column, set);
            }
        }
    }

    static List<List<TestRow>> uniqueParts(int keys, int parts) {
        List<List<TestRow>> out = new ArrayList<>();
        int per = (keys + parts - 1) / parts;
        for (int i = 0; i < keys; i += per) {
            List<TestRow> rows = new ArrayList<>();
            for (int j = i; j < Math.min(i + per, keys); j++) {
                rows.add(TestRow.of(String.format("k%05d", j), j));
            }
            out.add(rows);
        }
        return out;
    }

    /** Runs of equal keys (1 to 45 of each), in two parts split at "c". */
    static List<List<TestRow>> repeatedParts() {
        Random random = new Random(91);
        List<List<TestRow>> out = List.of(new ArrayList<>(), new ArrayList<>());
        int i = 0;
        for (String k : List.of("a", "a/", "a/b", "b", "c", "c/d", "d", "~")) {
            for (int n = random.nextInt(45) + 1; n > 0; n--) {
                out.get(k.compareTo("c") < 0 ? 0 : 1).add(TestRow.of(k, i++));
            }
        }
        return out;
    }

    static List<List<TestRow>> longGroups() {
        List<TestRow> rows = new ArrayList<>();
        String stem = "x".repeat(40);
        int i = 0;
        for (String k : List.of("a", "b", "c", "d")) {
            int n = switch (k) {
                case "a" -> 2;
                case "b" -> 60;
                case "c" -> 3;
                default -> 30;
            };
            for (; n > 0; n--) {
                rows.add(TestRow.of(k + stem, i++));
            }
        }
        return List.of(rows);
    }

    /**
     * Unique keys and runs of equal keys that cross row groups, in several geometries; ranges ending
     * everywhere, a cursor inside a group, and a selection inside one row group.
     */
    @Test
    void geometries() throws IOException {
        record Geometry(String name, boolean repeated, List<List<TestRow>> parts, int groupRows, int pageRows) {}
        int dir = 0;
        for (Geometry g : List.of(
                new Geometry("unique", false, uniqueParts(240, 3), 40, 7),
                new Geometry("unique groups of 7", false, uniqueParts(240, 4), 7, 3),
                new Geometry("repeated groups of 7", true, repeatedParts(), 7, 2),
                new Geometry("repeated groups of 19", true, repeatedParts(), 19, 4),
                new Geometry("repeated one row per group", true, repeatedParts(), 1, 1),
                new Geometry("long groups", true, longGroups(), 25, 3))) {
            TestDatasets.Spec spec = new TestDatasets.Spec();
            spec.unique = !g.repeated();
            spec.rowsPerGroup = g.groupRows();
            spec.rowsPerPage = g.pageRows();
            Path root = TestDatasets.write(tmp.resolve("d" + dir++), spec, g.parts());
            Oracle oracle = Oracle.of(root, "size");
            RecordingStorage storage = new RecordingStorage(root);
            try (Dataset d = Dataset.open(storage)) {
                List<Bytes> distinct = oracle.rows().stream().map(Row::key).distinct().toList();
                int step = Math.max(1, distinct.size() / 12);
                checkSplit(d, storage, oracle, "size", List.of(RangeQuery.all(1)));
                for (int i = 0; i < distinct.size(); i += step) {
                    Bytes k = distinct.get(i);
                    Bytes after = distinct.get(Math.min(i + distinct.size() / 3, distinct.size() - 1));
                    for (RangeQuery q : List.of(
                            RangeQuery.builder().start(k).limit(1).build(),
                            RangeQuery.builder().end(k).limit(1).build(),
                            RangeQuery.builder().start(k).end(after).limit(1).build(),
                            RangeQuery.builder().start(k).after(new Continuation.RowPosition(k, 1)).limit(1).build(),
                            RangeQuery.lookup(k, 1))) {
                        checkSplit(d, storage, oracle, "size", List.of(q));
                    }
                    checkSplit(d, storage, oracle, "size",
                            List.of(RangeQuery.builder().end(k).limit(1).build(), RangeQuery.builder().start(after).limit(1).build()));
                }
            }
        }
    }

    /**
     * Random sets of ranges, row cursors inside groups of equal keys among them, merged and disjoint, in
     * several geometries: planned, scanned and split, their rows and offsets are the oracle's.
     */
    @Test
    void cursorSetsInRepeatedKeys() throws IOException {
        int dir = 0;
        for (int[] geometry : new int[][] {{7, 2}, {19, 4}, {1, 1}, {25, 3}}) {
            TestDatasets.Spec spec = new TestDatasets.Spec();
            spec.unique = false;
            spec.rowsPerGroup = geometry[0];
            spec.rowsPerPage = geometry[1];
            Path root = TestDatasets.write(tmp.resolve("c" + dir++), spec, repeatedParts());
            Oracle oracle = Oracle.of(root, "size");
            RecordingStorage storage = new RecordingStorage(root);
            List<Row> rows = oracle.rows();
            List<Bytes> distinct = rows.stream().map(Row::key).distinct().toList();
            Random random = new Random(dir);
            try (Dataset d = Dataset.open(storage)) {
                for (int round = 0; round < 25; round++) {
                    List<RangeQuery> set = new ArrayList<>();
                    for (int n = 1 + random.nextInt(3); n > 0; n--) {
                        RangeQuery.Builder b = RangeQuery.builder().limit(1);
                        switch (random.nextInt(4)) {
                            case 0 -> {
                                Row r = rows.get(random.nextInt(rows.size()));
                                b.after(new Continuation.RowPosition(r.key(), r.offset()));
                            }
                            case 1 -> b.start(distinct.get(random.nextInt(distinct.size())));
                            case 2 -> b.startAfter(distinct.get(random.nextInt(distinct.size())));
                            default -> { }
                        }
                        if (random.nextBoolean()) {
                            b.end(distinct.get(random.nextInt(distinct.size())));
                        }
                        set.add(b.build());
                    }
                    checkSplit(d, storage, oracle, "size", set);
                }
            }
        }
    }

    /**
     * Boundaries fall at row groups and balance the projected bytes, or the rows of a plan without
     * columns; a selection inside one row group is one shard; boundaries in a repeated-key dataset decode
     * the key pages around them, in unique mode nothing; a cancelled split fails in either mode.
     */
    @Test
    void balance() throws IOException {
        for (boolean repeated : new boolean[] {false, true}) {
            TestDatasets.Spec spec = new TestDatasets.Spec();
            spec.unique = !repeated;
            spec.rowsPerGroup = 20;
            spec.rowsPerPage = 5;
            try (Dataset d = Dataset.open(TestDatasets.write(tmp.resolve("r" + repeated), spec, uniqueParts(240, 2)))) {
                for (List<String> columns : List.of(List.of("size"), List.<String>of())) {
                    List<RangePlan> shards = d.planRanges(List.of(RangeQuery.all(1)), columns).split(4);
                    assertEquals(4, shards.size());
                    for (int i = 0; i < 4; i++) {
                        long n = shards.get(i).count();
                        assertTrue(n % 20 == 0 && n >= 40 && n <= 80, "shard " + i + " has " + n + " rows");
                        // The boundary after shard 1 is part 1's first row.
                        assertEquals(repeated && i < 3 && i != 1, shards.get(i).stats().keyPagesDecoded() > 0,
                                "repeated " + repeated + ", " + columns + ": shard " + i + " " + shards.get(i).stats());
                    }
                }
                // A cancelled split fails in both modes, with or without columns: with unique keys it reads
                // nothing, and fails by checking its cancellation.
                Cancellation cancelled = Cancellation.create();
                cancelled.cancel();
                for (List<String> columns : List.of(List.of("size"), List.<String>of())) {
                    RangePlan plan = d.planRanges(List.of(RangeQuery.all(1)), columns);
                    assertThrows(CancellationException.class, () -> plan.split(4, cancelled), "repeated " + repeated + ", " + columns);
                }
                // An empty plan, or n below 1, has no shard, but a cancelled split still fails.
                RangePlan nothing = d.planRanges(List.of(RangeQuery.builder().start(Bytes.utf8("zzzz")).limit(1).build()), List.of());
                assertThrows(CancellationException.class, () -> nothing.split(3, cancelled));
                RangePlan whole = d.planRanges(List.of(RangeQuery.all(1)), List.of());
                assertThrows(CancellationException.class, () -> whole.split(0, cancelled));
                RangePlan inside = d.planRanges(List.of(RangeQuery.builder().start(Bytes.utf8("k00003")).end(Bytes.utf8("k00015")).limit(1).build()),
                        List.of("size"));
                assertEquals(1, inside.split(5).size());
                assertEquals(List.of(), d.planRanges(List.of(RangeQuery.builder().start(Bytes.utf8("zzzz")).limit(1).build()), List.of()).split(3));
                assertEquals(List.of(), inside.split(0));
            }
        }
    }

    /** The shards of a repeated-key plan scanned at once, each with its own stats, are the plan's scan. */
    @Test
    void concurrentScans() throws Exception {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.unique = false;
        spec.rowsPerGroup = 7;
        spec.rowsPerPage = 2;
        try (Dataset d = Dataset.open(TestDatasets.write(tmp, spec, repeatedParts()));
                ExecutorService executor = Executors.newFixedThreadPool(4)) {
            RangePlan plan = d.planRanges(List.of(RangeQuery.builder().start(Bytes.utf8("a/")).limit(1).build()), List.of("size"));
            List<Row> want = scan(plan);
            for (int round = 0; round < 4; round++) {
                List<RangePlan> shards = plan.split(7);
                List<Future<List<Row>>> scans = new ArrayList<>();
                List<AtomicReference<ReadStats>> stats = new ArrayList<>();
                for (RangePlan shard : shards) {
                    AtomicReference<ReadStats> st = new AtomicReference<>();
                    stats.add(st);
                    scans.add(executor.submit(() -> {
                        List<Row> rows = Collections.synchronizedList(new ArrayList<>());
                        shard.scan(Cancellation.none(), st::set, rows::add);
                        return rows;
                    }));
                }
                List<Row> got = new ArrayList<>();
                for (int i = 0; i < scans.size(); i++) {
                    List<Row> rows = scans.get(i).get();
                    got.addAll(rows);
                    ReadStats st = stats.get(i).get();
                    assertTrue(st.keysDecoded() >= rows.size() && st.valuePagesDecoded() > 0, "shard " + i + ": " + st);
                }
                assertEquals(want, got);
            }
        }
    }

    /**
     * A part a read refuses is refused to every shard touching it and to no other; a cancelled scan of a
     * shard fails, and so does a split of a closed dataset.
     */
    @Test
    void requests() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 25;
        spec.rowsPerPage = 5;
        Path root = TestDatasets.write(tmp.resolve("a"), spec, uniqueParts(300, 3));
        TestDatasets.Spec other = new TestDatasets.Spec();
        other.rowsPerGroup = 25;
        other.rowsPerPage = 5;
        other.schemaId = "test/v2";
        Path stamped = TestDatasets.write(tmp.resolve("b"), other, uniqueParts(300, 3));
        // Part 1's schema_id stamp differs from the manifest's: a refusal when a read touches it.
        Files.copy(stamped.resolve("data/part-00001.parquet"), root.resolve("data/part-00001.parquet"), StandardCopyOption.REPLACE_EXISTING);
        Dataset d = Dataset.open(root);
        RangePlan plan = d.planRanges(List.of(RangeQuery.builder().start(Bytes.utf8("k00010")).end(Bytes.utf8("k00290")).limit(1).build()), List.of());
        List<RangePlan> shards = plan.split(7);
        assertTrue(shards.size() >= 3, shards.size() + " shards");
        int refused = 0;
        long at = 0;
        for (RangePlan shard : shards) {
            long first = at;
            at += shard.count();
            boolean touches = first + 10 < 200 && at + 10 > 100;
            try {
                shard.scan(row -> {});
                assertFalse(touches, "a shard over part 1 was not refused");
            } catch (RefusalException e) {
                assertTrue(touches, "a shard not over part 1 was refused: " + e);
                refused++;
            }
        }
        assertEquals(1, refused);
        Cancellation cancelled = Cancellation.create();
        cancelled.cancel();
        assertThrows(CancellationException.class, () -> shards.getFirst().scan(cancelled, row -> {}));
        RangePlan empty = d.planRanges(List.of(RangeQuery.builder().start(Bytes.utf8("zzzz")).limit(1).build()), List.of());
        d.close();
        assertThrows(IllegalStateException.class, () -> plan.split(2));
        assertThrows(IllegalStateException.class, () -> empty.split(2), "an empty plan's split after close");
        assertThrows(IllegalStateException.class, () -> plan.split(0), "a split into no shard after close");
    }

    /** One repeated-key part of four row groups of 20 distinct keys, two key pages each. */
    Path boundaryDataset() throws IOException {
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 80; i++) {
            rows.add(TestRow.of(String.format("k%03d/%s", i, "x".repeat(40)), i));
        }
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.unique = false;
        spec.rowsPerGroup = 20;
        spec.rowsPerPage = 10;
        return TestDatasets.write(tmp.resolve("boundary"), spec, List.of(rows));
    }

    /**
     * Each shard's stats are exactly the key pages that placed its end, the last page of its last row
     * group and the first of the next, though one cursor places every boundary.
     */
    @Test
    void boundaryStats() throws IOException {
        try (Dataset d = Dataset.open(boundaryDataset())) {
            List<RangePlan> shards = d.planRanges(List.of(RangeQuery.all(1)), List.of()).split(4);
            assertEquals(List.of(2L, 2L, 2L, 0L), shards.stream().map(s -> s.stats().keyPagesDecoded()).toList());
        }
    }

    /** A boundary is a selected row that begins a row group, never one between two ranges. */
    @Test
    void disjointRanges() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 100;
        spec.rowsPerPage = 10;
        try (Dataset d = Dataset.open(TestDatasets.write(tmp, spec, uniqueParts(300, 1)))) {
            java.util.function.BiFunction<Integer, Integer, RangeQuery> between = (a, b) -> RangeQuery.builder()
                    .start(Bytes.utf8(String.format("k%05d", a))).end(Bytes.utf8(String.format("k%05d", b))).limit(1).build();
            RangePlan plan = d.planRanges(List.of(between.apply(10, 20), between.apply(210, 220)), List.of());
            assertEquals(1, plan.split(2).size(), "no row group begins inside the ranges");
            plan = d.planRanges(List.of(between.apply(10, 20), between.apply(100, 110)), List.of());
            List<RangePlan> shards = plan.split(2);
            assertEquals(2, shards.size(), "row group 1 begins the second range");
            for (int i = 0; i < 2; i++) {
                List<Row> rows = scan(shards.get(i));
                assertEquals(10, rows.size());
                assertEquals(Bytes.utf8(String.format("k%05d", 10 + 90 * i)), rows.getFirst().key());
            }
        }
    }

    /**
     * A split cancelled while it reads a boundary's key page fails, and the same split then succeeds; a
     * key page refused at a boundary fails the split with its refusal.
     */
    @Test
    void interrupted() throws IOException {
        Path root = boundaryDataset();
        AtomicReference<Cancellation> hook = new AtomicReference<>();
        LocalStorage local = LocalStorage.of(root);
        DatasetStorage storage = new DatasetStorage() {
            @Override public byte[] readManifest(int max, Cancellation c) throws IOException {
                return local.readManifest(max, c);
            }

            @Override public PartSource openPart(String path, long size, Cancellation c) throws IOException {
                PartSource inner = local.openPart(path, size, c);
                return new PartSource() {
                    @Override public String path() { return inner.path(); }
                    @Override public long size() { return inner.size(); }
                    @Override public Optional<String> version() { return inner.version(); }
                    @Override public void close() throws IOException { inner.close(); }

                    @Override
                    public ByteBuffer read(long position, int length, Cancellation token) throws IOException {
                        Cancellation cancel = hook.get();
                        if (cancel != null) {
                            cancel.cancel();
                        }
                        return inner.read(position, length, token);
                    }
                };
            }
        };
        long offset;
        int size;
        try (Dataset d = Dataset.open(storage)) {
            RangePlan plan = d.planRanges(List.of(RangeQuery.all(1)), List.of());
            Cancellation cancelled = Cancellation.create();
            hook.set(cancelled);
            assertThrows(CancellationException.class, () -> plan.split(4, cancelled));
            hook.set(null);
            List<RangePlan> shards = plan.split(4);
            assertEquals(4, shards.size());
            for (RangePlan shard : shards) {
                assertEquals(20, shard.count());
                assertTrue(shard.countExact());
            }
            ReadContext ctx = new ReadContext(Cancellation.none(), Limits.DEFAULT);
            PartFile p = d.parts().get(0, ctx);
            KeyIndex index = p.keyIndex(p.groups[1], ctx);
            offset = index.offset[0];
            size = index.size[0];
        }
        try (FileChannel f = FileChannel.open(root.resolve("data/part-00000.parquet"), StandardOpenOption.WRITE)) {
            f.write(ByteBuffer.allocate(size), offset);
        }
        try (Dataset broken = Dataset.open(root)) {
            RangePlan plan = broken.planRanges(List.of(RangeQuery.all(1)), List.of());
            assertThrows(RefusalException.class, () -> plan.split(4));
        }
    }
}
