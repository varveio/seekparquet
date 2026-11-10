// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.*;

import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.LocalStorage;
import io.varve.seekparquet.storage.PartSource;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ListIndexRegressionTest {
    @TempDir Path temp;

    private static Bytes raw(String value) { return Bytes.of(value.getBytes(StandardCharsets.ISO_8859_1)); }

    private Path fixture(String name, TestDatasets.Spec spec, String[]... parts) throws Exception {
        List<List<TestDatasets.TestRow>> rows = new ArrayList<>();
        int ordinal = 0;
        for (String[] part : parts) {
            List<TestDatasets.TestRow> out = new ArrayList<>();
            for (String key : part) out.add(new TestDatasets.TestRow(raw(key).toByteArray(), ordinal++));
            rows.add(out);
        }
        spec.stringKey = false;
        spec.noteColumn = true;
        return TestDatasets.write(temp.resolve(name), spec, rows);
    }

    private static ReadStats compare(Dataset plain, Dataset indexed, ListQuery query) {
        AtomicReference<ReadStats> stats = new AtomicReference<>();
        assertEquals(plain.list(query), indexed.list(query, Cancellation.none(), stats::set), query.toString());
        return stats.get();
    }

    @Test void arbitraryPrefixCursor() throws Exception {
        var spec = new TestDatasets.Spec(); spec.rowsPerGroup = 100;
        Path root = fixture("arbitrary", spec, new String[] {"a/x", "a/y", "b/z"});
        try (Dataset plain = Dataset.open(root); Dataset indexed = Dataset.open(root)) {
            indexed.buildListIndex();
            var query = new ListQuery(raw("a/"), (byte) '/', Optional.empty(), 100, List.of(),
                    Optional.of(new Continuation.CommonPrefix(raw("a/x"))));
            assertEquals(1, plain.list(query).size());
            compare(plain, indexed, query);
        }
    }

    @Test void cursorMatrixIncludingExactRows() throws Exception {
        var spec = new TestDatasets.Spec(); spec.rowsPerGroup = 3; spec.rowsPerPage = 2; spec.unique = false;
        Path root = fixture("cursors", spec, new String[] {"/", "//", "//x", "a/", "a/", "a/", "a/", "a/", "a/x", "a/y", "b/z", "\u00ff/", "\u00ff/x", "\u00ff/y", "\u00ff\u00ff/x"});
        try (Dataset plain = Dataset.open(root); Dataset indexed = Dataset.open(root)) {
            indexed.buildListIndex(new ListIndexOptions(0, 3, 0), Cancellation.none());
            List<Optional<Continuation>> cursors = new ArrayList<>(); cursors.add(Optional.empty());
            for (String p : List.of("", "a", "a/", "a/x", "a/w", "a/x/", "a/\u00ff", "missing/", "\u00ff", "\u00ff/", "\u00ff/x", "\u00ff\u00ff", "/", "//")) {
                cursors.add(Optional.of(new Continuation.CommonPrefix(raw(p))));
            }
            var entries = plain.list(ListQuery.builder((byte) '/').prefix(raw("a")).limit(10).build());
            var exact = ((ListEntry.CommonPrefix) entries.getFirst()).exact().orElseThrow();
            for (long offset : new long[] {0, 2, 4, 50}) cursors.add(Optional.of(new Continuation.RowPosition(exact.key(), offset)));
            for (String prefix : List.of("", "a", "a/", "//", "\u00ff")) for (var cursor : cursors) {
                for (String end : Arrays.asList(null, "a/", "a/x", "a/x\0", "\u00ff/y")) for (int limit : new int[] {1, 2, 4, 1000}) {
                    compare(plain, indexed, new ListQuery(raw(prefix), (byte) '/', Optional.ofNullable(end).map(ListIndexRegressionTest::raw),
                            limit, List.of("size", "note"), cursor));
                }
            }
        }
    }

    @Test void descendingFirstKeysAndPrewarmedChains() throws Exception {
        String[][] layouts = {{"a/0", "a/1", "c/0", "c/1", "b/0", "d/0"},
                {"a/0", "a/1", "d/0", "d/1", "c/0", "c/1", "b/0", "z/0"}};
        for (int layout = 0; layout < layouts.length; layout++) {
            var spec = new TestDatasets.Spec(); spec.rowsPerGroup = 2; spec.rowsPerPage = 2;
            Path root = fixture("bad" + layout, spec, layouts[layout]);
            for (int workers : new int[] {1, 2, 4}) for (int[] warm : new int[][] {{}, {0, 1, 2}, {2, 1, 0}, {3, 2, 1, 0}}) {
                try (Dataset d = Dataset.open(root); Dataset plain = Dataset.open(root)) {
                    for (int group : warm) {
                        try (var request = d.begin(Cancellation.none())) {
                            var part = d.parts().get(0, request.ctx);
                            if (group < part.groups.length) part.firstKey(part.groups[group], request.ctx);
                        }
                    }
                    var stats = d.buildListIndex(new ListIndexOptions(0, workers, 0), Cancellation.none());
                    long failed = layouts[layout].length / 2 - 1;
                    assertEquals(failed, stats.groupsFailed(), stats.toString());
                    assertEquals(failed, stats.groupsSkipped()); assertEquals(1, stats.groupsIndexed());
                    assertEquals(layouts[layout].length / 2, stats.pages());
                    assertEquals(0, d.parts().listIndex.groups[0].slot);
                    var query = ListQuery.builder((byte) '/').limit(100).build();
                    assertEquals(RefusalCode.KEY_ORDER, assertThrows(RefusalException.class, () -> plain.list(query)).code());
                    assertEquals(RefusalCode.KEY_ORDER, assertThrows(RefusalException.class, () -> d.list(query)).code());
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<Object, Object> cache(ReadContext ctx, String field) throws Exception {
        var f = ReadContext.class.getDeclaredField(field); f.setAccessible(true);
        return (Map<Object, Object>) f.get(ctx);
    }

    /** The second page request proves the worker decoded its first page. */
    private static final class BoundaryStorage implements DatasetStorage {
        final LocalStorage local;
        final long[] first = new long[3], second = new long[3];
        final CountDownLatch[] entered = new CountDownLatch[3], decoded = new CountDownLatch[3], release = new CountDownLatch[3];
        volatile boolean enabled;
        BoundaryStorage(Path root) {
            local = LocalStorage.of(root);
            for (int i = 0; i < 3; i++) {
                entered[i] = new CountDownLatch(1); decoded[i] = new CountDownLatch(1); release[i] = new CountDownLatch(1);
            }
        }
        @Override public byte[] readManifest(int max, Cancellation c) throws IOException { return local.readManifest(max, c); }
        @Override public PartSource openPart(String path, long size, Cancellation c) throws IOException {
            PartSource inner = local.openPart(path, size, c);
            return new PartSource() {
                @Override public String path() { return inner.path(); }
                @Override public long size() { return inner.size(); }
                @Override public Optional<String> version() { return inner.version(); }
                @Override public void close() throws IOException { inner.close(); }
                @Override public ByteBuffer read(long offset, int length, Cancellation cancel) throws IOException {
                    if (enabled) for (int i = 0; i < 3; i++) {
                        if (offset == first[i]) {
                            entered[i].countDown();
                            try (var registration = cancel.onCancel(release[i]::countDown)) {
                                try { release[i].await(); }
                                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new java.io.InterruptedIOException(); }
                                cancel.throwIfCancelled();
                            }
                        }
                        if (offset == second[i]) decoded[i].countDown();
                    }
                    return inner.read(offset, length, cancel);
                }
            };
        }
    }

    @Test void controlledWorkerBoundaryOrder() throws Exception {
        var spec = new TestDatasets.Spec(); spec.rowsPerGroup = 2; spec.rowsPerPage = 1;
        Path root = fixture("worker-order", spec, new String[] {"a/0", "a/1", "c/0", "c/1", "b/0", "d/0"});
        for (int[] order : new int[][] {{0, 1, 2}, {2, 1, 0}, {1, 0, 2}}) {
            var storage = new BoundaryStorage(root);
            try (Dataset d = Dataset.open(storage); var pool = Executors.newSingleThreadExecutor()) {
                try (var request = d.begin(Cancellation.none())) {
                    var part = d.parts().get(0, request.ctx);
                    for (int i = 0; i < 3; i++) {
                        var index = part.keyIndex(part.groups[i], request.ctx); assertEquals(2, index.pages());
                        storage.first[i] = index.offset[0]; storage.second[i] = index.offset[1];
                    }
                }
                storage.enabled = true;
                Cancellation cancel = Cancellation.create();
                try {
                    var building = pool.submit(() -> d.buildListIndex(new ListIndexOptions(0, 3, 0), cancel));
                    for (var gate : storage.entered) assertTrue(gate.await(10, TimeUnit.SECONDS));
                    for (int i : order) { storage.release[i].countDown(); assertTrue(storage.decoded[i].await(10, TimeUnit.SECONDS)); }
                    var stats = building.get(10, TimeUnit.SECONDS);
                    assertEquals(1, stats.groupsIndexed()); assertEquals(2, stats.groupsFailed());
                    assertEquals(2, stats.groupsSkipped()); assertEquals(6, stats.pages());
                } finally { cancel.cancel(); }
            }
        }
    }

    @Test void syntheticAnchorsRetainOnlyCurrentGroupValues() throws Exception {
        for (boolean dictionary : new boolean[] {false, true}) for (boolean sequential : new boolean[] {false, true}) {
            var spec = new TestDatasets.Spec(); spec.rowsPerGroup = 2; spec.rowsPerPage = 2;
            spec.dictionarySize = dictionary; spec.omitValueOffsetIndexes = sequential;
            List<List<TestDatasets.TestRow>> parts = new ArrayList<>();
            for (int p = 0; p < 2; p++) {
                List<TestDatasets.TestRow> rows = new ArrayList<>();
                for (int i = p * 5; i < (p + 1) * 5; i++) {
                    rows.add(TestDatasets.TestRow.of(String.format("d%02d/", i), i));
                    rows.add(TestDatasets.TestRow.of(String.format("d%02d/x", i), i));
                }
                parts.add(rows);
            }
            Path root = TestDatasets.write(temp.resolve("cache" + dictionary + sequential), spec, parts);
            var query = ListQuery.builder((byte) '/').columns("size").limit(100).build();
            List<ListEntry> expected;
            try (Dataset plain = Dataset.open(root)) { expected = plain.list(query); }
            for (boolean indexed : new boolean[] {false, true}) {
                try (Dataset d = Dataset.open(root)) {
                    if (indexed) d.buildListIndex();
                    try (var request = d.begin(Cancellation.none())) {
                        assertEquals(expected, Queries.list(d.parts(), query, request.ctx));
                        assertEquals(1, cache(request.ctx, "valuePages").size());
                        assertEquals(dictionary ? 1 : 0, cache(request.ctx, "dictionaries").size());
                        assertEquals(sequential ? 1 : 0, cache(request.ctx, "sequential").size());
                        if (indexed) assertTrue(request.ctx.stats.listIndexSkips >= 9);
                    }
                }
            }
        }
    }

    @Test void sparseDenseSparseContinuation() throws Exception {
        List<String> keys = new ArrayList<>();
        for (String dir : List.of("a", "b", "c")) for (int j = 0; j < 4; j++) keys.add(String.format("%s/%04d", dir, j));
        keys.addAll(List.of("c/0004", "d/0000", "e/0000", "f/0000"));
        for (int j = 1; j <= 4; j++) keys.add(String.format("f/%04d", j));
        for (String dir : List.of("g", "h")) for (int j = 0; j < 4; j++) keys.add(String.format("%s/%04d", dir, j));
        var spec = new TestDatasets.Spec(); spec.rowsPerPage = 4; spec.rowsPerGroup = 100;
        Path root = fixture("dense", spec, keys.toArray(String[]::new));
        try (Dataset plain = Dataset.open(root); Dataset indexed = Dataset.open(root)) {
            indexed.buildListIndex(new ListIndexOptions(0, 1, 1), Cancellation.none());
            var group = indexed.parts().listIndex.groups[0]; assertTrue(group.dense);
            assertArrayEquals(raw("f/0001").toByteArray(), group.key(group.runAt(16)));
            assertEquals(16, group.rows[group.runAt(16)]);
            for (Continuation cursor : Arrays.asList(null, new Continuation.CommonPrefix(raw("b/")),
                    new Continuation.CommonPrefix(raw("f/0000")), new Continuation.RowPosition(raw("f/0001"), 0))) {
                for (String end : Arrays.asList(null, "c/0002", "f/0003")) {
                    var stats = compare(plain, indexed, new ListQuery(Bytes.empty(), (byte) '/', Optional.ofNullable(end).map(ListIndexRegressionTest::raw),
                            100, List.of("size", "note"), Optional.ofNullable(cursor)));
                    if (cursor == null && end == null) {
                        assertTrue(stats.listIndexSkips() >= 3); assertTrue(stats.listIndexDensePage() > 0);
                    }
                }
            }
        }
    }

    @Test void multiPartRunsBudgetAndMissingMiddle() throws Exception {
        var spec = new TestDatasets.Spec(); spec.rowsPerGroup = 4; spec.rowsPerPage = 2;
        Path root = fixture("parts", spec, new String[] {"/", "//", "//a", "//b"}, new String[] {"//c", "a/", "a//x", "a/a"},
                new String[] {"a/b", "a/c", "b/", "b/x"}, new String[] {"\u00ff/", "\u00ff/a", "\u00ff/b", "\u00ff\u00ff/x"});
        try (Dataset plain = Dataset.open(root); Dataset full = Dataset.open(root); Dataset partial = Dataset.open(root)) {
            full.buildListIndex();
            long firstCost = 256 + full.parts().listIndex.groups[0].memoryBytes();
            var stats = partial.buildListIndex(new ListIndexOptions(firstCost, 4, 0), Cancellation.none());
            assertTrue(stats.budgetExhausted()); assertEquals(1, stats.groupsIndexed());
            for (Dataset d : List.of(full, partial)) for (String prefix : List.of("", "/", "//", "a", "a/", "\u00ff")) {
                for (String cursor : Arrays.asList(null, "//", "a/x", "\u00ff")) {
                    compare(plain, d, new ListQuery(raw(prefix), (byte) '/', Optional.empty(), 100, List.of("size", "note"),
                            Optional.ofNullable(cursor).map(c -> new Continuation.CommonPrefix(raw(c)))));
                }
            }
        }
        Path bad = fixture("missing", spec, new String[] {"a/0", "a/1"}, new String[] {"a/2", "b/0"}, new String[] {"c/0", "c/1"});
        Files.delete(bad.resolve("data/part-00001.parquet"));
        try (Dataset plain = Dataset.open(bad); Dataset indexed = Dataset.open(bad)) {
            assertEquals(1, indexed.buildListIndex().partsFailed());
            for (Dataset d : List.of(plain, indexed)) {
                assertEquals(RefusalCode.MISSING_PART, assertThrows(RefusalException.class,
                        () -> d.list(ListQuery.builder((byte) '/').limit(100).build())).code());
            }
            compare(plain, indexed, ListQuery.builder((byte) '/').prefix(raw("c/")).columns("size").limit(100).build());
        }
        Path flat = fixture("flat-middle", spec, new String[] {"a/0", "a/1"}, new String[] {"b", "b0"}, new String[] {"c/0", "c/1"});
        try (Dataset plain = Dataset.open(flat); Dataset indexed = Dataset.open(flat)) {
            var stats = indexed.buildListIndex();
            assertEquals(2, stats.groupsIndexed()); assertEquals(1, stats.groupsSkipped()); assertEquals(0, stats.groupsFailed());
            var read = compare(plain, indexed, ListQuery.builder((byte) '/').columns("size", "note").limit(100).build());
            assertTrue(read.listIndexMissingGroup() > 0);
        }
    }

    @Test void budgetEdgesAndIndependentRetainedLayout() throws Exception {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 30; i++) keys.add(String.format("d%02d/%s", i, "x".repeat(i + 1)));
        var spec = new TestDatasets.Spec(); spec.rowsPerGroup = 10; spec.rowsPerPage = 4;
        Path root = fixture("budget", spec, keys.toArray(String[]::new));
        try (Dataset full = Dataset.open(root)) {
            var all = full.buildListIndex(new ListIndexOptions(0, 1, 100), Cancellation.none());
            var index = full.parts().listIndex;
            long first = 256 + index.groups[0].memoryBytes();
            for (long budget : new long[] {1, first - 1, first, first + 1, all.bytes() - 1, all.bytes(), all.bytes() + 1}) {
                try (Dataset d = Dataset.open(root)) {
                    var stats = d.buildListIndex(new ListIndexOptions(budget, 3, 100), Cancellation.none());
                    assertTrue(stats.bytes() <= budget);
                    if (budget < first) assertEquals(0, stats.groupsIndexed());
                    else if (budget < all.bytes()) assertTrue(stats.groupsIndexed() > 0);
                    else assertEquals(3, stats.groupsIndexed());
                    assertTrue(stats.pages() >= 3, "all three workers decoded before admission");
                    if (budget >= all.bytes()) assertEquals(all.pages(), stats.pages());
                    var again = d.buildListIndex(new ListIndexOptions(all.bytes(), 1, 100), Cancellation.none());
                    if (stats.groupsIndexed() == 0) assertEquals(3, again.groupsIndexed()); else assertEquals(stats, again);
                }
            }
            // Independent graph accounting with VM reference/header/alignment settings,
            // not the index's per-group/per-entry charge constants.
            var vm = java.lang.management.ManagementFactory.getPlatformMXBean(com.sun.management.HotSpotDiagnosticMXBean.class);
            int ref = Boolean.parseBoolean(vm.getVMOption("UseCompressedOops").getValue()) ? 4 : 8;
            int header = Boolean.parseBoolean(vm.getVMOption("UseCompressedClassPointers").getValue()) ? 12 : 16;
            int alignment = Integer.parseInt(vm.getVMOption("ObjectAlignmentInBytes").getValue());
            long measured = retainedLayout(index, new IdentityHashMap<>(), ref, header, alignment);
            assertTrue(measured <= all.bytes(), "layout=" + measured + " charge=" + all.bytes());
        }
    }

    private static long align(long bytes, int alignment) { return (bytes + alignment - 1) / alignment * alignment; }
    private static int width(Class<?> type, int ref) {
        if (!type.isPrimitive()) return ref;
        if (type == boolean.class || type == byte.class) return 1;
        if (type == char.class || type == short.class) return 2;
        if (type == int.class || type == float.class) return 4;
        return 8;
    }
    private static long retainedLayout(Object object, IdentityHashMap<Object, Boolean> seen, int ref, int header, int alignment) throws Exception {
        if (object == null || seen.put(object, true) != null) return 0;
        Class<?> type = object.getClass();
        if (type.isArray()) {
            int length = Array.getLength(object);
            long bytes = align(align(header + 4, 8) + (long) width(type.getComponentType(), ref) * length, alignment);
            if (!type.getComponentType().isPrimitive()) for (int i = 0; i < length; i++) bytes += retainedLayout(Array.get(object, i), seen, ref, header, alignment);
            return bytes;
        }
        if (object instanceof Duration) return align(header + 12, alignment);
        long own = header, children = 0;
        for (var field : type.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) continue;
            own += width(field.getType(), ref);
            if (!field.getType().isPrimitive()) {
                field.setAccessible(true);
                children += retainedLayout(field.get(object), seen, ref, header, alignment);
            }
        }
        return align(own, alignment) + children;
    }

    @Test void publicationCancellationCloseAndConcurrentBuilds() throws Exception {
        for (String action : List.of("publish", "cancel", "close")) {
            Path root = fixture("lifecycle" + action, new TestDatasets.Spec(), new String[] {"a/0", "a/1", "b/0", "b/1"});
            var storage = new LifecycleTest.GatedStorage(root);
            try (Dataset d = Dataset.open(storage); var pool = Executors.newFixedThreadPool(3)) {
                storage.gate = new CountDownLatch(1);
                Cancellation cancel = Cancellation.create();
                var building = pool.submit(() -> d.buildListIndex(ListIndexOptions.DEFAULT, cancel));
                assertTrue(storage.blocked.await(10, TimeUnit.SECONDS)); assertNull(d.parts().listIndex);
                if (action.equals("close")) {
                    var closed = pool.submit(d::close);
                    var e = assertThrows(java.util.concurrent.ExecutionException.class, () -> building.get(10, TimeUnit.SECONDS));
                    assertInstanceOf(CancellationException.class, e.getCause()); closed.get(10, TimeUnit.SECONDS);
                    assertNull(d.parts().listIndex); assertEquals(0, storage.open.get()); continue;
                }
                Cancellation waiting = Cancellation.create(); AtomicReference<Throwable> outcome = new AtomicReference<>();
                Thread waiter = new Thread(() -> {
                    try { d.buildListIndex(new ListIndexOptions(1, 1, 0), waiting); }
                    catch (Throwable e) { outcome.set(e); }
                });
                waiter.start();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                boolean inLockWait = false;
                while (System.nanoTime() < deadline && waiter.isAlive()) {
                    if (waiter.getState() == Thread.State.TIMED_WAITING && Arrays.stream(waiter.getStackTrace())
                            .anyMatch(f -> f.getMethodName().equals("buildListIndex"))) { inLockWait = true; break; }
                    Thread.sleep(1);
                }
                waiting.cancel(); waiter.join(10_000);
                assertTrue(inLockWait, "waiter must reach the held build lock"); assertInstanceOf(CancellationException.class, outcome.get());
                if (action.equals("cancel")) {
                    cancel.cancel();
                    var e = assertThrows(java.util.concurrent.ExecutionException.class, () -> building.get(10, TimeUnit.SECONDS));
                    assertInstanceOf(CancellationException.class, e.getCause()); assertNull(d.parts().listIndex);
                    storage.gate.countDown(); assertTrue(d.buildListIndex().groupsIndexed() > 0);
                } else {
                    var second = pool.submit(() -> d.buildListIndex(new ListIndexOptions(1, 3, 0), Cancellation.none()));
                    storage.gate.countDown();
                    var stats = building.get(10, TimeUnit.SECONDS); assertEquals(stats, second.get(10, TimeUnit.SECONDS));
                    assertEquals(stats, d.buildListIndex(new ListIndexOptions(1, 1, 1), Cancellation.none()));
                    assertEquals(2, d.list(ListQuery.builder((byte) '/').limit(100).build()).size());
                }
            }
        }
    }
}
