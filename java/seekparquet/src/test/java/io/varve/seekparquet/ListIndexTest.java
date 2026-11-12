// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ListIndexTest {
    @TempDir Path temp;

    private static List<TestDatasets.TestRow> rows(List<byte[]> keys) {
        keys.sort(Arrays::compareUnsigned);
        List<TestDatasets.TestRow> rows = new ArrayList<>();
        for (byte[] key : keys) rows.add(new TestDatasets.TestRow(key, rows.size()));
        return rows;
    }

    @Test void differential() throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (String k : List.of("/", "//", "//x", "a", "a/", "a//x", "a/0", "a/b/", "a/b/x", "a/c", "ab", "root")) {
            keys.add(Bytes.utf8(k).toByteArray());
        }
        byte[] ff = new byte[161]; Arrays.fill(ff, (byte) 255); ff[160] = '/';
        for (byte[] head : List.of(Bytes.utf8("wide/").toByteArray(), Bytes.utf8("q".repeat(160) + "/").toByteArray(), ff)) {
            for (int i = 0; i < 12; i++) for (int j = 0; j < 30; j++) {
                byte[] suffix = Bytes.utf8(String.format("d%03d/f%03d", i, j)).toByteArray();
                byte[] key = Arrays.copyOf(head, head.length + suffix.length);
                System.arraycopy(suffix, 0, key, head.length, suffix.length); keys.add(key);
            }
        }
        for (int i = 0; i < 16; i++) keys.add(Bytes.utf8(String.format("one/%03d/x", i)).toByteArray());
        keys.addAll(new ArrayList<>(keys));
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerPage = 16; spec.rowsPerGroup = 300; spec.stringKey = false; spec.unique = false; spec.keyBoundBytes = 16;
        Path root = TestDatasets.write(temp.resolve("diff"), spec, List.of(rows(keys)));
        TreeSet<Bytes> prefixes = new TreeSet<>();
        prefixes.add(Bytes.empty()); prefixes.add(Bytes.utf8("wid")); prefixes.add(Bytes.utf8("wide/d00")); prefixes.add(Bytes.utf8("a/b"));
        for (byte[] key : keys) for (int i = 0; i < key.length; i++) if (key[i] == '/') prefixes.add(Bytes.of(Arrays.copyOf(key, i + 1)));
        try (Dataset plain = Dataset.open(root)) {
            for (int cap : new int[] {1, 2, 0, 1000000}) for (long budget : new long[] {1, 2048, 1 << 24}) {
                try (Dataset indexed = Dataset.open(root)) {
                    ListIndexStats stats = indexed.buildListIndex(new ListIndexOptions(budget, 2, cap), Cancellation.none());
                    assertTrue(stats.bytes() <= budget);
                    for (Bytes prefix : prefixes) for (byte delimiter : new byte[] {'/', 'f'}) {
                        ListQuery base = ListQuery.builder(delimiter).prefix(prefix).columns("size", "note").limit(1000).build();
                        List<ListEntry> all = plain.list(base);
                        List<Optional<Cursor>> cursors = new ArrayList<>(); cursors.add(Optional.empty());
                        for (int i = 0; i < all.size(); i++) if (i < 4 || i == all.size() - 1) cursors.add(base.next(all.get(i)).cursor());
                        List<Optional<Bytes>> ends = new ArrayList<>(); ends.add(Optional.empty());
                        if (all.size() > 1) {
                            byte[] key = all.get(1) instanceof ListEntry.CommonPrefix p ? p.prefix().toByteArray()
                                    : ((ListEntry.RowEntry) all.get(1)).row().key().toByteArray();
                            ends.add(Optional.of(Bytes.of(key)));
                            ends.add(Optional.of(Bytes.of(Arrays.copyOf(key, key.length + 1))));
                            ends.add(Optional.of(Bytes.of(Arrays.copyOf(key, key.length - 1))));
                            byte[] anchor = keys.stream().filter(k -> Arrays.compareUnsigned(k, key) >= 0).findFirst().orElseThrow();
                            ends.add(Optional.of(Bytes.of(anchor)));
                            ends.add(Optional.of(Bytes.of(Arrays.copyOf(anchor, anchor.length + 1))));
                            ends.add(Optional.of(Bytes.of(Arrays.copyOf(anchor, anchor.length - 1))));
                        }
                        for (Optional<Cursor> cursor : cursors) for (Optional<Bytes> end : ends) for (int limit : new int[] {1, 2, 4, 1000}) {
                            ListQuery q = Reads.listQuery(prefix, delimiter, end, limit, base.columns(), cursor, 0);
                            assertEquals(plain.list(q), indexed.list(q), () -> "cap=" + cap + " budget=" + budget + " query=" + q);
                        }
                    }
                }
            }
        }
    }

    @Test void pageCountsAndConcurrentBuild() throws Exception {
        for (String head : List.of("wide/", "x".repeat(200) + "/")) {
            List<byte[]> keys = new ArrayList<>();
            for (int i = 0; i < 100; i++) for (int j = 0; j < 100; j++) keys.add(Bytes.utf8(String.format("%sd%03d/f%03d", head, i, j)).toByteArray());
            TestDatasets.Spec spec = new TestDatasets.Spec(); spec.rowsPerPage = 16; spec.rowsPerGroup = 20000; spec.keyBoundBytes = 16;
            Path root = TestDatasets.write(temp.resolve("pages" + head.length()), spec, List.of(rows(keys)));
            try (Dataset dataset = Dataset.open(root); var pool = Executors.newFixedThreadPool(4)) {
                ListQuery q = ListQuery.builder((byte) '/').prefix(Bytes.utf8(head)).limit(1000).build();
                var want = dataset.list(q);
                var concurrent = pool.submit(() -> { for (int i = 0; i < 10; i++) assertEquals(want, dataset.list(q)); });
                dataset.buildListIndex(new ListIndexOptions(0, 2, 0), Cancellation.none());
                concurrent.get();
                AtomicReference<ReadStats> stats = new AtomicReference<>();
                assertEquals(want, Reads.list(dataset, q, Cancellation.none(), stats::set));
                assertTrue(stats.get().keyPagesDecoded() <= 2, stats.toString());
                assertTrue(stats.get().listIndexSkips() >= 99, stats.toString());
                var resumed = q.next(want.get(49));
                assertEquals(want.subList(50, want.size()), Reads.list(dataset, resumed, Cancellation.none(), stats::set));
                assertTrue(stats.get().keyPagesDecoded() <= 2, stats.toString());
                List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < 4; i++) futures.add(pool.submit(() -> {
                    for (int j = 0; j < 10; j++) assertEquals(want.subList(50, want.size()), dataset.list(resumed));
                }));
                for (var future : futures) future.get();
            }
        }
    }

    @Test void failuresAndBuildCancellation() throws Exception {
        List<TestDatasets.TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 200; i++) rows.add(TestDatasets.TestRow.of(String.format("a/f%03d", i), i));
        var a = rows.get(50); rows.set(50, rows.get(51)); rows.set(51, a);
        TestDatasets.Spec spec = new TestDatasets.Spec(); spec.rowsPerPage = 20; spec.rowsPerGroup = 200;
        Path root = TestDatasets.write(temp.resolve("bad"), spec, List.of(rows));
        try (Dataset d = Dataset.open(root)) {
            ListIndexStats stats = d.buildListIndex();
            assertEquals(1, stats.groupsFailed()); assertEquals(0, stats.groupsIndexed());
            var e = assertThrows(RefusalException.class, () -> d.list(ListQuery.builder((byte) '/').prefix(Bytes.utf8("a/f")).limit(1000).build()));
            assertEquals(RefusalCode.KEY_ORDER, e.code());
            Cancellation cancelled = Cancellation.create(); cancelled.cancel();
            assertThrows(CancellationException.class, () -> d.buildListIndex(ListIndexOptions.DEFAULT, cancelled));
        }
    }

    @Test void cancellationDuringBuildAndRetry() throws Exception {
        var spec = new TestDatasets.Spec();
        Path root = TestDatasets.write(temp.resolve("cancel"), spec,
                List.of(List.of(TestDatasets.TestRow.of("a/x", 0), TestDatasets.TestRow.of("b/x", 1))));
        var storage = new LifecycleTest.GatedStorage(root);
        try (Dataset d = Dataset.open(storage); var pool = Executors.newSingleThreadExecutor()) {
            storage.gate = new java.util.concurrent.CountDownLatch(1);
            Cancellation cancel = Cancellation.create();
            var future = pool.submit(() -> d.buildListIndex(ListIndexOptions.DEFAULT, cancel));
            assertTrue(storage.blocked.await(5, java.util.concurrent.TimeUnit.SECONDS));
            cancel.cancel();
            var e = assertThrows(java.util.concurrent.ExecutionException.class, future::get);
            assertInstanceOf(CancellationException.class, e.getCause());
            assertNull(d.parts().listIndex);
            storage.gate = null;
            assertEquals(1, d.buildListIndex().groupsIndexed());
        }
    }

    @Test void cancellationDuringInMemoryWalk() {
        List<ListBoundaryIndex.Boundary> runs = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) runs.add(new ListBoundaryIndex.Boundary(i, 0, 0, 2));
        byte[] prefix = {'a', '/'};
        var group = new ListBoundaryIndex.Group(0, 0, prefix, new byte[0], runs);
        Cancellation cancel = Cancellation.create();
        int[] checks = {0};
        assertThrows(CancellationException.class, () -> group.firstOutside(0, prefix, () -> {
            if (++checks[0] == 5) cancel.cancel();
            cancel.throwIfCancelled();
        }));
        assertEquals(5, checks[0]);
    }

    @Test void flatAndSingleDirectoryStorage() throws Exception {
        for (String prefix : List.of("", "one/")) {
            List<TestDatasets.TestRow> rows = new ArrayList<>();
            for (int i = 0; i < 1000; i++) rows.add(TestDatasets.TestRow.of(prefix + String.format("%01000d", i), i));
            var spec = new TestDatasets.Spec(); spec.rowsPerPage = 2; spec.rowsPerGroup = 1000; spec.keyBoundBytes = 16;
            Path root = TestDatasets.write(temp.resolve("flat" + prefix.length()), spec, List.of(rows));
            try (Dataset d = Dataset.open(root)) {
                var stats = d.buildListIndex();
                assertEquals(prefix.isEmpty() ? 0 : 1, stats.entries());
                assertTrue(stats.bytes() <= (prefix.isEmpty() ? 0 : 4096), stats.toString());
            }
        }
    }

    @Test void disorderedGroupBoundary() throws Exception {
        List<TestDatasets.TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 200; i++) rows.add(TestDatasets.TestRow.of(String.format("%s/f%03d", i < 100 ? "a" : "b", i), i));
        var last = rows.get(99); rows.set(99, rows.get(100)); rows.set(100, last);
        var spec = new TestDatasets.Spec(); spec.rowsPerPage = 20; spec.rowsPerGroup = 100;
        Path root = TestDatasets.write(temp.resolve("bad-boundary"), spec, List.of(rows));
        try (Dataset d = Dataset.open(root)) {
            var stats = d.buildListIndex(new ListIndexOptions(0, 2, 0), Cancellation.none());
            assertEquals(2, stats.groupsFailed()); assertEquals(0, stats.groupsIndexed());
            assertEquals(RefusalCode.KEY_ORDER, assertThrows(RefusalException.class,
                    () -> d.rows(Reads.all(1000))).code());
        }
    }

    @Test void unreadablePageRemainsAReadFailure() throws Exception {
        List<TestDatasets.TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 200; i++) rows.add(TestDatasets.TestRow.of(String.format("a/f%03d", i), i));
        var spec = new TestDatasets.Spec(); spec.rowsPerPage = 20; spec.rowsPerGroup = 200;
        Path root = TestDatasets.write(temp.resolve("unreadable"), spec, List.of(rows));
        var local = io.varve.seekparquet.storage.LocalStorage.of(root);
        var failAt = new java.util.concurrent.atomic.AtomicLong(-1);
        var storage = new io.varve.seekparquet.storage.DatasetStorage() {
            @Override public byte[] readManifest(int max, Cancellation c) throws java.io.IOException { return local.readManifest(max, c); }
            @Override public io.varve.seekparquet.storage.PartSource openPart(String path, long size, Cancellation c) throws java.io.IOException {
                var inner = local.openPart(path, size, c);
                return new io.varve.seekparquet.storage.PartSource() {
                    @Override public String path() { return inner.path(); }
                    @Override public long size() { return inner.size(); }
                    @Override public Optional<String> version() { return inner.version(); }
                    @Override public void close() throws java.io.IOException { inner.close(); }
                    @Override public java.nio.ByteBuffer read(long offset, int count, Cancellation cancel) throws java.io.IOException {
                        if (offset == failAt.get()) throw new java.io.IOException("injected unreadable page");
                        return inner.read(offset, count, cancel);
                    }
                };
            }
        };
        try (Dataset d = Dataset.open(storage); Dataset.Request request = d.begin(Cancellation.none())) {
            PartFile part = d.parts().get(0, request.ctx);
            failAt.set(part.keyIndex(0, request.ctx).offset[2]);
            assertThrows(StorageException.class, d::buildListIndex);
            assertNull(d.parts().listIndex);
            assertThrows(StorageException.class, () -> d.list(ListQuery.builder((byte) '/')
                    .prefix(Bytes.utf8("a/f")).limit(1000).build()));
            failAt.set(-1);
            assertEquals(1, d.buildListIndex().groupsIndexed());
        }
    }
}
