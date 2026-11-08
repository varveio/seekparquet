// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.varve.seekparquet.IndexedAccessTest.CountingStorage;
import io.varve.seekparquet.TestDatasets.TestRow;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.PartSource;
import java.nio.file.Path;
import java.nio.file.Files;
import io.varve.seekparquet.storage.LocalStorage;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Deterministic storage-call/byte regressions, not timing or throughput benchmarks. */
class SeekCostTest {
    @TempDir Path tmp;

    @Test
    void coldFinalKeyUsesLogarithmicProbesAndBoundedIndexOverfetch() throws IOException {
        for (int groups : new int[] {10, 100, 1000}) {
            List<TestRow> rows = new ArrayList<>();
            for (int i = 0; i < groups * 10; i++) {
                rows.add(TestRow.of(String.format("k%08d", i), i));
            }
            TestDatasets.Spec spec = new TestDatasets.Spec();
            spec.rowsPerGroup = 10;
            spec.rowsPerPage = 10;
            CountingStorage storage = new CountingStorage(TestDatasets.write(tmp.resolve("g" + groups), spec, List.of(rows)));
            try (Dataset ds = Dataset.open(storage)) {
                storage.reads.set(0);
                storage.bytes.set(0);
                Bytes target = Bytes.wrap(rows.getLast().key());
                assertEquals(target, ds.lookup(target, 1).getFirst().key());
                int logarithm = 32 - Integer.numberOfLeadingZeros(groups);
                assertTrue(storage.reads.get() <= 3 * logarithm + 4, "reads=" + storage.reads.get());
                assertTrue(storage.bytes.get() < 1000L * logarithm, "bytes=" + storage.bytes.get());
                storage.reads.set(0);
                ds.lookup(target, 1);
                assertEquals(1, storage.reads.get(), "checked first keys and indexes are retained");
            }
        }
    }

    @Test
    void manyPartsWithLongPrefixesHaveBoundedColdAndWarmShortRangesAndLists() throws IOException {
        String stem = "x".repeat(128) + "/";
        List<List<TestRow>> parts = new ArrayList<>();
        for (int p = 0; p < 16; p++) {
            List<TestRow> rows = new ArrayList<>();
            for (int i = 0; i < 256; i++) {
                rows.add(TestRow.of(String.format("%sp%02d/d%02d/f%03d", stem, p, i / 16, i), i));
            }
            parts.add(rows);
        }
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 16;
        spec.rowsPerPage = 16;
        spec.nonMonotoneKeyBounds = true;
        CountingStorage storage = new CountingStorage(TestDatasets.write(tmp, spec, parts));
        for (int n : new int[] {1, 5}) {
            try (Dataset ds = Dataset.open(storage)) {
                // Exclude CHECK-1 open, which checks every part, from query counters.
                for (boolean warm : new boolean[] {false, true}) {
                    storage.reads.set(0);
                    storage.bytes.set(0);
                    List<Row> got = ds.range(RangeQuery.builder()
                            .start(Bytes.wrap(parts.get(14).get(240).key())).limit(n).columns("size").build());
                    assertEquals(n, got.size());
                    for (int i = 0; i < n; i++) {
                        assertEquals(Bytes.wrap(parts.get(14).get(240 + i).key()), got.get(i).key());
                        assertEquals(new Value.Int64(240 + i), got.get(i).values().getFirst());
                    }
                    // Four binary probes, each an index pair and a 16-row key page,
                    // plus selected key/value pages. Long keys dominate byte cost.
                    assertTrue(storage.reads.get() <= 20, "reads=" + storage.reads.get());
                    assertTrue(storage.bytes.get() <= 32 * 1024, "bytes=" + storage.bytes.get());
                    if (warm) assertTrue(storage.reads.get() <= 3, "warm reads=" + storage.reads.get());
                    System.out.printf("ACCESS_METRIC {\"operation\":\"range\",\"rows\":%d,\"warm\":%b,\"reads\":%d,\"requested_bytes\":%d}%n",
                            n, warm, storage.reads.get(), storage.bytes.get());
                }
                for (int repeat = 0; repeat < 2; repeat++) {
                    storage.reads.set(0);
                    storage.bytes.set(0);
                    List<ListEntry> got = ds.list(ListQuery.builder((byte) '/')
                            .prefix(Bytes.utf8(stem + "p13/")).limit(100).build());
                    assertEquals(16, got.size());
                    for (int i = 0; i < 16; i++) {
                        assertEquals(new ListEntry.CommonPrefix(Bytes.utf8(String.format("%sp13/d%02d/", stem, i))), got.get(i));
                    }
                    // One directory per group: two index reads and up to two key
                    // reads per prefix, with space for routing probes.
                    assertTrue(storage.reads.get() <= 80, "reads=" + storage.reads.get());
                    assertTrue(storage.bytes.get() <= 160 * 1024, "bytes=" + storage.bytes.get());
                    System.out.printf("ACCESS_METRIC {\"operation\":\"list\",\"warm\":%b,\"reads\":%d,\"requested_bytes\":%d}%n",
                            repeat > 0, storage.reads.get(), storage.bytes.get());
                }
            }
        }
    }

    @Test
    void repeatedPaginationJumpsToPhysicalRowsForRangeAndList() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.unique = false;
        spec.rowsPerPage = 100;
        spec.rowsPerGroup = 1000;
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) rows.add(TestRow.of("same", i));
        Path root = TestDatasets.write(tmp, spec, List.of(rows));
        for (boolean listing : new boolean[] {false, true}) {
            CountingStorage storage = new CountingStorage(root);
            try (Dataset ds = Dataset.open(storage)) {
                storage.reads.set(0);
                storage.bytes.set(0);
                RangeQuery range = RangeQuery.lookup(Bytes.utf8("same"), 100);
                ListQuery list = ListQuery.builder((byte) '/').limit(100).build();
                for (int page = 0; page < 100; page++) {
                    int before = storage.reads.get();
                    List<Row> got;
                    if (listing) {
                        List<ListEntry> entries = ds.list(list);
                        got = entries.stream().map(e -> ((ListEntry.RowEntry) e).row()).toList();
                        list = list.next(entries.getLast());
                    } else {
                        got = ds.range(range);
                        range = range.next(got.getLast());
                    }
                    assertEquals(100, got.size());
                    for (int i = 0; i < 100; i++) assertEquals(page * 100L + i, got.get(i).offset());
                    assertTrue(storage.reads.get() - before <= 4, "page=" + page);
                }
                assertTrue(storage.reads.get() <= 220, "reads=" + storage.reads.get());
                assertTrue(storage.bytes.get() < 200_000, "bytes=" + storage.bytes.get());
            }
        }
    }

    @Test
    void resumedOffsetsPreserveProjectionAndOversizedCursorsDoNotSkipOtherKeys() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.unique = false;
        spec.rowsPerPage = 7;
        spec.rowsPerGroup = 19;
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 350; i++) rows.add(TestRow.of(i < 3 ? "a" : i < 206 ? "b" : "c", i));
        try (Dataset ds = Dataset.open(TestDatasets.write(tmp, spec, List.of(rows)))) {
            List<Row> expected = new ArrayList<>();
            for (int i = 0; i < 350; i++) {
                String key = i < 3 ? "a" : i < 206 ? "b" : "c";
                long offset = i < 3 ? i : i < 206 ? i - 3 : i - 206;
                expected.add(new Row(Bytes.utf8(key), offset, List.of("size"), List.of(new Value.Int64(i))));
            }
            RangeQuery query = RangeQuery.all(11).toBuilder().columns("size").build();
            List<Row> actual = new ArrayList<>();
            while (true) {
                List<Row> page = ds.range(query);
                if (page.isEmpty()) break;
                actual.addAll(page);
                query = query.next(page.getLast());
            }
            assertEquals(expected, actual);
            for (long offset : new long[] {202, 203, 220, 1000, Long.MAX_VALUE}) {
                List<Row> resumed = ds.range(RangeQuery.all(1000).toBuilder().columns("size")
                        .after(new Continuation.RowPosition(Bytes.utf8("b"), offset)).build());
                assertEquals(expected.stream().filter(r -> r.key().equals(Bytes.utf8("c"))).toList(), resumed);
            }
        }
    }

    @Test
    void outOfOrderBinaryProbesAreComparedInDatasetOrder() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 1;
        spec.rowsPerPage = 1;
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 1000; i++) rows.add(TestRow.of(String.format("k%08d", i), i));
        rows.set(750, TestRow.of("a", 750));
        try (Dataset ds = Dataset.open(TestDatasets.write(tmp, spec, List.of(rows)))) {
            assertEquals(RefusalCode.KEY_ORDER,
                    assertThrows(RefusalException.class, () -> ds.lookup(Bytes.utf8("k00000999"), 1)).code());
        }
    }
    @Test
    void nonMonotoneBoundsKeepThePredecessorOfARepeatedGroup() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.unique = false;
        spec.rowsPerGroup = 19;
        spec.rowsPerPage = 3;
        spec.nonMonotoneKeyBounds = true;
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            rows.add(TestRow.of(i < 498 ? "k0" : i < 802 ? "k1" : "k2", i));
        }
        try (Dataset ds = Dataset.open(TestDatasets.write(tmp, spec, List.of(rows)))) {
            List<Row> group = ds.lookup(Bytes.utf8("k1"), 1000, "size");
            assertEquals(304, group.size());
            assertEquals(0, group.getFirst().offset());
            assertEquals(303, group.getLast().offset());
            assertEquals(new Value.Int64(498), group.getFirst().values().getFirst());
            assertEquals(new Value.Int64(801), group.getLast().values().getFirst());
            assertEquals(Bytes.utf8("k2"), ds.range(RangeQuery.builder().startAfter(Bytes.utf8("k1"))
                    .limit(1).build()).getFirst().key());
        }
    }

    @Test
    void retainedExactFirstKeysAreChargedOnceBeforePublication() throws Exception {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        Path root = TestDatasets.write(tmp, spec, List.of(List.of(TestRow.of("long-key".repeat(100), 1))));
        Manifest manifest = Manifest.parse(Files.readAllBytes(root.resolve("manifest.json")));
        ReadContext ctx = new ReadContext(Cancellation.none(), Limits.DEFAULT);
        long indexBytes;
        try (PartFile part = PartFile.open(LocalStorage.of(root), manifest, 0, ctx, new IndexMemory(Long.MAX_VALUE))) {
            indexBytes = part.keyIndex(0, ctx).memoryBytes();
        }
        long firstBytes = 32L + ((rowsKeyLength(manifest) + 7L) & ~7L);
        for (long budget : new long[] {indexBytes, indexBytes + firstBytes}) {
            try (PartFile part = PartFile.open(LocalStorage.of(root), manifest, 0, ctx, new IndexMemory(budget))) {
                part.keyIndex(0, ctx);
                if (budget == indexBytes) {
                    assertEquals("maxIndexMemoryBytes", assertThrows(LimitExceededException.class,
                            () -> part.firstKey(0, ctx)).limit());
                } else {
                    assertEquals(manifest.parts().getFirst().minKey(), Bytes.wrap(part.firstKey(0, ctx)));
                    assertEquals(manifest.parts().getFirst().minKey(), Bytes.wrap(part.firstKey(0, ctx)));
                }
            }
        }
    }

    @Test
    void oversizedCursorLandingInALaterPartReturnsTheActualNextKey() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.unique = false;
        spec.rowsPerPage = 2;
        spec.rowsPerGroup = 3;
        Path root = TestDatasets.write(tmp, spec, List.of(
                List.of(TestRow.of("a", 0), TestRow.of("b", 1), TestRow.of("b", 2), TestRow.of("c", 3)),
                List.of(TestRow.of("x", 4), TestRow.of("y", 5), TestRow.of("z", 6))));
        try (Dataset ds = Dataset.open(root)) {
            for (long offset : new long[] {2, 3, 4, 5, 6, Long.MAX_VALUE}) {
                List<Row> got = ds.range(RangeQuery.all(10).toBuilder().columns("size")
                        .after(new Continuation.RowPosition(Bytes.utf8("b"), offset)).build());
                assertEquals(List.of("c", "x", "y", "z").stream().map(Bytes::utf8).toList(),
                        got.stream().map(Row::key).toList());
                assertEquals(List.of(3L, 4L, 5L, 6L), got.stream()
                        .map(r -> ((Value.Int64) r.values().getFirst()).value()).toList());
            }
        }
    }

    @Test
    void cachedProbesAndLaterListingSeeksPreserveOrder() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 10;
        spec.rowsPerPage = 10;
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 2000; i++) rows.add(TestRow.of(String.format("d%03d/f%02d", i / 20, i % 20), i));
        try (Dataset ds = Dataset.open(TestDatasets.write(tmp, spec, List.of(rows)))) {
            // Populate exact first keys out of order before the multi-seek listing request.
            for (int d : new int[] {90, 30, 60, 10}) {
                assertEquals(1, ds.lookup(Bytes.utf8(String.format("d%03d/f00", d)), 1).size());
            }
            for (int repeat = 0; repeat < 2; repeat++) {
                List<ListEntry> entries = ds.list(ListQuery.builder((byte) '/').limit(1000).build());
                assertEquals(100, entries.size());
                for (int d = 0; d < 100; d++) {
                    assertEquals(new ListEntry.CommonPrefix(Bytes.utf8(String.format("d%03d/", d))), entries.get(d));
                }
            }
        }
    }

    @Test
    void competingFirstKeyLoadsChargeOnlyThePublishedCopy() throws Exception {
        Path root = TestDatasets.write(tmp, new TestDatasets.Spec(), List.of(List.of(TestRow.of("key", 1))));
        LocalStorage local = LocalStorage.of(root);
        Manifest manifest = Manifest.parse(Files.readAllBytes(root.resolve("manifest.json")));
        ReadContext ctx = new ReadContext(Cancellation.none(), Limits.DEFAULT);
        long budget;
        try (PartFile part = PartFile.open(local, manifest, 0, ctx, new IndexMemory(Long.MAX_VALUE))) {
            budget = part.keyIndex(0, ctx).memoryBytes() + 40;
        }
        AtomicBoolean gate = new AtomicBoolean();
        CyclicBarrier reading = new CyclicBarrier(2);
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
                    @Override public ByteBuffer read(long position, int length, Cancellation token) throws IOException {
                        if (gate.get()) {
                            try {
                                reading.await(10, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                throw new IOException("first-key test barrier", e);
                            }
                        }
                        return inner.read(position, length, token);
                    }
                };
            }
        };
        try (PartFile part = PartFile.open(storage, manifest, 0, ctx, new IndexMemory(budget));
                var executor = Executors.newFixedThreadPool(2)) {
            part.keyIndex(0, ctx);
            gate.set(true);
            var a = executor.submit(() -> part.firstKey(0, new ReadContext(Cancellation.none(), Limits.DEFAULT)));
            var b = executor.submit(() -> part.firstKey(0, new ReadContext(Cancellation.none(), Limits.DEFAULT)));
            assertEquals(Bytes.utf8("key"), Bytes.wrap(a.get(15, TimeUnit.SECONDS)));
            assertEquals(Bytes.utf8("key"), Bytes.wrap(b.get(15, TimeUnit.SECONDS)));
            // A third access is cached; a second budget charge would have failed either racer.
            assertEquals(Bytes.utf8("key"), Bytes.wrap(part.firstKey(0, ctx)));
        }
    }

    private static int rowsKeyLength(Manifest manifest) {
        return manifest.parts().getFirst().minKey().unsafeArray().length;
    }

}
