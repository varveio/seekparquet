// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.varve.seekparquet.TestDatasets.TestRow;
import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.LocalStorage;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The value-column OffsetIndex cache, as Go's offsetindex_cache_test.go pins it. */
class OffsetIndexCacheTest {

    @TempDir Path tmp;

    private static final List<String> COLUMNS = List.of("size", "note");
    private static final int SIZE = 1;

    /** 200 keys, one part, with a note column: {@code rowsPerGroup}-row groups of five-row pages. */
    private Path write(String name, int rowsPerGroup) throws Exception {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = rowsPerGroup;
        spec.rowsPerPage = 5;
        spec.noteColumn = true;
        return TestDatasets.write(tmp.resolve(name), spec, List.of(rows()));
    }

    private static List<TestRow> rows() {
        return IntStream.range(0, 200).mapToObj(i -> TestRow.of(key(i), i)).toList();
    }

    private static String key(int i) {
        return String.format("k%03d", i);
    }

    private static Limits budget(long bytes) {
        return bytes == 0 ? Limits.DEFAULT : Limits.DEFAULT.toBuilder().offsetIndexMemory(bytes).build();
    }

    private static Dataset open(DatasetStorage storage, long bytes) {
        return Dataset.open(storage, OpenOptions.DEFAULT.withLimits(budget(bytes)), Cancellation.none());
    }

    private static Dataset open(Path root, long bytes) {
        return open(LocalStorage.of(root), bytes);
    }

    private static FileMetaData footer(byte[] part) throws DataException {
        int length = ByteBuffer.wrap(part, part.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return Thrift.fileMetaData(java.util.Arrays.copyOfRange(part, part.length - 8 - length, part.length - 8), Limits.DEFAULT.metadataMemory());
    }

    private static Path partPath(Path root) {
        return root.resolve("data/part-00000.parquet");
    }

    /** Checks the charge matches what the part's slots hold, within the budget; returns the bytes kept. */
    private static long checkCharge(Dataset d, long bytes) {
        long used = d.parts().offsetIndexMemory().used();
        assertEquals(d.parts().checked(0).keptValueIndexBytes(), used, "charged and kept");
        assertTrue(used <= budget(bytes).offsetIndexMemory(), "within the budget");
        return used;
    }

    /**
     * A kept OffsetIndex saves a later request its read and bytes, and decodes the same pages. Past the
     * budget nothing is kept and every request reads the indexes, with the same rows.
     */
    @Test
    void readStats() throws Exception {
        Path root = write("stats", 100);
        long indexBytes = 0;
        FileMetaData meta = footer(Files.readAllBytes(partPath(root)));
        for (ColumnChunk c : meta.getRow_groups().get(0).getColumns().subList(1, 3)) {
            indexBytes += c.getOffset_index_length();
        }
        Query query = Reads.builder().start(Bytes.utf8(key(10))).limit(50).columns(COLUMNS).build();
        var cachedSource = new RangePlanTest.RecordingStorage(root);
        var uncachedSource = new RangePlanTest.RecordingStorage(root);
        try (Dataset cached = open(cachedSource, 0); Dataset uncached = open(uncachedSource, 1)) {
            AtomicReference<ReadStats> st = new AtomicReference<>();
            Consumer<ReadStats> into = st::set;
            List<Row> want = Reads.range(uncached, query, Cancellation.none(), into);
            ReadStats u1 = st.get();
            uncachedSource.take();
            assertEquals(want, Reads.range(uncached, query, Cancellation.none(), into));
            ReadStats u2 = st.get();
            assertEquals(uncachedSource.take().size(), u2.sourceReads());
            assertEquals(u2, stats(uncached, query), "uncached repeats");
            assertEquals(0, u2.indexLoads());
            assertEquals(0, checkCharge(uncached, 1), "nothing kept over budget");

            assertEquals(want, Reads.range(cached, query, Cancellation.none(), into));
            assertEquals(u1, st.get(), "a cold request");
            assertTrue(cached.parts().checked(0).keptValueIndex(0, 1) && cached.parts().checked(0).keptValueIndex(0, 2));
            assertTrue(checkCharge(cached, 0) > 0);
            cachedSource.take();
            assertEquals(want, Reads.range(cached, query, Cancellation.none(), into));
            ReadStats c2 = st.get();
            List<RangePlanTest.RecordingStorage.Read> reads = cachedSource.take();
            assertEquals(reads.size(), c2.sourceReads());
            assertEquals(reads.stream().mapToLong(RangePlanTest.RecordingStorage.Read::length).sum(), c2.sourceBytes());
            // The repeat skips one read per projected column chunk: its OffsetIndex.
            assertEquals(new ReadStats(u2.partsOpened(), u2.keyPagesDecoded(), u2.valuePagesDecoded(), u2.keysDecoded(),
                    u2.rowGroupsOpened(), u2.indexLoads(), u2.pageIndexSeeks(), u2.prefixSkips(), u2.groupsSkipped(),
                    u2.sourceReads() - COLUMNS.size(), u2.sourceBytes() - indexBytes, u2.listIndexSkips(), u2.listIndexDelimiter(),
                    u2.listIndexMissingGroup(), u2.listIndexDensePage(), u2.listIndexUnsafeRun(), u2.readAheadReads(), u2.readAheadUnused(),
                    u2.readAheadUnusedBytes()), c2, "cached repeat");
        }
    }

    private static ReadStats stats(Dataset d, Query query) {
        AtomicReference<ReadStats> st = new AtomicReference<>();
        Reads.range(d, query, Cancellation.none(), st::set);
        return st.get();
    }

    /**
     * A value column's OffsetIndex that fails its own check is never kept and is refused on every request.
     * One that passes it but misdescribes a page is kept, and still refused on every request, by the page
     * checks each request makes against it.
     */
    @Test
    void malformed() throws Exception {
        record Edit(String name, Consumer<OffsetIndex> edit, boolean kept) {}
        for (Edit e : List.of(
                new Edit("check", index -> index.getPage_locations().get(1).setFirst_row_index(0), false),
                new Edit("page", index -> index.getPage_locations().get(1).setFirst_row_index(
                        index.getPage_locations().get(1).getFirst_row_index() + 1), true))) {
            Path root = write(e.name(), 100);
            byte[] raw = Files.readAllBytes(partPath(root));
            ColumnChunk chunk = footer(raw).getRow_groups().get(0).getColumns().get(SIZE);
            int at = (int) chunk.getOffset_index_offset();
            int length = chunk.getOffset_index_length();
            OffsetIndex index = Thrift.offsetIndex(java.util.Arrays.copyOfRange(raw, at, at + length), Limits.DEFAULT.metadataMemory());
            e.edit().accept(index);
            ByteArrayOutputStream encoded = new ByteArrayOutputStream();
            Util.writeOffsetIndex(index, encoded);
            assertEquals(length, encoded.size(), e.name() + ": the edited index changed size");
            System.arraycopy(encoded.toByteArray(), 0, raw, at, length);
            Files.write(partPath(root), raw);
            try (Dataset d = open(root, 0)) {
                for (int i = 0; i < 3; i++) {
                    RefusalException refusal = assertThrows(RefusalException.class,
                            () -> d.rows(Reads.builder().limit(200).columns("size").build()), e.name() + " request");
                    assertEquals(RefusalCode.INVALID_PARQUET, refusal.code(), e.name());
                }
                assertEquals(e.kept(), d.parts().checked(0).keptValueIndex(0, SIZE), e.name() + ": size locations kept");
                // The intact column is kept and served as before.
                for (int i = 0; i < 2; i++) {
                    assertEquals(200, d.rows(Reads.builder().limit(200).columns("note").build()).size());
                }
                assertTrue(d.parts().checked(0).keptValueIndex(0, 2), e.name() + ": the note column's locations kept");
                checkCharge(d, 0);
            }
        }
    }

    /**
     * Concurrent cold and warm requests at several budgets, from one that keeps nothing to the default:
     * rows are those of a dataset that keeps nothing, and the charge matches what was published.
     */
    @Test
    void concurrent() throws Exception {
        Path root = write("concurrent", 50);
        List<Integer> starts = new ArrayList<>();
        List<List<Row>> want = new ArrayList<>();
        try (Dataset ref = open(root, 1)) {
            for (int s = 0; s < 200; s += 7) {
                starts.add(s);
                want.add(ref.rows(Reads.builder().start(Bytes.utf8(key(s))).limit(30).columns(COLUMNS).build()));
            }
        }
        // Each chunk of ten pages is charged 384 bytes.
        for (long b : new long[] {1, 300, 1000, 2000, 0}) {
            try (Dataset d = open(root, b); ExecutorService pool = Executors.newFixedThreadPool(16)) {
                List<Callable<Void>> workers = new ArrayList<>();
                for (int w = 0; w < 16; w++) {
                    int offset = w;
                    workers.add(() -> {
                        for (int i = 0; i < starts.size(); i++) {
                            int j = (i + offset) % starts.size();
                            List<Row> got = d.rows(Reads.builder().start(Bytes.utf8(key(starts.get(j)))).limit(30).columns(COLUMNS).build());
                            assertEquals(want.get(j), got, "budget " + b + " start " + starts.get(j));
                        }
                        return null;
                    });
                }
                for (Future<Void> f : pool.invokeAll(workers)) {
                    f.get();
                }
                long kept = checkCharge(d, b);
                if (b == 0) {
                    assertEquals(8 * 384, kept, "every chunk kept at the default budget");
                } else if (b <= 300) {
                    assertEquals(0, kept);
                }
            }
        }
    }

    /**
     * A full OffsetIndex budget fails no request, and value indexes no longer take from the key-index
     * budget: the same key indexes are admitted, and refused, whatever the OffsetIndex budget.
     */
    @Test
    void fullBudgetFailsNothingAndLeavesKeyIndexesAlone() throws Exception {
        Path root = TestDatasets.write(tmp.resolve("keys"), new TestDatasets.Spec(),
                List.of(List.of(TestRow.of("k0", 0)), List.of(TestRow.of("k1", 1))));
        List<Row> first = null;
        for (long b : new long[] {1, 0}) {
            // One key index and first key fit in 400 bytes, two do not.
            Limits limits = budget(b).toBuilder().indexMemory(400).build();
            try (Dataset d = Dataset.open(LocalStorage.of(root), OpenOptions.DEFAULT.withLimits(limits), Cancellation.none())) {
                List<Row> rows = d.get(Bytes.utf8("k0"), List.of("size"));
                assertEquals(1, rows.size());
                assertEquals(rows, d.get(Bytes.utf8("k0"), List.of("size")));
                if (first == null) {
                    first = rows;
                }
                assertEquals(first, rows);
                assertEquals(LimitName.INDEX_MEMORY, assertThrows(LimitExceededException.class,
                        () -> d.get(Bytes.utf8("k1"), List.of("size"))).limit());
                assertEquals(b == 1 ? 0 : OffsetIndexMemory.charge(d.parts().checked(0).valueIndex(0, SIZE,
                        new ReadContext(Cancellation.none(), limits)).orElseThrow()), checkCharge(d, b));
            }
        }
        assertArrayEquals(new byte[] {'k', '0'}, first.get(0).key().toByteArray());
    }
}
