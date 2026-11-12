// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.management.ThreadMXBean;
import io.varve.seekparquet.TestDatasets.TestRow;
import io.varve.seekparquet.storage.LocalStorage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.parquet.format.BoundaryOrder;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.junit.jupiter.api.io.TempDir;

/** Regressions for the alignment reviews: allocation bounds, statistics delivery, detached refusals, INT96. */
class ReviewFixesTest {

    @TempDir Path tmp;

    private static final int PAYLOAD = 8 << 20;
    private static final long BUDGET = 64 << 10;

    /** Measures the bytes the current thread allocates while {@code op} runs, and its outcome. */
    private static <T> long allocated(ThrowingSupplier<T> op) {
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        assumeTrue(bean.isThreadAllocatedMemorySupported() && bean.isThreadAllocatedMemoryEnabled());
        long before = bean.getCurrentThreadAllocatedBytes();
        try {
            op.get();
        } catch (Throwable ignored) {
            // the outcome is asserted separately
        }
        return bean.getCurrentThreadAllocatedBytes() - before;
    }

    private static byte[] encoded(org.apache.parquet.format.FileMetaData meta) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writeFileMetaData(meta, out);
        return out.toByteArray();
    }

    /** A footer whose one key-value string is {@value #PAYLOAD} bytes. */
    private static byte[] footerWithLargeString() throws IOException {
        SchemaElement root = new SchemaElement("schema");
        root.setNum_children(1);
        SchemaElement key = new SchemaElement("key");
        key.setType(Type.BYTE_ARRAY);
        key.setRepetition_type(FieldRepetitionType.REQUIRED);
        FileMetaData meta = new FileMetaData(1, List.of(root, key), 0, List.of());
        meta.setKey_value_metadata(List.of(new KeyValue("big").setValue("x".repeat(PAYLOAD))));
        return encoded(meta);
    }

    /** A ColumnIndex whose one minimum is {@value #PAYLOAD} bytes. */
    private static byte[] columnIndexWithLargeBinary() throws IOException {
        ColumnIndex ci = new ColumnIndex(List.of(false), List.of(ByteBuffer.wrap(new byte[PAYLOAD])), List.of(ByteBuffer.wrap(new byte[] {1})),
                BoundaryOrder.UNORDERED);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writeColumnIndex(ci, out);
        return out.toByteArray();
    }

    /** A page header whose statistics maximum is {@value #PAYLOAD} bytes. */
    private static byte[] pageHeaderWithLargeBinary() throws IOException {
        DataPageHeader dph = new DataPageHeader(1, Encoding.PLAIN, Encoding.RLE, Encoding.RLE);
        dph.setStatistics(new Statistics().setMax_value(new byte[PAYLOAD]));
        PageHeader h = new PageHeader(PageType.DATA_PAGE, 1, 1).setData_page_header(dph);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writePageHeader(h, out);
        return out.toByteArray();
    }

    private static void overBudgetWithoutAllocating(byte[] raw, ThrowingSupplier<?> decode) {
        LimitExceededException e = assertThrows(LimitExceededException.class, decode::get);
        assertEquals(LimitName.METADATA_MEMORY, e.limit());
        assertEquals(BUDGET, e.maximum());
        long bytes = allocated(decode);
        assertTrue(bytes < PAYLOAD / 8, "the refusal allocated " + bytes + " bytes for a " + PAYLOAD + "-byte field");
    }

    /**
     * A string or binary field larger than the metadata budget is refused before it is allocated, in the
     * footer, page index and page header paths; a declared length beyond the input is malformed, again
     * without allocating it.
     */
    @Test
    void largeStringsAndBinariesAreChargedBeforeTheyAreAllocated() throws IOException, DataException {
        byte[] footer = footerWithLargeString();
        overBudgetWithoutAllocating(footer, () -> Thrift.fileMetaData(footer, BUDGET));
        byte[] index = columnIndexWithLargeBinary();
        overBudgetWithoutAllocating(index, () -> Thrift.columnIndex(index, BUDGET));
        byte[] header = pageHeaderWithLargeBinary();
        overBudgetWithoutAllocating(header, () -> Thrift.pageHeader(header, 0, header.length, BUDGET));
        // Within the budget each decodes.
        assertEquals(PAYLOAD, Thrift.fileMetaData(footer, 4L * PAYLOAD).getKey_value_metadata().getFirst().getValue().length());
        // Truncated: the declared length exceeds the bytes left. Malformed, and nothing of that size allocated.
        byte[] truncated = java.util.Arrays.copyOf(footer, footer.length / 2);
        assertThrows(DataException.class, () -> Thrift.fileMetaData(truncated, Long.MAX_VALUE));
        long bytes = allocated(() -> Thrift.fileMetaData(truncated, Long.MAX_VALUE));
        assertTrue(bytes < PAYLOAD / 8, "a truncated field allocated " + bytes + " bytes");
    }

    private static String manifest(String formatVersion, int parts) {
        StringBuilder p = new StringBuilder();
        for (int i = 0; i < parts; i++) {
            String key = java.util.Base64.getEncoder().encodeToString(String.format("k%07d", i).getBytes(StandardCharsets.UTF_8));
            p.append(i == 0 ? "" : ",").append("{\"path\":\"data/p").append(i).append(".parquet\",\"rows\":1,\"bytes\":100,\"min_key_base64\":\"")
                    .append(key).append("\",\"max_key_base64\":\"").append(key).append("\"}");
        }
        String version = formatVersion.equals("0") ? "\"format_version\":0,\"draft_revision\":1" : "\"format_version\":" + formatVersion;
        return "{\"format\":\"seekparquet\"," + version + ",\"schema_id\":\"s/v1\",\"key\":{\"column\":\"key\",\"unique\":true},\"parts\":[" + p + "]}";
    }

    /**
     * The parts limit is enforced before more than the permitted number of part entries are materialized,
     * including their intermediate JSON representations; the version is still decided first (MAN-7).
     */
    @Test
    void thePartsLimitBoundsWhatParsingKeeps() throws Exception {
        byte[] raw = manifest("0", 5000).getBytes(StandardCharsets.UTF_8);
        Json.Arr parts = (Json.Arr) ((Json.Obj) Json.parse(raw, 3)).members().get("parts");
        assertEquals(3, parts.items().size(), "only the permitted entries are kept");
        assertEquals(5000, parts.total());
        LimitExceededException e = assertThrows(LimitExceededException.class, () -> Manifest.parse(raw, Limits.builder().parts(3).build()));
        assertEquals(LimitName.PARTS, e.limit());
        assertEquals(5000, e.declared());
        // A manifest of another version is refused as such, however many parts it lists.
        byte[] other = manifest("7", 5000).getBytes(StandardCharsets.UTF_8);
        assertEquals(RefusalCode.UNSUPPORTED_VERSION,
                assertThrows(RefusalException.class, () -> Manifest.parse(other, Limits.builder().parts(3).build())).code());
        // The excess is still checked as strict JSON.
        byte[] broken = (manifest("0", 10).replace("]}", ",{\"path\":1,\"path\":2}]}")).getBytes(StandardCharsets.UTF_8);
        assertEquals(RefusalCode.INVALID_MANIFEST,
                assertThrows(RefusalException.class, () -> Manifest.parse(broken, Limits.builder().parts(3).build())).code());
    }

    /**
     * An excess part entry is checked and built into nothing: one with a huge extension array parses with
     * far less allocation than its tree would take, while its syntax, escapes, duplicate names and nesting
     * are still checked.
     */
    @Test
    void anExcessPartEntryIsNotMaterialized() throws Exception {
        String kept = manifest("0", 3);
        StringBuilder huge = new StringBuilder("{\"path\":\"data/x.parquet\",\"rows\":1,\"bytes\":1,\"min_key_base64\":\"eg==\",")
                .append("\"max_key_base64\":\"eg==\",\"extensions\":{\"t\":[");
        for (int i = 0; i < 1_000_000; i++) huge.append(i == 0 ? "" : ",").append("\"v\\u00e9\"");
        huge.append("]}}");
        byte[] raw = kept.replace("]}", "," + huge + "]}").getBytes(StandardCharsets.UTF_8);
        Json.Arr parts = (Json.Arr) ((Json.Obj) Json.parse(raw, 3)).members().get("parts");
        assertEquals(4, parts.total());
        assertEquals(3, parts.items().size());
        long bytes = allocated(() -> Json.parse(raw, 3));
        long whole = allocated(() -> Json.parse(raw, 4));
        assertTrue(bytes < raw.length / 4, "parsing past the limit allocated " + bytes + " bytes for a " + raw.length + "-byte manifest");
        assertTrue(whole > 4 * bytes, "keeping the entry allocates its tree: " + whole + " against " + bytes);
        // Still checked strictly in the excess: an invalid escape, a duplicate name, nesting beyond 512.
        Limits three = Limits.builder().parts(3).build();
        for (String bad : List.of("{\"a\":\"\\q\"}", "{\"a\":1,\"a\":2}", "{\"a\":\"\\ud800\"}")) {
            byte[] m = kept.replace("]}", "," + bad + "]}").getBytes(StandardCharsets.UTF_8);
            assertEquals(RefusalCode.INVALID_MANIFEST, assertThrows(RefusalException.class, () -> Manifest.parse(m, three)).code(), bad);
        }
        byte[] deep = kept.replace("]}", "," + "[".repeat(600) + "]".repeat(600) + "]}").getBytes(StandardCharsets.UTF_8);
        assertEquals(LimitName.MANIFEST_NESTING, assertThrows(LimitExceededException.class, () -> Manifest.parse(deep, three)).limit());
    }

    private Path dataset(String name, TestDatasets.Spec spec, int rows) throws IOException {
        List<TestRow> part = new ArrayList<>();
        for (int i = 0; i < rows; i++) part.add(TestRow.of(String.format("k%05d", i), i % 7 == 3 ? -1 : i));
        return TestDatasets.write(tmp.resolve(name), spec, List.of(part));
    }

    /** A statistics consumer that counts calls and then throws, or not. */
    private static final class Stats implements Consumer<ReadStats> {
        final AtomicInteger calls = new AtomicInteger();
        final RuntimeException thrown;
        ReadStats last;

        Stats(boolean throwing) {
            thrown = throwing ? new IllegalStateException("statistics") : null;
        }

        @Override
        public void accept(ReadStats s) {
            calls.incrementAndGet();
            last = s;
            if (thrown != null) throw thrown;
        }
    }

    private static void deliveredOnce(Class<? extends Throwable> type, java.util.function.Function<ReadOptions, ?> op) {
        Stats quiet = new Stats(false);
        assertThrows(type, () -> op.apply(ReadOptions.of(quiet)));
        assertEquals(1, quiet.calls.get());
        assertEquals(ReadStats.NONE, quiet.last);
        Stats loud = new Stats(true);
        Throwable e = assertThrows(type, () -> op.apply(ReadOptions.of(loud)));
        assertEquals(1, loud.calls.get());
        assertSame(loud.thrown, e.getSuppressed()[0], "the consumer's exception is suppressed onto the argument failure");
    }

    /** An invalid argument of any operation delivers zero statistics exactly once, a throwing consumer suppressed. */
    @Test
    void argumentFailuresDeliverStatisticsOnce() throws IOException {
        try (Dataset d = Dataset.open(dataset("args", new TestDatasets.Spec(), 10))) {
            Plan plan = d.plan(List.of(Selection.of(Span.all())), List.of("size"));
            List<String> withNull = new ArrayList<>();
            withNull.add(null);
            deliveredOnce(NullPointerException.class, o -> { d.scan(Query.builder().build(), o, null); return null; });
            deliveredOnce(NullPointerException.class, o -> d.get(null, List.of(), o));
            deliveredOnce(NullPointerException.class, o -> d.get(Bytes.utf8("k"), withNull, o));
            deliveredOnce(NullPointerException.class, o -> d.plan(List.of(Selection.of(Span.all())), withNull, o));
            deliveredOnce(NullPointerException.class, o -> { plan.scan(o, null); return null; });
            deliveredOnce(IllegalArgumentException.class, o -> plan.split(0, o));
            deliveredOnce(IllegalArgumentException.class, o -> d.rows(Query.builder().limit(Integer.MAX_VALUE + 1L).build(), o));
            deliveredOnce(IllegalArgumentException.class, o -> d.rows(Query.builder().limit(Long.MAX_VALUE).build(), o));
            deliveredOnce(IndexOutOfBoundsException.class, o -> d.columns(5, o));
            deliveredOnce(NullPointerException.class, o -> d.rows(null, o));
            deliveredOnce(NullPointerException.class, o -> { d.scan(null, o, b -> true); return null; });
            deliveredOnce(NullPointerException.class, o -> d.list(null, o));
            deliveredOnce(NullPointerException.class, o -> d.plan(null, List.of(), o));
            // rows takes up to Integer.MAX_VALUE; scan any limit.
            assertEquals(10, d.rows(Query.builder().limit(Integer.MAX_VALUE).build()).size());
            long[] n = new long[1];
            d.scan(Query.builder().limit(Long.MAX_VALUE).build(), b -> { n[0] += b.size(); return true; });
            assertEquals(10, n[0]);
        }
    }

    /**
     * A part's kept refusal is thrown to each request as a copy of its own: what one request attaches to its
     * exception (a throwing statistics consumer) never reaches another, sequentially or concurrently.
     */
    @Test
    void keptRefusalsAreDetachedPerRequest() throws Exception {
        Path root = dataset("missing", new TestDatasets.Spec(), 10);
        Files.delete(root.resolve("data/part-00000.parquet"));
        try (Dataset d = Dataset.open(root)) {
            Stats loud = new Stats(true);
            RefusalException first = assertThrows(RefusalException.class, () -> d.rows(Reads.all(5), ReadOptions.of(loud)));
            assertEquals(RefusalCode.MISSING_PART, first.code());
            assertEquals(1, first.getSuppressed().length);
            RefusalException second = assertThrows(RefusalException.class, () -> d.rows(Reads.all(5)));
            RefusalException third = assertThrows(RefusalException.class, () -> d.rows(Reads.all(5)));
            assertNotSame(first, second);
            assertNotSame(second, third);
            assertEquals(0, second.getSuppressed().length, "a later request inherits nothing");
            assertEquals(first.code(), second.code());
            assertEquals(first.part(), second.part());
            assertEquals(first.getMessage(), second.getMessage());
            ExecutorService pool = Executors.newFixedThreadPool(8);
            try {
                List<Future<Throwable>> results = new ArrayList<>();
                for (int i = 0; i < 64; i++) {
                    boolean throwing = i % 2 == 0;
                    results.add(pool.submit(() -> {
                        Stats s = new Stats(throwing);
                        RefusalException e = assertThrows(RefusalException.class, () -> d.rows(Reads.all(5), ReadOptions.of(s)));
                        assertEquals(throwing ? 1 : 0, e.getSuppressed().length);
                        if (throwing) assertSame(s.thrown, e.getSuppressed()[0]);
                        return e;
                    }));
                }
                List<Throwable> all = new ArrayList<>();
                for (Future<Throwable> f : results) all.add(f.get());
                assertEquals(64, all.stream().map(System::identityHashCode).distinct().count(), "each request has its own exception");
            } finally {
                pool.shutdownNow();
            }
            assertEquals(0, assertThrows(RefusalException.class, () -> d.rows(Reads.all(5))).getSuppressed().length);
        }
    }

    /**
     * A statistics consumer runs after the operation released the dataset: closing from it succeeds, closing
     * from the consumer of an operation nested in a visitor fails (the enclosing scan holds the dataset), and a
     * consumer that blocks does not hold back a concurrent close.
     */
    @Test
    void closingFromAStatisticsConsumer() throws Exception {
        Path root = dataset("close", new TestDatasets.Spec(), 10);
        Dataset top = Dataset.open(root);
        top.rows(Reads.all(5), ReadOptions.of(s -> top.close()));
        assertThrows(IllegalStateException.class, () -> top.rows(Reads.all(5)), "the consumer closed it");
        try (Dataset d = Dataset.open(root)) {
            AtomicReference<Throwable> nested = new AtomicReference<>();
            d.scan(Query.builder().build(), batch -> {
                try {
                    d.rows(Reads.all(1), ReadOptions.of(s -> d.close()));
                } catch (Throwable t) {
                    nested.set(t);
                }
                return false;
            });
            assertTrue(nested.get() instanceof IllegalStateException, "close inside an enclosing scan: " + nested.get());
            assertEquals(10, d.rows(Reads.all(100)).size(), "and the dataset stayed open");
        }
        Dataset d = Dataset.open(root);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> reader = pool.submit(() -> d.rows(Reads.all(5), ReadOptions.of(s -> {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            })));
            entered.await();
            Future<?> closer = pool.submit(d::close);
            closer.get(60, TimeUnit.SECONDS); // completes while the consumer still blocks
            assertFalse(reader.isDone());
            release.countDown();
            reader.get();
            assertThrows(IllegalStateException.class, () -> d.rows(Reads.all(5)));
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    /** Concurrent verification counts exactly the storage calls its part checks and page reads made. */
    @Test
    void concurrentVerifyStatisticsMatchTheRecordedReads() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        List<List<TestRow>> parts = new ArrayList<>();
        for (int p = 0; p < 6; p++) {
            List<TestRow> rows = new ArrayList<>();
            for (int i = 0; i < 12; i++) rows.add(TestRow.of(String.format("p%d/%03d", p, i), i));
            parts.add(rows);
        }
        Path root = TestDatasets.write(tmp.resolve("verify"), spec, parts);
        RangePlanTest.RecordingStorage storage = new RangePlanTest.RecordingStorage(root);
        try (Dataset d = Dataset.open(storage, OpenOptions.DEFAULT.withVerifyConcurrency(4), Cancellation.none())) {
            storage.take();
            AtomicReference<ReadStats> stats = new AtomicReference<>();
            d.verify(ReadOptions.of(stats::set));
            List<RangePlanTest.RecordingStorage.Read> reads = storage.take();
            assertEquals(reads.size(), stats.get().sourceReads());
            assertEquals(reads.stream().mapToLong(RangePlanTest.RecordingStorage.Read::length).sum(), stats.get().sourceBytes());
            assertEquals(6, stats.get().partsOpened());
        }
    }

    /** Page headers of unknown length are probed within min(16 MiB, metadataMemory), below the first 8 KiB window too. */
    @Test
    void headerProbesStayWithinTheMetadataBudget() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.omitValueOffsetIndexes = true; // the size chunk's pages are found by their headers
        spec.rowsPerGroup = 3000;
        spec.rowsPerPage = 400;
        Path root = dataset("headers", spec, 3000);
        assertEquals(16 << 20, Limits.DEFAULT.headerBytes());
        Limits small = Limits.builder().metadataMemory(7000).build();
        assertEquals(7000, small.headerBytes());
        RangePlanTest.RecordingStorage storage = new RangePlanTest.RecordingStorage(root);
        try (Dataset d = Dataset.open(storage, OpenOptions.DEFAULT.withLimits(small), Cancellation.none())) {
            List<Row> rows = d.rows(Reads.all(3000).toBuilder().columns("size").build());
            assertEquals(3000, rows.size());
            assertEquals(new Value.Int64(2998), rows.get(2998).values().getFirst());
            for (RangePlanTest.RecordingStorage.Read r : storage.take()) {
                assertTrue(r.tail() || r.length() != 8192, "the 8 KiB probe window was not clamped to the ceiling: " + r);
            }
        }
    }

    /** A schema nested 64 deep below the root is read; one deeper is an invalid footer, as in Go. */
    @Test
    void schemaNestingMatchesGo() throws DataException {
        assertEquals(64, PartSchema.MAX_NESTING);
        PartSchema.parse(nested(64));
        DataException e = assertThrows(DataException.class, () -> PartSchema.parse(nested(65)));
        assertTrue(e.getMessage().contains("nested"), e.getMessage());
    }

    /** {@return a schema with a key and a group chain of {@code depth} levels below the root, ending in a leaf} */
    private static List<SchemaElement> nested(int depth) {
        List<SchemaElement> out = new ArrayList<>();
        SchemaElement root = new SchemaElement("schema");
        root.setNum_children(2);
        out.add(root);
        SchemaElement key = new SchemaElement("key");
        key.setType(Type.BYTE_ARRAY);
        key.setRepetition_type(FieldRepetitionType.REQUIRED);
        out.add(key);
        for (int i = 1; i < depth; i++) {
            SchemaElement g = new SchemaElement("g" + i);
            g.setRepetition_type(FieldRepetitionType.OPTIONAL);
            g.setNum_children(1);
            out.add(g);
        }
        SchemaElement leaf = new SchemaElement("leaf");
        leaf.setType(Type.INT32);
        leaf.setRepetition_type(FieldRepetitionType.OPTIONAL);
        out.add(leaf);
        return out;
    }

    /** INT96 values are projected as raw bits, least-significant word first, by rows, get and batches. */
    @Test
    void int96IsProjectedAsRawBits() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.int96Size = true;
        try (Dataset d = Dataset.open(dataset("int96", spec, 20))) {
            assertEquals(Column.Type.INT96, d.columns(0).get(1).type());
            assertTrue(d.columns(0).get(1).projectable());
            List<Row> rows = d.rows(Reads.all(100).toBuilder().columns("size").build());
            assertEquals(20, rows.size());
            for (int i = 0; i < 20; i++) {
                Value v = rows.get(i).values().getFirst();
                if (i % 7 == 3) {
                    assertEquals(Value.Null.INSTANCE, v);
                    continue;
                }
                Value.Int96 want = new Value.Int96(i, 0, TestDatasets.int96High(i));
                assertEquals(want, v);
                assertTrue(want.word2() < 0, "the high word keeps its unsigned bits");
                assertEquals(want, Value.Int96.of(want.toByteArray()));
            }
            assertEquals(rows.subList(5, 6), d.get(Bytes.utf8("k00005"), List.of("size")));
            List<Row> batched = new ArrayList<>();
            d.scan(Query.builder().columns("size").build(), batch -> {
                BatchColumn c = batch.column(0);
                assertEquals(Column.Type.INT96, c.type());
                assertEquals(batch.size(), c.size());
                for (int i = 0; i < batch.size(); i++) {
                    int row = i;
                    if (!c.isNull(i)) {
                        assertEquals(c.value(i), c.getInt96(i));
                        assertThrows(IllegalStateException.class, () -> c.getLong(row));
                    }
                    batched.add(batch.row(i));
                }
                return true;
            });
            assertEquals(rows, batched);
        }
        assertThrows(IllegalArgumentException.class, () -> Value.Int96.of(new byte[11]));
    }
}
