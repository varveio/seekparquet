// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.varve.seekparquet.TestDatasets.TestRow;
import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.LocalStorage;
import io.varve.seekparquet.storage.PartSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

/**
 * The operations, semantics and defaults this reader shares with the Go reader: statistics on every exit,
 * {@code get} and its cap, unlimited scans, stopping visitors, cursors in plans, the limits and their
 * defaults, and the bounded manifest.
 */
class ApiAlignmentTest {

    @TempDir Path tmp;

    /** Groups: "a" 1 row, "b" 4, "c" 5, "d" 6, "e" 1 (repeated keys), across two parts. */
    private Path groups() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.unique = false;
        spec.rowsPerGroup = 3;
        spec.rowsPerPage = 2;
        List<TestRow> first = new ArrayList<>();
        List<TestRow> second = new ArrayList<>();
        int size = 0;
        first.add(TestRow.of("a", size++));
        for (int i = 0; i < 4; i++) first.add(TestRow.of("b", size++));
        for (int i = 0; i < 5; i++) first.add(TestRow.of("c", size++));
        for (int i = 0; i < 6; i++) second.add(TestRow.of("d", size++));
        second.add(TestRow.of("e", size++));
        return TestDatasets.write(tmp.resolve("groups"), spec, List.of(first, second));
    }

    /** A dataset whose first part's first two keys are out of order: every read of it is refused. */
    private Path refused() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.swapFirstKeys = true;
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 8; i++) rows.add(TestRow.of(String.format("k%02d", i), i));
        return TestDatasets.write(tmp.resolve("refused"), spec, List.of(rows));
    }

    private static Dataset open(Path root, Limits limits) {
        return Dataset.open(LocalStorage.of(root), OpenOptions.DEFAULT.withLimits(limits), Cancellation.none());
    }

    /** A statistics consumer that records what it received and then throws. */
    private static final class ThrowingStats implements Consumer<ReadStats> {
        final RuntimeException thrown = new IllegalStateException("the statistics consumer failed");
        final AtomicInteger calls = new AtomicInteger();
        ReadStats received;

        @Override
        public void accept(ReadStats stats) {
            calls.incrementAndGet();
            received = stats;
            throw thrown;
        }
    }

    /**
     * When an operation fails and its statistics consumer throws too, the operation's exception propagates
     * with the consumer's suppressed onto it, never replaced by it; the consumer is called once.
     */
    private static <T extends Throwable> T failsKeepingItsOwnException(Class<T> type, Executable op, ThrowingStats stats) {
        T e = assertThrows(type, op);
        assertEquals(1, stats.calls.get());
        assertTrue(List.of(e.getSuppressed()).contains(stats.thrown), "the consumer's exception is suppressed: " + e);
        return e;
    }

    @Test
    void aThrowingStatisticsConsumerNeverReplacesTheFailure() throws IOException {
        try (Dataset d = Dataset.open(refused())) {
            ThrowingStats s = new ThrowingStats();
            failsKeepingItsOwnException(RefusalException.class, () -> d.rows(Reads.all(10), ReadOptions.of(s)), s);
            assertTrue(s.received.keyPagesDecoded() > 0, "the failure's statistics are delivered: " + s.received);
            ThrowingStats scan = new ThrowingStats();
            failsKeepingItsOwnException(RefusalException.class, () -> d.scan(Reads.all(0), ReadOptions.of(scan), b -> true), scan);
            ThrowingStats list = new ThrowingStats();
            failsKeepingItsOwnException(RefusalException.class,
                    () -> d.list(ListQuery.builder((byte) '/').limit(10).build(), ReadOptions.of(list)), list);
            ThrowingStats get = new ThrowingStats();
            failsKeepingItsOwnException(RefusalException.class, () -> d.get(Bytes.utf8("k01"), List.of("size"), ReadOptions.of(get)), get);
            ThrowingStats plan = new ThrowingStats();
            failsKeepingItsOwnException(RefusalException.class,
                    () -> d.plan(List.of(Selection.of(Span.all())), List.of("size"), ReadOptions.of(plan)), plan);
            assertTrue(plan.received.keyPagesDecoded() > 0, "a failed plan reports its statistics: " + plan.received);
        }
        try (Dataset d = Dataset.open(groups())) {
            // A visitor's own exception, with a throwing consumer: the visitor's exception propagates.
            RuntimeException visitorFailure = new UnsupportedOperationException("the visitor failed");
            ThrowingStats scan = new ThrowingStats();
            assertSame(visitorFailure, failsKeepingItsOwnException(UnsupportedOperationException.class,
                    () -> d.scan(Reads.all(0), ReadOptions.of(scan), b -> { throw visitorFailure; }), scan));
            Plan plan = d.plan(List.of(Selection.of(Span.all())), List.of("size"));
            ThrowingStats planScan = new ThrowingStats();
            assertSame(visitorFailure, failsKeepingItsOwnException(UnsupportedOperationException.class,
                    () -> plan.scan(ReadOptions.of(planScan), b -> { throw visitorFailure; }), planScan));
            // A split that fails (cancelled) keeps its exception too.
            Cancellation cancelled = Cancellation.create();
            cancelled.cancel();
            ThrowingStats split = new ThrowingStats();
            failsKeepingItsOwnException(CancellationException.class, () -> plan.split(2, new ReadOptions(cancelled, split)), split);
            assertEquals(ReadStats.NONE, split.received);
            // On success the consumer's exception propagates: there is no other.
            ThrowingStats ok = new ThrowingStats();
            assertSame(ok.thrown, assertThrows(IllegalStateException.class, () -> d.rows(Reads.all(1), ReadOptions.of(ok))));
        }
    }

    @Test
    void aFailedPlanOrSplitReportsItsStatistics() throws IOException {
        try (Dataset d = Dataset.open(refused())) {
            AtomicReference<ReadStats> stats = new AtomicReference<>();
            assertThrows(RefusalException.class, () -> d.plan(List.of(Selection.of(Span.all())), List.of(), ReadOptions.of(stats::set)));
            assertTrue(stats.get().keyPagesDecoded() > 0, "the reads before the refusal: " + stats.get());
        }
        try (Dataset d = Dataset.open(groups())) {
            Plan plan = d.plan(List.of(Selection.of(Span.all())), List.of());
            AtomicReference<ReadStats> stats = new AtomicReference<>(ReadStats.NONE);
            Cancellation cancelled = Cancellation.create();
            cancelled.cancel();
            assertThrows(CancellationException.class, () -> plan.split(3, new ReadOptions(cancelled, stats::set)));
            assertEquals(ReadStats.NONE, stats.get());
            // A successful split of repeated keys reads key pages, and reports them once in total.
            AtomicInteger calls = new AtomicInteger();
            plan.split(3, ReadOptions.of(s -> {
                calls.incrementAndGet();
                stats.set(s);
            }));
            assertEquals(1, calls.get());
        }
    }

    /**
     * {@code get} returns whole groups up to the cap: cap - 1, cap and cap + 1 rows, the last failing with
     * the limit and no rows, having read no value page.
     */
    @Test
    void getReturnsWholeGroupsUpToTheCapAndReadsNoValueBeyondIt() throws IOException {
        Limits limits = Limits.builder().getRows(5).build();
        assertEquals(5, limits.getRows());
        try (Dataset d = open(groups(), limits)) {
            AtomicReference<ReadStats> stats = new AtomicReference<>();
            List<Row> b = d.get(Bytes.utf8("b"), List.of("size"), ReadOptions.of(stats::set));
            assertEquals(4, b.size());
            assertEquals(List.of(0L, 1L, 2L, 3L), b.stream().map(Row::offset).toList());
            assertEquals(List.of(new Value.Int64(1), new Value.Int64(2), new Value.Int64(3), new Value.Int64(4)),
                    b.stream().map(r -> r.values().getFirst()).toList());
            assertTrue(stats.get().valuePagesDecoded() > 0);
            List<Row> c = d.get(Bytes.utf8("c"), List.of("size"));
            assertEquals(5, c.size());
            assertEquals(d.rows(Query.builder(Span.key(Bytes.utf8("c"))).limit(100).columns("size").build()), c);
            LimitExceededException e = assertThrows(LimitExceededException.class,
                    () -> d.get(Bytes.utf8("d"), List.of("size"), ReadOptions.of(stats::set)));
            assertEquals(LimitName.GET_ROWS, e.limit());
            assertEquals(-1, e.declared());
            assertEquals(5, e.maximum());
            assertEquals(0, stats.get().valuePagesDecoded(), "an over-cap group reads no value page: " + stats.get());
            assertTrue(stats.get().keysDecoded() >= 6);
            // Missing and unique keys: none and one.
            assertEquals(List.of(), d.get(Bytes.utf8("bb"), List.of("size")));
            assertEquals(1, d.get(Bytes.utf8("e"), List.of()).size());
            // The default cap is Go's 1,000.
            assertEquals(1000, Limits.DEFAULT.getRows());
        }
    }

    /** A scan with limit 0 reads every row to the end of its span, through groups of equal keys, as paged rows do. */
    @Test
    void anUnlimitedScanReadsWhatPagedRowsRead() throws IOException {
        try (Dataset d = Dataset.open(groups())) {
            for (Span span : List.of(Span.all(), Span.key(Bytes.utf8("d")), Span.builder().start(Bytes.utf8("b")).end(Bytes.utf8("d")).build())) {
                Query q = Query.builder(span).columns("size").build();
                List<Row> scanned = new ArrayList<>();
                d.scan(q, batch -> {
                    for (int i = 0; i < batch.size(); i++) scanned.add(batch.row(i));
                    return true;
                });
                List<Row> paged = new ArrayList<>();
                Query page = q.toBuilder().limit(3).build();
                while (true) {
                    List<Row> rows = d.rows(page);
                    paged.addAll(rows);
                    if (rows.size() < page.limit()) break;
                    page = page.next(rows.getLast());
                }
                assertEquals(paged, scanned, span.toString());
                assertFalse(scanned.isEmpty());
            }
            assertEquals(17, countScanned(d, Query.builder().build()));
            assertThrows(IllegalArgumentException.class, () -> d.rows(Query.builder().build()), "rows needs a positive limit");
        }
    }

    private static long countScanned(Dataset d, Query q) {
        long[] n = new long[1];
        d.scan(q, batch -> {
            n[0] += batch.size();
            return true;
        });
        return n[0];
    }

    /** A visitor returning false stops the scan successfully after that batch, for dataset and plan scans. */
    @Test
    void aVisitorStopsTheScan() throws IOException {
        try (Dataset d = Dataset.open(groups())) {
            AtomicInteger visits = new AtomicInteger();
            AtomicReference<ReadStats> stats = new AtomicReference<>();
            d.scan(Query.builder().columns("size").build(), ReadOptions.of(stats::set), batch -> {
                visits.incrementAndGet();
                return false;
            });
            assertEquals(1, visits.get());
            assertTrue(stats.get().keyPagesDecoded() >= 1);
            Plan plan = d.plan(List.of(Selection.of(Span.all())), List.of("size"));
            visits.set(0);
            plan.scan(batch -> visits.incrementAndGet() < 2);
            assertEquals(2, visits.get());
            // A checked exception propagates as itself, without wrapping.
            IOException io = new IOException("sink");
            assertSame(io, assertThrows(IOException.class, () -> d.scan(Query.builder().build(), batch -> {
                throw io;
            })));
        }
    }

    /** A plan's selection may resume after a row cursor inside a group of equal keys. */
    @Test
    void planCursorsResumeInsideEqualKeyGroups() throws IOException {
        try (Dataset d = Dataset.open(groups())) {
            List<Row> d3 = d.rows(Query.builder(Span.key(Bytes.utf8("d"))).limit(100).columns("size").build());
            assertEquals(6, d3.size());
            Plan plan = d.plan(List.of(Selection.after(Span.key(Bytes.utf8("d")), d3.get(2).cursor()),
                    Selection.after(Span.key(Bytes.utf8("b")), new Cursor.RowPosition(Bytes.utf8("b"), Long.MAX_VALUE))), List.of("size"));
            assertEquals(new Estimate(3, true), plan.count());
            assertEquals(List.of("size"), plan.columns());
            assertEquals(d3.subList(3, 6), Reads.rows(plan));
            // A cursor beyond the group continues after it: nothing of b is selected.
            assertEquals(0, d.plan(List.of(Selection.after(Span.key(Bytes.utf8("b")),
                    new Cursor.RowPosition(Bytes.utf8("b"), 4))), List.of()).count().value());
        }
    }

    /** The Go reader's defaults, read from go/limits.go in this repository, so the two cannot drift silently. */
    @Test
    void defaultsAreTheGoReadersAsWritten() throws IOException {
        // The Go source is part of this repository: its absence is a failure, never a skip.
        String property = System.getProperty("seekparquet.conformance");
        assertTrue(property != null, "the build passes the repository's conformance directory");
        Path go = Path.of(property).resolveSibling("go");
        assertTrue(Files.isRegularFile(go.resolve("limits.go")), "go/limits.go is missing: " + go);
        String src = Files.readString(go.resolve("limits.go"), StandardCharsets.UTF_8);
        Map<String, Long> set = new java.util.HashMap<>();
        Matcher m = Pattern.compile("set\\(&l\\.(\\w+), (\\d+)<<(\\d+)\\)").matcher(src);
        while (m.find()) {
            set.put(m.group(1), Long.parseLong(m.group(2)) << Integer.parseInt(m.group(3)));
        }
        Limits l = Limits.DEFAULT;
        assertEquals(set.get("ManifestBytes"), (long) l.manifestBytes());
        assertEquals(set.get("FooterBytes"), (long) l.footerBytes());
        assertEquals(set.get("PageIndexBytes"), (long) l.pageIndexBytes());
        assertEquals(set.get("MetadataMemory"), l.metadataMemory());
        assertEquals(set.get("PageBytes"), (long) l.pageBytes());
        assertEquals(set.get("PageValues"), (long) l.pageValues());
        assertEquals(set.get("IndexMemory"), l.indexMemory());
        assertEquals(set.get("OffsetIndexMemory"), l.offsetIndexMemory());
        // Go-only until Java batched reads land: Java retains no raw page-index bytes from a part's end, so it has
        // no counterpart of Go's PageIndexCacheMemory. Each name must still be a default of go/limits.go.
        for (String goOnly : List.of("PageIndexCacheMemory")) {
            assertTrue(set.remove(goOnly) != null, "a Go-only default is no longer in go/limits.go: " + goOnly);
        }
        assertEquals(8, set.size(), "every shifted default of go/limits.go is compared: " + set);
        assertEquals(Long.parseLong(group(src, "l\\.Parts = ([0-9_]+)").replace("_", "")), l.parts());
        assertEquals(Long.parseLong(group(src, "l\\.GetRows = (\\d+)")), l.getRows());
        long per = Long.parseLong(group(src, "optionalValueSlices = (\\d+)")) * Long.parseLong(group(src, "valueBytes = (\\d+)"));
        assertEquals(per * l.pageValues() + l.pageBytes(), l.pageMemory());
        assertEquals(832L << 20, l.pageMemory());
        // The page-header probe ceiling: min(16 MiB, MetadataMemory).
        Matcher header = Pattern.compile("headerBytes\\(\\) int64 \\{ return min\\((\\d+)<<(\\d+), l\\.MetadataMemory\\) \\}").matcher(src);
        assertTrue(header.find(), "go/limits.go headerBytes");
        long ceiling = Long.parseLong(header.group(1)) << Integer.parseInt(header.group(2));
        assertEquals(ceiling, l.headerBytes());
        assertEquals(4096, Limits.builder().metadataMemory(4096).build().headerBytes());
        // The schema nesting bound of go/footer.go's checkSchema.
        String footer = Files.readString(go.resolve("footer.go"), StandardCharsets.UTF_8);
        assertEquals(Long.parseLong(group(footer, "if depth > (\\d+) \\{\\s*return errors.New\\(\"the schema is nested too deeply\"\\)")), PartSchema.MAX_NESTING);
        String storageIO = Files.readString(go.resolve("storageio.go"), StandardCharsets.UTF_8);
        assertEquals(Integer.parseInt(group(storageIO, "const readConcurrency = (\\d+)")), Dataset.VERIFY_CONCURRENCY);
        assertEquals(List.of("limits"), java.util.Arrays.stream(OpenOptions.class.getRecordComponents())
                .map(java.lang.reflect.RecordComponent::getName).toList());
        assertEquals(ReadOptions.DEFAULT.cancellation(), Cancellation.none());
    }

    private static String group(String src, String regex) {
        Matcher m = Pattern.compile(regex).matcher(src);
        assertTrue(m.find(), regex);
        return m.group(1);
    }

    @Test
    void limitsRejectNegativesAndTakeDefaultsForZero() {
        List<Consumer<Limits.Builder>> negatives = List.of(b -> b.manifestBytes(-1), b -> b.parts(-1), b -> b.footerBytes(-1),
                b -> b.pageIndexBytes(-1), b -> b.metadataMemory(-1), b -> b.pageBytes(-1), b -> b.pageValues(-1),
                b -> b.pageMemory(-1), b -> b.indexMemory(-1), b -> b.offsetIndexMemory(-1), b -> b.getRows(-1));
        for (Consumer<Limits.Builder> n : negatives) {
            Limits.Builder b = Limits.builder();
            n.accept(b);
            assertThrows(IllegalArgumentException.class, b::build);
        }
        Limits zero = Limits.builder().manifestBytes(0).parts(0).footerBytes(0).pageIndexBytes(0).metadataMemory(0).pageBytes(0)
                .pageValues(0).pageMemory(0).indexMemory(0).offsetIndexMemory(0).getRows(0).build();
        assertEquals(Limits.DEFAULT, zero);
        // A derived default follows what it derives from; an explicit setting stays.
        Limits fewer = Limits.DEFAULT.toBuilder().pageValues(1000).build();
        assertEquals(48L * 1000 + fewer.pageBytes(), fewer.pageMemory());
        assertEquals(7, Limits.builder().pageMemory(7).build().toBuilder().pageValues(1000).build().pageMemory());
    }

    /** The decoded-metadata budget bounds each footer as it is decoded, failing with the limit. */
    @Test
    void metadataMemoryBoundsDecodedFooters() throws IOException {
        Path root = groups();
        try (Dataset d = open(root, Limits.builder().metadataMemory(512).build())) {
            LimitExceededException e = assertThrows(LimitExceededException.class, () -> d.rows(Reads.all(1)));
            assertEquals(LimitName.METADATA_MEMORY, e.limit());
            assertEquals(512, e.maximum());
        }
        try (Dataset d = open(root, Limits.DEFAULT)) {
            assertEquals(17, d.rows(Reads.all(100)).size());
        }
    }

    private static final String PART = "{\"path\":\"data/p%d.parquet\",\"rows\":%d,\"bytes\":100,\"min_key_base64\":\"%s\",\"max_key_base64\":\"%s\"}";

    private static byte[] manifest(long... rows) {
        StringBuilder parts = new StringBuilder();
        for (int i = 0; i < rows.length; i++) {
            String key = java.util.Base64.getEncoder().encodeToString(new byte[] {(byte) ('a' + i)});
            parts.append(i == 0 ? "" : ",").append(String.format(PART, i, rows[i], key, key));
        }
        return ("{\"format\":\"seekparquet\",\"format_version\":0,\"draft_revision\":1,\"schema_id\":\"s/v1\","
                + "\"key\":{\"column\":\"key\",\"unique\":false},\"parts\":[" + parts + "]}").getBytes(StandardCharsets.UTF_8);
    }

    private static DatasetStorage manifestOnly(byte[] raw) {
        return new DatasetStorage() {
            @Override
            public byte[] readManifest(int maxBytes, Cancellation cancellation) {
                return raw;
            }

            @Override
            public PartSource openPart(String path, long expectedSize, Cancellation cancellation) throws IOException {
                throw new IOException("no parts here");
            }
        };
    }

    @Test
    void openRefusesARowTotalBeyondSigned64Bits() {
        long half = Long.MAX_VALUE / 2 + 1;
        LimitExceededException e = assertThrows(LimitExceededException.class, () -> Dataset.open(manifestOnly(manifest(half, half))));
        assertEquals(LimitName.DATASET_ROWS, e.limit());
        try (Dataset d = Dataset.open(manifestOnly(manifest(half, half - 1)))) {
            assertEquals(Long.MAX_VALUE, d.rowCount());
        }
    }

    @Test
    void manifestParsingIsBounded() {
        byte[] three = manifest(1, 1, 1);
        assertEquals(3, Manifest.parse(three, Limits.builder().parts(3).build()).parts().size());
        LimitExceededException parts = assertThrows(LimitExceededException.class, () -> Manifest.parse(three, Limits.builder().parts(2).build()));
        assertEquals(LimitName.PARTS, parts.limit());
        assertEquals(3, parts.declared());
        assertEquals(2, parts.maximum());
        LimitExceededException bytes = assertThrows(LimitExceededException.class,
                () -> Manifest.parse(three, Limits.builder().manifestBytes(three.length - 1).build()));
        assertEquals(LimitName.MANIFEST_BYTES, bytes.limit());
    }

    @Test
    void keysFromTextAreStrictAndPrefixEndsAreNotExactKeys() {
        assertThrows(IllegalArgumentException.class, () -> Bytes.utf8("a\ud800"));
        assertThrows(IllegalArgumentException.class, () -> Bytes.utf8("\udc00b"));
        assertEquals(Bytes.of((byte) 0xF0, (byte) 0x9F, (byte) 0x98, (byte) 0x80), Bytes.utf8("😀"));
        assertEquals("a�", Bytes.of((byte) 'a', (byte) 0xFF).toStringUtf8(), "display decoding is lossy");
        assertEquals(Optional.of(Bytes.utf8("b")), Bytes.utf8("a").prefixEnd());
        assertEquals(Optional.of(Bytes.utf8("b")), Bytes.of((byte) 'a', (byte) 0xFF).prefixEnd());
        assertEquals(Optional.empty(), Bytes.of((byte) 0xFF).prefixEnd());
        assertArrayEquals(new byte[] {'a', 0}, Span.key(Bytes.utf8("a")).end().orElseThrow().toByteArray(), "an exact key's span ends at key + 0x00");
    }

    @Test
    void absentNullAndPresentAreDisjoint() throws IOException {
        try (Dataset d = Dataset.open(groups())) {
            d.scan(Query.builder().columns("size", "nothing").build(), batch -> {
                assertTrue(batch.column(0).isPresent());
                assertFalse(batch.column(1).isPresent());
                for (int i = 0; i < batch.size(); i++) {
                    assertFalse(batch.column(1).isNull(i), "absent is not null");
                    assertEquals(Value.Absent.INSTANCE, batch.column(1).value(i));
                    assertTrue(batch.key(i).isReadOnly());
                    assertEquals(batch.copyKey(i), Bytes.of(batch.key(i)));
                    assertEquals(batch.row(i).cursor(), batch.cursor(i));
                }
                return true;
            });
        }
    }
}
