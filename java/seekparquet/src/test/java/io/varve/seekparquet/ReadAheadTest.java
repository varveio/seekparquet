// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.varve.seekparquet.TestDatasets.Spec;
import io.varve.seekparquet.TestDatasets.TestRow;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link ListQuery#readAhead()}: a listing reads the key pages it predicts it will decode next concurrently,
 * and returns, refuses and fails exactly as it does without read-ahead.
 */
class ReadAheadTest {

    @TempDir
    Path tmp;

    private int savedDefault;

    @BeforeEach
    void noDefaultReadAhead() {
        // The test-wide switch would give the listings these tests compare against a read-ahead too.
        savedDefault = ReadAhead.testDefault;
        ReadAhead.testDefault = 0;
    }

    @AfterEach
    void restoreDefaultReadAhead() {
        ReadAhead.testDefault = savedDefault;
        ReadAhead.testClosed = null;
    }

    private static final String SEGMENTS = "crawl-data/CC-MAIN-2024-18/segments/";
    private static final String[] KINDS = {"warc", "wat", "wet", "robotstxt", "crawldiagnostics"};

    /**
     * Keys shaped like a Common Crawl listing: {@code folders} segments of {@code perFolder} keys each, every
     * key written {@code repeat} times, in one part and one row group of {@code pageRows}-row key pages.
     */
    private Path folders(String name, int folders, int perFolder, int pageRows, int repeat) throws IOException {
        List<TestRow> rows = new ArrayList<>();
        int files = perFolder / KINDS.length;
        for (int i = 0; i < folders * perFolder; i++) {
            int file = i % files;
            int kind = i / files % KINDS.length;
            int segment = i / perFolder;
            String key = String.format("%s1712296817%03d.%02d/%s/CC-MAIN-20240412-%05d.%s.gz", SEGMENTS, segment, segment % 100, KINDS[kind],
                    file, KINDS[kind]);
            for (int r = 0; r < repeat; r++) {
                rows.add(TestRow.of(key, i));
            }
        }
        Collections.sort(rows, (a, b) -> java.util.Arrays.compareUnsigned(a.key(), b.key()));
        Spec spec = new Spec();
        spec.rowsPerPage = pageRows;
        spec.sizeRowsPerPage = rows.size();
        spec.rowsPerGroup = rows.size();
        spec.unique = repeat == 1;
        return TestDatasets.write(tmp.resolve(name), spec, List.of(rows));
    }

    /** One measured listing on a fresh dataset over a slow source. */
    private record Measured(List<ListEntry> entries, ReadStats stats, long wallNanos, long reads, long bytes, int maxInFlight) {}

    private static Measured measure(MemoryStorage storage, long delayNanos, ListQuery query) {
        storage.delayNanos = 0;
        try (Dataset d = Dataset.open(storage)) {
            storage.reset();
            storage.delayNanos = delayNanos;
            AtomicReference<ReadStats> stats = new AtomicReference<>();
            long start = System.nanoTime();
            List<ListEntry> entries = d.list(query, Cancellation.none(), stats::set);
            long wall = System.nanoTime() - start;
            storage.delayNanos = 0;
            assertEquals(0, storage.inFlight.get(), "reads in flight after the listing returned");
            ReadStats st = stats.get();
            assertEquals(storage.reads.get(), st.sourceReads(), "the source's reads against ReadStats");
            assertEquals(storage.bytes.get(), st.sourceBytes(), "the source's bytes against ReadStats");
            return new Measured(entries, st, wall, storage.reads.get(), storage.bytes.get(), storage.maxInFlight.get());
        }
    }

    /**
     * 60 folders of 400 keys in 100-row key pages, with a 10 ms wait per read: without read-ahead the
     * listing makes its page reads one after another; with read-ahead they overlap, at most that many read
     * ahead (and the listing's own read) at once, for the same entries, pages, reads and bytes. The test
     * compares the two wall clocks with each other only, by a wide margin: about 65 waits one after another
     * against about 65 / 8.
     */
    @Test
    void aSlowSourceListsInFewerRoundTrips() throws IOException {
        MemoryStorage storage = MemoryStorage.of(folders("latency", 60, 400, 100, 1));
        long delay = TimeUnit.MILLISECONDS.toNanos(10);
        ListQuery off = ListQuery.builder((byte) '/').prefix(Bytes.utf8(SEGMENTS)).limit(1000).build();
        Measured base = measure(storage.copy(), delay, off);
        assertEquals(60, base.entries.size());
        assertEquals(1, base.maxInFlight, "without read-ahead the reads are one after another");
        assertEquals(0, base.stats.readAheadReads());
        report("ReadAhead 0", base);
        for (int n : new int[] {2, 8, 32}) {
            Measured on = measure(storage.copy(), delay, off.toBuilder().readAhead(n).build());
            report("ReadAhead " + n, on);
            assertEquals(base.entries, on.entries, "ReadAhead " + n);
            assertEquals(base.stats.keyPagesDecoded(), on.stats.keyPagesDecoded(), "ReadAhead " + n + ": key pages decoded");
            assertTrue(on.maxInFlight <= n + 1, "ReadAhead " + n + ": " + on.maxInFlight + " reads at once");
            assertTrue(on.maxInFlight > 1, "ReadAhead " + n + ": no reads overlapped");
            assertTrue(on.stats.readAheadReads() > 0, "ReadAhead " + n + ": nothing read ahead");
            // Exact page bounds predict every page: the pages read ahead and unused are at most those past the end.
            assertTrue(on.stats.readAheadUnused() <= n && on.reads <= base.reads + n,
                    "ReadAhead " + n + ": " + on.stats + " against " + base.stats);
            if (n == 8) {
                assertTrue(on.wallNanos * 2 < base.wallNanos, "ReadAhead 8 took " + on.wallNanos / 1e6 + " ms, without "
                        + base.wallNanos / 1e6 + " ms");
            }
        }
    }

    private static void report(String name, Measured m) {
        System.out.printf("%-12s %3d entries %4d reads %8d bytes %4d key pages  %7.1f ms  max in flight %2d  read ahead %d, unused %d (%d bytes)%n",
                name, m.entries.size(), m.reads, m.bytes, m.stats.keyPagesDecoded(), m.wallNanos / 1e6, m.maxInFlight,
                m.stats.readAheadReads(), m.stats.readAheadUnused(), m.stats.readAheadUnusedBytes());
    }

    /** A fixture whose key page bounds make predictions wrong in different ways, with its keys in order. */
    private record Fixture(String name, MemoryStorage storage, List<String> keys) {}

    private List<Fixture> fixtures() throws IOException {
        List<Fixture> out = new ArrayList<>();
        // Bounds shortened to 8 bytes that overlap from page to page on long shared stems (RG-6), in three parts.
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            keys.add(String.format("stem/%s/%02d/f%05d", "s".repeat(i % 3), i / 40, i));
        }
        Collections.sort(keys);
        Spec spec = new Spec();
        spec.rowsPerPage = 16;
        spec.rowsPerGroup = 400;
        spec.keyBoundBytes = 8;
        out.add(fixture("shortened bounds", spec, keys, 3));
        // Keys far longer than their 64-byte bounds.
        List<String> longKeys = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            longKeys.add("p".repeat(300) + "/" + "q".repeat(i % 3 * 100) + String.format("%04d/f", i / 4) + i % 4);
        }
        Collections.sort(longKeys);
        spec = new Spec();
        spec.rowsPerPage = 6;
        spec.rowsPerGroup = 100;
        spec.keyBoundBytes = 64;
        out.add(fixture("long keys", spec, longKeys, 2));
        // Broad bounds out of order.
        List<String> dirs = new ArrayList<>();
        for (int i = 0; i < 640; i++) {
            dirs.add(String.format("d%02d/f%04d", i / 16, i));
        }
        spec = new Spec();
        spec.rowsPerPage = 4;
        spec.rowsPerGroup = 320;
        spec.nonMonotoneKeyBounds = true;
        out.add(fixture("broad non-monotone bounds", spec, dirs, 1));
        // Repeated keys, in two parts that share no key (PART-4).
        List<String> repeated = new ArrayList<>();
        for (int i = 0; i < 900; i++) {
            for (int r = 0; r <= i % 3; r++) {
                repeated.add(String.format("r%02d/s%d/k%04d", i / 30, i % 2, i));
            }
        }
        Collections.sort(repeated);
        spec = new Spec();
        spec.rowsPerPage = 7;
        spec.rowsPerGroup = 300;
        spec.unique = false;
        out.add(fixture("repeated keys", spec, repeated, 2));
        return out;
    }

    private Fixture fixture(String name, Spec spec, List<String> keys, int parts) throws IOException {
        List<List<TestRow>> split = new ArrayList<>();
        int from = 0;
        for (int p = 0; p < parts; p++) {
            int to = p + 1 == parts ? keys.size() : Math.max(from + 1, keys.size() * (p + 1) / parts);
            while (to < keys.size() && keys.get(to).equals(keys.get(to - 1))) {
                to++;
            }
            List<TestRow> rows = new ArrayList<>();
            for (int i = from; i < to; i++) {
                rows.add(TestRow.of(keys.get(i), i));
            }
            split.add(rows);
            from = to;
        }
        return new Fixture(name, MemoryStorage.of(TestDatasets.write(tmp.resolve(name.replace(' ', '-')), spec, split)), keys);
    }

    /** The queries a fixture is listed with: prefixes, row and prefix cursors, ends, limits and delimiters. */
    private static List<ListQuery> queries(List<String> keys) {
        List<String> probes = new ArrayList<>();
        for (int i = 0; i < keys.size(); i += Math.max(1, keys.size() / 37)) {
            probes.add(keys.get(i));
        }
        List<String> prefixes = new ArrayList<>(List.of(""));
        for (String p : probes) {
            int slash = p.indexOf('/');
            if (slash >= 0 && !prefixes.contains(p.substring(0, slash + 1))) {
                prefixes.add(p.substring(0, slash + 1));
            }
        }
        List<ListQuery> out = new ArrayList<>();
        for (String prefix : prefixes.subList(0, Math.min(prefixes.size(), 6))) {
            for (int i = 0; i < probes.size(); i++) {
                String b = probes.get(i);
                Optional<Continuation> cursor = switch (i % 3) {
                    case 1 -> Optional.of(new Continuation.RowPosition(Bytes.utf8(b), i % 2));
                    case 2 -> Optional.of(new Continuation.CommonPrefix(Bytes.utf8(b)));
                    default -> Optional.empty();
                };
                if (i % 5 != 0 && cursor.isPresent()) {
                    continue;
                }
                for (int limit : new int[] {1, 4, 1000}) {
                    out.add(new ListQuery(Bytes.utf8(prefix), (byte) '/', Optional.empty(), limit, List.of(), cursor));
                }
                if (i % 11 == 0) {
                    out.add(new ListQuery(Bytes.utf8(prefix), (byte) '/', Optional.of(Bytes.utf8(b)), 1000, List.of(), cursor));
                    out.add(new ListQuery(Bytes.utf8(prefix), (byte) 's', Optional.empty(), 50, List.of("size"), cursor));
                }
            }
        }
        return out;
    }

    /** The outcome of a listing: its entries, or its failure (class, refusal code and message). */
    private record Outcome(List<ListEntry> entries, String failure, ReadStats stats) {}

    private static Outcome list(Dataset d, ListQuery q) {
        AtomicReference<ReadStats> stats = new AtomicReference<>();
        try {
            return new Outcome(d.list(q, Cancellation.none(), stats::set), null, stats.get());
        } catch (RuntimeException e) {
            String code = e instanceof RefusalException r ? r.code() + " " : "";
            // Object identities in Thrift's messages differ from run to run.
            return new Outcome(null, (e.getClass().getSimpleName() + " " + code + e.getMessage()).replaceAll("@[0-9a-f]+", "@"), stats.get());
        }
    }

    /** {@return the counts of {@code st} less the pages read ahead and not used, without the read-ahead counts} */
    private static ReadStats beyondUnused(ReadStats st) {
        return new ReadStats(st.partsOpened(), st.keyPagesDecoded(), st.valuePagesDecoded(), st.keysDecoded(), st.rowGroupsOpened(),
                st.indexLoads(), st.pageIndexSeeks(), st.prefixSkips(), st.groupsSkipped(), st.sourceReads() - st.readAheadUnused(),
                st.sourceBytes() - st.readAheadUnusedBytes(), st.listIndexSkips(), st.listIndexDelimiter(), st.listIndexMissingGroup(),
                st.listIndexDensePage(), st.listIndexUnsafeRun());
    }

    /** Checks a listing with read-ahead against the one without: same entries or failure, same counts beyond the unused pages. */
    private static void assertSame(String what, Outcome want, Outcome got) {
        assertEquals(want.failure, got.failure, what + ": failure");
        assertEquals(want.entries, got.entries, what + ": entries");
        assertEquals(want.stats, beyondUnused(got.stats), what + ": counts beyond the unused pages read ahead (" + got.stats + ")");
        assertTrue(got.stats.readAheadUnused() <= got.stats.readAheadReads() && got.stats.readAheadReads() <= got.stats.sourceReads(),
                what + ": " + got.stats);
    }

    /**
     * Every fixture listed with prefixes, cursors, ends and limits, on one dataset per read-ahead width, so
     * that the caches evolve alike: the entries are those without read-ahead (and, for unique keys, the
     * oracle's), every count is equal, and the reads and bytes beyond are exactly the unused pages read ahead.
     */
    @Test
    void entriesAndCountsAreThoseWithoutReadAhead() throws IOException {
        long predicted = 0;
        long wasted = 0;
        int[] widths = {1, 3, 16};
        for (Fixture fx : fixtures()) {
            Dataset base = Dataset.open(fx.storage.copy());
            List<Dataset> with = new ArrayList<>();
            for (int i = 0; i < widths.length; i++) {
                with.add(Dataset.open(fx.storage.copy()));
            }
            try {
                for (ListQuery q : queries(fx.keys)) {
                    Outcome want = list(base, q);
                    if (want.failure == null && fx.name.equals("shortened bounds")) {
                        assertEquals(oracle(fx.keys, q), names(want.entries), fx.name + " " + q);
                    }
                    for (int i = 0; i < widths.length; i++) {
                        Outcome got = list(with.get(i), q.toBuilder().readAhead(widths[i]).build());
                        assertSame(fx.name + ", " + q + ", ReadAhead " + widths[i], want, got);
                        predicted += got.stats.readAheadReads();
                        wasted += got.stats.readAheadUnused();
                    }
                }
            } finally {
                base.close();
                with.forEach(Dataset::close);
            }
        }
        System.out.printf("%d pages read ahead, %d of them unused%n", predicted, wasted);
        assertTrue(predicted > 0, "nothing was read ahead");
        assertTrue(wasted > 0, "no prediction was wrong: the fixtures do not exercise wrong predictions");
    }

    private static List<String> names(List<ListEntry> entries) {
        List<String> out = new ArrayList<>();
        for (ListEntry e : entries) {
            out.add(switch (e) {
                case ListEntry.CommonPrefix p -> "P " + p.prefix().toStringUtf8();
                case ListEntry.RowEntry r -> "R " + r.row().key().toStringUtf8();
            });
        }
        return out;
    }

    /** The entries of a listing over unique sorted keys, computed directly. */
    private static List<String> oracle(List<String> keys, ListQuery q) {
        String prefix = q.prefix().toStringUtf8();
        String delimiter = String.valueOf((char) q.delimiter());
        List<String> out = new ArrayList<>();
        String last = null;
        for (String k : keys) {
            if (!k.startsWith(prefix) || q.end().isPresent() && k.compareTo(q.end().get().toStringUtf8()) >= 0) {
                continue;
            }
            if (q.cursor().isPresent()) {
                String c = q.cursor().get().bytes().toStringUtf8();
                boolean after = switch (q.cursor().get()) {
                    case Continuation.RowPosition r -> k.compareTo(c) > 0;
                    case Continuation.CommonPrefix p -> !k.startsWith(c) && k.compareTo(c) > 0;
                };
                if (!after) {
                    continue;
                }
            }
            int at = k.indexOf(delimiter, prefix.length());
            String entry = at < 0 ? "R " + k : "P " + k.substring(0, at + 1);
            if (!entry.equals(last)) {
                out.add(entry);
                last = entry;
            }
            if (out.size() == q.limit()) {
                break;
            }
        }
        return out;
    }

    /** Pages through listings with cursors, over keys written once and three times, with and without read-ahead. */
    @Test
    void paginationIsUnchanged() throws IOException {
        for (int repeat : new int[] {1, 3}) {
            MemoryStorage storage = MemoryStorage.of(folders("pages-" + repeat, 20, 200, 64, repeat));
            String folder = SEGMENTS + "1712296817003.03/";
            List<ListQuery> bases = List.of(
                    ListQuery.builder((byte) '/').prefix(Bytes.utf8(SEGMENTS)).limit(7).build(),
                    ListQuery.builder((byte) '/').prefix(Bytes.utf8(folder)).limit(2).build(),
                    ListQuery.builder((byte) '/').prefix(Bytes.utf8(folder + "warc/")).limit(50).build(),
                    ListQuery.builder((byte) '|').prefix(Bytes.utf8("crawl-data/")).limit(333).build(),
                    ListQuery.builder((byte) '.').limit(5).build());
            long readAhead = 0;
            for (ListQuery base : bases) {
                List<List<ListEntry>> pages = new ArrayList<>();
                for (int n : new int[] {0, 4}) {
                    List<ListEntry> all = new ArrayList<>();
                    try (Dataset d = Dataset.open(storage.copy())) {
                        ListQuery q = base.toBuilder().readAhead(n).build();
                        for (int i = 0; i < 1000; i++) {
                            AtomicReference<ReadStats> st = new AtomicReference<>();
                            List<ListEntry> page = d.list(q, Cancellation.none(), st::set);
                            readAhead += st.get().readAheadReads();
                            all.addAll(page);
                            if (page.size() < q.limit()) {
                                break;
                            }
                            q = q.next(page.getLast());
                            assertEquals(n, q.readAhead(), "next keeps the read-ahead");
                        }
                    }
                    pages.add(all);
                }
                assertFalse(pages.get(0).isEmpty(), base.toString());
                assertEquals(pages.get(0), pages.get(1), "repeat " + repeat + ", " + base);
            }
            assertTrue(readAhead > 0, "repeat " + repeat + ": nothing was read ahead");
        }
    }

    /**
     * Whatever a single flipped byte does to a part (its header, a page, an index, the footer), a listing
     * fails with the same error, or returns the same entries, with read-ahead as without: what is read ahead
     * is checked as any page read is, and a page read ahead and never decoded changes nothing.
     */
    @Test
    void corruptedPartsFailAlike() throws IOException {
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            rows.add(TestRow.of(String.format("d%02d/f%04d", i / 8, i), i));
        }
        Spec spec = new Spec();
        spec.rowsPerPage = 6;
        spec.rowsPerGroup = 200;
        MemoryStorage clean = MemoryStorage.of(TestDatasets.write(tmp.resolve("flips"), spec, List.of(rows)));
        String part = clean.parts.keySet().iterator().next();
        byte[] original = clean.parts.get(part);
        int failed = 0;
        long ahead = 0;
        for (int off = 0; off < original.length; off += 7) {
            MemoryStorage flipped = clean.copy();
            flipped.parts.get(part)[off] ^= 0x55;
            List<Outcome> outcomes = new ArrayList<>();
            for (int n : new int[] {0, 3, 40}) {
                Outcome o;
                try (Dataset d = Dataset.open(flipped)) {
                    o = list(d, ListQuery.builder((byte) '/').limit(1000).readAhead(n).build());
                }
                outcomes.add(o);
                ahead += o.stats == null ? 0 : o.stats.readAheadReads();
            }
            for (Outcome o : outcomes.subList(1, 3)) {
                assertEquals(outcomes.get(0).failure, o.failure, "byte " + off + " flipped");
                assertEquals(outcomes.get(0).entries, o.entries, "byte " + off + " flipped");
                assertEquals(outcomes.get(0).stats, beyondUnused(o.stats), "byte " + off + " flipped");
            }
            if (outcomes.get(0).failure != null) {
                failed++;
            }
        }
        System.out.printf("%d bytes, every 7th flipped: %d failed alike, %d pages read ahead%n", original.length, failed, ahead);
        assertTrue(failed > 0 && ahead > 0, failed + " flips failed, " + ahead + " pages read ahead");
    }

    /** Key pages out of order, or empty, are refused with the same refusal whether they were read ahead or not. */
    @Test
    void refusalsOfPagesReadAheadAreUnchanged() throws IOException {
        for (String flaw : List.of("out of order", "empty key")) {
            List<TestRow> rows = new ArrayList<>();
            for (int i = 0; i < 400; i++) {
                rows.add(TestRow.of(String.format("d%02d/f%04d", i / 8, i), i));
            }
            if (flaw.equals("out of order")) {
                Collections.swap(rows, 250, 251);
            } else {
                rows.set(300, new TestRow(new byte[0], 300));
            }
            Spec spec = new Spec();
            spec.rowsPerPage = 6;
            spec.rowsPerGroup = 400;
            // The manifest bounds stay those of the sorted keys.
            MemoryStorage storage = MemoryStorage.of(TestDatasets.write(tmp.resolve(flaw.replace(' ', '-')), spec, List.of(rows)));
            List<String> failures = new ArrayList<>();
            for (int n : new int[] {0, 2, 16}) {
                try (Dataset d = Dataset.open(storage.copy())) {
                    RefusalException e = assertThrows(RefusalException.class,
                            () -> d.list(ListQuery.builder((byte) '/').limit(1000).readAhead(n).build()), flaw + ", ReadAhead " + n);
                    failures.add(e.code() + " " + e.getMessage() + " " + e.rowGroup() + " " + e.row() + " " + e.key());
                }
            }
            assertEquals(failures.get(0), failures.get(1), flaw);
            assertEquals(failures.get(0), failures.get(2), flaw);
        }
    }

    /**
     * A storage read that throws on a read-ahead thread, whatever it throws, is a failed read ahead: the page
     * is read again by the listing, which returns what it returns without read-ahead, and no throwable
     * escapes a read-ahead thread.
     */
    @Test
    void aSourceThrowingInsideAReadAheadIsContained() throws IOException {
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            rows.add(TestRow.of(String.format("d%02d/f%04d", i / 6, i), i));
        }
        Spec spec = new Spec();
        spec.rowsPerPage = 4;
        spec.rowsPerGroup = 600;
        MemoryStorage storage = MemoryStorage.of(TestDatasets.write(tmp.resolve("throwing"), spec, List.of(rows)));
        List<ListEntry> want;
        try (Dataset d = Dataset.open(storage.copy())) {
            want = d.list(ListQuery.builder((byte) '/').limit(1000).build());
        }
        Set<Long> seen = ConcurrentHashMap.newKeySet();
        Thread.UncaughtExceptionHandler previous = Thread.getDefaultUncaughtExceptionHandler();
        List<Throwable> escaped = Collections.synchronizedList(new ArrayList<>());
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> escaped.add(e));
        try {
            MemoryStorage throwing = storage.copy();
            throwing.hook = (path, position, length) -> {
                if (MemoryStorage.onReadAheadThread() && seen.add(position)) {
                    switch (seen.size() % 4) {
                        case 0 -> throw new IllegalStateException("the source failed");
                        case 1 -> throw new IOException("the source failed");
                        case 2 -> throw new AssertionError("the source failed");
                        default -> throw new StackOverflowError("the source failed");
                    }
                }
            };
            AtomicReference<ReadStats> st = new AtomicReference<>();
            try (Dataset d = Dataset.open(throwing)) {
                List<ListEntry> got = d.list(ListQuery.builder((byte) '/').limit(1000).readAhead(4).build(), Cancellation.none(), st::set);
                assertEquals(want, got);
            }
            assertTrue(seen.size() >= 4, "only " + seen.size() + " reads ahead threw");
            assertTrue(st.get().readAheadUnused() >= seen.size() && st.get().readAheadReads() >= seen.size(), st.get().toString());
            assertTrue(escaped.isEmpty(), "escaped a read-ahead thread: " + escaped);
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(previous);
        }
    }

    /**
     * A listing over one row group of 30,000 one-key pages, read ahead 64 at a time: the read-ahead's
     * bookkeeping looks at a bounded number of queue entries per page read ahead (it does not grow with the
     * pages it has been through), holds at most 4 times the slots pending, and the entries, reads and bytes
     * are those without read-ahead.
     */
    @Test
    void manyTinyPagesKeepBookkeepingLinear() throws IOException {
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 30_000; i++) {
            rows.add(TestRow.of(String.format("k%08d", i), i));
        }
        Spec spec = new Spec();
        spec.rowsPerPage = 1;
        spec.sizeRowsPerPage = rows.size();
        spec.rowsPerGroup = rows.size();
        MemoryStorage storage = MemoryStorage.of(TestDatasets.write(tmp.resolve("tiny"), spec, List.of(rows)));
        List<ReadAhead> closed = Collections.synchronizedList(new ArrayList<>());
        ReadAhead.testClosed = closed::add;
        List<Outcome> outcomes = new ArrayList<>();
        long[] took = new long[2];
        int[] widths = {0, 64};
        for (int i = 0; i < 2; i++) {
            try (Dataset d = Dataset.open(storage.copy())) {
                // Warm the dataset (footer, page index) so that the listings compare the scan alone.
                d.list(ListQuery.builder((byte) '/').limit(1).readAhead(widths[i]).build());
                closed.clear();
                long start = System.nanoTime();
                outcomes.add(list(d, ListQuery.builder((byte) '/').limit(100_000).readAhead(widths[i]).build()));
                took[i] = System.nanoTime() - start;
            }
        }
        Outcome off = outcomes.get(0);
        Outcome on = outcomes.get(1);
        ReadAhead a = closed.get(closed.size() - 1);
        System.out.printf("30,000 one-key pages: ReadAhead 0 %.1f ms, ReadAhead 64 %.1f ms, %d queue steps, at most %d pending; %s%n",
                took[0] / 1e6, took[1] / 1e6, a.queueSteps, a.mostPending, on.stats);
        assertEquals(30_000, off.entries.size());
        assertSame("ReadAhead 64", off, on);
        assertTrue(on.stats.readAheadReads() > 20_000, on.stats.toString());
        assertEquals(1, closed.size());
        assertTrue(a.mostPending <= 4 * 64, a.mostPending + " pages pending at once");
        long work = on.stats.readAheadReads() + on.stats.keyPagesDecoded();
        assertTrue(a.queueSteps <= 4 * work, a.queueSteps + " queue steps for " + work + " pages read ahead and decoded");
        assertEquals(0, a.inFlight());
    }

    /**
     * A listing whose reads ahead wait ends when its cancellation is cancelled, or when the dataset closes,
     * with no read in flight and every read-ahead thread ended once it returns.
     */
    @Test
    void cancelAndCloseEndTheReadsAhead() throws Exception {
        MemoryStorage storage = MemoryStorage.of(folders("cancel", 40, 500, 64, 1));
        ListQuery query = ListQuery.builder((byte) '/').prefix(Bytes.utf8(SEGMENTS)).limit(1000).readAhead(8)
                .after(new Continuation.CommonPrefix(Bytes.utf8(SEGMENTS + "1712296817001.01/"))).build();
        for (String how : List.of("cancel", "close")) {
            MemoryStorage s = storage.copy();
            Dataset d = Dataset.open(s);
            // Open the part and its row group's index, then make every read ahead wait until it is cancelled.
            d.list(ListQuery.builder((byte) '/').prefix(Bytes.utf8(SEGMENTS)).limit(1).build());
            s.readAheadDelayNanos = Long.MAX_VALUE;
            Cancellation cancel = Cancellation.create();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread lister = Thread.ofPlatform().start(() -> {
                try {
                    d.list(query, cancel);
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (s.readAheadInFlight.get() < 2) {
                assertTrue(System.nanoTime() < deadline, "the reads ahead did not start");
                Thread.sleep(1);
            }
            if (how.equals("cancel")) {
                cancel.cancel();
            } else {
                d.close();
            }
            lister.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse(lister.isAlive(), how + ": the listing did not end");
            assertInstanceOf(CancellationException.class, failure.get(), how);
            assertEquals(0, s.inFlight.get(), how + ": reads in flight after the listing returned");
            assertFalse(s.readAheadThreads.isEmpty());
            for (Thread t : s.readAheadThreads) {
                t.join(TimeUnit.SECONDS.toMillis(30));
                assertFalse(t.isAlive(), how + ": " + t + " outlived the listing");
            }
            d.close();
        }
    }

    /** A '/' listing reads no page ahead while a list index is built; another delimiter, which it declines, does. */
    @Test
    void aListIndexTurnsReadAheadOffForSlashListings() throws IOException {
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            rows.add(TestRow.of(String.format("seg%03d/kind%d/f%04d", i / 40, i / 10 % 4, i), i));
        }
        rows.sort((a, b) -> java.util.Arrays.compareUnsigned(a.key(), b.key()));
        Spec spec = new Spec();
        spec.rowsPerPage = 16;
        spec.rowsPerGroup = 2000;
        MemoryStorage storage = MemoryStorage.of(TestDatasets.write(tmp.resolve("indexed"), spec, List.of(rows)));
        List<ReadStats> slash = new ArrayList<>();
        List<List<ListEntry>> entries = new ArrayList<>();
        for (int n : new int[] {0, 8}) {
            try (Dataset d = Dataset.open(storage.copy())) {
                d.buildListIndex(ListIndexOptions.DEFAULT, Cancellation.none());
                AtomicReference<ReadStats> st = new AtomicReference<>();
                entries.add(d.list(ListQuery.builder((byte) '/').limit(1000).readAhead(n).build(), Cancellation.none(), st::set));
                slash.add(st.get());
                AtomicReference<ReadStats> other = new AtomicReference<>();
                d.list(ListQuery.builder((byte) 'k').limit(1000).readAhead(n).build(), Cancellation.none(),
                        other::set);
                if (n > 0) {
                    assertTrue(other.get().readAheadReads() > 0, "a 'k' listing with the index built read nothing ahead: " + other.get());
                }
            }
        }
        assertEquals(50, entries.get(0).size());
        assertEquals(entries.get(0), entries.get(1));
        assertTrue(slash.get(0).listIndexSkips() > 0, slash.get(0).toString());
        assertEquals(slash.get(0), slash.get(1));
    }

    /** The read-ahead is a query argument: negative is refused, above the maximum is read as it, and builders keep it. */
    @Test
    void theReadAheadArgument() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> ListQuery.builder((byte) '/').limit(1).readAhead(-1).build());
        ListQuery q = ListQuery.builder((byte) '/').limit(1).readAhead(1000).build();
        assertEquals(1000, q.readAhead());
        assertEquals(1000, q.toBuilder().build().readAhead());
        assertEquals(0, new ListQuery(Bytes.empty(), (byte) '/', Optional.empty(), 1, List.of(), Optional.empty()).readAhead());
        MemoryStorage storage = MemoryStorage.of(folders("wide", 30, 100, 10, 1));
        List<ReadAhead> closed = Collections.synchronizedList(new ArrayList<>());
        ReadAhead.testClosed = closed::add;
        Function<Integer, List<ListEntry>> list = n -> {
            try (Dataset d = Dataset.open(storage.copy())) {
                return d.list(ListQuery.builder((byte) '/').prefix(Bytes.utf8(SEGMENTS)).limit(100).readAhead(n).build());
            }
        };
        List<ListEntry> want = list.apply(0);
        assertTrue(closed.isEmpty(), "a listing without read-ahead made one");
        assertEquals(want, list.apply(1000));
        assertEquals(1, closed.size());
        assertNotNull(closed.get(0));
        assertEquals(ListQuery.MAX_READ_AHEAD, closed.get(0).slots);
    }
}
