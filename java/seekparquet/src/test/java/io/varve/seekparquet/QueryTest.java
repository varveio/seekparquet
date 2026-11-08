// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.varve.seekparquet.TestDatasets.TestRow;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Compares range reads, prefix reads and delimited listings over randomly shaped datasets with an
 * independent, sequential implementation of spec §6 over the source rows.
 */
class QueryTest {

    @TempDir
    Path tmp;

    private static final byte[] ALPHABET = {'a', 'b', '/', (byte) 0xFF, 0x00, '-'};

    @Test
    void randomDatasetsAgreeWithTheOracle() throws IOException {
        Random random = new Random(20261108);
        for (int round = 0; round < 40; round++) {
            List<byte[]> keys = randomKeys(random, 1 + random.nextInt(60));
            List<List<TestRow>> parts = split(random, keys);
            TestDatasets.Spec spec = new TestDatasets.Spec();
            spec.rowsPerPage = 1 + random.nextInt(4);
            spec.rowsPerGroup = 1 + random.nextInt(7);
            spec.stringKey = false;
            spec.noteColumn = true;
            Path root = TestDatasets.write(tmp.resolve("d" + round), spec, parts);
            List<TestRow> all = parts.stream().flatMap(List::stream).toList();
            try (Dataset ds = Dataset.open(root)) {
                for (int q = 0; q < 60; q++) {
                    checkRange(ds, all, random);
                    checkList(ds, all, random);
                }
                checkPagination(ds, all, random);
            }
        }
    }

    private static List<byte[]> randomKeys(Random random, int n) {
        TreeSet<byte[]> set = new TreeSet<>(Arrays::compareUnsigned);
        while (set.size() < n) {
            set.add(randomBytes(random, 1 + random.nextInt(5)));
        }
        return new ArrayList<>(set);
    }

    private static byte[] randomBytes(Random random, int len) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = ALPHABET[random.nextInt(ALPHABET.length)];
        }
        return b;
    }

    private static List<List<TestRow>> split(Random random, List<byte[]> keys) {
        List<List<TestRow>> parts = new ArrayList<>();
        int i = 0;
        while (i < keys.size()) {
            int n = 1 + random.nextInt(Math.max(1, keys.size() / 2));
            List<TestRow> part = new ArrayList<>();
            for (int j = i; j < Math.min(keys.size(), i + n); j++) {
                part.add(new TestRow(keys.get(j), random.nextInt(10) == 0 ? -1 : j * 7L));
            }
            parts.add(part);
            i += n;
        }
        return parts;
    }

    private static Bytes maybe(Random random) {
        return random.nextInt(3) == 0 ? null : Bytes.of(randomBytes(random, random.nextInt(4)));
    }

    private static void checkRange(Dataset ds, List<TestRow> all, Random random) {
        Bytes prefix = random.nextBoolean() ? Bytes.empty() : Bytes.of(randomBytes(random, 1 + random.nextInt(2)));
        Bytes start = maybe(random);
        Bytes end = maybe(random);
        boolean exclusive = random.nextBoolean();
        int limit = 1 + random.nextInt(70);
        RangeQuery.Builder b = RangeQuery.builder().prefix(prefix).limit(limit).columns("size", "note", "missing", "key");
        if (start != null) {
            if (exclusive) {
                b.startAfter(start);
            } else {
                b.start(start);
            }
        }
        if (end != null) {
            b.end(end);
        }
        RangeQuery query = b.build();
        List<Row> got = ds.range(query);
        List<TestRow> want = new ArrayList<>();
        for (TestRow r : all) {
            Bytes k = Bytes.of(r.key());
            if (!k.startsWith(prefix)) {
                continue;
            }
            if (start != null && (exclusive ? k.compareTo(start) <= 0 : k.compareTo(start) < 0)) {
                continue;
            }
            if (end != null && k.compareTo(end) >= 0) {
                continue;
            }
            if (want.size() < limit) {
                want.add(r);
            }
        }
        assertEquals(want.size(), got.size(), () -> "range " + query);
        for (int i = 0; i < want.size(); i++) {
            TestRow w = want.get(i);
            Row g = got.get(i);
            assertEquals(Bytes.of(w.key()), g.key(), () -> "range " + query);
            assertEquals(w.size() < 0 ? Value.nullValue() : new Value.Int64(w.size()), g.values().get(0));
            byte[] note = TestDatasets.note(w);
            assertEquals(note == null ? Value.nullValue() : new Value.ByteArray(Bytes.of(note)), g.values().get(1));
            assertEquals(Value.absent(), g.values().get(2));
            assertEquals(new Value.ByteArray(Bytes.of(w.key())), g.values().get(3));
        }
    }

    /** The specification's listing, by walking every row in order. */
    static List<ListEntry> oracleList(List<TestRow> all, ListQuery q) {
        List<ListEntry> out = new ArrayList<>();
        Bytes p = q.prefix();
        for (TestRow r : all) {
            Bytes k = Bytes.of(r.key());
            if (!k.startsWith(p) || (q.after().isPresent() && k.compareTo(q.after().get()) <= 0)
                    || (q.end().isPresent() && k.compareTo(q.end().get()) >= 0)) {
                continue;
            }
            int at = k.indexOf(q.delimiter(), p.length());
            ListEntry e;
            if (at >= 0) {
                Bytes common = k.slice(0, at + 1);
                if (q.continuation() && q.after().isPresent() && common.compareTo(q.after().get()) <= 0) {
                    continue;
                }
                if (!out.isEmpty() && out.getLast().bytes().equals(common)) {
                    continue;
                }
                e = new ListEntry.CommonPrefix(common);
            } else {
                e = new ListEntry.RowEntry(new Row(k, 0, List.of(), List.of()));
            }
            if (out.size() < q.limit()) {
                out.add(e);
            }
        }
        return out;
    }

    private static ListQuery randomList(Random random, int limit) {
        ListQuery.Builder b = ListQuery.builder(ALPHABET[random.nextInt(3)]).limit(limit);
        if (random.nextBoolean()) {
            b.prefix(Bytes.of(randomBytes(random, 1 + random.nextInt(2))));
        }
        Bytes after = maybe(random);
        if (after != null) {
            b.after(after).continuation(random.nextBoolean());
        }
        Bytes end = maybe(random);
        if (end != null) {
            b.end(end);
        }
        return b.build();
    }

    private static void checkList(Dataset ds, List<TestRow> all, Random random) {
        ListQuery q = randomList(random, 1 + random.nextInt(30));
        assertEquals(oracleList(all, q), ds.list(q), () -> "list " + q);
    }

    private static void checkPagination(Dataset ds, List<TestRow> all, Random random) {
        for (int i = 0; i < 10; i++) {
            ListQuery full = randomList(random, Integer.MAX_VALUE).toBuilder().continuation(false).build();
            if (full.after().isPresent()) {
                full = full.toBuilder().after(Bytes.empty()).build();
            }
            List<ListEntry> want = oracleList(all, full);
            ListQuery page = full.toBuilder().limit(1 + random.nextInt(3)).build();
            List<ListEntry> got = new ArrayList<>();
            for (int guard = 0; guard < 1000; guard++) {
                List<ListEntry> entries = ds.list(page);
                got.addAll(entries);
                if (entries.size() < page.limit()) {
                    break;
                }
                page = page.next(entries.getLast());
            }
            assertEquals(want, got, "pagination of " + full);

            RangeQuery range = RangeQuery.all(1 + random.nextInt(5));
            List<Row> rows = new ArrayList<>();
            for (int guard = 0; guard < 1000; guard++) {
                List<Row> r = ds.range(range);
                rows.addAll(r);
                if (r.size() < range.limit()) {
                    break;
                }
                range = range.next(r.getLast());
            }
            assertEquals(all.size(), rows.size());
            for (int j = 0; j < rows.size(); j++) {
                assertTrue(Arrays.equals(all.get(j).key(), rows.get(j).key().toByteArray()));
            }
        }
    }
}
