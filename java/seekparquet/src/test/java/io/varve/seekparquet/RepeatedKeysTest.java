// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.varve.seekparquet.TestDatasets.TestRow;
import io.varve.seekparquet.storage.LocalStorage;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Groups spanning pages and row groups retain their positions and opaque file order. */
class RepeatedKeysTest {
    @TempDir Path tmp;

    private Path write(boolean unique) throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.unique = unique;
        spec.keyColumn = "object.id";
        spec.rowsPerPage = 2;
        spec.rowsPerGroup = 3;
        List<TestRow> rows = new ArrayList<>();
        rows.add(TestRow.of("a", 0));
        for (int i = 0; i < 17; i++) {
            rows.add(TestRow.of("b", 17 - i));
        }
        rows.add(TestRow.of("c/1", 19));
        rows.add(TestRow.of("c/2", 20));
        rows.add(TestRow.of("d", 21));
        return TestDatasets.write(tmp, spec, List.of(rows, List.of(TestRow.of("z", 22), TestRow.of("z", 23))));
    }

    @Test
    void lookupStartsAtTheFirstCandidateAndPreservesFileOrder() throws IOException {
        try (Dataset ds = Dataset.open(write(false))) {
            assertEquals(new KeyDefinition("object.id", false), ds.manifest().key());
            List<Row> group = ds.lookup(Bytes.utf8("b"), 100, "size", "object.id", "key");
            assertEquals(17, group.size());
            for (int i = 0; i < group.size(); i++) {
                assertEquals(i, group.get(i).offset());
                assertEquals(List.of(new Value.Int64(17 - i), new Value.ByteArray(Bytes.utf8("b")), Value.absent()), group.get(i).values());
            }
            assertTrue(ds.lookup(Bytes.utf8("bb"), 100).isEmpty());
        }
    }

    @Test
    void rangePaginationResumesInsideGroupsWithTheOriginalBounds() throws IOException {
        try (Dataset ds = Dataset.open(write(false))) {
            RangeQuery q = RangeQuery.lookup(Bytes.utf8("b"), 2).toBuilder().columns("size").build();
            List<Row> got = new ArrayList<>();
            for (int page = 0; page < 20; page++) {
                List<Row> rows = ds.range(q);
                got.addAll(rows);
                if (rows.size() < q.limit()) {
                    break;
                }
                q = q.next(rows.getLast());
            }
            assertEquals(ds.lookup(Bytes.utf8("b"), 100, "size"), got);
            assertEquals(LongStream.range(0, 17).boxed().toList(), got.stream().map(Row::offset).toList());
            assertTrue(ds.range(q.toBuilder().after(new Cursor.RowPosition(Bytes.utf8("b"), Long.MAX_VALUE)).build()).isEmpty());
            // Exclusive bounds apply to the whole group even when a row cursor names that group.
            assertTrue(ds.range(q.toBuilder().startAfter(Bytes.utf8("b")).build()).isEmpty());
        }
    }

    @Test
    void listPaginationCombinesRowOffsetsAndPrefixRollups() throws IOException {
        try (Dataset ds = Dataset.open(write(false))) {
            ListQuery q = ListQuery.builder((byte) '/').limit(1).columns("size").build();
            List<ListEntry> got = new ArrayList<>();
            for (int page = 0; page < 30; page++) {
                List<ListEntry> entries = ds.list(q);
                if (entries.isEmpty()) {
                    break;
                }
                got.addAll(entries);
                q = q.next(entries.getLast());
            }
            assertEquals(ds.list(ListQuery.builder((byte) '/').limit(100).columns("size").build()), got);
            assertEquals(22, got.size());
            assertEquals(new ListEntry.CommonPrefix(Bytes.utf8("c/")), got.get(18));
        }
    }

    @Test
    void equalKeysRemainAnErrorInUniqueMode() throws IOException {
        try (Dataset ds = Dataset.open(write(true))) {
            assertEquals(RefusalCode.KEY_ORDER,
                    assertThrows(RefusalException.class, () -> ds.range(RangeQuery.all(100))).code());
        }
    }

    @Test
    void entryLimitIsConfigurableAndCursorsRejectAmbiguity() throws IOException {
        Limits limits = new Limits(1 << 20, 100, 1 << 20, 1 << 20, 1 << 20, 100, 2);
        try (Dataset ds = Dataset.open(LocalStorage.of(write(false)), OpenOptions.DEFAULT.withLimits(limits), Cancellation.none())) {
            assertEquals(2, ds.lookup(Bytes.utf8("b"), 2).size());
            assertEquals("maxResultEntries", assertThrows(LimitExceededException.class, () -> ds.range(RangeQuery.all(3))).limit());
        }
        assertThrows(IllegalArgumentException.class, () -> new Cursor.RowPosition(Bytes.utf8("b"), -1));
        assertThrows(IllegalArgumentException.class, () -> RangeQuery.builder().limit(1)
                .after(new Cursor.CommonPrefix(Bytes.utf8("b"))).build());
    }
}
