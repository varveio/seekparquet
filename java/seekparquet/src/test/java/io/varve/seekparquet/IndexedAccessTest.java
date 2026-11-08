// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.varve.seekparquet.TestDatasets.TestRow;
import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.LocalStorage;
import io.varve.seekparquet.storage.PartSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Seeks read a few byte ranges, not the data around them. */
class IndexedAccessTest {

    @TempDir
    Path tmp;

    /** Counts the storage reads after open. */
    static final class CountingStorage implements DatasetStorage {
        final LocalStorage local;
        final AtomicInteger reads = new AtomicInteger();

        CountingStorage(Path root) {
            local = LocalStorage.of(root);
        }

        @Override
        public byte[] readManifest(int maxBytes, Cancellation cancellation) throws IOException {
            return local.readManifest(maxBytes, cancellation);
        }

        @Override
        public PartSource openPart(String path, long expectedSize, Cancellation cancellation) throws IOException {
            PartSource inner = local.openPart(path, expectedSize, cancellation);
            return new PartSource() {
                @Override
                public String path() {
                    return inner.path();
                }

                @Override
                public long size() {
                    return inner.size();
                }

                @Override
                public Optional<String> version() {
                    return inner.version();
                }

                @Override
                public ByteBuffer read(long position, int length, Cancellation c) throws IOException {
                    reads.incrementAndGet();
                    return inner.read(position, length, c);
                }

                @Override
                public void close() throws IOException {
                    inner.close();
                }
            };
        }
    }

    @Test
    void aPointSeekReadsAFewRanges() throws IOException {
        List<List<TestRow>> parts = new ArrayList<>();
        for (int p = 0; p < 4; p++) {
            List<TestRow> rows = new ArrayList<>();
            for (int r = 0; r < 4000; r++) {
                rows.add(TestRow.of(String.format("p%d/dir%03d/file%05d", p, r / 40, r), r));
            }
            parts.add(rows);
        }
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerPage = 20;
        spec.rowsPerGroup = 500;
        CountingStorage storage = new CountingStorage(TestDatasets.write(tmp, spec, parts));
        try (Dataset ds = Dataset.open(storage)) {
            assertEquals(4, storage.reads.get(), "open reads one footer per part");

            storage.reads.set(0);
            List<Row> rows = ds.range(RangeQuery.builder().start(Bytes.utf8("p2/dir050/file02000")).limit(1).columns("size").build());
            assertEquals(Bytes.utf8("p2/dir050/file02000"), rows.getFirst().key());
            assertEquals(new Value.Int64(2000), rows.getFirst().values().getFirst());
            // Scan row-group upper bounds (index loads), then read a key page, value index and value page.
            assertTrue(storage.reads.get() <= 8, "a point seek made " + storage.reads.get() + " reads");

            // A listing of 100 directories of 40 keys each skips to each directory's successor:
            // far fewer reads than the 200 key pages of the part.
            storage.reads.set(0);
            List<ListEntry> dirs = ds.list(ListQuery.builder((byte) '/').prefix(Bytes.utf8("p1/")).limit(1000).build());
            assertEquals(100, dirs.size());
            assertTrue(storage.reads.get() < 140, "a 100-directory listing made " + storage.reads.get() + " reads");
        }
    }
}
