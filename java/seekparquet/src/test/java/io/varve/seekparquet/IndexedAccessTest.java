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
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Seeks read a few byte ranges, not the data around them. */
class IndexedAccessTest {

    @TempDir
    Path tmp;

    /**
     * Counts the storage reads. A part's footer reads, made when a request first touches the part
     * (CHECK-1), are counted in {@code footers}, apart from the reads the query itself makes: the tail
     * window, and the bytes of a footer longer than it, which end where the window begins.
     */
    static final class CountingStorage implements DatasetStorage {
        final LocalStorage local;
        final AtomicInteger footers = new AtomicInteger();
        final AtomicInteger reads = new AtomicInteger();
        final AtomicLong bytes = new AtomicLong();

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
                    long end = position + length;
                    if (end == inner.size() && length == Math.min(inner.size(), PartFile.TAIL_WINDOW)
                            || inner.size() > PartFile.TAIL_WINDOW && end == inner.size() - PartFile.TAIL_WINDOW) {
                        footers.incrementAndGet();
                        return inner.read(position, length, c);
                    }
                    reads.incrementAndGet();
                    bytes.addAndGet(length);
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
            assertEquals(0, storage.footers.get() + storage.reads.get(), "open reads no part");
            List<Row> rows = ds.rows(Reads.builder().start(Bytes.utf8("p2/dir050/file02000")).limit(1).columns("size").build());
            assertEquals(Bytes.utf8("p2/dir050/file02000"), rows.getFirst().key());
            assertEquals(new Value.Int64(2000), rows.getFirst().values().getFirst());
            // The part's 4-byte leading magic (CHECK-1: the part is larger than the footer window);
            // five row-group indexes use two narrow reads each instead of overfetching the gaps;
            // then read a key page, value index and value page.
            assertTrue(storage.reads.get() <= 14, "a point seek made " + storage.reads.get() + " reads");
            assertEquals(1, storage.footers.get(), "a point seek checks the one part it routes to");

            // A listing of 100 directories of 40 keys each skips to each directory's successor:
            // far fewer reads than the 200 key pages of the part.
            storage.reads.set(0);
            List<ListEntry> dirs = ds.list(ListQuery.builder((byte) '/').prefix(Bytes.utf8("p1/")).limit(1000).build());
            assertEquals(100, dirs.size());
            assertTrue(storage.reads.get() < 140, "a 100-directory listing made " + storage.reads.get() + " reads");
            assertEquals(2, storage.footers.get(), "a listing of one part's prefix checks that part");
        }
    }
}
