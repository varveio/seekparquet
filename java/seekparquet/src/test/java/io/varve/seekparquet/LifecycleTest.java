// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Cancellation, close, concurrency and source ownership. */
class LifecycleTest {

    @TempDir
    Path tmp;

    private Path dataset(int parts, int rowsPerPart) throws IOException {
        List<List<TestRow>> all = new ArrayList<>();
        for (int p = 0; p < parts; p++) {
            List<TestRow> rows = new ArrayList<>();
            for (int r = 0; r < rowsPerPart; r++) {
                rows.add(TestRow.of(String.format("k%03d/%04d", p, r), r));
            }
            all.add(rows);
        }
        return TestDatasets.write(tmp, new TestDatasets.Spec(), all);
    }

    /** A storage whose part reads can be held until released, and which counts open sources. */
    static final class GatedStorage implements DatasetStorage {
        final LocalStorage local;
        final AtomicInteger open = new AtomicInteger();
        volatile CountDownLatch gate;
        final CountDownLatch blocked = new CountDownLatch(1);

        GatedStorage(Path root) {
            local = LocalStorage.of(root);
        }

        @Override
        public byte[] readManifest(int maxBytes, Cancellation cancellation) throws IOException {
            return local.readManifest(maxBytes, cancellation);
        }

        @Override
        public PartSource openPart(String path, long expectedSize, Cancellation cancellation) throws IOException {
            PartSource inner = local.openPart(path, expectedSize, cancellation);
            open.incrementAndGet();
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
                    CountDownLatch g = gate;
                    if (g != null) {
                        blocked.countDown();
                        // Wait until released or cancelled, as a remote read would.
                        CountDownLatch released = new CountDownLatch(1);
                        try (Cancellation.Registration r = c.onCancel(released::countDown)) {
                            while (g.getCount() > 0 && released.getCount() > 0) {
                                try {
                                    released.await(10, java.util.concurrent.TimeUnit.MILLISECONDS);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new java.io.InterruptedIOException();
                                }
                            }
                        }
                        c.throwIfCancelled();
                    }
                    return inner.read(position, length, c);
                }

                @Override
                public void close() throws IOException {
                    open.decrementAndGet();
                    inner.close();
                }
            };
        }
    }

    @Test
    void cancellationEndsABlockedRequest() throws Exception {
        GatedStorage storage = new GatedStorage(dataset(2, 10));
        try (Dataset ds = Dataset.open(storage); ExecutorService ex = Executors.newSingleThreadExecutor()) {
            storage.gate = new CountDownLatch(1);
            Cancellation cancel = Cancellation.create();
            Future<List<Row>> f = ex.submit(() -> ds.range(RangeQuery.all(100), cancel));
            storage.blocked.await();
            cancel.cancel();
            Exception e = assertThrows(java.util.concurrent.ExecutionException.class, f::get);
            assertTrue(e.getCause() instanceof CancellationException, e.getCause().toString());
            storage.gate = null;
            assertEquals(20, ds.range(RangeQuery.all(100)).size());
        }
        assertEquals(0, storage.open.get());
    }

    @Test
    void closeCancelsRequestsInProgressAndClosesSources() throws Exception {
        GatedStorage storage = new GatedStorage(dataset(3, 4));
        Dataset ds = Dataset.open(storage);
        assertEquals(3, storage.open.get());
        try (ExecutorService ex = Executors.newSingleThreadExecutor()) {
            storage.gate = new CountDownLatch(1);
            Future<List<Row>> f = ex.submit(() -> ds.range(RangeQuery.all(100)));
            storage.blocked.await();
            ds.close();
            Exception e = assertThrows(java.util.concurrent.ExecutionException.class, f::get);
            assertTrue(e.getCause() instanceof CancellationException, e.getCause().toString());
        }
        assertEquals(0, storage.open.get());
        assertThrows(IllegalStateException.class, () -> ds.range(RangeQuery.all(1)));
        ds.close();
    }

    @Test
    void aFailedOpenClosesEverySourceItOpened() throws Exception {
        Path root = dataset(4, 3);
        java.nio.file.Files.write(root.resolve("data/part-00002.parquet"), new byte[] {0}, java.nio.file.StandardOpenOption.APPEND);
        for (int concurrency : new int[] {1, 4}) {
            GatedStorage storage = new GatedStorage(root);
            RefusalException e = assertThrows(RefusalException.class,
                    () -> Dataset.open(storage, OpenOptions.DEFAULT.withOpenConcurrency(concurrency), Cancellation.none()));
            assertEquals(RefusalCode.PART_MISMATCH, e.code());
            assertEquals(Optional.of("data/part-00002.parquet"), e.part());
            assertEquals(0, storage.open.get());
        }
    }

    @Test
    void concurrentRequestsAgree() throws Exception {
        Path root = dataset(5, 40);
        try (Dataset ds = Dataset.open(root); ExecutorService ex = Executors.newFixedThreadPool(8)) {
            List<Row> expected = ds.range(RangeQuery.builder().limit(1000).columns("size").build());
            List<ListEntry> listing = ds.list(ListQuery.builder((byte) '/').limit(1000).build());
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                results.add(ex.submit(() -> ds.range(RangeQuery.builder().limit(1000).columns("size").build()).equals(expected)
                        && ds.list(ListQuery.builder((byte) '/').limit(1000).build()).equals(listing)));
            }
            for (Future<Boolean> r : results) {
                assertTrue(r.get());
            }
            assertEquals(200, expected.size());
            assertEquals(5, listing.size());
        }
    }

    @Test
    void invalidRequestsAreRejectedWhenBuilt() {
        assertThrows(IllegalArgumentException.class, () -> RangeQuery.all(0));
        assertThrows(IllegalArgumentException.class, () -> ListQuery.builder((byte) '/').build());
        assertThrows(IllegalArgumentException.class, () -> RangeQuery.builder().limit(-1).build());
    }
}
