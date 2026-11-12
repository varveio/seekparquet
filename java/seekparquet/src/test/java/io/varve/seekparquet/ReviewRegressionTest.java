// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.LocalStorage;
import io.varve.seekparquet.storage.PartSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
class ReviewRegressionTest {
    @TempDir Path tmp;

    private static ColumnPage decode(byte[] body, Encoding encoding, Column.Type type, int count,
            PageDecoder.DictionarySupplier dictionary) throws DataException {
        PageHeader header = new PageHeader(PageType.DATA_PAGE, body.length, body.length)
                .setData_page_header(new DataPageHeader(count, encoding, Encoding.RLE, Encoding.RLE));
        return PageDecoder.decodeData(header, ByteBuffer.wrap(body), ColumnSpec.of("value", 0, type, 0, false),
                CompressionCodec.UNCOMPRESSED, dictionary, Limits.DEFAULT, "test page", null, new Codecs.State());
    }

    /** A page whose header carries a CRC must match it, as in Go; a page without one is not checked. */
    @Test
    void pageChecksums() throws DataException {
        byte[] body = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putLong(7).putLong(8).array();
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(body);
        for (Integer stored : new Integer[] {null, (int) crc.getValue(), (int) crc.getValue() ^ 1}) {
            PageHeader header = new PageHeader(PageType.DATA_PAGE, body.length, body.length)
                    .setData_page_header(new DataPageHeader(2, Encoding.PLAIN, Encoding.RLE, Encoding.RLE));
            if (stored != null) {
                header.setCrc(stored);
            }
            PageDecoder.DictionarySupplier none = null;
            org.junit.jupiter.api.function.Executable read = () -> PageDecoder.decodeData(header, ByteBuffer.wrap(body),
                    ColumnSpec.of("value", 0, Column.Type.INT64, 0, false), CompressionCodec.UNCOMPRESSED, none, Limits.DEFAULT, "test page", null, new Codecs.State());
            if (stored == null || stored == (int) crc.getValue()) {
                assertEquals(7, PageDecoder.decodeData(header, ByteBuffer.wrap(body), ColumnSpec.of("value", 0, Column.Type.INT64, 0, false),
                        CompressionCodec.UNCOMPRESSED, none, Limits.DEFAULT, "test page", null, new Codecs.State()).longs[0]);
            } else {
                assertThrows(DataException.class, read);
            }
        }
    }

    /**
     * parquet-go writes a page whose values are all null, in a dictionary-encoded column, as a bit width
     * and one empty run: it holds no index, and reads as nulls (it is what Go-written datasets hold). An
     * empty run holds nothing anywhere in the levels or indexes, as in Go: the decoder must not read it as
     * values, which parquet-java's own decoder does (it repeats the run's value). Boolean values refuse
     * one, as Go does.
     */
    @Test
    void emptyRleRunsHoldNoValues() throws DataException {
        // The checker: an empty run is stepped over, and returns the stream without it.
        assertArrayEquals(new byte[0], EncodingChecks.rleHybrid(new byte[] {0, 0}, 1, 2, 0, 0));
        assertArrayEquals(new byte[] {4, 1}, EncodingChecks.rleHybrid(new byte[] {0, 1, 4, 1}, 0, 4, 1, 2));
        assertArrayEquals(new byte[] {4, 1}, EncodingChecks.rleHybrid(new byte[] {1, 4, 1}, 0, 3, 1, 2));
        assertEquals(null, EncodingChecks.rleHybrid(new byte[] {4, 1}, 0, 2, 1, 2));
        assertThrows(DataException.class, () -> EncodingChecks.rleHybrid(new byte[] {0, 1}, 0, 2, 1, 1));
        assertThrows(DataException.class, () -> EncodingChecks.rleHybrid(new byte[] {0}, 0, 1, 8, 0));
        // Boolean values: an empty run (value 1), then two false values, is refused, not read as true, true.
        byte[] booleans = {4, 0, 0, 0, 0, 1, 4, 0};
        assertThrows(DataException.class, () -> decode(booleans, Encoding.RLE, Column.Type.BOOLEAN, 2, null));
        // Dictionary indexes of width 1: an empty run of index 1, then two of index 0, read entry 0 twice.
        byte[] entries = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putLong(10).putLong(20).array();
        PageHeader dictHeader = new PageHeader(PageType.DICTIONARY_PAGE, 16, 16)
                .setDictionary_page_header(new org.apache.parquet.format.DictionaryPageHeader(2, Encoding.PLAIN));
        org.apache.parquet.column.Dictionary dict = PageDecoder.decodeDictionary(dictHeader, ByteBuffer.wrap(entries),
                ColumnSpec.of("value", 0, Column.Type.INT64, 0, false), CompressionCodec.UNCOMPRESSED, Limits.DEFAULT, "test", null, new Codecs.State());
        ColumnPage indexed = decode(new byte[] {1, 0, 1, 4, 0}, Encoding.RLE_DICTIONARY, Column.Type.INT64, 2, () -> dict);
        assertEquals(10, indexed.longs[0]);
        assertEquals(10, indexed.longs[1]);
        // The all-null page parquet-go writes: no index, a bit width and an empty run.
        ByteBuffer allNull = ByteBuffer.allocate(4 + 2 + 2).order(ByteOrder.LITTLE_ENDIAN).putInt(2).put((byte) 4).put((byte) 0)
                .put((byte) 0).put((byte) 0);
        PageHeader nullHeader = new PageHeader(PageType.DATA_PAGE, 8, 8)
                .setData_page_header(new DataPageHeader(2, Encoding.RLE_DICTIONARY, Encoding.RLE, Encoding.RLE));
        ColumnPage nulls = PageDecoder.decodeData(nullHeader, ByteBuffer.wrap(allNull.array()), ColumnSpec.of("value", 0, Column.Type.INT64, 0, true),
                CompressionCodec.UNCOMPRESSED, () -> dict, Limits.DEFAULT, "test page", null, new Codecs.State());
        assertArrayEquals(new boolean[] {true, true}, nulls.nulls);
        // Definition levels: an empty run of 0 (null), then three 1s: no row is null.
        byte[] levels = {0, 0, 6, 1};
        ByteBuffer optional = ByteBuffer.allocate(4 + levels.length + 24).order(ByteOrder.LITTLE_ENDIAN).putInt(levels.length).put(levels)
                .putLong(1).putLong(2).putLong(3);
        PageHeader optHeader = new PageHeader(PageType.DATA_PAGE, optional.capacity(), optional.capacity())
                .setData_page_header(new DataPageHeader(3, Encoding.PLAIN, Encoding.RLE, Encoding.RLE));
        ColumnPage defined = PageDecoder.decodeData(optHeader, ByteBuffer.wrap(optional.array()), ColumnSpec.of("value", 0, Column.Type.INT64, 0, true),
                CompressionCodec.UNCOMPRESSED, null, Limits.DEFAULT, "test page", null, new Codecs.State());
        assertEquals(null, defined.nulls == null ? null : java.util.stream.IntStream.range(0, 3).filter(i -> defined.nulls[i]).boxed().findFirst().orElse(null));
        assertEquals(3, defined.longs[2]);
    }

    @Test
    void malformedRleDoesNotFabricateValues() {
        // A packed group header with no payload; a zero run; too few/many RLE values;
        // and bytes following an otherwise complete stream.
        for (byte[] stream : List.of(new byte[] {3}, new byte[] {0, 0}, new byte[] {2, 0},
                new byte[] {18, 0}, new byte[] {16, 0, 2, 0})) {
            byte[] body = ByteBuffer.allocate(4 + stream.length).order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(stream.length).put(stream).array();
            assertThrows(DataException.class, () -> decode(body, Encoding.RLE, Column.Type.BOOLEAN, 8, null));
        }
    }

    @Test
    void finalPackedGroupCanPadOnlyItsLastSevenValues() throws DataException {
        EncodingChecks.rleHybrid(new byte[] {3, 0}, 0, 2, 1, 1);
        assertThrows(DataException.class, () -> EncodingChecks.rleHybrid(new byte[] {5, 0, 0}, 0, 3, 1, 1));
        // Huge and overflowing varints must be refused before run arithmetic or decoder allocation.
        byte[] huge = {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f};
        assertThrows(DataException.class, () -> EncodingChecks.rleHybrid(huge, 0, huge.length, 1, 8));
        byte[] overflowing = Arrays.copyOf(huge, 10);
        overflowing[8] = (byte) 0xff;
        overflowing[9] = 1;
        assertThrows(DataException.class, () -> EncodingChecks.rleHybrid(overflowing, 0, overflowing.length, 1, 8));
    }

    @Test
    void deltaScratchIncludesPaddingAndWidthArrays() throws DataException {
        Limits small = Limits.builder().manifestBytes(1024).parts(4).footerBytes(1024).pageIndexBytes(1024).pageBytes(1024).pageValues(4).build();
        byte[] hostile = {(byte) 128, (byte) 128, 64, 1, 1, 0};
        assertEquals(LimitName.PAGE_BYTES, assertThrows(LimitExceededException.class,
                () -> EncodingChecks.deltaBinaryPacked(hostile, 0, hostile.length, 1, false, small, "test")).limit());
        byte[] ordinary = {(byte) 128, 1, 4, 1, 0};
        assertEquals(ordinary.length, EncodingChecks.deltaBinaryPacked(ordinary, 0, ordinary.length, 1, false, small, "test"));
        // Valid block geometry with a large width array also uses the configured limit.
        byte[] widths = {(byte) 128, (byte) 128, 64, (byte) 128, (byte) 128, 2, 1, 0};
        assertThrows(LimitExceededException.class,
                () -> EncodingChecks.deltaBinaryPacked(widths, 0, widths.length, 1, false, small, "test"));
    }

    @Test
    void dictionaryFailuresKeepTheirClassification() {
        StorageException storage = new StorageException("dictionary read", new SocketTimeoutException());
        CancellationException cancelled = new CancellationException("dictionary cancelled");
        assertSame(storage, assertThrows(StorageException.class,
                () -> decode(new byte[] {0}, Encoding.RLE_DICTIONARY, Column.Type.BYTE_ARRAY, 1, () -> { throw storage; })));
        assertSame(cancelled, assertThrows(CancellationException.class,
                () -> decode(new byte[] {0}, Encoding.RLE_DICTIONARY, Column.Type.BYTE_ARRAY, 1, () -> { throw cancelled; })));
    }

    @Test
    void v2ChecksNullCountsLevelLengthsAndUncompressedSize() {
        ColumnSpec col = ColumnSpec.of("value", 0, Column.Type.INT32, 0, false);
        for (int fault = 0; fault < 3; fault++) {
            DataPageHeaderV2 v2 = new DataPageHeaderV2(1, fault == 0 ? 1 : 0, 1, Encoding.PLAIN, 0, fault == 1 ? 1 : 0)
                    .setIs_compressed(false);
            PageHeader h = new PageHeader(PageType.DATA_PAGE_V2, fault == 2 ? 0 : 4, 4).setData_page_header_v2(v2);
            assertThrows(DataException.class, () -> PageDecoder.decodeData(h, ByteBuffer.wrap(new byte[] {42, 0, 0, 0}),
                    col, CompressionCodec.UNCOMPRESSED, null, Limits.DEFAULT, "test", null, new Codecs.State()));
        }
    }

    private Path dataset(int parts) throws IOException {
        return TestDatasets.write(tmp, new TestDatasets.Spec(), java.util.stream.IntStream.range(0, parts)
                .mapToObj(i -> List.of(TestDatasets.TestRow.of("k" + i, i))).toList());
    }

    private static class Storage implements DatasetStorage {
        final LocalStorage local;
        Storage(Path root) { local = LocalStorage.of(root); }
        @Override public byte[] readManifest(int max, Cancellation c) throws IOException { return local.readManifest(max, c); }
        @Override public PartSource openPart(String path, long size, Cancellation c) throws IOException {
            return local.openPart(path, size, c);
        }
    }

    @Test
    void socketTimeoutsFromManifestOpenAndReadAreStorageFailures() throws IOException {
        Path root = dataset(1);
        for (int stage = 0; stage < 4; stage++) {
            int at = stage;
            AtomicBoolean failRead = new AtomicBoolean(at == 2);
            SocketTimeoutException timeout = new SocketTimeoutException("remote timeout");
            Storage storage = new Storage(root) {
                @Override public byte[] readManifest(int max, Cancellation c) throws IOException {
                    if (at == 0) { throw timeout; }
                    return super.readManifest(max, c);
                }
                @Override public PartSource openPart(String path, long size, Cancellation c) throws IOException {
                    if (at == 1) { throw timeout; }
                    PartSource inner = super.openPart(path, size, c);
                    return new PartSource() {
                        @Override public String path() { return path; }
                        @Override public long size() { return inner.size(); }
                        @Override public Optional<String> version() { return inner.version(); }
                        @Override public ByteBuffer read(long p, int n, Cancellation token) throws IOException {
                            if (failRead.get()) { throw timeout; }
                            return inner.read(p, n, token);
                        }
                        @Override public void close() throws IOException { inner.close(); }
                    };
                }
            };
            if (at == 0) {
                assertSame(timeout, assertThrows(StorageException.class, () -> Dataset.open(storage)).getCause());
                continue;
            }
            // Opening reads no part: the first touch (at 1 and 2) or a later read (at 3) meets the timeout.
            try (Dataset ds = Dataset.open(storage)) {
                if (at == 3) {
                    assertEquals(1, ds.rows(Reads.all(1)).size());
                    failRead.set(true);
                }
                assertSame(timeout, assertThrows(StorageException.class, () -> ds.rows(Reads.all(1))).getCause());
                if (at != 1) {
                    // A storage failure is not kept: the next read checks the part again.
                    failRead.set(false);
                    assertEquals(1, ds.rows(Reads.all(1)).size());
                }
            }
        }
    }

    @Test
    void interruptedVerifyDrainsSuccessfulTasksAndPreservesTheFlag() throws Exception {
        interruptedVerify(2);
    }

    @Test
    void interruptedVerifyDrainsTasksAtTheInternalBound() throws Exception {
        interruptedVerify(0);
    }

    private void interruptedVerify(int concurrency) throws Exception {
        Path root = dataset(3);
        int expectedOpened = concurrency == 0 ? 3 : 2;
        CountDownLatch reading = new CountDownLatch(expectedOpened);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        AtomicInteger opened = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        Storage storage = new Storage(root) {
            @Override public PartSource openPart(String path, long size, Cancellation c) throws IOException {
                PartSource inner = super.openPart(path, size, c);
                opened.incrementAndGet();
                return new PartSource() {
                    @Override public String path() { return path; }
                    @Override public long size() { return inner.size(); }
                    @Override public Optional<String> version() { return inner.version(); }
                    @Override public ByteBuffer read(long p, int n, Cancellation token) throws IOException {
                        try (Cancellation.Registration registration = token.onCancel(cancelled::countDown)) {
                            reading.countDown();
                            try {
                                if (!release.await(10, TimeUnit.SECONDS)) { throw new IOException("test gate timed out"); }
                            } catch (InterruptedException e) { throw new AssertionError(e); }
                            // Simulate successful I/O racing with scope cancellation.
                            return inner.read(p, n, Cancellation.none());
                        }
                    }
                    @Override public void close() throws IOException { closed.incrementAndGet(); inner.close(); }
                };
            }
        };
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Dataset ds = Dataset.open(storage, OpenOptions.DEFAULT, Cancellation.none());
        Parts parts = new Parts(storage, Manifest.parse(Files.readAllBytes(root.resolve("manifest.json")), Limits.DEFAULT), Limits.DEFAULT);
        Thread opener = Thread.ofPlatform().start(() -> {
            try {
                if (concurrency == 0) ds.verify();
                else parts.checkAll(concurrency, new ReadContext(Cancellation.none(), Limits.DEFAULT));
                failure.set(new AssertionError("verify should have been cancelled"));
            } catch (Throwable e) {
                failure.set(e);
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        try {
            assertTrue(reading.await(10, TimeUnit.SECONDS));
            opener.interrupt();
            assertTrue(cancelled.await(10, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            opener.join(10_000);
        }
        assertTrue(failure.get() instanceof CancellationException, String.valueOf(failure.get()));
        assertTrue(interrupted.get());
        assertEquals(expectedOpened, opened.get());
        ds.close();
        assertNull(parts.close());
        assertEquals(opened.get(), closed.get());
    }

    @Test
    void largeFootersDoNotConsumeSharedBuffers() throws Exception {
        Path root = dataset(1);
        Path file = root.resolve("data/part-00000.parquet");
        byte[] original = Files.readAllBytes(file);
        int length = ByteBuffer.wrap(original).order(ByteOrder.LITTLE_ENDIAN).getInt(original.length - 8);
        int start = original.length - 8 - length;
        var footer = Thrift.fileMetaData(Arrays.copyOfRange(original, start, start + length), Limits.DEFAULT.metadataMemory());
        footer.addToKey_value_metadata(new KeyValue("padding").setValue("x".repeat(75_000)));
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        Util.writeFileMetaData(footer, encoded);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(original, 0, start);
        bytes.write(encoded.toByteArray());
        bytes.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(encoded.size()).array());
        bytes.write(new byte[] {'P', 'A', 'R', '1'});
        byte[] data = bytes.toByteArray();
        Files.write(file, data);
        Path manifest = root.resolve("manifest.json");
        Files.writeString(manifest, Files.readString(manifest).replace("\"bytes\":" + original.length, "\"bytes\":" + data.length));
        Map<String, ByteBuffer> cache = new ConcurrentHashMap<>();
        Storage storage = new Storage(root) {
            @Override public PartSource openPart(String path, long size, Cancellation c) {
                return new PartSource() {
                    @Override public String path() { return path; }
                    @Override public long size() { return data.length; }
                    @Override public Optional<String> version() { return Optional.empty(); }
                    @Override public void close() { }
                    @Override public ByteBuffer read(long p, int n, Cancellation token) {
                        return cache.computeIfAbsent(p + ":" + n, key -> {
                            ByteBuffer buffer = ByteBuffer.allocate(n + 3).position(3);
                            buffer.put(data, (int) p, n).position(3);
                            return buffer.asReadOnlyBuffer();
                        });
                    }
                };
            }
        };
        for (int i = 0; i < 2; i++) {
            try (Dataset ds = Dataset.open(storage)) { assertEquals(1, ds.rows(Reads.all(1)).size()); }
        }
        cache.values().forEach(buffer -> assertEquals(3, buffer.position()));
    }

    @Test
    void retainedBudgetCoversKeyIndexesAndConcurrentPublication() throws Exception {
        Path root = dataset(1);
        // One key page costs 320 bytes. Value OffsetIndexes are kept under maxOffsetIndexMemoryBytes instead.
        Limits limits = Limits.DEFAULT.toBuilder().indexMemory(400).build();
        try (Dataset ds = Dataset.open(LocalStorage.of(root), OpenOptions.DEFAULT.withLimits(limits), Cancellation.none());
                var executor = java.util.concurrent.Executors.newFixedThreadPool(4)) {
            var reads = executor.invokeAll(java.util.stream.IntStream.range(0, 16)
                    .<java.util.concurrent.Callable<Integer>>mapToObj(i -> () -> ds.rows(Reads.all(1)).size()).toList());
            for (var read : reads) { assertEquals(1, read.get()); }
            assertEquals(1, ds.rows(Reads.builder().limit(1).columns("size").build()).size());
            assertEquals(1, ds.rows(Reads.all(1)).size());
        }
        try (Dataset ds = Dataset.open(LocalStorage.of(root),
                OpenOptions.DEFAULT.withLimits(limits.toBuilder().indexMemory(1).build()), Cancellation.none())) {
            assertThrows(LimitExceededException.class, () -> ds.rows(Reads.all(1)));
        }
    }

    @Test
    void retainedBudgetIsSharedAcrossParts() throws IOException {
        Limits limits = Limits.DEFAULT.toBuilder().indexMemory(400).build();
        try (Dataset ds = Dataset.open(LocalStorage.of(dataset(2)), OpenOptions.DEFAULT.withLimits(limits), Cancellation.none())) {
            assertEquals(1, ds.get(Bytes.utf8("k0"), List.of()).size());
            assertEquals(LimitName.INDEX_MEMORY, assertThrows(LimitExceededException.class,
                    () -> ds.get(Bytes.utf8("k1"), List.of())).limit());
            assertEquals(1, ds.get(Bytes.utf8("k0"), List.of()).size());
        }
    }

    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "tests the secure-directory-stream route, which Windows lacks: renames a "
            + "directory holding an open file and creates symbolic links, which need a privilege there; "
            + "LocalStoragePlainDirectoryTest covers the Windows route")
    void localPartsRejectSymlinksAndReopenFromPinnedDirectories() throws Exception {
        Path root = dataset(1);
        Path directory = root.resolve("data");
        Path pinned = root.resolve("pinned");
        LocalStorage storage = LocalStorage.of(root);
        try (PartSource source = storage.openPart("data/part-00000.parquet", 0, Cancellation.none())) {
            Files.move(directory, pinned);
            Files.createSymbolicLink(directory, pinned);
            assertThrows(IOException.class, () -> storage.openPart("data/part-00000.parquet", 0, Cancellation.none()));
            Files.createSymbolicLink(root.resolve("linked.parquet"), pinned.resolve("part-00000.parquet"));
            assertThrows(IOException.class, () -> storage.openPart("linked.parquet", 0, Cancellation.none()));
            // Force the shared channel to close as an interrupted read would.
            var field = source.getClass().getDeclaredField("channel");
            field.setAccessible(true);
            ((java.nio.channels.Channel) field.get(source)).close();
            assertEquals(4, source.read(0, 4, Cancellation.none()).remaining());
        }
    }
}
