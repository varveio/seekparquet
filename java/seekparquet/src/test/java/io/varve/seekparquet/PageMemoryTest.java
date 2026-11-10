// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import io.varve.seekparquet.TestDatasets.TestRow;
import io.varve.seekparquet.storage.LocalStorage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code maxPageMemory} bounds the memory decoding a page holds, estimated from its header: a page within
 * maxPageValues and maxPageBytes but over a lowered bound fails with {@link LimitExceededException} before
 * its values are allocated, and the same page decodes under a sufficient bound. The default refuses no page
 * the other limits admit.
 */
class PageMemoryTest {

    private static final int MIB = 1 << 20;

    @TempDir
    Path tmp;

    private static long allocated(Runnable r) {
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        long before = bean.getCurrentThreadAllocatedBytes();
        r.run();
        return bean.getCurrentThreadAllocatedBytes() - before;
    }

    private static byte[] uleb(long v) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while ((v & ~0x7FL) != 0) {
            out.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.write((int) v);
        return out.toByteArray();
    }

    private static byte[] le32(int v) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array();
    }

    /** A page of n Booleans, all true, as one RLE run; optional, every row is defined by one run of levels. */
    private static PageHeader booleanRunPage(int n, boolean optional, ByteArrayOutputStream body) {
        byte[] run = uleb((long) n << 1);
        if (optional) {
            body.writeBytes(le32(run.length + 1));
            body.writeBytes(run);
            body.write(1);
        }
        body.writeBytes(le32(run.length + 1));
        body.writeBytes(run);
        body.write(1);
        return new PageHeader(PageType.DATA_PAGE, body.size(), body.size())
                .setData_page_header(new DataPageHeader(n, Encoding.RLE, Encoding.RLE, Encoding.RLE));
    }

    private static ColumnPage decode(PageHeader header, byte[] body, boolean optional, Limits limits) {
        try {
            return PageDecoder.decodeData(header, ByteBuffer.wrap(body), ColumnSpec.of("flag", 0, Column.Type.BOOLEAN, 0, optional),
                    CompressionCodec.UNCOMPRESSED, null, limits, "test page", null, new Codecs.State());
        } catch (DataException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void defaultIsTheMostTheOtherLimitsAdmit() {
        assertEquals(832L * MIB, Limits.DEFAULT.maxPageMemory());
        // The worst page: a byte-array dictionary of maxPageValues entries in maxPageBytes.
        assertEquals(4L * MIB * PageDecoder.DICTIONARY_BINARY_ENTRY_BYTES + 2L * 256 * MIB, Limits.DEFAULT.maxPageMemory());
        Limits small = new Limits(MIB, 10, MIB, MIB, 1000, 100);
        assertEquals(Math.max(100L * 25 + 1000, 100L * 80 + 2000), small.maxPageMemory());
        assertEquals(small.maxPageMemory(), small.withMaxIndexMemoryBytes(5).maxPageMemory());
        assertEquals(12345, Limits.DEFAULT.withMaxPageMemory(12345).maxPageMemory());
        assertEquals(Limits.DEFAULT, Limits.DEFAULT.withMaxPageMemory(12345).withMaxPageMemory(0));
        assertThrows(IllegalArgumentException.class, () -> Limits.DEFAULT.withMaxPageMemory(-1));
        // Every page within maxPageValues and maxPageBytes estimates at most the default.
        ColumnSpec bytes = ColumnSpec.of("v", 0, Column.Type.BYTE_ARRAY, 0, true);
        PageHeader page = new PageHeader(PageType.DATA_PAGE, 256 * MIB, 256 * MIB)
                .setData_page_header(new DataPageHeader(4 * MIB, Encoding.DELTA_BYTE_ARRAY, Encoding.RLE, Encoding.RLE));
        PageHeader dictionary = new PageHeader(PageType.DICTIONARY_PAGE, 256 * MIB, 256 * MIB)
                .setDictionary_page_header(new DictionaryPageHeader(4 * MIB, Encoding.PLAIN));
        assertTrue(PageDecoder.pageMemory(page, bytes, Limits.DEFAULT) <= Limits.DEFAULT.maxPageMemory());
        assertEquals(Limits.DEFAULT.maxPageMemory(), PageDecoder.pageMemory(dictionary, bytes, Limits.DEFAULT));
    }

    /**
     * A million Booleans in a few bytes: within maxPageValues and maxPageBytes, they decode to a million
     * slots (and a million null flags, optional). Under a bound of 512 KiB the page fails with the limit
     * error before anything of that size is allocated; under the default it decodes. An optional column
     * counts its null flags: at 1.5 MB the required page decodes and the optional one does not.
     */
    @Test
    void overTheBoundFailsBeforeDecoding() {
        int n = 1_000_000;
        for (boolean optional : new boolean[] {false, true}) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            PageHeader header = booleanRunPage(n, optional, out);
            byte[] body = out.toByteArray();
            assertTrue(body.length < 64);
            Limits low = Limits.DEFAULT.withMaxPageMemory(512 * 1024);
            LimitExceededException[] thrown = new LimitExceededException[1];
            long used = allocated(() -> thrown[0] = assertThrows(LimitExceededException.class, () -> decode(header, body, optional, low)));
            assertEquals("maxPageMemory", thrown[0].limit());
            assertEquals(optional ? 2L * n : n, thrown[0].declared());
            assertTrue(used < 256 * 1024, "the refused page allocated " + used + " bytes");
            ColumnPage[] page = new ColumnPage[1];
            used = allocated(() -> page[0] = decode(header, body, optional, Limits.DEFAULT));
            assertEquals(n, page[0].rows);
            assertTrue(page[0].bools[0] && page[0].bools[n - 1]);
            assertTrue(used >= n, "the page decoded in " + used + " bytes: the test does not reach the bound");
            Limits between = Limits.DEFAULT.withMaxPageMemory(1_500_000);
            if (optional) {
                assertThrows(LimitExceededException.class, () -> decode(header, body, true, between));
            } else {
                assertEquals(n, decode(header, body, false, between).rows);
            }
        }
    }

    /** A byte-array dictionary page is bounded too: its entries and its bytes, twice. */
    @Test
    void dictionaryPagesAreBounded() throws DataException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            byte[] v = String.format("value-%04d", i).getBytes();
            out.writeBytes(le32(v.length));
            out.writeBytes(v);
        }
        byte[] body = out.toByteArray();
        PageHeader header = new PageHeader(PageType.DICTIONARY_PAGE, body.length, body.length)
                .setDictionary_page_header(new DictionaryPageHeader(count, Encoding.PLAIN));
        ColumnSpec col = ColumnSpec.of("v", 0, Column.Type.BYTE_ARRAY, 0, false);
        long estimate = (long) count * PageDecoder.DICTIONARY_BINARY_ENTRY_BYTES + 2L * body.length;
        assertEquals(estimate, PageDecoder.pageMemory(header, col, Limits.DEFAULT));
        LimitExceededException e = assertThrows(LimitExceededException.class, () -> PageDecoder.decodeDictionary(header, ByteBuffer.wrap(body),
                col, CompressionCodec.UNCOMPRESSED, Limits.DEFAULT.withMaxPageMemory(estimate - 1), "test dictionary", null, new Codecs.State()));
        assertEquals("maxPageMemory", e.limit());
        Dictionary dictionary = PageDecoder.decodeDictionary(header, ByteBuffer.wrap(body), col, CompressionCodec.UNCOMPRESSED,
                Limits.DEFAULT.withMaxPageMemory(estimate), "test dictionary", null, new Codecs.State());
        assertEquals(count - 1, dictionary.getMaxId());
    }

    /**
     * Key pages are bounded as value pages are: a dataset of 400-key pages fails a range with the limit,
     * not a refusal, under a bound below one key page, and reads (keys and an optional value column) under
     * a sufficient one.
     */
    @Test
    void keyPagesAreBounded() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerPage = 400;
        spec.rowsPerGroup = 800;
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 1600; i++) {
            rows.add(TestRow.of(String.format("k%05d", i), i % 7 == 0 ? -1 : i));
        }
        Path root = TestDatasets.write(tmp.resolve("keys"), spec, List.of(rows));
        // A key page of 400 keys holds 400 slots of 24 bytes and its keys' bytes: over 9,600 bytes.
        try (Dataset ds = Dataset.open(LocalStorage.of(root), OpenOptions.DEFAULT.withLimits(Limits.DEFAULT.withMaxPageMemory(9_000)),
                Cancellation.none())) {
            LimitExceededException e = assertThrows(LimitExceededException.class, () -> ds.range(RangeQuery.all(2000)));
            assertEquals("maxPageMemory", e.limit());
        }
        try (Dataset ds = Dataset.open(LocalStorage.of(root), OpenOptions.DEFAULT.withLimits(Limits.DEFAULT.withMaxPageMemory(64 * 1024)),
                Cancellation.none())) {
            List<Row> got = ds.range(RangeQuery.builder().limit(2000).columns("size").build());
            assertEquals(1600, got.size());
            assertEquals(Value.Null.INSTANCE, got.get(0).values().get(0));
            assertEquals(new Value.Int64(1599), got.get(1599).values().get(0));
        }
    }
}
