// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.varve.seekparquet.TestDatasets.TestRow;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BYTE_STREAM_SPLIT values, outside the ENC-1 baseline, decoded as the Go reader decodes them: {@code
 * FLOAT}, {@code DOUBLE}, {@code INT32}, {@code INT64} and {@code FIXED_LEN_BYTE_ARRAY}, holding exactly
 * their values; other types are malformed. Pages are encoded by parquet-java's writers, independently of the
 * decoder.
 */
class ByteStreamSplitTest {

    @TempDir Path tmp;

    private static final HeapByteBufferAllocator HEAP = new HeapByteBufferAllocator();

    private static byte[] bytes(ValuesWriter w) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        w.getBytes().writeAllTo(out);
        return out.toByteArray();
    }

    private static ColumnPage decode(byte[] body, Column.Type type, int typeLength, int count, boolean scratch) throws DataException {
        PageHeader header = new PageHeader(PageType.DATA_PAGE, body.length, body.length)
                .setData_page_header(new DataPageHeader(count, Encoding.BYTE_STREAM_SPLIT, Encoding.RLE, Encoding.RLE));
        return PageDecoder.decodeData(header, ByteBuffer.wrap(body), ColumnSpec.of("value", 0, type, typeLength, false),
                CompressionCodec.UNCOMPRESSED, null, Limits.DEFAULT, "test page", scratch ? new PageScratch() : null, new Codecs.State());
    }

    @Test
    void fixedWidthValuesKeepTheirExactBits() throws Exception {
        int[] floats = {Float.floatToRawIntBits(1.5f), Float.floatToRawIntBits(-0.0f), 0x7fc01234, Float.floatToRawIntBits(Float.NEGATIVE_INFINITY)};
        var f = new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(64, 1024, HEAP);
        var i32 = new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(64, 1024, HEAP);
        for (int v : floats) {
            f.writeFloat(Float.intBitsToFloat(v));
            i32.writeInteger(v);
        }
        long[] doubles = {Double.doubleToRawLongBits(-2.25), 0x7ff8000000000abcL, Long.MIN_VALUE, 1L};
        var d = new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(64, 1024, HEAP);
        var i64 = new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(64, 1024, HEAP);
        for (long v : doubles) {
            d.writeDouble(Double.longBitsToDouble(v));
            i64.writeLong(v);
        }
        for (boolean scratch : new boolean[] {false, true}) {
            assertArrayEquals(floats, Arrays.copyOf(decode(bytes(f), Column.Type.FLOAT, 0, 4, scratch).ints, 4));
            assertArrayEquals(floats, Arrays.copyOf(decode(bytes(i32), Column.Type.INT32, 0, 4, scratch).ints, 4));
            assertArrayEquals(doubles, Arrays.copyOf(decode(bytes(d), Column.Type.DOUBLE, 0, 4, scratch).longs, 4));
            assertArrayEquals(doubles, Arrays.copyOf(decode(bytes(i64), Column.Type.INT64, 0, 4, scratch).longs, 4));
        }
    }

    @Test
    void fixedLenByteArraysDecodeWithAndWithoutScratch() throws Exception {
        var w = new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(3, 64, 1024, HEAP);
        List<Bytes> values = List.of(Bytes.utf8("abc"), Bytes.utf8("xyz"), Bytes.of((byte) 0, (byte) -1, (byte) 7));
        for (Bytes v : values) {
            w.writeBytes(Binary.fromConstantByteArray(v.toByteArray()));
        }
        byte[] body = bytes(w);
        for (boolean scratch : new boolean[] {false, true}) {
            ColumnPage page = decode(body, Column.Type.FIXED_LEN_BYTE_ARRAY, 3, 3, scratch);
            for (int i = 0; i < values.size(); i++) {
                assertEquals(new Value.FixedLenByteArray(values.get(i)), page.value(i));
            }
        }
    }

    @Test
    void aPageHoldsExactlyItsValuesOfASplittableType() throws Exception {
        var w = new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(64, 1024, HEAP);
        for (long v = 0; v < 4; v++) {
            w.writeLong(v);
        }
        byte[] four = bytes(w);
        // Four values where three are declared, a value short, and a stray byte are all malformed.
        assertThrows(DataException.class, () -> decode(four, Column.Type.INT64, 0, 3, false));
        assertThrows(DataException.class, () -> decode(four, Column.Type.INT64, 0, 5, false));
        assertThrows(DataException.class, () -> decode(Arrays.copyOf(four, 33), Column.Type.INT64, 0, 4, false));
        assertThrows(DataException.class, () -> decode(new byte[12], Column.Type.FIXED_LEN_BYTE_ARRAY, 5, 2, true));
        DataException e = assertThrows(DataException.class, () -> decode(new byte[8], Column.Type.BYTE_ARRAY, 0, 2, true));
        assertTrue(e.getMessage().contains("BYTE_STREAM_SPLIT values in a BYTE_ARRAY column"), e.getMessage());
        assertThrows(DataException.class, () -> decode(new byte[1], Column.Type.BOOLEAN, 0, 2, true));
    }

    /** An optional INT64 column split into streams, with nulls, read by a range read, a scan and a batch scan. */
    @Test
    void anOptionalSplitColumnReadsThroughEveryPath() throws IOException {
        TestDatasets.Spec spec = new TestDatasets.Spec();
        spec.rowsPerGroup = 12;
        spec.rowsPerPage = 5;
        spec.byteStreamSplitSize = true;
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            rows.add(TestRow.of(String.format("k%03d", i), i % 4 == 0 ? -1 : (long) i << 40 | 0xabcdL));
        }
        try (Dataset d = Dataset.open(TestDatasets.write(tmp, spec, List.of(rows)))) {
            List<Row> got = d.range(RangeQuery.builder().limit(100).columns("size").build());
            assertEquals(30, got.size());
            for (int i = 0; i < 30; i++) {
                long size = rows.get(i).size();
                assertEquals(size < 0 ? Value.Null.INSTANCE : new Value.Int64(size), got.get(i).values().getFirst());
            }
            RangePlan plan = d.planRanges(List.of(RangeQuery.all(1)), List.of("size"));
            List<Row> scanned = new ArrayList<>();
            plan.scan(scanned::add);
            assertEquals(got, scanned);
            assertEquals(got, BatchScanTest.rows(plan::scanBatches));
        }
    }

    /** {@return {@code v} as little-endian bytes} */
    static byte[] le64(long v) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array();
    }

    @Test
    void theStreamsAreTheValuesBytesTransposed() throws Exception {
        // Two INT64 values: stream b holds byte b of each value.
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        byte[] a = le64(0x0102030405060708L);
        byte[] b = le64(-2);
        for (int k = 0; k < 8; k++) {
            body.write(a[k]);
            body.write(b[k]);
        }
        assertArrayEquals(new long[] {0x0102030405060708L, -2}, Arrays.copyOf(decode(body.toByteArray(), Column.Type.INT64, 0, 2, false).longs, 2));
    }
}
