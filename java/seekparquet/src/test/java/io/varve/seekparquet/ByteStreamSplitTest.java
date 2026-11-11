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

    private static ColumnPage decode(byte[] body, Column.Type type, int typeLength, int count, boolean optional, PageScratch scratch, Codecs.State codecs)
            throws DataException {
        PageHeader header = new PageHeader(PageType.DATA_PAGE, body.length, body.length)
                .setData_page_header(new DataPageHeader(count, Encoding.BYTE_STREAM_SPLIT, Encoding.RLE, Encoding.RLE));
        return PageDecoder.decodeData(header, ByteBuffer.wrap(body), ColumnSpec.of("value", 0, type, typeLength, optional),
                CompressionCodec.UNCOMPRESSED, null, Limits.DEFAULT, "test page", scratch, codecs);
    }

    /** {@return a data page body: RLE definition levels (a bit-packed run; present values are 1) then the values} */
    private static byte[] withLevels(boolean[] present, byte[] values) {
        int groups = (present.length + 7) / 8;
        byte[] levels = new byte[1 + groups];
        levels[0] = (byte) (groups << 1 | 1);
        for (int i = 0; i < present.length; i++) {
            if (present[i]) {
                levels[1 + i / 8] |= (byte) (1 << (i % 8));
            }
        }
        ByteBuffer out = ByteBuffer.allocate(4 + levels.length + values.length).order(ByteOrder.LITTLE_ENDIAN);
        out.putInt(levels.length).put(levels).put(values);
        return out.array();
    }

    /** Checks an INT32 or FLOAT page: nulls where {@code present} is false, {@code want} (bits) in order elsewhere. */
    private static void assertInts(ColumnPage page, boolean[] present, int[] want) {
        for (int i = 0, j = 0; i < present.length; i++) {
            if (present[i]) {
                assertEquals(want[j++], page.ints[i], "value " + i);
                assertTrue(!(page.value(i) instanceof Value.Null), "value " + i);
            } else {
                assertEquals(Value.Null.INSTANCE, page.value(i), "value " + i);
            }
        }
    }

    private static void assertLongs(ColumnPage page, boolean[] present, long[] want) {
        for (int i = 0, j = 0; i < present.length; i++) {
            if (present[i]) {
                assertEquals(want[j++], page.longs[i], "value " + i);
            } else {
                assertEquals(Value.Null.INSTANCE, page.value(i), "value " + i);
            }
        }
    }

    private static byte[] ints(int... values) throws IOException {
        var w = new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(64, 1024, HEAP);
        for (int v : values) {
            w.writeInteger(v);
        }
        return bytes(w);
    }

    private static byte[] floats(int... bits) throws IOException {
        var w = new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(64, 1024, HEAP);
        for (int v : bits) {
            w.writeFloat(Float.intBitsToFloat(v));
        }
        return bytes(w);
    }

    private static byte[] doubles(long... bits) throws IOException {
        var w = new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(64, 1024, HEAP);
        for (long v : bits) {
            w.writeDouble(Double.longBitsToDouble(v));
        }
        return bytes(w);
    }

    private static byte[] fixed(int length, String... values) throws IOException {
        var w = new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(length, 64, 1024, HEAP);
        for (String v : values) {
            w.writeBytes(Binary.fromConstantByteArray(Bytes.utf8(v).toByteArray()));
        }
        return bytes(w);
    }

    @Test
    void fixedLenByteArraysWithNulls() throws Exception {
        boolean[] present = {true, false, true, true, false, false, true, true, true, false};
        String[] values = {"abc", "xyz", "q\u0001r", "000", "zzz", "yyy"};
        byte[] body = withLevels(present, fixed(3, values));
        for (boolean scratch : new boolean[] {false, true}) {
            ColumnPage page = decode(body, Column.Type.FIXED_LEN_BYTE_ARRAY, 3, present.length, true, scratch ? new PageScratch() : null, new Codecs.State());
            for (int i = 0, j = 0; i < present.length; i++) {
                assertEquals(present[i] ? new Value.FixedLenByteArray(Bytes.utf8(values[j++])) : Value.Null.INSTANCE, page.value(i), "value " + i);
            }
        }
    }

    @Test
    void floatDoubleAndInt32WithNulls() throws Exception {
        boolean[] present = {false, true, true, false, true, false, false, true, true};
        int[] bits = {Float.floatToRawIntBits(1.5f), 0x7fc01234, -1, Integer.MIN_VALUE, 0x01020304};
        long[] wide = {Double.doubleToRawLongBits(-2.25), 0x7ff8000000000abcL, Long.MIN_VALUE, 1L, 0x0102030405060708L};
        for (boolean scratch : new boolean[] {false, true}) {
            PageScratch s = scratch ? new PageScratch() : null;
            Codecs.State codecs = new Codecs.State();
            assertInts(decode(withLevels(present, floats(bits)), Column.Type.FLOAT, 0, present.length, true, s, codecs), present, bits);
            assertInts(decode(withLevels(present, ints(bits)), Column.Type.INT32, 0, present.length, true, s, codecs), present, bits);
            assertLongs(decode(withLevels(present, doubles(wide)), Column.Type.DOUBLE, 0, present.length, true, s, codecs), present, wide);
        }
        // Every value null: no value bytes at all.
        boolean[] none = new boolean[5];
        assertInts(decode(withLevels(none, new byte[0]), Column.Type.INT32, 0, 5, true, new PageScratch(), new Codecs.State()), none, new int[0]);
    }

    /** Pages decoded one after another through the same scratch and decoder state show no trace of each other. */
    @Test
    void consecutivePagesOfDifferentSizesShareScratchSafely() throws Exception {
        int[][] sizes = {{9, 2}, {2, 9}, {1, 6, 3}};
        for (int[] run : sizes) {
            PageScratch scratch = new PageScratch();
            Codecs.State codecs = new Codecs.State();
            for (int n : run) {
                boolean[] present = new boolean[n];
                int[] ivals = new int[n];
                long[] lvals = new long[n];
                String[] fvals = new String[n];
                int count = 0;
                for (int i = 0; i < n; i++) {
                    present[i] = i % 3 != 1;
                    if (present[i]) {
                        ivals[count] = 0x10203040 * (n + 1) + i * 7919;
                        lvals[count] = 0x0102030405060708L * (n + 1) + i;
                        fvals[count] = "v" + (char) ('a' + n) + (char) ('a' + i);
                        count++;
                    }
                }
                assertInts(decode(withLevels(present, ints(Arrays.copyOf(ivals, count))), Column.Type.INT32, 0, n, true, scratch, codecs), present, ivals);
                assertLongs(decode(withLevels(present, doubles(Arrays.copyOf(lvals, count))), Column.Type.DOUBLE, 0, n, true, scratch, codecs), present, lvals);
                ColumnPage page = decode(withLevels(present, fixed(3, Arrays.copyOf(fvals, count))), Column.Type.FIXED_LEN_BYTE_ARRAY, 3, n, true, scratch, codecs);
                for (int i = 0, j = 0; i < n; i++) {
                    assertEquals(present[i] ? new Value.FixedLenByteArray(Bytes.utf8(fvals[j++])) : Value.Null.INSTANCE, page.value(i), "page of " + n + ", value " + i);
                }
            }
        }
    }

    /** A page whose value bytes are not exactly {@code count * width} is invalid, for every type and with nulls. */
    @Test
    void aWrongPayloadSizeIsInvalidForEveryType() throws Exception {
        Object[][] types = {
            {Column.Type.FLOAT, 0, 4}, {Column.Type.INT32, 0, 4}, {Column.Type.DOUBLE, 0, 8}, {Column.Type.INT64, 0, 8},
            {Column.Type.FIXED_LEN_BYTE_ARRAY, 5, 5},
        };
        for (Object[] t : types) {
            Column.Type type = (Column.Type) t[0];
            int length = (int) t[1];
            int width = (int) t[2];
            for (boolean scratch : new boolean[] {false, true}) {
                for (int extra : new int[] {-width, -1, 1, width}) {
                    byte[] body = new byte[3 * width + extra];
                    assertThrows(DataException.class, () -> decode(body, type, length, 3, scratch), type + " " + extra);
                    boolean[] present = {true, false, true, true};
                    assertThrows(DataException.class, () -> decode(withLevels(present, new byte[3 * width + extra]), type, length, 4, true,
                            scratch ? new PageScratch() : null, new Codecs.State()), type + " with nulls " + extra);
                }
                // A null counts for no bytes: four values, one null, want three values' bytes, not four.
                boolean[] present = {true, false, true, true};
                assertThrows(DataException.class, () -> decode(withLevels(present, new byte[4 * width]), type, length, 4, true,
                        scratch ? new PageScratch() : null, new Codecs.State()), type + " counting a null");
            }
        }
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
