// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

/**
 * Encoded-stream policies shared with the Go reader, from differential testing: where decoders differ on
 * malformed bytes, both readers refuse them (CHECK-5, invalid_parquet).
 */
class EncodingPolicyTest {

    private static ColumnPage decode(byte[] body, Encoding encoding, Column.Type type, int count, PageDecoder.DictionarySupplier dictionary)
            throws DataException {
        PageHeader header = new PageHeader(PageType.DATA_PAGE, body.length, body.length)
                .setData_page_header(new DataPageHeader(count, encoding, Encoding.RLE, Encoding.RLE));
        return PageDecoder.decodeData(header, ByteBuffer.wrap(body), ColumnSpec.of("value", 0, type, 0, false),
                CompressionCodec.UNCOMPRESSED, dictionary, Limits.DEFAULT, "test page", null, new Codecs.State());
    }

    private static ColumnPage decode(byte[] body, Encoding encoding, Column.Type type, int count) throws DataException {
        return decode(body, encoding, type, count, null);
    }

    private static byte[] bytes(int... b) {
        byte[] out = new byte[b.length];
        for (int i = 0; i < b.length; i++) {
            out[i] = (byte) b[i];
        }
        return out;
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            b.writeBytes(p);
        }
        return b.toByteArray();
    }

    private static byte[] le32(int v) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array();
    }

    /** Unsigned LEB128. */
    private static byte[] uleb(long v) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        while ((v & ~0x7FL) != 0) {
            b.write((int) (v & 0x7F) | 0x80);
            v >>>= 7;
        }
        b.write((int) v);
        return b.toByteArray();
    }

    private static byte[] zigzag(long v) {
        return uleb((v << 1) ^ (v >> 63));
    }

    /** A DELTA_BINARY_PACKED header: 128-value blocks of 4 miniblocks. */
    private static byte[] deltaHeader(long total, long first) {
        return concat(uleb(128), uleb(4), uleb(total), zigzag(first));
    }

    // Item 4: an RLE run's value must be below 2^width.

    /** Decompresses a GZIP page of {@code size} declared bytes into a fresh buffer of that size. */
    private static byte[] gunzipPage(ByteBuffer page, int size) throws DataException {
        return Codecs.decompress(CompressionCodec.GZIP, page, size, new byte[size], new Codecs.State());
    }

    @Test
    void aBooleanRunValueAboveOneBitIsRefused() throws DataException {
        // Three one-value RLE runs: 2, 0, 2 (parquet-java read true, false, true; parquet-go false, false, false).
        byte[] runs = bytes(0x02, 0x02, 0x02, 0x00, 0x02, 0x02);
        DataException e = assertThrows(DataException.class, () -> decode(concat(le32(runs.length), runs), Encoding.RLE, Column.Type.BOOLEAN, 3));
        assertTrue(e.getMessage().contains("RLE run value of 2"), e.getMessage());
        byte[] valid = bytes(0x02, 0x01, 0x02, 0x00, 0x02, 0x01);
        assertArrayEquals(new boolean[] {true, false, true},
                decode(concat(le32(valid.length), valid), Encoding.RLE, Column.Type.BOOLEAN, 3).bools);
    }

    @Test
    void aDictionaryIndexRunValueAboveItsWidthIsRefused() throws DataException {
        PageHeader dictHeader = new PageHeader(PageType.DICTIONARY_PAGE, 0, 0)
                .setDictionary_page_header(new DictionaryPageHeader(3, Encoding.PLAIN));
        byte[] entries = concat(le32(1), "a".getBytes(StandardCharsets.UTF_8), le32(1), "b".getBytes(StandardCharsets.UTF_8),
                le32(1), "c".getBytes(StandardCharsets.UTF_8));
        dictHeader.setUncompressed_page_size(entries.length).setCompressed_page_size(entries.length);
        PlainValuesDictionary dict = (PlainValuesDictionary) PageDecoder.decodeDictionary(dictHeader, ByteBuffer.wrap(entries),
                ColumnSpec.of("value", 0, Column.Type.BYTE_ARRAY, 0, false), CompressionCodec.UNCOMPRESSED, Limits.DEFAULT, "test", null, new Codecs.State());
        // Bit width 1, one RLE run of three indexes of value 2: index 2 exists, but not at width 1.
        assertThrows(DataException.class, () -> decode(bytes(1, 0x06, 0x02), Encoding.RLE_DICTIONARY, Column.Type.BYTE_ARRAY, 3, () -> dict));
        ColumnPage page = decode(bytes(2, 0x06, 0x02), Encoding.RLE_DICTIONARY, Column.Type.BYTE_ARRAY, 3, () -> dict);
        assertArrayEquals("c".getBytes(StandardCharsets.UTF_8), page.binaries[2]);
    }

    @Test
    void runValuesAreCheckedAtEveryWidth() {
        // One RLE run of one value: header 0x02, then the value's (width + 7) / 8 bytes.
        assertThrows(DataException.class, () -> EncodingChecks.rleHybrid(bytes(0x02, 8), 0, 2, 3, 1));
        assertDoesNotThrow(() -> EncodingChecks.rleHybrid(bytes(0x02, 7), 0, 2, 3, 1));
        assertThrows(DataException.class, () -> EncodingChecks.rleHybrid(bytes(0x02, 0x00, 0x02), 0, 3, 9, 1));
        assertDoesNotThrow(() -> EncodingChecks.rleHybrid(bytes(0x02, 0xFF, 0x01), 0, 3, 9, 1));
        assertDoesNotThrow(() -> EncodingChecks.rleHybrid(bytes(0x02, 0xFF, 0xFF, 0xFF, 0xFF), 0, 5, 32, 1));
        // An empty run's value is checked too, though it holds nothing.
        assertThrows(DataException.class, () -> EncodingChecks.rleHybrid(bytes(0x00, 2, 0x02, 1), 0, 4, 1, 1));
    }

    // Item 11: a GZIP page is exactly one gzip member.

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        try (GZIPOutputStream z = new GZIPOutputStream(b)) {
            z.write(data);
        }
        return b.toByteArray();
    }

    @Test
    void aGzipPageWithBytesAfterItsMemberIsRefused() throws Exception {
        byte[] data = concat(le32(1), le32(2), le32(3));
        byte[] member = gzip(data);
        assertArrayEquals(data, gunzipPage(ByteBuffer.wrap(member), data.length));
        assertThrows(DataException.class, () -> gunzipPage(ByteBuffer.wrap(concat(member, bytes(0))), data.length));
        assertThrows(DataException.class, () -> gunzipPage(ByteBuffer.wrap(concat(member, bytes(0x1F, 0x8B))), data.length));
        // Two members, as a gzip concatenation: refused, as the Go reader refuses it.
        byte[] two = concat(gzip(java.util.Arrays.copyOf(data, 4)), gzip(java.util.Arrays.copyOfRange(data, 4, 12)));
        assertThrows(DataException.class, () -> gunzipPage(ByteBuffer.wrap(two), data.length));
        // A corrupt trailer still fails its check.
        byte[] badCrc = member.clone();
        badCrc[badCrc.length - 8] ^= 1;
        assertThrows(DataException.class, () -> gunzipPage(ByteBuffer.wrap(badCrc), data.length));
        assertThrows(DataException.class, () -> gunzipPage(ByteBuffer.wrap(member, 0, member.length - 1), data.length));
    }

    @Test
    void aGzipMemberWithOptionalHeaderFieldsDecompresses() throws Exception {
        byte[] data = "a page of values".getBytes(StandardCharsets.UTF_8);
        // Header with FEXTRA, FNAME, FCOMMENT and FHCRC set.
        ByteArrayOutputStream h = new ByteArrayOutputStream();
        h.writeBytes(bytes(0x1F, 0x8B, 8, 4 | 8 | 16 | 2, 0, 0, 0, 0, 0, 255));
        h.writeBytes(bytes(3, 0, 'x', 'y', 'z'));
        h.writeBytes("name\0comment\0".getBytes(StandardCharsets.US_ASCII));
        CRC32 hcrc = new CRC32();
        hcrc.update(h.toByteArray());
        h.writeBytes(bytes((int) hcrc.getValue() & 0xFF, (int) (hcrc.getValue() >>> 8) & 0xFF));
        Deflater d = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
        d.setInput(data);
        d.finish();
        byte[] deflated = new byte[256];
        int n = d.deflate(deflated);
        d.end();
        CRC32 crc = new CRC32();
        crc.update(data);
        byte[] member = concat(h.toByteArray(), java.util.Arrays.copyOf(deflated, n), le32((int) crc.getValue()), le32(data.length));
        assertArrayEquals(data, gunzipPage(ByteBuffer.wrap(member), data.length));
    }

    // Item 19: byte-array delta lengths cover exactly the bytes after them.

    private static byte[] deltaLength(String... values) throws IOException {
        DeltaLengthByteArrayValuesWriter w = new DeltaLengthByteArrayValuesWriter(64, 1 << 16, HeapByteBufferAllocator.getInstance());
        for (String v : values) {
            w.writeBytes(Binary.fromString(v));
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        w.getBytes().writeAllTo(out);
        return out.toByteArray();
    }

    private static byte[] deltaByteArray(String... values) throws IOException {
        DeltaByteArrayWriter w = new DeltaByteArrayWriter(64, 1 << 16, HeapByteBufferAllocator.getInstance());
        for (String v : values) {
            w.writeBytes(Binary.fromString(v));
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        w.getBytes().writeAllTo(out);
        return out.toByteArray();
    }

    @Test
    void deltaLengthByteArrayValuesEndAtThePageEnd() throws Exception {
        byte[] exact = deltaLength("a", "bb", "ccc");
        assertArrayEquals("ccc".getBytes(StandardCharsets.UTF_8),
                decode(exact, Encoding.DELTA_LENGTH_BYTE_ARRAY, Column.Type.BYTE_ARRAY, 3).binaries[2]);
        byte[] surplus = concat(exact, "XYZ".getBytes(StandardCharsets.UTF_8));
        DataException e = assertThrows(DataException.class, () -> decode(surplus, Encoding.DELTA_LENGTH_BYTE_ARRAY, Column.Type.BYTE_ARRAY, 3));
        assertTrue(e.getMessage().contains("summing to 6 bytes where 9 follow"), e.getMessage());
        byte[] short1 = java.util.Arrays.copyOf(exact, exact.length - 1);
        assertThrows(DataException.class, () -> decode(short1, Encoding.DELTA_LENGTH_BYTE_ARRAY, Column.Type.BYTE_ARRAY, 3));
    }

    @Test
    void deltaByteArraySuffixesEndAtThePageEnd() throws Exception {
        byte[] exact = deltaByteArray("value-1", "value-2", "value-30");
        assertArrayEquals("value-30".getBytes(StandardCharsets.UTF_8),
                decode(exact, Encoding.DELTA_BYTE_ARRAY, Column.Type.BYTE_ARRAY, 3).binaries[2]);
        byte[] surplus = concat(exact, "XYZ".getBytes(StandardCharsets.UTF_8));
        assertThrows(DataException.class, () -> decode(surplus, Encoding.DELTA_BYTE_ARRAY, Column.Type.BYTE_ARRAY, 3));
    }

    // Item 21: an INT32 stream's first value is in INT32 range.

    @Test
    void anInt32DeltaFirstValueOutsideInt32IsRefused() throws DataException {
        byte[] min64 = deltaHeader(1, Long.MIN_VALUE);
        DataException e = assertThrows(DataException.class, () -> decode(min64, Encoding.DELTA_BINARY_PACKED, Column.Type.INT32, 1));
        assertTrue(e.getMessage().contains("outside INT32"), e.getMessage());
        assertThrows(DataException.class, () -> decode(deltaHeader(1, 1L << 31), Encoding.DELTA_BINARY_PACKED, Column.Type.INT32, 1));
        assertArrayEquals(new int[] {Integer.MAX_VALUE},
                decode(deltaHeader(1, Integer.MAX_VALUE), Encoding.DELTA_BINARY_PACKED, Column.Type.INT32, 1).ints);
        assertArrayEquals(new int[] {Integer.MIN_VALUE},
                decode(deltaHeader(1, Integer.MIN_VALUE), Encoding.DELTA_BINARY_PACKED, Column.Type.INT32, 1).ints);
        // An INT64 stream takes any 64-bit first value.
        assertArrayEquals(new long[] {Long.MIN_VALUE}, decode(min64, Encoding.DELTA_BINARY_PACKED, Column.Type.INT64, 1).longs);
    }

    @Test
    void aPrefixOrLengthFirstValueOutsideInt32IsRefused() {
        // DELTA_BYTE_ARRAY of one value "k": prefix lengths [-2^63] (parquet-java narrowed it to 0), suffix lengths [1].
        byte[] prefix = concat(deltaHeader(1, Long.MIN_VALUE), deltaHeader(1, 1), bytes('k'));
        assertThrows(DataException.class, () -> decode(prefix, Encoding.DELTA_BYTE_ARRAY, Column.Type.BYTE_ARRAY, 1));
        byte[] suffix = concat(deltaHeader(1, 0), deltaHeader(1, 1L + (1L << 32)), bytes('k'));
        assertThrows(DataException.class, () -> decode(suffix, Encoding.DELTA_BYTE_ARRAY, Column.Type.BYTE_ARRAY, 1));
        byte[] lengths = concat(deltaHeader(1, 1L + (1L << 32)), bytes('k'));
        assertThrows(DataException.class, () -> decode(lengths, Encoding.DELTA_LENGTH_BYTE_ARRAY, Column.Type.BYTE_ARRAY, 1));
        assertDoesNotThrow(() -> decode(concat(deltaHeader(1, 0), deltaHeader(1, 1), bytes('k')), Encoding.DELTA_BYTE_ARRAY, Column.Type.BYTE_ARRAY, 1));
    }

    @Test
    void anInt32MinDeltaOutsideInt32IsNotChecked() throws DataException {
        // As parquet-go: values 5 then 5 + (2^32 + 1), which wraps to 6 in 32 bits (a writer may compute deltas in 64).
        byte[] stream = concat(deltaHeader(2, 5), zigzag(1L + (1L << 32)), bytes(0, 0, 0, 0));
        assertArrayEquals(new int[] {5, 6}, decode(stream, Encoding.DELTA_BINARY_PACKED, Column.Type.INT32, 2).ints);
    }

    // Item 22: an INT32 stream's miniblock bit widths are at most 32.

    /** Two values 0, 0: first value 0, then one block of min delta 0 whose first miniblock has {@code width} bits per value. */
    private static byte[] zeros(int width) {
        return concat(deltaHeader(2, 0), zigzag(0), bytes(width, 0, 0, 0), new byte[32 * width / 8]);
    }

    @Test
    void anInt32DeltaBitWidthAbove32IsRefusedBeforeDecoding() throws DataException {
        DataException e = assertThrows(DataException.class, () -> decode(zeros(33), Encoding.DELTA_BINARY_PACKED, Column.Type.INT32, 2));
        // Refused by the structural walk, before parquet-java's decoder allocates or reads anything.
        assertTrue(e.getMessage().contains("bit width of 33 in an INT32 stream"), e.getMessage());
        assertArrayEquals(new int[] {0, 0}, decode(zeros(32), Encoding.DELTA_BINARY_PACKED, Column.Type.INT32, 2).ints);
        // INT64 streams keep widths up to 64.
        assertArrayEquals(new long[] {0, 0}, decode(zeros(33), Encoding.DELTA_BINARY_PACKED, Column.Type.INT64, 2).longs);
        assertThrows(DataException.class, () -> decode(zeros(65), Encoding.DELTA_BINARY_PACKED, Column.Type.INT64, 2));
    }

    @Test
    void aLengthStreamBitWidthAbove32IsRefusedBeforeDecoding() {
        // Lengths 0, 0 at width 33, and no value bytes.
        DataException e = assertThrows(DataException.class, () -> decode(zeros(33), Encoding.DELTA_LENGTH_BYTE_ARRAY, Column.Type.BYTE_ARRAY, 2));
        assertTrue(e.getMessage().contains("bit width of 33 in an INT32 stream"), e.getMessage());
        assertDoesNotThrow(() -> decode(zeros(32), Encoding.DELTA_LENGTH_BYTE_ARRAY, Column.Type.BYTE_ARRAY, 2));
    }
}
