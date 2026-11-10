// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.junit.jupiter.api.Test;

/**
 * Pages whose payloads hold more values than their headers declare are refused (CHECK-5), as Go refuses
 * them: a decoder reads only the declared values, so the rest would pass unnoticed. The exact payloads
 * decode, and so do the padding bits of a PLAIN Boolean page's final byte.
 */
class SurplusValuesTest {

    private static ColumnPage decode(byte[] body, Column.Type type, int typeLength, boolean optional, int count)
            throws DataException {
        PageHeader header = new PageHeader(PageType.DATA_PAGE, body.length, body.length)
                .setData_page_header(new DataPageHeader(count, Encoding.PLAIN, Encoding.RLE, Encoding.RLE));
        return PageDecoder.decodeData(header, ByteBuffer.wrap(body), ColumnSpec.of("value", 0, type, typeLength, optional),
                CompressionCodec.UNCOMPRESSED, null, Limits.DEFAULT, "test page", null, new Codecs.State());
    }

    private static void dictionary(byte[] body, Column.Type type, int typeLength, int count) throws DataException {
        PageHeader header = new PageHeader(PageType.DICTIONARY_PAGE, body.length, body.length)
                .setDictionary_page_header(new DictionaryPageHeader(count, Encoding.PLAIN));
        PageDecoder.decodeDictionary(header, ByteBuffer.wrap(body), ColumnSpec.of("value", 0, type, typeLength, false),
                CompressionCodec.UNCOMPRESSED, Limits.DEFAULT, "test dictionary", null, new Codecs.State());
    }

    private static byte[] byteArrays(String... values) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (String v : values) {
            out.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v.length()).array());
            out.writeBytes(v.getBytes());
        }
        return out.toByteArray();
    }

    private record Case(String name, Column.Type type, int typeLength, byte[] exact, byte[] excess) {}

    @Test
    void surplusPlainValuesAreRefused() throws DataException {
        List<Case> cases = List.of(
                new Case("int32", Column.Type.INT32, 0, new byte[12], new byte[16]),
                new Case("int64", Column.Type.INT64, 0, new byte[24], new byte[32]),
                new Case("float", Column.Type.FLOAT, 0, new byte[12], new byte[16]),
                new Case("double", Column.Type.DOUBLE, 0, new byte[24], new byte[32]),
                new Case("fixed", Column.Type.FIXED_LEN_BYTE_ARRAY, 5, new byte[15], new byte[20]),
                new Case("byte array", Column.Type.BYTE_ARRAY, 0, byteArrays("a", "bb", ""), byteArrays("a", "bb", "", "ccc")),
                // Three Booleans fill one byte; a second byte holds eight more.
                new Case("boolean", Column.Type.BOOLEAN, 0, new byte[] {0b101}, new byte[] {0b101, 0}));
        for (Case c : cases) {
            assertEquals(3, decode(c.exact(), c.type(), c.typeLength(), false, 3).rows, c.name());
            assertThrows(DataException.class, () -> decode(c.excess(), c.type(), c.typeLength(), false, 3), c.name());
            if (c.type() != Column.Type.BOOLEAN) {
                dictionary(c.exact(), c.type(), c.typeLength(), 3);
                assertThrows(DataException.class, () -> dictionary(c.excess(), c.type(), c.typeLength(), 3), c.name() + " dictionary");
            }
        }
        // Every count of PLAIN Booleans from 1 to 16 fills ceil(n/8) bytes, the final one padded; one more
        // byte is refused.
        for (int n = 1; n <= 16; n++) {
            int size = (n + 7) / 8;
            byte[] full = new byte[size];
            java.util.Arrays.fill(full, (byte) 0xff);
            ColumnPage page = decode(full, Column.Type.BOOLEAN, 0, false, n);
            for (int i = 0; i < n; i++) {
                assertTrue(page.bools[i]);
            }
            int count = n;
            assertThrows(DataException.class, () -> decode(new byte[size + 1], Column.Type.BOOLEAN, 0, false, count), n + " Booleans");
        }
    }

    @Test
    void surplusValuesOfAnOptionalPageAreRefused() throws DataException {
        // Definition levels: one RLE run of three values, 1, 0, 1 bit-packed; then two or three INT64s.
        byte[] levels = {3, 0b101};
        for (int values : new int[] {2, 3}) {
            ByteBuffer body = ByteBuffer.allocate(4 + levels.length + 8 * values).order(ByteOrder.LITTLE_ENDIAN);
            body.putInt(levels.length).put(levels);
            for (int i = 0; i < values; i++) {
                body.putLong(i + 1);
            }
            if (values == 2) {
                ColumnPage page = decode(body.array(), Column.Type.INT64, 0, true, 3);
                assertArrayEquals(new boolean[] {false, true, false}, page.nulls);
                assertEquals(1, page.longs[0]);
                assertEquals(2, page.longs[2]);
            } else {
                assertThrows(DataException.class, () -> decode(body.array(), Column.Type.INT64, 0, true, 3));
            }
        }
    }
}
