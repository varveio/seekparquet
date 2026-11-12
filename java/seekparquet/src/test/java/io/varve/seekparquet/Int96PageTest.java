// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.junit.jupiter.api.Test;

/**
 * INT96 pages decode to raw bits through every path a value column takes: PLAIN in data page v1 and v2,
 * dictionary encoding, required and optional columns with nulls, and a request's reused scratch.
 */
class Int96PageTest {

    private static final ColumnSpec REQUIRED = ColumnSpec.of("t", 0, Column.Type.INT96, 0, false);
    private static final ColumnSpec OPTIONAL = ColumnSpec.of("t", 0, Column.Type.INT96, 0, true);

    private static Value.Int96 value(int k) {
        return new Value.Int96(k, ~k, 0x80000000 | k);
    }

    private static byte[] plain(List<Value.Int96> values) {
        ByteBuffer b = ByteBuffer.allocate(12 * values.size()).order(ByteOrder.LITTLE_ENDIAN);
        for (Value.Int96 v : values) b.putInt(v.word0()).putInt(v.word1()).putInt(v.word2());
        return b.array();
    }

    /** RLE/bit-packed hybrid definition levels of bit width 1: one bit-packed run. */
    private static byte[] levels(boolean[] present) {
        int groups = (present.length + 7) / 8;
        byte[] out = new byte[1 + groups];
        out[0] = (byte) (groups << 1 | 1);
        for (int i = 0; i < present.length; i++) {
            if (present[i]) out[1 + i / 8] |= (byte) (1 << (i % 8));
        }
        return out;
    }

    private static List<Value.Int96> nonNull(boolean[] present, int base) {
        List<Value.Int96> out = new ArrayList<>();
        for (int i = 0; i < present.length; i++) if (present[i]) out.add(value(base + i));
        return out;
    }

    private static void check(ColumnPage page, boolean[] present, int base) {
        for (int i = 0; i < present.length; i++) {
            assertEquals(present[i] ? value(base + i) : Value.Null.INSTANCE, page.value(i), "row " + i);
        }
    }

    private static ColumnPage v1(ColumnSpec col, boolean[] present, int base, PageScratch scratch) throws DataException {
        byte[] values = plain(nonNull(present, base));
        byte[] body;
        if (col.optional()) {
            byte[] lv = levels(present);
            body = ByteBuffer.allocate(4 + lv.length + values.length).order(ByteOrder.LITTLE_ENDIAN).putInt(lv.length).put(lv).put(values).array();
        } else {
            body = values;
        }
        PageHeader h = new PageHeader(PageType.DATA_PAGE, body.length, body.length)
                .setData_page_header(new DataPageHeader(present.length, Encoding.PLAIN, Encoding.RLE, Encoding.RLE));
        return PageDecoder.decodeData(h, ByteBuffer.wrap(body), col, CompressionCodec.UNCOMPRESSED, null, Limits.DEFAULT, "test", scratch,
                new Codecs.State());
    }

    @Test
    void plainV1RequiredAndOptional() throws DataException {
        boolean[] all = {true, true, true, true, true};
        check(v1(REQUIRED, all, 0, null), all, 0);
        boolean[] some = {true, false, true, false, false, true, true, false, true};
        check(v1(OPTIONAL, some, 100, null), some, 100);
        // A PLAIN page whose bytes are not 12 per value is malformed.
        byte[] short11 = new byte[11];
        PageHeader h = new PageHeader(PageType.DATA_PAGE, 11, 11).setData_page_header(new DataPageHeader(1, Encoding.PLAIN, Encoding.RLE, Encoding.RLE));
        assertThrows(DataException.class, () -> PageDecoder.decodeData(h, ByteBuffer.wrap(short11), REQUIRED, CompressionCodec.UNCOMPRESSED,
                null, Limits.DEFAULT, "test", null, new Codecs.State()));
    }

    @Test
    void plainV2WithNulls() throws DataException {
        boolean[] present = {false, true, true, false, true, false, false, true};
        byte[] lv = levels(present);
        byte[] values = plain(nonNull(present, 7));
        byte[] body = ByteBuffer.allocate(lv.length + values.length).put(lv).put(values).array();
        int nulls = 0;
        for (boolean p : present) if (!p) nulls++;
        DataPageHeaderV2 v2 = new DataPageHeaderV2(present.length, nulls, present.length, Encoding.PLAIN, lv.length, 0).setIs_compressed(false);
        PageHeader h = new PageHeader(PageType.DATA_PAGE_V2, body.length, body.length).setData_page_header_v2(v2);
        check(PageDecoder.decodeData(h, ByteBuffer.wrap(body), OPTIONAL, CompressionCodec.UNCOMPRESSED, null, Limits.DEFAULT, "test",
                new PageScratch(), new Codecs.State()), present, 7);
    }

    private static Dictionary dictionary(List<Value.Int96> entries, PageScratch scratch) throws DataException {
        byte[] body = plain(entries);
        PageHeader h = new PageHeader(PageType.DICTIONARY_PAGE, body.length, body.length)
                .setDictionary_page_header(new DictionaryPageHeader(entries.size(), Encoding.PLAIN));
        return PageDecoder.decodeDictionary(h, ByteBuffer.wrap(body), OPTIONAL, CompressionCodec.UNCOMPRESSED, Limits.DEFAULT, "test", scratch,
                new Codecs.State());
    }

    /** Dictionary indexes in RLE runs of bit width 2, behind definition levels. */
    @Test
    void dictionaryEncodedWithNullsAndReusedScratch() throws DataException {
        List<Value.Int96> entries = List.of(value(1), value(2), value(3));
        PageScratch scratch = new PageScratch();
        Dictionary dict = dictionary(entries, scratch);
        boolean[] present = {true, true, false, true, true, true, false};
        int[] ids = {0, 0, 2, 1, 1};
        ByteArrayOutputStream runs = new ByteArrayOutputStream();
        runs.write(2); // bit width
        runs.write(2 << 1); runs.write(0);
        runs.write(1 << 1); runs.write(2);
        runs.write(2 << 1); runs.write(1);
        byte[] lv = levels(present);
        byte[] idx = runs.toByteArray();
        byte[] body = ByteBuffer.allocate(4 + lv.length + idx.length).order(ByteOrder.LITTLE_ENDIAN).putInt(lv.length).put(lv).put(idx).array();
        PageHeader h = new PageHeader(PageType.DATA_PAGE, body.length, body.length)
                .setData_page_header(new DataPageHeader(present.length, Encoding.RLE_DICTIONARY, Encoding.RLE, Encoding.RLE));
        ColumnPage page = PageDecoder.decodeData(h, ByteBuffer.wrap(body), OPTIONAL, CompressionCodec.UNCOMPRESSED, () -> dict, Limits.DEFAULT,
                "test", scratch, new Codecs.State());
        List<Value> owned = new ArrayList<>();
        for (int i = 0, j = 0; i < present.length; i++) {
            owned.add(page.value(i));
            assertEquals(present[i] ? entries.get(ids[j++]) : Value.Null.INSTANCE, page.value(i), "row " + i);
        }
        // The same scratch decodes the next page, a PLAIN one: the values taken from the first stay as they were.
        boolean[] next = {true, false, true};
        check(v1(OPTIONAL, next, 50, scratch), next, 50);
        for (int i = 0, j = 0; i < present.length; i++) {
            assertEquals(present[i] ? entries.get(ids[j++]) : Value.Null.INSTANCE, owned.get(i), "kept row " + i);
        }
        // A DELTA_BYTE_ARRAY page of an INT96 column is malformed.
        PageHeader delta = new PageHeader(PageType.DATA_PAGE, 4, 4)
                .setData_page_header(new DataPageHeader(1, Encoding.DELTA_BYTE_ARRAY, Encoding.RLE, Encoding.RLE));
        assertThrows(DataException.class, () -> PageDecoder.decodeData(delta, ByteBuffer.wrap(new byte[4]), REQUIRED, CompressionCodec.UNCOMPRESSED,
                null, Limits.DEFAULT, "test", null, new Codecs.State()));
    }
}
