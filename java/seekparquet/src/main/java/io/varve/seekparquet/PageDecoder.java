// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.io.api.Binary;

/**
 * Decodes the data and dictionary pages of top-level, non-repeated columns: data page versions 1 and
 * 2, and the baseline encodings (ENC-1). Decompression and the structure of every encoded stream are
 * checked here, against the sizes the page declares, before parquet-java's value decoders read them.
 */
final class PageDecoder {

    private PageDecoder() {}

    /** {@return the number of rows of a data page header: v1 values or v2 rows} */
    static long rowsOf(PageHeader header) throws DataException {
        if (header.getType() == PageType.DATA_PAGE) {
            DataPageHeader h = header.getData_page_header();
            if (h == null) {
                throw new DataException("a DATA_PAGE without its header");
            }
            return h.getNum_values();
        }
        if (header.getType() == PageType.DATA_PAGE_V2) {
            DataPageHeaderV2 h = header.getData_page_header_v2();
            if (h == null) {
                throw new DataException("a DATA_PAGE_V2 without its header");
            }
            return h.getNum_rows();
        }
        throw new DataException("a page of type " + header.getType() + " where a data page belongs");
    }

    /**
     * Requires the page body to match the CRC-32 its header carries, when it carries one, as the Go reader
     * does: over the page's bytes after its header, as stored (compressed, levels included). A header
     * without a CRC is not checked.
     */
    private static void checkCrc(PageHeader header, ByteBuffer body) throws DataException {
        if (!header.isSetCrc()) {
            return;
        }
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(body.duplicate());
        if ((int) crc.getValue() != header.getCrc()) {
            throw new DataException("the page fails its checksum");
        }
    }

    private static int uncompressedSize(PageHeader header, Limits limits, String where) throws DataException {
        int size = header.getUncompressed_page_size();
        if (size < 0) {
            throw new DataException("a negative uncompressed page size");
        }
        if (size > limits.maxPageBytes()) {
            throw new LimitExceededException("maxPageBytes", size, limits.maxPageBytes(), where);
        }
        return size;
    }

    /**
     * Decodes a dictionary page.
     *
     * @param body the page body: exactly its compressed size
     */
    static Dictionary decodeDictionary(PageHeader header, ByteBuffer body, ColumnSpec col, CompressionCodec codec, Limits limits, String where)
            throws DataException {
        DictionaryPageHeader h = header.getDictionary_page_header();
        if (header.getType() != PageType.DICTIONARY_PAGE || h == null) {
            throw new DataException("the column chunk's dictionary page is missing");
        }
        checkCrc(header, body);
        int size = uncompressedSize(header, limits, where);
        int count = h.getNum_values();
        int minWidth = switch (col.type()) {
            case BOOLEAN -> 1;
            case INT32, FLOAT, BYTE_ARRAY -> 4;
            case INT64, DOUBLE -> 8;
            case INT96 -> 12;
            case FIXED_LEN_BYTE_ARRAY -> Math.max(1, col.typeLength());
            case GROUP -> throw new IllegalStateException();
        };
        if (count < 0 || (long) count * minWidth > size) {
            throw new DataException("a dictionary of " + count + " values in " + size + " bytes");
        }
        if (count > limits.maxPageValues()) {
            throw new LimitExceededException("maxPageValues", count, limits.maxPageValues(), where);
        }
        if (h.getEncoding() == null) {
            throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_ENCODING, where + ": an unknown dictionary encoding");
        }
        switch (h.getEncoding()) {
            case PLAIN, PLAIN_DICTIONARY -> {}
            default -> throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_ENCODING,
                    where + ": dictionary encoding " + h.getEncoding());
        }
        byte[] data = Codecs.decompress(codec, body, size);
        // The dictionary holds exactly its declared values: bytes after them would be values the
        // decoder never reads (CHECK-5).
        if (col.type() == Column.Type.BYTE_ARRAY) {
            EncodingChecks.plainByteArrays(data, 0, data.length, count);
        } else if (data.length != (col.type() == Column.Type.BOOLEAN ? (count + 7L) / 8 : (long) count * minWidth)) {
            throw new DataException("a dictionary of " + count + " values in " + data.length + " bytes");
        }
        try {
            DictionaryPage page = new DictionaryPage(BytesInput.from(data), count, org.apache.parquet.column.Encoding.PLAIN);
            return org.apache.parquet.column.Encoding.PLAIN.initDictionary(col.descriptor(), page);
        } catch (IOException | RuntimeException e) {
            throw new DataException("the dictionary page does not decode", e);
        }
    }

    /**
     * Decodes a data page.
     *
     * @param body the page body: exactly its compressed size
     * @param dictionary the column chunk's dictionary, or {@code null} when it has none
     */
    static ColumnPage decodeData(
            PageHeader header, ByteBuffer body, ColumnSpec col, CompressionCodec codec, DictionarySupplier dictionary, Limits limits, String where)
            throws DataException {
        checkCrc(header, body);
        int size = uncompressedSize(header, limits, where);
        int n;
        byte[] data;
        int valuesFrom;
        boolean[] nulls = null;
        Encoding encoding;
        if (header.getType() == PageType.DATA_PAGE) {
            DataPageHeader h = header.getData_page_header();
            if (h == null) {
                throw new DataException("a DATA_PAGE without its header");
            }
            n = count(h.getNum_values(), limits, where);
            encoding = encoding(h.getEncoding(), where);
            data = Codecs.decompress(codec, body, size);
            valuesFrom = 0;
            if (col.optional()) {
                Encoding levels = encoding(h.getDefinition_level_encoding(), where);
                if (levels == Encoding.RLE) {
                    if (data.length < 4) {
                        throw new DataException("a truncated definition level length");
                    }
                    int len = ByteBuffer.wrap(data, 0, 4).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
                    if (len < 0 || len > data.length - 4) {
                        throw new DataException("definition levels of " + len + " bytes in a page of " + data.length);
                    }
                    nulls = rleLevels(data, 4, 4 + len, n);
                    valuesFrom = 4 + len;
                } else if (levels == Encoding.BIT_PACKED) {
                    int len = (n + 7) / 8;
                    if (len > data.length) {
                        throw new DataException("truncated bit-packed definition levels");
                    }
                    nulls = new boolean[n];
                    for (int i = 0; i < n; i++) {
                        nulls[i] = ((data[i >>> 3] >>> (7 - (i & 7))) & 1) == 0;
                    }
                    valuesFrom = len;
                } else {
                    throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_ENCODING, where + ": definition level encoding " + levels);
                }
            }
        } else if (header.getType() == PageType.DATA_PAGE_V2) {
            DataPageHeaderV2 h = header.getData_page_header_v2();
            if (h == null) {
                throw new DataException("a DATA_PAGE_V2 without its header");
            }
            n = count(h.getNum_values(), limits, where);
            if (h.getNum_rows() != n) {
                throw new DataException("a page of a non-repeated column with " + n + " values in " + h.getNum_rows() + " rows");
            }
            encoding = encoding(h.getEncoding(), where);
            int rl = h.getRepetition_levels_byte_length();
            int dl = h.getDefinition_levels_byte_length();
            if (rl != 0 || dl < 0 || (!col.optional() && dl != 0) || (long) rl + dl > body.remaining() || (long) rl + dl > size) {
                throw new DataException("level lengths outside the page");
            }
            byte[] levels = new byte[rl + dl];
            body.duplicate().get(levels);
            if (col.optional()) {
                nulls = rleLevels(levels, rl, rl + dl, n);
            }
            ByteBuffer values = body.duplicate();
            values.position(values.position() + rl + dl);
            int valuesSize = size - rl - dl;
            boolean compressed = !h.isSetIs_compressed() || h.isIs_compressed();
            if (compressed) {
                data = Codecs.decompress(codec, values, valuesSize);
            } else {
                data = Codecs.decompress(CompressionCodec.UNCOMPRESSED, values, valuesSize);
            }
            valuesFrom = 0;
        } else {
            throw new DataException("a page of type " + header.getType() + " where a data page belongs");
        }
        int nonNull = n;
        if (nulls != null) {
            for (boolean isNull : nulls) {
                if (isNull) {
                    nonNull--;
                }
            }
        }
        if (header.getType() == PageType.DATA_PAGE_V2 && header.getData_page_header_v2().getNum_nulls() != n - nonNull) {
            throw new DataException("the v2 null count disagrees with its definition levels");
        }
        return values(col, encoding, data, valuesFrom, n, nonNull, nulls, dictionary, limits, where);
    }

    /** Supplies a column chunk's dictionary on first need. */
    @FunctionalInterface
    interface DictionarySupplier {
        Dictionary get() throws DataException;
    }

    private static int count(int n, Limits limits, String where) throws DataException {
        if (n < 0) {
            throw new DataException("a negative value count");
        }
        if (n > limits.maxPageValues()) {
            throw new LimitExceededException("maxPageValues", n, limits.maxPageValues(), where);
        }
        return n;
    }

    private static Encoding encoding(Encoding e, String where) {
        if (e == null) {
            throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_ENCODING, where + ": an unknown encoding");
        }
        return e;
    }

    /** Decodes {@code n} one-bit definition levels from an RLE/bit-packed hybrid stream; true marks a null. */
    private static boolean[] rleLevels(byte[] b, int from, int to, int n) throws DataException {
        byte[] runs = EncodingChecks.rleHybrid(b, from, to, 1, n);
        if (runs != null) { // the levels without their empty runs, which parquet-java does not skip
            b = runs;
            from = 0;
            to = runs.length;
        }
        boolean[] nulls = new boolean[n];
        try {
            RunLengthBitPackingHybridDecoder d =
                    new RunLengthBitPackingHybridDecoder(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(b, from, to - from)));
            for (int i = 0; i < n; i++) {
                int level = d.readInt();
                if (level > 1) {
                    throw new DataException("a definition level of " + level + " in a column whose maximum is 1");
                }
                nulls[i] = level == 0;
            }
        } catch (IOException | RuntimeException e) {
            throw new DataException("the definition levels do not decode", e);
        }
        return nulls;
    }

    private static ColumnPage values(
            ColumnSpec col, Encoding encoding, byte[] data, int from, int n, int nonNull, boolean[] nulls, DictionarySupplier dictionary,
            Limits limits, String where) throws DataException {
        Column.Type type = col.type();
        boolean[] bools = type == Column.Type.BOOLEAN ? new boolean[n] : null;
        int[] ints = type == Column.Type.INT32 || type == Column.Type.FLOAT ? new int[n] : null;
        long[] longs = type == Column.Type.INT64 || type == Column.Type.DOUBLE ? new long[n] : null;
        byte[][] binaries = type == Column.Type.BYTE_ARRAY || type == Column.Type.FIXED_LEN_BYTE_ARRAY ? new byte[n][] : null;
        if (type == Column.Type.INT96 || type == Column.Type.GROUP) {
            throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_PROJECTION, where + ": " + type + " values");
        }
        int to = data.length;
        try {
            if (encoding == Encoding.PLAIN && (type == Column.Type.FLOAT || type == Column.Type.DOUBLE
                    || type == Column.Type.INT32 || type == Column.Type.INT64)) {
                // Fixed-width PLAIN values are read as raw little-endian bits, so that floating-point
                // values keep their exact bits. The page holds exactly its values: bytes after them would be
                // values no one reads (CHECK-5).
                int width = ints != null ? 4 : 8;
                if ((long) nonNull * width != to - from) {
                    throw new DataException(nonNull + " PLAIN values are not " + (to - from) + " bytes");
                }
                ByteBuffer buf = ByteBuffer.wrap(data, from, to - from).order(java.nio.ByteOrder.LITTLE_ENDIAN);
                for (int i = 0; i < n; i++) {
                    if (nulls != null && nulls[i]) {
                        continue;
                    }
                    if (ints != null) {
                        ints[i] = buf.getInt();
                    } else {
                        longs[i] = buf.getLong();
                    }
                }
                return new ColumnPage(type, n, nulls, bools, ints, longs, binaries);
            }
            if (encoding == Encoding.DELTA_BYTE_ARRAY) {
                if (binaries == null) {
                    throw new DataException("DELTA_BYTE_ARRAY for a " + type + " column");
                }
                deltaByteArray(data, from, to, n, nonNull, nulls, binaries, col, limits, where);
                return new ColumnPage(type, n, nulls, bools, ints, longs, binaries);
            }
            if ((encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY) && from < to
                    && dictionary != null && dictionary.get() != null) {
                // The indexes without their empty runs, which parquet-java does not skip: the bit width,
                // then the runs. The dictionary is read first, so that its failures keep their kind.
                byte[] runs = EncodingChecks.rleHybrid(data, from + 1, to, data[from] & 0xFF, nonNull);
                if (runs != null) {
                    byte[] indexes = new byte[1 + runs.length];
                    indexes[0] = data[from];
                    System.arraycopy(runs, 0, indexes, 1, runs.length);
                    data = indexes;
                    from = 0;
                    to = indexes.length;
                }
            }
            ValuesReader reader = reader(col, encoding, data, from, to, nonNull, dictionary, limits, where);
            reader.initFromPage(nonNull, ByteBufferInputStream.wrap(ByteBuffer.wrap(data, from, to - from)));
            if (binaries != null && (encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY)) {
                // Rows that share a dictionary entry share one array, so a page of many repeated
                // values costs a reference per row, not a copy.
                Dictionary dict = dictionary.get();
                byte[][] entries = new byte[dict.getMaxId() + 1][];
                for (int i = 0; i < n; i++) {
                    if (nulls != null && nulls[i]) {
                        continue;
                    }
                    int id = reader.readValueDictionaryId();
                    if (id < 0 || id >= entries.length) {
                        throw new DataException("a dictionary index " + id + " outside a dictionary of " + entries.length);
                    }
                    if (entries[id] == null) {
                        entries[id] = dict.decodeToBinary(id).getBytes();
                        if (type == Column.Type.FIXED_LEN_BYTE_ARRAY && entries[id].length != col.typeLength()) {
                            throw new DataException("a FIXED_LEN_BYTE_ARRAY value of " + entries[id].length + " bytes, not " + col.typeLength());
                        }
                    }
                    binaries[i] = entries[id];
                }
                return new ColumnPage(type, n, nulls, bools, ints, longs, binaries);
            }
            for (int i = 0; i < n; i++) {
                if (nulls != null && nulls[i]) {
                    continue;
                }
                switch (type) {
                    case BOOLEAN -> bools[i] = reader.readBoolean();
                    case INT32 -> ints[i] = reader.readInteger();
                    case INT64 -> longs[i] = reader.readLong();
                    case FLOAT -> ints[i] = Float.floatToRawIntBits(reader.readFloat());
                    case DOUBLE -> longs[i] = Double.doubleToRawLongBits(reader.readDouble());
                    case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
                        Binary b = reader.readBytes();
                        byte[] v = b.getBytes();
                        if (type == Column.Type.FIXED_LEN_BYTE_ARRAY && v.length != col.typeLength()) {
                            throw new DataException("a FIXED_LEN_BYTE_ARRAY value of " + v.length + " bytes, not " + col.typeLength());
                        }
                        binaries[i] = v;
                    }
                    default -> throw new IllegalStateException();
                }
            }
        } catch (DataException | SeekParquetException | java.util.concurrent.CancellationException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new DataException("the page values do not decode (" + encoding + ")", e);
        }
        return new ColumnPage(type, n, nulls, bools, ints, longs, binaries);
    }

    /** {@return parquet-java's reader for the values, after checking the encoded stream's structure} */
    private static ValuesReader reader(
            ColumnSpec col, Encoding encoding, byte[] data, int from, int to, int nonNull, DictionarySupplier dictionary, Limits limits, String where) throws DataException {
        Column.Type type = col.type();
        switch (encoding) {
            case PLAIN -> {
                if (type == Column.Type.BYTE_ARRAY) {
                    EncodingChecks.plainByteArrays(data, from, to, nonNull);
                } else if (type == Column.Type.FIXED_LEN_BYTE_ARRAY && (long) nonNull * col.typeLength() != to - from) {
                    throw new DataException(nonNull + " FIXED_LEN_BYTE_ARRAY values are not " + (to - from) + " bytes");
                } else if (type == Column.Type.BOOLEAN && (nonNull + 7L) / 8 != to - from) {
                    // Bit-packed: only the final byte may hold padding bits.
                    throw new DataException(nonNull + " PLAIN BOOLEAN values are not " + (to - from) + " bytes");
                }
                return org.apache.parquet.column.Encoding.PLAIN.getValuesReader(col.descriptor(), ValuesType.VALUES);
            }
            case PLAIN_DICTIONARY, RLE_DICTIONARY -> {
                Dictionary dict = dictionary == null ? null : dictionary.get();
                if (dict == null) {
                    throw new DataException("a dictionary-encoded page in a column chunk without a dictionary page");
                }
                if (from >= to) {
                    if (nonNull == 0) {
                        return org.apache.parquet.column.Encoding.PLAIN.getValuesReader(col.descriptor(), ValuesType.VALUES);
                    }
                    throw new DataException("dictionary indices are missing");
                }
                EncodingChecks.rleHybrid(data, from + 1, to, data[from] & 0xFF, nonNull);
                return org.apache.parquet.column.Encoding.RLE_DICTIONARY.getDictionaryBasedValuesReader(col.descriptor(), ValuesType.VALUES, dict);
            }
            case RLE -> {
                if (type != Column.Type.BOOLEAN) {
                    throw new DataException("RLE values in a " + type + " column");
                }
                if (to - from < 4) {
                    throw new DataException("a truncated RLE length");
                }
                int len = ByteBuffer.wrap(data, from, 4).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
                if (len < 0 || len > to - from - 4) {
                    throw new DataException("RLE values of " + len + " bytes outside the page");
                }
                // Go's Boolean decoder refuses an empty run, and parquet-java's would repeat its value.
                EncodingChecks.rleHybrid(data, from + 4, from + 4 + len, 1, nonNull, false);
                return org.apache.parquet.column.Encoding.RLE.getValuesReader(col.descriptor(), ValuesType.VALUES);
            }
            case DELTA_BINARY_PACKED -> {
                if (type != Column.Type.INT32 && type != Column.Type.INT64) {
                    throw new DataException("DELTA_BINARY_PACKED values in a " + type + " column");
                }
                EncodingChecks.deltaBinaryPacked(data, from, to, nonNull, limits, where);
                return new DeltaBinaryPackingValuesReader();
            }
            case DELTA_LENGTH_BYTE_ARRAY -> {
                if (type != Column.Type.BYTE_ARRAY) {
                    throw new DataException("DELTA_LENGTH_BYTE_ARRAY values in a " + type + " column");
                }
                EncodingChecks.deltaBinaryPacked(data, from, to, nonNull, limits, where);
                return new DeltaLengthByteArrayValuesReader();
            }
            default -> throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_ENCODING, "encoding " + encoding + " for values");
        }
    }

    /**
     * Decodes DELTA_BYTE_ARRAY values: prefix lengths, then suffixes as DELTA_LENGTH_BYTE_ARRAY.
     * The lengths are decoded and checked before any value is built, so that the values' total size
     * is bounded by the page limit.
     */
    private static void deltaByteArray(
            byte[] data, int from, int to, int n, int nonNull, boolean[] nulls, byte[][] out, ColumnSpec col, Limits limits, String where)
            throws DataException, IOException {
        int suffixFrom = EncodingChecks.deltaBinaryPacked(data, from, to, nonNull, limits, where);
        EncodingChecks.deltaBinaryPacked(data, suffixFrom, to, nonNull, limits, where);
        ByteBufferInputStream in = ByteBufferInputStream.wrap(ByteBuffer.wrap(data, from, to - from));
        DeltaBinaryPackingValuesReader prefixes = new DeltaBinaryPackingValuesReader();
        prefixes.initFromPage(nonNull, in);
        DeltaLengthByteArrayValuesReader suffixes = new DeltaLengthByteArrayValuesReader();
        suffixes.initFromPage(nonNull, in);
        byte[] previous = new byte[0];
        long total = 0;
        for (int i = 0; i < n; i++) {
            if (nulls != null && nulls[i]) {
                continue;
            }
            int prefix = prefixes.readInteger();
            if (prefix < 0 || prefix > previous.length) {
                throw new DataException("a DELTA_BYTE_ARRAY prefix of " + prefix + " bytes after a value of " + previous.length);
            }
            Binary suffix = suffixes.readBytes();
            long decodedLength = (long) prefix + suffix.length();
            total += decodedLength;
            if (total > limits.maxPageBytes()) {
                throw new LimitExceededException("maxPageBytes", total, limits.maxPageBytes(), where + " (decoded DELTA_BYTE_ARRAY values)");
            }
            int length = (int) decodedLength; // bounded by maxPageBytes before narrowing
            byte[] v = new byte[length];
            System.arraycopy(previous, 0, v, 0, prefix);
            suffix.toByteBuffer().get(v, prefix, suffix.length());
            if (col.type() == Column.Type.FIXED_LEN_BYTE_ARRAY && length != col.typeLength()) {
                throw new DataException("a FIXED_LEN_BYTE_ARRAY value of " + length + " bytes, not " + col.typeLength());
            }
            out[i] = v;
            previous = v;
        }
    }
}
