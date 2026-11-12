// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import org.apache.parquet.format.BoundaryOrder;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageLocation;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.SortingColumn;
import org.apache.parquet.format.StringType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.TypeDefinedOrder;
import org.apache.parquet.format.Util;

/**
 * Writes small SeekParquet datasets for tests: by default PLAIN, data page v1, uncompressed, with a page index
 * on every column. Each part has the columns {@code key} (BYTE_ARRAY, REQUIRED, STRING), {@code size}
 * (INT64, OPTIONAL: null when a row's size is negative) and, when asked, {@code note} (BYTE_ARRAY,
 * OPTIONAL). Value codecs and dictionary/plain transitions can be selected; faults can be injected for refusal tests.
 */
final class TestDatasets {

    /** One row of a test dataset. */
    record TestRow(byte[] key, long size) {
        static TestRow of(String key, long size) {
            return new TestRow(key.getBytes(StandardCharsets.UTF_8), size);
        }
    }

    /** What to write, and which faults to inject. */
    static final class Spec {
        int rowsPerPage = 2;
        /** Rows per page of the {@code size} and {@code note} columns when positive, else {@code rowsPerPage}. */
        int sizeRowsPerPage;
        int noteRowsPerPage;
        /** Encode the {@code size} column's plain pages BYTE_STREAM_SPLIT. */
        boolean byteStreamSplitSize;
        int keyBoundBytes;
        int rowsPerGroup = 4;
        boolean noteColumn;
        /** Write the {@code size} column as INT96: the size's 8 bytes little-endian, then {@link #int96High}. */
        boolean int96Size;
        /** Encode a constant, non-null size per group through a dictionary. */
        boolean dictionarySize;
        CompressionCodec valueCodec = CompressionCodec.UNCOMPRESSED;
        /** Alternate dictionary/plain chunks and dictionary/plain pages, with every supported codec. */
        boolean mixedValuePages;
        boolean dictionaryNote;
        boolean unique = true;
        /** Broad alternating bounds exercise routing without monotone index metadata. */
        boolean nonMonotoneKeyBounds;
        String keyColumn = "key";
        /** Whether the key column is annotated STRING (keys must then be UTF-8). */
        boolean stringKey = true;
        boolean omitKeyColumnIndex;
        boolean omitValueOffsetIndexes;
        /** When set, the first two keys of the first page of the first part are swapped. */
        boolean swapFirstKeys;
        String schemaId = "test/v1";
        /** When set, a field appended to the footer's FileMetaData (bytes before its closing stop). */
        byte[] footerExtraField;
        /** When set, a field appended to the header of the first key page (bytes before its closing stop). */
        byte[] keyPageHeaderExtraField;
    }

    private TestDatasets() {}

    /** Writes a dataset whose parts hold the given rows (each list in ascending key order). */
    static Path write(Path root, Spec spec, List<List<TestRow>> parts) throws IOException {
        Files.createDirectories(root.resolve("data"));
        StringBuilder manifest = new StringBuilder();
        manifest.append("{\"format\":\"seekparquet\",\"format_version\":0,\"draft_revision\":1,\"schema_id\":\"")
                .append(spec.schemaId).append("\",\"key\":{\"column\":\"").append(spec.keyColumn)
                .append("\",\"unique\":").append(spec.unique).append("},\"parts\":[");
        for (int p = 0; p < parts.size(); p++) {
            List<TestRow> rows = new ArrayList<>(parts.get(p));
            if (spec.swapFirstKeys && p == 0 && rows.size() >= 2) {
                TestRow a = rows.get(0);
                rows.set(0, rows.get(1));
                rows.set(1, a);
            }
            String path = String.format("data/part-%05d.parquet", p);
            byte[] file = part(spec, rows);
            Files.write(root.resolve(path), file);
            if (p > 0) {
                manifest.append(',');
            }
            List<TestRow> sorted = parts.get(p);
            manifest.append("{\"path\":\"").append(path).append("\",\"rows\":").append(rows.size())
                    .append(",\"bytes\":").append(file.length)
                    .append(",\"min_key_base64\":\"").append(Base64.getEncoder().encodeToString(sorted.get(0).key()))
                    .append("\",\"max_key_base64\":\"").append(Base64.getEncoder().encodeToString(sorted.get(sorted.size() - 1).key()))
                    .append("\"}");
        }
        manifest.append("]}");
        Files.writeString(root.resolve("manifest.json"), manifest.toString());
        return root;
    }

    private static final class Chunk {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        long uncompressedBytes;
        final List<PageLocation> locations = new ArrayList<>();
        final List<Boolean> nullPages = new ArrayList<>();
        final List<ByteBuffer> mins = new ArrayList<>();
        final List<ByteBuffer> maxs = new ArrayList<>();
    }

    static byte[] part(Spec spec, List<TestRow> rows) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write("PAR1".getBytes(StandardCharsets.US_ASCII));
        int ncols = spec.noteColumn ? 3 : 2;
        List<RowGroup> groups = new ArrayList<>();
        List<Chunk[]> chunkIndexes = new ArrayList<>();
        for (int g0 = 0; g0 < rows.size(); g0 += spec.rowsPerGroup) {
            List<TestRow> group = rows.subList(g0, Math.min(rows.size(), g0 + spec.rowsPerGroup));
            List<ColumnChunk> chunks = new ArrayList<>();
            Chunk[] built = new Chunk[ncols];
            long groupBytes = 0;
            for (int c = 0; c < ncols; c++) {
                long chunkStart = out.size();
                Chunk chunk = new Chunk();
                boolean dictionary = (c == 1 && spec.dictionarySize || c == 2 && spec.dictionaryNote)
                        && (!spec.mixedValuePages || (g0 / spec.rowsPerGroup) % 2 == 0);
                CompressionCodec codec = c == 0 ? CompressionCodec.UNCOMPRESSED : spec.mixedValuePages
                        ? List.of(CompressionCodec.ZSTD, CompressionCodec.UNCOMPRESSED, CompressionCodec.GZIP,
                                CompressionCodec.SNAPPY, CompressionCodec.LZ4_RAW).get((g0 / spec.rowsPerGroup) % 5) : spec.valueCodec;
                List<Bytes> entries = c == 2 && dictionary
                        ? group.stream().map(TestDatasets::note).filter(java.util.Objects::nonNull).map(Bytes::of).distinct().toList() : List.of();
                if (dictionary) {
                    byte[] body;
                    if (c == 1) {
                        long value = group.getFirst().size();
                        if (value < 0 || group.stream().anyMatch(r -> r.size() != value)) throw new IllegalArgumentException("dictionary fixture needs a constant non-null size per group");
                        body = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
                    } else {
                        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
                        for (Bytes entry : entries) { encoded.write(le32(entry.length())); encoded.write(entry.toByteArray()); }
                        body = encoded.toByteArray();
                    }
                    byte[] compressed = compress(body, codec);
                    PageHeader header = new PageHeader(PageType.DICTIONARY_PAGE, body.length, compressed.length);
                    header.setDictionary_page_header(new org.apache.parquet.format.DictionaryPageHeader(c == 1 ? 1 : entries.size(), Encoding.PLAIN));
                    Util.writePageHeader(header, chunk.bytes);
                    chunk.uncompressedBytes += chunk.bytes.size() + body.length;
                    chunk.bytes.write(compressed);
                }
                long dataStart = chunkStart + chunk.bytes.size();
                int perPage = c == 1 && spec.sizeRowsPerPage > 0 ? spec.sizeRowsPerPage
                        : c == 2 && spec.noteRowsPerPage > 0 ? spec.noteRowsPerPage : spec.rowsPerPage;
                for (int r0 = 0; r0 < group.size(); r0 += perPage) {
                    List<TestRow> page = group.subList(r0, Math.min(group.size(), r0 + perPage));
                    long pageOffset = chunkStart + chunk.bytes.size();
                    boolean dictionaryPage = dictionary && (!spec.mixedValuePages || (r0 / perPage) % 2 == 0);
                    boolean split = c == 1 && spec.byteStreamSplitSize && !dictionaryPage;
                    byte[] body = dictionaryPage ? (c == 1 ? dictionaryBody(page.size()) : dictionaryNotes(page, entries)) : pageBody(c, page, split, spec.int96Size);
                    byte[] compressed = compress(body, codec);
                    PageHeader header = new PageHeader(PageType.DATA_PAGE, body.length, compressed.length);
                    header.setData_page_header(new DataPageHeader(page.size(), dictionaryPage ? Encoding.RLE_DICTIONARY
                            : split ? Encoding.BYTE_STREAM_SPLIT : Encoding.PLAIN, Encoding.RLE, Encoding.RLE));
                    ByteArrayOutputStream h = new ByteArrayOutputStream();
                    Util.writePageHeader(header, h);
                    if (c == 0 && g0 == 0 && r0 == 0 && spec.keyPageHeaderExtraField != null) {
                        h = withField(h.toByteArray(), spec.keyPageHeaderExtraField);
                    }
                    chunk.uncompressedBytes += h.size() + body.length;
                    chunk.bytes.write(h.toByteArray());
                    chunk.bytes.write(compressed);
                    chunk.locations.add(new PageLocation(pageOffset, h.size() + compressed.length, r0));
                    bounds(c, page, chunk);
                    if (c == 0 && spec.keyBoundBytes > 0) {
                        int last = chunk.mins.size() - 1;
                        byte[] lo = chunk.mins.get(last).array();
                        byte[] hi = chunk.maxs.get(last).array();
                        if (lo.length > spec.keyBoundBytes) chunk.mins.set(last, ByteBuffer.wrap(Arrays.copyOf(lo, spec.keyBoundBytes)));
                        if (hi.length > spec.keyBoundBytes) {
                            byte[] shortHi = Arrays.copyOf(hi, spec.keyBoundBytes);
                            int at = shortHi.length - 1;
                            while (at >= 0 && shortHi[at] == (byte) 255) at--;
                            if (at >= 0) {
                                shortHi[at]++;
                                chunk.maxs.set(last, ByteBuffer.wrap(Arrays.copyOf(shortHi, at + 1)));
                            }
                        }
                    }
                    if (c == 0 && spec.nonMonotoneKeyBounds) {
                        chunk.mins.set(chunk.mins.size() - 1, ByteBuffer.wrap(new byte[] {'a'}));
                        if ((g0 / spec.rowsPerGroup + r0 / perPage) % 2 == 0) {
                            chunk.maxs.set(chunk.maxs.size() - 1, ByteBuffer.wrap(new byte[] {'z'}));
                        }
                    }
                }
                byte[] data = chunk.bytes.toByteArray();
                out.write(data);
                groupBytes += chunk.uncompressedBytes;
                Type type = c == 1 ? (spec.int96Size ? Type.INT96 : Type.INT64) : Type.BYTE_ARRAY;
                String name = c == 0 ? spec.keyColumn : c == 1 ? "size" : "note";
                ColumnMetaData md = new ColumnMetaData(type, dictionary ? List.of(Encoding.PLAIN, Encoding.RLE, Encoding.RLE_DICTIONARY) : List.of(Encoding.PLAIN, Encoding.RLE), List.of(name), codec,
                        group.size(), chunk.uncompressedBytes, data.length, dataStart);
                if (dictionary) md.setDictionary_page_offset(chunkStart);
                ColumnChunk cc = new ColumnChunk(chunkStart);
                cc.setMeta_data(md);
                chunks.add(cc);
                built[c] = chunk;
            }
            RowGroup rg = new RowGroup(chunks, groupBytes, group.size());
            rg.setSorting_columns(List.of(new SortingColumn(0, false, false)));
            groups.add(rg);
            chunkIndexes.add(built);
        }
        // Page indexes after the data, column indexes first.
        for (int g = 0; g < groups.size(); g++) {
            for (int c = 0; c < ncols; c++) {
                Chunk chunk = chunkIndexes.get(g)[c];
                if (c == 0 && spec.omitKeyColumnIndex && g == groups.size() - 1) {
                    continue;
                }
                ColumnIndex ci = new ColumnIndex(chunk.nullPages, chunk.mins, chunk.maxs, BoundaryOrder.UNORDERED);
                ByteArrayOutputStream b = new ByteArrayOutputStream();
                Util.writeColumnIndex(ci, b);
                ColumnChunk cc = groups.get(g).getColumns().get(c);
                cc.setColumn_index_offset(out.size());
                cc.setColumn_index_length(b.size());
                out.write(b.toByteArray());
            }
        }
        for (int g = 0; g < groups.size(); g++) {
            for (int c = 0; c < ncols; c++) {
                if (c > 0 && spec.omitValueOffsetIndexes) continue;
                ByteArrayOutputStream b = new ByteArrayOutputStream();
                Util.writeOffsetIndex(new OffsetIndex(chunkIndexes.get(g)[c].locations), b);
                ColumnChunk cc = groups.get(g).getColumns().get(c);
                cc.setOffset_index_offset(out.size());
                cc.setOffset_index_length(b.size());
                out.write(b.toByteArray());
            }
        }
        List<SchemaElement> schema = new ArrayList<>();
        SchemaElement root = new SchemaElement("schema");
        root.setNum_children(ncols);
        schema.add(root);
        SchemaElement key = new SchemaElement(spec.keyColumn);
        key.setType(Type.BYTE_ARRAY);
        key.setRepetition_type(FieldRepetitionType.REQUIRED);
        if (spec.stringKey) {
            key.setLogicalType(LogicalType.STRING(new StringType()));
        }
        schema.add(key);
        SchemaElement size = new SchemaElement("size");
        size.setType(spec.int96Size ? Type.INT96 : Type.INT64);
        size.setRepetition_type(FieldRepetitionType.OPTIONAL);
        schema.add(size);
        if (spec.noteColumn) {
            SchemaElement note = new SchemaElement("note");
            note.setType(Type.BYTE_ARRAY);
            note.setRepetition_type(FieldRepetitionType.OPTIONAL);
            schema.add(note);
        }
        FileMetaData meta = new FileMetaData(1, schema, rows.size(), groups);
        meta.setKey_value_metadata(List.of(
                new KeyValue("seekparquet.format_version").setValue("0"),
                new KeyValue("seekparquet.draft_revision").setValue("1"),
                new KeyValue("seekparquet.schema_id").setValue(spec.schemaId),
                new KeyValue("seekparquet.key_column").setValue(spec.keyColumn),
                new KeyValue("seekparquet.key_unique").setValue(Boolean.toString(spec.unique))));
        List<ColumnOrder> orders = new ArrayList<>();
        for (int c = 0; c < ncols; c++) {
            orders.add(ColumnOrder.TYPE_ORDER(new TypeDefinedOrder()));
        }
        meta.setColumn_orders(orders);
        ByteArrayOutputStream footer = new ByteArrayOutputStream();
        Util.writeFileMetaData(meta, footer);
        if (spec.footerExtraField != null) {
            footer = withField(footer.toByteArray(), spec.footerExtraField);
        }
        out.write(footer.toByteArray());
        out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footer.size()).array());
        out.write("PAR1".getBytes(StandardCharsets.US_ASCII));
        return out.toByteArray();
    }

    /** {@return a compact-protocol struct's bytes with {@code field} inserted before its closing stop} */
    private static ByteArrayOutputStream withField(byte[] struct, byte[] field) {
        if (struct[struct.length - 1] != 0) {
            throw new IllegalArgumentException("not a struct ending in a stop byte");
        }
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(struct, 0, struct.length - 1);
        b.write(field, 0, field.length);
        b.write(0);
        return b;
    }

    static byte[] compress(byte[] body, CompressionCodec codec) throws IOException {
        if (codec == CompressionCodec.UNCOMPRESSED) return body;
        if (codec == CompressionCodec.GZIP) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (var gzip = new java.util.zip.GZIPOutputStream(out)) { gzip.write(body); }
            return out.toByteArray();
        }
        io.airlift.compress.Compressor compressor = switch (codec) {
            case ZSTD -> new io.airlift.compress.zstd.ZstdCompressor();
            case SNAPPY -> new io.airlift.compress.snappy.SnappyCompressor();
            case LZ4_RAW -> new io.airlift.compress.lz4.Lz4Compressor();
            default -> throw new IllegalArgumentException("test codec " + codec);
        };
        byte[] out = new byte[compressor.maxCompressedLength(body.length)];
        return Arrays.copyOf(out, compressor.compress(body, 0, body.length, out, 0, out.length));
    }

    /** The note value of a row: null for every third size, else "n" + size. */
    static byte[] note(TestRow row) {
        return row.size() % 3 == 0 ? null : ("n" + row.size()).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] dictionaryNotes(List<TestRow> page, List<Bytes> entries) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        boolean[] present = new boolean[page.size()];
        for (int i = 0; i < page.size(); i++) present[i] = note(page.get(i)) != null;
        byte[] levels = rleLevels(present);
        out.write(le32(levels.length)); out.write(levels);
        int width = Math.max(1, 32 - Integer.numberOfLeadingZeros(Math.max(0, entries.size() - 1)));
        out.write(width);
        for (TestRow row : page) {
            byte[] note = note(row);
            if (note == null) continue;
            int id = entries.indexOf(Bytes.of(note));
            out.write(2);
            for (int i = 0; i < (width + 7) / 8; i++) out.write(id >>> (8 * i));
        }
        return out.toByteArray();
    }

    private static byte[] dictionaryBody(int rows) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        boolean[] present = new boolean[rows]; Arrays.fill(present, true);
        byte[] levels = rleLevels(present);
        out.write(le32(levels.length)); out.write(levels);
        out.write(1); // Dictionary-id bit width; every id is zero.
        for (int i = 0; i < rows; i++) { out.write(2); out.write(0); }
        return out.toByteArray();
    }

    /** The third word of the INT96 {@code size} of {@code size}: its high bit set, to exercise unsigned words. */
    static int int96High(long size) {
        return 0x80000000 | (int) size;
    }

    private static byte[] pageBody(int column, List<TestRow> page, boolean split, boolean int96) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        if (column == 0) {
            for (TestRow r : page) {
                b.write(le32(r.key().length));
                b.write(r.key());
            }
            return b.toByteArray();
        }
        boolean[] present = new boolean[page.size()];
        for (int i = 0; i < page.size(); i++) {
            present[i] = column == 1 ? page.get(i).size() >= 0 : note(page.get(i)) != null;
        }
        byte[] levels = rleLevels(present);
        b.write(le32(levels.length));
        b.write(levels);
        if (split) {
            long[] sizes = page.stream().mapToLong(TestRow::size).filter(v -> v >= 0).toArray();
            for (int k = 0; k < 8; k++) {
                for (long v : sizes) {
                    b.write((int) (v >>> (8 * k)));
                }
            }
            return b.toByteArray();
        }
        for (int i = 0; i < page.size(); i++) {
            if (!present[i]) {
                continue;
            }
            if (column == 1 && int96) {
                long v = page.get(i).size();
                b.write(ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(v).putInt(int96High(v)).array());
            } else if (column == 1) {
                b.write(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(page.get(i).size()).array());
            } else {
                byte[] v = note(page.get(i));
                b.write(le32(v.length));
                b.write(v);
            }
        }
        return b.toByteArray();
    }

    /** One RLE run per run of equal levels, bit width 1. */
    private static byte[] rleLevels(boolean[] present) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        int i = 0;
        while (i < present.length) {
            int j = i;
            while (j < present.length && present[j] == present[i]) {
                j++;
            }
            int header = (j - i) << 1;
            while ((header & ~0x7F) != 0) {
                b.write((header & 0x7F) | 0x80);
                header >>>= 7;
            }
            b.write(header);
            b.write(present[i] ? 1 : 0);
            i = j;
        }
        return b.toByteArray();
    }

    private static void bounds(int column, List<TestRow> page, Chunk chunk) {
        if (column == 0) {
            byte[] lo = page.get(0).key();
            byte[] hi = page.get(0).key();
            for (TestRow r : page) {
                if (Arrays.compareUnsigned(r.key(), lo) < 0) {
                    lo = r.key();
                }
                if (Arrays.compareUnsigned(r.key(), hi) > 0) {
                    hi = r.key();
                }
            }
            chunk.nullPages.add(false);
            chunk.mins.add(ByteBuffer.wrap(lo));
            chunk.maxs.add(ByteBuffer.wrap(hi));
            return;
        }
        // Value columns: an all-null page is a null page with empty bounds.
        boolean any = false;
        for (TestRow r : page) {
            any |= column == 1 ? r.size() >= 0 : note(r) != null;
        }
        chunk.nullPages.add(!any);
        chunk.mins.add(ByteBuffer.allocate(0));
        chunk.maxs.add(ByteBuffer.allocate(0));
    }

    private static byte[] le32(int v) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array();
    }
}
