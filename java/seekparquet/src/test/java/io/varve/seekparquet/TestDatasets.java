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
 * Writes small SeekParquet datasets for tests: PLAIN, data page v1, uncompressed, with a page index
 * on every column. Each part has the columns {@code key} (BYTE_ARRAY, REQUIRED, STRING), {@code size}
 * (INT64, OPTIONAL: null when a row's size is negative) and, when asked, {@code note} (BYTE_ARRAY,
 * OPTIONAL). Faults can be injected for refusal tests.
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
        int rowsPerGroup = 4;
        boolean noteColumn;
        boolean unique = true;
        String keyColumn = "key";
        /** Whether the key column is annotated STRING (keys must then be UTF-8). */
        boolean stringKey = true;
        boolean omitKeyColumnIndex;
        /** When set, the first two keys of the first page of the first part are swapped. */
        boolean swapFirstKeys;
        String schemaId = "test/v1";
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
                for (int r0 = 0; r0 < group.size(); r0 += spec.rowsPerPage) {
                    List<TestRow> page = group.subList(r0, Math.min(group.size(), r0 + spec.rowsPerPage));
                    long pageOffset = chunkStart + chunk.bytes.size();
                    byte[] body = pageBody(c, page);
                    PageHeader header = new PageHeader(PageType.DATA_PAGE, body.length, body.length);
                    header.setData_page_header(new DataPageHeader(page.size(), Encoding.PLAIN, Encoding.RLE, Encoding.RLE));
                    ByteArrayOutputStream h = new ByteArrayOutputStream();
                    Util.writePageHeader(header, h);
                    chunk.bytes.write(h.toByteArray());
                    chunk.bytes.write(body);
                    chunk.locations.add(new PageLocation(pageOffset, h.size() + body.length, r0));
                    bounds(c, page, chunk);
                }
                byte[] data = chunk.bytes.toByteArray();
                out.write(data);
                groupBytes += data.length;
                Type type = c == 1 ? Type.INT64 : Type.BYTE_ARRAY;
                String name = c == 0 ? spec.keyColumn : c == 1 ? "size" : "note";
                ColumnMetaData md = new ColumnMetaData(type, List.of(Encoding.PLAIN, Encoding.RLE), List.of(name), CompressionCodec.UNCOMPRESSED,
                        group.size(), data.length, data.length, chunkStart);
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
        size.setType(Type.INT64);
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
        out.write(footer.toByteArray());
        out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footer.size()).array());
        out.write("PAR1".getBytes(StandardCharsets.US_ASCII));
        return out.toByteArray();
    }

    /** The note value of a row: null for every third size, else "n" + size. */
    static byte[] note(TestRow row) {
        return row.size() % 3 == 0 ? null : ("n" + row.size()).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] pageBody(int column, List<TestRow> page) throws IOException {
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
        for (int i = 0; i < page.size(); i++) {
            if (!present[i]) {
                continue;
            }
            if (column == 1) {
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
