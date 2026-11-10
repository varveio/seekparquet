// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import io.varve.seekparquet.storage.DatasetStorage;
import io.varve.seekparquet.storage.PartSource;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.SortingColumn;

/**
 * One opened part: its source, its footer checked when a request first touches the part (CHECK-1),
 * and the key page index of each row group, loaded and checked on first use (CHECK-2). Thread-safe: the footer is never modified
 * after open, and indexes are published through atomic references.
 */
final class PartFile implements AutoCloseable {

    /** How much of a part's tail the first read fetches, to get the footer in one read. */
    static final int TAIL_WINDOW = 64 * 1024;

    private static final String STAMP_VERSION = "seekparquet.format_version";
    private static final String STAMP_DRAFT = "seekparquet.draft_revision";
    private static final String STAMP_KEY_COLUMN = "seekparquet.key_column";
    private static final String STAMP_KEY_UNIQUE = "seekparquet.key_unique";
    private static final String STAMP_SCHEMA = "seekparquet.schema_id";

    final int ordinal;
    final ManifestPart entry;
    final String name;
    private final PartSource source;
    private final IndexMemory indexMemory;
    final long size;
    private final FileMetaData meta;
    final PartSchema schema;
    final int keyLeaf;
    final boolean utf8Key;
    final boolean unique;
    final ColumnSpec keySpec;
    /** The physical indexes of the row groups that hold rows, in order. */
    final int[] groups;
    /** The row count of each physical row group. */
    final long[] groupRows;
    /** The part row of each physical row group's first row. */
    final long[] groupStart;
    /** The projectable columns (COL-3) by name, the key included. */
    final Map<String, ColumnSpec> projectable;
    private final AtomicReferenceArray<KeyIndex> keyIndexes;
    private final AtomicReferenceArray<byte[]> firstKeys;
    private final ConcurrentHashMap<Long, Optional<ValueIndex>> valueIndexes = new ConcurrentHashMap<>();

    private PartFile(int ordinal, ManifestPart entry, PartSource source, FileMetaData meta, PartSchema schema, int keyLeaf, boolean utf8Key,
            int[] groups, long[] groupRows, Map<String, ColumnSpec> projectable, boolean unique, IndexMemory indexMemory) {
        this.indexMemory = indexMemory;
        this.ordinal = ordinal;
        this.entry = entry;
        this.name = entry.path();
        this.source = source;
        this.size = source.size();
        this.meta = meta;
        this.schema = schema;
        this.keyLeaf = keyLeaf;
        this.utf8Key = utf8Key;
        this.keySpec = projectable.values().stream().filter(c -> c.leaf() == keyLeaf).findFirst().orElseThrow();
        this.unique = unique;
        this.groups = groups;
        this.groupRows = groupRows;
        this.groupStart = new long[groupRows.length];
        for (int g = 1; g < groupRows.length; g++) {
            groupStart[g] = groupStart[g - 1] + groupRows[g - 1];
        }
        this.projectable = projectable;
        this.keyIndexes = new AtomicReferenceArray<>(groupRows.length);
        this.firstKeys = new AtomicReferenceArray<>(groupRows.length);
    }

    private RefusalException refuse(RefusalCode code, String detail) {
        return refuse(code, name, detail);
    }

    private static RefusalException refuse(RefusalCode code, String part, String detail) {
        return new RefusalException(code, part, detail);
    }

    // ------------------------------------------------------------------
    // Checking on first touch (CHECK-1)

    /** Opens a part and applies the CHECK-1 checks that concern it alone. Closes the source on failure. */
    static PartFile open(DatasetStorage storage, Manifest manifest, int ordinal, ReadContext ctx, IndexMemory indexMemory) {
        ManifestPart entry = manifest.parts().get(ordinal);
        ctx.checkCancelled();
        PartSource source;
        try {
            source = storage.openPart(entry.path(), entry.bytes(), ctx.cancellation);
        } catch (NoSuchFileException e) {
            throw refuse(RefusalCode.MISSING_PART, entry.path(), "the part does not exist");
        } catch (InterruptedIOException e) {
            if (e instanceof java.net.SocketTimeoutException) {
                throw new StorageException("read or open part " + entry.path(), e);
            }
            throw cancelled(e);
        } catch (IOException e) {
            throw new StorageException("open part " + entry.path(), e);
        }
        ctx.stats.partsOpened++;
        try {
            return open(source, manifest, ordinal, entry, ctx, indexMemory);
        } catch (RuntimeException | Error e) {
            try {
                source.close();
            } catch (IOException | RuntimeException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private static PartFile open(PartSource source, Manifest manifest, int ordinal, ManifestPart entry, ReadContext ctx, IndexMemory indexMemory) {
        String name = entry.path();
        long size = source.size();
        if (size != entry.bytes()) {
            throw refuse(RefusalCode.PART_MISMATCH, name, "the part holds " + size + " bytes, the manifest says " + entry.bytes());
        }
        FileMetaData meta = readFooter(source, name, size, ctx);

        // FILE-2 and FILE-1, before following any reference.
        if (meta.isSetEncryption_algorithm() || meta.isSetFooter_signing_key_metadata()) {
            throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_ENCRYPTION, "part " + name + " uses Parquet modular encryption");
        }
        List<RowGroup> rowGroups = meta.getRow_groups();
        if (rowGroups == null) {
            throw refuse(RefusalCode.INVALID_FOOTER, name, "the footer has no row group list");
        }
        for (int g = 0; g < rowGroups.size(); g++) {
            List<ColumnChunk> chunks = rowGroups.get(g).getColumns();
            if (chunks == null) {
                throw refuse(RefusalCode.INVALID_FOOTER, name, "row group " + g + " has no column list");
            }
            for (ColumnChunk cc : chunks) {
                if (cc.isSetFile_path()) {
                    throw refuse(RefusalCode.INVALID_FOOTER, name, "row group " + g + " has a column chunk in another file (file_path "
                            + cc.getFile_path() + ")");
                }
                if (cc.isSetCrypto_metadata() || cc.isSetEncrypted_column_metadata()) {
                    throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_ENCRYPTION, "part " + name + " has an encrypted column");
                }
            }
        }

        PartSchema schema;
        try {
            schema = PartSchema.parse(meta.getSchema());
        } catch (DataException e) {
            throw refuse(RefusalCode.INVALID_FOOTER, name, e.getMessage());
        }
        int leaves = schema.leaves.size();
        long[] groupRows = new long[rowGroups.size()];
        long total = 0;
        int nonEmpty = 0;
        for (int g = 0; g < rowGroups.size(); g++) {
            RowGroup rg = rowGroups.get(g);
            if (rg.getColumns().size() != leaves) {
                throw refuse(RefusalCode.INVALID_FOOTER, name, "row group " + g + " has " + rg.getColumns().size()
                        + " column chunks for " + leaves + " leaf columns");
            }
            for (int c = 0; c < leaves; c++) {
                checkChunk(rg.getColumns().get(c), schema.leaves.get(c), schema.leafPaths.get(c), size, name, g);
            }
            long rows = rg.getNum_rows();
            if (rows < 0 || total + rows < total) {
                throw refuse(RefusalCode.INVALID_FOOTER, name, "row group " + g + " declares " + rows + " rows");
            }
            total += rows;
            groupRows[g] = rows;
            if (rows > 0) {
                nonEmpty++;
            }
        }
        if (total != meta.getNum_rows()) {
            throw refuse(RefusalCode.INVALID_FOOTER, name, "the row groups hold " + total + " rows, the footer says " + meta.getNum_rows());
        }
        if (total != entry.rows()) {
            throw refuse(RefusalCode.PART_MISMATCH, name, "the part holds " + total + " rows, the manifest says " + entry.rows());
        }

        checkStamps(meta, manifest, name);

        // KEY-1, KEY-5, KEY-6.
        PartSchema.Field key = schema.byName.get(manifest.key().column());
        if (key == null || schema.duplicates.contains(manifest.key().column())) {
            throw refuse(RefusalCode.INVALID_KEY_COLUMN, name, "there is no single top-level column named " + manifest.key().column());
        }
        if (key.leaf() < 0) {
            throw refuse(RefusalCode.INVALID_KEY_COLUMN, name, "the key column is nested");
        }
        if (key.type() != Column.Type.BYTE_ARRAY) {
            throw refuse(RefusalCode.INVALID_KEY_COLUMN, name, "the key column is " + key.type() + ", not BYTE_ARRAY");
        }
        if (key.repetition() != Column.Repetition.REQUIRED) {
            throw refuse(RefusalCode.INVALID_KEY_COLUMN, name, "the key column is " + key.repetition() + ", not REQUIRED");
        }
        boolean utf8Key = false;
        SchemaElement se = key.element();
        if (se.isSetLogicalType()) {
            LogicalType lt = se.getLogicalType();
            if (!lt.isSetSTRING()) {
                throw refuse(RefusalCode.INVALID_KEY_COLUMN, name, "the key column is annotated " + lt);
            }
            utf8Key = true;
        }
        if (se.isSetConverted_type()) {
            if (se.getConverted_type() != ConvertedType.UTF8) {
                throw refuse(RefusalCode.INVALID_KEY_COLUMN, name, "the key column has converted type " + se.getConverted_type());
            }
            utf8Key = true;
        }
        List<ColumnOrder> orders = meta.getColumn_orders();
        if (orders == null || orders.size() != leaves) {
            throw refuse(RefusalCode.INVALID_KEY_COLUMN, name, "column_orders does not have one entry per leaf column");
        }
        if (!orders.get(key.leaf()).isSetTYPE_ORDER()) {
            throw refuse(RefusalCode.INVALID_KEY_COLUMN, name, "column_orders does not give the key column TYPE_ORDER");
        }

        // RG-1 for every non-empty row group.
        int[] groups = new int[nonEmpty];
        int k = 0;
        for (int g = 0; g < rowGroups.size(); g++) {
            if (groupRows[g] == 0) {
                continue;
            }
            List<SortingColumn> sorting = rowGroups.get(g).getSorting_columns();
            if (sorting == null || sorting.isEmpty()) {
                throw refuse(RefusalCode.MISSING_SORTING_COLUMNS, name, "row group " + g + " declares no sorting columns");
            }
            SortingColumn first = sorting.get(0);
            if (first.getColumn_idx() != key.leaf() || first.isDescending()) {
                throw refuse(RefusalCode.MISSING_SORTING_COLUMNS, name, "row group " + g + " is not sorted by key ascending first");
            }
            groups[k++] = g;
        }

        Map<String, ColumnSpec> projectable = new LinkedHashMap<>();
        for (PartSchema.Field f : schema.fields) {
            if (f.leaf() < 0 || f.repetition() == Column.Repetition.REPEATED || f.type() == Column.Type.INT96
                    || schema.duplicates.contains(f.name())) {
                continue;
            }
            projectable.put(f.name(), ColumnSpec.of(f.name(), f.leaf(), f.type(), f.typeLength(), f.repetition() == Column.Repetition.OPTIONAL));
        }
        return new PartFile(ordinal, entry, source, meta, schema, key.leaf(), utf8Key, groups, groupRows, Map.copyOf(projectable), manifest.key().unique(), indexMemory);
    }

    private static FileMetaData readFooter(PartSource source, String name, long size, ReadContext ctx) {
        if (size < 12) {
            throw refuse(RefusalCode.INVALID_FOOTER, name, "the part is too short to be Parquet");
        }
        int tailLength = (int) Math.min(size, TAIL_WINDOW);
        ByteBuffer tail = read(source, name, size - tailLength, tailLength, ctx);
        int end = tail.position() + tailLength;
        byte[] magic = new byte[4];
        tail.duplicate().position(end - 4).get(magic);
        String m = new String(magic, StandardCharsets.ISO_8859_1);
        if (m.equals("PARE")) {
            throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_ENCRYPTION, "part " + name + " has an encrypted footer");
        }
        if (!m.equals("PAR1")) {
            throw refuse(RefusalCode.INVALID_FOOTER, name, "the part does not end with PAR1");
        }
        long footerLength = Integer.toUnsignedLong(tail.duplicate().order(ByteOrder.LITTLE_ENDIAN).getInt(end - 8));
        if (footerLength > size - 12) {
            throw refuse(RefusalCode.INVALID_FOOTER, name, "the footer length " + footerLength + " does not fit in the part");
        }
        if (footerLength > ctx.limits.maxFooterBytes()) {
            throw new LimitExceededException("maxFooterBytes", footerLength, ctx.limits.maxFooterBytes(), "part " + name);
        }
        byte[] footer = new byte[(int) footerLength];
        if (footerLength + 8 <= tailLength) {
            tail.duplicate().position(end - 8 - (int) footerLength).get(footer);
        } else {
            read(source, name, size - 8 - footerLength, (int) footerLength, ctx).get(footer);
        }
        try {
            return Thrift.fileMetaData(footer);
        } catch (DataException e) {
            throw refuse(RefusalCode.INVALID_FOOTER, name, "the footer does not parse: " + e.getMessage());
        }
    }

    /** Checks a column chunk's metadata against its schema leaf, and its byte range against the file. */
    private static void checkChunk(ColumnChunk cc, SchemaElement leaf, List<String> path, long size, String name, int g) {
        ColumnMetaData md = cc.getMeta_data();
        if (md == null) {
            throw refuse(RefusalCode.INVALID_FOOTER, name, "row group " + g + " has a column chunk without metadata");
        }
        if (!path.equals(md.getPath_in_schema()) || md.getNum_values() < 0 || md.getTotal_uncompressed_size() < 0) {
            throw refuse(RefusalCode.INVALID_FOOTER, name, "row group " + g + " has inconsistent column metadata for " + path);
        }
        if (md.getType() != leaf.getType()) {
            throw refuse(RefusalCode.INVALID_FOOTER, name, "row group " + g + " has a column chunk of type " + md.getType() + " for a "
                    + leaf.getType() + " column " + leaf.getName());
        }
        long[] range = chunkRange(md);
        if (range == null || range[1] > size) {
            throw refuse(RefusalCode.INVALID_FOOTER, name, "row group " + g + " has a column chunk outside the part (column " + leaf.getName() + ")");
        }
    }

    /** {@return a chunk's byte range {start, end}: from its dictionary page or first data page, or null on overflow} */
    static long[] chunkRange(ColumnMetaData md) {
        long start = md.getData_page_offset();
        if (md.isSetDictionary_page_offset() && md.getDictionary_page_offset() > 0 && md.getDictionary_page_offset() < start) {
            start = md.getDictionary_page_offset();
        }
        long length = md.getTotal_compressed_size();
        if (start < 0 || length < 0 || start > Long.MAX_VALUE - length) {
            return null;
        }
        return new long[] {start, start + length};
    }

    /** STAMP-1 and STAMP-2. */
    private static void checkStamps(FileMetaData meta, Manifest manifest, String name) {
        Map<String, String> found = new HashMap<>();
        List<KeyValue> kvs = meta.getKey_value_metadata();
        if (kvs != null) {
            for (KeyValue kv : kvs) {
                String k = kv.getKey();
                if (STAMP_VERSION.equals(k) || STAMP_DRAFT.equals(k) || STAMP_SCHEMA.equals(k) || STAMP_KEY_COLUMN.equals(k) || STAMP_KEY_UNIQUE.equals(k)) {
                    if (found.containsKey(k)) {
                        throw refuse(RefusalCode.STAMP_MISMATCH, name, "the stamp " + k + " appears more than once");
                    }
                    if (!kv.isSetValue()) {
                        throw refuse(RefusalCode.STAMP_MISMATCH, name, "the stamp " + k + " has no value");
                    }
                    found.put(k, kv.getValue());
                }
            }
        }
        FormatVersion v = manifest.version();
        expectStamp(found, STAMP_VERSION, Long.toString(v.formatVersion()), name);
        if (v.draftRevision().isPresent()) {
            expectStamp(found, STAMP_DRAFT, Long.toString(v.draftRevision().getAsLong()), name);
        } else if (found.containsKey(STAMP_DRAFT)) {
            throw refuse(RefusalCode.STAMP_MISMATCH, name, "the part carries a draft_revision stamp on a non-draft format version");
        }
        expectStamp(found, STAMP_SCHEMA, manifest.schemaId(), name);
        expectStamp(found, STAMP_KEY_COLUMN, manifest.key().column(), name);
        expectStamp(found, STAMP_KEY_UNIQUE, Boolean.toString(manifest.key().unique()), name);
    }

    private static void expectStamp(Map<String, String> found, String key, String want, String name) {
        String got = found.get(key);
        if (got == null) {
            throw refuse(RefusalCode.STAMP_MISMATCH, name, "the stamp " + key + " is missing");
        }
        if (!got.equals(want)) {
            throw refuse(RefusalCode.STAMP_MISMATCH, name, "the stamp " + key + " is \"" + got + "\", the manifest says \"" + want + "\"");
        }
    }

    // ------------------------------------------------------------------
    // Storage

    static CancellationException cancelled(Throwable cause) {
        CancellationException e = new CancellationException("interrupted");
        e.initCause(cause);
        return e;
    }

    private static ByteBuffer read(PartSource source, String name, long position, int length, ReadContext ctx) {
        ctx.checkCancelled();
        ctx.stats.sourceReads++;
        ctx.stats.sourceBytes += length;
        try {
            ByteBuffer b = source.read(position, length, ctx.cancellation);
            if (b.remaining() != length) {
                throw new StorageException("read part " + name, new IOException("the storage returned " + b.remaining() + " bytes for " + length));
            }
            return b.duplicate();
        } catch (InterruptedIOException e) {
            if (e instanceof java.net.SocketTimeoutException) {
                throw new StorageException("read part " + name, e);
            }
            throw cancelled(e);
        } catch (IOException e) {
            throw new StorageException("read part " + name, e);
        }
    }

    private byte[] readBytes(long position, int length, ReadContext ctx) {
        ByteBuffer b = read(source, name, position, length, ctx);
        if (b.hasArray() && b.arrayOffset() == 0 && b.position() == 0 && b.array().length == length) {
            return b.array();
        }
        byte[] out = new byte[length];
        b.duplicate().get(out);
        return out;
    }

    /** COL-3: a nested, repeated or INT96 column of this part is rejected, never answered as absent. */
    void checkProjection(List<String> columns) {
        for (String column : columns) {
            if (schema.byName.containsKey(column) && !projectable.containsKey(column)) {
                throw new UnsupportedFeatureException(UnsupportedCode.UNSUPPORTED_PROJECTION,
                        "column " + column + " is nested, repeated, of type INT96, or not unique in part " + name);
            }
        }
    }

    @Override
    public void close() throws IOException {
        source.close();
    }

    // ------------------------------------------------------------------
    // Key page index (RG-2 to RG-4)

    RowGroup rowGroup(int physical) {
        return meta.getRow_groups().get(physical);
    }

    /** {@return the checked key page index of a non-empty physical row group, loading it on first use} */
    KeyIndex keyIndex(int physical, ReadContext ctx) { return keyIndex(physical, ctx, true); }

    /** Build-only indexes never consume the shared non-evicting cache. */
    KeyIndex keyIndex(int physical, ReadContext ctx, boolean retain) {
        KeyIndex index = keyIndexes.get(physical);
        if (index != null) {
            ctx.stats.rowGroupsOpened++;
            return index;
        }
        String where = "row group " + physical + ": ";
        ColumnChunk cc = rowGroup(physical).getColumns().get(keyLeaf);
        if (!cc.isSetColumn_index_offset() || !cc.isSetColumn_index_length() || !cc.isSetOffset_index_offset() || !cc.isSetOffset_index_length()
                || cc.getColumn_index_offset() == 0 || cc.getColumn_index_length() == 0 || cc.getOffset_index_offset() == 0
                || cc.getOffset_index_length() == 0) {
            throw refuse(RefusalCode.MISSING_PAGE_INDEX, where + "the key column lacks a ColumnIndex or an OffsetIndex");
        }
        long ciOff = cc.getColumn_index_offset();
        long oiOff = cc.getOffset_index_offset();
        int ciLen = cc.getColumn_index_length();
        int oiLen = cc.getOffset_index_length();
        if (ciOff < 0 || ciLen < 0 || ciOff > size - ciLen || oiOff < 0 || oiLen < 0 || oiOff > size - oiLen) {
            throw refuse(RefusalCode.INVALID_PAGE_INDEX, where + "a key page index lies outside the part");
        }
        ColumnMetaData md = cc.getMeta_data();
        Codecs.checkSupported(md.getCodec(), "part " + name);
        long[] range = chunkRange(md);
        byte[][] raw = readTwo(ciOff, ciLen, oiOff, oiLen, ctx);
        ColumnIndex ci;
        OffsetIndex oi;
        try {
            ci = Thrift.columnIndex(raw[0]);
        } catch (DataException e) {
            throw refuse(RefusalCode.INVALID_PAGE_INDEX, where + "the key ColumnIndex does not parse: " + e.getMessage());
        }
        try {
            oi = Thrift.offsetIndex(raw[1]);
        } catch (DataException e) {
            throw refuse(RefusalCode.INVALID_PAGE_INDEX, where + "the key OffsetIndex does not parse: " + e.getMessage());
        }
        try {
            index = KeyIndex.check(ci, oi, groupRows[physical], range[0], range[1], utf8Key);
        } catch (DataException e) {
            throw refuse(RefusalCode.INVALID_PAGE_INDEX, where + e.getMessage());
        }
        if (!retain) {
            ctx.stats.rowGroupsOpened++;
            ctx.stats.indexLoads++;
            return index;
        }
        synchronized (indexMemory) {
            KeyIndex previous = keyIndexes.get(physical);
            if (previous != null) {
                ctx.stats.rowGroupsOpened++;
                return previous;
            }
            indexMemory.reserve(index.memoryBytes());
            keyIndexes.set(physical, index);
            ctx.stats.rowGroupsOpened++;
            ctx.stats.indexLoads++;
            return index;
        }
    }

    /** Exact first keys, unlike shortened index bounds, are safe for row-group routing. */
    byte[] firstKey(int physical, ReadContext ctx) {
        byte[] first = firstKeys.get(physical);
        if (first != null) {
            return first;
        }
        ctx.enterGroup(ordinal, physical);
        byte[][] decoded = keyPage(physical, keyIndex(physical, ctx), 0, ctx);
        ctx.checkKeyPage(this, physical, 0, decoded);
        return rememberFirst(physical, decoded[0]);
    }

    byte[] knownFirst(int physical) { return firstKeys.get(physical); }

    byte[] rememberFirst(int physical, byte[] key) {
        synchronized (indexMemory) {
            byte[] first = firstKeys.get(physical);
            if (first == null) {
                indexMemory.reserve(32L + ((key.length + 7L) & ~7L));
                first = key;
                firstKeys.set(physical, first);
            }
            return first;
        }
    }

    /** Coalesces only when the unused gap is no larger than the useful index bytes. */
    private byte[][] readTwo(long aOff, int aLen, long bOff, int bLen, ReadContext ctx) {
        if (aLen > ctx.limits.maxIndexBytes() || bLen > ctx.limits.maxIndexBytes()) {
            int len = Math.max(aLen, bLen);
            throw new LimitExceededException("maxIndexBytes", len, ctx.limits.maxIndexBytes(), "part " + name);
        }
        long from = Math.min(aOff, bOff);
        long to = Math.max(aOff + aLen, bOff + bLen);
        if (to - from <= Integer.MAX_VALUE && to - from <= (long) aLen + bLen + Math.min((long) aLen + bLen, 64 * 1024)) {
            byte[] span = readBytes(from, (int) (to - from), ctx);
            return new byte[][] {
                Arrays.copyOfRange(span, (int) (aOff - from), (int) (aOff - from) + aLen),
                Arrays.copyOfRange(span, (int) (bOff - from), (int) (bOff - from) + bLen)
            };
        }
        return new byte[][] {readBytes(aOff, aLen, ctx), readBytes(bOff, bLen, ctx)};
    }

    // ------------------------------------------------------------------
    // Key pages (CHECK-2)

    /**
     * Reads, decodes and checks key page {@code page} of a physical row group: its header locates
     * exactly one data page holding the rows its OffsetIndex entry gives it (RG-3), and it holds one
     * non-empty key per row (KEY-2), valid UTF-8 for a string key (KEY-6), ordered according to the manifest (KEY-3),
     * within the page's bounds (RG-5). Order against other pages is the caller's check.
     */
    byte[][] keyPage(int physical, KeyIndex index, int page, ReadContext ctx) {
        ctx.checkCancelled();
        String where = "row group " + physical + " key page " + page + ": ";
        int sz = index.size[page];
        checkPageSize(sz, ctx);
        byte[] raw = readBytes(index.offset[page], sz, ctx);
        PageHeader header;
        int headerLength;
        try {
            Thrift.Decoded<PageHeader> h = Thrift.pageHeader(raw, 0, raw.length);
            header = h.value();
            headerLength = h.length();
        } catch (DataException e) {
            throw refuse(RefusalCode.INVALID_PAGE_INDEX, where + "its OffsetIndex entry does not locate a page header");
        }
        if (header.getType() != PageType.DATA_PAGE && header.getType() != PageType.DATA_PAGE_V2) {
            throw refuse(RefusalCode.INVALID_PAGE_INDEX, where + "its OffsetIndex entry locates a " + header.getType() + ", not a data page");
        }
        if (header.getCompressed_page_size() < 0 || (long) headerLength + header.getCompressed_page_size() != sz) {
            throw refuse(RefusalCode.INVALID_PAGE_INDEX, where + "its OffsetIndex entry does not locate exactly one page");
        }
        long rows;
        try {
            rows = PageDecoder.rowsOf(header);
        } catch (DataException e) {
            throw refuse(RefusalCode.INVALID_PAGE_INDEX, where + e.getMessage());
        }
        if (rows != index.rows[page]) {
            throw refuse(RefusalCode.INVALID_PAGE_INDEX, where + "the page holds " + rows + " rows, its OffsetIndex entry " + index.rows[page]);
        }
        ColumnPage decoded = decode(physical, keySpec, header, ByteBuffer.wrap(raw, headerLength, sz - headerLength), ctx, where);
        ctx.stats.keyPagesDecoded++;
        ctx.stats.keysDecoded += decoded.rows;
        if (decoded.rows != rows) {
            throw refuse(RefusalCode.INVALID_PAGE_INDEX, where + "the page holds " + decoded.rows + " keys, its OffsetIndex entry " + rows);
        }
        byte[][] keys = decoded.binaries;
        for (int i = 0; i < keys.length; i++) {
            byte[] key = keys[i];
            if (key.length == 0 || (utf8Key && !Utf8.isValid(key))) {
                throw refuse(RefusalCode.INVALID_KEY, where + "row " + (index.firstRow[page] + i) + " holds an empty key or a string key that is not UTF-8");
            }
            if (i > 0 && (Arrays.compareUnsigned(keys[i - 1], key) > 0
                    || (unique && Arrays.equals(keys[i - 1], key)))) {
                throw refuse(RefusalCode.KEY_ORDER, where + "row " + (index.firstRow[page] + i) + " does not sort after the row before it");
            }
        }
        if (Arrays.compareUnsigned(keys[0], index.lo[page]) < 0 || Arrays.compareUnsigned(keys[keys.length - 1], index.hi[page]) > 0) {
            throw refuse(RefusalCode.INVALID_PAGE_INDEX, where + "the page holds a key outside its ColumnIndex bounds");
        }
        return keys;
    }

    private void checkPageSize(long sz, ReadContext ctx) {
        if (sz > ctx.limits.maxPageBytes()) {
            throw new LimitExceededException("maxPageBytes", sz, ctx.limits.maxPageBytes(), "part " + name);
        }
    }

    /**
     * Key lifetime policy: key pages and key dictionaries always decode with null scratch, producing
     * permanent arrays that are never modified. Rows, continuations, first-key caches, page boundaries
     * and list-index walkers may share these arrays. Only projected value columns reuse storage.
     */
    PageScratch scratch(int physical, ColumnSpec col, ReadContext ctx) {
        return col.leaf() == keyLeaf ? null
                : ctx.scratch(col, new ReadContext.ChunkKey(ordinal, physical, col.leaf()));
    }

    private ColumnPage decode(int physical, ColumnSpec col, PageHeader header, ByteBuffer body, ReadContext ctx, String where) {
        ColumnMetaData md = rowGroup(physical).getColumns().get(col.leaf()).getMeta_data();
        try {
            return PageDecoder.decodeData(header, body, col, md.getCodec(), () -> dictionary(physical, col, ctx), ctx.limits,
                    "part " + name + " " + where, scratch(physical, col, ctx), ctx.codecs);
        } catch (DataException e) {
            throw refuse(RefusalCode.INVALID_PARQUET, where + e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Dictionaries and value columns (COL-3, COL-4)

    private Dictionary dictionary(int physical, ColumnSpec col, ReadContext ctx) throws DataException {
        ReadContext.ChunkKey key = new ReadContext.ChunkKey(ordinal, physical, col.leaf());
        PageScratch slot = scratch(physical, col, ctx);
        Dictionary d = ctx.dictionary(key);
        if (d != null) {
            return d;
        }
        ColumnMetaData md = rowGroup(physical).getColumns().get(col.leaf()).getMeta_data();
        long[] range = chunkRange(md);
        ByteBuffer body;
        PageHeader header;
        Optional<ValueIndex> located = col.leaf() == keyLeaf ? null : valueIndexes.get(((long) physical << 32) | col.leaf());
        long extent = located == null || located.isEmpty() ? 0 : dictionaryBytes(physical, col.leaf(), located.get());
        if (extent > 0) {
            // The OffsetIndex locates the first data page: what precedes it is the dictionary, read at once.
            checkPageSize(extent, ctx);
            byte[] raw = readBytes(range[0], (int) extent, ctx);
            Thrift.Decoded<PageHeader> h = Thrift.pageHeader(raw, 0, raw.length);
            header = h.value();
            if (header.getType() != PageType.DICTIONARY_PAGE) {
                throw new DataException("the column chunk has no dictionary page");
            }
            int compressed = header.getCompressed_page_size();
            if (compressed < 0 || (long) h.length() + compressed > extent) {
                throw new DataException("the dictionary page overlaps the first data page");
            }
            body = ByteBuffer.wrap(raw, h.length(), compressed);
        } else {
            long at = md.isSetDictionary_page_offset() && md.getDictionary_page_offset() > 0 ? md.getDictionary_page_offset() : md.getData_page_offset();
            if (at < range[0] || at >= range[1]) {
                throw new DataException("the dictionary page lies outside its column chunk");
            }
            Header h = header(at, range[1], ctx);
            header = h.header;
            if (header.getType() != PageType.DICTIONARY_PAGE) {
                throw new DataException("the column chunk has no dictionary page");
            }
            int compressed = header.getCompressed_page_size();
            if (compressed < 0 || at + h.length + compressed > range[1]) {
                throw new DataException("the dictionary page overruns its column chunk");
            }
            checkPageSize(compressed, ctx);
            body = ByteBuffer.wrap(readBytes(at + h.length, compressed, ctx));
        }
        d = PageDecoder.decodeDictionary(header, body, col, md.getCodec(), ctx.limits, "part " + name,
                slot, ctx.codecs);
        ctx.putDictionary(key, d);
        return d;
    }

    private record Header(PageHeader header, int length) {}

    /** Reads the page header at {@code at}, whose length is unknown, within {@code [at, end)}. */
    private Header header(long at, long end, ReadContext ctx) throws DataException {
        long available = end - at;
        for (long window : new long[] {8 * 1024, 256 * 1024, ctx.limits.maxPageBytes()}) {
            int n = (int) Math.min(available, window);
            byte[] raw = readBytes(at, n, ctx);
            try {
                Thrift.Decoded<PageHeader> h = Thrift.pageHeader(raw, 0, n);
                return new Header(h.value(), h.length());
            } catch (DataException e) {
                if (n == available || window >= ctx.limits.maxPageBytes()) {
                    throw new DataException("no page header at offset " + at, e);
                }
            }
        }
        throw new DataException("no page header at offset " + at);
    }

    /**
     * {@return the extent of a value column chunk's dictionary page: the bytes from the chunk's start to
     * the first data page its OffsetIndex locates, 0 when the chunk has no dictionary}
     */
    long dictionaryBytes(int physical, int leaf, ValueIndex index) {
        return index.offset[0] - chunkRange(rowGroup(physical).getColumns().get(leaf).getMeta_data())[0];
    }

    /** Refuses a value column chunk whose codec this reader does not decode, before any page is read. */
    void checkCodec(int physical, int leaf) {
        Codecs.checkSupported(rowGroup(physical).getColumns().get(leaf).getMeta_data().getCodec(), "part " + name);
    }

    /** {@return the checked OffsetIndex of a value column chunk, or empty when it has none (COL-4)} */
    Optional<ValueIndex> valueIndex(int physical, int leaf, ReadContext ctx) {
        long cacheKey = ((long) physical << 32) | leaf;
        Optional<ValueIndex> cached = valueIndexes.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        ColumnChunk cc = rowGroup(physical).getColumns().get(leaf);
        Optional<ValueIndex> result;
        if (!cc.isSetOffset_index_offset() || !cc.isSetOffset_index_length() || cc.getOffset_index_length() == 0) {
            return Optional.empty();
        } else {
            long off = cc.getOffset_index_offset();
            int len = cc.getOffset_index_length();
            String where = "row group " + physical + " column " + schema.leaves.get(leaf).getName() + ": ";
            if (off <= 0 || len < 0 || off > size - len) {
                throw refuse(RefusalCode.INVALID_PARQUET, where + "its OffsetIndex lies outside the part");
            }
            if (len > ctx.limits.maxIndexBytes()) {
                throw new LimitExceededException("maxIndexBytes", len, ctx.limits.maxIndexBytes(), "part " + name);
            }
            long[] range = chunkRange(cc.getMeta_data());
            byte[] raw = readBytes(off, len, ctx);
            try {
                result = Optional.of(ValueIndex.check(Thrift.offsetIndex(raw), groupRows[physical], range[0], range[1]));
            } catch (DataException e) {
                throw refuse(RefusalCode.INVALID_PARQUET, where + "its OffsetIndex is invalid: " + e.getMessage());
            }
        }
        synchronized (indexMemory) {
            Optional<ValueIndex> previous = valueIndexes.get(cacheKey);
            if (previous != null) {
                return previous;
            }
            indexMemory.reserve(result.orElseThrow().memoryBytes());
            valueIndexes.put(cacheKey, result);
            return result;
        }
    }

    /** {@return the value of a projected column at a row of a physical row group} */
    Value value(int physical, ColumnSpec col, long row, ReadContext ctx) {
        ReadContext.ChunkKey key = new ReadContext.ChunkKey(ordinal, physical, col.leaf());
        ReadContext.DecodedPage page = ctx.valuePage(key);
        if (page == null || row < page.firstRow() || row >= page.firstRow() + page.page().rows) {
            page = valuePage(physical, col, row, key, ctx);
            ctx.putValuePage(key, page);
        }
        return page.page().value((int) (row - page.firstRow()));
    }

    private ReadContext.DecodedPage valuePage(int physical, ColumnSpec col, long row, ReadContext.ChunkKey key, ReadContext ctx) {
        ctx.checkCancelled();
        scratch(physical, col, ctx);
        String where = "row group " + physical + " column " + col.name() + ": ";
        ColumnMetaData md = rowGroup(physical).getColumns().get(col.leaf()).getMeta_data();
        Codecs.checkSupported(md.getCodec(), "part " + name);
        long groupRowCount = groupRows[physical];
        Optional<ValueIndex> index = valueIndex(physical, col.leaf(), ctx);
        if (index.isPresent()) {
            ValueIndex vi = index.get();
            if (ctx.planned && ctx.dictionary(key) == null && dictionaryBytes(physical, col.leaf(), vi) > 0) {
                try {
                    dictionary(physical, col, ctx);
                } catch (DataException e) {
                    throw refuse(RefusalCode.INVALID_PARQUET, where + e.getMessage());
                }
            }
            int p = vi.pageOf(row);
            int sz = vi.size[p];
            checkPageSize(sz, ctx);
            byte[] raw = readBytes(vi.offset[p], sz, ctx);
            PageHeader header;
            int headerLength;
            try {
                Thrift.Decoded<PageHeader> h = Thrift.pageHeader(raw, 0, raw.length);
                header = h.value();
                headerLength = h.length();
            } catch (DataException e) {
                throw refuse(RefusalCode.INVALID_PARQUET, where + "its OffsetIndex entry " + p + " does not locate a page header");
            }
            if ((long) headerLength + header.getCompressed_page_size() != sz) {
                throw refuse(RefusalCode.INVALID_PARQUET, where + "its OffsetIndex entry " + p + " does not locate exactly one page");
            }
            long want = vi.rowsOf(p, groupRowCount);
            ColumnPage decoded = decode(physical, col, header, ByteBuffer.wrap(raw, headerLength, sz - headerLength), ctx, where);
            ctx.stats.valuePagesDecoded++;
            if (decoded.rows != want) {
                throw refuse(RefusalCode.INVALID_PARQUET, where + "page " + p + " holds " + decoded.rows + " values for " + want + " rows");
            }
            return new ReadContext.DecodedPage(vi.firstRow[p], decoded);
        }
        // No OffsetIndex: walk the chunk's pages from where this request last stopped, or its start.
        long[] range = chunkRange(md);
        long[] state = ctx.sequential(key);
        if (state == null || row < state[1]) {
            state = new long[] {range[0], 0};
        }
        long at = state[0];
        long first = state[1];
        try {
            while (true) {
                if (at >= range[1]) {
                    throw new DataException("the column chunk holds fewer rows than its row group");
                }
                Header h = header(at, range[1], ctx);
                int compressed = h.header.getCompressed_page_size();
                if (compressed < 0 || at + h.length + compressed > range[1]) {
                    throw new DataException("a page overruns its column chunk");
                }
                long next = at + h.length + compressed;
                PageType type = h.header.getType();
                if (type == PageType.DATA_PAGE || type == PageType.DATA_PAGE_V2) {
                    long rows = PageDecoder.rowsOf(h.header);
                    if (rows <= 0 || first + rows > groupRowCount) {
                        throw new DataException("a page of " + rows + " rows at row " + first + " of a " + groupRowCount + "-row group");
                    }
                    if (row < first + rows) {
                        checkPageSize(compressed, ctx);
                        byte[] body = readBytes(at + h.length, compressed, ctx);
                        ColumnPage decoded = decode(physical, col, h.header, ByteBuffer.wrap(body), ctx, where);
                        ctx.stats.valuePagesDecoded++;
                        if (decoded.rows != rows) {
                            throw new DataException("a page holds " + decoded.rows + " values for " + rows + " rows");
                        }
                        ctx.putSequential(key, new long[] {next, first + rows});
                        return new ReadContext.DecodedPage(first, decoded);
                    }
                    first += rows;
                }
                at = next;
            }
        } catch (DataException e) {
            throw refuse(RefusalCode.INVALID_PARQUET, where + e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "PartFile[" + name + "]";
    }
}
