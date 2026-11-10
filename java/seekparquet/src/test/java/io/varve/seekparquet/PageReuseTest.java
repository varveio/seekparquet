// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(60)
class PageReuseTest {
    @TempDir Path tmp;

    private static final List<String> COLUMNS = List.of("size", "note", "key", "note", "missing");

    private static void poison(PageScratch s) {
        Arrays.fill(s.plain, (byte) 0xa5);
        Arrays.fill(s.dictionaryPlain, (byte) 0xa5);
        Arrays.fill(s.arena, (byte) 0xa5);
        Arrays.fill(s.levels, (byte) 0xa5);
        Arrays.fill(s.nulls, true);
        Arrays.fill(s.bools, false);
        Arrays.fill(s.ints, -99);
        Arrays.fill(s.longs, -99);
        Arrays.fill(s.offsets, -99);
        Arrays.fill(s.lengths, -99);
        Arrays.fill(s.binaries, null);
    }

    private static Row expected(TestDatasets.TestRow r) {
        Bytes key = Bytes.of(r.key());
        byte[] note = TestDatasets.note(r);
        Value value = note == null ? Value.Null.INSTANCE : new Value.ByteArray(Bytes.of(note));
        return new Row(key, 0, COLUMNS, List.of(r.size() < 0 ? Value.Null.INSTANCE : new Value.Int64(r.size()),
                value, new Value.ByteArray(key), value, Value.Absent.INSTANCE));
    }

    /** Independent generated oracle, both indexed and sequential value reads, every codec and null pages. */
    @Test void owningResultsAcrossGroupsPartsAndConcurrentRequests() throws Exception {
        var spec = new TestDatasets.Spec();
        spec.noteColumn = true;
        spec.dictionaryNote = true;
        spec.mixedValuePages = true;
        spec.rowsPerGroup = 31;
        spec.rowsPerPage = 7;
        List<List<TestDatasets.TestRow>> parts = new ArrayList<>();
        for (int p = 0; p < 2; p++) {
            List<TestDatasets.TestRow> rows = new ArrayList<>();
            for (int i = p * 310; i < (p + 1) * 310; i++) {
                // Some whole pages are null, with non-null pages before and after them.
                long size = (i % 31) / 7 == 2 ? -3 : i;
                rows.add(TestDatasets.TestRow.of(String.format("k%05d", i), size));
            }
            parts.add(rows);
        }
        List<Row> want = parts.stream().flatMap(List::stream).map(PageReuseTest::expected).toList();
        RangeQuery range = RangeQuery.all(1000).toBuilder().columns(COLUMNS).build();
        ListQuery list = ListQuery.builder((byte) '/').limit(1000).columns(COLUMNS).build();
        for (boolean sequential : List.of(false, true)) {
            spec.omitValueOffsetIndexes = sequential;
            Path root = TestDatasets.write(tmp.resolve("sequential-" + sequential), spec, parts);
            try (Dataset d = Dataset.open(root)) {
                RangePlan plan = d.planRanges(List.of(RangeQuery.all(1)), COLUMNS);
                for (boolean indexed : List.of(false, true)) {
                    if (indexed) d.buildListIndex(new ListIndexOptions(1 << 20, 1, 8), Cancellation.none());
                    try (Dataset.Request request = d.begin(Cancellation.none())) {
                        List<Row> got = Queries.range(d.parts(), range, request.ctx);
                        List<Continuation.RowPosition> cursors = got.stream().map(Row::continuation).toList();
                        PartFile last = d.parts().get(1, request.ctx);
                        poison(last.scratch(last.groups[last.groups.length - 1], last.projectable.get("size"), request.ctx));
                        poison(last.scratch(last.groups[last.groups.length - 1], last.projectable.get("note"), request.ctx));
                        assertEquals(want, got);
                        assertEquals(want.stream().map(Row::continuation).toList(), cursors);
                    }
                    try (Dataset.Request request = d.begin(Cancellation.none())) {
                        List<ListEntry> got = Queries.list(d.parts(), list, request.ctx);
                        PartFile last = d.parts().get(1, request.ctx);
                        poison(last.scratch(last.groups[last.groups.length - 1], last.projectable.get("size"), request.ctx));
                        poison(last.scratch(last.groups[last.groups.length - 1], last.projectable.get("note"), request.ctx));
                        assertEquals(want.stream().map(ListEntry.RowEntry::new).toList(), got);
                    }
                    List<Row> scanned = new ArrayList<>();
                    plan.scan(scanned::add);
                    assertEquals(want, scanned); // Keep every result past reuse of every slot.
                    try (var executor = Executors.newFixedThreadPool(2)) {
                        var a = executor.submit(() -> { List<Row> out = new ArrayList<>(); plan.scan(out::add); return out; });
                        var b = executor.submit(() -> d.range(range));
                        assertEquals(want, a.get());
                        assertEquals(want, b.get());
                    }
                    assertEquals(want, scanned);
                    assertEquals(want.subList(310, want.size()), d.range(range.next(want.get(309))));
                }
            }
        }
    }

    /** Keys stay on the permanent path, while result binary arenas are copied before reuse. */
    @Test void keyPagesHaveNoScratchAndResultsSurviveValueReuse() throws Exception {
        var spec = new TestDatasets.Spec();
        spec.noteColumn = true;
        Path root = TestDatasets.write(tmp, spec, List.of(List.of(TestDatasets.TestRow.of("a/", 1), TestDatasets.TestRow.of("b/", 2))));
        try (Dataset d = Dataset.open(root); Dataset.Request request = d.begin(Cancellation.none())) {
            KeyCursor cursor = new KeyCursor(d.parts(), null, request.ctx);
            assertTrue(cursor.seek(null, false));
            Row row = Queries.row(cursor, COLUMNS, request.ctx);
            Continuation continuation = row.continuation();
            PartFile part = cursor.partFile();
            assertNull(part.scratch(0, part.keySpec, request.ctx));
            assertSame(cursor.key(), row.key().unsafeArray());
            assertSame(row.key(), ((Value.ByteArray) row.values().get(2)).value());
            poison(part.scratch(0, part.projectable.get("note"), request.ctx));
            assertTrue(cursor.next());
            Row next = Queries.row(cursor, COLUMNS, request.ctx);
            assertNotSame(row.key().unsafeArray(), next.key().unsafeArray());
            assertEquals(expected(TestDatasets.TestRow.of("a/", 1)), row);
            assertEquals(Bytes.utf8("a/"), continuation.bytes());
        }
        // LIST-8 exact rows and common-prefix continuations own the same boundary.
        try (Dataset d = Dataset.open(root); Dataset.Request request = d.begin(Cancellation.none())) {
            List<ListEntry> entries = Queries.list(d.parts(), ListQuery.builder((byte) '/').limit(10).columns(COLUMNS).build(), request.ctx);
            PartFile part = d.parts().get(0, request.ctx);
            poison(part.scratch(0, part.projectable.get("note"), request.ctx));
            var first = (ListEntry.CommonPrefix) entries.getFirst();
            assertEquals(Bytes.utf8("a/"), first.prefix());
            assertEquals(expected(TestDatasets.TestRow.of("a/", 1)), first.exact().orElseThrow());
            assertEquals(Bytes.utf8("a/"), ListQuery.builder((byte) '/').limit(10).build().next(first).cursor().orElseThrow().bytes());
        }
    }

    private static byte[] binary(String value) {
        byte[] bytes = Bytes.utf8(value).toByteArray();
        return ByteBuffer.allocate(4 + bytes.length).order(ByteOrder.LITTLE_ENDIAN).putInt(bytes.length).put(bytes).array();
    }

    @Test void keyDictionariesAndPagesUsePermanentBacking() {
        try (Dataset d = Dataset.open(ConformanceFixturesTest.fixture("datasets/encodings"));
                Dataset.Request request = d.begin(Cancellation.none())) {
            ReadContext ctx = request.ctx;
            PartFile part = d.parts().get(0, ctx);
            ctx.enterGroup(part.ordinal, 0);
            assertNull(part.scratch(0, part.keySpec, ctx));
            byte[][] first = part.keyPage(0, part.keyIndex(0, ctx), 0, ctx);
            assertNotNull(ctx.dictionary(new ReadContext.ChunkKey(part.ordinal, 0, part.keyLeaf)));
            assertSame(first[0], part.rememberFirst(0, first[0]));
            byte[] expected = first[0].clone();
            ctx.enterGroup(part.ordinal, 1);
            assertNull(part.scratch(1, part.keySpec, ctx));
            part.keyPage(1, part.keyIndex(1, ctx), 0, ctx);
            ctx.enterGroup(part.ordinal, 0);
            byte[][] again = part.keyPage(0, part.keyIndex(0, ctx), 0, ctx);
            assertNotSame(first, again);
            assertNotSame(first[0], again[0]);
            assertArrayEquals(expected, first[0]);
            assertArrayEquals(expected, again[0]);
            assertSame(first[0], part.knownFirst(0));
        }
    }

    private static ColumnPage decode(byte[] body, int rows, ColumnSpec col, Encoding encoding,
            PageDecoder.DictionarySupplier dictionary, PageScratch scratch, Codecs.State codecs) throws DataException {
        var header = new PageHeader(PageType.DATA_PAGE, body.length, body.length)
                .setData_page_header(new DataPageHeader(rows, encoding, Encoding.RLE, Encoding.RLE));
        return PageDecoder.decodeData(header, ByteBuffer.wrap(body), col, CompressionCodec.UNCOMPRESSED,
                dictionary, Limits.DEFAULT, "test", scratch, codecs);
    }

    @Test void dictionariesOwnFreshEntriesAcrossFallbackAndChunkReplacement() throws Exception {
        for (Column.Type type : List.of(Column.Type.BYTE_ARRAY, Column.Type.FIXED_LEN_BYTE_ARRAY)) {
            var s = new PageScratch();
            var codecs = new Codecs.State();
            var col = ColumnSpec.of("binary", 1, type, type == Column.Type.BYTE_ARRAY ? 0 : 10, false);
            Dictionary[] dictionary = new Dictionary[1];
            PageDecoder.DictionarySupplier supplier = () -> {
                if (dictionary[0] == null) dictionary[0] = dictionary("dictionary", col, s, codecs);
                return dictionary[0];
            };
            byte[] ids = {1, 4, 0}; // two references to entry zero
            ColumnPage first = decode(ids, 2, col, Encoding.RLE_DICTIONARY, supplier, s, codecs);
            Value kept = first.value(0);
            Bytes keptBytes = type == Column.Type.BYTE_ARRAY ? ((Value.ByteArray) kept).value()
                    : ((Value.FixedLenByteArray) kept).value();
            byte[][] entries = s.entries;
            assertNotSame(s.plain, s.dictionaryPlain);
            assertSame(entries[0], keptBytes.unsafeArray(), "results share the immutable dictionary entry");
            assertSame(first.binaries[0], first.binaries[1]);
            byte[] fallback = type == Column.Type.BYTE_ARRAY ? binary("plain-data") : Bytes.utf8("plain-data").toByteArray();
            decode(fallback, 1, col, Encoding.PLAIN, supplier, s, codecs);
            assertTrue(Arrays.stream(s.binaries).allMatch(java.util.Objects::isNull));
            ColumnPage again = decode(ids, 2, col, Encoding.RLE_DICTIONARY, supplier, s, codecs);
            assertSame(entries, s.entries, "dictionary entries are cached across data pages");
            assertEquals(kept, again.value(1));
            poison(s); // Only storage production reuses, never the entry bytes themselves.
            assertEquals(Bytes.utf8("dictionary"), keptBytes);
            byte[] backing = s.dictionaryPlain;
            s.enterGroup();
            assertNull(s.entries);
            assertNull(s.dictionary);
            assertTrue(Arrays.stream(s.binaries).allMatch(java.util.Objects::isNull));
            dictionary[0] = dictionary("new-entry!", col, s, codecs);
            ColumnPage replaced = decode(ids, 2, col, Encoding.RLE_DICTIONARY, supplier, s, codecs);
            assertSame(backing, s.dictionaryPlain, "dictionary output capacity is reused");
            assertNotSame(entries, s.entries, "a new dictionary gets a fresh entries table");
            assertNotSame(entries[0], s.entries[0]);
            assertArrayEquals(Bytes.utf8("new-entry!").toByteArray(), replaced.binaries[0]);
            assertEquals(Bytes.utf8("dictionary"), keptBytes);
        }
    }

    private static Dictionary dictionary(String text, ColumnSpec col, PageScratch s, Codecs.State codecs) throws DataException {
        byte[] body = col.type() == Column.Type.BYTE_ARRAY ? binary(text) : Bytes.utf8(text).toByteArray();
        var h = new PageHeader(PageType.DICTIONARY_PAGE, body.length, body.length)
                .setDictionary_page_header(new DictionaryPageHeader(1, Encoding.PLAIN));
        return PageDecoder.decodeDictionary(h, ByteBuffer.wrap(body), col, CompressionCodec.UNCOMPRESSED,
                Limits.DEFAULT, "dictionary", s, codecs);
    }

    /** A key probe can set activeGroup ahead of the cursor without a subsequent enterGroup clearing it. */
    @Test void probeThenMoveAcrossGroupsEvictsTheSlotsPreviousChunk() throws Exception {
        for (boolean sequential : List.of(false, true)) {
            var spec = new TestDatasets.Spec();
            spec.noteColumn = true;
            spec.dictionaryNote = true;
            spec.rowsPerGroup = 2;
            spec.rowsPerPage = 2;
            spec.omitValueOffsetIndexes = sequential;
            Path root = TestDatasets.write(tmp.resolve("probe-" + sequential), spec, List.of(List.of(
                    TestDatasets.TestRow.of("a", 1), TestDatasets.TestRow.of("b", 2),
                    TestDatasets.TestRow.of("c", 4), TestDatasets.TestRow.of("d", 5))));
            try (Dataset d = Dataset.open(root); Dataset.Request request = d.begin(Cancellation.none())) {
                ReadContext ctx = request.ctx;
                KeyCursor cursor = new KeyCursor(d.parts(), null, ctx);
                assertTrue(cursor.seekOrdinal(0));
                PartFile part = cursor.partFile();
                assertNull(part.knownFirst(1));
                assertArrayEquals(Bytes.utf8("c").toByteArray(), part.firstKey(1, ctx)); // probe ahead
                Row first = Queries.row(cursor, COLUMNS, ctx); // project group 0 after the group 1 probe
                ColumnSpec note = part.projectable.get("note");
                var old = new ReadContext.ChunkKey(part.ordinal, 0, note.leaf());
                PageScratch slot = part.scratch(0, note, ctx);
                byte[][] entries = slot.entries;
                assertNotNull(ctx.valuePage(old));
                assertNotNull(ctx.dictionary(old));
                assertEquals(old, slot.owner);
                assertTrue(cursor.next());
                assertTrue(cursor.next()); // activeGroup already says group 1, so this does not clear caches
                Row next = Queries.row(cursor, COLUMNS, ctx);
                assertEquals(expected(TestDatasets.TestRow.of("c", 4)), next);
                assertEquals(new ReadContext.ChunkKey(part.ordinal, 1, note.leaf()), slot.owner);
                assertNull(ctx.valuePage(old));
                assertNull(ctx.dictionary(old));
                assertNull(ctx.sequential(old));
                assertNotSame(entries, slot.entries);
                assertEquals(expected(TestDatasets.TestRow.of("a", 1)), first);
                // Revisiting the evicted chunk must decode it, not return its overwritten cached page.
                assertEquals(new Value.ByteArray(Bytes.utf8("n1")), part.value(0, note, 0, ctx));
                assertEquals(expected(TestDatasets.TestRow.of("c", 4)), next);
            }
        }
    }

    @Test void slotCapacitiesSurviveGroupsButRequestsAndColumnsAreIndependent() throws Exception {
        ReadContext ctx = new ReadContext(Cancellation.none(), Limits.DEFAULT);
        var a = ColumnSpec.of("a", 1, Column.Type.INT64, 0, false);
        var b = ColumnSpec.of("b", 2, Column.Type.INT64, 0, false);
        ctx.enterGroup(0, 0);
        PageScratch s = ctx.scratch(a, new ReadContext.ChunkKey(0, 0, 1));
        ColumnPage large = decode(new byte[800], 100, a, Encoding.PLAIN, null, s, ctx.codecs);
        ctx.enterGroup(1, 3);
        assertSame(s, ctx.scratch(ColumnSpec.of("a", 7, Column.Type.INT64, 0, false), new ReadContext.ChunkKey(1, 3, 7)));
        ColumnPage small = decode(new byte[8], 1, a, Encoding.PLAIN, null, s, ctx.codecs);
        assertSame(large.longs, small.longs);
        assertNotSame(s, ctx.scratch(b, new ReadContext.ChunkKey(1, 3, 2)));
        assertNotSame(s, ctx.forPlannedValues().scratch(a, new ReadContext.ChunkKey(0, 0, 1)));
        assertNotSame(s, new ReadContext(Cancellation.none(), Limits.DEFAULT).scratch(a, new ReadContext.ChunkKey(0, 0, 1)));
        assertThrows(DataException.class, () -> decode(new byte[16], 1, a, Encoding.PLAIN, null, s, ctx.codecs));
    }

    @Test void codecsRespectDeclaredSizeWithLargerRetainedOutput() throws Exception {
        var codecs = new Codecs.State();
        for (CompressionCodec codec : List.of(CompressionCodec.UNCOMPRESSED, CompressionCodec.ZSTD,
                CompressionCodec.SNAPPY, CompressionCodec.GZIP, CompressionCodec.LZ4_RAW)) {
            byte[] out = new byte[2048];
            byte[] large = TestDatasets.compress(new byte[1024], codec);
            assertSame(out, Codecs.decompress(codec, ByteBuffer.wrap(large), 1024, out, codecs));
            Arrays.fill(out, (byte) 0x5a);
            byte[] small = TestDatasets.compress(new byte[] {1, 2, 3}, codec);
            Codecs.decompress(codec, ByteBuffer.wrap(small), 3, out, codecs);
            assertArrayEquals(new byte[] {1, 2, 3}, Arrays.copyOf(out, 3));
            for (int i = 3; i < out.length; i++) assertEquals((byte) 0x5a, out[i], codec.name());
            assertThrows(DataException.class, () -> Codecs.decompress(codec, ByteBuffer.wrap(large), 3, out, codecs));
            for (int i = 3; i < out.length; i++) assertEquals((byte) 0x5a, out[i], codec.name());
            assertThrows(DataException.class, () -> Codecs.decompress(codec, ByteBuffer.wrap(small), 4, out, codecs));
        }
    }

    @Test void v2NullCountsUseLogicalLengthAndKeepExactFloatBits() throws Exception {
        var s = new PageScratch();
        var codecs = new Codecs.State();
        var col = ColumnSpec.of("float", 1, Column.Type.FLOAT, 0, true);
        s.nulls = new boolean[100];
        Arrays.fill(s.nulls, true); // retired larger all-null page
        int bits = 0x7fc00042;
        byte[] body = ByteBuffer.allocate(6).order(ByteOrder.LITTLE_ENDIAN).put(new byte[] {2, 1}).putInt(bits).array();
        var h = new PageHeader(PageType.DATA_PAGE_V2, 6, 6)
                .setData_page_header_v2(new DataPageHeaderV2(1, 0, 1, Encoding.PLAIN, 2, 0).setIs_compressed(false));
        ColumnPage p = PageDecoder.decodeData(h, ByteBuffer.wrap(body), col, CompressionCodec.ZSTD,
                null, Limits.DEFAULT, "v2", s, codecs);
        Value kept = p.value(0);
        poison(s);
        assertEquals(new Value.Float32(bits), kept);
    }

    @Test void deltaArenasGrowPreservingPrefixesAndOwnReturnedBytes() throws Exception {
        for (boolean prefixes : List.of(false, true)) {
            var s = new PageScratch();
            var codecs = new Codecs.State();
            var col = ColumnSpec.of("binary", 1, Column.Type.BYTE_ARRAY, 0, false);
            var allocator = new org.apache.parquet.bytes.HeapByteBufferAllocator();
            org.apache.parquet.column.values.ValuesWriter writer = prefixes
                    ? new org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter(64, 4096, allocator)
                    : new org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter(64, 4096, allocator);
            try {
                List<String> strings = List.of("", "prefix", "prefix" + "x".repeat(200), "prefixxy", "tail");
                for (String string : strings) writer.writeBytes(org.apache.parquet.io.api.Binary.fromString(string));
                var out = new java.io.ByteArrayOutputStream();
                writer.getBytes().writeAllTo(out);
                byte[] encoded = out.toByteArray();
                ColumnPage page = decode(encoded, strings.size(), col, prefixes ? Encoding.DELTA_BYTE_ARRAY
                        : Encoding.DELTA_LENGTH_BYTE_ARRAY, null, s, codecs);
                List<Value> values = new ArrayList<>();
                for (int i = 0; i < strings.size(); i++) values.add(page.value(i));
                List<Value.ByteArray> want = strings.stream().map(Bytes::utf8).map(Value.ByteArray::new).toList();
                assertEquals(want, values);
                poison(s);
                assertEquals(want, values);
                // Decode again over the poisoned capacities and a shorter plain page after it.
                page = decode(encoded, strings.size(), col, prefixes ? Encoding.DELTA_BYTE_ARRAY
                        : Encoding.DELTA_LENGTH_BYTE_ARRAY, null, s, codecs);
                assertEquals(want.get(2), page.value(2));
                assertEquals(new Value.ByteArray(Bytes.utf8("z")), decode(binary("z"), 1, col,
                        Encoding.PLAIN, null, s, codecs).value(0));
            } finally {
                writer.close();
            }
        }
    }

    @Test void warmBinaryAndDictionaryBuffersStillRejectSurplus() throws Exception {
        var s = new PageScratch();
        var codecs = new Codecs.State();
        var col = ColumnSpec.of("binary", 1, Column.Type.BYTE_ARRAY, 0, false);
        decode(binary("x".repeat(1000)), 1, col, Encoding.PLAIN, null, s, codecs);
        assertThrows(DataException.class, () -> decode(binary("extra"), 0, col, Encoding.PLAIN, null, s, codecs));
        byte[] body = binary("x".repeat(1000));
        var header = new PageHeader(PageType.DICTIONARY_PAGE, body.length, body.length)
                .setDictionary_page_header(new DictionaryPageHeader(1, Encoding.PLAIN));
        PageDecoder.decodeDictionary(header, ByteBuffer.wrap(body), col, CompressionCodec.UNCOMPRESSED,
                Limits.DEFAULT, "dictionary", s, codecs);
        byte[] surplus = binary("extra");
        header.setUncompressed_page_size(surplus.length).setCompressed_page_size(surplus.length);
        header.getDictionary_page_header().setNum_values(0);
        assertThrows(DataException.class, () -> PageDecoder.decodeDictionary(header, ByteBuffer.wrap(surplus),
                col, CompressionCodec.UNCOMPRESSED, Limits.DEFAULT, "dictionary", s, codecs));
        header.getDictionary_page_header().setNum_values(1);
        PageDecoder.decodeDictionary(header, ByteBuffer.wrap(surplus), col, CompressionCodec.UNCOMPRESSED,
                Limits.DEFAULT, "dictionary", s, codecs);
    }

    @Test void checksumStateIsResetBetweenPages() throws Exception {
        var s = new PageScratch();
        var codecs = new Codecs.State();
        var col = ColumnSpec.of("binary", 1, Column.Type.BYTE_ARRAY, 0, false);
        for (String text : List.of("first", "second", "third")) {
            byte[] body = binary(text);
            var crc = new java.util.zip.CRC32();
            crc.update(body);
            var h = new PageHeader(PageType.DATA_PAGE, body.length, body.length)
                    .setData_page_header(new DataPageHeader(1, Encoding.PLAIN, Encoding.RLE, Encoding.RLE))
                    .setCrc((int) crc.getValue());
            assertEquals(new Value.ByteArray(Bytes.utf8(text)), PageDecoder.decodeData(h, ByteBuffer.wrap(body),
                    col, CompressionCodec.UNCOMPRESSED, null, Limits.DEFAULT, "crc", s, codecs).value(0));
            h.setCrc(h.getCrc() ^ 1);
            assertThrows(DataException.class, () -> PageDecoder.decodeData(h, ByteBuffer.wrap(body),
                    col, CompressionCodec.UNCOMPRESSED, null, Limits.DEFAULT, "crc", s, codecs));
        }
    }
}
