// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.parquet.format.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.protocol.TField;
import shaded.parquet.org.apache.thrift.protocol.TStruct;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

/** Proposal 0001 classification regressions, including cases intentionally kept out of the shared matrix. */
class RulingsTest {
    @TempDir Path tmp;
    private int sequence;

    private static Query request(String... columns) {
        return Query.builder().limit(100).columns(columns).build();
    }

    private Path dataset(Consumer<FileMetaData> edit) throws Exception {
        Path root = TestDatasets.write(tmp.resolve("d" + sequence++), new TestDatasets.Spec(),
                List.of(List.of(TestDatasets.TestRow.of("a", 1), TestDatasets.TestRow.of("b", 2))));
        rewriteFooter(root, edit);
        return root;
    }

    private static void rewriteFooter(Path root, Consumer<FileMetaData> edit) throws Exception {
        rewriteFooter(root, edit, false);
    }

    private static void rewriteFooter(Path root, Consumer<FileMetaData> edit, boolean rawCodec) throws Exception {
        Path file = root.resolve("data/part-00000.parquet");
        byte[] bytes = Files.readAllBytes(file);
        int size = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt(bytes.length - 8);
        int start = bytes.length - size - 8;
        FileMetaData meta = Thrift.fileMetaData(Arrays.copyOfRange(bytes, start, start + size), Limits.DEFAULT.metadataMemory());
        edit.accept(meta);
        ByteArrayOutputStream footer = new ByteArrayOutputStream();
        if (rawCodec) {
            TCompactProtocol protocol = new TCompactProtocol(new TIOStreamTransport(footer)) {
                String field;
                @Override public void writeFieldBegin(TField f) throws shaded.parquet.org.apache.thrift.TException {
                    field = f.name;
                    super.writeFieldBegin(f);
                }
                @Override public void writeI32(int value) throws shaded.parquet.org.apache.thrift.TException {
                    super.writeI32("codec".equals(field) ? 99 : value);
                }
            };
            meta.write(protocol);
        } else Util.writeFileMetaData(meta, footer);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(bytes, 0, start);
        out.write(footer.toByteArray());
        out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footer.size()).array());
        out.write(new byte[] {'P', 'A', 'R', '1'});
        Files.write(file, out.toByteArray());
        Path manifest = root.resolve("manifest.json");
        Files.writeString(manifest, Files.readString(manifest).replace("\"bytes\":" + bytes.length, "\"bytes\":" + out.size()));
    }

    @Test
    void unknownValueEncodingAndCodecAreNeededOnlyWhenProjected() {
        for (String name : List.of("unknown-value-encoding-99", "unknown-codec-99")) {
            try (Dataset ds = Dataset.open(ConformanceFixturesTest.fixture("datasets/invalid/" + name))) {
                assertEquals(4, ds.rows(request()).size());
                assertEquals(UnsupportedCode.ENCODING,
                        assertThrows(UnsupportedFeatureException.class, () -> ds.rows(request("value"))).code());
            }
        }
    }

    @Test
    void neededKeyEncodingIsUnsupported() {
        try (Dataset ds = Dataset.open(ConformanceFixturesTest.fixture("datasets/invalid/unknown-key-encoding-99-v1"))) {
            assertEquals(UnsupportedCode.ENCODING, assertThrows(UnsupportedFeatureException.class, () -> ds.rows(request())).code());
        }
    }

    @Test
    void unknownKeyCodecIsUnsupportedAtReadButDoesNotBlockColumns() throws Exception {
        Path root = dataset(f -> {});
        rewriteFooter(root, f -> {}, true);
        try (Dataset ds = Dataset.open(root)) {
            assertEquals(2, ds.columns(0).size());
            assertEquals(UnsupportedCode.ENCODING, assertThrows(UnsupportedFeatureException.class, () -> ds.rows(request())).code());
        }
    }

    /** Write raw enum integers, bypassing generated setters/validate without changing any decoder. */
    private static byte[] rawHeader(boolean v2, int encoding, int definition, Integer repetition, boolean wrongType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TCompactProtocol p = new TCompactProtocol(new TIOStreamTransport(out));
        p.writeStructBegin(new TStruct("PageHeader"));
        integer(p, 1, v2 ? 3 : 0);
        integer(p, 2, 4);
        integer(p, 3, 4);
        p.writeFieldBegin(new TField("", (byte) 12, (short) (v2 ? 8 : 5)));
        p.writeStructBegin(new TStruct("DataPageHeader"));
        integer(p, 1, 1);
        if (v2) {
            integer(p, 2, 0); integer(p, 3, 1); integer(p, 4, encoding);
            integer(p, 5, 0); integer(p, 6, 0);
        } else {
            integer(p, 2, encoding); integer(p, 3, definition);
            if (repetition != null) {
                if (wrongType) {
                    p.writeFieldBegin(new TField("", (byte) 11, (short) 4));
                    p.writeString("99"); p.writeFieldEnd();
                } else integer(p, 4, repetition);
            }
        }
        p.writeFieldStop(); p.writeStructEnd(); p.writeFieldEnd();
        p.writeFieldStop(); p.writeStructEnd();
        return out.toByteArray();
    }

    private static void integer(TCompactProtocol p, int field, int value) throws Exception {
        p.writeFieldBegin(new TField("", (byte) 8, (short) field));
        p.writeI32(value); p.writeFieldEnd();
    }

    private static PageHeader header(byte[] bytes) throws DataException {
        return Thrift.pageHeader(bytes, 0, bytes.length, Limits.DEFAULT.metadataMemory()).value();
    }

    private static ColumnPage decode(PageHeader h, boolean optional) throws DataException {
        return PageDecoder.decodeData(h, ByteBuffer.wrap(new byte[4]), ColumnSpec.of("value", 0, Column.Type.INT32, 0, optional),
                CompressionCodec.UNCOMPRESSED, null, Limits.DEFAULT, "test", null, new Codecs.State());
    }

    @Test
    void unknownV2AndNeededV1DefinitionEncodingsAreUnsupported() throws Exception {
        assertEquals(UnsupportedCode.ENCODING,
                assertThrows(UnsupportedFeatureException.class, () -> decode(header(rawHeader(true, 99, 3, 3, false)), false)).code());
        assertEquals(UnsupportedCode.ENCODING,
                assertThrows(UnsupportedFeatureException.class, () -> decode(header(rawHeader(false, 0, 99, 3, false)), true)).code());
        // A required column needs neither definition nor repetition levels, but their fields must exist.
        assertEquals(1, decode(header(rawHeader(false, 0, 99, 99, false)), false).rows);
    }

    @Test
    void ignoredEnumsKeepTheirRequiredPresenceAndWireTypes() throws Exception {
        try (Dataset ds = Dataset.open(ConformanceFixturesTest.fixture("datasets/ignored-enums-99"))) {
            assertEquals(4, ds.rows(request("value")).size());
        }
        assertThrows(DataException.class, () -> header(rawHeader(false, 0, 3, null, false)));
        assertThrows(DataException.class, () -> header(rawHeader(false, 0, 3, 99, true)));
        for (var entry : List.of(new String[] {"missing-boundary-order", "INVALID_PAGE_INDEX"},
                new String[] {"missing-repetition-encoding-value", "INVALID_PARQUET"})) {
            try (Dataset ds = Dataset.open(ConformanceFixturesTest.fixture("datasets/invalid/" + entry[0]))) {
                assertEquals(RefusalCode.valueOf(entry[1]), assertThrows(RefusalException.class, () -> ds.rows(request("value"))).code());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"format_version", "draft_revision", "schema_id", "key_column", "key_unique"})
    void valuelessListedStampsAndDuplicatesAreStampMismatch(String suffix) throws Exception {
        for (boolean duplicate : List.of(false, true)) {
            Path root = dataset(f -> {
                List<KeyValue> kvs = new ArrayList<>(f.getKey_value_metadata());
                if (duplicate) kvs.add(new KeyValue("seekparquet." + suffix));
                else kvs.stream().filter(k -> k.getKey().equals("seekparquet." + suffix)).findFirst().orElseThrow().unsetValue();
                f.setKey_value_metadata(kvs);
            });
            try (Dataset ds = Dataset.open(root)) {
                assertEquals(RefusalCode.STAMP_MISMATCH, assertThrows(RefusalException.class, () -> ds.rows(request())).code());
            }
        }
    }

    @Test
    void valuelessOrdinaryAndUnknownSeekparquetMetadataAreIgnored() {
        try (Dataset ds = Dataset.open(ConformanceFixturesTest.fixture("datasets/metadata-no-value"))) {
            assertEquals(4, ds.rows(request("value")).size());
        }
    }

    private static org.apache.parquet.format.LogicalType decimal(int precision, int scale) {
        return org.apache.parquet.format.LogicalType.DECIMAL(new DecimalType(scale, precision));
    }

    private static SchemaElement leaf(Type type, int length, org.apache.parquet.format.LogicalType logical) {
        SchemaElement se = new SchemaElement("size").setType(type).setRepetition_type(FieldRepetitionType.OPTIONAL);
        if (length > 0) se.setType_length(length);
        if (logical != null) se.setLogicalType(logical);
        return se;
    }

    static Stream<Arguments> badAnnotations() {
        var millis = org.apache.parquet.format.TimeUnit.MILLIS(new MilliSeconds());
        var micros = org.apache.parquet.format.TimeUnit.MICROS(new MicroSeconds());
        List<SchemaElement> cases = new ArrayList<>(List.of(
                leaf(Type.INT64, 0, org.apache.parquet.format.LogicalType.DATE(new DateType())),
                leaf(Type.INT32, 0, org.apache.parquet.format.LogicalType.STRING(new StringType())),
                leaf(Type.FIXED_LEN_BYTE_ARRAY, 15, org.apache.parquet.format.LogicalType.UUID(new UUIDType())),
                leaf(Type.FIXED_LEN_BYTE_ARRAY, 1, org.apache.parquet.format.LogicalType.FLOAT16(new Float16Type())),
                leaf(Type.INT64, 0, decimal(0, 0)), leaf(Type.INT64, 0, decimal(1, -1)), leaf(Type.INT64, 0, decimal(1, 2)),
                leaf(Type.INT32, 0, decimal(10, 0)), leaf(Type.INT64, 0, decimal(19, 0)),
                leaf(Type.FIXED_LEN_BYTE_ARRAY, 1, decimal(3, 0)),
                leaf(Type.INT64, 0, org.apache.parquet.format.LogicalType.TIME(new TimeType(false, millis))),
                leaf(Type.INT32, 0, org.apache.parquet.format.LogicalType.TIME(new TimeType(false, micros))),
                leaf(Type.INT96, 0, org.apache.parquet.format.LogicalType.TIMESTAMP(new TimestampType(false, micros))),
                leaf(Type.FIXED_LEN_BYTE_ARRAY, 11, org.apache.parquet.format.LogicalType.TIMESTAMP(new TimestampType(false, micros))),
                leaf(Type.INT32, 0, org.apache.parquet.format.LogicalType.INTEGER(new IntType((byte) 7, true))),
                leaf(Type.INT32, 0, org.apache.parquet.format.LogicalType.INTEGER(new IntType((byte) 64, true))),
                leaf(Type.INT64, 0, org.apache.parquet.format.LogicalType.MAP(new MapType())),
                leaf(Type.INT64, 0, org.apache.parquet.format.LogicalType.LIST(new ListType())),
                leaf(Type.INT64, 0, org.apache.parquet.format.LogicalType.VARIANT(new VariantType())),
                leaf(Type.INT32, 0, org.apache.parquet.format.LogicalType.JSON(new JsonType())),
                leaf(Type.INT32, 0, org.apache.parquet.format.LogicalType.BSON(new BsonType())),
                leaf(Type.INT32, 0, org.apache.parquet.format.LogicalType.ENUM(new EnumType()))));
        for (ConvertedType ct : ConvertedType.values()) {
            Type invalid = switch (ct) {
                case UTF8, ENUM, JSON, BSON -> Type.INT32;
                case DATE, TIME_MILLIS, UINT_8, UINT_16, UINT_32, INT_8, INT_16, INT_32 -> Type.INT64;
                case TIME_MICROS, TIMESTAMP_MILLIS, TIMESTAMP_MICROS, UINT_64, INT_64 -> Type.INT32;
                case INTERVAL -> Type.FIXED_LEN_BYTE_ARRAY;
                default -> Type.INT64;
            };
            SchemaElement se = leaf(invalid, invalid == Type.FIXED_LEN_BYTE_ARRAY ? 11 : 0, null).setConverted_type(ct);
            if (ct == ConvertedType.DECIMAL) se.setPrecision(19).setScale(0);
            cases.add(se);
        }
        return cases.stream().map(se -> Arguments.of(se.toString(), se));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("badAnnotations")
    void everyUnprojectedLeafIsCheckedAtTouch(String description, SchemaElement bad) throws Exception {
        Path root = dataset(f -> {
            f.getSchema().set(2, bad);
            // Keep chunk metadata consistent, so the annotation is the only fault.
            f.getRow_groups().getFirst().getColumns().get(1).getMeta_data().setType(bad.getType());
        });
        try (Dataset ds = Dataset.open(root)) {
            assertEquals(RefusalCode.INVALID_FOOTER, assertThrows(RefusalException.class, () -> ds.rows(request()), description).code());
        }
    }

    /** Serialize union alternatives the pinned generated Java classes cannot express. */
    private static final class RawAnnotation extends SchemaElement {
        final int member;
        final int unit; // -3: wrong wire type for MILLIS; -2: no temporal parameters; -1: missing unit; 0: empty; 99: future alternative
        RawAnnotation(int member, int unit) {
            super("size");
            setType(Type.INT64);
            setRepetition_type(FieldRepetitionType.OPTIONAL);
            this.member = member;
            this.unit = unit;
        }
        @Override
        public void write(shaded.parquet.org.apache.thrift.protocol.TProtocol p) throws shaded.parquet.org.apache.thrift.TException {
            p.writeStructBegin(new TStruct("SchemaElement"));
            writeInt(p, 1, Type.INT64.getValue());
            writeInt(p, 3, FieldRepetitionType.OPTIONAL.getValue());
            p.writeFieldBegin(new TField("name", (byte) 11, (short) 4)); p.writeString("size"); p.writeFieldEnd();
            p.writeFieldBegin(new TField("logicalType", (byte) 12, (short) 10));
            p.writeStructBegin(new TStruct("LogicalType"));
            p.writeFieldBegin(new TField("", (byte) 12, (short) member));
            p.writeStructBegin(new TStruct("Annotation"));
            if (unit != -2) {
                p.writeFieldBegin(new TField("", (byte) 2, (short) 1)); p.writeBool(false); p.writeFieldEnd();
                if (unit != -1) {
                    p.writeFieldBegin(new TField("", (byte) 12, (short) 2));
                    p.writeStructBegin(new TStruct("TimeUnit"));
                    if (unit == -3) {
                        writeInt(p, 1, 0); // recognized MILLIS alternative requires a struct
                    } else if (unit != 0) {
                        p.writeFieldBegin(new TField("", (byte) 12, (short) unit));
                        p.writeStructBegin(new TStruct("FutureUnit")); p.writeFieldStop(); p.writeStructEnd(); p.writeFieldEnd();
                    }
                    p.writeFieldStop(); p.writeStructEnd(); p.writeFieldEnd();
                }
            }
            p.writeFieldStop(); p.writeStructEnd(); p.writeFieldEnd();
            p.writeFieldStop(); p.writeStructEnd(); p.writeFieldEnd();
            p.writeFieldStop(); p.writeStructEnd();
        }
    }

    private static void writeInt(shaded.parquet.org.apache.thrift.protocol.TProtocol p, int id, int value)
            throws shaded.parquet.org.apache.thrift.TException {
        p.writeFieldBegin(new TField("", (byte) 8, (short) id)); p.writeI32(value); p.writeFieldEnd();
    }

    @ParameterizedTest
    @ValueSource(ints = {19, 20})
    void newerGroupOnlyAnnotationsAreInvalidOnLeaves(int member) throws Exception {
        Path root = dataset(f -> f.getSchema().set(2, new RawAnnotation(member, -2)));
        try (Dataset ds = Dataset.open(root)) {
            assertEquals(RefusalCode.INVALID_FOOTER, assertThrows(RefusalException.class, () -> ds.rows(request())).code());
        }
    }

    @Test
    void missingAndEmptyTimeUnitsDifferFromUnknownAlternatives() throws Exception {
        for (int member : new int[] {7, 8}) {
            for (int unit : new int[] {-3, -1, 0, 99}) {
                Path root = dataset(f -> f.getSchema().set(2, new RawAnnotation(member, unit)));
                try (Dataset ds = Dataset.open(root)) {
                    if (unit == 99) {
                        assertTrue(ds.columns(0).get(1).logicalType().isEmpty());
                        assertEquals(2, ds.rows(request("size")).size());
                    } else assertEquals(RefusalCode.INVALID_FOOTER,
                            assertThrows(RefusalException.class, () -> ds.rows(request())).code());
                }
            }
        }
    }

    @Test
    void nestedUnprojectedLeafIsAlsoChecked() throws Exception {
        Path root = dataset(f -> {
            f.getSchema().get(2).setName("child").setLogicalType(org.apache.parquet.format.LogicalType.DATE(new DateType()));
            f.getSchema().add(2, new SchemaElement("size").setRepetition_type(FieldRepetitionType.OPTIONAL).setNum_children(1));
            f.getRow_groups().getFirst().getColumns().get(1).getMeta_data().setPath_in_schema(List.of("size", "child"));
        });
        try (Dataset ds = Dataset.open(root)) {
            assertEquals(RefusalCode.INVALID_FOOTER, assertThrows(RefusalException.class, () -> ds.rows(request())).code());
        }
    }

    @Test
    void legalCarriersAndUnknownAnnotationsKeepPhysicalValuesAndColumnReporting() {
        try (Dataset ds = Dataset.open(ConformanceFixturesTest.fixture("datasets/annotation-physical-fallback-controls"))) {
            List<Column> columns = ds.columns(0);
            assertTrue(columns.stream().filter(c -> c.name().startsWith("future_")).allMatch(c -> c.logicalType().isEmpty()));
            assertEquals(4, ds.rows(request(columns.stream().map(Column::name).toArray(String[]::new))).size());
        }
    }

    @Test
    void convertedDecimalDefaultsScaleToZeroAndLegalCounterpartsPass() throws Exception {
        for (SchemaElement se : List.of(
                leaf(Type.INT64, 0, null).setConverted_type(ConvertedType.DECIMAL).setPrecision(18),
                leaf(Type.BYTE_ARRAY, 0, decimal(200, 100)),
                leaf(Type.INT32, 0, org.apache.parquet.format.LogicalType.TIME(
                        new TimeType(false, org.apache.parquet.format.TimeUnit.MILLIS(new MilliSeconds())))),
                leaf(Type.INT64, 0, org.apache.parquet.format.LogicalType.TIME(
                        new TimeType(false, org.apache.parquet.format.TimeUnit.NANOS(new NanoSeconds())))),
                leaf(Type.INT32, 0, org.apache.parquet.format.LogicalType.INTEGER(new IntType((byte) 8, true))),
                leaf(Type.INT64, 0, org.apache.parquet.format.LogicalType.INTEGER(new IntType((byte) 64, false))),
                leaf(Type.FIXED_LEN_BYTE_ARRAY, 12, null).setConverted_type(ConvertedType.INTERVAL))) {
            Path root = dataset(f -> {
                f.getSchema().set(2, se);
                f.getRow_groups().getFirst().getColumns().get(1).getMeta_data().setType(se.getType());
            });
            try (Dataset ds = Dataset.open(root)) {
                assertEquals(2, ds.rows(request()).size()); // tests footer validity independently of value decoding
            }
        }
    }

    private Path indexedHeader(Consumer<PageHeader> edit) throws Exception {
        Path root = TestDatasets.write(tmp.resolve("h" + sequence++), new TestDatasets.Spec(),
                List.of(List.of(TestDatasets.TestRow.of("a", 1), TestDatasets.TestRow.of("b", 2),
                        TestDatasets.TestRow.of("c", 3), TestDatasets.TestRow.of("d", 4))));
        Path file = root.resolve("data/part-00000.parquet");
        byte[] bytes = Files.readAllBytes(file);
        PageHeader h = Thrift.pageHeader(bytes, 4, bytes.length - 4, Limits.DEFAULT.metadataMemory()).value();
        edit.accept(h);
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        Util.writePageHeader(h, header);
        // Header corruption is checked before the old indexed extent; no payload is decoded here.
        System.arraycopy(header.toByteArray(), 0, bytes, 4, header.size());
        Files.write(file, bytes);
        return root;
    }

    @Test
    void indexedV2CorruptionAndValidExtentOrTypeDisagreementHaveDistinctCodes() throws Exception {
        List<Consumer<PageHeader>> corrupt = List.of(
                h -> h.setCompressed_page_size(-1), h -> h.setUncompressed_page_size(-1),
                h -> { h.setType(PageType.DATA_PAGE_V2); h.unsetData_page_header();
                    h.setData_page_header_v2(new DataPageHeaderV2(-5, 0, 3, Encoding.PLAIN, 0, 0)); },
                h -> { h.setType(PageType.DATA_PAGE_V2); h.unsetData_page_header();
                    h.setData_page_header_v2(new DataPageHeaderV2(3, 0, 3, Encoding.PLAIN, -1, 0)); });
        for (Consumer<PageHeader> edit : corrupt) {
            try (Dataset ds = Dataset.open(indexedHeader(edit))) {
                assertEquals(RefusalCode.INVALID_PARQUET, assertThrows(RefusalException.class, () -> ds.rows(request())).code());
            }
        }
        for (Consumer<PageHeader> edit : List.<Consumer<PageHeader>>of(
                h -> h.setCompressed_page_size(h.getCompressed_page_size() + 1),
                h -> h.setCompressed_page_size(h.getCompressed_page_size() - 1),
                h -> h.setCompressed_page_size(1_000_000),
                h -> h.setType(PageType.INDEX_PAGE))) {
            try (Dataset ds = Dataset.open(indexedHeader(edit))) {
                assertEquals(RefusalCode.INVALID_PAGE_INDEX, assertThrows(RefusalException.class, () -> ds.rows(request())).code());
            }
        }
    }

    @Test
    void indexedHeaderIntrinsicCorruptionPrecedesIndexDisagreement() throws Exception {
        for (String name : List.of("key-header-negative-num-values-v1", "key-index-valid-row-count-mismatch", "key-index-undecodable-header")) {
            RefusalCode expected = name.contains("negative") ? RefusalCode.INVALID_PARQUET : RefusalCode.INVALID_PAGE_INDEX;
            try (Dataset ds = Dataset.open(ConformanceFixturesTest.fixture("datasets/invalid/" + name))) {
                assertEquals(expected, assertThrows(RefusalException.class, () -> ds.rows(request())).code());
            }
        }
        for (Consumer<PageHeader> corrupt : List.<Consumer<PageHeader>>of(
                h -> h.setCompressed_page_size(-1), h -> h.setUncompressed_page_size(-1),
                h -> h.getData_page_header_v2().setNum_values(-1), h -> h.getData_page_header_v2().setNum_rows(-1),
                h -> h.getData_page_header_v2().setNum_nulls(-1), h -> h.getData_page_header_v2().setDefinition_levels_byte_length(-1),
                h -> h.getData_page_header_v2().setRepetition_levels_byte_length(5))) {
            PageHeader h = header(rawHeader(true, 0, 3, 3, false));
            corrupt.accept(h);
            assertThrows(DataException.class, () -> PageDecoder.checkHeader(h));
        }
    }
}
