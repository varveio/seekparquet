// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.TimeUnit;
import org.apache.parquet.format.TimestampType;
import org.apache.parquet.format.MicroSeconds;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.protocol.TField;
import shaded.parquet.org.apache.thrift.protocol.TStruct;
import shaded.parquet.org.apache.thrift.transport.TMemoryBuffer;

/** Typed columns of one part, from the footer the dataset checked and holds for it. */
class ColumnsTest {

    @TempDir Path tmp;

    /** The annotated fixture's columns, as its generator declares them; columns(int) checks the part. */
    @Test
    void annotated() {
        LogicalType ms = new LogicalType.TimestampType(LogicalType.TimeUnit.MILLIS, false);
        LogicalType us = new LogicalType.TimestampType(LogicalType.TimeUnit.MICROS, true);
        List<Column> want = List.of(
                new Column("key", Column.Type.BYTE_ARRAY, 0, Optional.empty(), Column.Repetition.REQUIRED, true, 1),
                new Column("d", Column.Type.INT32, 0, Optional.of(new LogicalType.DateType()), Column.Repetition.OPTIONAL, false, 1),
                new Column("ts_ms", Column.Type.INT64, 0, Optional.of(ms), Column.Repetition.OPTIONAL, false, 1),
                new Column("ts_utc", Column.Type.INT64, 0, Optional.of(us), Column.Repetition.OPTIONAL, false, 1),
                new Column("dec9", Column.Type.INT32, 0, Optional.of(new LogicalType.DecimalType(9, 2)), Column.Repetition.OPTIONAL, false, 1),
                new Column("dec18", Column.Type.INT64, 0, Optional.of(new LogicalType.DecimalType(18, 4)), Column.Repetition.OPTIONAL, false, 1),
                new Column("dec30", Column.Type.FIXED_LEN_BYTE_ARRAY, 13, Optional.of(new LogicalType.DecimalType(30, 5)),
                        Column.Repetition.OPTIONAL, false, 1),
                new Column("s", Column.Type.BYTE_ARRAY, 0, Optional.of(new LogicalType.StringType()), Column.Repetition.OPTIONAL, false, 1));
        try (Dataset d = Dataset.open(ConformanceFixturesTest.fixture("datasets/annotated"))) {
            assertEquals(List.of(), d.columns(), "no part is checked yet");
            assertEquals(want, d.columns(0));
            assertNotNull(d.parts().checked(0), "columns(int) answered without checking the part");
            assertEquals(want, d.columns(), "the checked part, as columns() reports it");
            d.verify();
            assertEquals(want, d.columns(0));
            assertThrows(IndexOutOfBoundsException.class, () -> d.columns(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> d.columns(1));
            d.close();
            assertThrows(IllegalStateException.class, () -> d.columns(0));
        }
    }

    /** Each legacy converted type is normalized into the logical type it stands for. */
    @Test
    void convertedTypes() {
        record Case(Type type, ConvertedType converted, LogicalType logical) {}
        LogicalType.TimeUnit ms = LogicalType.TimeUnit.MILLIS;
        LogicalType.TimeUnit us = LogicalType.TimeUnit.MICROS;
        for (Case c : List.of(
                new Case(Type.BYTE_ARRAY, ConvertedType.UTF8, new LogicalType.StringType()),
                new Case(Type.BYTE_ARRAY, ConvertedType.ENUM, new LogicalType.EnumType()),
                new Case(Type.BYTE_ARRAY, ConvertedType.JSON, new LogicalType.JsonType()),
                new Case(Type.BYTE_ARRAY, ConvertedType.BSON, new LogicalType.BsonType()),
                new Case(Type.INT32, ConvertedType.DATE, new LogicalType.DateType()),
                new Case(Type.INT32, ConvertedType.TIME_MILLIS, new LogicalType.TimeType(ms, true)),
                new Case(Type.INT64, ConvertedType.TIME_MICROS, new LogicalType.TimeType(us, true)),
                new Case(Type.INT64, ConvertedType.TIMESTAMP_MILLIS, new LogicalType.TimestampType(ms, true)),
                new Case(Type.INT64, ConvertedType.TIMESTAMP_MICROS, new LogicalType.TimestampType(us, true)),
                new Case(Type.INT32, ConvertedType.INT_8, new LogicalType.IntType(8, true)),
                new Case(Type.INT32, ConvertedType.INT_16, new LogicalType.IntType(16, true)),
                new Case(Type.INT32, ConvertedType.INT_32, new LogicalType.IntType(32, true)),
                new Case(Type.INT64, ConvertedType.INT_64, new LogicalType.IntType(64, true)),
                new Case(Type.INT32, ConvertedType.UINT_8, new LogicalType.IntType(8, false)),
                new Case(Type.INT32, ConvertedType.UINT_16, new LogicalType.IntType(16, false)),
                new Case(Type.INT32, ConvertedType.UINT_32, new LogicalType.IntType(32, false)),
                new Case(Type.INT64, ConvertedType.UINT_64, new LogicalType.IntType(64, false)),
                new Case(Type.INT64, ConvertedType.DECIMAL, new LogicalType.DecimalType(18, 4)),
                new Case(Type.FIXED_LEN_BYTE_ARRAY, ConvertedType.INTERVAL, null))) {
            SchemaElement se = new SchemaElement("v").setType(c.type()).setRepetition_type(FieldRepetitionType.OPTIONAL)
                    .setConverted_type(c.converted());
            if (c.converted() == ConvertedType.DECIMAL) {
                se.setPrecision(18).setScale(4);
            }
            assertEquals(Optional.ofNullable(c.logical()), PartSchema.logicalType(se), c.converted().toString());
        }
        // The logical type wins over a converted type.
        SchemaElement both = new SchemaElement("v").setType(Type.INT64).setRepetition_type(FieldRepetitionType.OPTIONAL)
                .setConverted_type(ConvertedType.TIMESTAMP_MICROS)
                .setLogicalType(org.apache.parquet.format.LogicalType.TIMESTAMP(new TimestampType(false, TimeUnit.MICROS(new MicroSeconds()))));
        assertEquals(Optional.of(new LogicalType.TimestampType(us, false)), PartSchema.logicalType(both));
    }

    /**
     * A logical_type union whose member this parquet-format does not know (a newer annotation) decodes
     * with no set field; the converted type then stands for it, as in Go, and without one there is none.
     */
    @Test
    void unknownLogicalTypeMember() throws Exception {
        TMemoryBuffer buffer = new TMemoryBuffer(64);
        TCompactProtocol out = new TCompactProtocol(buffer);
        out.writeStructBegin(new TStruct("LogicalType"));
        out.writeFieldBegin(new TField("future", (byte) 12 /* TType.STRUCT */, (short) 40));
        out.writeStructBegin(new TStruct("Future"));
        out.writeFieldStop();
        out.writeStructEnd();
        out.writeFieldEnd();
        out.writeFieldStop();
        out.writeStructEnd();
        byte[] raw = Arrays.copyOf(buffer.getArray(), buffer.length());
        org.apache.parquet.format.LogicalType lt = Thrift.read(raw, 0, raw.length, new org.apache.parquet.format.LogicalType()).value();
        assertNull(lt.getSetField(), "the unknown member leaves the union unset");
        SchemaElement se = new SchemaElement("v").setType(Type.BYTE_ARRAY).setRepetition_type(FieldRepetitionType.OPTIONAL)
                .setLogicalType(lt);
        assertEquals(Optional.empty(), PartSchema.logicalType(se));
        se.setConverted_type(ConvertedType.UTF8);
        assertEquals(Optional.of(new LogicalType.StringType()), PartSchema.logicalType(se), "the converted type stands for it");
    }

    /**
     * The annotated fixture with column d's logical_type member changed from DATE (id 6) to the unused id
     * 9: the part opens and reads, and its columns report d's converted type, DATE, as Go's do.
     */
    @Test
    void unknownLogicalTypeInAFooter() throws IOException {
        Path src = ConformanceFixturesTest.fixture("datasets/annotated");
        Path root = tmp.resolve("annotated");
        Files.createDirectories(root.resolve("data"));
        Files.copy(src.resolve("manifest.json"), root.resolve("manifest.json"));
        byte[] file = Files.readAllBytes(src.resolve("data/part-00000.parquet"));
        int footer = file.length - 8 - ByteBuffer.wrap(file, file.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        int at = -1;
        for (int i = footer; i + 2 < file.length - 8 && at < 0; i++) {
            // The schema element named "d" (field 4, binary, length 1), then its logical type's DATE
            // member, an empty struct (field 6), and the union's stop.
            if (file[i] == 0x18 && file[i + 1] == 0x01 && file[i + 2] == 'd') {
                for (int j = i + 3; j + 2 < file.length - 8; j++) {
                    if (file[j] == 0x6C && file[j + 1] == 0 && file[j + 2] == 0) {
                        at = j;
                        break;
                    }
                }
            }
        }
        assertTrue(at > 0, "the DATE member of column d");
        file[at] = (byte) 0x9C; // field id 6 -> 9, same length
        Files.write(root.resolve("data/part-00000.parquet"), file);
        try (Dataset d = Dataset.open(root)) {
            assertEquals(8, d.range(RangeQuery.builder().limit(100).columns("d").build()).size(), "the part reads");
            Column column = d.columns(0).get(1);
            assertEquals("d", column.name());
            assertEquals(Optional.of(new LogicalType.DateType()), column.logicalType());
            assertEquals(Optional.of(new LogicalType.DateType()), d.column("d").orElseThrow().logicalType());
        }
    }
}
