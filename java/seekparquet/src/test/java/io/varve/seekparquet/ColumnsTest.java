// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

/** Typed columns of one part, from the footer the dataset checked and holds for it. */
class ColumnsTest {

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
}
