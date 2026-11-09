// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;
import java.util.Optional;

/**
 * A column's Parquet logical type: the {@code logical_type} annotation of its schema element, or else
 * its legacy {@code converted_type} normalized into one as Parquet's LogicalTypes.md maps it. Values are
 * still reported as their physical type ({@link Value}); the logical type says how to interpret them.
 */
public sealed interface LogicalType {

    /** A time unit of {@link TimeType} and {@link TimestampType}. */
    enum TimeUnit {
        /** Milliseconds. */
        MILLIS,
        /** Microseconds. */
        MICROS,
        /** Nanoseconds. */
        NANOS
    }

    /** {@code STRING}: UTF-8 text (legacy {@code UTF8}). */
    record StringType() implements LogicalType {}

    /** {@code ENUM}. */
    record EnumType() implements LogicalType {}

    /** {@code JSON}. */
    record JsonType() implements LogicalType {}

    /** {@code BSON}. */
    record BsonType() implements LogicalType {}

    /** {@code UUID}. */
    record UuidType() implements LogicalType {}

    /** {@code FLOAT16}. */
    record Float16Type() implements LogicalType {}

    /** {@code DATE}. */
    record DateType() implements LogicalType {}

    /** {@code UNKNOWN}: a column whose values are always null. */
    record UnknownType() implements LogicalType {}

    /** {@code MAP}, which annotates a group. */
    record MapType() implements LogicalType {}

    /** {@code LIST}, which annotates a group. */
    record ListType() implements LogicalType {}

    /**
     * {@code VARIANT}, which annotates a group.
     *
     * @param specificationVersion the variant specification version, when declared
     */
    record VariantType(Optional<Integer> specificationVersion) implements LogicalType {
        /**
         * Creates a variant type.
         *
         * @param specificationVersion the specification version
         */
        public VariantType {
            Objects.requireNonNull(specificationVersion, "specificationVersion");
        }
    }

    /**
     * {@code TIME} (legacy {@code TIME_MILLIS} and {@code TIME_MICROS}, adjusted to UTC).
     *
     * @param unit the unit
     * @param adjustedToUtc whether the time is adjusted to UTC
     */
    record TimeType(TimeUnit unit, boolean adjustedToUtc) implements LogicalType {
        /**
         * Creates a time type.
         *
         * @param unit the unit
         * @param adjustedToUtc whether adjusted to UTC
         */
        public TimeType {
            Objects.requireNonNull(unit, "unit");
        }
    }

    /**
     * {@code TIMESTAMP} (legacy {@code TIMESTAMP_MILLIS} and {@code TIMESTAMP_MICROS}, adjusted to UTC).
     *
     * @param unit the unit
     * @param adjustedToUtc whether the instant is adjusted to UTC
     */
    record TimestampType(TimeUnit unit, boolean adjustedToUtc) implements LogicalType {
        /**
         * Creates a timestamp type.
         *
         * @param unit the unit
         * @param adjustedToUtc whether adjusted to UTC
         */
        public TimestampType {
            Objects.requireNonNull(unit, "unit");
        }
    }

    /**
     * {@code INTEGER} (legacy {@code INT_8} to {@code INT_64} and {@code UINT_8} to {@code UINT_64}).
     *
     * @param bitWidth 8, 16, 32 or 64
     * @param signed whether the integer is signed
     */
    record IntType(int bitWidth, boolean signed) implements LogicalType {}

    /**
     * {@code DECIMAL} (legacy {@code DECIMAL}, with the schema element's precision and scale).
     *
     * @param precision the number of digits
     * @param scale the digits after the decimal point
     */
    record DecimalType(int precision, int scale) implements LogicalType {}

    /**
     * {@code GEOMETRY}.
     *
     * @param crs the coordinate reference system, when declared
     */
    record GeometryType(Optional<String> crs) implements LogicalType {
        /**
         * Creates a geometry type.
         *
         * @param crs the coordinate reference system
         */
        public GeometryType {
            Objects.requireNonNull(crs, "crs");
        }
    }

    /**
     * {@code GEOGRAPHY}.
     *
     * @param crs the coordinate reference system, when declared
     * @param algorithm the edge interpolation algorithm's Parquet name ({@code SPHERICAL}, ...), when
     *     declared
     */
    record GeographyType(Optional<String> crs, Optional<String> algorithm) implements LogicalType {
        /**
         * Creates a geography type.
         *
         * @param crs the coordinate reference system
         * @param algorithm the edge interpolation algorithm
         */
        public GeographyType {
            Objects.requireNonNull(crs, "crs");
            Objects.requireNonNull(algorithm, "algorithm");
        }
    }
}
