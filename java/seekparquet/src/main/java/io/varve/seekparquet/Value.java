// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;

/**
 * One projected value of a row: absent, null, or a physical Parquet value.
 *
 * <p>Values are <em>physical</em>: logical annotations (string, decimal, date, timestamp, unsigned)
 * are not applied, so an {@code INT32} annotated as unsigned is its signed 32-bit value. Floating
 * point values keep their exact bits, NaN payloads and the sign of zero included.
 *
 * <p>{@link Absent} (the row's part has no such column, COL-2) is distinct from {@link Null} (the
 * part has the column and the row holds no value). The alternatives are a closed set, for use with
 * {@code switch} patterns:
 *
 * <pre>{@code
 * String text = switch (value) {
 *     case Value.Absent a -> "absent";
 *     case Value.Null n -> "null";
 *     case Value.Int64 v -> Long.toString(v.value());
 *     default -> value.toString();
 * };
 * }</pre>
 */
public sealed interface Value {

    /** The row's part has no such column (COL-2). */
    record Absent() implements Value {
        /** A shared instance; every instance is equal to it, so compare with {@code equals} or a pattern, not {@code ==}. */
        public static final Absent INSTANCE = new Absent();
    }

    /** The part has the column; this row holds no value. */
    record Null() implements Value {
        /** A shared instance; every instance is equal to it, so compare with {@code equals} or a pattern, not {@code ==}. */
        public static final Null INSTANCE = new Null();
    }

    /**
     * A {@code BOOLEAN} value.
     *
     * @param value the value
     */
    record Bool(boolean value) implements Value {}

    /**
     * An {@code INT32} value.
     *
     * @param value the value
     */
    record Int32(int value) implements Value {}

    /**
     * An {@code INT64} value.
     *
     * @param value the value
     */
    record Int64(long value) implements Value {}

    /**
     * A {@code FLOAT} value, held as its exact IEEE 754 bits.
     *
     * @param bits the bits, as {@link Float#floatToRawIntBits}
     */
    record Float32(int bits) implements Value {
        /** {@return the value as a {@code float}} */
        public float value() {
            return Float.intBitsToFloat(bits);
        }
    }

    /**
     * A {@code DOUBLE} value, held as its exact IEEE 754 bits.
     *
     * @param bits the bits, as {@link Double#doubleToRawLongBits}
     */
    record Float64(long bits) implements Value {
        /** {@return the value as a {@code double}} */
        public double value() {
            return Double.longBitsToDouble(bits);
        }
    }

    /**
     * A legacy {@code INT96} value, as its raw bits: three little-endian 32-bit words, least significant
     * first, each {@code int} carrying unsigned 32-bit bits. It is not interpreted (as a timestamp or
     * otherwise). The Go reader's {@code Int96}.
     *
     * @param word0 bits 0 to 31, the value's first 4 bytes read little-endian
     * @param word1 bits 32 to 63
     * @param word2 bits 64 to 95
     */
    record Int96(int word0, int word1, int word2) implements Value {
        /**
         * Returns the value whose 12 little-endian bytes are {@code b}.
         *
         * @param b the bytes, exactly 12
         * @return the value
         * @throws IllegalArgumentException if {@code b} is not 12 bytes
         */
        public static Int96 of(byte[] b) {
            if (b.length != 12) {
                throw new IllegalArgumentException("an INT96 value is 12 bytes, not " + b.length);
            }
            java.nio.ByteBuffer le = java.nio.ByteBuffer.wrap(b).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            return new Int96(le.getInt(0), le.getInt(4), le.getInt(8));
        }

        /** {@return the value's 12 little-endian bytes, a copy} */
        public byte[] toByteArray() {
            return java.nio.ByteBuffer.allocate(12).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(word0).putInt(word1).putInt(word2).array();
        }
    }

    /**
     * A {@code BYTE_ARRAY} value.
     *
     * @param value the bytes
     */
    record ByteArray(Bytes value) implements Value {
        /**
         * Creates the value.
         *
         * @param value the bytes
         */
        public ByteArray {
            Objects.requireNonNull(value, "value");
        }
    }

    /**
     * A {@code FIXED_LEN_BYTE_ARRAY} value.
     *
     * @param value the bytes, as many as the column's type length
     */
    record FixedLenByteArray(Bytes value) implements Value {
        /**
         * Creates the value.
         *
         * @param value the bytes
         */
        public FixedLenByteArray {
            Objects.requireNonNull(value, "value");
        }
    }

    /** {@return {@link Absent#INSTANCE}} */
    static Value absent() {
        return Absent.INSTANCE;
    }

    /** {@return {@link Null#INSTANCE}} */
    static Value nullValue() {
        return Null.INSTANCE;
    }

    /** {@return whether this is {@link Absent}} */
    default boolean isAbsent() {
        return this instanceof Absent;
    }

    /** {@return whether this is {@link Null}. An absent value is not null} */
    default boolean isNull() {
        return this instanceof Null;
    }
}
