// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * A top-level, non-repeated leaf column of one part, as the decoder needs it.
 *
 * @param name the column name
 * @param leaf the leaf column index in the part's schema
 * @param type the physical type
 * @param typeLength the type length of a fixed-length byte array, 0 otherwise
 * @param optional whether the column is OPTIONAL (one definition level) rather than REQUIRED
 * @param descriptor the column's descriptor for parquet-java's decoders
 */
record ColumnSpec(String name, int leaf, Column.Type type, int typeLength, boolean optional, ColumnDescriptor descriptor) {

    static ColumnSpec of(String name, int leaf, Column.Type type, int typeLength, boolean optional) {
        PrimitiveType.PrimitiveTypeName ptn = switch (type) {
            case BOOLEAN -> PrimitiveType.PrimitiveTypeName.BOOLEAN;
            case INT32 -> PrimitiveType.PrimitiveTypeName.INT32;
            case INT64 -> PrimitiveType.PrimitiveTypeName.INT64;
            case INT96 -> PrimitiveType.PrimitiveTypeName.INT96;
            case FLOAT -> PrimitiveType.PrimitiveTypeName.FLOAT;
            case DOUBLE -> PrimitiveType.PrimitiveTypeName.DOUBLE;
            case BYTE_ARRAY -> PrimitiveType.PrimitiveTypeName.BINARY;
            case FIXED_LEN_BYTE_ARRAY -> PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
            case GROUP -> throw new IllegalArgumentException("a group is not a leaf");
        };
        Type.Repetition repetition = optional ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
        PrimitiveType primitive = new PrimitiveType(repetition, ptn, typeLength, name);
        ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {name}, primitive, 0, optional ? 1 : 0);
        return new ColumnSpec(name, leaf, type, typeLength, optional, descriptor);
    }
}
