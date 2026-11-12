// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.SchemaElement;

/**
 * A part's schema, as far as the profile needs it: its top-level fields with their physical shape
 * (COL-5), and its leaf columns.
 */
final class PartSchema {

    /** The deepest nesting accepted. */
    static final int MAX_NESTING = 256;

    /**
     * A top-level field.
     *
     * @param name the field name
     * @param element the schema element
     * @param type the physical type, or GROUP
     * @param typeLength the FIXED_LEN_BYTE_ARRAY length, 0 otherwise
     * @param repetition the repetition
     * @param leaf the leaf column index of a leaf field, -1 for a group
     * @param signature the physical type, type length, repetition and nesting of the subtree
     */
    record Field(String name, SchemaElement element, Column.Type type, int typeLength, Column.Repetition repetition, int leaf, String signature) {}

    final List<Field> fields;
    final Map<String, Field> byName;
    /** Names given to more than one top-level field. */
    final Set<String> duplicates;
    /** The schema elements of the leaf columns, by leaf index. */
    final List<SchemaElement> leaves;
    final List<List<String>> leafPaths;

    private PartSchema(List<Field> fields, Map<String, Field> byName, Set<String> duplicates, List<SchemaElement> leaves, List<List<String>> leafPaths) {
        this.fields = fields;
        this.byName = byName;
        this.duplicates = duplicates;
        this.leaves = leaves;
        this.leafPaths = leafPaths;
    }

    /** Parses a footer's flattened schema. */
    static PartSchema parse(List<SchemaElement> schema) throws DataException {
        if (schema == null || schema.isEmpty()) {
            throw new DataException("the schema is empty");
        }
        Walker w = new Walker(schema);
        SchemaElement root = schema.get(0);
        if (root.isSetType() || !root.isSetNum_children()) {
            throw new DataException("the schema root is not a group");
        }
        int children = root.isSetNum_children() ? root.getNum_children() : 0;
        if (children < 0 || children > schema.size() - 1) {
            throw new DataException("the schema root declares " + children + " children");
        }
        List<Field> fields = new ArrayList<>(children);
        Map<String, Field> byName = new LinkedHashMap<>();
        Set<String> duplicates = new HashSet<>();
        for (int i = 0; i < children; i++) {
            Field f = w.field();
            if (byName.putIfAbsent(f.name(), f) != null) {
                duplicates.add(f.name());
            }
            fields.add(f);
        }
        if (w.pos != schema.size()) {
            throw new DataException("the schema holds " + (schema.size() - w.pos) + " elements outside its tree");
        }
        return new PartSchema(List.copyOf(fields), Collections.unmodifiableMap(byName), Set.copyOf(duplicates), List.copyOf(w.leaves), List.copyOf(w.leafPaths));
    }

    /**
     * {@return a top-level field as a {@link Column} of one part}: a group has no physical type, length
     * or logical type; a leaf's logical type is its {@code logical_type}, or else its legacy {@code
     * converted_type} normalized into one.
     */
    static Column column(Field f, String keyColumn) {
        boolean group = f.type() == Column.Type.GROUP;
        Optional<LogicalType> logical = group ? Optional.empty() : logicalType(f.element());
        return new Column(f.name(), f.type(), f.typeLength(), logical, f.repetition(), f.name().equals(keyColumn));
    }

    /**
     * {@return a schema element's logical type}: its {@code logical_type} when set to a member this reader
     * knows, else its {@code converted_type} as Parquet's LogicalTypes.md maps it (time and timestamp
     * adjusted to UTC), as Go's reader does; empty
     * for neither, and for {@code INTERVAL} and the group annotations, which have no leaf logical type.
     */
    static Optional<LogicalType> logicalType(SchemaElement se) {
        // A member this parquet-format does not know (a newer annotation) decodes with no set field: the
        // converted type, if any, then stands for it.
        if (se.isSetLogicalType() && se.getLogicalType().getSetField() != null) {
            org.apache.parquet.format.LogicalType lt = se.getLogicalType();
            return Optional.ofNullable(switch (lt.getSetField()) {
                case STRING -> new LogicalType.StringType();
                case MAP -> new LogicalType.MapType();
                case LIST -> new LogicalType.ListType();
                case ENUM -> new LogicalType.EnumType();
                case DECIMAL -> new LogicalType.DecimalType(lt.getDECIMAL().getPrecision(), lt.getDECIMAL().getScale());
                case DATE -> new LogicalType.DateType();
                case TIME -> unit(lt.getTIME().getUnit())
                        .<LogicalType>map(u -> new LogicalType.TimeType(u, lt.getTIME().isIsAdjustedToUTC())).orElse(null);
                case TIMESTAMP -> unit(lt.getTIMESTAMP().getUnit())
                        .<LogicalType>map(u -> new LogicalType.TimestampType(u, lt.getTIMESTAMP().isIsAdjustedToUTC())).orElse(null);
                case INTEGER -> new LogicalType.IntType(lt.getINTEGER().getBitWidth(), lt.getINTEGER().isIsSigned());
                case UNKNOWN -> new LogicalType.UnknownType();
                case JSON -> new LogicalType.JsonType();
                case BSON -> new LogicalType.BsonType();
                case UUID -> new LogicalType.UuidType();
                case FLOAT16 -> new LogicalType.Float16Type();
                case VARIANT -> new LogicalType.VariantType(lt.getVARIANT().isSetSpecification_version()
                        ? Optional.of((int) lt.getVARIANT().getSpecification_version()) : Optional.empty());
                case GEOMETRY -> new LogicalType.GeometryType(Optional.ofNullable(lt.getGEOMETRY().getCrs()));
                case GEOGRAPHY -> new LogicalType.GeographyType(Optional.ofNullable(lt.getGEOGRAPHY().getCrs()),
                        Optional.ofNullable(lt.getGEOGRAPHY().getAlgorithm()).map(Enum::name));
            });
        }
        if (!se.isSetConverted_type()) {
            return Optional.empty();
        }
        return Optional.ofNullable(switch (se.getConverted_type()) {
            case UTF8 -> new LogicalType.StringType();
            case ENUM -> new LogicalType.EnumType();
            case JSON -> new LogicalType.JsonType();
            case BSON -> new LogicalType.BsonType();
            case DATE -> new LogicalType.DateType();
            case TIME_MILLIS -> new LogicalType.TimeType(LogicalType.TimeUnit.MILLIS, true);
            case TIME_MICROS -> new LogicalType.TimeType(LogicalType.TimeUnit.MICROS, true);
            case TIMESTAMP_MILLIS -> new LogicalType.TimestampType(LogicalType.TimeUnit.MILLIS, true);
            case TIMESTAMP_MICROS -> new LogicalType.TimestampType(LogicalType.TimeUnit.MICROS, true);
            case INT_8 -> new LogicalType.IntType(8, true);
            case INT_16 -> new LogicalType.IntType(16, true);
            case INT_32 -> new LogicalType.IntType(32, true);
            case INT_64 -> new LogicalType.IntType(64, true);
            case UINT_8 -> new LogicalType.IntType(8, false);
            case UINT_16 -> new LogicalType.IntType(16, false);
            case UINT_32 -> new LogicalType.IntType(32, false);
            case UINT_64 -> new LogicalType.IntType(64, false);
            case DECIMAL -> new LogicalType.DecimalType(se.getPrecision(), se.getScale());
            case INTERVAL, MAP, MAP_KEY_VALUE, LIST -> null;
        });
    }

    private static Optional<LogicalType.TimeUnit> unit(org.apache.parquet.format.TimeUnit unit) {
        if (unit == null || unit.getSetField() == null) {
            return Optional.empty();
        }
        return Optional.of(switch (unit.getSetField()) {
            case MILLIS -> LogicalType.TimeUnit.MILLIS;
            case MICROS -> LogicalType.TimeUnit.MICROS;
            case NANOS -> LogicalType.TimeUnit.NANOS;
        });
    }

    private static final class Walker {
        final List<SchemaElement> schema;
        final List<SchemaElement> leaves = new ArrayList<>();
        final List<List<String>> leafPaths = new ArrayList<>();
        final List<String> path = new ArrayList<>();
        int pos = 1;

        Walker(List<SchemaElement> schema) {
            this.schema = schema;
        }

        Field field() throws DataException {
            SchemaElement se = element();
            StringBuilder sig = new StringBuilder();
            int leaf = leaves.size();
            Column.Repetition rep = repetition(se);
            String body = subtree(se, 1);
            sig.append(rep).append(body);
            Column.Type type = isGroup(se) ? Column.Type.GROUP : type(se);
            int length = type == Column.Type.FIXED_LEN_BYTE_ARRAY ? se.getType_length() : 0;
            return new Field(se.getName(), se, type, length, rep, isGroup(se) ? -1 : leaf, sig.toString());
        }

        private SchemaElement element() throws DataException {
            if (pos >= schema.size()) {
                throw new DataException("the schema is truncated");
            }
            SchemaElement se = schema.get(pos++);
            if (se.getName() == null) {
                throw new DataException("a schema element has no name");
            }
            return se;
        }

        /** {@return the signature of the subtree under se (already consumed)} */
        private String subtree(SchemaElement se, int depth) throws DataException {
            if (depth > MAX_NESTING) {
                throw new LimitExceededException(LimitName.SCHEMA_NESTING, depth, MAX_NESTING, "a part's schema");
            }
            if (se.isSetNum_children() && (se.getNum_children() < 0 || se.isSetType() && se.getNum_children() > 0)) {
                throw new DataException("a schema element has an invalid child count or both a type and children");
            }
            path.add(se.getName());
            if (!isGroup(se)) {
                Column.Type t = type(se);
                if (t == Column.Type.FIXED_LEN_BYTE_ARRAY && (!se.isSetType_length() || se.getType_length() <= 0)) {
                    throw new DataException("a FIXED_LEN_BYTE_ARRAY column without a positive type length");
                }
                leaves.add(se);
                leafPaths.add(List.copyOf(path));
                path.removeLast();
                return ":" + t + (t == Column.Type.FIXED_LEN_BYTE_ARRAY ? "(" + se.getType_length() + ")" : "");
            }
            int n = se.getNum_children();
            if (n > schema.size() - pos) {
                throw new DataException("a group declares more children than the schema holds");
            }
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < n; i++) {
                SchemaElement child = element();
                sb.append(child.getName().length()).append(':').append(child.getName()).append(':').append(repetition(child));
                sb.append(subtree(child, depth + 1)).append(';');
            }
            path.removeLast();
            return sb.append('}').toString();
        }

        private static boolean isGroup(SchemaElement se) {
            return se.isSetNum_children() && se.getNum_children() > 0;
        }

        private static Column.Repetition repetition(SchemaElement se) throws DataException {
            FieldRepetitionType r = se.getRepetition_type();
            if (r == null) {
                throw new DataException("the field " + se.getName() + " has no repetition");
            }
            return switch (r) {
                case REQUIRED -> Column.Repetition.REQUIRED;
                case OPTIONAL -> Column.Repetition.OPTIONAL;
                case REPEATED -> Column.Repetition.REPEATED;
            };
        }

        private static Column.Type type(SchemaElement se) throws DataException {
            if (se.getType() == null) {
                throw new DataException("the leaf " + se.getName() + " has no physical type");
            }
            return switch (se.getType()) {
                case BOOLEAN -> Column.Type.BOOLEAN;
                case INT32 -> Column.Type.INT32;
                case INT64 -> Column.Type.INT64;
                case INT96 -> Column.Type.INT96;
                case FLOAT -> Column.Type.FLOAT;
                case DOUBLE -> Column.Type.DOUBLE;
                case BYTE_ARRAY -> Column.Type.BYTE_ARRAY;
                case FIXED_LEN_BYTE_ARRAY -> Column.Type.FIXED_LEN_BYTE_ARRAY;
            };
        }
    }
}
