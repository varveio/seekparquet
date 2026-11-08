// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    private PartSchema(List<Field> fields, Map<String, Field> byName, Set<String> duplicates, List<SchemaElement> leaves) {
        this.fields = fields;
        this.byName = byName;
        this.duplicates = duplicates;
        this.leaves = leaves;
    }

    /** Parses a footer's flattened schema. */
    static PartSchema parse(List<SchemaElement> schema) throws DataException {
        if (schema == null || schema.isEmpty()) {
            throw new DataException("the schema is empty");
        }
        Walker w = new Walker(schema);
        SchemaElement root = schema.get(0);
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
        return new PartSchema(List.copyOf(fields), Collections.unmodifiableMap(byName), Set.copyOf(duplicates), List.copyOf(w.leaves));
    }

    private static final class Walker {
        final List<SchemaElement> schema;
        final List<SchemaElement> leaves = new ArrayList<>();
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
                throw new LimitExceededException("schemaNesting", depth, MAX_NESTING, "a part's schema");
            }
            if (!isGroup(se)) {
                Column.Type t = type(se);
                if (t == Column.Type.FIXED_LEN_BYTE_ARRAY && (!se.isSetType_length() || se.getType_length() <= 0)) {
                    throw new DataException("a FIXED_LEN_BYTE_ARRAY column without a positive type length");
                }
                leaves.add(se);
                return ":" + t + (t == Column.Type.FIXED_LEN_BYTE_ARRAY ? "(" + se.getType_length() + ")" : "");
            }
            int n = se.getNum_children();
            if (n > schema.size() - pos) {
                throw new DataException("a group declares more children than the schema holds");
            }
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < n; i++) {
                SchemaElement child = element();
                sb.append('"').append(child.getName().replace("\"", "\\\"")).append("\":").append(repetition(child));
                sb.append(subtree(child, depth + 1)).append(';');
            }
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
