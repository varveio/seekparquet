// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** The query algorithms of spec §6, over a {@link KeyCursor}. */
final class Queries {

    private Queries() {}

    /**
     * RANGE-1: seeks to the effective lower bound (start intersected with the prefix) and reads
     * forward until the effective upper bound or the limit. Value pages are decoded only for the rows
     * returned, and no key page beyond the last row returned is decoded.
     */
    static List<Row> range(PartFile[] parts, RangeQuery q, ReadContext ctx) {
        byte[] prefix = q.prefix().unsafeArray();
        byte[] start = q.start().map(Bytes::unsafeArray).orElse(null);
        boolean exclusive = q.startExclusive();
        byte[] end = q.end().map(Bytes::unsafeArray).orElse(null);
        if (prefix.length > 0) {
            if (start == null || Arrays.compareUnsigned(start, prefix) < 0) {
                start = prefix;
                exclusive = false;
            }
            end = min(end, q.prefix().successor());
        }
        Continuation after = q.cursor().orElse(null);
        if (after instanceof Continuation.CommonPrefix c) {
            Optional<Bytes> successor = c.bytes().successor();
            if (successor.isEmpty()) {
                return List.of();
            }
            byte[] floor = successor.get().unsafeArray();
            if (start == null || Arrays.compareUnsigned(start, floor) < 0) {
                start = floor;
                exclusive = false;
            }
        } else if (after != null && (start == null || Arrays.compareUnsigned(start, after.bytes().unsafeArray()) < 0)) {
            start = after.bytes().unsafeArray();
            exclusive = false;
        }
        if (parts.length == 0 || (end != null && start != null && Arrays.compareUnsigned(start, end) >= 0) || (end != null && end.length == 0)) {
            return List.of();
        }
        List<String> columns = q.columns();
        KeyCursor cursor = new KeyCursor(parts, ctx);
        List<Row> rows = new ArrayList<>(Math.min(q.limit(), 1024));
        boolean ok = cursor.seek(start, exclusive);
        while (ok) {
            byte[] key = cursor.key();
            if ((end != null && Arrays.compareUnsigned(key, end) >= 0) || !startsWith(key, prefix)) {
                break;
            }
            if (after instanceof Continuation.RowPosition r
                    && Arrays.equals(key, r.bytes().unsafeArray()) && cursor.offset() <= r.offset()) {
                ok = cursor.next();
                continue;
            }
            rows.add(row(cursor, columns, ctx));
            if (rows.size() >= q.limit()) {
                break;
            }
            ok = cursor.next();
        }
        return Collections.unmodifiableList(rows);
    }

    /**
     * LIST-2 to LIST-7: a skip-scan. After a common prefix the cursor seeks to the prefix's successor
     * instead of decoding the keys under it (keys sharing a page with the keys around them are still
     * decoded).
     */
    static List<ListEntry> list(PartFile[] parts, ListQuery q, ReadContext ctx) {
        byte[] prefix = q.prefix().unsafeArray();
        byte delimiter = q.delimiter();
        byte[] from = prefix.length == 0 ? null : prefix;
        boolean exclusive = false;
        boolean suppress = false;
        byte[] after = q.after().map(Bytes::unsafeArray).orElse(null);
        if (after != null && Arrays.compareUnsigned(after, prefix) >= 0) {
            from = after;
            exclusive = true;
            suppress = q.continuation();
        }
        Continuation tagged = q.cursor().orElse(null);
        if (tagged instanceof Continuation.CommonPrefix c) {
            Optional<Bytes> successor = c.bytes().successor();
            if (successor.isEmpty()) {
                return List.of();
            }
            byte[] floor = successor.get().unsafeArray();
            if (from == null || Arrays.compareUnsigned(from, floor) < 0) {
                from = floor;
                exclusive = false;
            }
        } else if (tagged != null && (from == null || Arrays.compareUnsigned(from, tagged.bytes().unsafeArray()) < 0)) {
            from = tagged.bytes().unsafeArray();
            exclusive = false;
        }
        byte[] to = min(q.end().map(Bytes::unsafeArray).orElse(null), q.prefix().successor());
        if (parts.length == 0 || (to != null && to.length == 0) || (to != null && from != null && Arrays.compareUnsigned(from, to) >= 0)) {
            return List.of();
        }
        List<String> columns = q.columns();
        KeyCursor cursor = new KeyCursor(parts, ctx);
        List<ListEntry> out = new ArrayList<>(Math.min(q.limit(), 1024));
        boolean ok = cursor.seek(from, exclusive);
        while (ok && out.size() < q.limit()) {
            byte[] key = cursor.key();
            if ((to != null && Arrays.compareUnsigned(key, to) >= 0) || !startsWith(key, prefix)) {
                break;
            }
            if (tagged instanceof Continuation.RowPosition r
                    && Arrays.equals(key, r.bytes().unsafeArray()) && cursor.offset() <= r.offset()) {
                ok = cursor.next();
                continue;
            }
            int at = indexOf(key, delimiter, prefix.length);
            if (at < 0) {
                out.add(new ListEntry.RowEntry(row(cursor, columns, ctx)));
                if (out.size() == q.limit()) {
                    break;
                }
                ok = cursor.next();
                continue;
            }
            byte[] common = Arrays.copyOf(key, at + 1);
            if (!(suppress && Arrays.compareUnsigned(common, after) <= 0)) {
                out.add(new ListEntry.CommonPrefix(Bytes.wrap(common)));
            }
            if (out.size() == q.limit()) {
                break;
            }
            Optional<Bytes> next = Bytes.wrap(common).successor();
            if (next.isEmpty()) {
                break;
            }
            ok = cursor.seek(next.get().unsafeArray(), false);
        }
        return Collections.unmodifiableList(out);
    }

    private static Row row(KeyCursor cursor, List<String> columns, ReadContext ctx) {
        Bytes key = Bytes.wrap(cursor.key());
        if (columns.isEmpty()) {
            return new Row(key, cursor.offset(), columns, List.of());
        }
        PartFile part = cursor.partFile();
        Value[] values = new Value[columns.size()];
        for (int i = 0; i < values.length; i++) {
            String name = columns.get(i);
            ColumnSpec spec = part.projectable.get(name);
            if (spec == null) {
                values[i] = Value.Absent.INSTANCE;
            } else if (spec.leaf() == part.keyLeaf) {
                values[i] = new Value.ByteArray(key);
            } else {
                values[i] = part.value(cursor.physical(), spec, cursor.row(), ctx);
            }
        }
        return new Row(key, cursor.offset(), columns, Arrays.asList(values));
    }

    private static byte[] min(byte[] end, Optional<Bytes> successor) {
        if (successor.isEmpty()) {
            return end;
        }
        byte[] s = successor.get().unsafeArray();
        return end == null || Arrays.compareUnsigned(s, end) < 0 ? s : end;
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        return key.length >= prefix.length && Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length);
    }

    private static int indexOf(byte[] key, byte b, int from) {
        for (int i = from; i < key.length; i++) {
            if (key[i] == b) {
                return i;
            }
        }
        return -1;
    }
}
