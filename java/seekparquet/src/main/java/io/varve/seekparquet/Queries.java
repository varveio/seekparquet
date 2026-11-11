// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/** The query algorithms of spec §6, over a {@link KeyCursor}. */
final class Queries {

    private Queries() {}

    /**
     * RANGE-1: seeks to the effective lower bound (start intersected with the prefix) and reads
     * forward until the effective upper bound or the limit. Value pages are decoded only for the rows
     * returned. Row-group routing may probe key pages beyond the returned interval.
     */
    static List<Row> range(Parts parts, RangeQuery q, ReadContext ctx) {
        byte[] prefix = q.prefix().unsafeArray();
        Bounds b = Bounds.of(q);
        byte[] end = b.end();
        Continuation.RowPosition after = b.after();
        if (parts.size() == 0 || b.empty()) {
            return List.of();
        }
        List<String> columns = q.columns();
        KeyCursor cursor = new KeyCursor(parts, end, ctx);
        List<Row> rows = new ArrayList<>(Math.min(q.limit(), 1024));
        boolean ok = cursor.seek(b.start(), b.exclusive());
        if (ok && after != null) {
            ok = cursor.resume(after);
        }
        while (ok) {
            byte[] key = cursor.key();
            if ((end != null && Arrays.compareUnsigned(key, end) >= 0) || !startsWith(key, prefix)) {
                break;
            }
            if (b.before(key, cursor.offset())) {
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
     * The rows {@link #range} returns, handed to {@code action} as borrowed batches: positioned as a range
     * read is, it stops at the limit without moving past it, and at the first key beyond the bounds.
     */
    static void rangeBatches(Parts parts, RangeQuery q, ReadContext ctx, Consumer<? super Batch> action) {
        Bounds b = Bounds.of(q);
        if (parts.size() == 0 || b.empty()) {
            return;
        }
        KeyCursor cursor = new KeyCursor(parts, b.end(), ctx);
        if (!b.position(cursor)) {
            return;
        }
        byte[] prefix = q.prefix().unsafeArray();
        new BatchScan(q.columns(), ctx, ctx.forValues()).walk(cursor, q.limit(), b.end(), prefix.length == 0 ? null : prefix, action);
    }

    /**
     * LIST-2 to LIST-7: a skip-scan. After a common prefix the cursor seeks to the prefix's successor
     * instead of decoding the keys under it (keys sharing a page with the keys around them are still
     * decoded).
     */
    static List<ListEntry> list(Parts parts, ListQuery q, ReadContext ctx) {
        byte[] prefix = q.prefix().unsafeArray();
        byte delimiter = q.delimiter();
        byte[] from = prefix.length == 0 ? null : prefix;
        boolean exclusive = false;
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
        if (parts.size() == 0 || (to != null && to.length == 0) || (to != null && from != null && Arrays.compareUnsigned(from, to) >= 0)) {
            return List.of();
        }
        List<String> columns = q.columns();
        KeyCursor cursor = new KeyCursor(parts, to, ctx);
        List<ListEntry> out = new ArrayList<>(Math.min(q.limit(), 1024));
        boolean ok = tagged instanceof Continuation.CommonPrefix c && delimiter == '/'
                ? cursor.seekPrefixCursor(c.bytes().unsafeArray(), from) : cursor.seek(from, exclusive);
        if (ok && tagged instanceof Continuation.RowPosition r) {
            ok = cursor.resume(r);
        }
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
            // LIST-8: the first candidate under the prefix is its exact row when its key is the prefix.
            Optional<Row> exact = at + 1 == key.length ? Optional.of(row(cursor, columns, ctx)) : Optional.empty();
            out.add(new ListEntry.CommonPrefix(Bytes.wrap(common), exact));
            if (out.size() == q.limit()) {
                break;
            }
            Optional<Bytes> next = Bytes.wrap(common).successor();
            if (next.isEmpty()) {
                break;
            }
            ctx.stats.prefixSkips++;
            ok = cursor.seekAfterPrefix(common, next.get().unsafeArray(), delimiter);
        }
        ctx.checkCancelled();
        return Collections.unmodifiableList(out);
    }

    /**
     * A range's effective bounds (RANGE-1): its start intersected with its prefix and raised to its
     * cursor's key, and its end intersected with the prefix's successor.
     *
     * @param start the first key, or null for the dataset's first
     * @param exclusive whether a key equal to start is excluded
     * @param end the exclusive upper bound, or null for none
     * @param after the row cursor, or null: rows of its key at or before its offset are excluded
     */
    record Bounds(byte[] start, boolean exclusive, byte[] end, Continuation.RowPosition after) {

        static Bounds of(RangeQuery q) {
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
            Continuation.RowPosition after = (Continuation.RowPosition) q.cursor().orElse(null);
            if (after != null && (start == null || Arrays.compareUnsigned(start, after.bytes().unsafeArray()) < 0)) {
                start = after.bytes().unsafeArray();
                exclusive = false;
            }
            return new Bounds(start, exclusive, end, after);
        }

        /** {@return whether the bounds admit no key} */
        boolean empty() {
            return end != null && (end.length == 0 || (start != null && Arrays.compareUnsigned(start, end) >= 0));
        }

        /** {@return whether the row at {@code key} and group {@code offset} is at or before the cursor} */
        boolean before(byte[] key, long offset) {
            return after != null && Arrays.equals(key, after.bytes().unsafeArray()) && offset <= after.offset();
        }

        /**
         * Positions {@code cursor} at the first row within these bounds' start and cursor. {@return false
         * when there is none}
         */
        boolean position(KeyCursor cursor) {
            boolean ok = cursor.seek(start, exclusive);
            if (ok && after != null) {
                ok = cursor.resume(after);
            }
            while (ok && before(cursor.key(), cursor.offset())) {
                ok = cursor.next();
            }
            return ok;
        }
    }

    static Row row(KeyCursor cursor, List<String> columns, ReadContext ctx) {
        // The owning boundary for range, list (including exact rows), scan and continuations.
        Bytes key = Bytes.wrap(cursor.key());
        if (columns.isEmpty()) {
            return new Row(key, cursor.offset(), columns, List.of());
        }
        PartFile part = cursor.partFile();
        part.checkProjection(columns);
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
