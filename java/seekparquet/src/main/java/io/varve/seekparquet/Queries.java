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
     * returned. Row-group routing may probe key pages beyond the returned interval.
     */
    static List<Row> range(Parts parts, Query q, ReadContext ctx) {
        byte[] prefix = q.span().prefix().unsafeArray();
        Bounds b = Bounds.of(q.selection());
        byte[] end = b.end();
        Cursor.RowPosition after = b.after();
        if (parts.size() == 0 || b.empty()) {
            return List.of();
        }
        List<String> columns = q.columns();
        KeyCursor cursor = new KeyCursor(parts, end, ctx);
        List<Row> rows = new ArrayList<>((int) Math.min(q.limit(), 1024));
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
     * The rows {@link #range} returns (Dataset.rows), handed to {@code visitor} as borrowed batches: positioned as a range
     * read is, it stops at the limit (0 for none) without moving past it, and at the first key beyond the
     * bounds.
     */
    static <E extends Exception> void scan(Parts parts, Query q, ReadContext ctx, BatchVisitor<E> visitor) throws E {
        Bounds b = Bounds.of(q.selection());
        if (parts.size() == 0 || b.empty()) {
            return;
        }
        KeyCursor cursor = new KeyCursor(parts, b.end(), ctx);
        if (!b.position(cursor)) {
            return;
        }
        byte[] prefix = q.span().prefix().unsafeArray();
        long n = q.limit() == 0 ? Long.MAX_VALUE : q.limit();
        new BatchScan(q.columns(), ctx, ctx.forValues()).walk(cursor, n, b.end(), prefix.length == 0 ? null : prefix, visitor);
    }

    /** A row {@link #get} found: where, its key and its group offset. */
    private record Found(PartFile part, int physical, long row, byte[] key, long offset) {}

    /**
     * Every row of {@code key}'s group, at most {@code cap}: it walks the group's keys, at most {@code cap}
     * plus one, before it reads any value page, and fails with {@link LimitExceededException} ({@link
     * LimitName#GET_ROWS}) when there is one more, having read no value page.
     */
    static List<Row> get(Parts parts, Bytes key, List<String> columns, int cap, ReadContext ctx) {
        Bounds b = Bounds.of(Selection.of(Span.key(key)));
        if (parts.size() == 0) {
            return List.of();
        }
        KeyCursor cursor = new KeyCursor(parts, b.end(), ctx);
        List<Found> found = new ArrayList<>();
        boolean ok = b.position(cursor);
        while (ok && Arrays.compareUnsigned(cursor.key(), b.end()) < 0) {
            if (found.size() == cap) {
                throw new LimitExceededException(LimitName.GET_ROWS, -1, cap, "the group of key " + key
                        + " has more rows than Limits.getRows(): read it in pages of Dataset.rows over Span.key(key)");
            }
            found.add(new Found(cursor.partFile(), cursor.physical(), cursor.row(), cursor.key(), cursor.offset()));
            ok = cursor.next();
        }
        List<Row> rows = new ArrayList<>(found.size());
        for (Found f : found) {
            rows.add(row(f.part(), f.physical(), f.row(), f.key(), f.offset(), columns, ctx));
        }
        return Collections.unmodifiableList(rows);
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
        Cursor tagged = q.cursor().orElse(null);
        if (tagged instanceof Cursor.CommonPrefix c) {
            Optional<Bytes> successor = c.prefix().prefixEnd();
            if (successor.isEmpty()) {
                return List.of();
            }
            byte[] floor = successor.get().unsafeArray();
            if (from == null || Arrays.compareUnsigned(from, floor) < 0) {
                from = floor;
                exclusive = false;
            }
        } else if (tagged instanceof Cursor.RowPosition r && (from == null || Arrays.compareUnsigned(from, r.key().unsafeArray()) < 0)) {
            from = r.key().unsafeArray();
            exclusive = false;
        }
        byte[] to = min(q.end().map(Bytes::unsafeArray).orElse(null), q.prefix().prefixEnd());
        if (parts.size() == 0 || (to != null && to.length == 0) || (to != null && from != null && Arrays.compareUnsigned(from, to) >= 0)) {
            return List.of();
        }
        KeyCursor cursor = new KeyCursor(parts, to, ctx);
        // With a list index built, a '/' listing reads no page ahead, even for a row group the index declines:
        // the index answers its skips without decoding the pages a read-ahead would predict.
        List<ListEntry> out;
        try (ReadAhead ahead = delimiter == '/' && cursor.listIndexed() ? null : ReadAhead.open(ctx, q.readAhead())) {
            ctx.readAhead = ahead;
            out = list(q, cursor, ahead, from, exclusive, to, ctx);
        } finally {
            ctx.readAhead = null;
        }
        // Closing the read-ahead waits for its reads: a cancellation, interrupt or close meanwhile ends the request.
        // A failure thrown above is kept as it is (this check is not reached).
        ctx.checkCancelled();
        return out;
    }

    private static List<ListEntry> list(ListQuery q, KeyCursor cursor, ReadAhead ahead, byte[] from, boolean exclusive, byte[] to,
            ReadContext ctx) {
        byte[] prefix = q.prefix().unsafeArray();
        byte delimiter = q.delimiter();
        Cursor tagged = q.cursor().orElse(null);
        List<String> columns = q.columns();
        List<ListEntry> out = new ArrayList<>(Math.min(q.limit(), 1024));
        boolean ok = tagged instanceof Cursor.CommonPrefix c && delimiter == '/'
                ? cursor.seekPrefixCursor(c.prefix().unsafeArray(), from) : cursor.seek(from, exclusive);
        if (ok && tagged instanceof Cursor.RowPosition r) {
            ok = cursor.resume(r);
        }
        while (ok && out.size() < q.limit()) {
            byte[] key = cursor.key();
            if ((to != null && Arrays.compareUnsigned(key, to) >= 0) || !startsWith(key, prefix)) {
                break;
            }
            if (tagged instanceof Cursor.RowPosition r
                    && Arrays.equals(key, r.key().unsafeArray()) && cursor.offset() <= r.offset()) {
                ok = cursor.next();
                continue;
            }
            int at = indexOf(key, delimiter, prefix.length);
            if (at < 0) {
                out.add(new ListEntry.RowEntry(row(cursor, columns, ctx)));
                if (out.size() == q.limit()) {
                    break;
                }
                cursor.readAhead(ahead, prefix, delimiter, q.limit() - out.size());
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
            Optional<Bytes> next = Bytes.wrap(common).prefixEnd();
            if (next.isEmpty()) {
                break;
            }
            cursor.readAhead(ahead, prefix, delimiter, q.limit() - out.size());
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
    record Bounds(byte[] start, boolean exclusive, byte[] end, Cursor.RowPosition after) {

        static Bounds of(Selection sel) {
            Span q = sel.span();
            byte[] prefix = q.prefix().unsafeArray();
            byte[] start = q.start().map(Bytes::unsafeArray).orElse(null);
            boolean exclusive = q.startExclusive();
            byte[] end = q.end().map(Bytes::unsafeArray).orElse(null);
            if (prefix.length > 0) {
                if (start == null || Arrays.compareUnsigned(start, prefix) < 0) {
                    start = prefix;
                    exclusive = false;
                }
                end = min(end, q.prefix().prefixEnd());
            }
            Cursor.RowPosition after = sel.cursor().orElse(null);
            if (after != null && (start == null || Arrays.compareUnsigned(start, after.key().unsafeArray()) < 0)) {
                start = after.key().unsafeArray();
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
            return after != null && Arrays.equals(key, after.key().unsafeArray()) && offset <= after.offset();
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
        return row(cursor.partFile(), cursor.physical(), cursor.row(), cursor.key(), cursor.offset(), columns, ctx);
    }

    /** The row at {@code row} of physical row group {@code physical} of {@code part}, whose key is {@code k}. */
    private static Row row(PartFile part, int physical, long row, byte[] k, long offset, List<String> columns, ReadContext ctx) {
        // The owning boundary for rows, get, list (including exact rows) and cursors.
        Bytes key = Bytes.wrap(k);
        if (columns.isEmpty()) {
            return new Row(key, offset, columns, List.of());
        }
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
                values[i] = part.value(physical, spec, row, ctx);
            }
        }
        return new Row(key, offset, columns, Arrays.asList(values));
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
