// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A delimited listing (spec §6.3). The candidate keys begin with {@code prefix}, come after
 * {@code cursor} when it is given, and are less than {@code end} when it is given. A candidate whose
 * remainder after the prefix contains the delimiter yields the common prefix up to and including the
 * first such delimiter; any other candidate yields its row. Common prefixes are distinct; repeated rows retain file order. Entries are in ascending byte
 * order, at most {@code limit} of them.
 *
 * <p>Use {@link #next(ListEntry)} to continue after the last returned entry with a tagged
 * row position or common prefix (PAGE-1/PAGE-2).
 *
 * <pre>{@code
 * ListQuery q = ListQuery.builder((byte) '/').prefix(Bytes.utf8("photos/")).limit(100).build();
 * List<ListEntry> page = dataset.list(q);
 * while (page.size() == q.limit()) {
 *     q = q.next(page.getLast());
 *     page = dataset.list(q);
 * }
 * }</pre>
 *
 * @param prefix keys must begin with it; empty for no constraint
 * @param delimiter the delimiter byte
 * @param end keys must be strictly less, when given
 * @param limit the most entries returned; positive (QUERY-1)
 * @param cursor the tagged continuation, when present
 * @param columns the columns to project for row entries, by exact top-level name, in order
 */
public record ListQuery(
        Bytes prefix,
        byte delimiter,
        Optional<Bytes> end,
        int limit,
        List<String> columns, Optional<Continuation> cursor) {

    /**
     * Creates a listing query.
     *
     * @param prefix the prefix
     * @param delimiter the delimiter byte
     * @param end the exclusive upper bound
     * @param limit the limit
     * @param cursor the tagged continuation
     * @param columns the projected columns
     * @throws IllegalArgumentException if {@code limit} is not positive (QUERY-1)
     */
    public ListQuery {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(end, "end");
        Objects.requireNonNull(cursor, "cursor");
        columns = List.copyOf(columns);
        if (limit <= 0) {
            throw new IllegalArgumentException("limit " + limit + " is not a positive integer");
        }
    }

    /**
     * Returns a builder for a listing with {@code delimiter} and no prefix, bounds, columns or limit.
     *
     * @param delimiter the delimiter byte
     * @return the builder
     */
    public static Builder builder(byte delimiter) {
        return new Builder(delimiter);
    }

    /** {@return a builder holding this query's arguments} */
    public Builder toBuilder() {
        Builder b = new Builder(delimiter);
        b.prefix = prefix;
        b.end = end;
        b.limit = limit;
        b.columns = columns;
        b.cursor = cursor;
        return b;
    }

    /**
     * Returns the query for the page after one whose last entry is {@code last} (LIST-6).
     *
     * @param last the last entry of the previous page
     * @return the next query
     */
    public ListQuery next(ListEntry last) {
        Continuation next = switch (last) {
            case ListEntry.RowEntry r -> r.row().continuation();
            case ListEntry.CommonPrefix p -> new Continuation.CommonPrefix(p.prefix());
        };
        Builder b = toBuilder();
        return b.after(next).build();
    }

    /** Builds a {@link ListQuery}. Not thread-safe. */
    public static final class Builder {
        private final byte delimiter;
        private Bytes prefix = Bytes.empty();
        private Optional<Bytes> end = Optional.empty();
        private int limit;
        private List<String> columns = List.of();
        private Optional<Continuation> cursor = Optional.empty();

        private Builder(byte delimiter) {
            this.delimiter = delimiter;
        }

        /**
         * Sets the prefix every key must begin with.
         *
         * @param prefix the prefix; empty for none
         * @return this builder
         */
        public Builder prefix(Bytes prefix) {
            this.prefix = Objects.requireNonNull(prefix, "prefix");
            return this;
        }

        /**
         * Sets the exclusive upper bound on candidate keys.
         *
         * @param end the bound
         * @return this builder
         */
        public Builder end(Bytes end) {
            this.end = Optional.of(end);
            return this;
        }

        /**
         * Sets the most entries returned.
         *
         * @param limit a positive integer
         * @return this builder
         */
        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        /**
         * Sets the columns to project for row entries, in order.
         *
         * @param columns the column names
         * @return this builder
         */
        public Builder columns(String... columns) {
            return columns(Arrays.asList(columns));
        }

        /**
         * Sets the columns to project for row entries, in order.
         *
         * @param columns the column names
         * @return this builder
         */
        public Builder columns(List<String> columns) {
            this.columns = List.copyOf(columns);
            return this;
        }

        /** Sets the tagged position after the last returned entry.
         * @param cursor the continuation
         * @return this builder
         */
        public Builder after(Continuation cursor) {
            this.cursor = Optional.of(cursor);
            return this;
        }

        /**
         * Builds the query.
         *
         * @return the query
         * @throws IllegalArgumentException if the limit is not set or not positive
         */
        public ListQuery build() {
            return new ListQuery(prefix, delimiter, end, limit, columns, cursor);
        }
    }
}
