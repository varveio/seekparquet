// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A range read (spec §6.2): the rows whose keys begin with {@code prefix}, are at or after {@code
 * start} (strictly after it when {@code startExclusive}) and are strictly before {@code end}, in
 * ascending key order, at most {@code limit} of them, each with the values of {@code columns}.
 *
 * <p>Bounds are byte strings and may be empty (QUERY-2): an empty start includes every key, and an
 * empty end includes none. A prefix read is a range with only a prefix ({@link #prefix(Bytes,
 * int)}).
 *
 * <pre>{@code
 * RangeQuery q = RangeQuery.builder()
 *         .start(Bytes.utf8("photos/2024/"))
 *         .end(Bytes.utf8("photos/2025/"))
 *         .limit(1000)
 *         .columns("size", "etag")
 *         .build();
 * }</pre>
 *
 * @param prefix keys must begin with it; empty for no constraint
 * @param start the lower bound, when there is one
 * @param startExclusive whether a key equal to {@code start} is excluded
 * @param end the exclusive upper bound, when there is one
 * @param limit the most rows returned; positive (QUERY-1)
 * @param cursor the tagged continuation, when present
 * @param columns the columns to project, by exact top-level name (COL-6), in order
 */
public record RangeQuery(
        Bytes prefix, Optional<Bytes> start, boolean startExclusive, Optional<Bytes> end, int limit, List<String> columns, Optional<Continuation> cursor) {

    /**
     * Creates a range query.
     *
     * @param prefix the prefix
     * @param start the lower bound
     * @param startExclusive whether the lower bound is exclusive
     * @param end the upper bound
     * @param limit the limit
     * @param cursor the tagged continuation
     * @param columns the projected columns
     * @throws IllegalArgumentException if {@code limit} is not positive (QUERY-1)
     */
    public RangeQuery {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(end, "end");
        Objects.requireNonNull(cursor, "cursor");
        if (cursor.orElse(null) instanceof Continuation.CommonPrefix) {
            throw new IllegalArgumentException("a range accepts only a row cursor");
        }
        columns = List.copyOf(columns);
        if (limit <= 0) {
            throw new IllegalArgumentException("limit " + limit + " is not a positive integer");
        }
    }

    /**
     * Returns a query for every row, up to {@code limit}.
     *
     * @param limit the most rows returned
     * @return the query
     */
    public static RangeQuery all(int limit) {
        return builder().limit(limit).build();
    }

    /**
     * Returns a prefix read: the rows whose keys begin with {@code prefix}, up to {@code limit}.
     *
     * @param prefix the prefix
     * @param limit the most rows returned
     * @return the query
     */
    public static RangeQuery prefix(Bytes prefix, int limit) {
        return builder().prefix(prefix).limit(limit).build();
    }

    /** Creates a range covering exactly one equal-key group, paginated with {@link #next(Row)}.
     * @param key the key to look up
     * @param limit the maximum rows returned
     * @return the lookup range
     */
    public static RangeQuery lookup(Bytes key, int limit) {
        byte[] end = Arrays.copyOf(key.unsafeArray(), Math.addExact(key.length(), 1));
        return builder().start(key).end(Bytes.wrap(end)).limit(limit).build();
    }

    /** {@return a builder with no bounds, no columns and no limit set} */
    public static Builder builder() {
        return new Builder();
    }

    /** {@return a builder holding this query's arguments} */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.prefix = prefix;
        b.start = start;
        b.startExclusive = startExclusive;
        b.end = end;
        b.limit = limit;
        b.columns = columns;
        b.cursor = cursor;
        return b;
    }

    /**
     * Returns the query for the rows after {@code last}, the last row of a result of this query: the
     * same bounds with a tagged continuation after that row, including its group offset.
     *
     * @param last the last row returned
     * @return the next query
     */
    public RangeQuery next(Row last) {
        return toBuilder().after(last.continuation()).build();
    }

    /** Builds a {@link RangeQuery}. Not thread-safe. */
    public static final class Builder {
        private Bytes prefix = Bytes.empty();
        private Optional<Bytes> start = Optional.empty();
        private boolean startExclusive;
        private Optional<Bytes> end = Optional.empty();
        private int limit;
        private List<String> columns = List.of();
        private Optional<Continuation> cursor = Optional.empty();

        private Builder() {}

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
         * Sets an inclusive lower bound.
         *
         * @param start the lowest key returned
         * @return this builder
         */
        public Builder start(Bytes start) {
            this.start = Optional.of(start);
            this.startExclusive = false;
            return this;
        }

        /**
         * Sets an exclusive lower bound: only keys strictly greater are returned.
         *
         * @param start the bound
         * @return this builder
         */
        public Builder startAfter(Bytes start) {
            this.start = Optional.of(start);
            this.startExclusive = true;
            return this;
        }

        /**
         * Sets the exclusive upper bound.
         *
         * @param end the least key not returned
         * @return this builder
         */
        public Builder end(Bytes end) {
            this.end = Optional.of(end);
            return this;
        }

        /**
         * Sets the most rows returned.
         *
         * @param limit a positive integer
         * @return this builder
         */
        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        /**
         * Sets the columns to project, in order.
         *
         * @param columns the column names
         * @return this builder
         */
        public Builder columns(String... columns) {
            return columns(Arrays.asList(columns));
        }

        /**
         * Sets the columns to project, in order.
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
        public RangeQuery build() {
            return new RangeQuery(prefix, start, startExclusive, end, limit, columns, cursor);
        }
    }
}
