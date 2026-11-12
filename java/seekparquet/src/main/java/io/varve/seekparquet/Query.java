// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A range request (spec §6.2): the rows of a {@link Selection}, in ascending key order and equal keys in
 * file order, at most {@link #limit()} of them, each with the values of {@link #columns()}. Immutable and
 * safe to share. The Go reader's {@code Query}.
 *
 * <p>{@link Dataset#rows} requires a positive limit (QUERY-1). {@link Dataset#scan} also takes 0, the
 * default, for every row to the end of the span: a streaming extension of this library, not a query of
 * the specification. A negative limit is rejected when the query is built.
 *
 * <pre>{@code
 * Query q = Query.builder(Span.prefix(Bytes.utf8("photos/"))).limit(1000).columns("size", "etag").build();
 * List<Row> page = dataset.rows(q);
 * while (page.size() == q.limit()) {
 *     q = q.next(page.getLast());
 *     page = dataset.rows(q);
 * }
 * }</pre>
 */
public final class Query {

    private final Span span;
    private final Optional<Cursor.RowPosition> cursor;
    private final long limit;
    private final List<String> columns;

    private Query(Builder b) {
        this.span = b.span;
        this.cursor = b.cursor;
        this.limit = b.limit;
        this.columns = b.columns;
    }

    /**
     * Returns a builder of a query of {@code span}, with no cursor, no columns and limit 0.
     *
     * @param span the key bounds
     * @return the builder
     */
    public static Builder builder(Span span) {
        return new Builder().span(span);
    }

    /** {@return a builder of a query of every key, with no cursor, no columns and limit 0} */
    public static Builder builder() {
        return builder(Span.all());
    }

    /** {@return a builder holding this query's arguments} */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.span = span;
        b.cursor = cursor;
        b.limit = limit;
        b.columns = columns;
        return b;
    }

    /** {@return the key bounds} */
    public Span span() {
        return span;
    }

    /** {@return the row cursor the query continues after, when present} */
    public Optional<Cursor.RowPosition> cursor() {
        return cursor;
    }

    /** {@return the span and cursor, the rows the query reads before its limit} */
    public Selection selection() {
        return new Selection(span, cursor);
    }

    /**
     * {@return the most rows returned} Positive for {@link Dataset#rows}; 0 for {@link Dataset#scan} reads
     * to the end of the span.
     */
    public long limit() {
        return limit;
    }

    /**
     * {@return the columns to project for each row, by exact top-level name (COL-6), in order, duplicates
     * included} Empty reads keys only.
     */
    public List<String> columns() {
        return columns;
    }

    /**
     * Returns the query for the rows after {@code last}, the last row of a result of this query: the same
     * bounds, limit and columns with the row's cursor.
     *
     * @param last the last row returned
     * @return the next query
     */
    public Query next(Row last) {
        return toBuilder().cursor(last.cursor()).build();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Query q && span.equals(q.span) && cursor.equals(q.cursor) && limit == q.limit && columns.equals(q.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(span, cursor, limit, columns);
    }

    @Override
    public String toString() {
        return "Query[span=" + span + ", cursor=" + cursor + ", limit=" + limit + ", columns=" + columns + "]";
    }

    /** Builds a {@link Query}. Not thread-safe. */
    public static final class Builder {
        private Span span = Span.all();
        private Optional<Cursor.RowPosition> cursor = Optional.empty();
        private long limit;
        private List<String> columns = List.of();

        private Builder() {}

        /**
         * Sets the key bounds, keeping the cursor.
         *
         * @param span the bounds
         * @return this builder
         */
        public Builder span(Span span) {
            this.span = Objects.requireNonNull(span, "span");
            return this;
        }

        /**
         * Sets the key bounds and the cursor of {@code selection}.
         *
         * @param selection the selection
         * @return this builder
         */
        public Builder selection(Selection selection) {
            this.span = selection.span();
            this.cursor = selection.cursor();
            return this;
        }

        /**
         * Sets the row cursor to continue after.
         *
         * @param cursor the cursor of a row a read of the same query returned
         * @return this builder
         */
        public Builder cursor(Cursor.RowPosition cursor) {
            this.cursor = Optional.of(cursor);
            return this;
        }

        /**
         * Clears the cursor: the query reads from the start of its span.
         *
         * @return this builder
         */
        public Builder clearCursor() {
            this.cursor = Optional.empty();
            return this;
        }

        /**
         * Sets the most rows returned.
         *
         * @param limit positive, or 0 for every row (a scan only)
         * @return this builder
         */
        public Builder limit(long limit) {
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

        /**
         * Builds the query.
         *
         * @return the query
         * @throws IllegalArgumentException if the limit is negative
         */
        public Query build() {
            if (limit < 0) {
                throw new IllegalArgumentException("limit " + limit + " is negative");
            }
            return new Query(this);
        }
    }
}
