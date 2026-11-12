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
 * @param cursor the cursor, when present
 * @param columns the columns to project for row entries, by exact top-level name, in order
 * @param readAhead the number of key pages the listing may read ahead concurrently; 0, the default,
 *     for none (see {@link Builder#readAhead(int)})
 */
public record ListQuery(
        Bytes prefix,
        byte delimiter,
        Optional<Bytes> end,
        int limit,
        List<String> columns, Optional<Cursor> cursor, int readAhead) {

    /** The largest read-ahead a listing uses: a larger {@link #readAhead()} is read as it. */
    public static final int MAX_READ_AHEAD = 64;

    /**
     * Creates a listing query.
     *
     * @param prefix the prefix
     * @param delimiter the delimiter byte
     * @param end the exclusive upper bound
     * @param limit the limit
     * @param cursor the cursor
     * @param columns the projected columns
     * @param readAhead the key pages read ahead, at least 0
     * @throws IllegalArgumentException if {@code limit} is not positive (QUERY-1), or {@code readAhead}
     *     is negative
     */
    public ListQuery {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(end, "end");
        Objects.requireNonNull(cursor, "cursor");
        columns = List.copyOf(columns);
        if (limit <= 0) {
            throw new IllegalArgumentException("limit " + limit + " is not a positive integer");
        }
        if (readAhead < 0) {
            throw new IllegalArgumentException("read-ahead " + readAhead + " is negative");
        }
    }

    /**
     * Creates a listing query that reads no key page ahead.
     *
     * @param prefix the prefix
     * @param delimiter the delimiter byte
     * @param end the exclusive upper bound
     * @param limit the limit
     * @param columns the projected columns
     * @param cursor the cursor
     * @throws IllegalArgumentException if {@code limit} is not positive (QUERY-1)
     */
    public ListQuery(Bytes prefix, byte delimiter, Optional<Bytes> end, int limit, List<String> columns, Optional<Cursor> cursor) {
        this(prefix, delimiter, end, limit, columns, cursor, 0);
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
        b.readAhead = readAhead;
        return b;
    }

    /**
     * Returns the query for the page after one whose last entry is {@code last} (LIST-6).
     *
     * @param last the last entry of the previous page
     * @return the next query
     */
    public ListQuery next(ListEntry last) {
        return toBuilder().after(last.cursor()).build();
    }

    /** Builds a {@link ListQuery}. Not thread-safe. */
    public static final class Builder {
        private final byte delimiter;
        private Bytes prefix = Bytes.empty();
        private Optional<Bytes> end = Optional.empty();
        private int limit;
        private List<String> columns = List.of();
        private Optional<Cursor> cursor = Optional.empty();
        private int readAhead;

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
         * @param cursor the cursor
         * @return this builder
         */
        public Builder after(Cursor cursor) {
            this.cursor = Optional.of(cursor);
            return this;
        }

        /**
         * Sets how many key pages the listing may read ahead concurrently: 0, the default, for none; a
         * value above {@link #MAX_READ_AHEAD} is read as it.
         *
         * <p>A listing that skips from common prefix to common prefix reads one key page after each, one
         * after another: over an object store, one round trip per prefix. With read-ahead, after each
         * entry the listing predicts, from the checked key page index of the row group it is in, the key
         * pages it will decode next (after a common prefix, the first page whose maximum reaches the
         * prefix's successor; after a row, the next page) and reads up to this many of them concurrently,
         * on virtual threads, so that it waits for about one read per {@code readAhead} pages instead of
         * one per page. The prediction stays within the row group the listing is in.
         *
         * <p>Pages read ahead are checked exactly as pages read when decoded are (CHECK-2): the entries,
         * errors and refusals are the same with or without read-ahead. A page predicted wrongly (shortened
         * or overlapping page bounds, RG-5, RG-6) is read and transferred for nothing ({@link
         * ReadStats#readAheadUnused()}); a read ahead that fails, or throws, is not used, and the page is
         * read again when the listing reaches it. Memory: a request holds at most {@code readAhead} times
         * 1 MiB of pages read ahead and not yet used, and at most 4 times {@code readAhead} pages, until
         * the listing uses or passes them or ends; a key page above that budget, or above {@link
         * Limits#maxPageBytes()}, is not read ahead. The reads ahead end, and are waited for, before the
         * listing returns, including when it fails, is cancelled, or the dataset closes.
         *
         * <p>It pays where a read waits on a round trip (an object store), not on a local file. A listing
         * with delimiter {@code '/'} reads no page ahead while a list index ({@link
         * Dataset#buildListIndex}) is built, whether or not the index serves a given row group.
         *
         * @param readAhead the key pages read ahead, at least 0
         * @return this builder
         */
        public Builder readAhead(int readAhead) {
            this.readAhead = readAhead;
            return this;
        }

        /**
         * Builds the query.
         *
         * @return the query
         * @throws IllegalArgumentException if the limit is not set or not positive, or the read-ahead is
         *     negative
         */
        public ListQuery build() {
            return new ListQuery(prefix, delimiter, end, limit, columns, cursor, readAhead);
        }
    }
}
