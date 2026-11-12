// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * The key bounds of a range (spec §6.2): the keys that begin with {@code prefix}, are at or after {@code
 * start} (strictly after it when {@code startExclusive}) and are strictly before {@code end}, in ascending
 * unsigned byte order. Bounds select whole groups of equal keys. A span is an immutable value; it owns its
 * bytes as {@link Bytes} do. The Go reader's {@code Span}.
 *
 * <p>Bounds are byte strings and may be empty (QUERY-2): an empty start includes every key, and an empty
 * end includes none, since every key is non-empty. An absent bound is unbounded. The prefix, when not
 * empty, is intersected with the bounds, so a read never leaves the keys that begin with it.
 *
 * <pre>{@code
 * Span span = Span.builder().start(Bytes.utf8("photos/2024/")).end(Bytes.utf8("photos/2025/")).build();
 * }</pre>
 *
 * @param prefix keys must begin with it; empty for no constraint
 * @param start the lower bound, when there is one
 * @param startExclusive whether a key equal to {@code start} is excluded; meaningless without a start
 * @param end the exclusive upper bound, when there is one
 */
public record Span(Bytes prefix, Optional<Bytes> start, boolean startExclusive, Optional<Bytes> end) {

    private static final Span ALL = new Span(Bytes.empty(), Optional.empty(), false, Optional.empty());

    /**
     * Creates a span.
     *
     * @param prefix the prefix, empty for none
     * @param start the lower bound
     * @param startExclusive whether the lower bound is exclusive
     * @param end the upper bound
     */
    public Span {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(end, "end");
    }

    /** {@return the span of every key} The Go reader's zero {@code Span}. */
    public static Span all() {
        return ALL;
    }

    /**
     * Returns the span of the keys that begin with {@code prefix}, with no other bounds. The Go reader's
     * {@code PrefixSpan}.
     *
     * @param prefix the prefix
     * @return the span
     */
    public static Span prefix(Bytes prefix) {
        return new Span(prefix, Optional.empty(), false, Optional.empty());
    }

    /**
     * Returns the span of exactly {@code key}'s group of equal keys: {@code [key, key + 0x00)}. A limit may
     * split the group; continue with the last row's cursor as with any span. {@link Dataset#get} reads one
     * whole group. The Go reader's {@code KeySpan}.
     *
     * @param key the key
     * @return the span
     */
    public static Span key(Bytes key) {
        byte[] end = Arrays.copyOf(key.unsafeArray(), Math.addExact(key.length(), 1));
        return new Span(Bytes.empty(), Optional.of(key), false, Optional.of(Bytes.wrap(end)));
    }

    /** {@return a builder of a span with no bounds} */
    public static Builder builder() {
        return new Builder();
    }

    /** {@return a builder holding this span's bounds} */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.prefix = prefix;
        b.start = start;
        b.startExclusive = startExclusive;
        b.end = end;
        return b;
    }

    /** Builds a {@link Span}. Not thread-safe. */
    public static final class Builder {
        private Bytes prefix = Bytes.empty();
        private Optional<Bytes> start = Optional.empty();
        private boolean startExclusive;
        private Optional<Bytes> end = Optional.empty();

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
         * @param start the lowest key selected
         * @return this builder
         */
        public Builder start(Bytes start) {
            this.start = Optional.of(start);
            this.startExclusive = false;
            return this;
        }

        /**
         * Sets an exclusive lower bound: only keys strictly greater are selected.
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
         * @param end the least key not selected
         * @return this builder
         */
        public Builder end(Bytes end) {
            this.end = Optional.of(end);
            return this;
        }

        /** {@return the span} */
        public Span build() {
            return new Span(prefix, start, startExclusive, end);
        }
    }
}
