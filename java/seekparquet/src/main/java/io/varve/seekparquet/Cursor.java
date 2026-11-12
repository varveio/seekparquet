// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;

/**
 * A position after a result of a query, to continue the query from (PAGE-1): after a row ({@link
 * RowPosition}) or after a common prefix a listing returned ({@link CommonPrefix}). A cursor is an
 * immutable value that owns its bytes; the reader makes one for every result ({@link Row#cursor()},
 * {@link ListEntry#cursor()}, {@link Batch#cursor(int)}), and a caller may also construct one.
 *
 * <p>A cursor applies to the query that produced it, on the same dataset: repeating the query with the
 * cursor of each page's last entry returns every result exactly once. It names a position, not a row:
 * an offset beyond the end of its group continues at the next key, so {@link Long#MAX_VALUE} means after
 * the whole group, and a nonzero offset on a dataset of unique keys means after the key. A range takes
 * only a row cursor; a listing takes either.
 *
 * <p>{@link #encode()} and {@link #parse(String)} carry a cursor as one line of text, version 1 of the
 * SeekParquet cursor token ({@code spec/cursor-token-1.md}), shared with the Go reader's {@code
 * Cursor.Encode} and {@code ParseCursor}: a token one reader makes, the other reads as the same cursor.
 */
public sealed interface Cursor permits Cursor.RowPosition, Cursor.CommonPrefix {

    /**
     * The length in characters of the longest token {@link #encode()} makes and {@link #parse} reads:
     * 1 MiB (1,048,576), a key or prefix of at least 786,411 bytes whatever its offset. The Go reader's
     * {@code MaxCursorToken}.
     */
    int MAX_TOKEN_LENGTH = 1 << 20;

    /**
     * Returns this cursor as a version 1 cursor token: {@code spc1.r.KEY.OFFSET} for a row cursor and
     * {@code spc1.p.PREFIX} for a prefix cursor, the bytes in unpadded base64url (RFC 4648 §5) and the
     * offset in canonical decimal. An empty key or prefix is an empty segment. The token carries a position
     * only, as the cursor does.
     *
     * @return the token, ASCII
     * @throws IllegalArgumentException if the token would be longer than {@link #MAX_TOKEN_LENGTH}
     */
    String encode();

    /**
     * Returns the cursor a version 1 token encodes. Only the canonical spelling is accepted: the exact
     * version {@code spc1} and a known tag ({@code r} or {@code p}), exactly the segments of the tag,
     * unpadded base64url whose bits beyond the bytes are zero, and an offset of decimal digits without a
     * sign or leading zeros from 0 to 2^63-1. An empty segment is an empty key or prefix, never no cursor.
     * The length is checked before anything is decoded.
     *
     * @param token the token
     * @return the cursor, owning its bytes
     * @throws IllegalArgumentException if the token is malformed, not canonical, of an unknown version or
     *     tag, or longer than {@link #MAX_TOKEN_LENGTH}: an invalid request (QUERY-1), never a refusal
     */
    static Cursor parse(String token) {
        return Tokens.parse(token);
    }

    /**
     * The position after a row: its key and its zero-based offset in its group of equal keys (PAGE-1).
     *
     * @param key the row's key
     * @param offset the row's position in its group, in file order; at least zero
     */
    record RowPosition(Bytes key, long offset) implements Cursor {
        /**
         * Creates a row cursor.
         *
         * @param key the key
         * @param offset the position in its group, at least zero
         * @throws IllegalArgumentException if {@code offset} is negative
         */
        public RowPosition {
            Objects.requireNonNull(key, "key");
            if (offset < 0) {
                throw new IllegalArgumentException("cursor offset " + offset + " is negative");
            }
        }

        @Override
        public String encode() {
            return Tokens.encode("r", key, Long.toString(offset));
        }
    }

    /**
     * The position after a common prefix a listing returned: after every key that rolls up to it (LIST-6).
     *
     * @param prefix the common prefix
     */
    record CommonPrefix(Bytes prefix) implements Cursor {
        /**
         * Creates a prefix cursor.
         *
         * @param prefix the common prefix
         */
        public CommonPrefix {
            Objects.requireNonNull(prefix, "prefix");
        }

        @Override
        public String encode() {
            return Tokens.encode("p", prefix, null);
        }
    }
}
