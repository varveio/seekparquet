// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * The base of the domain failures this library throws: what the data, the reader's limits or storage
 * make of a well-formed request. It is unchecked, and its subclasses are a closed set, so a caller can
 * tell the kinds apart exhaustively:
 *
 * <ul>
 *   <li>{@link RefusalException}: the data breaks the specification; it carries a stable {@link
 *       RefusalCode}.
 *   <li>{@link UnsupportedFeatureException}: the data is valid but uses a feature this reader does
 *       not implement.
 *   <li>{@link LimitExceededException}: the data or a result exceeds a {@link Limits limit} or a fixed
 *       capacity of this reader ({@link LimitName}).
 *   <li>{@link StorageException}: reading storage failed; this says nothing about the data.
 * </ul>
 *
 * <p>Other failures are outside this hierarchy, as Java idiom has them: a malformed request (a
 * non-positive limit for {@code rows}, a malformed cursor token, negative limits) is an {@link
 * IllegalArgumentException}, thrown before anything is read; a part index outside the manifest an {@link
 * IndexOutOfBoundsException}; a cancelled or interrupted request a {@link
 * java.util.concurrent.CancellationException}; a request on a closed dataset an {@link
 * IllegalStateException}. A {@link BatchVisitor}'s or statistics consumer's own exceptions propagate
 * unchanged. The Go reader reports these as {@code ErrInvalidRequest}, {@code ErrClosed} and context
 * errors.
 */
public abstract sealed class SeekParquetException extends RuntimeException
        permits RefusalException, UnsupportedFeatureException, LimitExceededException, StorageException {

    private static final long serialVersionUID = 1L;

    SeekParquetException(String message, Throwable cause) {
        super(message, cause);
    }
}
