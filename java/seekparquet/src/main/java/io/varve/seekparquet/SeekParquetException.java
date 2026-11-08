// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * The base of every exception this library throws for a dataset or a request. It is unchecked, and
 * its subclasses are a closed set, so a caller can tell the kinds apart exhaustively:
 *
 * <ul>
 *   <li>{@link RefusalException}: the data breaks the specification; it carries a stable {@link
 *       RefusalCode}.
 *   <li>{@link UnsupportedFeatureException}: the data is valid but uses a feature this reader does
 *       not implement.
 *   <li>{@link LimitExceededException}: a size declared by the data exceeds a configured {@link
 *       Limits limit}.
 *   <li>{@link StorageException}: reading storage failed; this says nothing about the data.
 * </ul>
 *
 * <p>Requests that are malformed (a limit that is not positive, say) are rejected when they are
 * built, with {@link IllegalArgumentException}. A cancelled request ends with {@link
 * java.util.concurrent.CancellationException}, and a request on a closed dataset with {@link
 * IllegalStateException}.
 */
public abstract sealed class SeekParquetException extends RuntimeException
        permits RefusalException, UnsupportedFeatureException, LimitExceededException, StorageException {

    private static final long serialVersionUID = 1L;

    SeekParquetException(String message, Throwable cause) {
        super(message, cause);
    }
}
