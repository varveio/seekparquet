// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * Corrupt Parquet data met while decoding (CHECK-5). Internal: the caller turns it into a {@link
 * RefusalException} with the code that fits where it was met and the part it was met in.
 */
final class DataException extends Exception {

    private static final long serialVersionUID = 1L;

    DataException(String message) {
        super(message);
    }

    DataException(String message, Throwable cause) {
        super(message + (cause.getMessage() == null ? " (" + cause.getClass().getSimpleName() + ")" : ": " + cause.getMessage()), cause);
    }
}
