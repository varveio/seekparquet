// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.io.IOException;

/**
 * Reading storage failed. This is an error of the environment, not a statement about the data
 * (CHECK-5): the same request may succeed later. The cause is the storage's {@link IOException}; a
 * part whose stored object changed after it was opened is reported with a {@link
 * io.varve.seekparquet.storage.PartChangedException} cause.
 */
public final class StorageException extends SeekParquetException {

    private static final long serialVersionUID = 1L;

    StorageException(String message, IOException cause) {
        super(message + ": " + cause.getMessage(), cause);
    }

    /** {@return the storage's exception} */
    @Override
    public synchronized IOException getCause() {
        return (IOException) super.getCause();
    }
}
