// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet.storage;

import java.io.IOException;

/**
 * The stored object of a part is no longer the version that was opened: it was replaced, truncated
 * or rewritten. Parts are immutable while their manifest may be read (COMMIT-2), so this is reported
 * as a storage failure, not as corrupt data.
 */
public class PartChangedException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param message what changed
     */
    public PartChangedException(String message) {
        super(message);
    }
}
