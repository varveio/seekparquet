// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * Why valid data could not be read: it uses a feature this reader does not implement (spec §10).
 * This is not a refusal of the data.
 */
public enum UnsupportedCode {
    /** A codec or encoding outside what this reader implements (ENC-1). */
    ENCODING("unsupported_encoding"),
    /** A request to project a nested, repeated or {@code INT96} column (COL-3). */
    PROJECTION("unsupported_projection"),
    /** A part that uses Parquet modular encryption (FILE-2). */
    ENCRYPTION("unsupported_encryption");

    private final String id;

    UnsupportedCode(String id) {
        this.id = id;
    }

    /** {@return the code as the specification spells it, for example {@code unsupported_encoding}} */
    public String id() {
        return id;
    }
}
