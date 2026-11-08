// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;

/** The data is valid but uses a feature this reader does not implement. Not a refusal. */
public final class UnsupportedFeatureException extends SeekParquetException {

    private static final long serialVersionUID = 1L;

    /** The kind of unsupported feature. */
    private final UnsupportedCode code;

    UnsupportedFeatureException(UnsupportedCode code, String detail) {
        super(code.id() + ": " + detail, null);
        this.code = Objects.requireNonNull(code);
    }

    /** {@return which kind of feature is unsupported} */
    public UnsupportedCode code() {
        return code;
    }
}
