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

    private UnsupportedFeatureException(UnsupportedFeatureException other) {
        super(other.getMessage(), null);
        this.code = other.code;
    }

    /** {@return a detached copy: the same code and message, no suppressed exceptions} */
    UnsupportedFeatureException detached() {
        return new UnsupportedFeatureException(this);
    }

    /** {@return which kind of feature is unsupported} */
    public UnsupportedCode code() {
        return code;
    }
}
