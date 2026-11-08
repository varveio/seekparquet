// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;
import java.util.Optional;

/**
 * The data breaks the specification. The {@linkplain #code() code} is stable; the message is
 * diagnostic text and may change between releases.
 */
public final class RefusalException extends SeekParquetException {

    private static final long serialVersionUID = 1L;

    /** The refusal code. */
    private final RefusalCode code;

    /** The part at fault, or null. */
    private final String part;

    /**
     * Creates a refusal.
     *
     * @param code the refusal code
     * @param part the manifest path of the part at fault, or {@code null} when the fault is not in
     *     one part
     * @param detail a diagnostic message
     */
    RefusalException(RefusalCode code, String part, String detail) {
        super(code.id() + (part == null ? "" : " in part " + part) + ": " + detail, null);
        this.code = Objects.requireNonNull(code);
        this.part = part;
    }

    /** {@return the refusal code} */
    public RefusalCode code() {
        return code;
    }

    /** {@return the manifest path of the part at fault, when the fault lies in one part} */
    public Optional<String> part() {
        return Optional.ofNullable(part);
    }
}
