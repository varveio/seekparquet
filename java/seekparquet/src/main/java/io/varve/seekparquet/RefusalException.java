// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * The data breaks the specification. The {@linkplain #code() code} is stable; the message is
 * diagnostic text and may change between releases. Besides the code, a refusal carries the facts
 * known where it was met, for diagnosis: the {@linkplain #part() part}, the {@linkplain #rowGroup()
 * physical row group} in it, the {@linkplain #row() row} in that row group and the {@linkplain
 * #key() key}. Each is empty when the fault is not one part's, row group's, row's or key's, or when
 * the reader does not have the value in hand where it refuses.
 */
public final class RefusalException extends SeekParquetException {

    private static final long serialVersionUID = 1L;

    /** The refusal code. */
    private final RefusalCode code;

    /** The part at fault, or null. */
    private final String part;

    /** The physical row group at fault, or null, including in streams written before locations existed. */
    private final Integer rowGroup;

    /** The 0-based row within the row group, or null. */
    private final Long row;

    /** The offending key, or null. */
    private final byte[] key;

    /**
     * Creates a refusal.
     *
     * @param code the refusal code
     * @param part the manifest path of the part at fault, or {@code null} when the fault is not in
     *     one part
     * @param detail a diagnostic message
     */
    RefusalException(RefusalCode code, String part, String detail) {
        this(code, part, detail, -1, -1, null);
    }

    /**
     * Creates a refusal located in a row group, and perhaps at a row and key of it.
     *
     * @param code the refusal code
     * @param part the manifest path of the part at fault, or {@code null}
     * @param detail a diagnostic message
     * @param rowGroup the physical row group in the part, or -1 when not one row group's
     * @param row the 0-based row within the row group, or -1 when not one row's
     * @param key the offending key, copied, or {@code null} when none
     */
    RefusalException(RefusalCode code, String part, String detail, int rowGroup, long row, byte[] key) {
        this(code, part, part, detail, rowGroup, row, key);
    }

    /**
     * Creates a refusal whose message names {@code messagePart}, which may differ from {@code part},
     * the part its location fields are in.
     */
    RefusalException(RefusalCode code, String part, String messagePart, String detail, int rowGroup, long row, byte[] key) {
        super(code.id() + (messagePart == null ? "" : " in part " + messagePart) + ": " + detail, null);
        this.code = Objects.requireNonNull(code);
        this.part = part;
        this.rowGroup = part == null || rowGroup < 0 ? null : rowGroup;
        this.row = this.rowGroup == null || row < 0 ? null : row;
        this.key = key == null ? null : key.clone();
    }

    /**
     * A detached copy of {@code other}: the same code, message and location, with no suppressed exceptions
     * and a stack trace of its own, for a dataset to throw per request from a refusal it keeps.
     */
    private RefusalException(RefusalException other) {
        super(other.getMessage(), null);
        this.code = other.code;
        this.part = other.part;
        this.rowGroup = other.rowGroup;
        this.row = other.row;
        this.key = other.key;
    }

    /** {@return a detached copy of this refusal} */
    RefusalException detached() {
        return new RefusalException(this);
    }

    /** {@return the refusal code} */
    public RefusalCode code() {
        return code;
    }

    /** {@return the manifest path of the part at fault, when the fault lies in one part} */
    public Optional<String> part() {
        return Optional.ofNullable(part);
    }

    /**
     * {@return the physical row group within the {@linkplain #part() part} at fault, counted in the
     * part's footer, when the fault lies in one row group}
     */
    public OptionalInt rowGroup() {
        return rowGroup == null ? OptionalInt.empty() : OptionalInt.of(rowGroup);
    }

    /**
     * {@return the 0-based row within the {@linkplain #rowGroup() row group} at fault, when the fault
     * lies at one row}
     */
    public OptionalLong row() {
        return row == null ? OptionalLong.empty() : OptionalLong.of(row);
    }

    /**
     * {@return the offending key, when there is one: for {@link RefusalCode#KEY_ORDER}, the key that is
     * below the key before it, or equal to it when keys are unique}
     */
    public Optional<Bytes> key() {
        return Optional.ofNullable(key).map(Bytes::of);
    }
}
