// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.List;
import java.util.OptionalLong;

/**
 * A (format_version, draft_revision) pair (spec §9). Datasets written to a draft carry {@code
 * format_version} 0 and a draft revision; the frozen format will carry {@code format_version} 1 and
 * none.
 *
 * @param formatVersion the format version
 * @param draftRevision the draft revision, present exactly when {@code formatVersion} is 0
 */
public record FormatVersion(long formatVersion, OptionalLong draftRevision) {

    /** Format 1, draft revision 1, stamped on disk as {@code format_version} 0, draft revision 1. */
    public static final FormatVersion DRAFT_1 = new FormatVersion(0, OptionalLong.of(1));

    /** The pairs this reader supports (VER-1). */
    public static final List<FormatVersion> SUPPORTED = List.of(DRAFT_1);

    /**
     * Creates a version pair.
     *
     * @param formatVersion the format version, at least 0
     * @param draftRevision the draft revision, at least 1, present exactly when {@code formatVersion}
     *     is 0
     */
    public FormatVersion {
        if (formatVersion < 0) {
            throw new IllegalArgumentException("format_version " + formatVersion + " is negative");
        }
        if ((formatVersion == 0) != draftRevision.isPresent()) {
            throw new IllegalArgumentException("a draft_revision is present exactly when format_version is 0");
        }
        if (draftRevision.isPresent() && draftRevision.getAsLong() < 1) {
            throw new IllegalArgumentException("draft_revision " + draftRevision.getAsLong() + " is below 1");
        }
    }

    /** {@return whether this reader supports the pair} */
    public boolean isSupported() {
        return SUPPORTED.contains(this);
    }

    @Override
    public String toString() {
        return "format_version " + formatVersion + (draftRevision.isPresent() ? " draft_revision " + draftRevision.getAsLong() : "");
    }
}
