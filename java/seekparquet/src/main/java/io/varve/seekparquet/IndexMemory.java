// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/** Dataset-wide accounting for retained indexes, guarded by this object's monitor. */
final class IndexMemory {
    private final long maximum;
    private long retained;

    IndexMemory(long maximum) {
        this.maximum = maximum;
    }

    void reserve(long bytes) {
        if (bytes > maximum - retained) {
            throw new LimitExceededException(LimitName.INDEX_MEMORY, bytes > Long.MAX_VALUE - retained ? Long.MAX_VALUE : retained + bytes,
                    maximum, "retained page indexes");
        }
        retained += bytes;
    }
}
