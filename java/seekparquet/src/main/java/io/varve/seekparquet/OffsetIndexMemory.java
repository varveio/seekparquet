// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Dataset-wide accounting for kept value-column OffsetIndex page locations ({@link
 * Limits#offsetIndexMemory()}), apart from {@link IndexMemory}. Lock-free.
 */
final class OffsetIndexMemory {
    private final long maximum;
    private final AtomicLong used = new AtomicLong();

    OffsetIndexMemory(long maximum) {
        this.maximum = maximum;
    }

    /** {@return the bytes charged for keeping {@code index}: 32 per page plus 64, as in Go} */
    static long charge(ValueIndex index) {
        return 32L * index.firstRow.length + 64;
    }

    /**
     * Publishes {@code index} to slot {@code i} of {@code slots}, for later requests to share, when the
     * budget has room for it. Only checked indexes reach it. When a concurrent first use published first,
     * the charge is refunded and the slot keeps that one.
     *
     * @return whether the slot now holds an index; false when the budget has no room
     */
    boolean keep(AtomicReferenceArray<ValueIndex> slots, int i, ValueIndex index) {
        long size = charge(index);
        while (true) {
            long current = used.get();
            if (size > maximum - current) {
                return false;
            }
            if (used.compareAndSet(current, current + size)) {
                break;
            }
        }
        if (!slots.compareAndSet(i, null, index)) {
            used.addAndGet(-size);
        }
        return true;
    }

    long used() {
        return used.get();
    }
}
