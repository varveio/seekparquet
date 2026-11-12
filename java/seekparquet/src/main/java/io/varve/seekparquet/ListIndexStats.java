// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.time.Duration;

/**
 * A completed optional listing index build.
 * @param groupsIndexed retained row groups
 * @param groupsSkipped visited groups omitted (flat, failed, or over budget)
 * @param groupsFailed groups omitted for data refusals (not transient failures)
 * @param partsFailed parts omitted for data refusals
 * @param pages key pages decoded during the build
 * @param entries retained run starts, including dense-page markers
 * @param bytes conservative total retained index bytes, including containers
 * @param budgetExhausted the next group would exceed the budget, stopping admission
 * @param duration elapsed build time
 * @param options effective build options
 * @param optionsMismatch a nonempty index was reused with different requested options
 */
public record ListIndexStats(long groupsIndexed, long groupsSkipped, long groupsFailed,
        long partsFailed, long pages, long entries, long bytes, boolean budgetExhausted, Duration duration, ListIndexOptions options, boolean optionsMismatch) {
    /**
     * Creates build statistics.
     *
     * @throws NullPointerException if {@code duration} or {@code options} is null
     */
    public ListIndexStats {
        java.util.Objects.requireNonNull(duration, "duration");
        java.util.Objects.requireNonNull(options, "options");
    }

    /** {@return whether the completed snapshot indexes any groups} */
    public boolean hasIndex() { return groupsIndexed > 0; }
    ListIndexStats forOptions(ListIndexOptions requested) {
        boolean mismatch = !options.equals(requested);
        return mismatch == optionsMismatch ? this : new ListIndexStats(groupsIndexed, groupsSkipped, groupsFailed,
                partsFailed, pages, entries, bytes, budgetExhausted, duration, options, mismatch);
    }

    ListIndexStats(long groupsIndexed, long groupsSkipped, long groupsFailed, long partsFailed,
            long pages, long entries, long bytes, boolean budgetExhausted, Duration duration, ListIndexOptions options) {
        this(groupsIndexed, groupsSkipped, groupsFailed, partsFailed, pages, entries, bytes, budgetExhausted, duration, options, false);
    }
}
