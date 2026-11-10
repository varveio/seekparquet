// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.time.Duration;

/**
 * A completed optional listing index build.
 * @param groupsIndexed retained row groups
 * @param groupsSkipped visited groups omitted (flat, failed, or over budget)
 * @param groupsFailed groups omitted because reading or checking them failed
 * @param partsFailed parts that could not be opened or checked
 * @param pages key pages decoded during the build
 * @param entries retained run starts, including dense-page markers
 * @param bytes conservative total retained index bytes, including containers
 * @param budgetExhausted the next group would exceed the budget, stopping admission
 * @param duration elapsed build time
 */
public record ListIndexStats(long groupsIndexed, long groupsSkipped, long groupsFailed,
        long partsFailed, long pages, long entries, long bytes, boolean budgetExhausted, Duration duration) {}
