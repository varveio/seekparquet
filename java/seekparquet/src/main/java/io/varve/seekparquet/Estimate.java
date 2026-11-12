// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * A count a {@link Plan} reports, and whether it is exact: the Go reader returns the pair as {@code (n,
 * exact)}. An inexact count is incomplete accounting (see {@link Plan#count()} and {@link
 * Plan#projectedBytes()}), not a safe upper bound.
 *
 * @param value the count
 * @param exact whether it is exact
 */
public record Estimate(long value, boolean exact) {}
