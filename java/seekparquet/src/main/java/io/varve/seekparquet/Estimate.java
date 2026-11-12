// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

/**
 * A count a {@link Plan} reports, and whether it is exact: the Go reader returns the pair as {@code (n,
 * exact)}. An inexact row count relies on unchecked manifest counts. Inexact projected bytes omit chunks
 * without indexes. Neither is a guaranteed upper bound.
 *
 * @param value the count
 * @param exact whether it is exact
 */
public record Estimate(long value, boolean exact) {}
