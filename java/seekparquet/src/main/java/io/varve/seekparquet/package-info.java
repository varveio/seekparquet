// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

/**
 * A reader for SeekParquet, a sorted-key profile for Apache Parquet (format 1, draft revision 1).
 *
 * <p>{@link io.varve.seekparquet.Dataset} opens a dataset and answers range requests ({@link
 * io.varve.seekparquet.Query} over a {@link io.varve.seekparquet.Span}: {@code rows}, {@code scan},
 * {@code get}), planned selections ({@link io.varve.seekparquet.Plan}) and {@link
 * io.varve.seekparquet.ListQuery delimited listings}. Keys and binary values are {@link
 * io.varve.seekparquet.Bytes}; rows are {@link io.varve.seekparquet.Row}s of {@link
 * io.varve.seekparquet.Value}s; listing entries are {@link io.varve.seekparquet.ListEntry}s; positions to
 * continue from are {@link io.varve.seekparquet.Cursor}s. Scans hand rows to a {@link
 * io.varve.seekparquet.BatchVisitor} as borrowed {@link io.varve.seekparquet.Batch}es, read without an
 * object per row. Domain failures are {@link io.varve.seekparquet.SeekParquetException}s: a refusal of
 * the data with its stable {@link io.varve.seekparquet.RefusalCode}, an unsupported feature, a limit, or
 * a storage failure; a malformed request is an {@link IllegalArgumentException}.
 *
 * <p>The public API never returns or accepts {@code null}; optional values are {@link
 * java.util.Optional}. Storage is reached through {@link io.varve.seekparquet.storage}.
 */
package io.varve.seekparquet;
