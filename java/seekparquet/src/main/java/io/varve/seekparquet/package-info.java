// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

/**
 * A reader for SeekParquet, a sorted-key profile for Apache Parquet (format 1, draft revision 1).
 *
 * <p>{@link io.varve.seekparquet.Dataset} opens a dataset and answers {@link
 * io.varve.seekparquet.RangeQuery range reads} and {@link io.varve.seekparquet.ListQuery delimited
 * listings}. Keys and binary values are {@link io.varve.seekparquet.Bytes}; rows are {@link
 * io.varve.seekparquet.Row}s of {@link io.varve.seekparquet.Value}s; listing entries are {@link
 * io.varve.seekparquet.ListEntry}s. {@link io.varve.seekparquet.Dataset#rangeBatches} and {@link
 * io.varve.seekparquet.RangePlan#scanBatches} hand the same rows to a callback as borrowed {@link
 * io.varve.seekparquet.Batch}es, read without an object per row. Failures are {@link io.varve.seekparquet.SeekParquetException}s:
 * a refusal of the data with its stable {@link io.varve.seekparquet.RefusalCode}, an unsupported
 * feature, a configured limit, or a storage failure.
 *
 * <p>The public API never returns or accepts {@code null}; optional values are {@link
 * java.util.Optional}. Storage is reached through {@link io.varve.seekparquet.storage}.
 */
package io.varve.seekparquet;
