// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package seekparquet reads SeekParquet datasets: ordinary Parquet files
// sorted by one byte-string key column named "key", split into key-disjoint
// parts and committed by a manifest.json (SeekParquet format 1, draft
// revision 1, stamped format_version 0).
//
// A read is routed to a part and row group by the decoded first key of
// every row group; the key column's ColumnIndex and OffsetIndex locate the
// page that can hold the start; only that page onward is decoded, in
// unsigned byte order of the keys. Keys are bytes; nothing assumes UTF-8
// unless the key column is annotated STRING.
//
// # Opening
//
// OpenDataset opens a dataset through a Source (source/local provides one
// for directories) and checks what the specification requires on open
// (CHECK-1): the manifest, every part's existence, size, footer, stamps,
// row count, key column, column consistency (COL-5) and sorting_columns.
// It also decodes the first page of every non-empty row group, after
// checking that row group's key page index, and each part's last page, and
// requires each part's first and last keys to equal the manifest's bounds.
// OpenParts is the advanced path over parts given directly.
//
// # Reading
//
// Range, List and Scan check every key page they decode (CHECK-2): the page
// header matches its OffsetIndex entry, one non-empty key per row, strictly
// ascending, inside the page's ColumnIndex bounds, and in dataset order with
// every other page the request decoded. A fault ends the request with a
// *RefusalError carrying a stable Code.
//
// What is not certified: rows a request does not decode (CHECK-4), values
// beyond decoding them, the parts' SHA-256 digests, and that the files are
// unchanged after opening. Full certification is validation, which this
// package does not do.
//
// # Errors
//
// A *RefusalError reports data that breaks the specification (CodeOf
// returns its code). ErrInvalidRequest reports a malformed request, such as
// a limit that is not positive. An *UnsupportedError reports valid data
// this reader does not handle, such as a nested projected column or an
// encrypted part. An *ExpectationError reports a part that fails a caller's
// ColumnExpectation. Anything else (storage failures, a cancelled context,
// ErrClosed) is an error of the environment, not a statement about the
// data.
//
// # Values
//
// Projected values are parquet-go values (github.com/parquet-go/parquet-go
// Value): this package commits to parquet-go's types instead of defining
// its own, so its API moves with parquet-go's. A Value additionally reports
// whether the row's part lacks the column (Absent), which is distinct from
// null (COL-2).
//
// # Concurrency and cancellation
//
// A Reader or Dataset is safe for concurrent use; page indexes are loaded
// once per row group and shared. Every source read is bounded by the
// request's context when the part file implements ContextBinder; a plain
// io.ReaderAt cannot be interrupted mid-call, and is checked between pages.
// Close ends the reads in progress at their next cancellation point, waits
// for them, and closes the part files.
package seekparquet
