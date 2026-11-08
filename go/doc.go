// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package seekparquet reads and validates SeekParquet datasets: ordinary
// Parquet files sorted by one manifest-named byte-string key column, split
// into key-disjoint parts and committed by a manifest.json (SeekParquet
// format 1, draft revision 1, stamped format_version 0).
//
// A read is routed to a part by its manifest bounds and to a row group by
// the decoded first keys of the row groups; the key column's ColumnIndex
// and OffsetIndex locate the page that can hold the start; only that page
// onward is decoded, in unsigned byte order of the keys. Keys are bytes;
// nothing assumes UTF-8 unless the key column is annotated STRING.
//
// # Opening
//
// OpenDataset opens a dataset through a Source, a small interface any
// storage can implement; OpenDir opens a local directory through an
// os.Root. Opening checks what the specification requires (CHECK-1): the
// manifest, every part's existence, size, footer, stamps, row count, key
// column, column consistency (COL-5) and sorting_columns. It reads the
// manifest and the footers and nothing else, unless Options.VerifyOnOpen
// asks it also to check every row group's first key page and each part's
// last (CHECK-3).
//
// Every size a file declares is checked against Options.Limits before
// anything is allocated for it, and so are the value counts and decoded
// sizes inside each page; exceeding a limit is ErrLimitExceeded.
//
// # Reading
//
// Dataset.Range and Dataset.List check a row group's key page index before
// using the group, and every key page they decode (CHECK-2): the page
// header matches its OffsetIndex entry, one non-empty key per row, in the
// declared order (strictly ascending when keys are unique), inside the
// page's ColumnIndex bounds and its part's manifest bounds, and in dataset
// order with every other page the request decoded. A fault ends the
// request with a *RefusalError carrying a stable Code.
//
// What is not certified: rows a request does not decode (CHECK-4), values
// beyond decoding them, the parts' SHA-256 digests, and that the files are
// unchanged after opening. Full certification is validation.
//
// # Validation
//
// Validate and ValidateDir check a dataset at the specification's levels
// (§8): structural, profile and full, each requiring the one before, and
// digest on its own. Every column chunk a level checks is walked page
// header by page header from its start, and its indexes are compared with
// the pages found, so that no index decides what is visited. The Report
// says, per level, whether it completed, failed (with the refusal code and
// where) or is incomplete: an unsupported feature, a limit or a storage
// error, never a statement about the data.
//
// # Equal keys and pagination
//
// When the manifest declares keys not unique, consecutive rows with equal
// keys form a group in producer file order, and a group never crosses
// parts. Each Row carries its Offset in its group. KeyRange looks up one
// whole group. Every result is paginated the same way: set the request's
// Cursor to the last entry's Cursor (Row.Cursor, Entry.Cursor) and repeat
// it. A row cursor is the row's key and offset; a listing may also
// continue after a common prefix. A page may end inside a group, and a
// cursor deep inside a large group resumes there through the page index
// without reading the rows before it.
//
// # Errors
//
// A *RefusalError reports data that breaks the specification (CodeOf
// returns its code). ErrInvalidRequest reports a malformed request, such as
// a limit that is not positive or a prefix cursor on a range. An
// *UnsupportedError reports valid data this reader does not handle, such
// as a nested projected column or an encrypted part. Anything else
// (storage failures, a cancelled context, ErrClosed, ErrLimitExceeded) is
// an error of the environment or of the caller's policy, not a statement
// about the data.
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
// A Dataset is safe for concurrent use; page indexes are loaded once per
// row group and shared. Every source read is bounded by the request's
// context when the part file implements ContextBinder; a plain io.ReaderAt
// cannot be interrupted mid-call, and is checked between pages. Close ends
// the reads in progress at their next cancellation point, waits for them,
// and closes the part files.
package seekparquet
