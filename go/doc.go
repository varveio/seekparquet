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
// os.Root. Opening reads and checks the manifest and nothing else
// (CHECK-1): the manifest carries each part's exact key bounds, row count
// and size, which is all a read needs to be routed. A part is checked the
// first time a read routes to it: its existence, size, footer, stamps, row
// count, key column, sorting_columns, and column consistency with the
// parts already checked (COL-5). A part that fails is refused by every
// read that touches it. Dataset.Verify checks every part at once, and
// also every row group's first key page and each part's last (CHECK-3).
// Dataset.Columns returns a part's top-level columns (physical type,
// logical type with a legacy converted type normalized into it,
// repetition, the key) from the footer the dataset checked and
// holds for it, checking the part first if no read has.
//
// Every size a file declares is checked against OpenOptions.Limits before
// anything is allocated for it, and so are the value counts and decoded
// sizes inside each page; exceeding a limit is a *LimitError wrapping
// ErrLimitExceeded.
//
// # Reading
//
// A Query reads the rows of a Selection (a Span of keys, and a Cursor to
// continue after), at most Limit of them, with the values of its Columns.
// Dataset.Rows returns them as owning rows; Dataset.Scan hands them to a
// callback as borrowed batches (Batch, ColumnView), and with a zero Limit
// scans to the end of the span. Dataset.Get returns every row of one key,
// up to Limits.GetRows. Dataset.List lists a prefix with delimiter rollups.
//
// Every read checks a row group's key page index before using the group,
// and every key page it decodes (CHECK-2): the page header matches its
// OffsetIndex entry, one non-empty key per row, in the declared order
// (strictly ascending when keys are unique), inside the page's ColumnIndex
// bounds and its part's manifest bounds, and in dataset order with every
// other page the request decoded. A fault ends the request with a
// *RefusalError carrying a stable RefusalCode.
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
// # Planned selections
//
// Dataset.Plan plans reading every row of a set of selections before
// reading it: Plan.Count is the number of rows, derived from the key pages
// at the two ends of each selection (checked as Rows checks them),
// row-group row counts and OffsetIndex row positions, and exact once every
// part holding one of the rows is checked; Plan.ProjectedBytes is the
// compressed bytes of the projected columns' pages, and dictionaries, that
// Plan.Scan will request, each extent once, so that a caller can refuse a
// read over budget before it reads a data page. Plan.Scan yields the rows
// in dataset order as borrowed batches, checking every key page it
// decodes. A count certifies only the rows at the ends it decoded
// (CHECK-4).
//
// Plan.Split divides a plan into shards along row-group boundaries,
// never inside a group of equal keys, balanced by projected bytes (or rows
// without exact bytes), for a caller to scan concurrently with its own
// goroutines: the shards' scans together yield the plan's rows, each once,
// and their counts and bytes add up to the plan's. Splitting is a
// convenience of this package over the query model, not a format rule: the
// specification and the conformance protocol do not define it. The Java
// reader has it as RangePlan.split.
//
// # Equal keys and pagination
//
// When the manifest declares keys not unique, consecutive rows with equal
// keys form a group in producer file order, and a group never crosses
// parts. Each Row carries its Offset in its group. KeySpan selects one
// whole group, and Get reads one. Every result is paginated the same way:
// set the request's Cursor to the last entry's Cursor (Row.Cursor,
// ListEntry.Cursor) and repeat it. A row cursor is the row's key and
// offset; a listing may also continue after a common prefix. A page may
// end inside a group, and a cursor deep inside a large group resumes there
// through the page index without reading the rows before it.
// Cursor.Encode and ParseCursor carry a cursor between pages as a text
// token (spec/cursor-token-1.md).
//
// # Errors
//
// A *RefusalError reports data that breaks the specification (RefusalCodeOf
// returns its code). ErrInvalidRequest reports a malformed request, such as
// a limit that is not positive, a prefix cursor on a range or a malformed
// cursor token. An *UnsupportedError reports valid data this reader does
// not handle, such as a nested projected column or an encrypted part (its
// Code is an UnsupportedCode). A *LimitError (ErrLimitExceeded) reports a
// limit of the caller's policy. Anything else (storage failures, a
// cancelled context, ErrClosed) is an error of the environment, not a
// statement about the data.
//
// # Values
//
// A projected Value is one of three states: absent, when the row's part
// lacks the column (COL-2); a null stored in the column; or a physical
// Parquet value of a Kind, read with typed accessors. Absent is not null.
// A ColumnView reads one projected column of a batch in place. Schema
// descriptions (Column) use this package's Kind, Repetition and Logical.
// No parquet-go type appears in the read API; package write takes a
// parquet-go schema.
//
// A Source may offer optional capabilities for remote storage: a whole
// manifest read (ManifestReader), part opens with the manifest's size and
// no metadata request (SizedSource), batched range reads on its files
// (RangeReader), and reports of its physical requests to the context's
// IOObserver (IOReporter), which ReadStats counts. A Source with none is
// read through Open and ReadAt. A part's first check reads its end in one
// window (ReadPolicy.MetadataPrefetchBytes) and keeps the page-index bytes
// it holds (Limits.MetadataCacheMemory), so that later index loads of the
// part read nothing.
//
// # Read statistics
//
// Passing a *ReadStats to a read (Rows, Get, Scan, List, Plan, Split,
// Plan.Scan) makes it report the pages, keys, row groups and Source reads
// it used, exactly, even when it fails. They are diagnostics, not part of
// the format contract.
//
// # Manifests, parts and writing
//
// ParseManifest and MarshalManifest read and write a manifest.json as a
// Manifest; InspectFooter and InspectKeyPages read one part's footer and
// key page index without its pages. Package write writes part files and
// package local publishes them and commits the manifest.
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
