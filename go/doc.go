// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package seekparquet reads and validates SeekParquet datasets: ordinary
// Parquet files sorted by a byte-string key, split into key-disjoint parts,
// and committed by manifest.json. Keys use unsigned byte order. See the
// [concepts] and [reading guide] for the model and runnable examples.
//
// # Opening
//
// [OpenDir] opens a local directory; [OpenDataset] accepts a caller-owned
// [Source]. Opening checks the manifest; parts are checked on first touch
// (CHECK-1). [Dataset.Columns] describes a checked part's columns, including
// physical [Kind], [Repetition], and [Logical] annotations. [Dataset.Metadata]
// returns an owned copy of the manifest metadata. See the [reading guide].
//
// # Reading
//
// [Dataset.Rows] returns owning [Row] values for a [Query]; [Dataset.Scan]
// visits borrowed [Batch] values. Rows requires a positive result limit;
// Scan also accepts zero to read to the span's end. [Dataset.Get] reads one
// key group up to [Limits.GetRows]. [Dataset.List] emits row entries and
// common prefixes, optionally carrying an exact row ([ListEntry.Exact]).
// Reads through keys check the indexes used and key pages decoded
// (CHECK-2), not skipped data (CHECK-4). See the [reading guide].
//
// # Validation
//
// [Dataset.Verify] checks all parts and selected boundary key pages
// (CHECK-3); it does not check every row or part digests. [Validate] and
// [ValidateDir] produce a [Report] at structural, profile, full, or digest
// levels. Unsupported features, resource limits, and storage failures leave
// a level incomplete, rather than declaring data invalid. See the
// [validation guide] for level selection and the command-line tool.
//
// # Planned selections
//
// [Dataset.Plan] resolves key selections before scanning projected values.
// [Plan.Count] and [Plan.ProjectedBytes] return estimates with exactness
// flags; inspect the flags before admission decisions. [Plan.Split] creates
// shards for caller-managed concurrent scans, keeping key groups whole at
// shard boundaries. It can return fewer shards than requested. Planning
// and splitting can perform reads. See the [reading guide].
//
// [Dataset.PlanRows] accepts known [RowSpan] boundaries produced by this
// dataset, including known [ListEntry.Rows] boundaries. Without columns it
// does no storage I/O; its count can still be provisional for unchecked
// parts. [Plan.InputRowSpans] preserves input descriptions before union
// normalization. [Plan.SelectRows] selects contained spans without I/O.
// [Plan.ScanValues] visits borrowed [ValueBatch] values without implicit key
// decoding or equal-key offsets. Unknown boundaries require resolution
// through Dataset.Plan. See the [counting guide] for complete examples.
//
// # Equal keys and pagination
//
// Equal keys form a key group in producer file order and stay within a
// part (KEY-7, PART-4). Span bounds select whole key groups, while a result
// limit can split one across pages. Set the query cursor from the last
// [Row.Cursor] or [ListEntry.Cursor] and repeat until the page is empty.
// [Cursor.Encode] and [ParseCursor] carry row or prefix cursors as stable
// version 1 tokens. Tokens bind neither query nor snapshot and provide no
// authentication. See the [pagination guide] for service envelopes.
//
// # Values
//
// A projected [Value] is absent, null, or a physical value of a [Kind].
// Absent means the part lacks the column (COL-2), distinct from a stored
// null. [ColumnView] accesses a projected column in place. Batches, views,
// keys, and value bytes are borrowed until the callback returns;
// [Batch.Row] and [Batch.Cursor] make owning copies. Nested and repeated
// projections report [UnsupportedError]. See the [reading guide].
//
// # Storage
//
// A [SourceFile] supplies a stable size and bytes from one immutable object
// version, including across concurrent reads and retries (CHECK-6).
// [Source] implementations own physical request execution, byte caching,
// and request observation. Optional [ManifestReader], [SizedSource], and
// [RangeReader] capabilities supply whole manifests, opens with expected
// sizes, and batched logical ranges. The reader discovers RangeReader on
// the original file before applying [ContextBinder] to scalar reads.
// [ErrObjectChanged] is an environment error; an initial
// [SizeMismatchError] maps to part_mismatch. See the [Source guide].
//
// # Limits and memory
//
// [Limits] bounds declared sizes, decoding, and selected metadata retention.
// Cache budgets differ from failing limits; zero fields select defaults.
// [ReadPolicy] controls tail reads and speculation. Reader windows retain
// bounded fetched, undecoded bytes per operation; decoded pages, retained
// metadata, results, and concurrent operations add memory outside that
// bound. There is no aggregate dataset memory limit or eviction of checked
// footers and open part files. See the [operating guide].
//
// # Statistics
//
// Pass an independent [ReadStats] to each operation to count logical part
// reads and format work, including on failure. Operations reset the struct;
// [ReadStats.Add] combines completed operations. Counts exclude manifest
// reads and physical requests; instrument wire bytes, retries, and storage
// byte-cache hits at the Source. Counters are diagnostics, not a format
// contract. See the [remote I/O guide].
//
// # Errors
//
// [RefusalError] and [RefusalCodeOf] identify format refusals.
// [UnsupportedError] identifies features the reader cannot handle.
// [LimitError] wraps [ErrLimitExceeded] for caller policy or reader capacity.
// [ErrInvalidRequest] covers invalid queries and malformed tokens.
// Storage errors, context cancellation, [ErrObjectChanged], and [ErrClosed]
// are environment errors. See [troubleshooting] for actions by code.
//
// # Manifests, parts and writing
//
// [ParseManifest] and [MarshalManifest] convert manifest bytes and [Manifest].
// [InspectFooter] and [InspectKeyPages] inspect a part's metadata. Package
// github.com/varveio/seekparquet/go/write writes parts; package
// github.com/varveio/seekparquet/go/local publishes them and commits a
// manifest locally. See the [producing guide].
//
// # Concurrency and cancellation
//
// A [Dataset] is safe for concurrent use; each operation has independent
// scan scratch and statistics. RangeReader receives the operation context;
// ContextBinder supplies context-bound scalar reads. A plain ReadAt cannot
// be interrupted mid-call. [Dataset.Close] cancels active operations, waits
// for reads and callbacks, and closes part files. Do not call Close from a
// scan callback or wait there for another goroutine to close it. The caller
// owns the Source. See the [operating guide].
//
// [concepts]: https://github.com/varveio/seekparquet/blob/main/docs/concepts.md
// [reading guide]: https://github.com/varveio/seekparquet/blob/main/docs/guides/reading.md
// [validation guide]: https://github.com/varveio/seekparquet/blob/main/docs/guides/validating.md
// [counting guide]: https://github.com/varveio/seekparquet/blob/main/docs/guides/counting.md
// [pagination guide]: https://github.com/varveio/seekparquet/blob/main/docs/guides/pagination.md
// [Source guide]: https://github.com/varveio/seekparquet/blob/main/docs/guides/storage-sources.md
// [operating guide]: https://github.com/varveio/seekparquet/blob/main/docs/guides/operating.md
// [remote I/O guide]: https://github.com/varveio/seekparquet/blob/main/docs/guides/remote-io.md
// [troubleshooting]: https://github.com/varveio/seekparquet/blob/main/docs/guides/troubleshooting.md
// [producing guide]: https://github.com/varveio/seekparquet/blob/main/docs/guides/producing.md
package seekparquet
