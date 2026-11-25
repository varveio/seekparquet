# Changelog

All notable changes to the Go module `github.com/varveio/seekparquet/go`. The module follows semantic
versioning; while it is v0, minor versions may change the API.

## Unreleased

### Changed

- A scan decompresses zstd pages with a decoder of its own scratch, one per projected column, instead of
  one decoder shared by the process, whose every decode passed through a single channel. Scans of more than
  about four workers no longer queue on it: on a dataset of 57.9M rows in 184 row groups, 18 workers scan
  three value columns in 0.37 s instead of 0.65 s, and `Aggregate` with `CheckKeys` takes 0.67 s instead
  of 0.85 s. One to four workers are unchanged. A read without scratch keeps the shared decoder.

## v0.2.0 (2026-11-15)

Changes since `go/v0.1.0`. Implements format 1, draft revision 1 (on disk `format_version` 0,
`draft_revision` 1). Passes conformance revision `0.1.0-draft.5`, targets `reader`, `validator-structural`,
`validator-profile`, `validator-full` and `validator-digest`, with the optional `verify` and `planning`
capabilities. No format change. The first release that writes files of draft revision 1, so that revision is
no longer edited in place ([VER-2](../spec/format-1.md#ver-2)).

### Breaking

- The read API is redesigned, with no deprecated aliases:
  - `Range`, `KeyRange` and `PrefixRange` become `Span` (with `KeySpan` and `PrefixSpan`), `Selection` (a
    span and a row cursor) and `Query` (a selection, an `int64` `Limit` and `Columns`).
  - `Dataset.Range` becomes `Dataset.Rows`; `List` becomes `ListQuery`, `Entry` `ListEntry`, `Options`
    `OpenOptions`.
  - Every read takes a `*ReadStats` argument, reset on entry and filled on every exit; pass nil to discard.
  - `Value` is an opaque type with typed accessors, `Kind`, `IsNull`, `Absent` and `Clone`; an absent value is
    not null. No parquet-go type remains in the read API.
  - `Code` and `CodeOf` become `RefusalCode` and `RefusalCodeOf`; `UnsupportedError.Code` is an
    `UnsupportedCode`. Limit failures are `*LimitError` (limit name, value, maximum), wrapping
    `ErrLimitExceeded`.
- A negative `Limits` field is `ErrInvalidRequest` at open instead of taking the default.
- Opening reads and checks the manifest only. A part is opened and checked the first time a read touches it
  ([CHECK-1](../spec/format-1.md#check-1)), so a part's refusal comes from that read, and the part stays
  refused. A custom `Source` must stay usable until `Close`.
- `Options.VerifyOnOpen` is removed: call `Dataset.Verify` after opening.

### Added

- Reads: `Dataset.Get` for one key group, bounded by `Limits.GetRows`; `Dataset.Scan` with borrowed `Batch`es
  and `ColumnView`s, and `Stop`; `ListEntry.Exact`, the row stored at a common prefix's own key (LIST-8);
  `Dataset.Columns`, a part's typed columns (`Column` with `Kind`, `Repetition` and `Logical`).
- Planning: `Dataset.Plan` and `Plan` (`Count`, `ProjectedBytes`, `Columns`, `Scan`, `Split`), and
  `Plan.ScanParallel`, a queued scan of shards on several goroutines.
- Row positions: `RowPosition`, `RowSpan`, `ListEntry.Rows`, `Dataset.PlanRows`, `Plan.InputRowSpans`,
  `Plan.SelectRows`, `Plan.ScanValues` and `ValueBatch`, to count listed spans without storage reads and read
  values by position.
- Aggregation: `Dataset.Aggregate` and `AggregateRows` with `Reduction`s (`CountRows`, `Sum`, `Max`,
  `CountNulls`, `CountAbsent`), exactness statuses, value budgets and a `BatchFilter`; `Dataset.Page`, a
  listing page with aggregates for each complete common prefix.
- Cursor tokens: `Cursor.Encode`, `ParseCursor` and `MaxCursorToken` implement the stable
  [cursor token, version 1](../spec/cursor-token-1.md); `RowCursor` and `PrefixCursor` build cursors.
- `Dataset.Verify` checks every part, row-group first key page and part end now (CHECK-3).
- `Dataset.BuildListIndex`, an optional bounded index for `/` listings (`ListIndexOptions`,
  `ListIndexStats`).
- Remote storage: the optional `ManifestReader`, `SizedSource` and `RangeReader` capabilities (with
  `BatchError`, `MaxReadRanges`, `MaxReadBatchBytes`), `ErrObjectChanged`, `ErrObjectTooLarge`,
  `SizeMismatchError` and `Metadata.ManifestIdentity`; `OpenOptions.Read`, a `ReadPolicy` with
  `MetadataPrefetchBytes` and `DisableSpeculation`. A size mismatch is refused `part_mismatch`, a missing
  object `missing_part`; a broken pin stays an environment error.
- `ListQuery.ReadAhead`: over a non-local Source, a listing predicts and reads ahead the key pages and key
  page indexes it needs next, across row groups, and routes forward seeks by those indexes. Zero selects
  adaptive prediction; zero-policy `OpenDir` reads on demand. Results are the same with or without it.
- `ReadStats` counts key and value pages decoded, keys, row groups, index loads, seeks, prefix skips, list
  index skips and declines, logical source reads and bytes, read-ahead, batching and metadata reuse.
- Limits: `PageMemory`, `OffsetIndexMemory`, `PageIndexCacheMemory` and `GetRows`; the fixed capacities
  `LimitManifestNesting` (512) and `LimitDatasetRows` (2^63-1).
- Writing: package `write` (`NewPart`, `PartWriter` with `WriteRows`, `WritePage` and `Seal`) enforces the
  profile; package `local` (`WriteDir`, `PublishPart`, `Commit`) publishes durably in a local directory.
- Manifests: `Manifest`, `ParseManifest`, `MarshalManifest` and `PartInfo.Extensions`; `InspectFooter` and
  `InspectKeyPages`; the constants `Format`, `FormatVersion`, `DraftRevision`, `ManifestName` and the stamp
  keys.
- Validation at the four levels of §8: `Validate`, `ValidateDir`, `ValidateOptions`, `Level` and `Report`,
  `CodeDigestMismatch`, and the command `seekparquet validate [--level LEVEL] [--json] DIR`.

### Changed

- A part's first touch reads its end in one tail read (2 MiB; 64 KiB through `OpenDir`), with a 4-byte read
  of its head only when the part is larger, and keeps the page-index bytes it holds. A page or dictionary that
  an OffsetIndex locates is read in one request of exactly its extent, and a projected chunk's checked
  OffsetIndex is kept for later requests.

### Fixed

- Malformed pages that were read wrongly, or crashed the process, are `invalid_parquet`: a non-Boolean page
  labelled RLE; a PLAIN `BYTE_ARRAY` value overrunning its page; an RLE or bit-packed run whose value does not
  fit its width, or that holds more values than the page; a definition level above 1; unchecked V2
  repetition-level bytes, `REQUIRED` definition-level bytes or `num_nulls`; a chunk without an OffsetIndex
  holding a page of no rows or past its row group; delta lengths that do not cover exactly their bytes; a
  `DELTA_BINARY_PACKED` miniblock above 32 bits in an INT32 stream; a payload holding more values than its
  header declares; a `FIXED_LEN_BYTE_ARRAY` page that is not a whole number of values. A column chunk whose
  metadata names another path or type than its schema leaf is `invalid_footer`.
- Every CRC a page header carries is checked, a zero CRC included.
- A page whose OffsetIndex entry disagrees with its header's rows is refused for RG-3, not reported as a limit.
- Valid files that were refused are read: a dictionary-encoded page of only nulls with no index bytes; a
  `sorting_columns` entry after the first that names no column; row-group ordinals that differ from the row
  groups' positions.
- A manifest nested deeper than 512 is a limit, not unbounded recursion.
- A request started while `Close` waits fails with `ErrClosed` instead of waiting behind it, which could hang
  a request made inside a callback.

## v0.1.0 (2026-11-08)

First public version. Implements format 1 draft revision 1; passes conformance revision `0.1.0-draft.4`.

- Reads SeekParquet format 1, draft revision 1 (`format_version` 0, `draft_revision` 1).
- One public package. `OpenDataset` opens a dataset through a `Source`; `OpenDir` opens a local
  directory through an `os.Root`, refusing anything but regular files without blocking on them.
  `Dataset` serves `Range`, `List`, `Metadata` and `Close`.
- The manifest's `key` declaration names the key column and whether keys are unique; the footer stamps
  must match it, and `Metadata.Key` exposes it. Equal keys form groups in producer file order that never
  cross parts, on every open path. `KeyRange` looks up a whole group.
- One pagination contract: `Range.Cursor` and `List.Cursor`, built by `Row.Cursor()` and
  `Entry.Cursor()`. A row cursor is a key and its `Row.Offset` in its group; a listing may also continue
  after a common prefix. Resuming inside a large group seeks through the OffsetIndex row positions, so a
  page costs a few page reads however deep its cursor lies.
- `Range` (optional prefix, inclusive or exclusive start, exclusive end) and `List` (single-byte
  delimiter, end) share one traversal, which counts offsets once.
- Projection of top-level non-repeated columns with absent distinct from null; nested and repeated
  columns rejected as unsupported.
- Refusal codes from `spec/refusal-codes.json` with structured locations; invalid requests and
  unsupported features reported as distinct error types.
- Opening checks CHECK-1 only, reading the manifest and the footers; reads are routed by the manifest's
  part bounds and lazily decoded row-group first keys. `Options.VerifyOnOpen` checks every row group's
  first key page and each part's last on open.
- `Limits` bounds every size a file declares (manifest, footer, page index, page header, page, value
  count, retained index memory) before allocation. Thrift metadata is walked with a bounded decoder,
  nesting depth included, before it is decoded. Pages are located and decoded by the reader itself: a
  page must decompress to exactly its declared size (decompression stops beyond it), the counts its
  encodings declare are checked against its header, and its decoded size against `Limits.PageBytes`.
  Exceeding a limit is `ErrLimitExceeded`.
- A conformance adapter (`internal/cmd/conformance`) for the reader target.
