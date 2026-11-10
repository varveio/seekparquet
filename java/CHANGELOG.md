# Changelog

All notable changes to the Java implementation are recorded here. Versions follow `java/v…` tags,
independent of the specification's draft revisions.

## Unreleased

### Fixed

- Malformed streams the Go reader refuses, or reads differently, are `invalid_parquet` (found by
  differential testing against the Go reader):
  - an RLE run whose value is not below 2^width in levels, dictionary indexes or Boolean values (a Boolean
    run value of 2 read as true, where Go reads false);
  - a GZIP page with bytes after its one gzip member (trailing bytes or a second member were ignored or read);
  - DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY lengths that do not sum to exactly the bytes after them
    (surplus bytes were ignored);
  - a DELTA_BINARY_PACKED first value outside INT32 range in an INT32 column or a length or prefix-length
    stream (it was narrowed, giving wrong values or keys), and a miniblock bit width above 32 in such a
    stream, now refused before decoding. Min deltas are not range checked, as in Go.
- A dictionary-encoded page whose values are all null, as parquet-go writes it (a bit width, then an
  empty run), was refused as `invalid_parquet`. An RLE/bit-packed run of no values in definition levels or
  dictionary indexes now holds nothing, as in Go: it is removed before parquet-java's decoder, which would
  repeat its value, reads the stream. Boolean values still refuse one, as Go does. Every Go-written dataset
  with an all-null dictionary-encoded page was unreadable.
- A page whose PLAIN payload held more values than its header declares (or a dictionary page more than its
  count) was read as its declared prefix; it is now `invalid_parquet`, as in Go. A PLAIN Boolean page may
  still pad its final byte.
- A page whose header carries a CRC is checked against it, as in Go.
- `Dataset.close()` from inside one of its own requests on the same thread (a scan's action) waited for
  ever; it now fails with `IllegalStateException` and changes nothing.
- A request started on another thread while `close()` waited for the requests in progress waited behind
  it, so a scan's action waiting for such a request never returned, nor did the close. A request started
  once the dataset is closing now fails at once.
- `RangePlan.split` of an empty plan, or into fewer than one shard, returned no shard for a cancelled
  split or a closed dataset; it now fails as any split does.
- `RangePlan.split` no longer rescans the whole plan for each shard, nor walks candidates linearly where Go
  searches them.
- `Dataset.columns(int)`, `columns()` and `column(name)` threw `NullPointerException` for a part with a
  column whose `logical_type` is a member this reader's parquet-format does not know (a newer annotation);
  the column's converted type, if any, now stands for it, as in Go.
- `Dataset.planRanges` with columns walked every row group of a part from its first for each range, where
  Go searches for the first: a plan of many ranges in a part of many row groups cost their product.
- A footer, page header or page index whose Thrift structure nests more than 64 deep (an unknown field
  of a few kilobytes of nested structs, which the shaded Thrift 0.23 runtime skips without its recursion
  limit) overflowed the stack: `StackOverflowError` escaped the request instead of a refusal. Nesting is
  now bounded as the Go reader's `internal/thriftcheck` bounds it, and deeper nesting is refused as
  malformed Thrift is (`invalid_footer` for a footer, `invalid_page_index` for a key page header or
  ColumnIndex, `invalid_parquet` for a value page).
- `LocalStorage`: a part or `manifest.json` replaced by a FIFO between its regular-file check and its
  open (by whoever can write the dataset directory) blocked the open until a writer appeared, and neither
  cancellation, interruption nor `Dataset.close()` could end the request; `close()` waited for ever.
  Opens now run on a thread of their own, through their own directory handle, and a cancelled request
  stops waiting; at most `LocalStorage.MAX_STRANDED_OPENS` (64) such stranded opens per storage, after
  which its opens fail with a storage error. Each part open costs a short-lived thread and a directory
  handle. `manifest.json` is now read through a pinned root handle without following a symbolic link.

### Changed

- A value column chunk's dictionary is read in one request of the bytes before the first data page its
  OffsetIndex locates, instead of a header window and then the body.
- `Column` is `(name, type, typeLength, logicalType, repetition, key, parts)`.
- `ListEntry.CommonPrefix` is `(prefix, exact)`: a common prefix carries its exact row (spec LIST-8),
  the first candidate row that rolls up to it when that row's key is the prefix itself, with its
  projected values, or empty. `new CommonPrefix(prefix)` makes one without. The conformance adapter
  reports it as the prefix entry's `exact` member.
- Lazy open (spec CHECK-1): `Dataset.open` checks the manifest only; each part is opened and checked
  the first time a request touches it (existence, exact size, footer, row counts, stamps, KEY-1/KEY-5,
  COL-5 against the parts already checked, RG-1). A part's refusal is kept for the dataset's lifetime;
  storage failures, limits and cancellation are not. Concurrent first touches wait for one check. A
  request whose bounds the manifest places before a part does not touch it.
- `Dataset.verify()` checks every part now, then each row group's first key page and each part's last
  against the manifest bounds (CHECK-3). It replaces `OpenOptions.verifyPartBounds`;
  `OpenOptions.openConcurrency` becomes `verifyConcurrency` (8 by default).
- `Dataset.columns()` describes the parts checked so far; projection of a nested, repeated or `INT96`
  column is refused when the request reaches a part that holds it so (COL-3).
- The conformance adapter claims the optional `verify` capability, and an `open` of a dataset it
  holds opens it afresh.

### Completed

- Named manifest keys (`KeyDefinition`) and exact `key_column`/`key_unique` footer stamps.
- Repeated-key groups with non-decreasing order, stable file order, group offsets on `Row`, and
  lookup through `RangeQuery.lookup` or `Dataset.lookup`.
- Tagged `Continuation.RowPosition` and `Continuation.CommonPrefix` cursors, including exact range
  and listing pagination inside groups and across row groups.
- Earliest-candidate routing through non-monotone page and row-group bounds.
- Parquet `InputFile` interoperability in `PartSource`, independent `SeekableInputStream` views,
  and an adapter for existing `InputFile` implementations. Local files remain the only supplied storage.
- Footer schema/path checks before value decoding, exact Thrift structure boundaries, and refusal
  of every `file_path` reference, including an empty one.
- Configurable result-entry limits and request caches scoped to the current row group.
- One pagination cursor per query: ranges accept a row cursor and listings accept a row or prefix
  cursor. The conformance adapter rejects signed, fractional and exponent-form offsets.
- Strict RLE run validation and configured limits on padded delta-decoder scratch allocations.
- Preservation of dictionary storage failures, cancellation and storage timeouts.
- Bounded parallel-open workers that drain all opened sources before restoring interruption.
- A dataset-wide budget for retained key and value indexes, including concurrent publication.
- Shared storage buffers remain untouched, including when reading large footers.
- Local part paths use secure directory handles and reject symlinks; interrupted readers leave
  channel repair to surviving requests.
- Updated fixtures and regression tests for grouped pagination, footer validation, storage
  interoperability, resource limits, and optional eager checks. Lazy CHECK-1 opening remains the default.
- Passes the conformance suite's `reader` target.

### Added

- Selection planning: `Dataset.planRanges(ranges, columns[, cancellation])` returns a `RangePlan` with
  `count()`/`countExact()`, `projectedBytes()`/`projectedBytesExact()`, `stats()` and `scan(...)`, which
  hands every selected row to a callback in dataset order, the semantics of the Go reader's `PlanRanges`
  (see README, Parity with the Go reader).
- `RangePlan.split(n[, cancellation])` divides a plan into at most `n` shards at selected rows that begin
  a row group, never inside a group of equal keys (repeated keys decode the key pages around each
  boundary), balancing projected bytes when exact, else rows; the shards' counts and projected bytes add up
  to the plan's, and each shard's `stats()` are the reads that placed its end. It starts no thread. Go's
  `RangePlan.Split` semantics, except that an empty plan or `n` below 1 returns an empty list where Go
  returns nil.
- `Dataset.columns(part[, cancellation])`: one part's top-level columns from the footer the dataset checked
  and holds for it. `Column` gains `logicalType()` (`LogicalType`, a sealed interface; legacy converted types
  normalized into it) and `key()`.
- `ReadStats`, per-request read statistics, delivered to a `Consumer<ReadStats>` by new `range` and `list`
  overloads and by `RangePlan.scan`, with the Go reader's fields and counting rules.
- The conformance adapter claims the optional `planning` capability and answers `plan`.
- A reader for SeekParquet format 1, draft revision 1 (on disk: `format_version` 0,
  `draft_revision` 1), in `io.varve.seekparquet`:
  - `Dataset`: opens a dataset with the CHECK-1 checks, and answers range and prefix reads
    (`RangeQuery`) and delimited listings with a tagged cursor (`ListQuery`). It checks
    each row group's key page index and each decoded key page as a request reads them (CHECK-2).
  - Immutable value types: `Bytes`, `Row`, `Value` (sealed: `Absent`, `Null` and the physical
    types), `ListEntry` (sealed: `RowEntry`, `CommonPrefix`), `Manifest`, `ManifestPart`,
    `FormatVersion`, `Column`.
  - A closed, unchecked exception hierarchy: `RefusalException` (with `RefusalCode`),
    `UnsupportedFeatureException` (with `UnsupportedCode`), `LimitExceededException`,
    `StorageException`.
  - Explicit cancellation (`Cancellation`) and configurable limits (`Limits`, `OpenOptions`).
- A storage boundary in `io.varve.seekparquet.storage`: `DatasetStorage`, `PartSource`,
  `PartChangedException`, and `LocalStorage` for local directories.
- A conformance adapter (`./gradlew :conformance:installDist`) that claims the `reader` target.
- Parquet access through parquet-java 1.18.1's Hadoop-free modules, with pure-Java decompression
  (see QUALIFICATION.md).
