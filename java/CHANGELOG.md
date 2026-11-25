# Changelog

All notable changes to the Java implementation are recorded here. Versions follow `java/v…` tags,
independent of the specification's draft revisions.

## Unreleased

The first release: nothing has been released before it. A reader for format 1, draft revision 1 (on disk
`format_version` 0, `draft_revision` 1), in package `io.varve.seekparquet`, as `io.varve:seekparquet` built
from source. Passes conformance revision `0.1.0-draft.5`, target `reader`, with the optional `verify` and
`planning` capabilities. It reads, lists and plans; it does not write or validate. The
[README](README.md#what-java-does-not-have) lists what it lacks compared with the Go module.

### Reading

- `Dataset.open` over a local directory or a `storage.DatasetStorage`; opening checks the manifest only, and
  each part is checked the first time a request touches it (CHECK-1).
- `rows`, `get`, `scan` (borrowed `Batch`es through a `BatchVisitor`) and `list` over immutable requests:
  `Span`, `Selection`, `Query` and `ListQuery`, with `next` for the following page. A common prefix carries its
  exact row (LIST-8).
- `Cursor` (sealed: `RowPosition`, `CommonPrefix`), with `encode` and `parse` implementing the shared
  [cursor token, version 1](../spec/cursor-token-1.md).
- Projection of top-level columns as sealed `Value` records, absent distinct from null, `INT96` as raw bits.
- `plan(selections, columns)`: a `Plan` with `count()` and `projectedBytes()` as `Estimate(value, exact)`,
  `scan`, and `split(n)` for scans on the caller's threads.
- `verify()` checks every part now (CHECK-3); `columns(part)` returns a part's columns with logical types;
  `rowCount()` the dataset's row total.
- `buildListIndex`, the optional index for `/` listings, with Go's defaults and byte accounting.
- `ListQuery.Builder.readAhead(n)`: a listing reads up to `n` predicted key pages ahead on virtual threads
  (default 0, off; at most 64).
- `ReadOptions`: a `Cancellation` and a consumer of the operation's `ReadStats`, delivered once on every exit.

### Limits, errors and storage

- `Limits`, built with `Limits.builder()`, with the Go reader's defaults; the fixed capacities
  `MANIFEST_NESTING` and `DATASET_ROWS`.
- A sealed, unchecked `SeekParquetException`: `RefusalException` (a `RefusalCode`, with the part, row group,
  row and key where known), `UnsupportedFeatureException`, `LimitExceededException` (a `LimitName`) and
  `StorageException`.
- `storage.DatasetStorage` and `storage.PartSource`, with `PartChangedException` for a part that changed
  while open; `storage.LocalStorage` for local directories on POSIX systems and Windows; and
  `storage.ParquetJavaInterop` to and from parquet-java's `InputFile`.
- Decodes data page versions 1 and 2, the ENC-1 baseline codecs and encodings, and `BYTE_STREAM_SPLIT`.
  `BROTLI`, LZO, the legacy `LZ4` codec, modular encryption and nested or repeated projections are reported
  as unsupported.
