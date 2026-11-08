# Changelog

All notable changes to the Java implementation are recorded here. Versions follow `java/v…` tags,
independent of the specification's draft revisions.

## Unreleased

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
- Updated fixtures and regression tests for grouped pagination, footer validation, storage
  interoperability, resource limits, and optional eager checks. Lazy CHECK-1 opening remains the default.
- Complete reader conformance: 1,379 passed, zero failed, zero skipped reader cases.

### Added

- A reader for SeekParquet format 1, draft revision 1 (on disk: `format_version` 0,
  `draft_revision` 1), in `io.varve.seekparquet`:
  - `Dataset`: opens a dataset with the CHECK-1 checks, and answers range and prefix reads
    (`RangeQuery`) and delimited listings with `after` and `continuation` (`ListQuery`). It checks
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
