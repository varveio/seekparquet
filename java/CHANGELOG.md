# Changelog

All notable changes to the Java implementation are recorded here. Versions follow `java/v…` tags,
independent of the specification's draft revisions.

## Unreleased

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
