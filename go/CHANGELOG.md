# Changelog

All notable changes to the Go module `github.com/varveio/seekparquet/go`. The module follows semantic
versioning; while it is v0, minor versions may change the API.

## Unreleased

First public version.

- Required manifest `key` declaration and matching `StampKeyColumn`/`StampKeyUnique` footer stamps.
  `Metadata.Key` exposes `Key`; `PartsOptions.Key` configures directly opened parts.
- Non-decreasing repeated-key groups in producer file order, with earliest-candidate routing and
  whole-group bounds. Unique-key routing retains its existing fast path.
- `KeyRange` looks up a whole group. `Row.Offset`, `Cursor`, `Range.Cursor`, `List.Cursor`,
  `Row.Cursor()` and `Entry.Cursor()` support pages ending inside a group.
- `seekparquettest.Dataset.KeyColumn` and `.RepeatedKeys`, sequential oracle properties, tagged
  pagination tests and repeated-key fuzz seeds.

- Reads SeekParquet format 1, draft revision 1 (`format_version` 0, `draft_revision` 1).
- `OpenDataset` over a `Source`, with `source/local` for directories (`os.Root` confinement, a bounded
  descriptor cache, non-blocking opens that refuse anything but regular files); `OpenParts` for parts given
  directly.
- `Range` (optional prefix, inclusive or exclusive start, exclusive end), `List` (single-byte delimiter,
  `After` with `Continuation`, end), `Scan` with an error-returning `ColumnSink`.
- Projection of top-level non-repeated columns with absent distinct from null; nested and repeated columns
  rejected as unsupported.
- Refusal codes from `spec/refusal-codes.json` with structured locations; invalid requests, unsupported
  features and caller expectations reported as distinct error types.
- Opening checks CHECK-1 only, reading the manifest and the footers; reads are routed by the manifest's part
  bounds and lazily decoded row-group first keys. `Options.VerifyOnOpen` checks every row group's first key
  page and each part's last on open.
- `Limits` bounds every size a file declares (manifest, footer, page index, page header, page, value count,
  retained index memory) before allocation; Thrift metadata is walked with a bounded decoder before
  parquet-go decodes it; exceeding a limit is `ErrLimitExceeded`.
- `seekparquettest`, a test writer of conforming and deliberately invalid datasets.
- A conformance adapter (`internal/cmd/conformance`) for the reader target.
