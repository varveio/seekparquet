# Changelog

All notable changes to the Go module `github.com/varveio/seekparquet/go`. The module follows semantic
versioning; while it is v0, minor versions may change the API.

## Unreleased

First public version.

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
- `seekparquettest`, a test writer of conforming and deliberately invalid datasets.
- A conformance adapter (`internal/cmd/conformance`) for the reader target.
