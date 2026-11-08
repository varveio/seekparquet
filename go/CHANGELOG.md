# Changelog

All notable changes to the Go module `github.com/varveio/seekparquet/go`. The module follows semantic
versioning; while it is v0, minor versions may change the API.

## Unreleased

- Lazy open (spec CHECK-1 as now written): `OpenDataset` and `OpenDir` read and check the manifest only,
  and open no part. A part is opened and checked (size, footer, stamps, row count, key column, RG-1, and
  COL-5 against the parts already checked) the first time a read routes to it; a refusal surfaces from
  that `Range` or `List` with the same codes as before, and the part stays refused. `OpenDir` keeps the
  directory open until `Close`; a custom `Source` must stay usable until `Close`.
- `Dataset.Verify` checks every part now, then every row group's first key page and each part's last
  (CHECK-3). It replaces `Options.VerifyOnOpen`, which is removed: call `Verify` after opening.
- `ReadStats.PartsOpened` counts the parts a request was the first to touch; their footer reads are in
  `SourceReads` and `SourceBytes`.
- The structural validation level checks every part explicitly, as `Verify` does without key pages.
- `Range.Stats` and `List.Stats` take a `*ReadStats` that the reader fills with the request's exact
  counts: key and value pages decoded, keys decoded, row groups opened, key page index loads, page-index
  seeks, prefix resumes, row groups skipped whole, and `Source` reads and requested bytes.
- Validation at the specification's four levels: `Validate` over a `Source`, `ValidateDir` over a local
  directory, with `ValidateOptions{Level, Limits}` and a `Report` of each level's result, refusal code and
  location, or the reason it is incomplete. Profile and full validation walk every column chunk page by
  page from its start and compare the indexes with the pages found. `CodeDigestMismatch` is new.
- Command `cmd/seekparquet`: `seekparquet validate [--level LEVEL] [--json] DIR`.
- The conformance adapter claims the four validator targets.

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
