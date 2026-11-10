# Changelog

All notable changes to the Go module `github.com/varveio/seekparquet/go`. The module follows semantic
versioning; while it is v0, minor versions may change the API.

## Unreleased

- Fix indexed listings with arbitrary prefix cursors: only slash-ended cursors use directory-run
  certification. Omit both sides of disordered group boundaries regardless of cached first keys or
  worker order, including chained violations. Cancelled builds include decoded work from the final
  batch; completed builds return stable statistics. Admit groups by their compacted retained size.

- `Dataset.BuildListIndex` explicitly builds a bounded in-memory directory-run index for `/` listings.
  Wide-prefix listings can resume from exact run anchors without decoding a page per prefix. Sparse,
  shared-prefix storage, per-page change caps and group admission keep retained memory bounded; flat
  groups retain nothing. Concurrent reads see an immutable snapshot; cancellation discards the build,
  and omitted or failed groups retain the ordinary read path. Build statistics and `ReadStats` expose
  retained bytes, entries, answered skips and decline reasons. No format or query-result change.

- The conformance adapter claims the optional `planning` capability: it answers `plan` (PlanRanges, then
  Scan, Count and ProjectedBytes) for the conformance suite's planning cases.
- `RangePlan.Split(ctx, n)` divides a selection plan into at most `n` shards along row-group boundaries,
  never inside a group of equal keys, for a caller to scan concurrently: the shards' scans together yield the
  plan's rows, each once, and their `Count` and `ProjectedBytes` add up to the plan's. Boundaries balance the
  projected bytes when exact, else the rows. Unique keys split without reading; repeated keys decode the two
  key pages around each boundary inside a part. No goroutines in the package and no format change: splitting
  is a Go convenience, not part of the specification or the conformance protocol.
- `Entry.Exact`: a common prefix entry carries its exact row (spec LIST-8), the first candidate row of the
  request that rolls up to the prefix when that row's key is the prefix itself, with its offset and projected
  values; nil otherwise. A listing pairs a prefix with the row stored at its exact key (an S3-style folder
  object) without a second request. It costs the value pages of that row only; a listing without such rows
  reads what it read before, and the group shortcut still skips the groups under a prefix. The conformance
  adapter reports it as the prefix entry's `exact` member.
- `Dataset.Columns(ctx, i)` returns part `i`'s top-level columns as `[]Column` (name, physical type and
  `FIXED_LEN_BYTE_ARRAY` length, logical type with a legacy converted type normalized into it,
  repetition, nested or not, key or not) from the footer the dataset checked and holds for the part,
  checking the part as a first read would if none has, so a caller never reopens a part to learn its
  types. `Footer.Schema` carries the same from `InspectFooter`. No format change.
- Selection planning: `Dataset.PlanRanges(ctx, ranges, columns)` returns a `RangePlan` for every row of a set
  of ranges. `Count()` is its row count, from the key pages at the ends, row-group row counts and OffsetIndex
  row positions, and reports whether it is exact (every part holding a row checked). `ProjectedBytes()` is the
  compressed bytes `Scan` will request for the projected columns, shared pages and dictionaries once, and
  reports whether it is exact (every projected chunk has an OffsetIndex). `Scan(ctx, stats, fn)` yields the
  rows in dataset order. A plan is bound to its `Dataset`. No format change.
- A page that an OffsetIndex locates is read in one request of exactly its extent instead of a 4 KiB header
  window and a second read for a larger payload, and so is a value chunk's dictionary page, as the extent
  before the chunk's first data page. `Range` and `List` still read a dictionary only for a page that uses
  it; a plan's `Scan` reads it before the chunk's first page, as `ProjectedBytes` counts it.

- `PartWriter.WritePage` writes a page column by column from the caller's own values, without copying
  them and checking only the keys (KEY-2, KEY-3): with swath's binding it writes swath's bytes at its old
  writer's speed (`BenchmarkSwathWriter`). `WriteRows` still copies, for callers that reuse their rows.
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
- `Format`, `FormatVersion`, `DraftRevision`, `ManifestName` and the five stamp keys are exported.
- `Manifest`, `ParseManifest` (the strict parser opening uses) and `MarshalManifest` (canonical JSON in
  the member order of the spec's examples, checked by the same parser).
- `InspectFooter` and `InspectKeyPages` read one part's footer and key page index, checked as a read
  checks them, without reading a data page.
- Package `write`: a streaming part writer that enforces the profile (key column, key order, page indexes
  with exact bounds on every column, `sorting_columns`, stamps, column orders) and returns the part's
  rows, size, exact bounds and SHA-256.
- Package `local`: `PublishPart` and `Commit`, durable root-confined publication (swath's protocol: sync,
  rename, sync the directory), refused where directories cannot be synced (Windows) unless the caller
  accepts a visible but unproven commit; `WriteDir` writes sorted rows as a new dataset in a directory it
  creates, its parent and root synced before any part is published.
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
