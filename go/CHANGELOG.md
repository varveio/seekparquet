# Changelog

All notable changes to the Go module `github.com/varveio/seekparquet/go`. The module follows semantic
versioning; while it is v0, minor versions may change the API.

## Unreleased

- Added `Dataset.RangeBatches(ctx, rng, func(*Batch) error)`: a limited range read as borrowed
  `Batch`es, with no plan. It returns the rows, keys, values and cursors `Range` returns and reads
  what it reads (equal `ReadStats` for a successful, fully consumed request against the same cache
  state; a request ended by an error, a callback stop or cancellation can differ), opening projected chunks and dictionaries only as the rows reach
  them, and stops at `Limit` without advancing to the next row. A nil callback is
  `ErrInvalidRequest`; callback errors, cancellation, Close and borrowing follow `ScanBatches`.
  `RangePlan.ScanBatches` now runs on the same internal batch walker, unchanged in behaviour.
  No format change.
- A Dataset keeps the checked page locations of each projected column chunk's OffsetIndex once a
  request has loaded them, so later requests do not read and decode them again (Java keeps
  them too, under its own `maxOffsetIndexMemoryBytes`). The new `Limits.OffsetIndexMemory` (default 256 MiB, apart from `IndexMemory`) bounds what is
  kept; past it requests load the locations they need without keeping them, never an error. An
  OffsetIndex that fails to read or check is never kept and is refused on every request.
  `ReadStats.SourceReads` and `SourceBytes` leave out an index a request found kept.
- Added `RangePlan.ScanBatches(ctx, stats, func(*Batch) error)` for borrowed column scans.
  `Batch` exposes `Len`, `Key`, `Offset`, and `Column`; `Row` and `Cursor` make owning copies.
  The batch object and all borrowed slices/bytes are read-only and valid only until the callback
  returns. Batches stay within a row group, every key/projected page, and a selected span;
  maximal size is not promised. Duplicate projections, projected keys, absent columns,
  repeated-key offsets and chunks without value OffsetIndexes retain their semantics.
  `Scan` now adapts batches into owning rows. It delivers completed batches before a later
  error, including a corrupt projected page in the middle of a key page or a failing next key
  page, instead of withholding the whole preceding key-page buffer. Cancellation stops delivery
  between batches; cancelling within a Scan callback allows the current batch's remaining rows
  to be delivered. Both APIs hold the Dataset open during callbacks;
  callbacks must not call Close or wait for a goroutine that does. No format change.
- Tightened the owning scan allocation budget and added per-slot allocation gates that reject
  losing even one warmed PLAIN page buffer. Removed the redundant value-slice clear when opening
  a column; decoding already clears the previous page and its stale tail before reuse.

- `Dataset.BuildListIndex` explicitly builds an optional, bounded directory-run index for `/` listings.
  Compact exact anchors, binary run searches and first-key group routing avoid a key-page decode per
  wide prefix. Dense intervals decline safely; fully checked flat groups retain no index data.
  Both languages use the same conservative byte accounting. Build-only page indexes and first keys
  never fill the ordinary reader's index-memory cache; ordinary probes and synthetic materialization
  retain their usual caches. Transient loads count in read statistics. Refusals and unsupported
  codecs/encodings omit groups (both sides of bad boundaries); storage failures, resource limits
  and cancellation discard the attempt and permit retry.
  Publication is atomic. Empty outcomes reuse the most recent options; nonempty outcomes expose their
  effective options and whether any groups are indexed. Statistics count skips, declines and build work; empty indexes do not count declines.
  Slash-ended cursors alone use directory-run certification; all other valid cursors keep ordinary
  seeking. Synthetic transitions preserve logical position and current-group projection caches.
  Listings check cancellation before returning, including the no-index path. No format change.
- Fixed: a page whose PLAIN or BYTE_STREAM_SPLIT payload held more values than its header declares (or a
  dictionary page more than its count) was decoded as its declared prefix, so full validation counted only
  those and could certify the page (CHECK-5). It is now `invalid_parquet`, in reads and in validation; a
  PLAIN Boolean page may still pad its final byte.
- Fixed: an RLE/bit-packed stream of dictionary indexes or definition levels could hold more values than
  the page through a repeated run, or a bit-packed run beyond its final group's padding, read as its first
  values; it is now `invalid_parquet`, as in Java. A FIXED_LEN_BYTE_ARRAY page decoding to bytes that are
  not a whole number of values (DELTA_BYTE_ARRAY) is too.
- Fixed: a page header carrying a CRC of zero was not checked, parquet-go's header not telling it from
  none; every CRC a header carries is now checked, in reads and in validation, as in Java.
- Fixed: a request started while a `Close` waited for the requests in progress waited behind it, so a
  request started inside a `Scan` callback (or one the callback waits for) never returned, nor did the
  `Close`. A request started once `Close` has begun now fails with `ErrClosed` at once.
- Fixed: `write.PartWriter.WriteRows` panicked on a row beginning with a continuation value (repetition
  level above 0) whose column index is unset; it fails with `ErrProfile`.
- Fixed: `RangePlan.Split` with unique keys reads nothing, and so returned shards for a cancelled context or
  a closing dataset; it now observes its context on entry and through its loops, an empty plan's included.
  Building many shards no
  longer rescans the whole plan for each (O(G²) for G row groups split G ways).
- Documented: `PartOptions.Codec` compresses the columns whose schema nodes set no compression of their own;
  a node's compression takes precedence, as in parquet-go. `RangePlan.Scan`'s `fn` must not close the
  Dataset. `Limits.PageBytes` is not a bound on the memory of decoding a page: `PageValues` bounds its
  `parquet.Value` slices. A touched part keeps its file and footer until `Close`, unbounded by any limit
  (a known limitation: no cap, no eviction).
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
