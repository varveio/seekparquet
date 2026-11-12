# Changelog

All notable changes to the Go module `github.com/varveio/seekparquet/go`. The module follows semantic
versioning; while it is v0, minor versions may change the API.

## Unreleased

- **Breaking: the read API is redesigned** before any stability promise (no deprecated aliases):
  - Requests: `Range`, `KeyRange` and `PrefixRange` are replaced by `Span` (`KeySpan`, exactly
    `[key, key+0x00)`, and `PrefixSpan`), `Selection` (a span and a row cursor) and `Query` (a selection,
    an `int64` `Limit` and `Columns`). `List` is renamed `ListQuery`, `Entry` `ListEntry`, `Options`
    `OpenOptions`. `RowCursor` and `PrefixCursor` build cursors.
  - Reads: `Dataset.Range` becomes `Rows` (owning rows, positive limit, no rows on error; it still finds
    every key before reading a value page); `Dataset.RangeBatches` becomes `Scan`, where a zero `Limit`
    scans to the end of the span (a library streaming extension; the specification's range and the
    conformance cases are unchanged); `PlanRanges` becomes `Plan` over `[]Selection`, and `RangePlan`
    becomes `Plan`, whose `Scan` takes the batch callback (`ScanBatches` and the row-callback `Scan` are
    removed). New `Dataset.Get` returns one key's whole group, bounded by the new `Limits.GetRows`
    (default 1,000): a larger group fails, before any value page is read, with no rows.
  - Statistics: every read takes one nullable `*ReadStats` argument, reset at entry and filled on every
    exit, including failed planning and splitting; request `Stats` fields and `RangePlan.Stats` are
    removed. `Split` reports its reads through its argument rather than per shard. `ReadStats.Add` sums.
  - Values: `Value` is an opaque type with typed accessors, `Kind`, `IsNull`, `Absent`, `Clone` and
    constructors; absent and null are now disjoint (`IsNull` is false for an absent value).
    `Batch.Column` returns a zero-copy `ColumnView`; a projected column a part lacks takes no storage, and a
    batch projecting one holds at most 4,096 rows. `Column` describes types with `Kind`, `Repetition`
    and `Logical` (parameters and converted-type normalization kept). No parquet-go type remains in the
    read API; package `write` still takes a parquet-go schema.
  - Errors: `Code` and `CodeOf` are renamed `RefusalCode` and `RefusalCodeOf`; `UnsupportedError.Code`
    is an `UnsupportedCode`. Limit failures are `*LimitError` (limit name, value, maximum) wrapping
    `ErrLimitExceeded`. A negative field of `Limits` is `ErrInvalidRequest` at open instead of taking
    the default. A part's cached refusal is returned to each read as a copy of its own.
  - Fixed: a nil callback to a planned scan, and `Split` with `n` below 1, are `ErrInvalidRequest`. A
    callback returning `Stop` ends either scan with a nil error. `Batch.Columns` and `Plan.Columns`
    return copies of the projection.
  - `Cursor.Encode` and `ParseCursor` carry a cursor as a version 1 cursor token
    ([spec/cursor-token-1.md](../spec/cursor-token-1.md)), with shared golden vectors.

- Optional storage capabilities for remote sources, each with a fallback to `Open` and `ReadAt`:
  `ManifestReader` (one bounded whole-manifest read on the ordinary path; `Metadata.ManifestIdentity`),
  `SizedSource` (part opens with the manifest's size, proven by the first read, for parts of 12 bytes or more),
  `RangeReader` with `ReadRange`, `BatchError`, `MaxReadRanges` and `MaxReadBatchBytes`, and `ErrObjectChanged`,
  `ErrObjectTooLarge` and `SizeMismatchError`. A size mismatch is refused `part_mismatch` and a missing object
  `missing_part` wherever they surface; a broken pin (`ErrObjectChanged`) stays an environment error, also on the
  manifest; a short read, or a read that returned every byte with an error that says more than `io.EOF`, is the
  environment's error, never a refusal, keeping a size mismatch or broken pin it carries. Physical reporting: `IOReporter`, `IOObserver` (`WithIOObserver`, `IOObserverFromContext`) with
  `IORequest`, `IORequestKind` and `IOResult`, counted in new `ReadStats` fields (`SourceBatches`,
  `ScalarFallbackReads`, `Physical*`, `Planned*`, `ByteCacheHitBytes`, `MetadataHitBytes`, `RequestMemoHitBytes`,
  `UnreportedIOCalls`, `PeakPhysicalRequests`; `Add` takes the larger peak) and in `OpenOptions.OpenStats` for
  `OpenDataset` and `OpenDir`; every operation observes the requests of each receiver that reports, Source or file,
  from its first call. Cache control: `CacheMode` with `WithCacheMode` and `CacheModeFromContext`;
  `Dataset.Scan`, `Plan.Scan` and `BuildListIndex` ask for `CacheBypass`. `OpenOptions.Read` is a `ReadPolicy`:
  `Concurrency` (default 64) bounds an operation's outstanding ranges, read-ahead included;
  `MetadataPrefetchBytes`; `DisableSpeculation`, which also turns off `ListQuery.ReadAhead`.
- **Breaking:** `Dataset.Verify` takes a `*ReadStats`, reset on entry and filled on every exit.
- A part's first check reads its end in one `ReadPolicy.MetadataPrefetchBytes` window (default 2 MiB), with its
  4-byte head, and keeps the page-index bytes it holds within the new `Limits.PageIndexCacheMemory` (default
  128 MiB, least recently used parts evicted, never a failure; footers and decoded indexes are outside it): later
  ColumnIndex and OffsetIndex loads of the part read nothing, or only the missing beginning of an index that
  starts before the window. A key ColumnIndex and OffsetIndex not retained are read together. Within a request,
  complete key-page extents admitted to a bounded request memo are reused. `Verify` checks up to
  `ReadPolicy.Concurrency` parts at once, reading each part's first key pages and last key page together, its
  head merged into the first, and commits each check and its verdict in manifest order, so it reports what a
  serial check reports; checked first keys and last pages are not checked again. It fetches at most
  `ReadPolicy.Concurrency` parts ahead of the part it checks, so the part ends it holds stay bounded, and a page it
  fetched is used only once the part's check adopts the handle it was read from. `ReadPolicy.Concurrency` bounds
  every range an operation has in flight: scalar reads, batches, read-ahead and list-index workers. `OpenDir` with
  a zero `ReadPolicy` reads as before these capabilities: a 64 KiB window, each part's ranges one at a time, no
  retained index bytes and no memo (`Verify` still checks several parts at once).

- `ListQuery.ReadAhead` (default 0, off; at most 64): a listing predicts from the checked key page index of the
  row group it is in the key pages it will decode next (after each common prefix, the first page whose
  maximum reaches the prefix's successor) and reads up to that many of them concurrently, holding at most
  1 MiB per slot of pages read ahead and not yet used (`ReadAhead: 64` can hold 64 MiB per request). The page
  read path takes those bytes instead of reading them and checks them as before; entries and every other count are unchanged. `ReadStats` gains
  `ReadAheadReads`, `ReadAheadUnused` and `ReadAheadUnusedBytes` (the reads also count in `SourceReads` and
  `SourceBytes`); a page predicted wrongly is read and transferred for nothing and held until the scan passes
  it or the key scan ends. With a 5 ms wait per read, a cold 100-folder listing (4,000 keys per folder, 1,024-row
  pages) takes 106 ms with `ReadAhead: 8` instead of 570 ms, for the same 105 reads. The read-ahead does not
  cross row groups or parts, is not used by `Rows` or batch scans, and is off for every `/` listing while a
  list index is built, including for a row group the index declines. The reads ahead end when the key scan
  ends, before values are projected.

- A part's footer is read in fewer reads: one speculative read of the part's last 64 KiB (bounded by
  `Limits.FooterBytes`, the whole part when smaller) holds the trailer and, when it fits, the footer, with
  a 4-byte read of the head magic only for a part larger than the window and one more read only for a
  footer larger than it. The footer and the head magic are no longer read a second time when parquet-go
  opens the checked footer. A footer took four reads; it now takes one (part up to 64 KiB), two, or three
  (footer over 64 KiB). `SourceBytes` grows by up to 64 KiB per part opened; the window is freed once the footer is checked, and an open part
  retains only its footer, trailer and head magic, as before. Every check and refusal is
  unchanged. `InspectFooter` and `InspectKeyPages` still read only the head magic and the footer (and the
  key page indexes), now once instead of twice.

- `Plan.Scan` and `Dataset.Scan` reuse one set of key page buffers
  per call (read, decompressed and decoded bytes, offsets, dictionary indexes), as projected value pages
  already did: each key page overwrites the one before after the callbacks borrowing it have returned.
  Key pages are decoded into bytes and offsets (or a dictionary's, with the page's indexes) instead of a
  `parquet.Value` per key, by every key page walker (batches, `Rows`, `List`, routing, planning, `Split`,
  `Verify`, the list index); validation and a projected key column still use the value decoder. On a 4,000,000-row listing of 101 to 106-byte keys (1,024-row
  ZSTD pages, DELTA_BYTE_ARRAY keys) a whole `Plan.Scan` allocates about 3 bytes per row instead of
  197. `Batch.Key` is borrowed until the callback returns, as before; `Batch.Row`, `Batch.Cursor`,
  `Rows`, `List` and every other key walker keep owning or never-reused keys. `Limits.PageMemory` still
  estimates a key page at 24 bytes per value, more than it now holds.

- Fixed (#44): a page its OffsetIndex locates with other rows than its header declares is refused for
  breaking RG-3 even when the header declares more values than `Limits.PageValues`; it was reported as a
  limit (`ErrLimitExceeded`), as if the data might be valid. A header that agrees with its OffsetIndex entry
  is still held to the limit. Java already answered this way.
- `Limits.PageMemory` bounds the memory decoding one page holds (issue #46), data and dictionary pages,
  key pages included: estimated from the page's header before anything is allocated for its values (24
  bytes per value for its `parquet.Value`, twice that for an optional column, plus its decoded bytes, at
  most `PageBytes`), it fails a page over it with an error wrapping `ErrLimitExceeded`. Its default is the
  most `PageValues` and `PageBytes` allow (832 MiB at their defaults), so no page they admit is newly
  refused; lower it to bound decoding memory. Concurrent calls add up.
- Fixed (differential testing against the Java reader): a page of a type other than BOOLEAN labelled RLE
  made parquet-go decode its payload as runs of bit width 0 and allocate tens of GiB, killing the process; it
  is `invalid_parquet` before anything is decoded. A PLAIN BYTE_ARRAY value overrunning its page could be
  completed with the bytes after it (the next page's header, read with this one), or make parquet-go panic
  where a planned scan or full validation reads a dictionary, which killed the process: every length is
  checked against its page first, page payloads are sliced with their capacity ending at the page, and a
  dictionary is decoded under the same recover as a page.
- Fixed: a repeated run of an RLE/bit-packed stream (levels, dictionary indexes, Booleans) whose value does
  not fit its bit width was read (a Boolean 2 as false); a definition level above 1 was counted as that many
  values; a DATA_PAGE_V2 page's repetition-level bytes, a REQUIRED column's definition-level bytes and a
  `num_nulls` that disagrees with the levels were not checked; a column chunk without an OffsetIndex could
  hold a page of no rows, or pages past its row group's end; a DELTA_LENGTH_BYTE_ARRAY or DELTA_BYTE_ARRAY
  page's lengths need not cover exactly the bytes after them (parquet-go's amd64 decoder then returned the
  wrong values); a DELTA_BINARY_PACKED miniblock above 32 bits in an INT32 stream reached a size estimate
  and failed as a limit. Each is now `invalid_parquet`, as in Java. A column chunk whose metadata names
  another path or physical type than its schema leaf is `invalid_footer`.
- Fixed: a dictionary-encoded page with no non-null value and no index bytes is read as all nulls, as Java
  and PyArrow read it; a `sorting_columns` entry after the first that names no column is ignored (RG-1), and
  row-group ordinals that differ from the row groups' positions are ignored (they serve encryption): both
  were `invalid_footer`. parquet-go is given the footer without them.
- Added `Dataset.Scan(ctx, q, stats, func(*Batch) error)`: a limited range read as borrowed
  `Batch`es, with no plan. It returns the rows, keys, values and cursors `Rows` returns and reads
  what it reads (equal `ReadStats` for a successful, fully consumed request against the same cache
  state; a request ended by an error, a callback stop or cancellation can differ), opening projected chunks and dictionaries only as the rows reach
  them, and stops at `Limit` without advancing to the next row. A nil callback is
  `ErrInvalidRequest`; callback errors, cancellation, Close and borrowing follow `Plan.Scan`.
  `Plan.Scan` now runs on the same internal batch walker, unchanged in behaviour.
  No format change.
- A Dataset keeps the checked page locations of each projected column chunk's OffsetIndex once a
  request has loaded them, so later requests do not read and decode them again (Java keeps
  them too, under its own `maxOffsetIndexMemoryBytes`). The new `Limits.OffsetIndexMemory` (default 256 MiB, apart from `IndexMemory`) bounds what is
  kept; past it requests load the locations they need without keeping them, never an error. An
  OffsetIndex that fails to read or check is never kept and is refused on every request.
  `ReadStats.SourceReads` and `SourceBytes` leave out an index a request found kept.
- Added `Plan.Scan(ctx, stats, func(*Batch) error)` for borrowed column scans.
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
- Fixed: `Plan.Split` with unique keys reads nothing, and so returned shards for a cancelled context or
  a closing dataset; it now observes its context on entry and through its loops, an empty plan's included.
  Building many shards no
  longer rescans the whole plan for each (O(G²) for G row groups split G ways).
- Documented: `PartOptions.Codec` compresses the columns whose schema nodes set no compression of their own;
  a node's compression takes precedence, as in parquet-go. `Plan.Scan`'s `fn` must not close the
  Dataset. `Limits.PageBytes` is not a bound on the memory of decoding a page: `PageValues` bounds its
  `parquet.Value` slices. A touched part keeps its file and footer until `Close`, unbounded by any limit
  (a known limitation: no cap, no eviction).
- The conformance adapter claims the optional `planning` capability: it answers `plan` (Plan, then
  Scan, Count and ProjectedBytes) for the conformance suite's planning cases.
- `Plan.Split(ctx, n, stats)` divides a selection plan into at most `n` shards along row-group boundaries,
  never inside a group of equal keys, for a caller to scan concurrently: the shards' scans together yield the
  plan's rows, each once, and their `Count` and `ProjectedBytes` add up to the plan's. Boundaries balance the
  projected bytes when exact, else the rows. Unique keys split without reading; repeated keys decode the two
  key pages around each boundary inside a part. No goroutines in the package and no format change: splitting
  is a Go convenience, not part of the specification or the conformance protocol.
- `ListEntry.Exact`: a common prefix entry carries its exact row (spec LIST-8), the first candidate row of the
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
- Selection planning: `Dataset.Plan(ctx, selections, columns, stats)` returns a `Plan` for every row of a set
  of ranges. `Count()` is its row count, from the key pages at the ends, row-group row counts and OffsetIndex
  row positions, and reports whether it is exact (every part holding a row checked). `ProjectedBytes()` is the
  compressed bytes `Scan` will request for the projected columns, shared pages and dictionaries once, and
  reports whether it is exact (every projected chunk has an OffsetIndex). `Scan(ctx, stats, fn)` yields the
  rows in dataset order. A plan is bound to its `Dataset`. No format change.
- A page that an OffsetIndex locates is read in one request of exactly its extent instead of a 4 KiB header
  window and a second read for a larger payload, and so is a value chunk's dictionary page, as the extent
  before the chunk's first data page. `Rows` and `List` still read a dictionary only for a page that uses
  it; a plan's `Scan` reads it before the chunk's first page, as `ProjectedBytes` counts it.

- `PartWriter.WritePage` writes a page column by column from the caller's own values, without copying
  them and checking only the keys (KEY-2, KEY-3): with swath's binding it writes swath's bytes at its old
  writer's speed (`BenchmarkSwathWriter`). `WriteRows` still copies, for callers that reuse their rows.
- Lazy open (spec CHECK-1 as now written): `OpenDataset` and `OpenDir` read and check the manifest only,
  and open no part. A part is opened and checked (size, footer, stamps, row count, key column, RG-1, and
  COL-5 against the parts already checked) the first time a read routes to it; a refusal surfaces from
  that `Rows` or `List` with the same codes as before, and the part stays refused. `OpenDir` keeps the
  directory open until `Close`; a custom `Source` must stay usable until `Close`.
- `Dataset.Verify` checks every part now, then every row group's first key page and each part's last
  (CHECK-3). It replaces `Options.VerifyOnOpen`, which is removed: call `Verify` after opening.
- `ReadStats.PartsOpened` counts the parts a request was the first to touch; their footer reads are in
  `SourceReads` and `SourceBytes`.
- The structural validation level checks every part explicitly, as `Verify` does without key pages.
- Every read takes a `*ReadStats` that the reader fills with the request's exact
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
