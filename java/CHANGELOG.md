# Changelog

All notable changes to the Java implementation are recorded here. Versions follow `java/v…` tags,
independent of the specification's draft revisions.

## Unreleased

### Changed: proposal 0001 classification rulings and concurrency parity

- Unknown numeric page encodings and column codecs retain wire presence and raw values through bounded
  Thrift decoding. Needed data reports `unsupported_encoding`; unprojected values do not block a read.
  Unknown boundary order and unused V1 repetition encodings are ignored; required fields remain required.
- Unknown `PageEncodingStats.encoding` values are accepted as advisory metadata, with the raw integer
  preserved. Wire records are retained only for schema annotations; maps are allocated only for unknown
  enum integers, and OffsetIndex decoding needs no wire records. Metadata accounting charges 96 bytes
  per tracked structure (48 for OffsetIndex structures), plus 32 per recorded unknown enum integer.
- Intrinsic indexed key-header corruption, including negative V1 counts and V2 level lengths, reports
  `invalid_parquet` before index extent or row disagreement. Undecodable headers remain `invalid_page_index`.
- Every leaf’s recognized annotation restrictions are checked at first touch, including unprojected and
  nested leaves. DECIMAL carrier ceilings and the pinned TIMESTAMP FLBA(12) alternative are enforced;
  unknown annotation/time-unit alternatives retain physical fallback. Unknown units have no logical type
  in `columns(part)`. Valueless ordinary metadata and listed-stamp checks have regression coverage.
- Removed public `OpenOptions.verifyConcurrency` and `withVerifyConcurrency(int)`. `OpenOptions` takes
  only `Limits`; verification uses an internal bound matching Go, with checks applied in manifest order.

### Changed: the read API is aligned with the Go reader's

Pre-release, without deprecated aliases. Java exposes corresponding reader operations and request/result
concepts, with matching configurable limit defaults. Java representation differences and the capability,
resource-accounting and execution differences listed in README.md remain; see its correspondence table.

- Requests: `Span` (`all`, `prefix`, `key`, builder), `Selection` (a span and an optional row cursor) and an
  immutable `Query` (span or selection, `long` limit, columns, cursor set and clear, `next(Row)`) replace
  `RangeQuery`.
- Reads: `Dataset.rows(query)` (a limit from 1 to `Integer.MAX_VALUE`, a `List`'s capacity), `scan(query, visitor)` (limit 0 reads to the end of the span),
  `get(key, columns)` (one whole group, capped by `Limits.getRows()`, checked before any value page is read),
  `plan(selections, columns)` and `list(query)`, each also with `ReadOptions`. `range`, `rangeBatches`,
  `lookup` and `planRanges` are removed; so are the row-callback plan scan and `scanBatches`.
- `Plan` (was `RangePlan`): `count()` and `projectedBytes()` return `Estimate(value, exact)`; `columns()`,
  `scan(ReadOptions, BatchVisitor)`, `split(n, ReadOptions)` (`n` below 1 is `IllegalArgumentException`).
  A planned selection takes no limit. `Plan.stats()` is removed: planning and splitting report through
  `ReadOptions`, including when they fail.
- `ReadOptions(cancellation, statistics)` for every operation, `verify` and `columns(part)` included. The
  statistics are delivered once, on every exit, after the request is released, an invalid argument
  (a null visitor, key or column, a limit out of range) included, with zero counts. Fixed: a statistics consumer
  that threw while the operation (or the batch callback) failed replaced the operation's exception; it is now
  added to it as suppressed. `ReadStats.plus` sums two.
- `BatchVisitor<E extends Exception>` (`true` continues, `false` stops successfully) replaces
  `Consumer<Batch>`. `Batch.key(i)` is the borrowed read-only `ByteBuffer` (was `keyBuffer`), `copyKey(i)` the
  owning `Bytes` (was `key`), `cursor(i)` (was `continuation`); `BatchColumn.isPresent()` (was `isAbsent`),
  `byteLength` (was `length`), borrowed `bytes(i)` (was `byteBuffer`), owning `copyBytes(i)` (was `bytes`).
- `Cursor` (sealed: `RowPosition(key, offset)`, `CommonPrefix(prefix)`) replaces `Continuation`;
  `Row.cursor()`, `ListEntry.cursor()`. `Cursor.encode()` and `Cursor.parse(token)` implement the shared
  cursor token (spec/cursor-token-1.md), tested against its golden vectors. `ListQuery.Builder.cursor(c)` and
  `clearCursor()` replace `after(c)`.
- `Limits` is an immutable class with `builder()`/`toBuilder()` and named settings, with the Go reader's
  defaults: footer 16 MiB (was 64), page 64 MiB (was 256), 16,777,216 page values (was 4,194,304), index memory
  1 GiB (was 256 MiB), page memory derived as Go derives it (48 × page values + page bytes, 832 MiB), the new
  `metadataMemory` (256 MiB), charged in the protocol the Thrift decoder reads each footer, page index and page
  header through, before each string, binary or container is allocated (strings at twice their encoded length), which also caps page-header probing at
  min(16 MiB, `metadataMemory`) as in Go (was up to the page-byte limit), and `getRows` (1,000). Zero means the default; a negative setting is `IllegalArgumentException`. `maxResultEntries` is
  removed: `rows` and `list` are bounded by the caller's limit, `scan` by nothing.
- `LimitExceededException.limit()` is a `LimitName` enum. A manifest whose row total exceeds 2^63-1 is refused
  at open (`DATASET_ROWS`). `Manifest.parse(bytes, limits)` enforces the manifest size and the parts limit:
  the parts limit is enforced before materializing more than the permitted number of part entries, including
  intermediate JSON representations, and after the version is decided. `OpenOptions.DEFAULT` verifies one part at a time (was 8), as the Go reader does.
- Schema: `Dataset.columns()` and `column(name)`, which described only the parts checked so far, and
  `Column.parts()` are removed; `columns(part)` remains. `Dataset.rows()` (the row total) is `rowCount()`.
- Storage: `PartSource` no longer extends parquet-java's `InputFile`; `storage.ParquetJavaInterop` holds the
  conversions (`partSource(path, inputFile, version)`, was `PartSource.of`; `inputFile(source)`). `read`
  documents the returned buffer's lifetime.
- `INT96` columns are projected as raw bits: `Value.Int96(word0, word1, word2)`, least-significant word
  first, and `BatchColumn.getInt96(i)`; no timestamp interpretation (was `unsupported_projection`).
- A part schema nested deeper than 64 is `invalid_footer`, as in Go (was a limit at 256).
- `ListQuery` is an immutable class with `builder(delimiter)` and `toBuilder()` (was a record);
  `BatchColumn.size()`; `ReadStats.NONE` is public.
- Fixed: a part's kept refusal was thrown as one shared exception, so what one request attached to it (a
  suppressed statistics failure) reached later requests; each request now gets a fresh exception with the same
  code and location.
- Null extension names and values, and a null `ListIndexStats` duration or options, are rejected.
- Smaller: `Bytes.successor()` is `prefixEnd()`; `Bytes.utf8` rejects unpaired surrogates;
  `UnsupportedCode` constants are `ENCODING`, `PROJECTION`, `ENCRYPTION`; the compatibility constructors of
  `ReadStats`, `ListIndexStats`, `ListQuery` and `ManifestPart` are removed; `LocalStorage.MAX_STRANDED_OPENS`,
  `Cancellation.linkedTo`, `detach` and `Registration.NOOP` are no longer public.

The entries below predate the alignment and use the names of their time.

### Added

- `ManifestPart.extensions()`: a part's `extensions` object (spec §3.4, EXT-1 to EXT-5), each namespace mapped
  to its value's JSON text exactly as written, as `Manifest.extensions()` is for the top level; empty when the
  part has none. A part's `extensions` that is not an object, or has an empty namespace name, is
  `invalid_manifest`; an undefined part member beside it still is. The six-argument constructor remains, with no
  extensions. The reader ignores extensions (EXT-3).

- Key page read-ahead for delimited listings, the Go reader's `List.ReadAhead`: `ListQuery.Builder.readAhead(n)`
  (a new record component `readAhead()`, default 0, off; a value above `ListQuery.MAX_READ_AHEAD`, 64, is read
  as it; a negative one is `IllegalArgumentException`). After each entry a listing predicts, from the checked
  key page index of the row group it is in, the key pages it will decode next (after a common prefix, the
  first page whose maximum reaches the prefix's successor; after a row, the next page) and reads up to `n` of
  them concurrently on virtual threads, holding at most 1 MiB per slot of pages read and not yet used and at
  most `4 * n` pages. The key page read takes those bytes instead of reading them and checks them exactly as
  before: entries, errors, refusal codes and every count but the reads and bytes of unused pages are those
  without read-ahead, including on corrupted parts. `ReadStats` gains `readAheadReads`, `readAheadUnused` and
  `readAheadUnusedBytes` (the reads also count in `sourceReads` and `sourceBytes`); a page predicted wrongly is
  read for nothing. A read ahead that fails or throws, whatever it throws, is not used and the page is read
  again. The reads run under a cancellation linked to the request's, and are ended and waited for before the
  listing returns, including when it fails, is cancelled or the dataset closes; a cancellation, interrupt or
  close during that wait fails the listing. A page's bytes are released when it is retired (taken, or dropped
  once passed; a passed page stays pending, within the budget, while a page predicted further ahead is still
  being read). A `/` listing reads nothing
  ahead while a list index is built; the prediction stays within a row group; `range` and the scans do not
  read ahead. As an illustration (one run, not asserted by any test): with a simulated 10 ms wait per read, a
  cold listing of 60 folders (400 keys each, 100-row key pages) took about 640 ms without read-ahead and about
  115 ms with `readAhead(8)`, for the same reads.
  The existing `ListQuery` and `ReadStats` constructors remain, with no read-ahead.
- Structured refusal locations, the Go reader's `RefusalError` fields: `RefusalException` gains `rowGroup()`
  (`OptionalInt`, the physical row group in the part's footer), `row()` (`OptionalLong`, 0-based within that row
  group) and `key()` (`Optional<Bytes>`, a copy), beside `code()` and `part()`. They are filled wherever the
  reader has the value in hand, as Go fills them: footer, sorting-column, page-index, key-page and value-page
  refusals carry the row group; `invalid_key` and `key_order` in a page also the row; `key_order` also the key;
  a `key_order` found against a neighbouring decoded page is located at the later of the two pages, as in Go.
  Manifest, part-level and dataset-level refusals carry none. Messages and codes are unchanged, and the
  existing constructors and accessors are source-compatible. Refusals remain Java-serializable, including
  their key bytes; serialized refusals written before locations existed retain unknown locations.
- Borrowed batch scans, the Go reader's `RangePlan.ScanBatches` and `Dataset.RangeBatches`:
  `RangePlan.scanBatches(...)` and `Dataset.rangeBatches(query, ...)`, each with the three overloads of `scan`
  (cancellation, read statistics), hand the rows `scan` and `range` return to a callback as `Batch`es read
  through primitive accessors (`BatchColumn`: `isAbsent`, `type`, `isNull`, a getter per physical type,
  floating-point bits, and `length`/`copyBytes`/`byteBuffer` for byte arrays; keys through `keyLength`,
  `copyKey`, `compareKey`, `keyBuffer`), without a `Row`, `Value` or `Bytes` object per row. A batch never
  crosses a row group, a key page, a projected page or a gap between a plan's ranges. It is borrowed: valid only
  until the callback returns, after which its accessors throw `IllegalStateException`; `row(i)`,
  `continuation(i)`, `key(i)`, `value(i)` and `bytes(i)` make owning copies. `rangeBatches` plans nothing and
  takes a range's bounds, cursor, columns and limit, with its errors and limits; its read statistics are the
  range's when the callbacks return normally. Cancellation is also checked before every callback; `close()`
  cancels a scan and waits for its callback, and closing from inside one fails as it does from `scan`. The
  existing `scan` and `range` paths are unchanged; tests hold the batch paths to them over every valid
  conformance dataset.
- `BYTE_STREAM_SPLIT` values, outside the ENC-1 baseline, are decoded as the Go reader decodes them:
  `FLOAT`, `DOUBLE`, `INT32`, `INT64` and `FIXED_LEN_BYTE_ARRAY` columns, a page holding exactly its values;
  another type is `invalid_parquet`, as in Go. Such pages were reported as `unsupported_encoding`. Tests decode
  pages encoded by parquet-java's writers (exact bits, with and without nulls, wrong sizes refused) and read an
  optional split column through range reads, scans and batch scans.
- `Limits.maxPageMemory` (a new record component; `Limits.withMaxPageMemory`) bounds the memory decoding
  one page holds (issue #46), data and dictionary pages, key pages included: estimated from the page's
  header before anything is allocated for its values (a slot per value for its representation, a null
  flag per row when optional, the arena's bytes, and for byte-array dictionaries an entry of 80 bytes
  per value and their bytes twice), it fails a page over it with `LimitExceededException` naming
  `maxPageMemory`. Every existing constructor keeps its signature and takes the default: the most
  `maxPageValues` and `maxPageBytes` allow (832 MiB at their defaults), so no page they admit is newly
  refused. Concurrent requests add up.

### Fixed

- `LocalStorage` refused every dataset on Windows ("the filesystem does not support secure part-path
  traversal"), since the JDK's Windows file system has no secure directory streams (issue #45). On a file
  system without them, each part path is now resolved segment by segment as a path, refusing symbolic
  links, junctions and other reparse points and segments Windows would reinterpret (`:`, a trailing dot or
  space, device names), with every directory and the file's identity (its `fileKey`, else its size and
  times) checked before and after each open. The POSIX route is unchanged. The README sets out what each
  platform guarantees. CI builds and unit-tests the Java module on Windows.
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

- The leading `PAR1` magic is checked when a part is first touched (CHECK-1), as Go does: a part no larger than
  the 64 KiB footer window holds it in the window, so nothing more is read; a larger part costs one more
  storage read, of its first 4 bytes, before the read of its end. `PARE` at either end is
  `unsupported_encryption`; any other magic but `PAR1` is `invalid_footer`, with Go's message. LZ4_RAW is now
  in the ENC-1 baseline, which this reader already decoded.
- A part's footer is read in fewer bytes: the first read of a part's end, the last 64 KiB or the whole part
  when smaller, is now never more than `Limits.maxFooterBytes` plus the 8-byte trailer, and a footer larger
  than that window costs one more read of only the footer bytes before it (it read the whole footer again).
  Still one read for a footer within the window and two above it; the window is not kept once the footer is
  decoded. Every check, refusal code and message is unchanged, including for truncated parts and garbage
  tails (the leading-magic check above adds a read for a part larger than the window).
- The checked page locations of each projected column chunk's OffsetIndex, kept once a request has loaded
  them so that later requests do not read and decode them again, are now bounded by the new
  `Limits.maxOffsetIndexMemoryBytes` (default 256 MiB, `withMaxOffsetIndexMemoryBytes`, apart from
  `maxIndexMemoryBytes`), as Go's `Limits.OffsetIndexMemory`, with the same charge: 32 bytes per page plus 64
  per chunk. Past it requests load the locations they need without keeping them, never an error; before,
  they were charged to `maxIndexMemoryBytes`, competed with key indexes and failed the request with
  `LimitExceededException` once it was full. An OffsetIndex that fails to read or check is never kept and is
  refused on every request. `ReadStats.sourceReads` and `sourceBytes` leave out an index a request found kept.
  Compatibility: `maxOffsetIndexMemoryBytes` is a ninth `Limits` record component. The eight-argument
  constructor and the existing `with…` methods remain, but record patterns on `Limits` need the new arity,
  and reflection over its components, `equals`, `hashCode` and `toString` see the new field.
- `Dataset.buildListIndex` explicitly builds an optional, bounded directory-run index for `/` listings.
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
