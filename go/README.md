# seekparquet for Go

A Go reader, writer and validator for [SeekParquet](../spec/format-1.md) datasets: ordinary Parquet files sorted
by one byte-string key column named by the manifest, split into key-disjoint parts and committed by a
`manifest.json`. It finds the first row at or after a key, reads key ranges and prefixes, and lists a
prefix with delimiter rollups, by reading the manifest, the footers, the key column's page index and a
few pages. It also validates a dataset at the specification's four levels ([Validating](#validating)).

```
go get github.com/varveio/seekparquet/go@v0.1.0
```

Versioning is described in [RELEASING.md](../RELEASING.md#go).

| | |
|---|---|
| Module | `github.com/varveio/seekparquet/go` (v0: the API may still change) |
| Format | format 1, draft revision 1: files stamped `format_version` 0, `draft_revision` 1. Every other pair is refused with `unsupported_version`. |
| Go | 1.26 or later |
| Dependencies | the standard library, `github.com/parquet-go/parquet-go` v0.32.0, and the decompressors it uses |

## Usage

```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "/data/listing", seekparquet.OpenOptions{})
if err != nil {
	return err // seekparquet.RefusalCodeOf(err) gives the refusal code, if it is one
}
defer d.Close()

// A range: start inclusive (StartExclusive to exclude it), end exclusive.
q := seekparquet.Query{Limit: 100, Columns: []string{"size"}}
q.Start = []byte("logs/2026/")
rows, err := d.Rows(ctx, q, nil)

// A prefix read, which may also take a start and an end.
rows, err = d.Rows(ctx, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.PrefixSpan([]byte("logs/2026/"))}, Limit: 100}, nil)

// Every row stored under one key (Limits.GetRows bounds the group).
rows, err = d.Get(ctx, []byte("logs/readme"), nil, "size")

// A delimited listing, paged.
req := seekparquet.ListQuery{Prefix: []byte("logs/"), Delimiter: '/', Limit: 1000}
for {
	entries, err := d.List(ctx, req, nil)
	if err != nil || len(entries) == 0 {
		break
	}
	// entries[i].IsPrefix(), entries[i].CommonPrefix (with entries[i].Exact), entries[i].Row
	req.Cursor = entries[len(entries)-1].Cursor()
}
```

Runnable examples are in [example_test.go](example_test.go).

## Writing

```go
schema := parquet.SchemaOf(new(Object)) // a top-level REQUIRED []byte or string key column
err := local.WriteDir(ctx, "/data/listing", schema, sortedRows, local.DatasetOptions{
	Part:      write.PartOptions{Key: seekparquet.Key{Column: "key", Unique: true}, SchemaID: "example/objects/v1"},
	PartBytes: 256 << 20,
})
```

`WriteDir` writes rows already in key order (a `parquet.RowReader`) as a new dataset: parts named
`data/part-NNNNN.parquet`, each ended at the first new key past the size target so that a group of equal
keys never crosses parts, each synced into place, then the manifest, with every part's SHA-256. A
producer with its own part rotation drives the pieces itself: `write.NewPart` streams one part and
returns its `PartInfo` from `Seal`; `local.PublishPart` installs it; `local.Commit` publishes the
manifest. `PartWriter.WriteRows` copies the rows it is given, so they may be reused at once;
`PartWriter.WritePage` takes a page column by column from a fill function and copies nothing, for a
producer that keeps its own rows until the page is written. The part writer enforces the profile (key column, key order, page indexes with exact bounds,
`sorting_columns`, stamps, column orders) and cannot be configured out of it.
`seekparquet.MarshalManifest` and `ParseManifest` write and read a manifest; `InspectFooter` and
`InspectKeyPages` read a part's footer and key page index without reading its pages.

The recipe, the guarantees and what `WriteDir` does not do are in [docs/producing.md](../docs/producing.md).

## Keys and pagination

The manifest declares `key: {"column": "<name>", "unique": true|false}`; the footer stamps must match
it, and `d.Metadata().Key` exposes it. When keys are not unique, consecutive equal keys form a group in
producer file order; a group may span pages and row groups but never parts. Start, end and prefix select
whole groups. `Get(ctx, key, stats, columns...)` returns one whole group, at most `Limits.GetRows` rows
(default 1,000); `KeySpan(key)` selects one group for `Rows`, `Scan` and `Plan`, paged like any span.

Every result pages the same way: set the request's `Cursor` to the last entry's `Cursor()` and repeat
the request. A row cursor is the row's key and its `Row.Offset`, the zero-based position in its group
(always zero with unique keys); a listing may also continue after a common prefix (`ListEntry.Cursor()` of
a prefix entry). A range takes only a row cursor. `Cursor.Encode` and `ParseCursor` carry a cursor as a
text token ([spec/cursor-token-1.md](../spec/cursor-token-1.md)). A prefix entry carries its exact row in `ListEntry.Exact`
when its first candidate row is stored at the prefix itself (an `a/` folder object under the prefix `a/`), with
its projected values, so a listing pairs the two without a lookup (spec LIST-8). A page may end inside a group, and resuming deep
inside a large group costs a few pages, not the rows before the cursor.

## API

| Package | What it is |
|---|---|
| `seekparquet` | `OpenDataset` (a dataset through a `Source`), `OpenDir` (a local directory), `Dataset` with `Rows`, `Get`, `Scan`, `List`, `Plan`, `BuildListIndex`, `Verify`, `Metadata`, `Columns` and `Close`; `Plan` with `Count`, `ProjectedBytes`, `Columns`, `Scan` and `Split`; the requests `Span` (`KeySpan`, `PrefixSpan`), `Selection`, `Query`, `ListQuery` and `Cursor` (`RowCursor`, `PrefixCursor`, `Encode`, `ParseCursor`); the results `Row`, `Value`, `ListEntry`, `Batch` and `ColumnView`; `ReadStats`; `Column`, a part's column as its footer declares it, with `Kind`, `Repetition` and `Logical`; `OpenOptions` and `Limits`; refusal codes and errors; `Validate` and `ValidateDir` with `ValidateOptions`, `Level` and `Report`; the format constants and stamp keys, `Manifest` with `ParseManifest` and `MarshalManifest`, `InspectFooter` and `InspectKeyPages` |
| `write` | `NewPart` and `PartWriter`: one part, streamed from sorted rows |
| `local` | `PublishPart`, `Commit` and `WriteDir`: durable publication in a local directory |
| `cmd/seekparquet` | the `seekparquet validate` command |
| `internal/...` | the manifest and Thrift checkers, the test-data writer, the sample datasets (`internal/samples`, written by `internal/cmd/samples`), and the conformance adapter (`internal/cmd/conformance`) for the [conformance harness](../conformance/protocol.md) |

To run the conformance suite from the repository root:

```
python3 conformance/harness/run.py --adapter "go -C go run ./internal/cmd/conformance"
```

or build the adapter first (`go -C go build -o /tmp/sp-adapter ./internal/cmd/conformance`) and pass its path.
The adapter claims the `reader` target and the four validator targets, so one run qualifies all five.

## Results

- Rows own their key and value bytes. `Row.Columns` is the result's own copy of the projected names.
- A value is absent, null or a value: `Value.Absent()` reports a projected column the row's part does not
  have (COL-2), and `IsNull()` only a null stored in a present column; an absent value is not null. A
  column no part has is absent in every row. Projecting a nested or repeated column is an
  `*UnsupportedError` (`unsupported_projection`), never reported as absent.
- Values are this module's own `Value`s of the column's physical type (`Kind`), read with typed accessors
  (`Int64`, `Bytes`, ...); an accessor of another kind returns its zero value. No parquet-go type appears in
  the read API. Constructors (`Int64Value`, `ByteArrayValue`, `NullValue`, `AbsentValue`, ...) build values.
- A limit that is not positive (zero is allowed for `Scan`), or a malformed cursor, is `ErrInvalidRequest`,
  not an empty result.

## What opening and reading check, and what they do not certify

Opening checks the manifest and nothing else (CHECK-1): duplicate member names, undefined members, exact
integers, canonical base64, part paths, bound order. It opens no part. The manifest carries each part's
exact key bounds, row count and size, which is all a read needs to be routed.

A part is checked the first time a read routes to it: it exists with the manifest's size and row count, its
footer is readable, self-contained and unencrypted, its `seekparquet.*` stamps match, the manifest-named key
column is valid (type, repetition, annotation, `column_orders`), its columns agree with those of every part
already checked (COL-5), and every non-empty row group declares `sorting_columns`. A part that fails is
refused by the read that touched it, with the same codes, and by every later read that touches
it; a part known from its bounds to lie outside a read is not touched. Concurrent reads wait for one check.

`Dataset.Verify` checks every part now, then every non-empty row group's key page index and first key page,
and each part's last key page (CHECK-3): row-group first keys must be in the declared order and each part's
first and last keys must equal its manifest bounds. That suits a long-lived reader of local files, or a
caller that wants every part's fault before its first query; it costs reads across the whole dataset.
Without it the same checks happen when a read reaches those parts and pages.

Every read checks each row group's key page index before it uses the group, and each key page it decodes
(CHECK-2): the page header matches its OffsetIndex entry, one non-empty key per row, valid UTF-8 for a
`STRING` key, non-decreasing (strictly ascending when `key.unique` is true), inside the page's bounds and the
part's manifest bounds, and in dataset order with every other page the request decoded. Page bounds are used
as Parquet defines them, as bounds whatever their length; overlapping bounds and `boundary_order` are never
relied on.

What is **not** certified: rows a request does not decode (a query that succeeds says nothing about the pages
it skipped), values beyond decoding them, `sha256` digests, and that files are unchanged after opening. Full
certification is validation (spec §8): see [Validating](#validating).

## Validating

```go
report, err := seekparquet.ValidateDir(ctx, "/data/listing", seekparquet.ValidateOptions{Level: seekparquet.LevelFull})
if err != nil {
	return err // the directory could not be opened, or ctx ended
}
for _, lr := range report.Levels {
	// lr.Result is Complete, Failed (lr.Failure.Code, located by lr.At) or Incomplete (lr.Reason, lr.Err)
}
valid := report.Result() == seekparquet.Complete
```

or, from the repository root, `go -C go run ./cmd/seekparquet validate [--level LEVEL] [--json] DIR`. The command
prints one line per level (or, with `--json`, one object) and exits 0 when every level completed, 1 when a
level failed, and 2 when a level is incomplete, validation could not run, or the arguments are wrong.
`Validate` does the same over any `Source`.

| Level | What this implementation checks |
|---|---|
| `structural` | the manifest and every part, as the first read to touch each would (CHECK-1; with every part checked, COL-5 covers every column); for every non-empty row group, the key column's ColumnIndex and OffsetIndex (RG-2 to RG-4), and the page header at every OffsetIndex entry, which must begin one data page of exactly the entry's size and rows (RG-3). Page data is not read. |
| `profile` | every key column chunk walked page header by page header from its start, without the OffsetIndex, which must then list exactly the data pages found (offsets, sizes, row positions, count); every key page decoded and every key checked: non-empty, valid UTF-8 for a `STRING` key, bounds valid UTF-8 too (KEY-2, KEY-6), in the declared order across pages, row groups and parts (KEY-3, PART-4), inside its page's ColumnIndex bounds (RG-5); each part's first and last keys equal to its manifest bounds (PART-3); the key chunk's metadata value count |
| `full` | every other column chunk walked the same way and decoded: a dictionary page is checked and decoded when found, whether or not a page uses it; each page passes its CRC when its header carries one, decompresses to its declared size, its levels (RLE; `BIT_PACKED` is unsupported, anything else corrupt) and values agree with its header, V2 null counts included (nested and repeated columns too; a chunk, and every V2 page, begins a row), the chunk holds exactly the row group's rows and the value count its metadata declares; an OffsetIndex, when present, lists exactly the pages found; a ColumnIndex, when present, has one entry per page and, for a column that is neither nested nor repeated, marks a page null only if every value is null and counts nulls exactly |
| `digest` | the SHA-256 of every part that declares `sha256`, streamed from the file, whether or not it is valid Parquet (PART-5); with no declared digest it completes with nothing checked and says so (VAL-2) |

Structural, profile and full each run the levels before them; digest runs alone. The indexes under
examination never choose what is read: every page the profile and full levels check is one they found by
walking the chunk, and an index that omits, adds or misplaces a page fails (spec §8). A fault fails the
level that finds it with its refusal code (`digest_mismatch` only at the digest level), and the levels after
it are incomplete. An unsupported codec or encoding (for example LZO, or legacy `BIT_PACKED` levels or values),
an encrypted part, a limit, or a storage error leaves the level incomplete, never failed: it says nothing
about validity. The full level still decodes the other columns, so that a fault in one of them fails it.
`ValidateOptions.Limits` bounds allocation as for reading (IndexMemory aside: no page index is kept).

Not checked: value-column ColumnIndex bounds against their values (their order depends on the column's
type), and what validation cannot see (VAL-3): atomic publication, and that files stay unchanged after they
are read.

## Untrusted input and limits

Every size a file declares (manifest, footer, page index, page header, page, value count) is checked against
`OpenOptions.Limits` before anything is allocated for it, and Thrift structures are walked with a bounded
decoder, nesting included, before they are decoded. The reader locates and decodes pages itself: every page
it decodes is one it located, through a checked OffsetIndex entry whose header must describe exactly that
page, or by walking the page headers. A page must decompress to exactly the size its header declares, and
decompression stops beyond it; the value counts its encodings declare (RLE runs, delta headers) are checked
against the header, and its decoded size against `Limits.PageBytes`, before parquet-go's decoders allocate.
Exceeding a limit is a `*LimitError` naming the limit, wrapping `ErrLimitExceeded`: the caller's policy, not a
refusal of the data. A negative limit is `ErrInvalidRequest` when the dataset is opened; zero takes the default. Defaults suit local
files; set them lower for untrusted remote data. A panic inside parquet-go on a hostile footer or page is
reported as `invalid_footer` or `invalid_parquet` as a last line of defence.

Dataset-wide row positions use signed 64-bit ordinals. Opening fails with `ErrLimitExceeded` if the
sum of the manifest's part row counts exceeds 2^63-1, before any ordinal can wrap. This fixed implementation
limit is separate from the configurable allocation limits and does not make the manifest invalid.

## Errors

| Error | Meaning |
|---|---|
| `*RefusalError` (`RefusalCodeOf`) | the data breaks the specification; its `RefusalCode` is from `spec/refusal-codes.json`, with the part, row group, row and key where known (validation reports it as a failed level) |
| `ErrInvalidRequest` | a malformed request, a malformed cursor token included |
| `*UnsupportedError` | valid data this reader does not handle; its `UnsupportedCode` is `unsupported_projection`, `unsupported_encoding` or `unsupported_encryption` |
| `*LimitError` (wraps `ErrLimitExceeded`) | a limit of `Limits`, or the reader's row capacity, with its name, value and maximum |
| anything else | the environment: storage, the context, `ErrClosed` |

## Sources, contexts and ownership

A `Source` opens the files of a dataset root (the manifest and the parts) by relative path, as `SourceFile`s.
`OpenDir` uses one over an `os.Root`, so no path or symbolic link reads outside the directory, and opens
files without blocking on a FIFO or device, which it reports as missing; it keeps the directory open until
the dataset closes. A part is opened the first time a read touches it, so the `Source` must stay usable
until `Dataset.Close`. Each part keeps its file open, and its checked footer in memory, until the dataset
closes; a part refused by its check is closed at once. No limit bounds how many: `Limits.Parts` bounds the
manifest and `Limits.IndexMemory` the retained page indexes, but a long-lived `Dataset` holds one open file
and one footer for every part it has touched, and `Verify` touches them all. Size the process's file
descriptors (an `OpenDir` part holds one) and memory for the most parts one `Dataset` will touch: a single
`Verify` or broad read over more parts than that fails when the process runs out, and opening a fresh
`Dataset` for it does not help. This is a known limitation, documented rather than solved: there is no cap
that refuses such a pass early, and no eviction. A remote store would be another `Source`; its file must read the same bytes for as long
as it is open, bound to one stored object (one version or generation) across its range requests and retries.

A file that implements `ContextBinder` has every read (manifest, footers, page indexes, pages) bound to the
operation's context, so a cancelled request or `Close` ends a read that waits on the source. `WithContext`
may be called concurrently and must return independent views. A plain `io.ReaderAt` cannot be interrupted
mid-call; the reader checks the context between pages.

`OpenDataset` opens and closes only the manifest. `Dataset.Close` closes every part file the dataset opened,
after the reads in progress end.

A remote `Source` can offer optional capabilities; a source with none of them is read through `Open` and
`ReadAt` as before. `ManifestReader.ReadManifest` reads the manifest whole within `Limits.ManifestBytes`, in one
request (`ErrObjectTooLarge` becomes a `*LimitError` naming `ManifestBytes`). `SizedSource.OpenSized` opens a
part with its manifest size, so that no metadata request precedes its first read, which must prove the size:
a `*SizeMismatchError` from it, or from `OpenSized`, is refused `part_mismatch`, a missing object
`missing_part`, and `ErrObjectChanged` (a broken pin) stays an environment error, never a refusal. A part file
that is a `RangeReader` reads several ranges in one `ReadRanges` call, with a `*BatchError` giving each range's
outcome; without it, ranges the reader issues together go to concurrent `ReadAt` calls, at most
`ReadPolicy.Concurrency` (default 64) per operation. A source that implements `IOReporter` reports its
physical requests to the `IOObserver` on each operation's context (`IOObserverFromContext`): the reader counts
them in `ReadStats` (`PhysicalRequests`, `PhysicalBytes`, `PeakPhysicalRequests`, ...; `OpenOptions.OpenStats`
for the open) and passes them on to an observer the caller set with `WithIOObserver`. `WithCacheMode` asks a
source's byte cache to be bypassed (`CacheBypass`).

Projected value pages reuse buffers within each `Rows`, `Get`, `Scan`, `List`, or `Plan.Scan` call,
including across row groups. `Rows`, `Get` and `List` results own their keys and binary values;
`Dataset.Scan` and `Plan.Scan` borrow page storage as described below. Dictionaries have separate storage
for their chunk. `Dataset.Scan` and `Plan.Scan` also decode every key page into one
buffer set of their own, each page overwriting the one before once the callbacks borrowing it have
returned; what the call keeps of a key across pages (the first key of a group of equal keys, the decoded
pages' bounds, a row group's cached first key) is copied first. Every other read (`Rows`, `Get`, `List`, routing
and planning probes, `Verify`, the list index) decodes each key page into storage of its own, never
reused, and validation does not reuse page buffers. No scratch is shared between calls.
The call retains each projection slot's largest byte and element buffers until it returns, including
codec state and both compact and expanded values for optional columns. `PageBytes` bounds page data;
header buffers can retain up to `headerBytes` (16 MiB, bounded by `MetadataMemory`), and indexed raw
extents include both header and payload.
`Limits.PageMemory` bounds the memory decoding one page holds, data and dictionary pages alike, key
pages included. It is estimated from the page's header before anything is allocated for its values:
24 bytes per decoded value (64-bit platforms), twice that for an optional column (its
non-null values, then every row), plus the bytes its decoded values hold (at most `PageBytes`). A
page over it fails with an error wrapping `ErrLimitExceeded`. Its default is the most `PageValues`
and `PageBytes` allow, 2 × 24 × `PageValues` + `PageBytes` (832 MiB at their defaults), so the default
refuses no page those limits admit; set it lower to bound decoding memory, at the cost of refusing
pages within the other limits. The bound is per page and per call: concurrent calls and scanned
shards each decode their own pages, so their memory adds up. Dictionaries, page headers/logs, indexes
and results add to that. Bound concurrent calls/shards and lower `Limits` for tighter memory use.
`IndexMemory` is separate; there is no aggregate transient-memory budget or guarantee of immediate
return of memory to the OS.

## Cost model

Opening reads the manifest: one read, whatever the number of parts. A read is routed to a part by its
manifest bounds; the first read to touch a part opens it and reads its footer (counted in
`ReadStats.PartsOpened` and `SourceReads`: the last `ReadPolicy.MetadataPrefetchBytes` of the part in one read
(2 MiB, or 64 KiB through `OpenDir`), the whole part when it is smaller, together with a 4-byte read of its
head when it is larger, and one more read of exactly the missing bytes when the footer is larger than that
window), so a cold query costs the manifest plus one footer per part it
touches, independent of the part count. The page-index bytes that read holds (a part's ColumnIndexes and
OffsetIndexes sit between its data and its footer) are kept, within `Limits.PageIndexCacheMemory` (128 MiB,
least recently used parts evicted first; footers and decoded indexes are outside it), and serve the part's later
index loads without a read (`ReadStats.MetadataHitBytes`); an index is still decoded and checked only when a
request needs it. `ReadPolicy.DisableSpeculation` reads a 64 KiB window, keeps only the footer and reads no page
ahead. A key column chunk's ColumnIndex and OffsetIndex not kept that way are read together. Within a request,
complete key-page extents admitted to the bounded request memo are reused (`ReadStats.RequestMemoHitBytes`);
extents not admitted may be read again. `Verify` checks up to `ReadPolicy.Concurrency` parts at once, each in
two rounds of reads (its end; then its row groups' first key pages and its last key page, its head merged into
the first when that page begins within 64 KiB), and reports the first failure in the part and check order of a
serial check; a second `Verify` of the same `Dataset` reads nothing. `OpenDir` with a zero `ReadPolicy` reads as
suits a local file: a 64 KiB window, one range at a time, no retained index bytes and no memo. Within the part, a read is routed by a binary search over its row groups' first
keys; the first time a row group is needed, its key page index is loaded and its first key page decoded, and
both are kept (`Limits.IndexMemory` accounts for key-page entries, their bound bytes,
and decoded row-group first-key bytes). This budget does not include allocator overhead,
parsed footers, transient page-decoding buffers or returned results. A seek to a repeated key begins at the predecessor
of the first row group whose first key is at or above it, so earlier rows of its group are kept. A cursor
finds its group's first row the same way, then moves to the row after it through the OffsetIndex row
positions, without decoding the rows between. A warm read then decodes the key pages from the one that can
hold its start, and value pages only for the rows it returns; a value column without an OffsetIndex has its
page headers walked from the start of its chunk, decoding only the pages it returns rows from. A projected
column chunk's OffsetIndex is read and checked the first time a request needs it and its page locations kept,
within `Limits.OffsetIndexMemory` (default 256 MiB, apart from `IndexMemory`); past that budget each request
reads it again, never failing for it. A listing
resumes after each common prefix at the prefix's successor, skipping the pages wholly under it, but the page
it lands on, and the pages around a prefix's boundaries, still decode some of the keys under it.

**Key page read-ahead for listings.** Each of those resumes reads one key page, one after another: over an
object store a listing of 100 common prefixes waits for about 100 round trips. `ListQuery.ReadAhead` (default 0,
off) lets a listing predict, from the row group's key page index, the pages it will decode next and read up to
that many of them concurrently, holding at most 1 MiB per slot of pages read and not yet used (a request with
`ReadAhead: 64` can hold 64 MiB). The pages read ahead are checked as any page read is, and the entries do not
change; a page predicted wrongly (shortened or overlapping page bounds) is read and transferred for nothing and
held until the scan passes it or the key scan ends, counted in `ReadStats.ReadAheadUnused` and
`ReadAheadUnusedBytes`. The reads ahead end when the key scan does, before values are projected. The
prediction stays within the row group the listing is in. With a list index built, a `/` listing reads nothing
ahead, including a row group the index declines; other delimiters read ahead as usual. With a 5 ms wait per read, a cold listing of 100 folders of 4,000 keys (1,024-row key pages) took 570 ms
without read-ahead, and 106 ms (`ReadAhead: 8`) or 71 ms (`ReadAhead: 16`) with it, for the same 105 reads. On
a local file it saves nothing and costs a goroutine per read ahead: leave it off there.

**Optional slash listing index.** A server that needs predictable wide-prefix listing latency can call:

```go
stats, err := d.BuildListIndex(ctx, seekparquet.ListIndexOptions{
    MaxBytes: 64 << 20, Workers: 2, MaxChangesPerPage: 8,
})
```

Opening remains lazy. The build reads key pages and keeps directory-run starts as suffixes relative
to a shared group anchor. Unchanged pages add nothing; fully decoded flat groups retain no index data.
Dense pages retain exact first-key markers and decline skipped intervals containing a marker.
Build-only page indexes and first keys are transient: they never consume the shared, non-evicting
`Limits.IndexMemory` cache (Java: `maxIndexMemoryBytes`). Checked part footers remain shared. Ordinary routing probes and synthetic landings
use the normal retaining cache and its memory limit; successful transient index loads also count
in read statistics.

Complete groups are admitted in dataset order until the first over-budget group. Data refusals and unsupported codecs/encodings omit
the affected groups, including both sides of a bad boundary; ordinary reads still report those
conditions when encountered. Storage/I/O errors, resource limits and cancellation fail the attempt without
publication, so a caller may retry after resolving them. Publication is atomic alongside concurrent lists.

A nonempty index is reused by later calls. `ListIndexStats.Options` and `HasIndex` report the **effective** options
and whether any groups are indexed. `OptionsMismatch` (Java: `optionsMismatch()`) explicitly flags
reuse with different normalized settings. To replace a nonempty index, reopen the dataset. An empty outcome is cached for
the most recent options, including its original duration, so repeating that build performs no I/O;
changing options replaces that empty outcome. This cache holds only one result, not a map of all calls.
Defaults are 64 MiB, one worker, and eight directory changes per page. A retained index does not
guarantee every listing benefits: if the default cap marks every page dense, all skips may decline.
Check the skip/decline counters before choosing a larger cap or budget.

Both implementations charge the same conservative formula: 256 bytes for the snapshot, and per
retained group 256 bytes plus each of the base-key length, suffix-data length and 40 × run count
rounded up to a power of two (zero stays zero). This includes compact containers, dense counts and
allocator slack. Empty outcomes retain only a fixed build-status record, not boundary arrays.
The retained byte budget is **not a peak build-memory limit**: allow roughly Workers × 2 × MaxBytes
for candidate buffers and compaction, plus the retained index, one page's boundary expansion per
worker, transient page indexes, decoding buffers and part footers under the ordinary reader limits.
Oversized single keys/pages can exceed that estimate; flat groups must still finish checking.
Sequential build order checking retains only the preceding page's boundary, not every decoded page.

Only `/` uses the index. Slash-ended prefix cursors may start at certified anchors; other valid cursors
use ordinary seeking. Sorted anchors are searched by binary search for the prefix successor, with
cumulative dense counts certifying the skipped interval. Exact first keys route over whole groups.
Cancellation is checked during searches and at listing completion, including ordinary listings.
Logical position, exact rows, projected values and cursors remain unchanged. Java synthetic group
transitions clear the previous group's value pages, dictionaries and sequential readers.
Build statistics report groups, pages, entries, retained bytes, budget exhaustion and duration;
skipped counts cover visited groups, not unopened parts. Go cancellation reports decoded work,
including the last batch, but zero retained groups/entries/bytes.
Read statistics report answered skips and delimiter, missing-group, dense-page and unsafe-run declines.

The retained benchmark generates 2,000 directories × 3,000 keys with `local.WriteDir` defaults, plus
six million flat keys with 1,000-byte numeric fields. Set `SEEKPARQUET_LIST_BENCH_DIR` to an existing
parent directory to keep and reuse its `wide/` and `flat/` datasets (also used by the Java benchmark):

```sh
SEEKPARQUET_LIST_BENCH_DIR=/path/to/bench go test ./local -run '^$' -bench BenchmarkListBoundary -benchtime=5x
```

**Planned selections.** `d.Plan(ctx, selections, columns, stats)` plans reading every row of a set of
selections (their spans and cursors, a row cursor resuming inside a group; rows several selections select
count once). Planning decodes the key pages at the two ends of each selection, as `Rows` starting there
would, and with columns reads the OffsetIndex of each projected column chunk the rows span, but no data page.
`Count()` is the row count, exact once every part holding a selected row is checked: always with columns,
and otherwise only when no part between the ends is still untouched. `ProjectedBytes()` is the compressed
bytes `Scan` will request for the projected columns: the extent of each page holding a selected row and of
its chunk's dictionary, once each however many selections share them; it is not exact when a projected
chunk has no OffsetIndex. `plan.Scan(ctx, stats, fn func(*Batch) error)` yields the rows in dataset order
with their values as borrowed batches, reading each counted extent in one request, and checks every key page
it decodes, reading a chunk's dictionary before its first page whether or not the pages use it. A page an
OffsetIndex locates is read in one request of exactly its extent, by `Rows` and `List` too; they read a
dictionary only for a page that uses it. A projected key column is read as a value column and its chunk counts
in `ProjectedBytes`; the Java reader takes key values from its key pages and counts nothing for it (see
[java/README.md](../java/README.md#parity-with-the-go-reader)), so the conformance planning cases never
project the key.

**Batches.** `Batch.Len()`, `Key(i)`, and `Offset(i)` identify rows; `Column(c)` returns a `ColumnView` of
projection slot `c`, in request order, including duplicates: a small value over the decoded page, made
without allocating, with `Present()`, `Kind()`, `Len()`, `IsNull(i)`, `Value(i)` (a borrowed `Value`) and
typed accessors (`Int64(i)`, `Bytes(i)`, ...) for loops over a column of a known `Kind`: they test no kind
per value, so on a column of another kind their bits are unspecified (never a panic; `Bytes` returns no bytes), where
`Value(i)`'s accessors return the zero value. A view that is not `Present` is a column the part lacks: its
`Len()` values are absent. `Batch.Columns()` names the slots. A projected key remains an ordinary projected
column.

Each batch is a positive run within one row group, one key page, one page of every projected column,
and one selected span, and holds at most 4,096 rows when a projected column is absent from the part (such a
column takes no storage). It never crosses a gap in the selection. Pages need not align across columns;
batches may be small, and maximal size is not promised. **The batch object, views, keys and value
bytes are borrowed, read-only, and valid only until the callback returns.** Do not retain them or
pass them to work that outlives the callback. `batch.Row(i)` and `batch.Cursor(i)` make owning copies,
and `Value.Clone` an owning value. Accessors panic on out-of-range indexes.

Both scans stop at the first callback error, return it, and populate read statistics on failure; a
callback that returns `seekparquet.Stop` ends the scan with a nil error. A nil callback is
`ErrInvalidRequest`. Cancellation is checked before each batch callback. Completed batches are delivered
before a later decoding error, even if the failing projected page is within the same key page or the next
key page fails to decode. Each concurrent or reentrant scan has its own scratch. Both hold a Dataset lease
during callbacks: **the callback must not close the Dataset or wait for a goroutine that does**, since
Close waits for the scan. Return `Stop` or an error, or cancel the context, to stop, and close after the
scan returns. Requests started by the callback work normally until Close begins, then fail with `ErrClosed`.

**Unplanned batch reads.** `d.Scan(ctx, q, stats, fn func(*Batch) error)` hands `fn` the rows `Rows`
would return for the same query, as the same borrowed `Batch`es, with no plan: span, `Cursor`, `Columns`
and a positive `Limit` mean what they mean to `Rows`, and a zero `Limit` scans to the end of the span (a
streaming extension of this library; the specification's range, and `Rows`, take a positive limit). It reads
what `Rows` reads: it opens each projected chunk when the scan reaches its row group, reads a dictionary
only for a page that uses it, and does not position at the upper bound or read the OffsetIndexes of the rest
of the span, so a small limit on a long span costs what the rows it returns cost. A successful request
whose callbacks all return nil has the `ReadStats` the same `Rows` would have against the same cache state.
A request that stops early does not: `Rows` reads every key before any value, so a corrupt later key page
fails it before it decodes a value page, while `Scan` has already decoded values and run callbacks;
a callback error, cancellation, or another request warming the caches meanwhile changes the counts too.
It stops at the limit without advancing to the next row; the last batch ends at the limit or the upper bound,
and a span with no rows never calls `fn`. It is not a planned scan: there is no `Count`, `ProjectedBytes`
or `Split`; use `Plan` for those.

`plan.Split(ctx, n, stats)` divides a plan into at most `n` shards for a whole scan on several cores
(`n` below 1 is `ErrInvalidRequest`; a plan with no rows has no shard): each shard is a `Plan` over
consecutive rows of the plan, and the shards' `Scan`s, run on the caller's goroutines with their own
`ReadStats`, together yield the plan's rows, each once, shard by shard in dataset order. Boundaries
fall at a selected row that begins a row group and balance `ProjectedBytes` when the plan's bytes are exact,
else the row count; in repeated-key mode a boundary never divides a group of equal keys, and moves past a group that
continues from the row group before, after decoding the two key pages around it (unique keys need no read).
The shards' counts and projected bytes add up to the plan's, since each row group's pages and dictionaries
belong to one shard. Splitting is a convenience of this reader, not part of the format or the conformance
protocol.

**Read statistics.** Pass a `*ReadStats` to any read (`Rows`, `Get`, `Scan`, `List`, `Plan`, `Split`,
`Plan.Scan`) to see what it did: key and value pages decoded, keys decoded, row groups opened and key page
indexes loaded, page-index seeks and prefix resumes, row groups a listing skipped whole, and the `Source`
reads and requested bytes made on its behalf. It is reset when the operation starts; the counts are exact
for the operation, are filled whether it succeeds or fails, and cost one integer increment per event.
`ReadStats.Add` sums the counts of several operations. They are diagnostics of this reader's access path,
not part of the format; the caller owns a `ReadStats` for the duration of the call and must not share it
between concurrent requests.
