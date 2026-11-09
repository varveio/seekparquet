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
d, err := seekparquet.OpenDir(ctx, "/data/listing", seekparquet.Options{})
if err != nil {
	return err // seekparquet.CodeOf(err) gives the refusal code, if it is one
}
defer d.Close()

// A range: start inclusive (StartExclusive to exclude it), end exclusive.
rows, err := d.Range(ctx, seekparquet.Range{Start: []byte("logs/2026/"), Limit: 100, Columns: []string{"size"}})

// A prefix read, which may also take a start and an end.
rows, err = d.Range(ctx, seekparquet.PrefixRange([]byte("logs/2026/"), 100))

// A delimited listing, paged.
req := seekparquet.List{Prefix: []byte("logs/"), Delimiter: '/', Limit: 1000}
for {
	entries, err := d.List(ctx, req)
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
whole groups. `KeyRange(key, limit, columns...)` looks up one group.

Every result pages the same way: set the request's `Cursor` to the last entry's `Cursor()` and repeat
the request. A row cursor is the row's key and its `Row.Offset`, the zero-based position in its group
(always zero with unique keys); a listing may also continue after a common prefix (`Entry.Cursor()` of
a prefix entry). A range takes only a row cursor. A prefix entry carries its exact row in `Entry.Exact`
when its first candidate row is stored at the prefix itself (an `a/` folder object under the prefix `a/`), with
its projected values, so a listing pairs the two without a lookup (spec LIST-8). A page may end inside a group, and resuming deep
inside a large group costs a few pages, not the rows before the cursor.

## API

| Package | What it is |
|---|---|
| `seekparquet` | `OpenDataset` (a dataset through a `Source`), `OpenDir` (a local directory), `Dataset` with `Range`, `List`, `PlanRanges`, `Verify`, `Metadata`, `Columns` and `Close`; `RangePlan` with `Count`, `ProjectedBytes`, `Scan`, `Split` and `Stats`; the requests `Range`, `List`, `PrefixRange`, `KeyRange` and `Cursor`; the results `Row`, `Value`, `Entry`; `Column`, a part's column as its footer declares it; `Options` and `Limits`; refusal codes and errors; `Validate` and `ValidateDir` with `ValidateOptions`, `Level` and `Report`; the format constants and stamp keys, `Manifest` with `ParseManifest` and `MarshalManifest`, `InspectFooter` and `InspectKeyPages` |
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
- A projected column a part does not have is `Value.Absent`, which is distinct from a null value (COL-2). A
  column no part has is absent in every row. Projecting a nested or repeated column is an
  `*UnsupportedError` (`unsupported_projection`), never reported as absent.
- Values are parquet-go `Value`s of the column's physical type. This module commits to parquet-go's value
  types rather than defining its own, so a parquet-go upgrade can change this API.
- A limit that is not positive, or a malformed cursor, is `ErrInvalidRequest`, not an empty result.

## What opening and reading check, and what they do not certify

Opening checks the manifest and nothing else (CHECK-1): duplicate member names, undefined members, exact
integers, canonical base64, part paths, bound order. It opens no part. The manifest carries each part's
exact key bounds, row count and size, which is all a read needs to be routed.

A part is checked the first time a read routes to it: it exists with the manifest's size and row count, its
footer is readable, self-contained and unencrypted, its `seekparquet.*` stamps match, the manifest-named key
column is valid (type, repetition, annotation, `column_orders`), its columns agree with those of every part
already checked (COL-5), and every non-empty row group declares `sorting_columns`. A part that fails is
refused by the `Range` or `List` that touched it, with the same codes, and by every later read that touches
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
| `full` | every other column chunk walked the same way and decoded: a dictionary page is checked and decoded when found, whether or not a page uses it; each page passes its CRC when it has one, decompresses to its declared size, its levels (RLE; `BIT_PACKED` is unsupported, anything else corrupt) and values agree with its header, V2 null counts included (nested and repeated columns too; a chunk, and every V2 page, begins a row), the chunk holds exactly the row group's rows and the value count its metadata declares; an OffsetIndex, when present, lists exactly the pages found; a ColumnIndex, when present, has one entry per page and, for a column that is neither nested nor repeated, marks a page null only if every value is null and counts nulls exactly |
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
`Options.Limits` before anything is allocated for it, and Thrift structures are walked with a bounded
decoder, nesting included, before they are decoded. The reader locates and decodes pages itself: every page
it decodes is one it located, through a checked OffsetIndex entry whose header must describe exactly that
page, or by walking the page headers. A page must decompress to exactly the size its header declares, and
decompression stops beyond it; the value counts its encodings declare (RLE runs, delta headers) are checked
against the header, and its decoded size against `Limits.PageBytes`, before parquet-go's decoders allocate.
Exceeding a limit is `ErrLimitExceeded`, the caller's policy, not a refusal of the data. Defaults suit local
files; set them lower for untrusted remote data. A panic inside parquet-go on a hostile footer or page is
reported as `invalid_footer` or `invalid_parquet` as a last line of defence.

Dataset-wide row positions use signed 64-bit ordinals. Opening fails with `ErrLimitExceeded` if the
sum of the manifest's part row counts exceeds 2^63-1, before any ordinal can wrap. This fixed implementation
limit is separate from the configurable allocation limits and does not make the manifest invalid.

## Errors

| Error | Meaning |
|---|---|
| `*RefusalError` (`CodeOf`) | the data breaks the specification; `Code` is from `spec/refusal-codes.json`, with the part, row group, row and key where known (validation reports it as a failed level) |
| `ErrInvalidRequest` | a malformed request |
| `*UnsupportedError` | valid data this reader does not handle: `unsupported_projection`, `unsupported_encoding`, `unsupported_encryption` |
| anything else | the environment: storage, the context, `ErrClosed`, `ErrLimitExceeded` |

## Sources, contexts and ownership

A `Source` opens the files of a dataset root (the manifest and the parts) by relative path, as `SourceFile`s.
`OpenDir` uses one over an `os.Root`, so no path or symbolic link reads outside the directory, and opens
files without blocking on a FIFO or device, which it reports as missing; it keeps the directory open until
the dataset closes. A part is opened the first time a read touches it, so the `Source` must stay usable
until `Dataset.Close`. Each part keeps its file open until the dataset closes; a part refused by its check
is closed at once. A remote store would be another `Source`; its file must read the same bytes for as long
as it is open, bound to one stored object (one version or generation) across its range requests and retries.

A file that implements `ContextBinder` has every read (manifest, footers, page indexes, pages) bound to the
operation's context, so a cancelled request or `Close` ends a read that waits on the source. `WithContext`
may be called concurrently and must return independent views. A plain `io.ReaderAt` cannot be interrupted
mid-call; the reader checks the context between pages.

`OpenDataset` opens and closes only the manifest. `Dataset.Close` closes every part file the dataset opened,
after the reads in progress end.

## Cost model

Opening reads the manifest: one read, whatever the number of parts. A read is routed to a part by its
manifest bounds; the first read to touch a part opens it and reads its footer (a few reads, counted in
`ReadStats.PartsOpened` and `SourceReads`), so a cold query costs the manifest plus one footer per part it
touches, independent of the part count. Within the part, a read is routed by a binary search over its row groups' first
keys; the first time a row group is needed, its key page index is loaded and its first key page decoded, and
both are kept (`Limits.IndexMemory` accounts for key-page entries, their bound bytes,
and decoded row-group first-key bytes). This budget does not include allocator overhead,
parsed footers, transient page-decoding buffers or returned results. A seek to a repeated key begins at the predecessor
of the first row group whose first key is at or above it, so earlier rows of its group are kept. A cursor
finds its group's first row the same way, then moves to the row after it through the OffsetIndex row
positions, without decoding the rows between. A warm read then decodes the key pages from the one that can
hold its start, and value pages only for the rows it returns; a value column without an OffsetIndex has its
page headers walked from the start of its chunk, decoding only the pages it returns rows from. A listing
resumes after each common prefix at the prefix's successor, skipping the pages wholly under it, but the page
it lands on, and the pages around a prefix's boundaries, still decode some of the keys under it.

**Planned selections.** `d.PlanRanges(ctx, ranges, columns)` plans reading every row of a set of ranges
(their bounds and cursors; rows several ranges select count once). Planning decodes the key pages at the two
ends of each range, as a `Range` starting there would, and with columns reads the OffsetIndex of each
projected column chunk the rows span, but no data page. `Count()` is the row count, exact once every part
holding a selected row is checked: always with columns, and otherwise only when no part between the ends is
still untouched. `ProjectedBytes()` is the compressed bytes `Scan` will request for the projected columns:
the extent of each page holding a selected row and of its chunk's dictionary, once each however many ranges
share them; it is not exact when a projected chunk has no OffsetIndex. `Scan(ctx, stats, fn)` yields the
rows in dataset order with their values, reading each counted extent in one request, and checks every key
page it decodes, reading a chunk's dictionary before its first page whether or not the pages use it. A page an
OffsetIndex locates is read in one request of exactly its extent, by `Range` and `List` too; they read a
dictionary only for a page that uses it. A projected key column is read as a value column and its chunk counts
in `ProjectedBytes`; the Java reader takes key values from its key pages and counts nothing for it (see
[java/README.md](../java/README.md#parity-with-the-go-reader)), so the conformance planning cases never
project the key.

`plan.Split(ctx, n)` divides a plan into at most `n` shards for a whole scan on several cores: each shard is a
`RangePlan` over consecutive rows of the plan, and the shards' `Scan`s, run on the caller's goroutines with
their own `ReadStats`, together yield the plan's rows, each once, shard by shard in dataset order. Boundaries
fall at a selected row that begins a row group and balance `ProjectedBytes` when the plan's bytes are exact,
else the row count; in repeated-key mode a boundary never divides a group of equal keys, and moves past a group that
continues from the row group before, after decoding the two key pages around it (unique keys need no read).
The shards' counts and projected bytes add up to the plan's, since each row group's pages and dictionaries
belong to one shard. Splitting is a convenience of this reader, not part of the format or the conformance
protocol.

**Read statistics.** Set `Range.Stats` or `List.Stats` to a `*ReadStats` to see what one request did: key and
value pages decoded, keys decoded, row groups opened and key page indexes loaded, page-index seeks and prefix
resumes, row groups a listing skipped whole, and the `Source` reads and requested bytes made on its behalf.
The counts are exact for the request, are filled whether it succeeds or fails, and cost one integer increment
per event. They are diagnostics of this reader's access path, not part of the format; the caller owns a
`ReadStats` for the duration of the call and must not share it between concurrent requests.
