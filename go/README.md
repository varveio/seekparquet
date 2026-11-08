# seekparquet for Go

A Go reader and validator for [SeekParquet](../spec/format-1.md) datasets: ordinary Parquet files sorted
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
	// entries[i].IsPrefix(), entries[i].CommonPrefix, entries[i].Row
	req.Cursor = entries[len(entries)-1].Cursor()
}
```

Runnable examples are in [example_test.go](example_test.go).

## Keys and pagination

The manifest declares `key: {"column": "<name>", "unique": true|false}`; the footer stamps must match
it, and `d.Metadata().Key` exposes it. When keys are not unique, consecutive equal keys form a group in
producer file order; a group may span pages and row groups but never parts. Start, end and prefix select
whole groups. `KeyRange(key, limit, columns...)` looks up one group.

Every result pages the same way: set the request's `Cursor` to the last entry's `Cursor()` and repeat
the request. A row cursor is the row's key and its `Row.Offset`, the zero-based position in its group
(always zero with unique keys); a listing may also continue after a common prefix (`Entry.Cursor()` of
a prefix entry). A range takes only a row cursor. A page may end inside a group, and resuming deep
inside a large group costs a few pages, not the rows before the cursor.

## API

| Package | What it is |
|---|---|
| `seekparquet` | `OpenDataset` (a dataset through a `Source`), `OpenDir` (a local directory), `Dataset` with `Range`, `List`, `Metadata` and `Close`; the requests `Range`, `List`, `PrefixRange`, `KeyRange` and `Cursor`; the results `Row`, `Value`, `Entry`; `Options` and `Limits`; refusal codes and errors; `Validate` and `ValidateDir` with `ValidateOptions`, `Level` and `Report` |
| `cmd/seekparquet` | the `seekparquet validate` command |
| `internal/...` | the manifest and Thrift checkers, the test-data writer, and the conformance adapter (`internal/cmd/conformance`) for the [conformance harness](../conformance/protocol.md) |

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

Opening checks what the specification's CHECK-1 requires, and by default nothing more: the manifest
(strictly: duplicate member names, undefined members, exact integers, canonical base64, part paths, bound
order), that every part exists with the manifest's size and row count, that its footer is readable,
self-contained and unencrypted, its `seekparquet.*` stamps, the manifest-named key column (type, repetition,
annotation, `column_orders`), that columns agree across parts (COL-5), and `sorting_columns` on every
non-empty row group.

`Options.VerifyOnOpen` also checks, on open, every non-empty row group's key page index and first key page,
and each part's last key page (CHECK-3): row-group first keys must be in the declared order and each part's
first and last keys must equal its manifest bounds. That suits a long-lived reader of local files; it costs
reads across the whole dataset, so it is off by default. Without it the same checks happen when a read
reaches those pages.

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
| `structural` | everything opening checks (CHECK-1, which includes COL-5 for every column); for every non-empty row group, the key column's ColumnIndex and OffsetIndex (RG-2 to RG-4), and the page header at every OffsetIndex entry, which must begin one data page of exactly the entry's size and rows (RG-3). Page data is not read. |
| `profile` | every key column chunk walked page header by page header from its start, without the OffsetIndex, which must then list exactly the data pages found (offsets, sizes, row positions, count); every key page decoded and every key checked: non-empty, valid UTF-8 for a `STRING` key, bounds valid UTF-8 too (KEY-2, KEY-6), in the declared order across pages, row groups and parts (KEY-3, PART-4), inside its page's ColumnIndex bounds (RG-5); each part's first and last keys equal to its manifest bounds (PART-3) |
| `full` | every other column chunk walked the same way and decoded: each page passes its CRC when it has one, decompresses to its declared size, its levels and values agree with its header (nested and repeated columns included), the chunk holds exactly the row group's rows and the value count its metadata declares; an OffsetIndex, when present, lists exactly the pages found; a ColumnIndex, when present, has one entry per page and, for a column that is neither nested nor repeated, marks a page null only if every value is null and counts nulls exactly |
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
sum of the parts' row counts exceeds 2^63-1, before any ordinal can wrap. This fixed implementation
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
files without blocking on a FIFO or device, which it reports as missing. Each part keeps its file open until
the dataset closes. A remote store would be another `Source`; its file must read the same bytes for as long
as it is open, bound to one stored object (one version or generation) across its range requests and retries.

A file that implements `ContextBinder` has every read (manifest, footers, page indexes, pages) bound to the
operation's context, so a cancelled request or `Close` ends a read that waits on the source. `WithContext`
may be called concurrently and must return independent views. A plain `io.ReaderAt` cannot be interrupted
mid-call; the reader checks the context between pages.

`OpenDataset` closes every file it opened when it fails; on success `Dataset.Close` closes them, after the
reads in progress end.

## Cost model

Opening reads the manifest and every part's footer: a few reads per part, whatever its size. A read is
routed to a part by its manifest bounds, and within the part by a binary search over its row groups' first
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
