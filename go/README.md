# seekparquet for Go

A Go reader for [SeekParquet](../spec/format-1.md) datasets: ordinary Parquet files sorted by one byte-string
key column named by the manifest, split into key-disjoint parts and committed by a `manifest.json`. It finds the first row at or
after a key, reads key ranges and prefixes, and lists a prefix with delimiter rollups, by reading the
manifest, the footers, the key column's page index and a few pages.

```
go get github.com/varveio/seekparquet/go
```

| | |
|---|---|
| Module | `github.com/varveio/seekparquet/go` (v0: the API may still change) |
| Format | format 1, draft revision 1: files stamped `format_version` 0, `draft_revision` 1. Every other pair is refused with `unsupported_version`. |
| Go | 1.26 or later |
| Dependencies | the standard library and `github.com/parquet-go/parquet-go` v0.32.0 |

## Usage

```go
ctx := context.Background()
d, err := local.OpenDataset(ctx, "/data/listing", seekparquet.Options{})
if err != nil {
	return err // seekparquet.CodeOf(err) gives the refusal code, if it is one
}
defer d.Close()

// A range: start inclusive (StartExclusive to exclude it), end exclusive.
rows, err := d.Range(ctx, seekparquet.Range{Start: []byte("logs/2026/"), Limit: 100, Columns: []string{"size"}}, nil)

// A prefix read, which may also take a start and an end.
rows, err = d.Range(ctx, seekparquet.PrefixRange([]byte("logs/2026/"), 100), nil)

// A delimited listing, paged.
req := seekparquet.List{Prefix: []byte("logs/"), Delimiter: '/', Limit: 1000}
for {
	entries, err := d.List(ctx, req, nil)
	if err != nil || len(entries) == 0 {
		break
	}
	// entries[i].IsPrefix(), entries[i].CommonPrefix, entries[i].Row
	req.Cursor = entries[len(entries)-1].Cursor()
}
```

The manifest requires `key: {"column": "<name>", "unique": true|false}`. Its exact name and
uniqueness must match the footer stamps. `d.Metadata().Key` exposes the declaration.
Equal-key groups retain file order, may span pages and row groups, and stay in one part.
Start/end and prefix filters select whole groups; limits may split them.

`KeyRange(key, limit, columns...)` looks up one group. To paginate a range, set
`req.Cursor = rows[len(rows)-1].Cursor()`. `Row.Offset` is the zero-based position in the
group, always zero in unique mode. `Entry.Cursor()` also tags common prefixes correctly.
The existing `List.After` byte floor and `Continuation` remain available for explicit listing
bounds; use `Cursor` to paginate repeated rows and do not combine it with `After`.

`OpenParts` defaults to unique keys in column `key`; set `PartsOptions.Key` to an explicit
`Key` declaration for other names or repeated keys. `seekparquettest.Dataset` supports
`KeyColumn` and `RepeatedKeys` for writing such test data.

Runnable examples are in [example_test.go](example_test.go).

| Package | What it is |
|---|---|
| `seekparquet` | `OpenDataset` (a manifest dataset through a `Source`), `OpenParts` (advanced: parts given directly, with footer and row-group hooks), `Reader`, `Dataset`, `Range`, `List`, `Scan` with a `ColumnSink`, `Row`, `Value`, `Entry`, `Stats`, `Limits`, `Successor`, `CommonPrefix`, refusal codes and errors |
| `source/local` | `Dir`, a `Source` over a local directory confined by `os.Root`, and `FileCache`, which bounds the descriptors the parts hold open |
| `seekparquettest` | writes small spec-conforming datasets for tests, with knobs for invalid variants |
| `internal/cmd/conformance` | the adapter for the [conformance harness](../conformance/protocol.md) |

To run the conformance suite from the repository root:

```
python3 conformance/harness/run.py --adapter "go -C go run ./internal/cmd/conformance"
```

or build the adapter first (`go -C go build -o /tmp/sp-adapter ./internal/cmd/conformance`) and pass its path.

## Results

- Rows own their key and value bytes. `Row.Columns` is the result's own copy of the projected names.
- A projected column a part does not have is `Value.Absent`, which is distinct from a null value (COL-2). A
  column no part has is absent in every row. Projecting a nested or repeated column is an
  `*UnsupportedError` (`unsupported_projection`), never reported as absent.
- Values are parquet-go `Value`s of the column's physical type. This module commits to parquet-go's value
  types rather than defining its own, so a parquet-go upgrade can change this API.
- A limit that is not positive is `ErrInvalidRequest`, not an empty result.

## What opening and reading check, and what they do not certify

Opening checks what the specification's CHECK-1 requires, and by default nothing more: the manifest
(strictly: duplicate member names, undefined members, exact integers, canonical base64, part paths, bound
order), that every part exists with the manifest's size and row count, that its footer is readable,
self-contained and unencrypted, its `seekparquet.*` stamps, the manifest-named key column (type, repetition, annotation,
`column_orders`), that columns agree across parts (COL-5), and `sorting_columns` on every non-empty row group.

`Options.VerifyOnOpen` also checks, on open, every non-empty row group's key page index and first key page,
and each part's last key page (CHECK-3): row-group first keys must ascend and each part's first and last keys
must equal its manifest bounds. That suits a long-lived reader of local files; it costs reads across the whole
dataset, so it is off by default. Without it the same checks happen when a read reaches those pages.

Every read checks each row group's key page index before it uses the group, and each key page it decodes
(CHECK-2): the page header matches its OffsetIndex entry, one non-empty key per row, valid UTF-8 for a
`STRING` key, non-decreasing (strictly ascending when `key.unique` is true), inside the page's bounds and the part's manifest bounds, and in dataset
order with every other page the request decoded. Page bounds are used as Parquet defines them, as bounds
whatever their length; overlapping bounds and `boundary_order` are never relied on.

What is **not** certified: rows a request does not decode (a query that succeeds says nothing about the pages
it skipped), values beyond decoding them, `sha256` digests, and that files are unchanged after opening (a
part replaced by another file is refused when it is reopened, not one rewritten in place). Full
certification is validation (spec §8), which this module does not yet provide.

## Untrusted input and limits

Every size a file declares (manifest, footer, page index, page header, page, value count) is checked against
`Options.Limits` before anything is allocated for it, and Thrift structures are walked with a bounded
decoder before parquet-go decodes them, so a few hostile bytes cannot ask for gigabytes. Exceeding a limit is
`ErrLimitExceeded`, the caller's policy, not a refusal of the data. Defaults suit local files; set them lower
for untrusted remote data. A panic inside parquet-go on a hostile footer or page is reported as
`invalid_footer` or `invalid_parquet` as a last line of defence.

## Errors

| Error | Meaning |
|---|---|
| `*RefusalError` (`CodeOf`) | the data breaks the specification; `Code` is from `spec/refusal-codes.json`, with the part, row group, row and key where known |
| `ErrInvalidRequest` | a malformed request |
| `*UnsupportedError` | valid data this reader does not handle: `unsupported_projection`, `unsupported_encoding`, `unsupported_encryption` |
| `*ExpectationError` | a part fails a caller's `ColumnExpectation`; a policy failure, not a refusal |
| anything else | the environment: storage, the context, `ErrClosed`, `ErrLimitExceeded` |

## Sources, contexts and ownership

A `Source` opens the files of a dataset root (the manifest and the parts) by relative path, as `SourceFile`s;
`local.Dir` is one, and a remote store would be another. A file must read the same bytes for as long as it is
open: a remote source binds it to one stored object (one version or generation) across its range requests
and retries.

A file that implements `ContextBinder` has every read (manifest, footers, page indexes, pages) bound to the
operation's context, so a cancelled request or `Close` ends a read that waits on the source. `WithContext`
may be called concurrently and must return independent views. A plain `io.ReaderAt` cannot be interrupted
mid-call; the reader checks the context between pages.

`OpenDataset` closes every file it opened when it fails; on success `Dataset.Close` closes them, after the
reads in progress end. `OpenParts` takes ownership of the files it is given in the same way. A `ColumnSink`
must not call `Close` from a callback.

## Cost model

Opening reads the manifest and every part's footer: a few reads per part, whatever its size. A read is
routed to a part by its manifest bounds, and within the part by a binary search over its row groups' first
keys; the first time a row group is needed, its key page index is loaded and its first key page decoded, and
both are kept (`Limits.IndexMemory` bounds what is kept). Repeated-key seeks begin at the
predecessor of the first row-group first key at or above the target, so earlier rows of a group
are retained. Row-offset continuation counts forward from that group's start. A warm read then decodes the key pages from the one
that can hold its start, and value pages only for the rows it returns; a value column without an OffsetIndex
is decoded from the start of its chunk. A listing resumes after each common prefix at the prefix's successor,
skipping the pages wholly under it, but the page it lands on, and the pages around a prefix's boundaries,
still decode some of the keys under it. Each page read also reads its header once to check it. `Stats`
counts decoding work only, not bytes read or opening costs.
