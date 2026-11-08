# seekparquet for Go

A Go reader for [SeekParquet](../spec/format-1.md) datasets: ordinary Parquet files sorted by one byte-string
`key` column, split into key-disjoint parts and committed by a `manifest.json`. It finds the first row at or
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

// A prefix read.
rows, err = d.Range(ctx, seekparquet.PrefixRange([]byte("logs/2026/"), 100), nil)

// A delimited listing, paged.
req := seekparquet.List{Prefix: []byte("logs/"), Delimiter: '/', Limit: 1000}
for {
	entries, err := d.List(ctx, req, nil)
	if err != nil || len(entries) == 0 {
		break
	}
	// entries[i].IsPrefix(), entries[i].CommonPrefix, entries[i].Row
	req.After, req.Continuation = entries[len(entries)-1].Bytes(), true
}
```

Runnable examples are in [example_test.go](example_test.go).

| Package | What it is |
|---|---|
| `seekparquet` | `OpenDataset` (a manifest dataset through a `Source`), `OpenParts` (advanced: parts given directly, with footer and row-group hooks), `Reader`, `Dataset`, `Range`, `List`, `Scan` with a `ColumnSink`, `Row`, `Value`, `Entry`, `Stats`, `Successor`, `CommonPrefix`, refusal codes and errors |
| `source/local` | `Dir`, a `Source` over a local directory confined by `os.Root`, and `FileCache`, which bounds the descriptors the parts hold open |
| `seekparquettest` | writes small spec-conforming datasets for tests, with knobs for invalid variants |
| `internal/cmd/conformance` | the adapter for the [conformance harness](../conformance/protocol.md) |

## Results

- Rows own their key and value bytes. `Row.Columns` is the result's own copy of the projected names.
- A projected column a part does not have is `Value.Absent`, which is distinct from a null value (COL-2). A
  column no part has is absent in every row. Projecting a nested or repeated column is an
  `*UnsupportedError` (`unsupported_projection`), never reported as absent.
- Values are parquet-go `Value`s of the column's physical type in the row's part. This module commits to
  parquet-go's value types rather than defining its own.
- A limit that is not positive is `ErrInvalidRequest`, not an empty result.

## What opening and reading check, and what they do not certify

Opening checks everything the specification's CHECK-1 requires: the manifest (strictly: duplicate member
names, undefined members, exact integers, canonical base64, part paths, bound order), that every part exists
with the manifest's size and row count, that its footer is readable, self-contained and unencrypted, its
`seekparquet.*` stamps, the `key` column (type, repetition, annotation, `column_orders`), that columns agree
across parts, and `sorting_columns` on every non-empty row group. As CHECK-3 allows, it also checks every
non-empty row group's key page index and decodes its first key page, and decodes each part's last key page,
so that row-group first keys must ascend and each part's first and last keys must equal the manifest's
bounds.

Every read checks each key page it decodes (CHECK-2): the page header matches its OffsetIndex entry, one
non-empty key per row, valid UTF-8 for a `STRING` key, strictly ascending, inside the page's bounds, and in
dataset order with every other page the request decoded. Page bounds are used as Parquet defines them, as
bounds whatever their length; overlapping bounds and `boundary_order` are never relied on.

What is **not** certified: rows a request does not decode (a query that succeeds says nothing about the pages
it skipped), values beyond decoding them, `sha256` digests, and that files are unchanged after opening (a
part replaced by another file is refused when it is reopened, not one rewritten in place). Full
certification is validation (spec §8), which this module does not yet provide.

## Errors

| Error | Meaning |
|---|---|
| `*RefusalError` (`CodeOf`) | the data breaks the specification; `Code` is from `spec/refusal-codes.json`, with the part, row group, row and key where known |
| `ErrInvalidRequest` | a malformed request |
| `*UnsupportedError` | valid data this reader does not handle: `unsupported_projection`, `unsupported_encoding`, `unsupported_encryption` |
| `*ExpectationError` | a part fails a caller's `ColumnExpectation`; a policy failure, not a refusal |
| anything else | the environment: storage, the context, `ErrClosed`, `ErrLimitExceeded` |

## Sources, contexts and ownership

A `Source` opens files of a dataset root by relative path; `local.Dir` is one. A part file that implements
`ContextBinder` has every read (manifest, footers, page indexes, pages) bound to the operation's context, so a
cancelled request or `Close` ends a read that waits on the source. A plain `io.ReaderAt` cannot be interrupted
mid-call; the reader checks the context between pages.

`OpenDataset` closes every file it opened when it fails; on success `Dataset.Close` closes them, after the
reads in progress end. `OpenParts` takes ownership of the files it is given in the same way. A `ColumnSink`
must not call `Close` from a callback.

## Cost model

Opening reads the manifest, every footer, and for every non-empty row group its key page index and first key
page, plus each part's last key page. A cold query loads the page index of each row group it touches (once,
then shared); a warm query decodes the key pages from the one that can hold its start, and value pages only
for the rows it returns. A value column without an OffsetIndex is decoded from the start of its chunk.
`Stats` counts decoding work only, not bytes read or opening costs.
