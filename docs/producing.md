# Producing SeekParquet datasets

This page is for code that writes datasets. The format is defined by the
[specification](../spec/format-1.md); this page is the recipe the Go module implements and what it
promises. Readers and validators check the result; nothing here relaxes a rule of the specification.

## The recipe

1. **Sort the rows** by the key, in unsigned byte order (§1). Decide whether keys are unique. When
   they are not, equal keys are a group, kept in the order you give them (KEY-7).
2. **Cut the rows into parts** at key boundaries. A group of equal keys never crosses parts (PART-4),
   so a part may exceed its size target to keep a large group whole.
3. **Write each part** as an ordinary Parquet file with:
   - the key column as a top-level `REQUIRED BYTE_ARRAY`, unannotated or `STRING` (KEY-1);
   - `column_orders`, with `TYPE_ORDER` for the key (KEY-5);
   - `sorting_columns` on every row group, the key first and ascending (RG-1);
   - a ColumnIndex and an OffsetIndex on the key column of every row group (RG-2 to RG-4), with page
     bounds that are bounds: exact, or shortened only where the shortened value still bounds the page
     (RG-5). Value columns should have an OffsetIndex too (COL-4);
   - key pages of at most 1,024 rows (GEO-1, a recommendation);
   - the five `seekparquet.*` footer stamps, repeating the manifest's version, schema id and key
     declaration (STAMP-1).
4. **Make every part durable** under its final path: sync the file, move it into place, and sync its
   directory.
5. **Commit the manifest** (§3): the parts in key order with their exact row counts, sizes and first and
   last keys, optionally their SHA-256 digests and an `extensions` object. Write it whole, sync it, and
   install it atomically as `manifest.json`, then sync the root (COMMIT-1). Until then the root holds no
   dataset (LAYOUT-1).

## In Go

| Step | API |
|---|---|
| 3 | `write.NewPart`, `PartWriter.WriteRows`, `PartWriter.Seal` (or `Abort`) |
| 4 | `local.PublishPart` |
| 5 | `seekparquet.MarshalManifest`, `local.Commit` |
| 2 to 5, from sorted rows | `local.WriteDir` |
| checking a written part | `seekparquet.InspectFooter`, `seekparquet.InspectKeyPages` |

`write` enforces step 3 and cannot be told otherwise: its options set the key, the schema id, the codec
(ZSTD by default), page and row-group sizes, `created_by` and column-parallel encoding, but not the
indexes, bounds or stamps. It checks every key as it arrives (KEY-2, KEY-3) and fails with
`write.ErrProfile`, naming the rule, rather than write a part a reader would refuse. It streams: it holds
one page of rows and one row group of encoded pages, and computes the part's SHA-256 as the bytes are
written. Its page bounds are exact on every column, including keys longer than 1,024 bytes and keys that
begin with `0xFF` bytes, whose bounds a shortening writer can get wrong (RG-5).

`local` confines every path to the dataset root (an `os.Root`). `Commit` checks the manifest as a reader
would (`MarshalManifest`), checks every listed part's size on disk, and links the manifest into place,
so an existing manifest is never replaced, even by a concurrent writer. Its result says what an error
left behind: `Unpublished` (no dataset), `Visible` (the dataset is there, but not proven to survive a
power loss) or `Durable`.

Windows has no directory sync in Go, so a rename or link there cannot be made durable. `local` refuses
to publish on Windows unless the caller sets `AllowUnsynced` and accepts `Visible` commits.

## What `WriteDir` does not do

`WriteDir` writes sorted rows into a new directory and commits them. It does not sort, deduplicate or
merge rows; write to an object store; replace, extend or delete an existing dataset (COMMIT-3); or retry.
A producer that needs any of these, or its own part rotation and recovery, drives `write` and `local`
itself, as swath does.

## Checking the output

Open the dataset with `seekparquet.OpenDir` and `Options.VerifyOnOpen` for the open-time checks. Full
validation (§8) is a separate tool. The conformance job validates the module's sample datasets at the
`full` and `digest` levels with the reference adapter and with the Python validator oracle
(`conformance/validator/`), and reads them with DuckDB
(`conformance/harness/check_datasets.py`, fed by `go -C go run ./internal/cmd/samples DIR`).
