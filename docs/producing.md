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
| 3 | `write.NewPart`, `PartWriter.WriteRows` or `PartWriter.WritePage`, `PartWriter.Seal` (or `Abort`) |
| 4 | `local.PublishPart` |
| 5 | `seekparquet.MarshalManifest`, `local.Commit` |
| 2 to 5, from sorted rows | `local.WriteDir` |
| checking a written part | `seekparquet.InspectFooter`, `seekparquet.InspectKeyPages` |

`write` enforces step 3 and cannot be told otherwise: its options set the key, the schema id, the codec
(ZSTD by default; a column whose schema node names its own compression keeps it), page and row-group sizes, `created_by` and column-parallel encoding, but not the
indexes, bounds or stamps. It checks every key as it arrives (KEY-2, KEY-3, KEY-6) and fails with
`write.ErrProfile`, naming the rule, rather than write a part a reader would refuse. It streams: it holds
one page of rows and one row group of encoded pages, and computes the part's SHA-256 as the bytes are
written. `WriteRows` copies each row's byte values, so the caller may reuse its rows at once.
`WritePage` copies nothing: the caller fills each column of a page from its own rows, which must stay
unchanged until the call returns; it checks only the keys (KEY-2, KEY-3), leaving KEY-6 to the caller
and to validation, and encodes as fast as a writer built for one schema. Its page bounds are exact on every column, including keys longer than 1,024 bytes and keys that
begin with `0xFF` bytes, whose bounds a shortening writer can get wrong (RG-5).

`local` confines every path to the dataset root (an `os.Root`) and follows swath's protocol. `WriteDir`
creates the dataset root, which must not exist, and syncs its parent; it creates `data/` and syncs the
root, before any part is published. `PublishPart` syncs a part, renames it into place and syncs its
directory. `Commit` checks the manifest as a reader would (`MarshalManifest`), checks every listed part's
size on disk, writes the manifest to a temporary file, syncs it, renames it to `manifest.json` and syncs
the root. The rename replaces a manifest already there: one writer owns a root, and `WriteDir` keeps
datasets immutable by writing only into a directory it creates. `Commit` reports whether the manifest was
installed; with an error, that means the root's last sync failed and the dataset is visible but not
proven durable, so the caller must not remove it.

Durability is what the file system guarantees for synced files and directories: on tmpfs, a synced
dataset does not survive a reboot, and network file systems vary. Windows has no directory sync in Go,
so a rename there cannot be made durable: `local` refuses to publish on Windows unless the caller sets
`AllowUnsynced` and accepts datasets that are visible but not proven durable.

## What `WriteDir` does not do

`WriteDir` writes sorted rows into a new directory and commits them. It does not sort, deduplicate or
merge rows; write to an object store; replace, extend or delete an existing dataset (COMMIT-3); or retry.
A producer that needs any of these, or its own part rotation and recovery, drives `write` and `local`
itself, as swath does.

## Writing a dataset with pyarrow

pyarrow can write a valid part when it is told to. This is a recipe, checked with pyarrow 26.0.1 and
`seekparquet validate --level full`, not a supported writer: nothing here checks keys as they are written,
and a future pyarrow may change a default it relies on. The settings that matter:

- the key is a non-nullable `binary` (or `string`) field, so Parquet writes it `REQUIRED` (KEY-1);
- `write_page_index=True` writes the ColumnIndex and OffsetIndex (RG-2);
- `sorting_columns=[pq.SortingColumn(i)]`, with `i` the key's column index, sets RG-1;
- the five `seekparquet.*` stamps go in the Arrow schema metadata, which pyarrow copies into the footer
  with its default `store_schema=True`. With `store_schema=False` it drops them, and validation reports
  `stamp_mismatch`;
- `data_page_size=1, write_batch_size=1024` ends a page every 1,024 rows (GEO-1).

The rows must already be sorted by key in unsigned byte order, which is how Python compares `bytes`; pyarrow
does not check it. The manifest is written by hand after the part. One part:

```python
import base64, json, os
import pyarrow as pa
import pyarrow.parquet as pq

root = "example-dataset"
schema_id = "example/objects/v1"
keys = sorted(f"logs/2026/{i:05d}.json".encode() for i in range(5000))  # bytes sort unsigned
stamps = {
    "seekparquet.format_version": "0",
    "seekparquet.draft_revision": "1",
    "seekparquet.schema_id": schema_id,
    "seekparquet.key_column": "key",
    "seekparquet.key_unique": "true",
}
schema = pa.schema(
    [pa.field("key", pa.binary(), nullable=False), pa.field("size", pa.int64())],
    metadata=stamps,
)
table = pa.table({"key": keys, "size": [len(k) * 100 for k in keys]}, schema=schema)

part = "data/part-00000.parquet"
os.makedirs(os.path.join(root, "data"))
pq.write_table(
    table,
    os.path.join(root, part),
    write_page_index=True,
    sorting_columns=[pq.SortingColumn(0)],  # column 0 is the key
    data_page_size=1,  # with write_batch_size, ends a page every 1,024 rows
    write_batch_size=1024,
)

manifest = {
    "format": "seekparquet",
    "format_version": 0,
    "draft_revision": 1,
    "schema_id": schema_id,
    "key": {"column": "key", "unique": True},
    "parts": [{
        "path": part,
        "rows": table.num_rows,
        "bytes": os.path.getsize(os.path.join(root, part)),
        "min_key_base64": base64.b64encode(keys[0]).decode(),
        "max_key_base64": base64.b64encode(keys[-1]).decode(),
    }],
}
with open(os.path.join(root, "manifest.json"), "w") as f:
    json.dump(manifest, f)
```

Then check it:

```
go -C go run ./cmd/seekparquet validate --level full /path/to/example-dataset
```

The example does not sync the part or install the manifest atomically; a producer does both (steps 4
and 5). DuckDB 2.0's `COPY ... TO` cannot produce a valid part today: its key column is `OPTIONAL`, and it
writes no page index and no `sorting_columns`, so validation reports `invalid_key_column`.

## Checking the output

Open the dataset with `seekparquet.OpenDir` and call `Dataset.Verify` to check every part now, and
validate it in full with `seekparquet.ValidateDir` (§8); the module's own tests do both for every dataset
they write. The conformance job validates the module's sample datasets at the
`full` and `digest` levels with the reference adapter and with the Python validator oracle
(`conformance/validator/`), and reads them with DuckDB
(`conformance/harness/check_datasets.py`, fed by `go -C go run ./internal/cmd/samples DIR`). The same
datasets are queried through the Go and the Java readers (`check_datasets.py --target reader`): ranges,
bounds, prefixes and delimiter listings projecting every column, paged through their cursors, against the
source rows as PyArrow reads them.
