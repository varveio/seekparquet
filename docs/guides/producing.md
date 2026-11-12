# Producing datasets

This guide is for code that writes SeekParquet datasets. The [specification](../../spec/format-1.md) defines
what is valid; this guide is the recipe, the Go API that implements it, and the choices that decide how fast
the result reads. Nothing here relaxes a rule of the specification. The terms are defined in
[concepts](../concepts.md).

Writing is Go only: the Java module reads datasets and does not write them. You can also write a valid
dataset with pyarrow, below.

## The recipe

1. **Sort the rows** by the key, in unsigned byte order. Decide whether keys are unique. When they are not,
   equal keys form a key group, kept in the order you give them ([KEY-7](../../spec/format-1.md#key-7)).
2. **Cut the rows into parts** at key boundaries. A key group never crosses parts
   ([PART-4](../../spec/format-1.md#part-4)), so a part may exceed its size target to keep a large group whole.
3. **Write each part** as an ordinary Parquet file with:
   - the key column as a top-level `REQUIRED BYTE_ARRAY`, unannotated or `STRING`
     ([KEY-1](../../spec/format-1.md#key-1));
   - `column_orders`, with `TYPE_ORDER` for the key ([KEY-5](../../spec/format-1.md#key-5));
   - `sorting_columns` on every non-empty row group, the key first and ascending ([RG-1](../../spec/format-1.md#rg-1));
   - a ColumnIndex and an OffsetIndex on the key column of every non-empty row group
     ([RG-2](../../spec/format-1.md#rg-2) to [RG-4](../../spec/format-1.md#rg-4)), with page bounds that are
     bounds: exact, or shortened only where the shortened value still bounds the page
     ([RG-5](../../spec/format-1.md#rg-5));
   - an OffsetIndex on value columns too, recommended ([COL-4](../../spec/format-1.md#col-4));
   - key pages of at most 1,024 rows, recommended ([GEO-1](../../spec/format-1.md#geo-1));
   - baseline codecs, recommended: `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `ZSTD` or `LZ4_RAW`, which is not the legacy
     `LZ4` ([ENC-1](../../spec/format-1.md#enc-1)). `BROTLI` is valid, but a reader may not support it;
   - the five `seekparquet.*` footer stamps, repeating the manifest's version, schema id and key declaration
     ([STAMP-1](../../spec/format-1.md#stamp-1)).
4. **Make every part complete and durable** under its final path before publishing the manifest
   ([COMMIT-1](../../spec/format-1.md#commit-1)). Keep those bytes immutable while the manifest may be read
   ([COMMIT-2](../../spec/format-1.md#commit-2)).
5. **Commit the manifest atomically** ([§3](../../spec/format-1.md#3-the-manifest)): the parts in key order
   with their exact row counts, sizes, and first and last keys. Optionally add SHA-256 digests, and
   `extensions` objects for metadata readers ignore ([§3.4](../../spec/format-1.md#34-extensions); the
   namespace `seekparquet` is reserved). Until the manifest is published the root holds no dataset
   ([LAYOUT-1](../../spec/format-1.md#layout-1)).

On a local filesystem, the implementation of COMMIT-1 is: make the directory hierarchy durable by syncing
its parent entries; sync each completed part, rename it into place, and sync its directory. Write the whole
manifest to a temporary file, sync it, rename it atomically to `manifest.json`, then sync the root. Object
storage needs equivalent durability and atomic publication guarantees from its storage service.

## Write a dataset from sorted rows

`local.WriteDir` does steps 2 to 5 for a new local directory. You supply the rows in key order, as
parquet-go rows of your schema.

**Go**

<!-- snippet: go/local/example_test.go#ExampleWriteDir -->
```go
parent, err := os.MkdirTemp("", "seekparquet-example-")
if err != nil {
	log.Fatal(err)
}
defer os.RemoveAll(parent)
dir := filepath.Join(parent, "objects") // WriteDir creates it

schema := parquet.SchemaOf(new(object))
objects := []object{ // in unsigned byte order of the key
	{Key: []byte("logs/a.gz"), Size: 10},
	{Key: []byte("logs/b.gz"), Size: 20},
	{Key: []byte("photos/cat.jpg"), Size: 30},
}
var rows []parquet.Row
for i := range objects {
	rows = append(rows, schema.Deconstruct(nil, &objects[i]))
}
reader := parquet.RowReaderFunc(func(buf []parquet.Row) (int, error) {
	n := copy(buf, rows)
	rows = rows[n:]
	if len(rows) == 0 {
		return n, io.EOF
	}
	return n, nil
})

err = local.WriteDir(context.Background(), dir, schema, reader, local.DatasetOptions{
	Part: write.PartOptions{
		Key:      seekparquet.Key{Column: "key", Unique: true},
		SchemaID: "example/objects/v1",
	},
})
if err != nil {
	log.Fatal(err)
}

report, err := seekparquet.ValidateDir(context.Background(), dir, seekparquet.ValidateOptions{})
if err != nil {
	log.Fatal(err)
}
fmt.Println("validation:", report.Result())
// Output:
// validation: complete
```
<!-- /snippet -->

Here `object` is a struct with a `[]byte` field tagged `parquet:"key"` and an `int64` field tagged
`parquet:"size"`, as in the next example.

**Java:** not available. The Java module reads datasets only.

`WriteDir` creates the dataset directory, which must not exist, and `data/`. It syncs the parent and root
directories, making the root's entry in its parent and `data/`'s entry in the root
durable before publishing any part. It names parts `data/part-NNNNN.parquet`, records each part's SHA-256 in
the manifest, and checks the encoded part size after each input batch. Once it reaches
`DatasetOptions.PartBytes`, the next key change ends the part. With no rows it
commits an empty dataset. When it fails before the commit, including on cancellation, it removes the
directory.

`WriteDir` does not:

- sort, deduplicate or merge rows;
- write to an object store;
- replace, extend or delete an existing dataset ([COMMIT-3](../../spec/format-1.md#commit-3));
- retry.

A producer that needs any of these, or its own part boundaries and recovery, drives `write` and `local`
itself.

## Write parts and commit them yourself

**Go**

<!-- snippet: go/write/example_test.go#ExampleNewPart -->
```go
dir, err := os.MkdirTemp("", "seekparquet-example-")
if err != nil {
	log.Fatal(err)
}
defer os.RemoveAll(dir)
root, err := os.OpenRoot(dir)
if err != nil {
	log.Fatal(err)
}
defer root.Close()
if err := root.Mkdir("data", 0o755); err != nil {
	log.Fatal(err)
}
// Make the root's entry in its parent, and data's entry in the root,
// durable before publishing a part (the local implementation of COMMIT-1).
for _, path := range []string{filepath.Dir(dir), dir} {
	d, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	if err := errors.Join(d.Sync(), d.Close()); err != nil {
		log.Fatal(err)
	}
}
ops := local.Ops{Root: root}

type object struct {
	Key  []byte `parquet:"key"`
	Size int64  `parquet:"size"`
}
schema := parquet.SchemaOf(new(object))
key := seekparquet.Key{Column: "key", Unique: true}

// 1. Write one part to a temporary name. The writer checks every key
// as it arrives and fails with write.ErrProfile rather than write a
// part a reader would refuse.
f, err := root.Create("data/part-00000.parquet.tmp")
if err != nil {
	log.Fatal(err)
}
w, err := write.NewPart(f, schema, write.PartOptions{Key: key, SchemaID: "example/objects/v1"})
if err != nil {
	log.Fatal(err)
}
for i, k := range []string{"logs/a.gz", "logs/b.gz", "photos/cat.jpg"} {
	row := object{Key: []byte(k), Size: int64(10 * (i + 1))}
	if _, err := w.WriteRows([]parquet.Row{schema.Deconstruct(nil, &row)}); err != nil {
		log.Fatal(err)
	}
}
part, err := w.Seal() // rows, size, exact first and last keys, SHA-256
if err != nil {
	log.Fatal(err)
}

// 2. Publish it under its final name: sync, rename, sync the directory.
part.Path = "data/part-00000.parquet"
if err := local.PublishPart(ops, "data/part-00000.parquet.tmp", part.Path); err != nil {
	log.Fatal(err)
}

// 3. Commit the manifest atomically. Until then the root holds no dataset.
m := seekparquet.Manifest{
	Key:      key,
	Version:  seekparquet.Version{FormatVersion: seekparquet.FormatVersion, DraftRevision: seekparquet.DraftRevision},
	SchemaID: "example/objects/v1",
	Parts:    []seekparquet.PartInfo{part},
}
if _, err := local.Commit(ops, m); err != nil {
	log.Fatal(err)
}
fmt.Printf("%s: %d rows, %s to %s\n", part.Path, part.Rows, part.MinKey, part.MaxKey)
// Output:
// data/part-00000.parquet: 3 rows, logs/a.gz to photos/cat.jpg
```
<!-- /snippet -->

**Java:** not available.

| Step | Go API |
|---|---|
| 3 | `write.NewPart`, then `PartWriter.WriteRows` or `PartWriter.WritePage`, then `PartWriter.Seal` (or `Abort`) |
| 4 | `local.PublishPart` |
| 5 | `seekparquet.MarshalManifest` (bytes only) or `local.Commit` (local directory) |
| 2 to 5 | `local.WriteDir` |
| inspect a written part | `seekparquet.InspectFooter`, `seekparquet.InspectKeyPages` |

### What `write` guarantees

The writer installs the required profile metadata and exact page bounds. `WriteRows` checks
[KEY-2](../../spec/format-1.md#key-2), [KEY-3](../../spec/format-1.md#key-3) and
[KEY-6](../../spec/format-1.md#key-6) as rows arrive. `WritePage` checks KEY-2 and KEY-3; its caller must
supply valid UTF-8 where required and schema-correct column values. A checked key violation fails with
`write.ErrProfile`.

- Options set the key, schema id, codec, page and row-group sizes, `created_by` and column-parallel
  encoding. They cannot turn off the indexes, bounds or stamps.
- Page bounds are exact on every column, including keys longer than 1,024 bytes and keys beginning with
  `0xFF` bytes.
- The writer holds one page of rows and one row group of encoded pages, and computes the part's SHA-256
  as bytes are written.
- `WriteRows` copies each row's byte values, so you may reuse the rows after the call.
- `WritePage` avoids buffering a copy of every input row, but retains copies of boundary keys. Its inputs
  must stay unchanged until the call returns.

The codec defaults to ZSTD. A column whose parquet-go schema node names its own compression keeps it.

### What `local` guarantees

- Every path is confined to the dataset root, an `os.Root`.
- `PublishPart` syncs a part, renames it into place and syncs its directory.
- `Commit` checks the manifest as a reader would, checks every listed part's size on disk, writes the
  manifest to a temporary file, syncs it, renames it to `manifest.json` and syncs the root.
- The rename replaces a manifest already there. One writer owns a root; `WriteDir` keeps datasets immutable
  by writing only into a directory it creates.
- `Commit` reports whether the manifest was installed. When it returns an error with `installed` true, the
  root's last sync failed: the dataset is visible but not proven durable, and you must not remove it.

Durability is what the file system guarantees for synced files and directories. On tmpfs, a synced dataset
does not survive a reboot, and network file systems vary. Go cannot sync a directory on Windows, so `local`
refuses to publish there unless you set `AllowUnsynced` and accept datasets that are visible but not proven
durable.

## Choose geometry

Geometry does not change the query semantics. Within the reader's limits, it affects how much a read
decodes and how many dependent requests it waits for. Defaults of the Go writer are in parentheses.

| Choice | Smaller | Larger |
|---|---|---|
| Rows per key page (1,024, [GEO-1](../../spec/format-1.md#geo-1)) | fewer keys decoded to find one; a larger page index | more keys decoded per seek |
| Row group (8 MiB or 1,000,000 rows) | more row-group first keys to route by; a larger footer | a larger key page index to load per row group |
| Part (`PartBytes`, 128 MiB) | more first touches, each a footer read, and more open files | larger footers to read on first touch |
| Page bounds | exact bounds distinguish neighboring pages and help a seek skip unrelated pages; routing still decodes first key pages | bounds shortened until they cannot tell neighboring pages apart can force decoding many pages, up to a whole row group |
| Value-column OffsetIndex ([COL-4](../../spec/format-1.md#col-4)) | present: a read fetches only the value pages of the rows it returns | absent: the reader walks page headers to find them |

- Keys that share a long prefix need bounds long enough to tell pages apart. For example, if keys share
  `logs/customer-0001/2026/` and differ only after it, bounds cut to eight bytes cannot distinguish pages.
  Smaller pages do not repair indistinguishable bounds. Inspect actual ColumnIndex bounds, including long shared prefixes and arbitrary byte keys.
  Shorten a maximum only when it still bounds the page; otherwise retain it in full
  ([RG-5](../../spec/format-1.md#rg-5), [RG-6](../../spec/format-1.md#rg-6)).
  Conservative overlapping or nonmonotone bounds remain valid; a selective read can decode a whole row group.
- A single large key group can exceed the part target: key groups never cross parts
  ([PART-4](../../spec/format-1.md#part-4)).
- Measure page index placement and storage coalescing together; combining nearby logical reads can fetch extra bytes.
- More parts add verification and touched-handle work; they do not make `OpenDataset` open parts
  ([`OpenDataset`](https://pkg.go.dev/github.com/varveio/seekparquet/go#OpenDataset)).
- Test the combination on your own keys and storage, including sparse delimited listings with projected
  columns and deep pages of repeated keys, not just key-only ranges.

[Remote I/O](remote-io.md) describes the phases of a read to measure.

## Write a dataset with pyarrow

pyarrow writes a valid part when it is told to. This recipe runs in CI with pyarrow 26.0.1, followed by
`seekparquet validate --level full` ([source](../examples/pyarrow_writer.py)). It is a recipe, not a supported
writer: nothing checks keys as they are written, and a future pyarrow may change a default it relies on.

The settings that matter:

- the key is a non-nullable `binary` (or `string`) field, so Parquet writes it `REQUIRED`
  ([KEY-1](../../spec/format-1.md#key-1));
- `write_page_index=True` writes the ColumnIndex and OffsetIndex ([RG-2](../../spec/format-1.md#rg-2));
- `sorting_columns=[pq.SortingColumn(i)]`, with `i` the key's column index, sets
  [RG-1](../../spec/format-1.md#rg-1);
- the five `seekparquet.*` stamps go in the Arrow schema metadata, which pyarrow copies into the footer with
  its default `store_schema=True`. With `store_schema=False` it drops them, and validation reports
  `stamp_mismatch`;
- `data_page_size=1, write_batch_size=1024` ends a page every 1,024 rows.

The rows must already be sorted by key in unsigned byte order, which is how Python compares `bytes`. pyarrow
does not check it. You write the manifest after the part.

**Python**

<!-- snippet: docs/examples/pyarrow_writer.py#recipe -->
```python
import base64, json, os, sys
import pyarrow as pa
import pyarrow.parquet as pq

root = sys.argv[1]  # the dataset root to create
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
<!-- /snippet -->

The recipe does not sync the part or install the manifest atomically; a producer does both (steps 4 and 5).

DuckDB's `COPY ... TO` (checked with DuckDB 1.4.5) writes the key column `OPTIONAL`, so validation reports
`invalid_key_column` even when the stamps are added with `KV_METADATA`.

## Check the output

- Open the dataset and call Go `Dataset.Verify(ctx, stats)` (Java `Dataset.verify`) to check every part now;
  pass a `*ReadStats` for Go statistics, or `nil` to discard them.
- Validate it in full: see [validating](validating.md).

The writer tests include verification and validation checks. In CI, the module's sample datasets are
validated at the `full` and `digest` levels by the Go validator and by an independent Python validator, read
by DuckDB, and queried through the Go and Java readers against their source rows
(`conformance/harness/check_datasets.py`).
