# Five-minute quickstart

Read a real, checked-in SeekParquet dataset with the Go reader. You need Git and
**Go 1.26 or later**, plus network access for the initial Go dependency download.
No Python, Java, object-store account, or fixture generation is required.

## Run it

From a terminal (or skip the clone and start at the repository root if you already
have a checkout):

```sh
git clone https://github.com/varveio/seekparquet.git
cd seekparquet
go -C go run ./examples/quickstart ../conformance/datasets/multipart
```

`go -C go` runs inside the Go module, so the dataset path is relative to `go/`.
The command prints:

```text
preview: logs/2026/01/f000.gz
preview: logs/2026/01/f001.gz
page 1: prefix logs/2026/01/
page 2: prefix logs/2026/02/
page 3: prefix logs/2026/03/
```

## What just happened

The [complete program](../go/examples/quickstart/main.go) opens a local directory
with `OpenDir`, then:

1. Uses `Rows` over a `PrefixSpan` to preview the first two January keys. This is a limited
   preview, not every row under that prefix. No value columns are requested.
2. Uses `List` with prefix `logs/2026/`, delimiter `/`, and limit `1` to return
   one month per page rather than all the files below each month.
3. Assigns the last entry's `Cursor()` to the next request's `Cursor` and repeats
   until the result is empty. February spans two parts but appears only once.
   Keep the query and dataset unchanged while using a cursor. A prefix cursor
   resumes after the entire common prefix, not just its first row.

The [multipart fixture](../conformance/datasets/multipart/manifest.json) has 134
synthetic rows in three manifest-listed Parquet parts (about 21 KB of part bytes).
Its [source rows](../conformance/generators/rows/multipart.json) and
[layout](../conformance/datasets/multipart/STRUCTURE.txt) are checked in. This is
test data for learning and conformance, **not a production producer or a
representative performance benchmark**. No files are written by this example.

## Next steps and current limits

- Read [read costs and writer guidance](read-costs.md) before choosing part, row-group and page
  sizes. This tiny example demonstrates semantics, not cold remote performance.
- Read the [Go API and examples](../go/README.md) for ranges, projected values,
  repeated keys, limits, and refusal errors. Run the quickstart's regression test
  with `go -C go test ./examples/quickstart` from the repository root.
- Java users can start with the [Java build and usage guide](../java/README.md).
- To write your own dataset, follow [producing datasets](guides/producing.md); arbitrary sorted Parquet
  files alone do not satisfy the profile. To check one, see [validating](guides/validating.md).
- This example reads local files. To read from other storage, implement the reader's source interface,
  which must return the bytes of one object version for as long as a file is open. See
  [Go sources and ownership](../go/README.md#sources-contexts-and-ownership).
