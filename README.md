# SeekParquet

**A sorted-key profile for Apache Parquet.**

<!-- generated: status -->
> **Pre-release.** Format 1 is a draft: draft revision 1, stored on disk as `format_version` 0 so that a draft file is never mistaken for a final one. The conformance suite is at revision `0.1.0-draft.5`. The Go module has version 0 releases ([changelog](go/CHANGELOG.md)); the Java reader has none. This documentation describes `main`, which may be ahead of the latest release. See [compatibility](docs/reference/compatibility.md) for what is stable.
<!-- /generated -->

A SeekParquet dataset is a set of ordinary Parquet files sorted by one byte-string key column, split into
key-disjoint parts, and committed by a `manifest.json` that lists the parts in key order with their exact key
bounds. Every part carries a Parquet page index on its key column.

Any Parquet engine (DuckDB, Spark, pyarrow, Trino) reads the files as a table. A reader that knows the profile
also, with no query engine or catalog, looks up a key, reads a range or a prefix, lists a prefix with a
delimiter S3 style, counts and sums by prefix, and resumes any of these from a cursor. It reads the manifest,
the footers and page indexes of the parts it touches, and the pages that can hold the rows; broad reads can
still scan the dataset. A local directory implementation ships; an object store is an implementation you write.

## Read, list and count

The main use. Open a dataset, read the first keys under a prefix, then list its subfolders two per page. The
dataset is a fixture of the conformance suite.

<!-- snippet: go/example_docs_test.go#Example_readme -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()

// The first three keys under a prefix.
q := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))}, Limit: 3}
rows, err := d.Rows(ctx, q, nil)
if err != nil {
	log.Fatal(err)
}
for _, row := range rows {
	fmt.Printf("row %s\n", row.Key)
}

// Common prefixes under logs/2026/, two per page, resuming from a cursor.
list := seekparquet.ListQuery{Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: 2}
for page := 1; ; page++ {
	entries, err := d.List(ctx, list, nil)
	if err != nil {
		log.Fatal(err)
	}
	if len(entries) == 0 {
		break
	}
	for _, e := range entries {
		fmt.Printf("page %d: %s\n", page, e.Bytes())
	}
	list.Cursor = entries[len(entries)-1].Cursor()
}
// Output:
// row logs/2026/01/f000.gz
// row logs/2026/01/f001.gz
// row logs/2026/01/f002.gz
// page 1: logs/2026/01/
// page 1: logs/2026/02/
// page 2: logs/2026/03/
```
<!-- /snippet -->

- Run it in five minutes: [quickstart](docs/quickstart.md).
- Go: [README](go/README.md) and the [reading guide](docs/guides/reading.md). For counts and sums by prefix,
  [counting](docs/guides/counting.md).
- Java: the [Java README](java/README.md) has the same example and states what the Java reader lacks.
- From an object store: [storage](docs/guides/storage-sources.md) and [remote I/O](docs/guides/remote-io.md).

## Produce a dataset

Sort rows by key, write parts with the profile's metadata, then publish the manifest last. Go's `write` and
`local` packages enforce the profile; other languages follow the recipe, for example with pyarrow.

- [Producing datasets](docs/guides/producing.md): the recipe, the Go writer, geometry and a pyarrow recipe.

## Check a dataset or an implementation

Validate a dataset at the specification's four levels with the Go command, from the repository root (`-C go`
runs inside the Go module, so the path is relative to `go/`):

```sh
go -C go run ./cmd/seekparquet validate ../conformance/datasets/multipart
```

Qualify an implementation with the language-neutral conformance suite and its adapter protocol:

```sh
python3 conformance/harness/run.py --adapter "go -C go run ./internal/cmd/conformance"
```

- [Validating datasets](docs/guides/validating.md): levels, exit status, the report.
- [Conformance suite](conformance/README.md): targets, fixtures, writing an adapter.
- [Troubleshooting](docs/guides/troubleshooting.md): every refusal, unsupported and limit code.

## What it adds to Parquet

| Parquet offers | SeekParquet requires |
|---|---|
| `sorting_columns`, which a file may declare without being sorted | one key column, in unsigned byte order across the whole dataset: strictly ascending when the manifest declares keys unique, otherwise in key groups of equal keys that never cross parts |
| an optional page index | a ColumnIndex and OffsetIndex on the key column of every non-empty row group |
| nothing across files | key-disjoint parts, in order, committed by a manifest with exact bounds, row counts and sizes |
| no query semantics | range, prefix and delimited-listing semantics, pagination, and named refusal codes when a dataset does not meet the profile |

SeekParquet is for immutable, listing-shaped snapshots: inventories, and derived or cross-store listings that
carry extra columns. It does not update or delete in place, and it is neither a query engine nor a table
format. [Workload fit](docs/background/workload-fit.md) says when to use something else, and
[related work](docs/related-work.md) compares it with alternatives.

## Specification and implementations

- [Specification](spec/format-1.md), the only normative text, with the [implementer's guide](spec/IMPLEMENTING.md)
  and [proposals](spec/proposals/).
- [Concepts](docs/concepts.md): the model in plain terms, and every page in the [documentation index](docs/README.md).

| | Go | Java |
|---|---|---|
| Module | `github.com/varveio/seekparquet/go` | `io.varve:seekparquet`, built from source |
| Requires | Go 1.26 or later | JDK 21 or later |
| Reads, lists, plans, verifies | yes | yes |
| Counts and aggregates by row position, browse pages | yes | no |
| Writes and validates | yes | no |
| Conformance | `reader` and the four validator targets | `reader` |

[swath](https://github.com/varveio/swath), a parallel object-store and filesystem lister, writes its listings
as sorted Parquet; SeekParquet grew out of that format.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md); specification changes go through [proposals](spec/proposals/).
Security reports: [SECURITY.md](SECURITY.md). Contact: oss@varve.io. Licensed under the Apache License 2.0
([LICENSE](LICENSE), [NOTICE](NOTICE)).
