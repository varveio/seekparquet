# SeekParquet for Go

The Go module reads, writes and validates [SeekParquet](../README.md) datasets: sorted Parquet parts committed
by a `manifest.json`. It is the complete implementation: everything the [Java reader](../java/README.md) does,
plus writing, validation, row-position counting and aggregation.

| | |
|---|---|
| Module | `github.com/varveio/seekparquet/go` (v0: the API may change in any minor version) |
| Format | format 1, draft revision 1 (on disk `format_version` 0, `draft_revision` 1); every other version pair is refused with `unsupported_version` |
| Go | 1.26 or later |
| Dependencies | the standard library, `github.com/parquet-go/parquet-go` and the decompressors it uses |
| Conformance | the `reader` target and the four validator targets |

## Install

```sh
go get github.com/varveio/seekparquet/go@main
```

This README describes `main`. The tagged `go/v0.1.0` has an earlier API; until the next release, pin a
`main` commit as a pseudo-version ([compatibility](../docs/reference/compatibility.md#using-the-go-module-before-its-next-release),
[RELEASING.md](../RELEASING.md#go)).

## Read and list

Open a directory, read the first keys under a prefix, then page through a delimited listing with a cursor.

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

The same `Dataset` also serves lookups (`Get`), borrowed batch scans (`Scan`), planned reads with exact
counts and byte estimates (`Plan`, `Split`, `ScanParallel`) and an optional list index for wide listings
(`BuildListIndex`). The [reading guide](../docs/guides/reading.md) shows each one.

## Count and aggregate

Reduce named columns over key selections. Each result says whether it is exact.

<!-- snippet: go/example_generic_test.go#ExampleDataset_Aggregate -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
if err := d.Verify(ctx, nil); err != nil {
	log.Fatal(err)
}
results, err := d.Aggregate(ctx, []seekparquet.Selection{
	{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))},
	{Span: seekparquet.PrefixSpan([]byte("logs/2026/02/"))},
}, []seekparquet.Reduction{
	{Op: seekparquet.CountRows},
	{Op: seekparquet.Sum, Column: "size"},
	{Op: seekparquet.CountNulls, Column: "size"},
}, seekparquet.AggregateOptions{ValueBudgetBytes: 4 << 20}, nil)
if err != nil {
	log.Fatal(err)
}
for _, r := range results {
	if r[0].Status == seekparquet.AggregateExact && r[1].Status == seekparquet.AggregateExact {
		fmt.Printf("rows=%d size=%d nulls=%d\n", r[0].Value.Int64(), r[1].Value.Int64(), r[2].Value.Int64())
	}
}
// Output:
// rows=30 size=31543127898 nulls=0
// rows=40 size=43279389918 nulls=0
```
<!-- /snippet -->

For browse pages with per-prefix totals (`Page`), counts over listed row spans without storage reads
(`PlanRows`, `ScanValues`) and filters, see [counting and aggregating](../docs/guides/counting.md) and
[pagination](../docs/guides/pagination.md#include-complete-prefix-aggregates).

## Produce a dataset

`local.WriteDir` writes rows already in key order as a new local dataset; packages `write` and `local` let a
producer drive its own part rotation and commit.

```go
schema := parquet.SchemaOf(new(Object)) // a top-level REQUIRED []byte or string key column
err := local.WriteDir(ctx, "/data/listing", schema, sortedRows, local.DatasetOptions{
	Part: write.PartOptions{Key: seekparquet.Key{Column: "key", Unique: true}, SchemaID: "example/objects/v1"},
})
```

The [producing guide](../docs/guides/producing.md) has the runnable example, the recipe the writer enforces,
geometry choices, and a pyarrow recipe for producers in other languages.

## Check a dataset

Validate at the specification's four levels from the command line, from the repository root (a relative
`DIR` is relative to `go/`):

```sh
go -C go run ./cmd/seekparquet validate [--level structural|profile|full|digest] [--json] DIR
```

or from Go with `ValidateDir` (a local directory) or `Validate` (any `Source`). To check every part's
metadata and key page ends before serving reads, call `Dataset.Verify`. The
[validating guide](../docs/guides/validating.md) covers the levels, exit status and report.

To run the conformance suite against this module, from the repository root:

```sh
python3 conformance/harness/run.py --adapter "go -C go run ./internal/cmd/conformance"
```

The adapter claims the `reader` target and the four validator targets, so one run qualifies all five.

## Packages

| Package | What it is |
|---|---|
| `seekparquet` | open (`OpenDir`, `OpenDataset`), read, list, plan, aggregate, verify and validate; manifests, cursors, errors, limits and read statistics |
| `write` | `NewPart` and `PartWriter`: one part, streamed from sorted rows, enforcing the profile |
| `local` | `WriteDir`, `PublishPart` and `Commit`: durable publication in a local directory |
| `cmd/seekparquet` | the `seekparquet validate` command |
| `internal/...` | Thrift and manifest checkers, test-data writers, the sample datasets and the conformance adapter |

Every exported symbol is documented in the package documentation; `go doc` shows it.

## Where the detail is

| Topic | Page |
|---|---|
| the data model, what is checked when, the four kinds of error | [concepts](../docs/concepts.md) |
| ranges, prefixes, lookups, listings, projection, scans, plans, shards | [reading](../docs/guides/reading.md) |
| cursors, cursor tokens and service envelopes | [pagination](../docs/guides/pagination.md) |
| row positions, counts and aggregates | [counting](../docs/guides/counting.md) |
| limits, memory, open parts, verify, the list index, concurrency | [operating](../docs/guides/operating.md) |
| reading from an object store: `Source` and its optional capabilities | [storage Sources](../docs/guides/storage-sources.md) |
| what a read costs, read-ahead, tuning and measuring | [remote I/O](../docs/guides/remote-io.md) |
| writing and validating | [producing](../docs/guides/producing.md), [validating](../docs/guides/validating.md) |
| every error code and what to do | [troubleshooting](../docs/guides/troubleshooting.md) |
| defaults, counters, codes, Java names | [limits](../docs/reference/limits.md), [read statistics](../docs/reference/read-stats.md), [refusal codes](../docs/reference/refusal-codes.md), [parity](../docs/reference/parity.md) |
| what changed | [CHANGELOG.md](CHANGELOG.md) |
