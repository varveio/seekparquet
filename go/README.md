# SeekParquet for Go

The Go module reads, writes and validates [SeekParquet](../README.md) datasets: sorted Parquet parts committed
by a `manifest.json`. It is the complete implementation: everything the [Java reader](../java/README.md) does,
plus writing, validation, counting by row position and folds.

| | |
|---|---|
| Module | `github.com/varveio/seekparquet/go` (v0: the API may change in any minor version) |
| Format | format 1, draft revision 1 (on disk `format_version` 0, `draft_revision` 1); every other version pair is refused with `unsupported_version` |
| Go | 1.26 or later |
| Dependencies | the standard library, `github.com/parquet-go/parquet-go` and the decompressors it uses |
| Conformance | the `reader` target and the four validator targets |

## Install

```sh
go get github.com/varveio/seekparquet/go@v0.3.0
```

This README describes `main`, which may be ahead of the latest release; the [changelog](CHANGELOG.md) lists
the changes ([compatibility](../docs/reference/compatibility.md#installing-the-go-module),
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

## Count and fold

Count the rows under each common prefix of a listing. A count reads no values: the listing's row spans and
the parts' checked row counts give it.

<!-- snippet: go/example_docs_test.go#Example_countPrefixes -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()

// This fixture has three entries. Let List traverse beyond each prefix
// so that it establishes both row boundaries.
entries, err := d.List(ctx, seekparquet.ListQuery{
	Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: 4,
}, nil)
if err != nil {
	log.Fatal(err)
}
var counts seekparquet.ReadStats
for _, entry := range entries {
	_, startKnown := entry.Rows.Start.Ordinal()
	_, endKnown := entry.Rows.End.Ordinal()
	if !startKnown || !endKnown {
		log.Fatal("use a key-range plan for an unknown boundary")
	}
	p, err := d.PlanRows(ctx, []seekparquet.RowSpan{entry.Rows}, nil, &counts)
	if err != nil {
		log.Fatal(err)
	}
	n, exact := p.Count()
	fmt.Printf("%s: %d rows, exact=%t, reads=%d\n", entry.Bytes(), n, exact, counts.SourceReads)
}
// Output:
// logs/2026/01/: 30 rows, exact=true, reads=0
// logs/2026/02/: 40 rows, exact=true, reads=0
// logs/2026/03/: 30 rows, exact=true, reads=0
```
<!-- /snippet -->

To reduce column values, give `FoldRows` the row spans and a `Fold` of your own; it folds each span
separately and reads the columns the fold names. The [counting guide](../docs/guides/counting.md) shows
both, with budgets and key checks, and [pagination](../docs/guides/pagination.md#count-and-fold-the-prefixes-of-a-page)
the prefixes of a page.

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
| `seekparquet` | open (`OpenDir`, `OpenDataset`), read, list, plan, count, fold, verify and validate; manifests, cursors, errors, limits and read statistics |
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
| row positions, counts and folds | [counting](../docs/guides/counting.md) |
| limits, memory, open parts, verify, the list index, concurrency | [operating](../docs/guides/operating.md) |
| reading from an object store: `Source` and its optional capabilities | [storage Sources](../docs/guides/storage-sources.md) |
| what a read costs, read-ahead, tuning and measuring | [remote I/O](../docs/guides/remote-io.md) |
| writing and validating | [producing](../docs/guides/producing.md), [validating](../docs/guides/validating.md) |
| every error code and what to do | [troubleshooting](../docs/guides/troubleshooting.md) |
| defaults, counters, codes, Java names | [limits](../docs/reference/limits.md), [read statistics](../docs/reference/read-stats.md), [refusal codes](../docs/reference/refusal-codes.md), [parity](../docs/reference/parity.md) |
| what changed | [CHANGELOG.md](CHANGELOG.md) |
