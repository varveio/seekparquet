# SeekParquet

**A sorted-key profile for Apache Parquet.**

> **Status: draft.** The specification is at draft revision 1 and the Go and Java readers are pre-release. Files
> written to the draft are stamped so they can never be mistaken for the final format. Expect changes.

A SeekParquet dataset is a set of ordinary Parquet files sorted by one byte-string key column that the
manifest names, split into key-disjoint parts, and committed by a `manifest.json` that lists the parts in key
order with their exact key bounds. Every part also carries a Parquet page index on its key column.

Any Parquet reader (DuckDB, Spark, pyarrow, Trino) reads the files as an ordinary table. A reader that knows
the profile can also:

- find the first row at or after a key, read a key range, or read a prefix;
- list a prefix with a delimiter, S3 style, rolling keys up into common prefixes and skipping the pages of
  keys under them;
- page through results and resume exactly where a page ended;

using the manifest, part footers and page indexes, with no query engine and no catalog. Selective reads
can touch only a few data pages, but having an index alone does not guarantee that cost: opening checks
every part, and coarse key bounds can require much more decoding. See [read costs and writer guidance](docs/read-costs.md).

## Try it in five minutes

Follow the [runnable quickstart](docs/quickstart.md) to read a checked-in sample,
list prefixes with a delimiter, and resume a listing with a cursor. Requires Go 1.26 or later.

## What it guarantees that plain Parquet does not

| Parquet offers | SeekParquet requires |
|---|---|
| `sorting_columns`, which a file may declare without being sorted | one key column, in unsigned byte order across the whole dataset: strictly ascending when the manifest declares keys unique, otherwise in groups of equal keys that never cross parts |
| an optional page index | a ColumnIndex and OffsetIndex on the key column of every row group |
| nothing across files | key-disjoint parts, in order, committed by a manifest with exact bounds, row counts and sizes |
| no query semantics | range, prefix and delimited-listing semantics, pagination, and named refusal codes when a dataset does not meet the profile |

The [related-work page](docs/related-work.md) compares SeekParquet with engines that store sorted Parquet
internally, external indexes over Parquet, and table formats.

## Repository

| Path | What it holds |
|---|---|
| [`spec/`](spec/) | The specification ([format-1.md](spec/format-1.md)), the manifest JSON Schema, the refusal codes, examples and proposals |
| [`conformance/`](conformance/) | The language-neutral conformance suite: fixtures, cases, the runner protocol and harness |
| [`go/`](go/) | The Go reader, module `github.com/varveio/seekparquet/go` |
| [`java/`](java/) | The Java reader, package `io.varve.seekparquet` |
| [`tools/seekparquet/`](tools/seekparquet/) | Local Python producer and independent dataset validator |
| [`docs/`](docs/) | Related work and guides |

## Implementations

| Language | Status | Supports |
|---|---|---|
| [Go](go/README.md) | pre-release | format 1 draft revision 1: open, range, list, projection |
| [Java](java/README.md) | pre-release; not yet published | format 1 draft revision 1: open, range, list, projection |

The Go and Java implementations provide reading APIs. The [Python tooling](tools/seekparquet/README.md)
creates new local datasets from sorted Arrow tables and [validates stored datasets](tools/seekparquet/VALIDATOR.md)
at the structural, profile, full and digest levels (§8). The tools remain pre-release and local-only.
The shared conformance suite checks reader semantics; it does not yet certify indexed-access efficiency.
See the [Go usage examples](go/example_test.go) and [Java usage and build instructions](java/README.md)
to get started, and the [Java qualification report](java/QUALIFICATION.md) for its Parquet library choice
and limitations. Go requires Go 1.26 or later; Java requires JDK 21 or later.

Implementations in other languages are welcome; please open an issue first.

## Used by

[swath](https://github.com/varveio/swath), a parallel object-store and filesystem lister, writes its listings
as sorted Parquet datasets. SeekParquet grew out of that format, and swath is moving to write SeekParquet.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Specification changes go through [proposals](spec/proposals/).
Security reports: [SECURITY.md](SECURITY.md). Contact: oss@varve.io.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
