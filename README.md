# SeekParquet

**A sorted-key profile for Apache Parquet.**

<!-- generated: status -->
> **Pre-release.** Format 1 is a draft: draft revision 1, stored on disk as `format_version` 0 so that a draft file is never mistaken for a final one. The conformance suite is at revision `0.1.0-draft.5`. The Go module and the Java reader have no release of this API yet: this documentation describes `main`, which may change before the first release. See [compatibility](docs/reference/compatibility.md) for what is stable.
<!-- /generated -->

A SeekParquet dataset is a set of ordinary Parquet files sorted by one byte-string key column that the
manifest names, split into key-disjoint parts, and committed by a `manifest.json` that lists the parts in key
order with their exact key bounds. Every part also carries a Parquet page index on its key column.

Any Parquet reader (DuckDB, Spark, pyarrow, Trino) reads the files as an ordinary table. A reader that knows
the profile can also, with no query engine and no catalog:

- find the first row at or after a key, read a key range, or read a prefix;
- list a prefix with a delimiter, S3 style, rolling keys up into common prefixes and skipping the pages of
  keys under them;
- page through results and resume exactly where a page ended.

Selective reads use manifest bounds, part footers and page indexes to avoid unrelated data. The pages and
requests needed depend on the query, file geometry, bounds and projected columns; broad reads can scan the
dataset. The storage interface supports local disk and object storage.

## What it looks like

Read the first three keys under a prefix, then list the prefix's subfolders two per page. The dataset is a
fixture of the conformance suite.

**Go**

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

**Java**

<!-- snippet: java/seekparquet/src/test/java/io/varve/seekparquet/docs/DocSnippetsTest.java#readme -->
```java
try (Dataset dataset = Dataset.open(dir)) {
    // The first three keys under a prefix.
    Query query = Query.builder(Span.prefix(Bytes.utf8("logs/2026/01/"))).limit(3).build();
    for (Row row : dataset.rows(query)) {
        out.println("row " + row.key().toStringUtf8());
    }

    // Common prefixes under logs/2026/, two per page, resuming from a cursor.
    ListQuery list = ListQuery.builder((byte) '/').prefix(Bytes.utf8("logs/2026/")).limit(2).build();
    for (int page = 1; ; page++) {
        List<ListEntry> entries = dataset.list(list);
        if (entries.isEmpty()) {
            break;
        }
        for (ListEntry e : entries) {
            out.println("page " + page + ": " + e.bytes().toStringUtf8());
        }
        list = list.next(entries.getLast());
    }
}
```
<!-- /snippet -->

Both print the same lines. To run the Go version yourself, follow the [five-minute quickstart](docs/quickstart.md).

## What it guarantees that plain Parquet does not

| Parquet offers | SeekParquet requires |
|---|---|
| `sorting_columns`, which a file may declare without being sorted | one key column, in unsigned byte order across the whole dataset: strictly ascending when the manifest declares keys unique, otherwise in key groups of equal keys that never cross parts |
| an optional page index | a ColumnIndex and OffsetIndex on the key column of every non-empty row group |
| nothing across files | key-disjoint parts, in order, committed by a manifest with exact bounds, row counts and sizes |
| no query semantics | range, prefix and delimited-listing semantics, pagination, and named refusal codes when a dataset does not meet the profile |

These are format requirements, not checks all performed on open. Readers check the manifest on open, parts
on first touch and key pages as they decode them; see [what is checked, and when](docs/concepts.md#what-is-checked-and-when).

## When not to use it

SeekParquet is for immutable, listing-shaped snapshots: point-in-time inventories, and derived or
cross-store listings that carry extra columns. It is not for updating or deleting in place, and it is neither
a query engine nor a table format. Composite keys need a producer-side, order-preserving encoding into the
one key column.

- For a live listing of a store, use the store's own LIST.
- For analytics and broad scans, plain sorted Parquet in your engine may be enough.
- For local lookups with frequent updates, use SQLite or a key-value store.
- For rich predicates and evolving tables, use Iceberg, Delta or an external Parquet index.

[Related work](docs/related-work.md) compares SeekParquet with engines that store sorted Parquet internally,
external indexes over Parquet, and table formats.

## Read next

| You want to | Read |
|---|---|
| understand the model | [concepts](docs/concepts.md), [related work](docs/related-work.md) |
| read a dataset | [quickstart](docs/quickstart.md), then the [Go](go/README.md) or [Java](java/README.md) README |
| write a dataset | [producing](docs/guides/producing.md), [validating](docs/guides/validating.md) |
| fix an error | [troubleshooting](docs/guides/troubleshooting.md) |
| implement SeekParquet in another language | [specification](spec/format-1.md), [implementer's guide](spec/IMPLEMENTING.md), [conformance suite](conformance/README.md) |
| know what is stable | [compatibility](docs/reference/compatibility.md) |

Every page is listed in the [documentation index](docs/README.md).

## Implementations

| | Go | Java |
|---|---|---|
| Module | `github.com/varveio/seekparquet/go` | `io.varve:seekparquet`, built from source |
| Requires | Go 1.26 or later | JDK 21 or later |
| Reads: ranges, prefixes, lookups, delimited listings, projection, verify | yes | yes |
| Writes datasets (`write`, `local`) | yes | no |
| Validates at the four levels of §8 (`seekparquet validate`) | yes | no |
| Passes the conformance suite | reader and all four validator targets | reader target |
| Parquet library | parquet-go | Apache parquet-java ([qualification](java/QUALIFICATION.md)) |

Go and Java names for the same concept are in the [parity table](docs/reference/parity.md). Implementations in
other languages are welcome; please open an issue first.

## Repository

| Path | What it holds |
|---|---|
| [`spec/`](spec/) | the specification, the manifest JSON Schema, the refusal codes, examples and proposals |
| [`conformance/`](conformance/) | the language-neutral conformance suite: fixtures, cases, the adapter protocol and harness |
| [`go/`](go/) | the Go reader, writer and validator |
| [`java/`](java/) | the Java reader |
| [`docs/`](docs/) | concepts, guides and reference |

## Used by

[swath](https://github.com/varveio/swath), a parallel object-store and filesystem lister, writes its listings
as sorted Parquet datasets. SeekParquet grew out of that format, and swath is moving to write SeekParquet.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Specification changes go through [proposals](spec/proposals/).
Security reports: [SECURITY.md](SECURITY.md). Contact: oss@varve.io.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
