# SeekParquet for Java

A reader for [SeekParquet](../README.md) datasets: it answers range reads, prefix reads, lookups and delimited
listings with projected columns, plans and counts selections, and checks every part on demand. The
conformance suite holds it and the [Go module](../go/README.md) to the same results; it lacks some Go
features, listed in [what Java does not have](#what-java-does-not-have).

| | |
|---|---|
| Coordinates | `io.varve:seekparquet`, package `io.varve.seekparquet`; not published to any repository yet |
| Format | format 1, draft revision 1 (on disk `format_version` 0, `draft_revision` 1); every other version pair is refused with `unsupported_version` |
| JDK | 21 or later |
| Parquet library | Apache parquet-java's Hadoop-free modules (`parquet-format-structures`, `parquet-column`, `parquet-common`) and pure-Java decompression; [QUALIFICATION.md](QUALIFICATION.md) explains the choice and lists the dependency tree |
| Conformance | the `reader` target, with the optional `verify` and `planning` capabilities |

## Build

```sh
cd java
./gradlew build                          # compile, Javadoc, unit tests
./gradlew :seekparquet:publishToMavenLocal   # install io.varve:seekparquet into ~/.m2 for your build
```

The build uses the checked-in Gradle wrapper and a Java 21 toolchain. The public API is in
`io.varve.seekparquet`, storage interfaces in `io.varve.seekparquet.storage`; everything else is
package-private. The Javadoc documents every public type.

## Read and list

Open a directory, read the first keys under a prefix, then page through a delimited listing with a cursor.

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

Here `dir` is the conformance fixture `conformance/datasets/multipart` and `out` a `PrintStream`; the
snippet is a tested example, and it prints what the [Go example](../go/README.md#read-and-list) prints.

A request names its keys with a `Span` (`Span.all()`, `Span.prefix(p)`, `Span.key(k)` for one key group, or
`Span.builder()` for start, exclusive start and end), and adds a result limit, projected columns and an
optional cursor in a `Query`. `Query.next(row)` and `ListQuery.next(entry)` build the next page's request;
`cursor.encode()` and `Cursor.parse(token)` carry a cursor as a [cursor token](../spec/cursor-token-1.md)
that the Go reader reads too. Values are sealed `Value` records of the physical type, with `Value.Absent`
(the row's part has no such column) and `Value.Null` kept apart.

| Operation | What it returns |
|---|---|
| `rows(query)` | an owning `List<Row>` of at most the query's limit (1 to `Integer.MAX_VALUE`) |
| `get(key, columns)` | every row of one key group; more than `Limits.getRows()` rows fails with `LimitExceededException` (`GET_ROWS`), so page a larger group with `rows` over `Span.key(key)` |
| `scan(query, visitor)` | the same rows as borrowed `Batch`es with primitive accessors; a limit of 0 reads to the end of the span |
| `list(listQuery)` | sealed `ListEntry.RowEntry` and `ListEntry.CommonPrefix` entries; a common prefix carries its exact row ([LIST-8](../spec/format-1.md#list-8)) |
| `columns(part)` | one part's top-level columns, with logical types |

Every operation also takes `ReadOptions`: a `Cancellation` and a consumer of the operation's `ReadStats`.

## Plan and count

Plan the rows of a set of selections, read the exact count when every part holding them is checked, and split
the plan for parallel scans on your own threads:

```java
Plan plan = dataset.plan(List.of(Selection.of(Span.prefix(Bytes.utf8("logs/2026/")))), List.of("size"));
Estimate rows = plan.count();            // rows.value(), rows.exact()
Estimate bytes = plan.projectedBytes();  // compressed bytes of the projected pages a scan reads
List<Plan> shards = plan.split(Runtime.getRuntime().availableProcessors());
```

Java has no aggregation API: sum or count values in `Plan.scan` or `Dataset.scan` yourself. See the
[reading guide](../docs/guides/reading.md#plan-and-admit-a-read) for what planning reads and when a count is
exact.

## Check

`dataset.verify()` checks every part now ([CHECK-3](../spec/format-1.md#check-3)): footers, each row group's
key page index and first key page, and each part's last, against the manifest's bounds. Java does not
validate (§8); use the Go `seekparquet validate` command, described in [validating](../docs/guides/validating.md).

To run the conformance suite against this reader, from the repository root:

```sh
(cd java && ./gradlew :conformance:installDist)
python3 conformance/harness/run.py \
    --adapter java/conformance/build/install/seekparquet-conformance/bin/seekparquet-conformance
```

The adapter claims the `reader` target and the `verify` and `planning` capabilities; the harness skips the
validator targets. It assembles one protocol response from bounded library requests of at most 1,000,000 rows
or entries each, so it accepts any positive query limit, though it buffers the whole JSON response. CI runs the
build, the unit tests, this suite, the Java reader over Go-written datasets, and the deterministic
indexed-access regressions (`conformance/harness/check_indexed_access.py --language java`) on changes under
`java/`, `spec/`, `conformance/` or `.github/`. Those regressions are repository evidence, not a normative
conformance target; [indexed-access evidence](../docs/dev/indexed-access.md) lists what they cover.

## What Java does not have

Every gap is a missing feature, not a difference in the format: the shared conformance suite, and CI's run of
the Java reader over Go-written datasets, hold both readers to the same results for valid files. Where a row
below says so, Java reports an unsupported feature or a limit for a file Go reads.

| Go capability | Java |
|---|---|
| Writing: `write`, `local`, `MarshalManifest` | absent; Java reads only. `Manifest.parse` parses a manifest |
| Validation at the four levels (`Validate`, `ValidateDir`, `seekparquet validate`) | absent |
| Footer and key-page inspection (`InspectFooter`, `InspectKeyPages`) | absent |
| Row positions: `ListEntry.Rows`, `RowPosition`, `RowSpan`, `PlanRows`, `InputRowSpans`, `SelectRows`, `ScanValues`, `ValueBatch` | absent |
| Aggregation: `Aggregate`, `AggregateRows`, `Reduction`, `BatchFilter` | absent |
| Browse pages with per-prefix aggregates: `Page` | absent |
| Queued parallel scan: `Plan.ScanParallel` | absent; scan `Plan.split` shards on your own threads |
| Adaptive listing read-ahead (zero `ReadAhead` over a non-local Source), prediction across row groups, read-ahead of key page indexes, gap reads | absent; `ListQuery.Builder.readAhead(n)` is a fixed count, default 0 (off), within one row group ([remote I/O](../docs/guides/remote-io.md#the-java-readers-access-path)) |
| Forward routing of listing seeks by read-ahead key page indexes | absent; every seek is routed by first keys |
| Listing row-group shortcut (a whole row group emitted as one common prefix) | absent; `ReadStats.groupsSkipped` is always 0 |
| Batched storage reads (`RangeReader`), fetch windows, `ReadPolicy`, kept raw page index bytes (`Limits.PageIndexCacheMemory`) | absent; one `PartSource.read` per range, a 64 KiB tail read |
| Read statistics `SourceBatches`, `ScalarFallbackReads`, `MetadataHitBytes`, `RequestMemoHitBytes` | absent; the other counters are present, and `readAheadReads` counts key pages only |
| The `BROTLI` codec | reports `unsupported_encoding`, which [ENC-1](../spec/format-1.md#enc-1) allows for a codec outside the baseline |
| Go's page-memory estimate | Java estimates decoding memory its own way, with the same default limits, so a page can exceed Java's `pageMemory` within Go's ([operating](../docs/guides/operating.md#account-for-per-operation-memory)) |

Present in both: range and prefix reads, `get`, `scan`, `list` with exact rows, projection with absent
distinct from null, `INT96` as raw bits, `BYTE_STREAM_SPLIT`, cursor tokens, `plan`, `split`,
`verify`, `columns(part)` with logical types, `buildListIndex`, listing read-ahead (fixed), `ReadStats`,
`Limits` with Go's defaults, structured refusal locations, and the four kinds of error. Both readers lack LZO,
the legacy `LZ4` codec, modular encryption and projection of nested or repeated columns, and both report them
as unsupported.

## How the Java API differs

The [parity table](../docs/reference/parity.md) maps every Go name to its Java name. Beyond the names:

- Immutable `Bytes`, records and collections where Go has byte slices and structs; sealed alternatives for
  `Cursor`, `ListEntry` and `Value`; `Optional` instead of nil; builders for `Span`, `Query`, `ListQuery` and
  `Limits`; `Estimate(value, exact)` for Go's `(n, exact)`.
- Failures are unchecked: the sealed `SeekParquetException` (`RefusalException`,
  `UnsupportedFeatureException`, `LimitExceededException`, `StorageException`); an invalid request is
  `IllegalArgumentException`, a closed dataset `IllegalStateException`, a cancelled request
  `CancellationException`. The storage interfaces throw checked `IOException`.
- Every operation takes `ReadOptions` where Go takes a context and a `*ReadStats`: Go `Dataset.Verify(ctx, st)`
  is Java `verify(ReadOptions)`. Cancellation is an explicit `Cancellation` plus thread interruption, not a
  context. Statistics go to a
  consumer as an immutable snapshot after the operation ends, not into a caller-owned struct.
- A batch visitor returns `false` to stop and may throw its own checked exception; `BatchColumn` getters throw
  on an absent column, a null row or another type.
- `Dataset.rowCount()` returns the dataset's row total; `Manifest.rowsExact()` returns it as a `BigInteger`.
- Planning positions each selection's ends with its own cursor, and a projected key column takes its values
  from the key pages, so read statistics and projected-byte estimates can differ from Go's for the same plan.
  Rows, order and cursors do not.

## Where the detail is

| Topic | Page |
|---|---|
| the data model, what is checked when, the four kinds of error | [concepts](../docs/concepts.md) |
| reads, scans, plans and columns, with Java notes | [reading](../docs/guides/reading.md) |
| cursors and tokens | [pagination](../docs/guides/pagination.md) |
| limits, memory, open parts, the list index, threads and cancellation | [operating](../docs/guides/operating.md) |
| implementing `DatasetStorage` and `PartSource`; `LocalStorage` per platform | [storage](../docs/guides/storage-sources.md#implement-java-storage) |
| what a read costs, read-ahead | [remote I/O](../docs/guides/remote-io.md#the-java-readers-access-path) |
| every error code and what to do | [troubleshooting](../docs/guides/troubleshooting.md) |
| defaults, counters, codes, Go and Java names | [limits](../docs/reference/limits.md), [read statistics](../docs/reference/read-stats.md), [refusal codes](../docs/reference/refusal-codes.md), [parity](../docs/reference/parity.md) |
| benchmarks | [benchmark recipes](../docs/dev/benchmarks.md#list-index) |
| what changed | [CHANGELOG.md](CHANGELOG.md) |
