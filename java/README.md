# SeekParquet for Java

A reader for [SeekParquet](../spec/format-1.md), a sorted-key profile for Apache Parquet. Given a
dataset (a `manifest.json` and the Parquet parts it lists), it answers range reads, prefix reads and
delimited listings with projected columns. To do so it reads the manifest, the footer of each part a
request touches, a page index and a few pages, with no query engine and no catalog.

- **Format:** format 1, draft revision 1 (written on disk as `format_version` 0, `draft_revision` 1).
- **JDK:** 21 or later.
- **Coordinates:** `io.varve:seekparquet` (not yet published), package `io.varve.seekparquet`.
- **Parquet library:** Apache parquet-java 1.18.1 (`parquet-format-structures` and
  `parquet-column`), plus `parquet-common`, whose `InputFile` appears only in the interoperability adapter
  `storage.ParquetJavaInterop`, without Hadoop. See [QUALIFICATION.md](QUALIFICATION.md) for why, and for the
  full dependency tree (about 6.7 MB, no native code).
- **Status:** passes every case of the shared conformance suite's `reader` target. It does not
  implement validation (§8); see [What it does not do](#what-it-does-not-do).

## Usage

```java
import io.varve.seekparquet.*;
import java.nio.file.Path;

try (Dataset dataset = Dataset.open(Path.of("/data/listing"))) {
    // A range: keys in [photos/2024/, photos/2025/), at most 1000, with two columns.
    Span span = Span.builder().start(Bytes.utf8("photos/2024/")).end(Bytes.utf8("photos/2025/")).build();
    List<Row> rows = dataset.rows(Query.builder(span).limit(1000).columns("size", "etag").build());
    for (Row row : rows) {
        String size = switch (row.values().get(0)) {
            case Value.Int64 v -> Long.toString(v.value());
            case Value.Null n -> "null";
            case Value.Absent a -> "absent";      // this row's part has no "size" column (COL-2)
            default -> "other";
        };
        System.out.println(row.key().toStringUtf8() + " " + size);
    }

    // A prefix read.
    List<Row> logs = dataset.rows(Query.builder(Span.prefix(Bytes.utf8("logs/"))).limit(100).build());

    // A delimited listing, page by page (LIST-6).
    ListQuery query = ListQuery.builder((byte) '/').prefix(Bytes.utf8("photos/")).limit(100).build();
    List<ListEntry> page = dataset.list(query);
    while (true) {
        for (ListEntry entry : page) {
            switch (entry) {
                case ListEntry.CommonPrefix p -> System.out.println("dir  " + p.prefix().toStringUtf8());
                case ListEntry.RowEntry r -> System.out.println("file " + r.row().key().toStringUtf8());
            }
        }
        if (page.size() < query.limit()) {
            break;
        }
        query = query.next(page.getLast());
        page = dataset.list(query);
    }
}
```

A request names its keys with a `Span` (prefix, start, exclusive start, end; `Span.all()`, `Span.prefix(p)`,
`Span.key(k)` for exactly `[k, k + 0x00)`), optionally resumes after a row with a `Selection` cursor, and adds
a limit and a projection in a `Query`. The reads over a query are:

- `rows(query)`: a collected page of at most `limit` rows, which must be between 1 (QUERY-1) and
  `Integer.MAX_VALUE` (a `List`'s capacity). The caller's limit is the only bound on the result; on an error it
  returns no rows.
- `scan(query, visitor)`: the same rows as borrowed batches ([Batch scans](#batch-scans)). A limit of 0, the
  default, reads to the end of the span: a streaming extension of this library, not a query of the
  specification. No result limit applies.
- `get(key, columns)`: every row of one group of equal keys, none when the key is absent. A group of more than
  `Limits.getRows()` rows (1,000 by default) fails with `LimitExceededException` naming `GET_ROWS`, with no rows
  and before any value page is read: page such a group with `rows` over `Span.key(key)`.

Every operation also takes `ReadOptions`: a `Cancellation` and a statistics consumer ([Read
statistics](#read-statistics)). The overloads without it use `ReadOptions.DEFAULT`.

Keys, bounds and binary values are `Bytes`: immutable byte strings compared in unsigned
lexicographic order. They need not be UTF-8; `Bytes.utf8(text)` rejects text that is not well-formed
UTF-16 rather than changing the key, and `toStringUtf8()` is a lossy decoding for display. Values are physical Parquet values (`Bool`, `Int32`,
`Int64`, `Int96`, `Float32`, `Float64`, `ByteArray`, `FixedLenByteArray`), with `Null` and `Absent` kept
apart; an `INT96` is its raw bits, three little-endian 32-bit words least significant first, not interpreted as a
timestamp: absent (the row's part has no such column), null (a stored null) and a value are three disjoint states. Floating-point values carry their exact bits. The manifest's `key` declaration names the
key column; projecting that exact name returns the key's bytes, including literal dots in its name.

When `manifest().key().unique()` is false, equal keys form a group in file order. Groups can cross
pages and row groups, but never parts. Each `Row` includes its zero-based `offset()` within its
group (zero in unique mode). Cursors preserve that position:

```java
Query query = Query.builder(Span.key(Bytes.utf8("photos/item"))).limit(100).columns("size").build();
List<Row> page = dataset.rows(query);
if (!page.isEmpty()) {
    List<Row> nextPage = dataset.rows(query.next(page.getLast()));
}
// The whole group at once, within Limits.getRows():
List<Row> group = dataset.get(Bytes.utf8("photos/item"), List.of("size"));
```

`Query.next(Row)` and `ListQuery.next(ListEntry)` keep the original bounds and set the entry's cursor, one of
the sealed `Cursor` alternatives `RowPosition(key, offset)` and `CommonPrefix(prefix)`; the builders also set and
clear a cursor (`cursor(c)`, `clearCursor()`). `Row.cursor()`, `ListEntry.cursor()` and `Batch.cursor(i)` make
them. A result limit can split a group. Resuming a row continues inside that group before later keys; resuming
a common prefix skips its subtree. Ranges accept only row cursors; listings accept either. A cursor names a
position, not a row: an offset past its group continues at the next key (`Long.MAX_VALUE` is after the whole
group).

`cursor.encode()` and `Cursor.parse(token)` carry a cursor as one line of text, version 1 of the
[cursor token](../spec/cursor-token-1.md) (`spc1.r.<key>.<offset>`, `spc1.p.<prefix>`, unpadded base64url and
canonical decimal), the Go reader's `Cursor.Encode` and `ParseCursor`: a token one reader makes, the other reads
as the same cursor (tested against the shared vectors). A malformed or non-canonical token, an unknown version
or a token longer than 1 MiB is `IllegalArgumentException`.

### Planning a selection

`Dataset.plan(selections, columns)` plans reading every row of a list of `Selection`s, each a span read whole,
from its start or after its row cursor (inside a group of equal keys too). A `Plan` holds the selected rows (rows
several selections select are one row), their `count()`, the `projectedBytes()` a scan of the projected columns
will request, each an `Estimate(value, exact)`, and its `columns()`; `scan(visitor)` then hands every row, with
its values, to a visitor as borrowed batches in dataset order.

```java
Plan plan = dataset.plan(List.of(
        Selection.of(Span.prefix(Bytes.utf8("photos/2024/"))),
        Selection.of(Span.builder().start(Bytes.utf8("logs/")).end(Bytes.utf8("logs0")).build())),
        List.of("size"));
Estimate rows = plan.count();               // rows.exact() when every part holding a row was checked
Estimate bytes = plan.projectedBytes();     // bytes.exact() when every projected chunk has an OffsetIndex
plan.scan(new ReadOptions(cancel, stats -> log(stats)), batch -> {
    for (int i = 0; i < batch.size(); i++) process(batch.row(i));
    return true;
});
```

Planning decodes the key pages at the two ends of each selection, as a range read would, and adds up row
counts from footers and OffsetIndex row positions; with columns it checks every part holding a selected
row and reads each projected chunk's OffsetIndex, but no data page. Without columns, a count across a part
no read has checked rests on the manifest's row count, and the count is not exact. `projectedBytes()` is,
per column chunk the rows span, the extents of the pages holding them and of the dictionary page (the bytes
before the first data page), each counted once; a scan reads each counted extent in one request and no
other byte of those chunks. A chunk without an OffsetIndex (COL-4) is omitted and the bytes are not exact:
incomplete accounting, not a bound. A plan is immutable, bound to its dataset, and its scan fails once the
dataset is closed.

`plan.split(n)` divides a plan into at most `n` shards (`n` below 1 is `IllegalArgumentException`) for a whole
scan on several cores: each shard is a `Plan` over consecutive rows of the plan, and the shards' scans, run on
the caller's threads (the library starts none), together yield the plan's rows, each once. A boundary is a
selected row that begins a row group (or a part no read has checked yet), never inside a group of equal keys:
with repeated keys, splitting decodes the two key pages around each boundary inside a part and moves a boundary
that would divide a group. Boundaries balance `projectedBytes()` when the plan has columns and exact bytes,
else the rows. The shards' counts and projected bytes add up to the plan's, each row group's bytes counted in
the one shard holding it; the first shard keeps the plan's start bounds and cursors, the last its end. The
reads of splitting go to the split's own statistics consumer.

```java
List<Plan> shards = plan.split(Runtime.getRuntime().availableProcessors());
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    for (Plan shard : shards) {
        executor.submit(() -> shard.scan(ReadOptions.of(cancel), batch -> consume(batch)));
    }
}
```

### Batch scans

`Dataset.scan(query, visitor)` and `Plan.scan(visitor)` hand rows to a `BatchVisitor` as borrowed `Batch`es,
read through primitive accessors without a `Row`, `Value` or `Bytes` object per row. `Dataset.scan` reads what
`rows` reads for the same query, a dictionary only for a page that uses it, plans nothing, and stops at the limit
without moving past it (or reads to the end of the span with limit 0). The visitor returns `true` to continue and
`false` to stop successfully; it may throw a checked exception of its own type (`BatchVisitor<E extends
Exception>`), which the scan propagates unchanged.

```java
dataset.scan(Query.builder(Span.prefix(Bytes.utf8("logs/"))).columns("size").build(), batch -> {
    BatchColumn size = batch.column(0);                 // projection slot 0, in request order
    for (int i = 0; i < batch.size(); i++) {
        ByteBuffer key = batch.key(i);                  // borrowed; copyKey(i) owns, or keyLength, compareKey
        if (!size.isPresent()) {                        // the batch's part has no such column (COL-2)
            absent(key);
        } else if (!size.isNull(i)) {
            total += size.getLong(i);                   // getBoolean, getInt, getFloat(Bits), getDouble(Bits)
        }
    }
    return total < budget;                              // false stops the scan, successfully
});
```

`Batch` has `size()`, `columns()`, `offset(i)` (the row's offset in its group of equal keys), `key(i)`
(borrowed, read-only `ByteBuffer`), `copyKey(i)` (owning `Bytes`), `keyLength(i)`, `copyKey(i, dst, offset)`,
`compareKey(i, bytes)`, `row(i)` and `cursor(i)` (owning), and `column(c)`; a `BatchColumn` has `isPresent()`,
`size()`, `type()` (the physical type in the batch's part), `isNull(i)`, a getter per physical type (`getInt96`
included; floating-point values also as their exact bits), and for byte arrays `byteLength(i)`, `bytes(i)` (borrowed `ByteBuffer`),
`copyBytes(i)` (owning `Bytes`) and `copyBytes(i, dst, offset)`, and `value(i)` (owning `Value`). A projected key
column is a `BYTE_ARRAY` slot holding the keys; duplicate projections are slots of their own. Reading another
type than the slot's, a null row or an absent slot throws `IllegalStateException`; an index outside the batch,
`IndexOutOfBoundsException`.

A batch is a positive run of consecutive rows that never crosses a row group, a key page, a page of any
projected column, or a gap between a plan's selections; batches may be smaller, and their size is not promised.
**A batch, its columns, and the buffers `key` and `bytes` return are borrowed: read-only and valid only until
the visit returns.** Using a batch or its column afterwards throws `IllegalStateException`; a buffer view kept
past it cannot be invalidated and may show bytes of a later page. `row(i)`, `cursor(i)`, `copyKey(i)`,
`BatchColumn.value(i)` and `BatchColumn.copyBytes(i)` make owning copies, which may be kept. The `ByteBuffer`
views and owning copies allocate; the typed accessors do not. A batch is not `AutoCloseable`.

An exception the visitor throws ends the request and propagates unchanged; batches already handed over stand,
as they do before a read error or refusal. Cancellation is checked as `rows` checks it, and also before every
visit. The request holds the dataset open while the visitor runs: `close()` from another thread cancels it and
waits for the visitor to return, and closing the dataset from inside the visitor fails with
`IllegalStateException`. Concurrent and nested requests have storage of their own. `rows` keeps its own row
path; the batch path returns the same rows (tested over every conformance dataset).

### Typed columns

`Dataset.columns(part)` returns one part's top-level columns, in schema order, from the footer the dataset
checked and holds for that part, checking the part first if no read has. Each `Column` has its physical type
and `FIXED_LEN_BYTE_ARRAY` length, its `LogicalType` (the `logical_type` annotation, or else the legacy
`converted_type` normalized into one), repetition, and whether it is the key. There is no dataset-wide schema
call: COL-5 holds physical declarations equal in every part that has a column, but a part may lack it and
logical annotations are not compared, so a caller that needs every part's columns asks for each part.

### Optional slash listing index

Call the explicit build before serving latency-sensitive wide-prefix listings:

```java
ListIndexStats build = dataset.buildListIndex(
        new ListIndexOptions(64L << 20, 2, 8), Cancellation.none());
// dataset.buildListIndex() uses 64 MiB, one worker, eight changes per page.
```

Opening remains lazy. The build reads key pages and keeps directory-run starts as suffixes relative
to a shared group anchor. Unchanged pages add nothing; fully decoded flat groups retain no index data.
Dense pages retain exact first-key markers and decline skipped intervals containing a marker.
Build-only page indexes and first keys are transient: they never consume the shared, non-evicting
`Limits.IndexMemory` cache (Java: `indexMemory`). Checked part footers remain shared. Ordinary routing probes and synthetic landings
use the normal retaining cache and its memory limit; successful transient index loads also count
in read statistics.

Complete groups are admitted in dataset order until the first over-budget group. Data refusals and unsupported codecs/encodings omit
the affected groups, including both sides of a bad boundary; ordinary reads still report those
conditions when encountered. Storage/I/O errors, resource limits and cancellation fail the attempt without
publication, so a caller may retry after resolving them. Publication is atomic alongside concurrent lists.

A nonempty index is reused by later calls. `ListIndexStats.options()` and `hasIndex()` report the **effective** options
and whether any groups are indexed. `OptionsMismatch` (Java: `optionsMismatch()`) explicitly flags
reuse with different normalized settings. To replace a nonempty index, reopen the dataset. An empty outcome is cached for
the most recent options, including its original duration, so repeating that build performs no I/O;
changing options replaces that empty outcome. This cache holds only one result, not a map of all calls.
Defaults are 64 MiB, one worker, and eight directory changes per page. A retained index does not
guarantee every listing benefits: if the default cap marks every page dense, all skips may decline.
Check the skip/decline counters before choosing a larger cap or budget.

Both implementations charge the same conservative formula: 256 bytes for the snapshot, and per
retained group 256 bytes plus each of the base-key length, suffix-data length and 40 × run count
rounded up to a power of two (zero stays zero). This includes compact containers, dense counts and
allocator slack. Empty outcomes retain only a fixed build-status record, not boundary arrays.
The retained byte budget is **not a peak build-memory limit**: allow roughly Workers × 2 × MaxBytes
for candidate buffers and compaction, plus the retained index, one page's boundary expansion per
worker, transient page indexes, decoding buffers and part footers under the ordinary reader limits.
Oversized single keys/pages can exceed that estimate; flat groups must still finish checking.
Sequential build order checking retains only the preceding page's boundary, not every decoded page.

Only `/` uses the index. Slash-ended prefix cursors may start at certified anchors; other valid cursors
use ordinary seeking. Sorted anchors are searched by binary search for the prefix successor, with
cumulative dense counts certifying the skipped interval. Exact first keys route over whole groups.
Cancellation is checked during searches and at listing completion, including ordinary listings.
Logical position, exact rows, projected values and cursors remain unchanged. Java synthetic group
transitions clear the previous group's value pages, dictionaries and sequential readers.
Build statistics report groups, pages, entries, retained bytes, budget exhaustion and duration;
skipped counts cover visited groups, not unopened parts. Go cancellation reports decoded work,
including the last batch, but zero retained groups/entries/bytes.
Read statistics report answered skips and delimiter, missing-group, dense-page and unsafe-run declines.

`ListIndexBenchmarkTest` reuses the six-million-key datasets generated by the
[Go benchmark](../go/README.md#cost-model), keeping geometry identical:

```sh
SEEKPARQUET_LIST_BENCH_DIR=/path/to/bench ./gradlew :seekparquet:test --tests '*ListIndexBenchmarkTest' --rerun-tasks
```

Measurements appear in the test XML's standard output, including plain/indexed latency and decoded
pages, build time, retained bytes and entries. Without the environment variable the benchmark is skipped.

### Read statistics

Every operation (`rows`, `scan`, `get`, `list`, `plan`, `Plan.scan`, `Plan.split`, `verify`, `columns(part)`)
takes `ReadOptions(cancellation, statistics)`, and its statistics consumer receives one immutable `ReadStats`
exactly once, when the operation ends, whether it succeeds or fails (zero counts when it fails before reading),
after the operation has released its lease on the dataset: parts opened, key and value pages decoded, keys
decoded, row-group key indexes taken and loaded, page index seeks, prefix skips, and the storage reads and bytes
requested, and for a listing with [read-ahead](#key-page-read-ahead-for-listings) the key pages it read ahead
(`readAheadReads`, also counted in `sourceReads` and `sourceBytes`) and those it did not use
(`readAheadUnused`, `readAheadUnusedBytes`). A consumer that throws while the operation fails is added to the
operation's exception as suppressed, never replacing it. `ReadStats.plus` sums two, as Go's `ReadStats.Add`
does: a plan's statistics are those of planning; each scan and split reports its own. They describe this
reader's access path, not the format.

### Key page read-ahead for listings

A listing resumes after each common prefix at the prefix's successor, which lands on one more key page: over an
object store, a listing of 100 common prefixes waits for about 100 round trips, one after another.
`ListQuery.Builder.readAhead(n)` (default 0, off; at most `ListQuery.MAX_READ_AHEAD`, 64) lets a listing predict,
from the checked key page index of the row group it is in, the key pages it will decode next, and read up to `n`
of them concurrently, each on a virtual thread, holding at most 1 MiB per slot of pages read and not yet used (a
listing with `readAhead(64)` can hold 64 MiB) and at most `4 * n` pages.

```java
ListQuery query = ListQuery.builder((byte) '/').prefix(Bytes.utf8("crawl-data/")).limit(1000).readAhead(8).build();
```

Pages read ahead are checked exactly as pages read when decoded are (CHECK-2): the entries, errors and refusals
are the same with or without read-ahead, and `query.next(last)` keeps it. A page predicted wrongly (shortened or
overlapping page bounds, RG-5, RG-6) is read and transferred for nothing and held until the listing passes it or
ends, counted in `ReadStats.readAheadUnused` and `readAheadUnusedBytes`. A read ahead that fails, or whose storage
read throws anything, is not used: the page is read again when the listing reaches it. The reads ahead run under
a cancellation linked to the request's, so cancelling it or closing the dataset ends them, and the listing waits
for every one before it returns, including when it fails: none outlives the request. A cancellation, interrupt or
`close()` that comes while it waits fails the listing with `CancellationException`. A page's bytes are released
as soon as it is retired: when the listing takes it, or drops it once passed (a passed page can stay pending,
and counted in the budget, while a page predicted further ahead is still being read). The prediction stays within
the row group the listing is in; `rows`, `get` and the scans do not read ahead. With a list index built, a `/` listing
reads nothing ahead, including in a row group the index declines; other delimiters read ahead as usual.

As an illustration only (one run on one machine, printed by `ReadAheadTest` and not asserted): with a simulated
10 ms wait per read, a cold listing of 60 folders of 400 keys (100-row key pages) took about 640 ms without
read-ahead, 345 ms with `readAhead(2)`, 115 ms with `readAhead(8)` and 52 ms with `readAhead(32)`, for the same
reads. What the tests do assert is the overlap itself: with reads held at a gate, `readAhead(n)` has exactly `n`
reads ahead in flight before any is released, and never more. On a local file it saves nothing and costs a virtual thread per page read ahead:
leave it off there.

### Parity with the Go reader

Java exposes corresponding reader operations and request/result concepts, with matching configurable limit
defaults. Java representation differences and the capability, resource-accounting and execution differences
listed below remain. The correspondence, Go (package `seekparquet`) to Java (package `io.varve.seekparquet`):

| Go | Java |
|---|---|
| `OpenDataset(ctx, src, OpenOptions)`, `OpenDir(ctx, path, OpenOptions)` | `Dataset.open(storage, OpenOptions, Cancellation)`, `Dataset.open(path[, OpenOptions, Cancellation])` |
| `OpenOptions{Limits}` | `OpenOptions(limits, verifyConcurrency)` |
| `Limits{...}`, zero for a default, negative invalid | `Limits.builder()...build()`, `toBuilder()`; 0 for a default, negative `IllegalArgumentException` |
| `Span`, `PrefixSpan(p)`, `KeySpan(k)`, `Span{}` | `Span`, `Span.prefix(p)`, `Span.key(k)`, `Span.all()`, `Span.builder()` |
| `Selection{Span, Cursor}` | `Selection(span, Optional<Cursor.RowPosition>)`, `Selection.of`, `Selection.after` |
| `Query{Selection, Limit int64, Columns}` | `Query.builder(span).limit(long).columns(...).cursor(c).build()`, `next(row)` |
| `Dataset.Rows(ctx, q, st)` | `Dataset.rows(query, ReadOptions)` |
| `Dataset.Scan(ctx, q, st, fn)`, `Stop` | `Dataset.scan(query, ReadOptions, BatchVisitor)`, visitor returns `false` |
| `Dataset.Get(ctx, key, st, columns...)` | `Dataset.get(key, columns, ReadOptions)` |
| `Dataset.Plan(ctx, selections, columns, st)` | `Dataset.plan(selections, columns, ReadOptions)` |
| `Plan.Count()`, `ProjectedBytes()` `(n, exact)` | `Plan.count()`, `projectedBytes()`: `Estimate(value, exact)` |
| `Plan.Columns()`, `Plan.Scan(ctx, st, fn)`, `Plan.Split(ctx, n, st)` | `Plan.columns()`, `Plan.scan(ReadOptions, visitor)`, `Plan.split(n, ReadOptions)` |
| `Dataset.List(ctx, ListQuery, st)` | `Dataset.list(ListQuery, ReadOptions)` |
| `ListQuery{Prefix, Delimiter, End, Cursor, Limit, Columns, ReadAhead}` | immutable `ListQuery`, `builder(delimiter)`, `toBuilder()`, same accessors |
| `ListEntry{CommonPrefix, Row, Exact}`, `IsPrefix`, `Bytes`, `Cursor` | sealed `ListEntry.RowEntry` / `ListEntry.CommonPrefix(prefix, exact)`, `bytes()`, `cursor()` |
| `Row{Key, Offset, Columns, Values}`, `Value(name)`, `Cursor()` | `Row` record, `value(name)` (`Optional`), `cursor()` |
| `Cursor{Key, Offset, Prefix}`, `RowCursor`, `PrefixCursor` | sealed `Cursor.RowPosition(key, offset)`, `Cursor.CommonPrefix(prefix)` |
| `Cursor.Encode()`, `ParseCursor(token)`, `MaxCursorToken` | `Cursor.encode()`, `Cursor.parse(token)`, `Cursor.MAX_TOKEN_LENGTH` |
| `Batch.Len, Key, Offset, Column, Columns, Row, Cursor` | `Batch.size, key, offset, column, columns, row, cursor`; plus `copyKey`, `keyLength`, `compareKey` |
| `ColumnView.Present, Kind, Len, IsNull, Value, Boolean ... Double, Int96, Bytes` | `BatchColumn.isPresent, type, size, isNull, value, getBoolean ... getDouble(Bits), getInt96, byteLength, bytes, copyBytes` |
| `Value` (opaque; `Absent`, `IsNull`, `Kind`, typed accessors), `Int96 [3]uint32` | sealed `Value` records (`Absent`, `Null`, `Bool`, `Int32`, `Int64`, `Int96(word0, word1, word2)`, `Float32`, `Float64`, `ByteArray`, `FixedLenByteArray`) |
| `ReadStats` (mutable, reset per call), `Add` | `ReadStats` (immutable snapshot to a consumer), `plus` |
| `Dataset.Verify(ctx)`, `Columns(ctx, part)`, `BuildListIndex(ctx, options)`, `Close()` | `verify(ReadOptions)`, `columns(part, ReadOptions)`, `buildListIndex(options, Cancellation)`, `close()` |
| `Dataset.Metadata()` | `manifest()`, `rowCount()` |
| `ParseManifest(raw, Limits)` | `Manifest.parse(raw, Limits)` |
| `Column{Name, Group, Type, Length, Logical, Repetition, Key}` | `Column(name, type, typeLength, logicalType, repetition, key)` |
| `RefusalError{Code, Part, RowGroup, Row, Key}`, `RefusalCodeOf` | `RefusalException`: `code()`, `part()`, `rowGroup()`, `row()`, `key()` |
| `UnsupportedError{Code UnsupportedCode}` | `UnsupportedFeatureException`: `code()`, `UnsupportedCode.PROJECTION`, `ENCODING`, `ENCRYPTION` |
| `LimitError{Limit LimitName, Value, Max}`, `ErrLimitExceeded` | `LimitExceededException`: `limit()` (`LimitName`), `declared()` (-1 when only known to exceed), `maximum()` |
| `ErrInvalidRequest`, `ErrClosed`, context errors | `IllegalArgumentException`, `IllegalStateException`, `CancellationException` |
| `Source`, `SourceFile`, `ContextBinder` | `storage.DatasetStorage`, `storage.PartSource` (explicit `Cancellation` per read) |

**Representation differences (Java idiom).** Java has immutable `Bytes`, collections and records where Go has
byte slices and structs; sealed alternatives (`Cursor`, `ListEntry`, `Value`) where Go has tagged structs;
`Optional` for absent results (`Row.value`, `Selection.cursor`) and primitives on the batch path; `Estimate` for
Go's `(n, exact)`; builders for `Span`, `Query`, `ListQuery` and `Limits`. Failures are unchecked domain
exceptions (closed set under `SeekParquetException`) and the storage SPI throws checked `IOException`; an invalid
value is `IllegalArgumentException`, a prohibited null `NullPointerException`, a part index outside the manifest
`IndexOutOfBoundsException`, a closed dataset or retired batch `IllegalStateException` (Go has
`ErrInvalidRequest` and `ErrClosed`). Cancellation is an explicit `Cancellation` plus thread interruption instead
of a context. Statistics go to a consumer as an immutable snapshot after the operation releases the dataset,
instead of a caller-owned mutable struct; a throwing consumer is suppressed onto a failure. The batch visitor
returns `false` to stop and may throw a checked exception of its type, instead of returning `Stop` or an error.
`AutoCloseable` is only on owning resources (`Dataset`, `PartSource`), never on a batch. Negative limits fail
when `Limits` is built, not at open. Byte sizes that size one array (manifest, footer, page index, page) are
`int`; `ListQuery.limit` is an `int` as Go's `int`, and a `rows` limit must fit a `List` (at most
`Integer.MAX_VALUE`, as Go checks its slice length). `BatchColumn`'s typed getters throw on an absent column, a
null row or another type, where Go's `ColumnView` accessors return zero or unspecified bits: test `isPresent`,
`isNull` and `type` first. `Batch` has no 4,096-row cap for an absent projected column (it stores nothing for
one); batch sizes are not promised in either reader. `buildListIndex` returns its statistics and takes a
`Cancellation`, not `ReadOptions`: it delivers no `ReadStats`, and a failed build reports nothing.

**Execution and accounting differences.** Rows, ordering and cursor semantics correspond. Read statistics and
projected-byte estimates describe each implementation's access path and can differ, including for projected
keys and duplicate projection names (Java counts no bytes for a projected key column, whose values come from the
key pages, and counts a column projected twice once). Planning positions each selection's ends with a cursor of
its own, where Go moves one scan through every selection. The page-memory and decoded-metadata estimates are
this reader's own (same defaults and the same derivation, not the same accounting), so the two readers need not
refuse the same pages or footers. `get` walks the group's keys and then projects its rows; `rows` projects each
row as it walks.

**Remaining gaps** (not in this reader): validation (§8) and its reports; footer and key-page inspection;
manifest serialisation; a writer and publication; BROTLI (the baseline codecs are implemented); batched storage
reads in the storage SPI and convergence of the reader's internals and read scheduling with Go's; the listing's
row-group shortcut (`ReadStats.groupsSkipped` stays 0). Shared with the Go reader: no aggregate cap or eviction
for the part handles and footers a long-lived dataset retains (size the touched-part working set); no explicit
offset-index cache disable (0 means the default); no dataset-wide schema operation with per-part coverage and
annotation provenance (use `columns(part)` for each part).

### Options, limits and cancellation

```java
OpenOptions options = OpenOptions.DEFAULT
        .withLimits(Limits.builder().footerBytes(8 << 20).parts(100_000).getRows(10_000).build())
        .withVerifyConcurrency(32);     // verify() reads footers concurrently, on virtual threads

Cancellation cancel = Cancellation.create();
try (Dataset dataset = Dataset.open(storage, options, cancel)) {   // reads the manifest only
    dataset.verify(ReadOptions.of(cancel));                        // optional: check every part now
    List<Row> rows = dataset.rows(query, ReadOptions.of(cancel));  // cancel.cancel() from any thread ends it
}
```

Cancellation is explicit. The reader checks the `Cancellation` between storage reads and pages, and
passes it to every storage read so that an adapter can abort I/O already in flight
(`Cancellation#onCancel`). Interrupting the request's thread also ends it. Either way the request
ends with `java.util.concurrent.CancellationException`. `Dataset#close()` cancels the requests in
progress, waits for them to return, then closes the part sources.

`Limits` bound what the data may make the reader use (spec §11). The published defaults, the Go reader's
(`go/limits.go`; a test compares them):

| Setting | Kind | Default |
|---|---|---:|
| `manifestBytes` | admission | 64 MiB |
| `parts` | admission, before more entries are materialized | 1,000,000 |
| `footerBytes` | admission | 16 MiB |
| `pageIndexBytes` | admission | 16 MiB |
| `metadataMemory` | admission: estimated decoded size of one footer, page index or page header | 256 MiB |
| `pageBytes` | admission | 64 MiB |
| `pageValues` | admission | 16,777,216 |
| `pageMemory` | admission: estimated memory of decoding one page | 48 × `pageValues` + `pageBytes` (832 MiB) |
| `indexMemory` | retained budget (fails past it) | 1 GiB |
| `offsetIndexMemory` | retained cache (never fails) | 256 MiB |
| `getRows` | result limit of `get` | 1,000 |
| page-header probe ceiling | windows of 8 KiB, 256 KiB, then the ceiling | min(16 MiB, `metadataMemory`) |
| manifest nesting | fixed capacity | 512 |
| schema nesting | deeper is `invalid_footer` | 64 |
| dataset row total | fixed capacity, refused at open | 2^63 − 1 |
| `OpenOptions.verifyConcurrency` | parts `verify` checks at once | 1 |
| `ListIndexOptions` | bytes, workers, changes per page | 64 MiB, 1, 8 |
| `ListQuery.readAhead` | key pages read ahead | 0 (off), at most 64 |

`rows` and `list` are bounded by the caller's limit and `scan` by nothing: there is no separate collected-result
cap. Each request keeps one scratch slot per projected value column,
shared by duplicate projections and retained at its high-water capacity until the request ends.
Logical pages and dictionaries belong to the current row group; dictionary backing is separate from
live data-page storage, including during lazy dictionary loading. Slots survive row-group and part
changes. A slot records its owning chunk and evicts that chunk's cached page and dictionary before
another chunk decodes into it, including after out-of-order key probes. Codec state is request-local.

Range, get, list and plan results own their keys and binary values. Key pages and key dictionaries
always decode without scratch; their arrays are permanent and may be shared by results and caches.
Dictionary binary entries are also immutable and shared by returned values. The result boundary copies
binary values from reusable plain/delta arenas before those buffers can change. Rows, values, read-only
byte views and cursors can be retained after later reads or after the dataset closes. Batch scans
are the borrowed result API: their batches read the request's page storage directly, and only their owning
copies outlive the visit. Storage reads still use the `PartSource` buffer contract; this optimization does not
remove source buffer allocations or Parquet header/decoder allocations.

Each projected-column slot can retain up to approximately **4 × `pageBytes` per request** in byte
buffers: decompressed page output, V2 levels, a delta binary arena and dictionary output. Each buffer
retains its own high-water capacity across pages, row groups and parts; at the default 64 MiB page
limit these four buffers alone can total about 256 MiB per column. Primitive, offset/length and reference
arrays are additional, separately bounded by `pageValues` times their element widths. Codec state,
dictionary entries, key pages and boundary logs add memory beyond those arrays. Capacities are not charged to
the shared index budget. Caller-controlled concurrency multiplies this transient storage; these limits do not
provide an aggregate request-memory budget.

`pageMemory` bounds the memory decoding one page holds, data and dictionary pages alike, key pages included,
estimated from the page's header before anything is allocated for its values: a slot per value for its
column's representation (boolean 1, int 4, long 8, byte array 24), a null flag per row for an optional column,
and for byte arrays the arena's bytes; a byte-array dictionary counts 80 bytes per entry and its bytes twice. A
page over it fails with `LimitExceededException` naming `PAGE_MEMORY`. Its default derives as Go's does, so a
byte-array dictionary near both `pageValues` and `pageBytes` can exceed it under this reader's estimate; set it
higher to admit such a page. `metadataMemory` is charged by a walk of a footer's, page index's or page header's encoded bytes before the
decoder allocates anything: 48 bytes per structure, 16 per list element (32 per map entry), and 48 plus twice its
encoded length per string or binary value (a Java string can take two bytes per UTF-8 byte), each declared length
checked against the bytes left. A page header of unknown length is read in windows of at most min(16 MiB,
`metadataMemory`). The parts limit is enforced before materializing more than the permitted number of part
entries, including intermediate JSON representations, and after the manifest's version is decided. `indexMemory` is a dataset-wide budget for retained key indexes and exact row-group first
keys; publication is charged once under a shared lock, and a budget failure leaves already cached indexes
usable. A projected column chunk's OffsetIndex is read and checked the first time a request needs it and its
page locations kept, within `offsetIndexMemory`; past that budget each request reads it again, never failing
for it. That budget bounds an accounting charge, not heap: 32 bytes per kept page plus 64 per kept chunk, as
Go counts. Delta decoders also check padded scratch arrays against `pageBytes` before initialization.

### Errors

Every domain failure, what the data, the limits or storage make of a well-formed request, is a
`SeekParquetException`, which is unchecked. Its subclasses are a closed set:

| Exception | Meaning |
|---|---|
| `RefusalException` | The data breaks the specification. `code()` is a stable `RefusalCode` (`key_order`, `invalid_manifest`, ...) and `part()` names the part at fault. `rowGroup()` (`OptionalInt`, physical, in the part's footer), `row()` (`OptionalLong`, within the row group) and `key()` (`Optional<Bytes>`) locate it further where the reader has the value in hand, as the Go reader's `RefusalError` does; each is empty otherwise (a manifest or part-level fault has no row group, only `invalid_key` and `key_order` in a page have a row, only `key_order` a key). Only `code()` is a contract; the message is for people. |
| `UnsupportedFeatureException` | The data is valid but uses something this reader does not implement: `UnsupportedCode.ENCODING` (`unsupported_encoding`), `PROJECTION` (`unsupported_projection`: nested or repeated columns), `ENCRYPTION` (`unsupported_encryption`). |
| `LimitExceededException` | Input or a result beyond a `Limits` setting or a fixed capacity: `limit()` is a `LimitName`, with `declared()` and `maximum()`. |
| `StorageException` | Reading storage failed. Says nothing about the data. |

Outside that hierarchy: an invalid value (a `rows` limit outside 1 to `Integer.MAX_VALUE`, a negative limit, a
malformed cursor token, `split(0)`) is `IllegalArgumentException` and a prohibited null `NullPointerException`,
both thrown before anything is read and with zero statistics delivered; a part index outside the manifest
`IndexOutOfBoundsException`; a request on a closed dataset `IllegalStateException`; a cancelled or interrupted
one `CancellationException`. A part's refusal is kept for the dataset's lifetime and thrown to each request as a
fresh exception, with the same code and location. A visitor's or statistics
consumer's own exceptions propagate unchanged. The public API never returns or accepts `null`; optional values
are `Optional`.

### Thread safety

`Dataset` is safe for concurrent requests from many threads. Footers and checked page indexes are
shared and immutable once loaded; concurrent first touches of a part wait for one check. Each request decodes its own pages. A listing with read-ahead reads pages on virtual threads of its own and waits for them before it returns. Queries, results, `Bytes`,
`Manifest` and `Value`s are immutable; a `Batch` is borrowed by its callback (see [Batch scans](#batch-scans)). `Cancellation` is thread-safe. Query builders are not.

## Storage

The core is storage-agnostic. It reads through two small interfaces in `io.varve.seekparquet.storage`, with no
parquet-java type in them:

- `DatasetStorage` reads `manifest.json` and opens parts by their manifest path. The reader never
  closes it; it belongs to the caller, and must stay usable until the dataset is closed, since a part
  is opened when a request first touches it.
- `PartSource` is one opened part: its actual `size()`, an optional pinned `version()`, and
  positioned `read(position, length, cancellation)` calls. It must be safe for concurrent reads.
  A returned buffer's bytes must stay unchanged for as long as the reader retains it (a footer or page index
  until the dataset closes), even after the source is closed; its position and limit are the reader's, so a
  shared buffer is returned as its own duplicate or slice, and nothing is released by the caller.
  Every read must return bytes of the object version that was opened, or fail with
  `PartChangedException`. The reader closes every source it opened: a part refused by its check at
  once, the others when the dataset closes.

`ParquetJavaInterop` converts to and from parquet-java: `partSource(path, inputFile, version)` adapts an
existing `InputFile` (a fresh stream for each positioned read, and cancellation that closes it), and
`inputFile(partSource)` views a source as an `InputFile` whose independent streams do not close the source.

`openPart` receives the size the manifest records. An object-store adapter can use it as a hint,
for example to fetch the footer with a single suffix-range request. It must still report the
object's real size, which the reader compares with the manifest. Caching or request-coalescing
layers are decorators of these interfaces.

`LocalStorage` is the only implementation in this module. It uses positioned reads on one
`FileChannel` per part, resolves each part path one segment at a time from the root, refusing a symbolic
link in any segment, requires the part and `manifest.json` to be regular files, and detects a part file
that changes size or is replaced. Object-store adapters belong in separate modules. What it can guarantee
depends on the platform:

| | Linux, macOS (secure directory streams) | Windows (plain directory streams) |
|---|---|---|
| Lookup | relative to a pinned handle on each directory: no later replacement of a directory can redirect it | by path, each directory checked to be a real directory (no symbolic link, junction or other reparse point) and checked again before and after every open |
| Refused in a segment | symbolic links | symbolic links, junctions and other reparse points; `:` (drives, alternate data streams), `<>"\|?*`, a trailing dot or space, device names such as `NUL` or `COM1` |
| File identity, before and after each open | `fileKey` (device and inode) | size, modification and creation times (Windows has no `fileKey`), and the opened file's size |
| Reopen after an interrupt | through the same pinned handles | by path, with the directories and the file's identity checked again |
| A FIFO swapped in after the check | cannot hold a request: each open runs on its own thread (below) | cannot occur: a named pipe cannot be a directory entry, so opens run on the caller's thread |

On Windows, a replacement made between two checks, or one made to match a file's size and times, can go
unseen: keep a dataset directory writable only by its producer. Files the JDK does not report as regular
(cloud-storage placeholders, for example) are refused as not regular files. Other file systems without
secure directory streams (a zip file system, for example) take the Windows route.

Java cannot open a file without blocking on a special file, and on POSIX systems whoever can write the
dataset directory can replace an entry `LocalStorage` has checked as a regular file with a FIFO before it
opens it. So that such an open cannot hold a request beyond cancellation, interruption or `close()`, each
open of a part or of the manifest there runs on a new platform thread, through its own handle on the
file's directory, and the request stops waiting for it when cancelled (the open then ends on its own and
closes what it opened). This costs a short-lived thread and a directory handle per part open, which
happens once per part per dataset (again only after an interrupt closed the part's channel). An open that
never returns strands its thread and directory handle until the file is opened for writing or the process
exits; once 64 opens through one `LocalStorage` are stranded, its
further opens fail with a storage error instead of starting more threads.

## What opening checks, what reads check

**Open (CHECK-1)** reads the manifest and nothing else. It checks strict JSON, no duplicate names at
any depth, no undefined members, exact integers, canonical base64, PART-1 paths, ascending disjoint
bounds, and the version first (MAN-7). The manifest's exact part bounds and row counts are all a
request needs to route, so no part file is opened.

**First touch (CHECK-1).** The first request routed to a part opens it and reads its footer, and
before using any of its data checks:

- that the part exists and has exactly the manifest's size;
- that it begins and ends with `PAR1` (`PARE` at either end is `unsupported_encryption`, anything else
  `invalid_footer`);
- that each footer is readable, unencrypted and self-contained (no `file_path`), with row group
  counts that sum to the footer's row count, which must equal the manifest's;
- the `seekparquet.*` stamps;
- the key column (KEY-1, KEY-5, with one `column_orders` entry per leaf);
- that every column has the same physical type, type length, repetition and nesting as in every part
  checked before it (COL-5, incremental: untouched parts are not compared);
- that every non-empty row group declares `key` ascending as its first sorting column (RG-1).

A request whose bounds the manifest places before a part (a prefix or end at or below its `min_key`)
does not touch it. A part's refusal ends the request that touched it and is kept for as long as the
dataset is open: every later request that touches the part, and `verify()`, is refused with the same
code. Storage failures, limits and cancellation are not kept; the next request checks the part again.
Concurrent first touches wait for one check.

A touched part stays open, with its checked footer in memory, until the dataset closes, and no limit
bounds how many: `Limits.parts()` bounds the manifest, the index memory budget the retained page indexes,
and `verifyConcurrency` how many parts `verify()` checks at a time, not how many stay open.
`LocalStorage` keeps a file channel per part, and on POSIX systems a directory handle too. Size file descriptors and memory for
the most parts one dataset will touch (`verify()` touches them all): a single pass over more parts than
that fails when the process runs out, and opening a fresh dataset for it does not help. This is a known
limitation, documented rather than solved: there is no cap that refuses such a pass early, and no eviction.

**Verify (CHECK-3).** `Dataset.verify()` checks every part not yet checked, `verifyConcurrency` at a
time (1 by default, as the Go reader), then decodes every row group's first key page and each part's last, and compares
each part's first and last key with the manifest (`bounds_mismatch`). It throws the first failure in
manifest order. It reads one footer per unchecked part plus a page index and one or two key pages per
row group, never every row. Use it for eager, fail-fast checks of a long-lived local reader.

**Reads (CHECK-2).** Requests binary-search the manifest's exact part bounds. Within large parts,
they binary-search checked, decoded row-group first keys, retaining the predecessor so a repeated
key that starts at the previous group's tail is not skipped. Exact first keys are cached and charged
to `Limits.indexMemory()`. Small remaining sets (at most eight groups) use an index-bound scan.
Page maxima are examined in order: shortened, overlapping, non-monotone bounds are never
binary-searched. Broad page bounds can still require reading many pages within a candidate group.
A request checks what it reads:

- each row group's key ColumnIndex and OffsetIndex before first use (RG-2 to RG-4); the result is
  cached for the dataset;
- each key page it decodes: the page header matches its OffsetIndex entry; the page holds one
  non-empty key per row (valid UTF-8 for string keys); keys obey the manifest's unique or grouped order, within the
  page's bounds, and ordered consistently with every other key page the request decoded, including
  out-of-order routing probes. Equal keys are accepted only in grouped mode.

Row continuations first locate the repeated group's start, then translate the logical offset into a
physical row and use the OffsetIndex to jump directly to its page. Oversized or stale offsets fall
back to seeking strictly after the key, so unrelated later keys are not skipped. This avoids replaying
all earlier pages of a large repeated group on every request. Locating the group start still depends
on the usefulness of its page bounds, and crossing row groups uses their footer row counts.

Index reads coalesce only when the gap is no larger than the useful bytes (and at most 64 KiB),
bounding each combined read at 2× the useful index bytes rather than repeatedly fetching large overlaps.
This can trade one wide read for two narrow reads. Routing may decode probe pages outside the result
interval; decoded page boundaries are held for the request to check their relative key order.

**Cost model.** Opening reads the manifest: one read, whatever the number of parts. A cold request
adds one footer read for each part it touches (the last 64 KiB, the whole part when smaller, never more than
`Limits.footerBytes()` plus the 8-byte trailer; a footer larger than that costs a second read of the footer bytes before
it), and, for a part larger than that window, one 4-byte read of its leading magic (CHECK-1), independent of the part
count, then the page indexes and pages above. The deterministic `SeekCostTest` counts a request's
storage calls and requested bytes apart from those footer reads, independently of latency. With 1,000 ten-row groups, a cold final-key lookup uses 28 reads / 1,877 bytes (previously
1,001 / 33,956,205); a warm lookup still uses one read. Paginating 10,000 identical keys in 100-row
responses uses 219 reads / 165,479 bytes (previously 5,060 / 4,160,780). These synthetic layouts expose
access patterns, not typical throughput. A complete lookup now uses 120 narrow reads / 84,299 bytes
rather than 110 wider reads / 101,780 bytes.

Overlapping or non-monotone bounds are valid, and `boundary_order` is never read (RG-6). Corrupt
data in the pages a request decodes is `invalid_parquet` (CHECK-5).

**What it does not certify.** A successful request certifies nothing about rows it did not decode
(CHECK-4). In particular, routing trusts the manifest's part bounds. If they are wrong
(`bounds_mismatch`, which only validation must find), a seek may land in the wrong part. Call
`verify()` to compare each part's first and last key with the manifest.

## What it does not do

- Validation (§8: `structural`, `profile`, `full`, `digest`), including SHA-256 digests. The
  conformance adapter answers `validate` with `unsupported_operation`.
- Writing datasets.
- Projecting nested or repeated columns (reported as `unsupported_projection`). `INT96` columns are projected as raw bits.
- BROTLI, LZO and Hadoop-framed LZ4 compression, and modular encryption (reported as unsupported).
  BROTLI is outside the ENC-1 baseline, which includes LZ4_RAW; the Go reader also decodes BROTLI. Beyond
  the baseline, this reader decodes, as the Go reader does, `BYTE_STREAM_SPLIT` values of `FLOAT`, `DOUBLE`, `INT32`, `INT64` and
  `FIXED_LEN_BYTE_ARRAY` columns (another type is `invalid_parquet`).
- The planned indexed-access conformance suite. The reader is index-guided, but passing the
  semantic suite does not prove that.

## Building and running the conformance suite

```sh
# Set JAVA_HOME to your JDK 21 installation.
cd java
./gradlew build                       # compile, javadoc, unit tests
./gradlew :conformance:installDist    # build the adapter launcher
cd ..
python3 conformance/harness/run.py \
    --adapter java/conformance/build/install/seekparquet-conformance/bin/seekparquet-conformance
```

The adapter accepts arbitrarily large positive integer query limits. It uses bounded library
requests (at most 1,000,000 rows or entries per internal page, an adapter constant; the library has no
result cap) and cursors to assemble one protocol response, stopping at the requested limit or exhaustion.
It never clamps the requested total to Java's `int` range. A planned range becomes a `Selection`, with its
row cursor and no limit. The full
JSON response is still buffered in memory; internal paging is not a total response-memory bound.

The adapter claims the `reader` target and must pass every case of it, and the optional `verify` and
`planning` capabilities; validator targets are not claimed yet, so the harness skips their cases. Run the build and harness commands above for Java verification.
The [CI workflow](../.github/workflows/ci.yml) runs the Java build, unit tests and reader conformance
suite for changes under `java/`, `spec/`, `conformance/` or `.github/`. Its `ci-ok` job requires the
Java job to succeed whenever those paths make it necessary.

The build uses the checked-in Gradle wrapper (Gradle 9.9.0) and a Java 21 toolchain. The unit
tests write their Parquet files to temporary directories, using a small test-only writer. They also
read the shared fixtures under `conformance/datasets`.

| Path | What it is |
|---|---|
| `seekparquet/` | The library: public API in `io.varve.seekparquet`, storage interfaces in `io.varve.seekparquet.storage`. Everything else is package-private. |
| `conformance/` | The conformance adapter (`io.varve.seekparquet.conformance.Adapter`), not part of the library. |

`Manifest.rows()` returns a signed 64-bit total and throws `ArithmeticException`
if the sum exceeds `Long.MAX_VALUE`; `Manifest.rowsExact()` returns the exact `BigInteger` total, including
zero for an empty dataset. Parsing accepts such a manifest (the format does not cap the sum), but
`Dataset.open` refuses it as beyond this reader's capacity (`LimitExceededException`, `DATASET_ROWS`), as the Go
reader does, so `Dataset.rowCount()` always fits.

### Page allocation benchmark

Run the opt-in generated-fixture benchmark with JDK 21:

```sh
SEEKPARQUET_PAGE_BENCH=1 ./gradlew :seekparquet:test --tests '*PageReuseBenchmarkTest' --offline --max-workers=1 --rerun-tasks
```

The test reports current-thread allocated bytes and time per row for plan scans of 65,536 rows, once making an
owning `Batch.row` per row (`PAGE_BENCH`) and once reading the borrowed typed accessors (`BATCH_BENCH`),
two parts, 4,096 rows/group and 256 rows/page, with nullable binary and INT64 projections. It compares
PLAIN binary pages with dictionary binary pages (eight non-null entries, one-third null rows), each
with uncompressed and Zstd value pages; keys are uncompressed. Fixture generation, planning and warmup are
outside measurement; every measured scan creates fresh request scratch. Results include a field
checksum, and are recorded in the test XML under `seekparquet/build/test-results/test/`. Timing is
informational on shared hosts. The benchmark is skipped in the normal build.
