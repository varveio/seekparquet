# SeekParquet for Java

A reader for [SeekParquet](../spec/format-1.md), a sorted-key profile for Apache Parquet. Given a
dataset (a `manifest.json` and the Parquet parts it lists), it answers range reads, prefix reads and
delimited listings with projected columns. To do so it reads the manifest, the footer of each part a
request touches, a page index and a few pages, with no query engine and no catalog.

- **Format:** format 1, draft revision 1 (written on disk as `format_version` 0, `draft_revision` 1).
- **JDK:** 21 or later.
- **Coordinates:** `io.varve:seekparquet` (not yet published), package `io.varve.seekparquet`.
- **Parquet library:** Apache parquet-java 1.18.1 (`parquet-format-structures` and
  `parquet-column`), plus `parquet-common` for the public `InputFile` interface, without Hadoop. See [QUALIFICATION.md](QUALIFICATION.md) for why, and for the
  full dependency tree (about 6.7 MB, no native code).
- **Status:** passes every case of the shared conformance suite's `reader` target. It does not
  implement validation (§8); see [What it does not do](#what-it-does-not-do).

## Usage

```java
import io.varve.seekparquet.*;
import java.nio.file.Path;

try (Dataset dataset = Dataset.open(Path.of("/data/listing"))) {
    // A range: keys in [photos/2024/, photos/2025/), at most 1000, with two columns.
    List<Row> rows = dataset.range(RangeQuery.builder()
            .start(Bytes.utf8("photos/2024/"))
            .end(Bytes.utf8("photos/2025/"))
            .limit(1000)
            .columns("size", "etag")
            .build());
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
    List<Row> logs = dataset.range(RangeQuery.prefix(Bytes.utf8("logs/"), 100));

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

Keys, bounds and binary values are `Bytes`: immutable byte strings compared in unsigned
lexicographic order. They need not be UTF-8. Values are physical Parquet values (`Bool`, `Int32`,
`Int64`, `Float32`, `Float64`, `ByteArray`, `FixedLenByteArray`), with `Null` and `Absent` kept
apart. Floating-point values carry their exact bits. The manifest's `key` declaration names the
key column; projecting that exact name returns the key's bytes, including literal dots in its name.

When `manifest().key().unique()` is false, equal keys form a group in file order. Groups can cross
pages and row groups, but never parts. Each `Row` includes its zero-based `offset()` within its
group (zero in unique mode). Tagged continuations preserve that position:

```java
RangeQuery query = RangeQuery.lookup(Bytes.utf8("photos/item"), 100)
        .toBuilder().columns("size").build();
List<Row> page = dataset.range(query);
if (!page.isEmpty()) {
    List<Row> nextPage = dataset.range(query.next(page.getLast()));
}
// A convenient first-page lookup:
List<Row> group = dataset.lookup(Bytes.utf8("photos/item"), 100, "size");
```

`RangeQuery.next(Row)` and `ListQuery.next(ListEntry)` keep the original bounds and use the sealed
`Continuation` alternatives: `RowPosition(key, offset)` and `CommonPrefix(bytes)`. A result limit
can split a group. Resuming a row continues inside that group before later keys; resuming a common
prefix skips its subtree. Ranges accept only row cursors; listings accept either cursor type.
The adapter accepts only unsigned decimal integer offsets in the range 0 through 2^63-1;
spellings such as `-0`, `0.0` and `0e0` are rejected.

### Planning a selection

`Dataset.planRanges(ranges, columns)` plans reading every row of a set of ranges before reading it. A
`RangePlan` holds the selected rows (rows several ranges select are one row), their `count()`, and the
`projectedBytes()` a scan of the projected columns will request; `scan(...)` then hands every row, with its
values, to a callback in dataset order.

```java
RangePlan plan = dataset.planRanges(List.of(
        RangeQuery.builder().prefix(Bytes.utf8("photos/2024/")).limit(1).build(),
        RangeQuery.builder().start(Bytes.utf8("logs/")).end(Bytes.utf8("logs0")).limit(1).build()),
        List.of("size"));
long rows = plan.count();                   // exact when plan.countExact()
long bytes = plan.projectedBytes();         // exact when plan.projectedBytesExact()
plan.scan(cancel, stats -> log(stats), row -> process(row));
```

Planning decodes the key pages at the two ends of each range, as a range read would, and adds up row
counts from footers and OffsetIndex row positions; with columns it checks every part holding a selected
row and reads each projected chunk's OffsetIndex, but no data page. Without columns, a count across a part
no read has checked rests on the manifest's row count, and `countExact()` is false. `projectedBytes()` is,
per column chunk the rows span, the extents of the pages holding them and of the dictionary page (the bytes
before the first data page), each counted once; a scan reads each counted extent in one request and no
other byte of those chunks. A chunk without an OffsetIndex (COL-4) is omitted and `projectedBytesExact()`
is false. A plan is immutable, bound to its dataset, and its scan fails once the dataset is closed.

`plan.split(n)` divides a plan into at most `n` shards for a whole scan on several cores: each shard is a
`RangePlan` over consecutive rows of the plan, and the shards' scans, run on the caller's threads (the
library starts none), together yield the plan's rows, each once. A boundary is a selected row that begins a
row group (or a part no read has checked yet), never inside a group of equal keys: with repeated keys,
splitting decodes the two key pages around each boundary inside a part and moves a boundary that would divide
a group. Boundaries balance `projectedBytes()` when the plan has columns and exact bytes, else the rows. The
shards' counts and projected bytes add up to the plan's, each row group's bytes counted in the one shard
holding it; the first shard keeps the plan's start bounds and cursors, the last its end. A shard's `stats()`
are the reads that placed its end.

```java
List<RangePlan> shards = plan.split(Runtime.getRuntime().availableProcessors());
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    for (RangePlan shard : shards) {
        executor.submit(() -> shard.scan(cancel, row -> process(row)));
    }
}
```

### Batch scans

`RangePlan.scanBatches(...)` and `Dataset.rangeBatches(query, ...)` hand the rows `scan` and `range` return to a
callback as borrowed `Batch`es instead of `Row`s, read through primitive accessors without a `Row`, `Value` or
`Bytes` object per row. `rangeBatches` takes a `RangeQuery` exactly as `range` does (bounds, prefix, cursor,
columns and a limit, with its errors and limits) and plans nothing: it reads what `range` reads, a dictionary
only for a page that uses it, and stops at the limit. `scanBatches` reads what `scan` reads. Each has the three
overloads of `scan`: with a `Cancellation`, and with a `Consumer<ReadStats>` that receives the request's counts
whether it succeeds or fails; when every callback returns normally, `rangeBatches` counts what the same `range`
counts against the same caches.

```java
plan.scanBatches(cancel, stats -> log(stats), batch -> {
    BatchColumn size = batch.column(0);                 // projection slot 0, in request order
    for (int i = 0; i < batch.size(); i++) {
        int n = batch.copyKey(i, keyBuffer, 0);         // or keyLength(i), compareKey(i, bytes), keyBuffer(i)
        if (size.isAbsent()) {                          // the batch's part has no such column (COL-2)
            absent(keyBuffer, n);
        } else if (!size.isNull(i)) {
            total += size.getLong(i);                   // getBoolean, getInt, getFloat(Bits), getDouble(Bits)
        }
    }
});
```

`Batch` has `size()`, `columns()`, `offset(i)` (the row's offset in its group of equal keys), the key accessors
and `column(c)`; a `BatchColumn` has `isAbsent()`, `type()` (the physical type in the batch's part), `isNull(i)`,
a getter per physical type (floating-point values also as their exact bits), and for byte arrays `length(i)`,
`copyBytes(i, dst, offset)` and `byteBuffer(i)`. A projected key column is a `BYTE_ARRAY` slot holding the keys;
duplicate projections are slots of their own. Reading another type than the slot's, a null row or an absent slot
throws `IllegalStateException`; an index outside the batch, `IndexOutOfBoundsException`.

A batch is a positive run of consecutive rows that never crosses a row group, a key page, a page of any
projected column, or a gap between a plan's ranges; batches may be smaller, and their size is not promised.
**A batch, its columns, and the buffers `keyBuffer` and `byteBuffer` return are borrowed: read-only and valid
only until the callback returns.** Using a batch or its column afterwards throws `IllegalStateException`; a
buffer view kept past it may show bytes of a later page. `row(i)`, `continuation(i)`, `key(i)`,
`BatchColumn.value(i)` and `BatchColumn.bytes(i)` make owning copies: the rows and continuations `scan` and
`range` return, which may be kept. The `ByteBuffer` views and owning copies allocate.

An exception the callback throws ends the request and propagates unchanged; batches already handed over stand,
as they do before a read error or refusal. Cancellation is checked as `scan` and `range` check it, and also before
every callback. The request holds the dataset open while the callback runs: `close()` from another thread
cancels it and waits for the callback to return, and closing the dataset from inside the callback fails with
`IllegalStateException`. Concurrent and nested requests have storage of their own. `scan` and `range` keep their
own row paths; the batch paths return the same rows (tested over every conformance dataset).

### Typed columns

`Dataset.columns(part)` returns one part's top-level columns, in schema order, from the footer the dataset
checked and holds for that part, checking the part first if no read has. Each `Column` has its physical type
and `FIXED_LEN_BYTE_ARRAY` length, its `LogicalType` (the `logical_type` annotation, or else the legacy
`converted_type` normalized into one), repetition, and whether it is the key. `Dataset.columns()` still
describes the parts checked so far without checking any, merged by name with the number of parts that
have each column.

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
`Limits.IndexMemory` cache (Java: `maxIndexMemoryBytes`). Checked part footers remain shared. Ordinary routing probes and synthetic landings
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

`range`, `list`, `rangeBatches`, `RangePlan.scan` and `RangePlan.scanBatches` accept a `Consumer<ReadStats>` that receives the request's counts when
it ends, whether it succeeds or fails: parts opened, key and value pages decoded, keys decoded, row-group key
indexes taken and loaded, page index seeks, prefix skips, and the storage reads and bytes requested.
`RangePlan.stats()` holds planning's. They describe this reader's access path, not the format.

### Parity with the Go reader

| Go | Java | Difference |
|---|---|---|
| `Dataset.BuildListIndex(ctx, options)` | `Dataset.buildListIndex(options, cancellation)` | Same defaults and admission policy. Java throws on cancellation; Go also returns partial build statistics with zero retained bytes. |
| `Dataset.PlanRanges(ctx, ranges, columns)` | `Dataset.planRanges(ranges, columns, cancellation)` | A Java `RangeQuery` always has a positive limit, so a planned range's limit is ignored rather than rejected; a range with columns is rejected (`IllegalArgumentException`), as in Go. |
| `RangePlan.Count() (n, exact)` | `count()`, `countExact()` | |
| `RangePlan.ProjectedBytes() (n, exact)` | `projectedBytes()`, `projectedBytesExact()` | The key column, when projected, counts nothing: Java takes its values from the key pages the scan decodes, where Go reads its chunk. A column projected twice counts once, as the scan reads it once. |
| `RangePlan.Scan(ctx, st, fn)` | `scan(cancellation, stats, action)` | `action` stops the scan by throwing. A closed dataset is `IllegalStateException`. Closing the dataset from inside `action` fails at once with `IllegalStateException`, where Go's `Close` from inside `fn` waits for ever: neither may close from inside a scan. |
| `RangePlan.Stats()` | `stats()` | Java positions each range's ends with a cursor of its own, where Go moves one scan through every range and continues from its current row when it can: for ranges near each other, Java can count more key pages, keys and row-group openings for the same plan. Rows, counts and projected bytes do not differ. |
| `RangePlan.Split(ctx, n)` | `split(n, cancellation)` | No shard is an empty list, not nil. |
| `RangePlan.ScanBatches(ctx, st, fn)` | `scanBatches(cancellation, stats, action)` | Same rows, boundaries and lifetime; it reads what Java's `scan` reads. `Batch.Column(c)` returning Go `[]parquet.Value` is `Batch.column(c)`, a `BatchColumn` read by row through typed accessors; Go's `Key(i) []byte` is `copyKey`, `keyLength`, `compareKey` or `keyBuffer`, since Java hands out no array of its page storage: key arrays and dictionary entries are shared with owning results and the dataset's caches. Go's accessors panic on a bad index and a retained batch is undefined; Java throws `IndexOutOfBoundsException`, and `IllegalStateException` for a batch used after its callback. A null action is `NullPointerException`. |
| `Dataset.RangeBatches(ctx, rng, fn)` | `rangeBatches(query, cancellation, stats, action)` | As `scanBatches`. Go's nil `fn` is `ErrInvalidRequest`. |
| `Batch.Row(i)`, `Batch.Cursor(i)` | `row(i)`, `continuation(i)` | |
| `Dataset.Columns(ctx, i)` | `columns(i, cancellation)` | An index outside the parts is `IndexOutOfBoundsException`; `Column.parts()` is 1. |
| `Column{Logical *format.LogicalType}` | `Column.logicalType()`: `Optional<LogicalType>` | `LogicalType` is a sealed interface of records, so the API does not expose parquet-java's Thrift classes. |
| `Limits.OffsetIndexMemory` | `Limits.maxOffsetIndexMemoryBytes()`, `withMaxOffsetIndexMemoryBytes(bytes)` | Same default (256 MiB), charge (32 bytes per page plus 64 per chunk) and fallback past it. Go's zero means the default; Java's limits must be positive. |
| `ReadStats` via `Range.Stats`, `List.Stats` | `ReadStats` via a `Consumer<ReadStats>` argument | Same fields and counting rules. `groupsSkipped` is always 0: Java's listing has no row-group shortcut. The access paths differ, so counts for the same request can differ. |

### Options, limits and cancellation

```java
OpenOptions options = OpenOptions.DEFAULT
        .withLimits(new Limits(16 << 20, 100_000, 16 << 20, 4 << 20, 64 << 20, 1 << 22, 100_000))
        .withVerifyConcurrency(32);     // verify() reads footers concurrently, on virtual threads

Cancellation cancel = Cancellation.create();
try (Dataset dataset = Dataset.open(storage, options, cancel)) {   // reads the manifest only
    dataset.verify(cancel);                          // optional: check every part now
    List<Row> rows = dataset.range(query, cancel);   // cancel.cancel() from any thread ends it
}
```

Cancellation is explicit. The reader checks the `Cancellation` between storage reads and pages, and
passes it to every storage read so that an adapter can abort I/O already in flight
(`Cancellation#onCancel`). Interrupting the request's thread also ends it. Either way the request
ends with `java.util.concurrent.CancellationException`. `Dataset#close()` cancels the requests in
progress, waits for them to return, then closes the part sources.

`Limits` bound what the data may make the reader allocate (spec §11): the manifest size, the number
of parts, footer, page index and page sizes, page value counts, and the maximum requested result
entries (1,000,000 by default). Each request keeps one scratch slot per projected value column,
shared by duplicate projections and retained at its high-water capacity until the request ends.
Logical pages and dictionaries belong to the current row group; dictionary backing is separate from
live data-page storage, including during lazy dictionary loading. Slots survive row-group and part
changes. A slot records its owning chunk and evicts that chunk's cached page and dictionary before
another chunk decodes into it, including after out-of-order key probes. Codec state is request-local.

Existing range, list and scan results own their keys and binary values. Key pages and key dictionaries
always decode without scratch; their arrays are permanent and may be shared by results and caches.
Dictionary binary entries are also immutable and shared by returned values. The result boundary copies
binary values from reusable plain/delta arenas before those buffers can change. Rows, values, read-only
byte views and continuations can be retained after later reads or after the dataset closes. Batch scans
(`scanBatches`, `rangeBatches`) are the borrowed result API: their batches read the request's page storage
directly, and only their owning copies outlive the callback.
Storage reads still use the `PartSource` buffer contract; this optimization does not remove source
buffer allocations or Parquet header/decoder allocations.

Each projected-column slot can retain up to approximately **4 × `maxPageBytes` per request** in byte
buffers: decompressed page output, V2 levels, a delta binary arena and dictionary output. Each buffer
retains its own high-water capacity across pages, row groups and parts; at the default 256 MiB page
limit these four buffers alone can total about 1 GiB per column. Primitive, offset/length and reference
arrays are additional, separately bounded by `maxPageValues` times their element widths. Codec state,
dictionary entries, key pages and boundary logs add memory beyond those arrays. Capacities are not charged to the shared index budget. Caller-controlled concurrency multiplies
this transient storage; these limits do not provide an aggregate request-memory budget.

`Limits.withMaxPageMemory(bytes)` bounds the memory decoding one page holds, data and dictionary pages
alike, key pages included, estimated from the page's header before anything is allocated for its
values: a slot per value for its column's representation (boolean 1, int 4, long 8, byte array 24: an
offset and a length into the arena, or a reference and an array header for keys and dictionary
entries), a null flag per row for an optional column, and for byte arrays the arena's bytes; a
byte-array dictionary counts 80 bytes per entry (parquet-java's `Binary` and the entry a request
materializes) and its bytes twice. A page over it fails with `LimitExceededException` naming
`maxPageMemory`. Its default (0, or any constructor without it) is the most `maxPageValues` and
`maxPageBytes` allow, 832 MiB at their defaults (a byte-array dictionary of 4 Mi entries in 256 MiB),
so the default refuses no page those limits admit; set it lower to bound decoding memory, at the cost
of refusing pages within the other limits. The bound is per page and per request: concurrent requests
and shards scanned at once each decode their own pages, so their memory adds up.
`Limits.withMaxIndexMemoryBytes(bytes)` sets a dataset-wide budget for retained
key indexes and retained exact row-group first keys (256 MiB by default), using conservative estimates of their decoded arrays,
bounds and cache entries. Publication is charged once under a shared lock; a budget failure leaves
already cached indexes usable. A projected column chunk's OffsetIndex is read and checked the first time a
request needs it and its page locations kept, within `Limits.withMaxOffsetIndexMemoryBytes(bytes)` (default
256 MiB, apart from `maxIndexMemoryBytes`); past that budget each request reads it again, never failing for it.
The budget bounds an accounting charge, not heap: 32 bytes per kept page plus 64 per kept chunk, as Go's
`Limits.OffsetIndexMemory` counts; JVM headers and alignment can exceed it. It leaves out each opened part's
slot array (one reference per physical row group and leaf, allocated even for key-only requests or a 1-byte
budget), transient Thrift data, duplicate loads by concurrent first uses, and the locations a request holds
for itself past the budget. Delta decoders also check padded scratch arrays against `maxPageBytes`
before initialization. Page and entry limits are not a total request-memory guarantee: concurrent
requests each retain their own pages, dictionaries and results. A size above a limit fails with
`LimitExceededException`, which is not a refusal.

### Errors

Every failure about a dataset or request is a `SeekParquetException`, which is unchecked. Its
subclasses are a closed set:

| Exception | Meaning |
|---|---|
| `RefusalException` | The data breaks the specification. `code()` is a stable `RefusalCode` (`key_order`, `invalid_manifest`, ...) and `part()` names the part at fault. |
| `UnsupportedFeatureException` | The data is valid but uses something this reader does not implement: `unsupported_encoding`, `unsupported_projection` (nested, repeated or `INT96` columns), `unsupported_encryption`. |
| `LimitExceededException` | A declared size exceeds a configured limit. |
| `StorageException` | Reading storage failed. Says nothing about the data. |

A malformed request (a limit that is not positive) is rejected when the query is built, with
`IllegalArgumentException`. A request on a closed dataset fails with `IllegalStateException`. The
public API never returns or accepts `null`; optional values are `Optional`.

### Thread safety

`Dataset` is safe for concurrent requests from many threads. Footers and checked page indexes are
shared and immutable once loaded; concurrent first touches of a part wait for one check. Each request decodes its own pages. Queries, results, `Bytes`,
`Manifest` and `Value`s are immutable; a `Batch` is borrowed by its callback (see [Batch scans](#batch-scans)). `Cancellation` is thread-safe. Query builders are not.

## Storage

The core is storage-agnostic. `parquet-common` is an API dependency so consumers can use the
standard Parquet I/O types directly. The small `PartSource` extension adds concurrent positioned
reads, explicit cancellation and optional identity pinning to that contract. It reads through two small interfaces in
`io.varve.seekparquet.storage`:

- `DatasetStorage` reads `manifest.json` and opens parts by their manifest path. The reader never
  closes it; it belongs to the caller, and must stay usable until the dataset is closed, since a part
  is opened when a request first touches it.
- `PartSource` extends parquet-java’s `InputFile` and is one opened part: its actual `size()`, an optional pinned `version()`, and
  positioned `read(position, length, cancellation)` calls. It must be safe for concurrent reads.
  Its `newStream()` supplies an independent parquet-java `SeekableInputStream` without closing the
  source when that stream closes. `PartSource.of(path, inputFile, version)` adapts an existing
  `InputFile`, with a fresh stream for each positioned read and cancellation that closes that stream.
  Every read must return bytes of the object version that was opened, or fail with
  `PartChangedException`. The reader closes every source it opened: a part refused by its check at
  once, the others when the dataset closes.

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
exits; once `LocalStorage.MAX_STRANDED_OPENS` (64) opens through one `LocalStorage` are stranded, its
further opens fail with a storage error instead of starting more threads.

## What opening checks, what reads check

**Open (CHECK-1)** reads the manifest and nothing else. It checks strict JSON, no duplicate names at
any depth, no undefined members, exact integers, canonical base64, PART-1 paths, ascending disjoint
bounds, and the version first (MAN-7). The manifest's exact part bounds and row counts are all a
request needs to route, so no part file is opened.

**First touch (CHECK-1).** The first request routed to a part opens it and reads its footer, and
before using any of its data checks:

- that the part exists and has exactly the manifest's size;
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
Concurrent first touches wait for one check. `columns()` describes the parts checked so far.

A touched part stays open, with its checked footer in memory, until the dataset closes, and no limit
bounds how many: `maxParts` bounds the manifest, the index memory budget the retained page indexes,
and `verifyConcurrency` how many parts `verify()` checks at a time, not how many stay open.
`LocalStorage` keeps a file channel per part, and on POSIX systems a directory handle too. Size file descriptors and memory for
the most parts one dataset will touch (`verify()` touches them all): a single pass over more parts than
that fails when the process runs out, and opening a fresh dataset for it does not help. This is a known
limitation, documented rather than solved: there is no cap that refuses such a pass early, and no eviction.

**Verify (CHECK-3).** `Dataset.verify()` checks every part not yet checked, `verifyConcurrency` at a
time (8 by default), then decodes every row group's first key page and each part's last, and compares
each part's first and last key with the manifest (`bounds_mismatch`). It throws the first failure in
manifest order. It reads one footer per unchecked part plus a page index and one or two key pages per
row group, never every row. Use it for eager, fail-fast checks of a long-lived local reader.

**Reads (CHECK-2).** Requests binary-search the manifest's exact part bounds. Within large parts,
they binary-search checked, decoded row-group first keys, retaining the predecessor so a repeated
key that starts at the previous group's tail is not skipped. Exact first keys are cached and charged
to `maxIndexMemoryBytes`. Small remaining sets (at most eight groups) use an index-bound scan.
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
adds one footer read (two for a footer above 64 KiB) for each part it touches, independent of the part
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
- Projecting nested or repeated columns, or `INT96` (reported as `unsupported_projection`).
- BROTLI, LZO and Hadoop-framed LZ4 compression, and modular encryption (reported as unsupported).
  The Go reader also decodes BROTLI. Beyond the ENC-1 baseline, this reader decodes LZ4_RAW and, as the
  Go reader does, `BYTE_STREAM_SPLIT` values of `FLOAT`, `DOUBLE`, `INT32`, `INT64` and
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
requests (at most the default `maxResultEntries` per internal page) and tagged continuations to
assemble one protocol response, stopping at the requested limit or exhaustion. It never clamps
the requested total to Java's `int` range, and it leaves the library defaults unchanged. The full
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
if the sum exceeds `Long.MAX_VALUE`, preserving its existing signature and overflow
behavior. `Manifest.rowsExact()` returns the exact `BigInteger` total, including
zero for an empty dataset. This accessor limit is not a format refusal: parsing,
opening, and queries do not reject a dataset merely because its total exceeds the
range of `rows()`.

### Page allocation benchmark

Run the opt-in generated-fixture benchmark with JDK 21:

```sh
SEEKPARQUET_PAGE_BENCH=1 ./gradlew :seekparquet:test --tests '*PageReuseBenchmarkTest' --offline --max-workers=1 --rerun-tasks
```

The test reports current-thread allocated bytes and time per row for owning scans of 65,536 rows,
two parts, 4,096 rows/group and 256 rows/page, with nullable binary and INT64 projections. It compares
PLAIN binary pages with dictionary binary pages (eight non-null entries, one-third null rows), each
with uncompressed and Zstd value pages; keys are uncompressed. Fixture generation, planning and warmup are
outside measurement; every measured scan creates fresh request scratch. Results include a field
checksum, and are recorded in the test XML under `seekparquet/build/test-results/test/`. Timing is
informational on shared hosts. The benchmark is skipped in the normal build.
