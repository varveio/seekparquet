# SeekParquet for Java

A reader for [SeekParquet](../spec/format-1.md), a sorted-key profile for Apache Parquet. Given a
dataset (a `manifest.json` and the Parquet parts it lists), it answers range reads, prefix reads and
delimited listings with projected columns. To do so it reads the manifest, the footers, a page index
and a few pages, with no query engine and no catalog.

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

### Options, limits and cancellation

```java
OpenOptions options = OpenOptions.DEFAULT
        .withLimits(new Limits(16 << 20, 100_000, 16 << 20, 4 << 20, 64 << 20, 1 << 22, 100_000))
        .withOpenConcurrency(32)        // read footers concurrently, on virtual threads
        .withVerifyPartBounds(false);   // true also decodes each part's first and last key on open

Cancellation cancel = Cancellation.create();
try (Dataset dataset = Dataset.open(storage, options, cancel)) {
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
entries (1,000,000 by default). Per-request dictionaries and value pages are retained only for the
current row group. `Limits.withMaxIndexMemoryBytes(bytes)` sets a dataset-wide budget for retained
key and value indexes and retained exact row-group first keys (256 MiB by default), using conservative estimates of their decoded arrays,
bounds and cache entries. Publication is charged once under a shared lock; a budget failure leaves
already cached indexes usable. Delta decoders also check padded scratch arrays against `maxPageBytes`
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
shared and immutable once loaded. Each request decodes its own pages. Queries, results, `Bytes`,
`Manifest` and `Value`s are immutable. `Cancellation` is thread-safe. Query builders are not.

## Storage

The core is storage-agnostic. `parquet-common` is an API dependency so consumers can use the
standard Parquet I/O types directly. The small `PartSource` extension adds concurrent positioned
reads, explicit cancellation and optional identity pinning to that contract. It reads through two small interfaces in
`io.varve.seekparquet.storage`:

- `DatasetStorage` reads `manifest.json` and opens parts by their manifest path. The reader never
  closes it; it belongs to the caller.
- `PartSource` extends parquet-java’s `InputFile` and is one opened part: its actual `size()`, an optional pinned `version()`, and
  positioned `read(position, length, cancellation)` calls. It must be safe for concurrent reads.
  Its `newStream()` supplies an independent parquet-java `SeekableInputStream` without closing the
  source when that stream closes. `PartSource.of(path, inputFile, version)` adapts an existing
  `InputFile`, with a fresh stream for each positioned read and cancellation that closes that stream.
  Every read must return bytes of the object version that was opened, or fail with
  `PartChangedException`. The reader closes every source it opened, including on a failed open.

`openPart` receives the size the manifest records. An object-store adapter can use it as a hint,
for example to fetch the footer with a single suffix-range request. It must still report the
object's real size, which the reader compares with the manifest. Caching or request-coalescing
layers are decorators of these interfaces.

`LocalStorage` is the only implementation in this module. It uses positioned reads on one
`FileChannel` per part, opens parts through pinned directory handles and rejects symbolic links in each part-path component, and detects a
part file that changes size or is replaced. Filesystems without `SecureDirectoryStream` are refused
with a storage error; reopening after interruption uses the same pinned parent handle. Object-store adapters belong in separate modules.

## What opening checks, what reads check

**Open (CHECK-1)** reads the manifest and one footer per part. It checks:

- the manifest: strict JSON, no duplicate names at any depth, no undefined members, exact integers,
  canonical base64, PART-1 paths, ascending disjoint bounds, and the version first (MAN-7);
- that each part exists and has exactly the manifest's size;
- that each footer is readable, unencrypted and self-contained (no `file_path`), with row group
  counts that sum to the footer's row count, which must equal the manifest's;
- the `seekparquet.*` stamps;
- the key column (KEY-1, KEY-5, with one `column_orders` entry per leaf);
- that every column has the same physical type, type length, repetition and nesting in every part
  (COL-5);
- that every non-empty row group declares `key` ascending as its first sorting column (RG-1).

It does not load page indexes or decode keys unless `verifyPartBounds` is set.

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

The deterministic `SeekCostTest` counts storage calls and requested bytes after open, independently
of latency. With 1,000 ten-row groups, a cold final-key lookup uses 28 reads / 1,877 bytes (previously
1,001 / 33,956,205); a warm lookup still uses one read. Paginating 10,000 identical keys in 100-row
responses uses 219 reads / 165,479 bytes (previously 5,060 / 4,160,780). These synthetic layouts expose
access patterns, not typical throughput. A complete lookup now uses 120 narrow reads / 84,299 bytes
rather than 110 wider reads / 101,780 bytes.

Overlapping or non-monotone bounds are valid, and `boundary_order` is never read (RG-6). Corrupt
data in the pages a request decodes is `invalid_parquet` (CHECK-5).

**What it does not certify.** A successful request certifies nothing about rows it did not decode
(CHECK-4). In particular, routing trusts the manifest's part bounds. If they are wrong
(`bounds_mismatch`, which only validation must find), a seek may land in the wrong part. Turn on
`verifyPartBounds` to compare each part's first and last key with the manifest on open.

## What it does not do

- Validation (§8: `structural`, `profile`, `full`, `digest`), including SHA-256 digests. The
  conformance adapter answers `validate` with `unsupported_operation`.
- Writing datasets.
- Projecting nested or repeated columns, or `INT96` (reported as `unsupported_projection`).
- BROTLI, LZO and Hadoop-framed LZ4 compression, `BYTE_STREAM_SPLIT` encoding, and modular
  encryption (reported as unsupported).
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

The adapter claims the `reader` target and must pass every case of it; validator targets are not claimed
yet, so the harness skips their cases. Run both commands above for Java verification; the repository
CI workflow does not yet gate Java changes.

The build uses the checked-in Gradle wrapper (Gradle 9.9.0) and a Java 21 toolchain. The unit
tests write their Parquet files to temporary directories, using a small test-only writer. They also
read the shared fixtures under `conformance/datasets`.

| Path | What it is |
|---|---|
| `seekparquet/` | The library: public API in `io.varve.seekparquet`, storage interfaces in `io.varve.seekparquet.storage`. Everything else is package-private. |
| `conformance/` | The conformance adapter (`io.varve.seekparquet.conformance.Adapter`), not part of the library. |
