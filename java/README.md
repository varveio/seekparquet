# SeekParquet for Java

A reader for [SeekParquet](../spec/format-1.md), a sorted-key profile for Apache Parquet. Given a
dataset (a `manifest.json` and the Parquet parts it lists), it answers range reads, prefix reads and
delimited listings with projected columns. To do so it reads the manifest, the footers, a page index
and a few pages, with no query engine and no catalog.

- **Format:** format 1, draft revision 1 (written on disk as `format_version` 0, `draft_revision` 1).
- **JDK:** 21 or later.
- **Coordinates:** `io.varve:seekparquet` (not yet published), package `io.varve.seekparquet`.
- **Parquet library:** Apache parquet-java 1.18.1 (`parquet-format-structures` and
  `parquet-column`) without Hadoop. See [QUALIFICATION.md](QUALIFICATION.md) for why, and for the
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
apart. Floating-point values carry their exact bits. Projecting `key` returns the key's bytes.

### Options, limits and cancellation

```java
OpenOptions options = OpenOptions.DEFAULT
        .withLimits(new Limits(16 << 20, 100_000, 16 << 20, 4 << 20, 64 << 20, 1 << 22))
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
of parts, and footer, page index and page sizes and value counts. A size above a limit fails with
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

The core is storage-agnostic. It reads through two small interfaces in
`io.varve.seekparquet.storage`:

- `DatasetStorage` reads `manifest.json` and opens parts by their manifest path. The reader never
  closes it; it belongs to the caller.
- `PartSource` is one opened part: its actual `size()`, an optional pinned `version()`, and
  positioned `read(position, length, cancellation)` calls. It must be safe for concurrent reads.
  Every read must return bytes of the object version that was opened, or fail with
  `PartChangedException`. The reader closes every source it opened, including on a failed open.

`openPart` receives the size the manifest records. An object-store adapter can use it as a hint,
for example to fetch the footer with a single suffix-range request. It must still report the
object's real size, which the reader compares with the manifest. Caching or request-coalescing
layers are decorators of these interfaces.

`LocalStorage` is the only implementation in this module. It uses positioned reads on one
`FileChannel` per part, confines parts to the dataset root (symbolic links included), and detects a
part file that changes size or is replaced. Object-store adapters belong in separate modules.

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

**Reads (CHECK-2).** Requests route through the manifest's exact part bounds, then binary-search row
groups and pages by their ColumnIndex maxima. Bounds may be shortened, so the search may land early
but never late. A request checks what it reads:

- each row group's key ColumnIndex and OffsetIndex before first use (RG-2 to RG-4); the result is
  cached for the dataset;
- each key page it decodes: the page header matches its OffsetIndex entry; the page holds one
  non-empty key per row (valid UTF-8 for string keys); keys are strictly ascending, within the
  page's bounds, and after every key page the request decoded before it.

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
cd java
./gradlew build                       # compile, javadoc, unit tests
./gradlew :conformance:installDist    # build the adapter launcher
cd ..
python3 conformance/harness/run.py \
    --adapter java/conformance/build/install/seekparquet-conformance/bin/seekparquet-conformance
```

The build uses the checked-in Gradle wrapper (Gradle 8.14.3) and a Java 21 toolchain. The unit
tests write their Parquet files to temporary directories, using a small test-only writer. They also
read the shared fixtures under `conformance/datasets`.

| Path | What it is |
|---|---|
| `seekparquet/` | The library: public API in `io.varve.seekparquet`, storage interfaces in `io.varve.seekparquet.storage`. Everything else is package-private. |
| `conformance/` | The conformance adapter (`io.varve.seekparquet.conformance.Adapter`), not part of the library. |
