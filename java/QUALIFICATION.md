# Parquet library qualification

The Java reader needs public access to four things: the footer as written (including repeated
key-value entries, `column_orders`, `sorting_columns` and `file_path`), the ColumnIndex and
OffsetIndex of a column chunk, individual page headers, and page-level value decoding for data page
versions 1 and 2. It must also run without Hadoop, and its dependencies must stay small enough for
a JVM service that reads datasets from object storage.

**Decision: Apache parquet-java 1.18.1, the latest release on Maven Central (checked 2026-11-08),
through its Hadoop-free modules `parquet-format-structures` and `parquet-column`.** The
maintainers chose parquet-java before qualification began. Hardwood was not evaluated for this
release.

## What was tried

### `parquet-hadoop` (`ParquetFileReader` over `InputFile`)

`ParquetFileReader.open(InputFile, ParquetReadOptions)` with `LocalInputFile` is the documented
Hadoop-free entry point. A probe against a conformance fixture, with Hadoop absent from the runtime
classpath, gave these results:

| Step | Result |
|---|---|
| Compile against `parquet-hadoop` without Hadoop | Fails: `ParquetFileReader` overloads mention `org.apache.hadoop.conf.Configuration`, so `hadoop-client-api` is needed at compile time. |
| `ParquetReadOptions.builder()` | `NoClassDefFoundError: org/apache/hadoop/conf/Configuration` (the builder creates a `HadoopParquetConfiguration`). |
| `ParquetReadOptions.builder(new PlainParquetConfiguration())` | `NoClassDefFoundError: org/apache/hadoop/io/compress/zlib/ZlibCompressor$CompressionLevel` (the builder always creates a Hadoop codec factory, `HadoopCodecs.newFactory`). |

`parquet-hadoop` declares `hadoop-client`, `hadoop-common`, `hadoop-mapreduce-client-core` and
`hadoop-annotations` as `provided`, so they do not show up in the resolved tree, but the read path
cannot run without them. A user would have to add Hadoop themselves; the shaded client alone is
about 50 MB (`hadoop-client-api` 19.6 MB plus `hadoop-client-runtime` 30.4 MB, version 3.4.1).
Working around this with split-package classes in `org.apache.parquet` is ruled out.

`ParquetFileReader` would also be the wrong layer here. It turns the footer's key-value list into a
map, so a duplicated `seekparquet.*` stamp (STAMP-2) cannot be seen, and it reports malformed
indexes and pages as generic exceptions, not as the refusal codes the specification needs.

### `parquet-format-structures` and `parquet-column` (used)

These modules have no Hadoop dependency. Between them they cover everything the reader needs:

| Need | parquet-java API |
|---|---|
| Footer as written | `org.apache.parquet.format.FileMetaData` and friends (Thrift structures), decoded from the footer bytes |
| ColumnIndex, OffsetIndex | `org.apache.parquet.format.ColumnIndex`, `OffsetIndex`, `PageLocation` |
| Page headers | `org.apache.parquet.format.PageHeader`, `DataPageHeader`, `DataPageHeaderV2`, `DictionaryPageHeader` |
| Value decoding | `org.apache.parquet.column.Encoding#getValuesReader`, `getDictionaryBasedValuesReader` and `initDictionary`, `DeltaBinaryPackingValuesReader`, `DeltaLengthByteArrayValuesReader`, `RunLengthBitPackingHybridDecoder` |

The reader does its own I/O through its storage interface. It locates pages from the OffsetIndex,
decompresses them, and hands the decoded streams to parquet-java's value readers. This is the level
the profile works at: a seek reads one page of one row group.

Two choices follow from wanting malformed input to be refused cleanly rather than exhaust memory:

- **Bounded Thrift decoding.** `Util.readPageHeader`, `readColumnIndex` and `readOffsetIndex` use
  Thrift's default 100 MB message limit. A few hostile bytes could therefore make a list
  allocation that large. The reader decodes these structures itself with the Thrift runtime that
  `parquet-format-structures` shades (`shaded.parquet.org.apache.thrift`), with the message, string
  and container limits set to the length of the buffer. These are public classes of that artifact.
  The coupling is to its shading layout, and it is confined to one class (`Thrift.java`).
- **Structure checks before decoding.** parquet-java's decoders allocate from counts declared in the
  stream (a bit-packed run's group count, a delta block's value count, a dictionary's size). Before
  any decoder sees a stream, the reader walks it (`EncodingChecks.java`) and checks those counts
  against what the page can hold.

Decompression does not use parquet-java either, because its codec factory lives in
`parquet-hadoop` and needs Hadoop. GZIP uses `java.util.zip`. SNAPPY, ZSTD and LZ4_RAW use
`io.airlift:aircompressor` 2.0.3: pure Java, a Java 8 baseline, no native code, and the same
version `parquet-hadoop` 1.18.1 depends on. BROTLI, LZO and Hadoop-framed LZ4 are reported as
`unsupported_encoding`.

## Resolved runtime dependency tree

`./gradlew :seekparquet:dependencies --configuration runtimeClasspath`:

```text
+--- org.apache.parquet:parquet-format-structures:1.18.1
|    \--- javax.annotation:javax.annotation-api:1.3.2
+--- org.apache.parquet:parquet-column:1.18.1
|    +--- org.apache.parquet:parquet-common:1.18.1
|    |    +--- org.apache.parquet:parquet-format-structures:1.18.1 (*)
|    |    \--- org.slf4j:slf4j-api:1.7.33
|    +--- org.apache.parquet:parquet-encoding:1.18.1
|    |    +--- org.apache.parquet:parquet-common:1.18.1 (*)
|    |    \--- org.slf4j:slf4j-api:1.7.33
|    +--- org.slf4j:slf4j-api:1.7.33
|    \--- org.locationtech.jts:jts-core:1.20.0
\--- io.airlift:aircompressor:2.0.3
```

| JAR | Size |
|---|---|
| parquet-column-1.18.1 | 3.27 MB |
| jts-core-1.20.0 | 1.24 MB |
| parquet-format-structures-1.18.1 (includes shaded Thrift) | 0.87 MB |
| parquet-encoding-1.18.1 | 0.84 MB |
| aircompressor-2.0.3 | 0.26 MB |
| parquet-common-1.18.1 | 0.13 MB |
| slf4j-api, javax.annotation-api | 0.07 MB |
| **Total, excluding the reader itself** | **about 6.7 MB** |

The tree has no Hadoop, no Jackson, no native libraries and no cloud SDK. `jts-core` (geometry,
used by parquet-column's geospatial statistics) is a real transitive dependency even though this
reader never loads it. It could be excluded, but that is not done here: excluding it would rely on
an internal detail of parquet-column. parquet-java logs through SLF4J 1.7; the library binds no
logger.

## Notes for maintenance

- The value-decoder classes used are public, but parquet-java does not document them as a stable
  API. An upgrade should rerun the conformance suite and the unit tests.
- `parquet-column` and `parquet-encoding` both contain the package
  `org.apache.parquet.column.values.bitpacking`, so they cannot both be on the module path. The library therefore ships no `module-info`. Its non-API
  classes are package-private.
