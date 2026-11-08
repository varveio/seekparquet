# Related work

SeekParquet does not invent sorted Parquet, page-index seeks, or SSTables. It packages established techniques
as a portable, engine-neutral contract with a shared conformance suite. This page lists the closest work and
how SeekParquet differs, so you can choose the right tool.

## Parquet itself

Parquet's [page index](https://github.com/apache/parquet-format/blob/master/PageIndex.md) (ColumnIndex and
OffsetIndex) and the `sorting_columns` field already allow a reader to navigate a sorted column. They are
optional, they stop at the edge of one file, and nothing guarantees that a file which declares sorting is
sorted. SeekParquet requires them on the key column, defines the dataset across files, and defines what a
reader checks.

## Profiles on Parquet

[GeoParquet](https://github.com/opengeospatial/geoparquet) is the model for a profile: plain Parquet plus
metadata conventions, readable by any Parquet reader, with a specification, example files and validators.
SeekParquet follows the same approach for key order.

## Engines that store sorted Parquet internally

- [GreptimeDB](https://docs.greptime.com/contributor-guide/datanode/storage-engine/) stores immutable Parquet
  SST files ordered by primary key and time, with per-file key ranges in its manifest.
- [NamiDB](https://github.com/namidb/namidb/blob/main/docs/rfc/002-sst-format.md) stores node SSTs as Parquet
  sorted by node id, uses the page index for point lookups, and records key ranges in its manifest.
- [HoraeDB](https://github.com/apache/horaedb) has used Parquet SSTs with page-index pruning.
- [merutable](https://github.com/salesforce-misc/merutable) is an embeddable LSM engine whose SSTs are Parquet
  files, with an Iceberg view for analytics.

These are storage engines. Their files belong to the engine: levels overlap and are merged on read, rows carry
versions or tombstones, and reading the files directly can show duplicates or deleted rows. A SeekParquet
dataset is a snapshot: parts are key-disjoint, keys are unique, every row is live, and an ordinary Parquet
reader that reads every part gets exactly the dataset. There is no engine to run.

## External indexes over Parquet

[Random Access Parquet](https://engineering.atspotify.com/2026/7/indexing-the-data-lake-for-online-point-queries)
(Spotify) keeps an external index that maps lookup keys to files and rows in ordinary Parquet. It works on
existing, unsorted files and supports several lookup keys. SeekParquet needs no separate index to build or
keep in sync, because its index is standard Parquet metadata inside each file, but it requires the data to be
sorted by one key. Prefer an external index for point lookups over data you cannot rewrite.

## SSTables on other containers

Hadoop's `MapFile` (a sorted SequenceFile plus a separate index file) and Avro's `SortedKeyValueFile` (a
sorted data file plus an index file) apply the same idea to row-oriented containers. HBase's
[HFile](https://hbase.apache.org/docs/hfile-format/) keeps its block index inside the file. All three are
row-oriented and are not read directly by today's analytics engines; Parquet stores data by column and is.

## Table formats

Apache Iceberg, Delta Lake and Apache Hudi manage tables: snapshots, schema evolution, deletes, catalogs.
Their sort orders are advisory and their metadata prunes whole files; page-level skipping, where it happens,
is up to the engine reading the Parquet files (Trino, for example, uses Parquet page indexes). They solve a
broader problem, and no table format guarantees key order across files or defines listing semantics.
SeekParquet files are valid Parquet data files, so a table format can reference them, as long as nothing
rewrites them outside the profile.

## What SeekParquet adds

The combination: an engine-neutral profile with standard page indexes, a multi-part dataset committed by a
manifest with exact key bounds, S3-style prefix and delimiter listing, explicit refusal behaviour, and a small
reader. We found no published project combining these, which is not a claim that none exists.
