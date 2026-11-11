# Workload fit and measurement

SeekParquet targets immutable, ordered inventories: point lookups, short key
ranges, prefix/delimiter listings, and snapshots with additional columns that
ordinary Parquet consumers can scan. These are candidate workloads, not a claim
of measured superiority. A single opaque byte key requires a producer-defined,
order-preserving encoding for composite keys.

Compare the same source data and queries before choosing a deployment:

| Need | Alternative to evaluate | What to verify |
| --- | --- | --- |
| Live listing of the source store | Native object-store LIST | Freshness, pagination semantics, price and latency |
| Analytics and broad scans | Plain sorted Parquet in the intended engine | Actual page-index use in that engine/version |
| Local lookups and frequent updates | SQLite or a key-value store | Update rate, transactions, deployment and concurrency |
| Rich predicates and evolving tables | Iceberg/Delta or an external Parquet index | Snapshot management, index maintenance and engine integration |

The profile can offer little benefit when native listing already supplies the
required result, most queries scan the whole table, or updates dominate. No
cross-system win or novelty claim follows from reader correctness tests. Keep the
specification, readers and conformance together unless a concrete maintenance
problem warrants a repository split.

## Reproduce local measurements

The Go benchmark exercises the real library with files generated into temporary
directories by the existing test writer. It does not modify a supplied dataset or
commit generated files. From the repository root, with the declared Go toolchain:

```sh
GOMAXPROCS=2 go -C go test -run '^$' -bench '^BenchmarkLocalWorkloads$' -benchtime=1x -count=1
SEEKPARQUET_BENCH_PARTS=10,1000 GOMAXPROCS=2 go -C go test -run '^$' -bench '^BenchmarkLocalWorkloads$' -benchtime=3x -count=3
```

The bounded default is 10 parts. Explicitly setting `SEEKPARQUET_BENCH_PARTS`
allows 1 through 100000 parts, including the proposed 100000-part stress case.
That case is optional: generation, disk space, manifest limits, memory and the
per-process file-descriptor limit can prevent completion. Opening reads only the
manifest; each part a read touches keeps one file handle until the dataset
closes. An implementation limit failure is not a format refusal.
Use a disposable environment sized for the chosen count and record its limits.

Each part has 128 unique, sorted keys, 32 rows per row group, a 512-byte page
buffer target, and nullable `size`/`name` values. Point and range queries project
`size`; delimiter listing reads keys only. Point/range/resume target the final
part. The short range returns 100 rows. Listing returns up to 100 common
prefixes. Late resume uses a real cursor from the same request and dataset,
after row 64 of the final part; it does not measure traversal of all earlier
pages or a huge repeated-key group.

`OpenDir` reports elapsed open plus close. `OpenCounted` separately measures
`OpenDataset` plus close over the same directory source wrapped to count actual
`SourceFile.ReadAt` calls and their requested buffer lengths. Opening reads the
manifest and opens no part, so an open is one read whose size grows with the
manifest. Queries exclude open, close and priming from timing and counters.
Each cold query starts with a new dataset, so it includes the first touch of
every part it routes to: opening the part and reading and checking its footer.
Each warm query first executes the identical query once on that dataset, so
the parts it needs are already open. Warm means reader caches,
not guaranteed elimination of data-page reads. Generated data and earlier runs
can already populate the operating-system cache, so neither open is a cold-disk
measurement. Read counts are logical source calls, not physical disk operations,
network requests, transferred bytes or billed requests. Requested bytes can
include overlapping reads. The benchmark checks result cardinality and selected
keys but is not an independent interoperability qualification.

### Recorded smoke measurement

Both commands above were run on 2026-11-11 on Linux amd64, Intel(R) Xeon(R)
Processor @ 2.10GHz (as `/proc/cpuinfo` names it), Go 1.27.4, and
`GOMAXPROCS=2`, in a shared managed workspace. No cache flush or resource
isolation was performed. The table is from the second command: each elapsed
time is the median of its three `-count` runs of three iterations each. Read
counts and requested bytes were the same in every run. These numbers
demonstrate execution and instrumentation; repeat with longer runs before
drawing latency conclusions. The 100000-part case was not run.

| Operation | 10 parts elapsed (ms) | 1000 parts elapsed (ms) | 1000 parts ReadAt calls | 1000 parts requested bytes |
| --- | ---: | ---: | ---: | ---: |
| OpenDir + close | 0.124 | 7.350 | uninstrumented | uninstrumented |
| Counted open + close | 0.089 | 7.606 | 1 | 230178 |
| Cold point | 0.126 | 0.162 | 13 | 4142 |
| Warm point | 0.018 | 0.021 | 2 | 974 |
| Cold range100 | 0.205 | 0.439 | 30 | 8795 |
| Warm range100 | 0.076 | 0.096 | 14 | 6577 |
| Cold delimiter | 0.611 | 8.375 | 700 | 236494 |
| Warm delimiter | 0.082 | 1.160 | 300 | 79094 |
| Cold late resume | 0.122 | 0.289 | 14 | 4784 |
| Warm late resume | 0.021 | 0.025 | 3 | 1616 |

At 1000 parts the open is one read of the 230178-byte manifest and opens no
part. Cold point, range and resume queries
touch one part and make the same number of reads at 10 and 1000 parts. The
delimiter listing returns one common prefix per part, so at 1000 parts it
touches 100 parts and at 10 parts all ten; at both sizes it made 7 reads per
touched part cold and 3 warm (70/30 at 10 parts, 700/300 at 1000).

The open runs occur sequentially, so their times cannot isolate instrumentation
overhead. This harness does not measure retained heap, retained footer/index
bytes, peak descriptors, remote retries, no-OffsetIndex value-header walks or
read-ahead amplification. Add those scenarios and measurements before making
production capacity or remote-cost claims. No object-store adapter ships here;
remote qualification remains open. A future adapter must preserve immutable
object identity across reads/retries and propagate cancellation and storage
errors. Measure actual requests and bytes at that adapter separately from
reader counters. The benchmark does not relax CHECK-1: every part a query
touches is checked on that first touch.

## Decisions before stable adoption

These remain maintainer decisions, not new normative requirements:

- Repeated-key groups cannot cross parts. A huge group can exceed a producer's
  part-size target. Treat targets as best-effort and qualify large groups before
  promising bounded part sizes.
- COMMIT-3 leaves replacement, retention and deletion outside the profile. Reusing
  unaffected immutable parts, rebuilding affected ranges, atomically publishing a
  manifest and retaining old parts for existing snapshots is a possible producer
  architecture. It is not a lifecycle guarantee supplied by the reader.
- Raw cursors require the same query and immutable dataset. A service should
  decide how to bind tokens to snapshot identity and query parameters affecting
  membership/order, and whether authentication is required by its threat model.
  Library cursors do not supply an authenticated service protocol.
- Physical schema compatibility does not prove semantic consistency. Decide
  whether producer `schema_id` governs logical meaning or stronger annotation
  checks are needed, while preserving the physical-value API.
- Go exposes `parquet-go.Value`; Java has its own value classes. Accept or revise
  that dependency coupling before promising API stability. Naming alignment alone
  does not justify semantic changes.
- Draft format version 0/revision 1 is deliberate. Publish an early-adopter
  migration policy before freeze; the current profile implies no mandatory
  rewrite policy.
- Java LocalStorage requires `SecureDirectoryStream`, and Java CI covers Linux.
  Parquet portability does not establish adapter portability on other platforms.
