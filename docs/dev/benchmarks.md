# Benchmark reader workloads

Use these recipes in a suitably sized environment. Record measurements in a dated
[receipt](receipts/2026-11-11-local-workloads.md), including tested revision and cache state.
The [remote I/O guide](../guides/remote-io.md) explains the counters; [operating](../guides/operating.md)
explains handle and memory retention. These commands are benchmark recipes, not docs-check prerequisites.

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

## Read the local receipt

The [2026-11-11 smoke receipt](receipts/2026-11-11-local-workloads.md) records elapsed times,
logical read counts, geometry, machine details, and measurement exclusions.
It does not establish current reader counts or remote latency.

## Collect indexed-access evidence

Use the [indexed-access evidence page](indexed-access.md) for the selected regression runner,
its JSON output, instrumentation scope, and qualification gaps.
