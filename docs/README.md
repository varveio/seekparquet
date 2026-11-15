# Documentation

Start with the job you have; [concepts](concepts.md) defines the terms every page uses.

## Read, list and count

- [Quickstart](quickstart.md): read a checked-in dataset with Go in five minutes.
- [Reading](guides/reading.md): open, look up, read ranges and prefixes, list, project, scan, plan, and classify errors.
- [Pagination](guides/pagination.md): row and prefix cursors, tokens, service envelopes, and browse pages.
- [Counting and aggregating](guides/counting.md): reductions by prefix, counts over listed row spans, filters.
- [Operating](guides/operating.md): limits, memory, open parts, verification, the list index, and concurrency.
- [Storage](guides/storage-sources.md): implement a Go `Source` or Java `DatasetStorage` for an object store.
- [Remote I/O](guides/remote-io.md): what a read costs, read-ahead, tuning and measuring.
- The [Go README](../go/README.md) and the [Java README](../java/README.md), which states what Java lacks.

## Produce a dataset

- [Producing datasets](guides/producing.md): the recipe, the Go writer, geometry, and pyarrow.

## Check a dataset or an implementation

- [Validating datasets](guides/validating.md): levels, the `seekparquet` command, the API.
- [Troubleshooting](guides/troubleshooting.md): every refusal, unsupported and limit code, and what to do.
- [Conformance suite](../conformance/README.md): qualify an implementation in any language.

## Reference

- [Limits and caches](reference/limits.md) (generated)
- [Read statistics](reference/read-stats.md) (generated)
- [Refusal and unsupported codes](reference/refusal-codes.md) (generated)
- [Go and Java names](reference/parity.md) (generated)
- [Compatibility and stability](reference/compatibility.md)
- Go API: the package documentation in [`go/`](../go/); Java API: the Javadoc of [`java/`](../java/).

## Specification

- [Specification](../spec/format-1.md), the only normative text, and the [implementer's guide](../spec/IMPLEMENTING.md).
- [Concepts](concepts.md): datasets, keys, queries, cursors, how a read finds rows, what is checked, errors.
- [Related work](related-work.md) and [workload fit](background/workload-fit.md): when to use SeekParquet and
  what to compare it with.

## Contributors

- [How the documentation works](dev/docs.md): generated pages, snippets, checks and style.
- [Benchmark recipes](dev/benchmarks.md) and the [local receipt](dev/receipts/2026-11-11-local-workloads.md).
- [Indexed-access evidence](dev/indexed-access.md): regression scope and qualification gaps.
