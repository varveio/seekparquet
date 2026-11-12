# Documentation

Start with [concepts](concepts.md) for the terms every other page uses.

## Learn

- [Quickstart](quickstart.md): read a checked-in dataset with Go in five minutes.
- [Concepts](concepts.md): datasets, keys, queries, cursors, how a read finds rows, what is checked, errors.
- [Related work](related-work.md): how SeekParquet compares with other approaches.

## Guides

- [Reading](guides/reading.md): open, query, project, scan, plan, and classify errors.
- [Pagination](guides/pagination.md): row and prefix cursors, tokens, and service envelopes.
- [Operating](guides/operating.md): limits, memory, file handles, verification, concurrency, and list indexes.
- [Storage Sources](guides/storage-sources.md): contracts, capabilities, identity, cancellation, and storage observation.
- [Remote I/O](guides/remote-io.md): logical reads, fake-store counts, speculation, and measurement.

- [Counting and aggregating by prefix](guides/counting.md): count listed row spans without I/O, select rows, and sum a column.
- [Producing datasets](guides/producing.md): the recipe, the Go writer, geometry, pyarrow.
- [Validating datasets](guides/validating.md): levels, the `seekparquet` command, the API.
- [Troubleshooting](guides/troubleshooting.md): every refusal, unsupported and limit code, and what to do.

## Reference

- [Limits and caches](reference/limits.md) (generated)
- [Read statistics](reference/read-stats.md) (generated)
- [Refusal and unsupported codes](reference/refusal-codes.md) (generated)
- [Go and Java names](reference/parity.md) (generated)
- [Compatibility and stability](reference/compatibility.md)
- Go API: the package documentation in [`go/`](../go/); Java API: the Javadoc of [`java/`](../java/).

## Specification and conformance

- [Specification](../spec/format-1.md), the only normative text, and the [implementer's guide](../spec/IMPLEMENTING.md).
- [Conformance suite](../conformance/README.md): run it against an implementation.

## Background

- [Workload fit](background/workload-fit.md): candidate workloads and alternatives to evaluate.

## Contributors

- [How the documentation works](dev/docs.md): generated pages, snippets, checks and style.
- [Benchmark recipes](dev/benchmarks.md) and the [local receipt](dev/receipts/2026-11-11-local-workloads.md).
- [Indexed-access evidence](dev/indexed-access.md): regression scope and qualification gaps.
