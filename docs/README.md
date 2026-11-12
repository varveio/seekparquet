# Documentation

Start with [concepts](concepts.md) for the terms every other page uses.

## Learn

- [Quickstart](quickstart.md): read a checked-in dataset with Go in five minutes.
- [Concepts](concepts.md): datasets, keys, queries, cursors, how a read finds rows, what is checked, errors.
- [Related work](related-work.md): how SeekParquet compares with other approaches.

## Guides

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

## Pages not yet rewritten

These pages predate the current structure. Later waves of the documentation rewrite move their content into
the guides above.

- [Read costs](read-costs.md)
- [Workload fit and measurement](workloads.md)
- [Indexed access evidence](indexed-access.md)

## Contributors

- [How the documentation works](dev/docs.md): generated pages, snippets, checks and style.
