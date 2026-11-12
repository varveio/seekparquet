# Workload fit

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
cross-system win or novelty claim follows from reader correctness tests.

Use [benchmark recipes](../dev/benchmarks.md) to compare your workload. The
[local receipt](../dev/receipts/2026-11-11-local-workloads.md) records one smoke measurement,
with its scope and limitations. [Related work](../related-work.md) describes other approaches.
