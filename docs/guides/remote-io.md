# Understand remote I/O

Separate what the reader chooses to read from how storage executes it.
The reader routes by metadata, loads page indexes, and fetches demanded and speculative ranges.
Your Source schedules physical requests, retries them, caches bytes, and records wire traffic.

> Pending: WP-L/W9 real-store latency and throughput need a dated SeekParquet-stack receipt naming store, region, geometry, and cache state.

> Pending: WP-L Java examples wait for Java API alignment (`claude/java-api-alignment`).

<!-- java: pending alignment -->

## Measure dependent rounds, requests, and bytes

A round is a dependent step that cannot begin until an earlier result arrives.
Batching independent reads can reduce rounds without reducing bytes.
Coalescing nearby reads can reduce physical requests while transferring more bytes.
Choose the tradeoff using measurements from your Source, rather than a universal per-round latency.
See [storage layering](storage-sources.md) and the [read-path diagram](../concepts.md#how-a-read-finds-its-rows).

Measure these phases separately:

| Phase | Work to include |
|---|---|
| Open | Manifest read and checks; no part opens ([`OpenDataset`](https://pkg.go.dev/github.com/varveio/seekparquet/go#OpenDataset)) |
| Cold query | First touch of selected parts, routing and index loads, demanded pages and speculative reads |
| Warm query | Repeated operation on the same dataset; retained reader metadata can avoid loads, while data pages can require reads and decoding |

A successful query does not validate skipped data ([CHECK-4](../../spec/format-1.md#check-4)).
Reopening per operation repeats first-touch work and changes the experiment.
Reader metadata reuse and storage byte-cache reuse are separate variables.

## Compare counts on the fake store

The following are counts on the fake store, copied from `ioBudgets` and `ioCapableBudgets` in
[`go/iobudget_test.go`](../../go/iobudget_test.go).
They describe the unique-key `ioSmall` geometry in
[`go/iobudget_fixture_test.go`](../../go/iobudget_fixture_test.go).
They are fixture-specific regression assertions, not latency measurements or promises for arbitrary datasets.

The fake uses simulated 10 ms latency plus 128 MiB/s transfer, at most 64 physical requests in flight,
and one physical request per submitted range. It does not coalesce ranges.
Plain storage discovers metadata on open and supports scalar reads;
capable storage adds whole-manifest reads, sized opens, and range batches.
Both use the reader's metadata reuse.
Open is measured separately; query rows exclude it. `page2` resumes page 1's listing on the same dataset.
Indexed listing rows follow a completed index build; their cold label names the test step, not a fresh unindexed dataset.
The `ra0` scenarios use adaptive listing read-ahead over a non-local Source; zero does not disable it.
`ra8` bounds the predictor at eight future key pages.
See the test's `ioScenario` steps for projections, priming, and setup.

| Operation / step | Plain physical requests / rounds | Capable physical requests / rounds | Plain / capable bytes |
|---|---:|---:|---:|
| `open/cold` | 2 / 2 | 1 / 1 | 1,591 / 1,591 |
| `rows-key/mid/cold` | 8 / 7 | 8 / 5 | 2,121,071 / 2,121,731 |
| `rows-key/mid/warm` | 3 / 3 | 3 / 2 | 16,685 / 16,685 |
| `get/mid/cold` | 8 / 7 | 8 / 5 | 2,121,071 / 2,121,731 |
| `get/mid/warm` | 3 / 3 | 3 / 2 | 16,685 / 16,685 |
| `rows-100/2-cols/cold` | 8 / 7 | 8 / 5 | 2,121,071 / 2,121,731 |
| `rows-100/2-cols/warm` | 3 / 3 | 3 / 2 | 16,685 / 16,685 |
| `list-ra0/page1-cold` | 17 / 11 | 17 / 9 | 4,277,090 / 4,281,911 |
| `list-ra0/page2` | 4 / 2 | 4 / 2 | 31,301 / 31,301 |
| `list-ra0/page1-warm` | 6 / 2 | 6 / 2 | 73,570 / 73,570 |
| `list-ra8/page1-cold` | 25 / 21 | 24 / 18 | 4,257,026 / 4,262,536 |
| `list-ra8/page2` | 8 / 6 | 8 / 6 | 31,301 / 31,301 |
| `list-ra8/page1-warm` | 13 / 11 | 13 / 11 | 52,846 / 52,846 |
| `build-list-index` | 28 / 4 | 24 / 3 | 10,389,655 / 10,389,655 |
| `list-indexed-ra0/page1-cold` | 7 / 2 | 7 / 2 | 364,516 / 364,516 |
| `list-indexed-ra0/page2` | 4 / 1 | 4 / 1 | 315,430 / 315,430 |
| `scan-part/cold` | 7 / 3 | 6 / 3 | 2,763,754 / 2,763,754 |
| `scan-part/warm` | 4 / 1 | 4 / 1 | 666,598 / 666,598 |
| `plan-part/plan-cold` | 3 / 2 | 2 / 2 | 2,097,156 / 2,097,156 |
| `plan-part/plan-warm` | 0 / 0 | 0 / 0 | 0 / 0 |
| `plan-part/scan-cold` | 4 / 1 | 4 / 1 | 666,598 / 666,598 |
| `split/split4` | 0 / 0 | 0 / 0 | 0 / 0 |
| `split/shards-concurrent` | 4 / 1 | 4 / 1 | 666,598 / 666,598 |
| `verify/cold` | 28 / 3 | 24 / 2 | 8,477,103 / 8,477,103 |
| `verify/warm` | 0 / 0 | 0 / 0 | 0 / 0 |
| `browse/count-prefix` | 0 / 0 | 0 / 0 | 0 / 0 |
| `browse/sum-prefix` | 1 / 1 | 1 / 1 | 6,153 / 6,153 |

Plan, split, shard scans, and verification have separate rows because their costs occur at separate steps.
These counts show that read-ahead can increase physical requests and rounds for this fixture.
Compare the full tables, including repeated keys, before interpreting one row as a tuning recommendation.

Not guaranteed: these counts on another store, geometry, cache state, reader version, or query.
No real-store latency follows from the simulated timing model.

## Tune logical fetching

Use reader controls to choose logical work; configure physical execution in your Source.

| Mechanism | Control | Scope and tradeoff |
|---|---|---|
| Tail read | `ReadPolicy.MetadataPrefetchBytes` | Fetch part-end metadata together on first touch; oversized footers need additional bytes |
| Retained raw page indexes | `Limits.PageIndexCacheMemory` | Reuse tail bytes for later index loads; eviction trades memory for logical reads |
| Batched index reads | `RangeReader` capability | Submit independent logical ranges together; storage chooses their physical execution |
| Operation memo | Internal reader policy | Reuse admitted complete key-page extents during routing and walking; not a cross-operation byte cache |
| Listing read-ahead | `ListQuery.ReadAhead` | Predict future key pages; unused reads add bytes and work |
| Scan and projection windows | `ReadPolicy` and storage capabilities | Fetch bounded ranges across columns and pages, including value routing probes |
| Suppress optional fetching | `ReadPolicy.DisableSpeculation` | Disable speculative fetching while retaining demanded batching and reuse of available bytes |

Contracts: [`ReadPolicy`](https://pkg.go.dev/github.com/varveio/seekparquet/go#ReadPolicy),
[`ListQuery`](https://pkg.go.dev/github.com/varveio/seekparquet/go#ListQuery),
[`RangeReader`](https://pkg.go.dev/github.com/varveio/seekparquet/go#RangeReader),
[`Limits`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Limits).
The [window implementation](../../go/windows.go) joins outstanding work and releases its buffers at operation completion.
See [operating](operating.md#account-for-per-operation-memory) for the separate decoded-memory and concurrency costs.

`OpenDir` uses a smaller default tail read and no retained raw page index bytes with zero options.
It reads on demand unless listing read-ahead or an explicit read policy enables windows
([entry-point policy](../../go/dir.go)). See [reference defaults](../reference/limits.md).
`OpenDataset` enables the non-local logical-read path; actual store behavior still belongs to the Source.

Not guaranteed: one logical batch becomes one physical request, or speculation improves latency.
A storage implementation can merge, split, retry, deduplicate, or serve bytes from its cache.

## Diagnose a costly listing

Pass a fresh `ReadStats` to each operation and inspect logical I/O and decoding separately.

**Go**

<!-- snippet: go/example_reading_test.go#ExampleReadStats -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
var stats seekparquet.ReadStats
rows, err := d.Rows(ctx, seekparquet.Query{Limit: 1, Columns: []string{"size"}}, &stats)
if err != nil {
	log.Fatal(err)
}
fmt.Println("rows:", len(rows))
fmt.Println("logical part reads:", stats.SourceReads > 0)
fmt.Println("key pages decoded:", stats.KeyPagesDecoded > 0)
fmt.Println("value pages decoded:", stats.ValuePagesDecoded > 0)
// Output:
// rows: 1
// logical part reads: true
// key pages decoded: true
// value pages decoded: true
```
<!-- /snippet -->

| Observation | Next comparison |
|---|---|
| High `PartsOpened` or `IndexLoads` | Repeat on the same dataset; inspect the part layout and cache admission |
| High `KeyPagesDecoded` | Inspect page bounds, prefix density, and list-index decline counters |
| High `ValuePagesDecoded` | Compare a keys-only listing with projected values; include exact rows |
| High `ReadAheadUnused` or `ReadAheadUnusedBytes` | Compare speculation enabled and disabled using identical queries |
| High `ScalarFallbackReads`, low `SourceBatches` | Check whether original Source files implement `RangeReader` |
| High `MetadataHitBytes` or `RequestMemoHitBytes` | Retained metadata or operation-local reuse is supplying bytes |
| Few logical reads but many physical requests | Inspect storage retries, range splitting, metadata discovery, and cache misses |

For example, compare a cold listing and an identical warm listing.
If `PartsOpened` drops while value decoding remains similar, first-touch metadata work explains part of the cold cost.
If storage reports more physical requests with similar `SourceReads`, investigate retries or splitting there.
Then compare `ReadAheadUnusedBytes` with speculation disabled to assess overfetch.
Do not infer physical requests from `SourceBatches` or `SourceReads`.

The [statistics reference](../reference/read-stats.md) defines each counter and its exclusions.
`SourceReads` and `SourceBytes` exclude manifest I/O.
Statistics are reset per operation and remain available on failure
([`ReadStats`](https://pkg.go.dev/github.com/varveio/seekparquet/go#ReadStats)).
For plan-plus-scan totals, add their completed statistics with `ReadStats.Add`.

## Record a reproducible measurement

Record reader revision, producer options, geometry, key distribution, projection, result limit,
snapshot identity, open/first-touch state, storage cache policy, retries, and admitted concurrency.
Measure logical ranges, requested bytes, repeated decodes, retained metadata, open handles,
physical requests, wire bytes, and elapsed time separately.
`KeyPagesDecoded` counts repeated decoding, not distinct pages; instrument extents when you need distinctness.

Compare the same workload against a suitable baseline. Small synthetic probes expose an access pattern;
they do not establish production throughput or cloud latency.
See [benchmark recipes](../dev/benchmarks.md), the
[local receipt](../dev/receipts/2026-11-11-local-workloads.md), and
[indexed-access evidence](../dev/indexed-access.md) for scope and qualification gaps.
Real-store measurements belong in a dated receipt before they appear as numbers in this guide.
