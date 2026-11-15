# Understand remote I/O

Separate what the reader chooses to read from how storage executes it.
The reader routes by metadata, loads page indexes, and fetches demanded and speculative ranges.
Your Source schedules physical requests, retries them, caches bytes, and records wire traffic.
This guide describes the Go reader; [the Java reader](#the-java-readers-access-path) issues scalar reads
without batching or adaptive read-ahead. It gives no latency numbers: none has been measured on a real store
yet.

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

## Know what a read costs

What the Go reader reads, by step:

| Step | Logical reads |
|---|---|
| Open | the manifest, in one read, whatever the number of parts |
| First touch of a part | a tail read of the part's last `ReadPolicy.MetadataPrefetchBytes` (2 MiB; 64 KiB through `OpenDir`), the whole part when smaller; a 4-byte read of its head when it is larger; one more read of exactly the missing bytes when the footer is larger than the window |
| Index loads | page index bytes the tail read held are kept within `Limits.PageIndexCacheMemory` and serve later loads without a read (`MetadataHitBytes`); a key column chunk's ColumnIndex and OffsetIndex not kept are read together |
| Routing within a part | a binary search over row groups' first keys; the first time a row group is needed, its key page index is loaded and its first key page decoded, and both are kept within `Limits.IndexMemory` |
| Seek to a repeated key or a cursor | starts at the predecessor of the first row group whose first key is at or above it, so earlier rows of the key group are kept; a cursor moves to its row through the OffsetIndex row positions without decoding the rows between |
| Projected values | the value pages of the returned rows; a projected chunk's OffsetIndex is read the first time a request needs it and kept within `Limits.OffsetIndexMemory`; a chunk without one has its page headers walked from its start |
| Listing | resumes after each common prefix at the prefix's successor, skipping the pages wholly under it; the page it lands on and the pages around a prefix's boundaries still decode some keys under it |
| `Verify` | checks parts concurrently, each in two rounds (its end; then its row groups' first key pages and its last key page); a second `Verify` of the same `Dataset` reads nothing |

Within a request, complete key-page extents admitted to a bounded memo are reused (`RequestMemoHitBytes`).
So a cold query costs the manifest plus one tail read per part it touches, then the index loads and pages
above; a warm query on the same `Dataset` skips the tail reads and kept indexes. `OpenDir` with a zero
`ReadPolicy` reads as suits a local file: a 64 KiB window, each part's ranges one at a time, no kept raw
page index bytes and no memo.

### Listing read-ahead and forward routing

Over a non-local Source, a listing predicts the key pages it decodes next and reads them ahead. A zero
`ListQuery.ReadAhead` sizes each prediction toward `Limit`+1 entries; a positive value caps the future key
pages one prediction considers. It predicts one key page per common prefix it will seek past, counts a page
whose bounds are both rows as a quarter of its rows in entries, skips pages the request has already read, and
reads the bytes between two predicted pages of a part, in one row group or across two, when they are at most
4 KiB. It also reads ahead the key page indexes of the row groups it has yet to open. With those indexes at
hand, a forward seek past a common prefix is routed by their bounds, with no first-key probe, when they prove
the seek lands no later than routing would; otherwise routing decides.

Pages read ahead are checked exactly as pages read on demand, so results are the same with or without
read-ahead. Unused reads count in `ReadAheadUnused` and `ReadAheadUnusedBytes`. A zero-policy `OpenDir` reads
on demand unless `ReadAhead` is positive; `ReadPolicy.DisableSpeculation` turns read-ahead off; `/` listings
on a dataset with a [list index](operating.md#build-a-list-index-for-wide-listings) do not read ahead.

### The Java reader's access path

The Java reader reads through scalar `PartSource.read` calls: it has no batched range reads, no fetch windows,
no `ReadPolicy` and no kept raw page index bytes. Its tail read is the last 64 KiB of a part, never more than
`footerBytes` plus the 8-byte trailer, with the same 4-byte head read and extra footer read. It routes by
binary search over checked row-group first keys, and scans index bounds directly when at most eight row groups
remain. It coalesces two index reads only when the gap between them is no larger than the bytes used (and at
most 64 KiB). A cursor inside a large key group jumps to its physical row through the OffsetIndex.

Java listing read-ahead is opt-in and fixed: `ListQuery.Builder.readAhead(n)` (default 0, off; at most 64)
reads up to `n` predicted key pages concurrently on virtual threads, holding at most 1 MiB per slot and
`4 * n` pages. It predicts from the key page index of the row group the listing is in, at most one page per
entry the listing can still return: after a common prefix, the page its successor lands on; after a row, the
next page. It stops at the row group's end. It does not read ahead key page indexes, read the gaps between
pages, or route seeks by read-ahead indexes. On a local file it saves nothing; leave it off there. The
[indexed-access evidence](../dev/indexed-access.md) lists the Java tests that bound its reads.

## Compare counts on the fake store

The following are counts on the fake store asserted by `TestReadBudgets`, from `ioBudgets` and `ioCapableBudgets` in
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
For cold page 1, the fixed `ra8` cap made more physical requests and rounds than adaptive `ra0`:
plain storage used 25 / 21 versus 17 / 11; capable storage used 24 / 18 versus 17 / 9.
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
| Listing read-ahead | `ListQuery.ReadAhead` | Predict the key pages a listing decodes next, one for each common prefix it seeks past, and the key page indexes of the row groups ahead; unused reads add bytes and work |
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

<!-- snippet: go/example_reading_test.go#ExampleReadStats_listing -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/delimiter", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
var stats seekparquet.ReadStats
entries, err := d.List(ctx, seekparquet.ListQuery{Prefix: []byte("a/"), Delimiter: '/', Limit: 3, Columns: []string{"size"}}, &stats)
if err != nil {
	log.Fatal(err)
}
fmt.Println("entries:", len(entries))
fmt.Println("logical part reads:", stats.SourceReads > 0)
fmt.Println("key pages decoded:", stats.KeyPagesDecoded > 0)
fmt.Println("value pages decoded:", stats.ValuePagesDecoded > 0)
// Output:
// entries: 3
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

See [`ReadStats`](https://pkg.go.dev/github.com/varveio/seekparquet/go#ReadStats) and the [statistics reference](../reference/read-stats.md) for reset, failure, and exclusion rules.
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
