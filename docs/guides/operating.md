# Operate a reader

Reuse a `Dataset` for an immutable snapshot, and size the process for the parts it touches.
The [limits reference](../reference/limits.md) owns defaults; the
[statistics reference](../reference/read-stats.md) owns counter definitions.

> Pending: WP-J Java examples wait for Java API alignment (`claude/java-api-alignment`).

<!-- java: pending alignment -->

## Keep a dataset open for a snapshot

Share a dataset across goroutines; completed part checks and decoded indexes are shared
([`Dataset`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset)). Close it when the snapshot leaves service.

| Retained state | Lifetime and accounting |
|---|---|
| Open part files and checked footers | Retained until close; size for every successfully checked part touched ([implementation](../../go/reader.go)) |
| Key page indexes and row-group first-key bytes | `Limits.IndexMemory`; an admission failure reports `LimitError` |
| Decoded projected OffsetIndex locations | `Limits.OffsetIndexMemory`; beyond this cache budget, operations load locations without retaining them |
| Raw page index bytes from tail reads | `Limits.PageIndexCacheMemory`; eviction affects subsequent logical reads |
| Storage byte cache | Owned and accounted by your Source, outside reader cache budgets |

See [`Limits`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Limits) for exclusions. There is no aggregate bound on dataset metadata.
Footers and open part files have no eviction policy; index budgets do not bound them.
An `OpenDir` dataset holds one file descriptor per retained part and its root.
`Verify` touches all parts. A broad scan or verification can exhaust descriptors even when each part is small
([part ownership](../../go/reader.go), [local files](../../go/dir.go)).
Size for the largest touched-part set; opening a fresh dataset for the same broad operation does not reduce that set.

## Choose resource limits

Set limits from your producer's geometry, expected result size, and acceptable service concurrency.
These illustrative profiles are exercised against the fixture; they are not universal deployment presets.

**Go**

<!-- snippet: go/example_reading_test.go#ExampleLimits_untrusted -->
```go
// Illustrative admission profiles. Match them to your producer and workload.
profiles := []struct {
	name   string
	limits seekparquet.Limits
}{
	{"local trusted", seekparquet.Limits{}},
	{"remote untrusted", seekparquet.Limits{ManifestBytes: 1 << 20, Parts: 100, FooterBytes: 1 << 20, PageBytes: 4 << 20, PageValues: 1 << 16, PageMemory: 16 << 20, IndexMemory: 16 << 20}},
	{"memory tight", seekparquet.Limits{PageBytes: 1 << 20, PageValues: 1 << 14, PageMemory: 4 << 20, IndexMemory: 4 << 20, OffsetIndexMemory: 1, PageIndexCacheMemory: 1}},
}
for _, profile := range profiles {
	d, err := seekparquet.OpenDir(context.Background(), "../conformance/datasets/multipart", seekparquet.OpenOptions{Limits: profile.limits})
	if err != nil {
		log.Fatal(err)
	}
	_, err = d.Rows(context.Background(), seekparquet.Query{Limit: 1}, nil)
	closeErr := d.Close()
	if err != nil {
		log.Fatal(err)
	}
	if closeErr != nil {
		log.Fatal(closeErr)
	}
	fmt.Println(profile.name, "read succeeds")
}
// Output:
// local trusted read succeeds
// remote untrusted read succeeds
// memory tight read succeeds
```
<!-- /snippet -->

| Profile | What to decide |
|---|---|
| Local trusted | Whether defaults fit your producer; broad reads still retain touched-part metadata and handles |
| Remote untrusted | Accepted manifest, footer, page, and decoded-page sizes; maximum parts and admitted concurrent operations |
| Memory tight | Smaller decoded pages and index retention; small positive cache budgets trade reuse for reads |

Zero fields select defaults; a negative field is an invalid request at open ([`Limits`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Limits)).
A zero cache budget does not disable retention. Small positive values constrain admission.
A `LimitError` reports reader policy or capacity, rather than a format refusal.
See [troubleshooting](troubleshooting.md) before raising a limit.

## Account for per-operation memory

Use `Limits.PageMemory` as a per-page estimate, then allow for scratch, windows, results, and concurrency.
The estimates below come from [`Limits`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Limits) and the
[page accounting implementation](../../go/limits.go).

| Component | Estimate or sizing method |
|---|---|
| Required scalar page | Decoded value slots × value count + decoded byte payload |
| Optional scalar page | Slots for non-null values and row-aligned values + decoded byte payload |
| Key page | Byte payload plus offsets or dictionary indexes; `PageMemory` uses the conservative scalar estimate |
| Projected scratch | High-water capacity retained per projected column until the operation ends; allow for header and dictionary buffers too |
| Fetched, undecoded windows | Per-operation retention, separate from decoded-page memory; see `ReadPolicy` |
| Owning rows and values | Result cardinality × actual key/value sizes; use `Scan` to process borrowed batches |
| Concurrent operations or shards | Sum their independent scratch, decoded pages, windows, and results |

A decoded scalar slot is 24 bytes on 64-bit platforms; the optional-page estimate allows two slots per value
([`Limits`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Limits)). Use this as accounting guidance, not a total-heap formula.
`PageMemory` does not bound an operation's high-water scratch, or the dataset's total memory.
Bound admitted concurrency and owning result sizes in your application too.

Not guaranteed: a process memory ceiling from any single `Limits` field, or lower wire traffic from a smaller reader cache.
Storage caching and request execution have their own accounting.

## Verify before serving reads

Call [`Dataset.Verify`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.Verify) when you want checks across all parts before serving queries.
It checks part metadata, key page indexes, row-group first key pages, and part endpoints
([CHECK-3](../../spec/format-1.md#check-3)). It performs concurrent fetching with checks reported in manifest and row-group order.
Account for all touched footers and files before calling it.
Reads can run concurrently with verification; they share the checked metadata ([`Dataset`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset)).

Not guaranteed: verification of every row or digest. Use [validation](validating.md) for those tasks
([CHECK-4](../../spec/format-1.md#check-4), [PART-5](../../spec/format-1.md#part-5)).

## Build a list index for wide listings

Call `BuildListIndex` when slash-delimited listings justify decoding the dataset's keys to retain prefix-run anchors.

**Go**

<!-- snippet: go/example_reading_test.go#ExampleDataset_BuildListIndex -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
built, err := d.BuildListIndex(ctx, seekparquet.ListIndexOptions{MaxBytes: 1 << 20, Workers: 1, MaxChangesPerPage: 8})
if err != nil {
	log.Fatal(err)
}
fmt.Println("has index:", built.HasIndex)
fmt.Println("within cache budget:", built.Bytes <= built.Options.MaxBytes)
entries, err := d.List(ctx, seekparquet.ListQuery{Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: 4}, nil)
if err != nil {
	log.Fatal(err)
}
fmt.Println("entries:", len(entries))
// Output:
// has index: true
// within cache budget: true
// entries: 3
```
<!-- /snippet -->

The build is optional and separate from open. `MaxBytes` bounds retained list-index storage;
`Workers` bounds concurrently decoded row groups; `MaxChangesPerPage` omits boundaries from denser pages.
The accounting formula lives on [`ListIndexOptions`](https://pkg.go.dev/github.com/varveio/seekparquet/go#ListIndexOptions).
Inspect `HasIndex`, `BudgetExhausted`, `GroupsSkipped`, `GroupsFailed`, and `PartsFailed` after the build
([`ListIndexStats`](https://pkg.go.dev/github.com/varveio/seekparquet/go#ListIndexStats)). Whole row groups are admitted in dataset order until the next exceeds the cache budget.
Data refusals and unsupported features omit affected row groups; storage failures and resource limits fail the attempt.
Cancellation discards the attempt. See [`Dataset.BuildListIndex`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.BuildListIndex) for retry and publication semantics.

A nonempty successful build is reused by later calls. Compare `Options` and `OptionsMismatch` before assuming a different configuration took effect.
Listings still use their ordinary checks and fall back when an index entry cannot answer safely.
Export `ListIndexSkips` alongside `ListIndexDelimiter`, `ListIndexMissingGroup`, `ListIndexDensePage`, and `ListIndexUnsafeRun` to explain declines.

Not guaranteed: an index for every row group, a decode-free listing, or reduced latency for your workload.
Benchmark build cost and steady-state listings separately.

## Coordinate concurrency, cancellation, and close

Give concurrent operations their own contexts, `ReadStats`, scratch in callbacks, and result destinations.
`Dataset` is concurrency-safe, but a `ReadStats` is not ([`ReadStats`](https://pkg.go.dev/github.com/varveio/seekparquet/go#ReadStats)).
Your Source governs physical scheduling, connection limits, retries, and storage-specific observation.
The reader's internal scheduling is not a caller-configured concurrency limit.

`RangeReader` receives the operation context. Scalar reads use an independent `ContextBinder` view when available.
Without it, a blocked `ReadAt` delays cancellation until it returns ([`ContextBinder`](https://pkg.go.dev/github.com/varveio/seekparquet/go#ContextBinder)).
Close cancels operations, joins active reads and callbacks, and closes part files; later reads report `ErrClosed`
([`Dataset.Close`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.Close)). The caller still owns the Source and closes it after its datasets.

> Do not close the dataset inside a scan callback, or wait there for another goroutine to close it.
> Close waits for that callback to return ([`Dataset.Scan`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.Scan)).

## Monitor reader work and storage work separately

Export operation duration, errors by class/code, `PartsOpened`, `SourceReads`, `SourceBytes`,
`KeyPagesDecoded`, and `ValuePagesDecoded`. Add `IndexLoads`, `MetadataHitBytes`, and `RequestMemoHitBytes` when investigating reuse.
Use `ReadAheadUnused` and `ReadAheadUnusedBytes` to assess speculative listing reads.
Record labels with bounded cardinality; avoid putting arbitrary keys into metric labels.

Instrument physical requests, wire bytes, retries, queueing, and byte-cache hits in your Source.
A logical batch count does not establish a physical request count.
See [remote I/O](remote-io.md) for a measurement workflow and
[read statistics](../reference/read-stats.md) for exact counter scope.
