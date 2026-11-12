# Count and aggregate by prefix

Use sorted hierarchical keys to select rows, then aggregate their columns; the keys can represent any ordered hierarchy.
This guide uses the small [multipart fixture](../../conformance/datasets/multipart/manifest.json) shared by the Go documentation examples.
The example treats its `size` column as an INT64 measure and skips null values.
An absent column or another physical type is an error in this example.
See [row positions and spans](../concepts.md#row-positions-and-spans) for the model.

## Count entries and sum selected rows

List a prefix with a delimiter, count each known span without storage reads, and sum a column over two entries.

**Go**

<!-- snippet: go/example_docs_test.go#Example_countAndAggregatePrefix -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()

// This fixture has three entries. Let List traverse beyond each prefix
// so that it establishes both row boundaries.
entries, err := d.List(ctx, seekparquet.ListQuery{
	Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: 4,
}, nil)
if err != nil {
	log.Fatal(err)
}
var spans []seekparquet.RowSpan
var counts seekparquet.ReadStats
for _, entry := range entries {
	_, startKnown := entry.Rows.Start.Ordinal()
	_, endKnown := entry.Rows.End.Ordinal()
	if !startKnown || !endKnown {
		log.Fatal("use a key-range plan for an unknown boundary")
	}
	p, err := d.PlanRows(ctx, []seekparquet.RowSpan{entry.Rows}, nil, &counts)
	if err != nil {
		log.Fatal(err)
	}
	n, exact := p.Count()
	fmt.Printf("%s: %d rows, exact=%t, reads=%d\n", entry.Bytes(), n, exact, counts.SourceReads)
	spans = append(spans, entry.Rows)
}

// Load projection metadata once for the listed rows, then select the
// first two entries without I/O. InputRowSpans preserves the input order.
all, err := d.PlanRows(ctx, spans, []string{"size"}, nil)
if err != nil {
	log.Fatal(err)
}
inputs := all.InputRowSpans()[:2]
selected, err := all.SelectRows(inputs)
if err != nil {
	log.Fatal(err)
}
sums := make([]int64, len(inputs))
var scan seekparquet.ReadStats
err = selected.ScanValues(ctx, &scan, func(b *seekparquet.ValueBatch) error {
	column := b.Column(0)
	if !column.Present() || column.Kind() != seekparquet.KindInt64 {
		return fmt.Errorf("size must be an INT64 column")
	}
	start, _ := b.Start().Ordinal()
	for i := range b.Len() {
		if column.IsNull(i) {
			continue
		}
		// Union normalization may join adjacent inputs into one batch.
		// Distribute each row by ordinal, including to overlapping inputs.
		ordinal := start + int64(i)
		for j, span := range inputs {
			lo, _ := span.Start.Ordinal()
			hi, _ := span.End.Ordinal()
			if lo <= ordinal && ordinal < hi {
				sums[j] += column.Int64(i)
			}
		}
	}
	return nil
})
if err != nil {
	log.Fatal(err)
}
fmt.Printf("size sums per input: %v; key pages decoded: %d\n", sums, scan.KeyPagesDecoded)
// Output:
// logs/2026/01/: 30 rows, exact=true, reads=0
// logs/2026/02/: 40 rows, exact=true, reads=0
// logs/2026/03/: 30 rows, exact=true, reads=0
// size sums per input: [31543127898 43279389918]; key pages decoded: 0
```
<!-- /snippet -->

**Java:** pending; see the [parity table](../reference/parity.md#planning).

Run the compiled examples from the repository root with `go -C go test -run '^Example_count' .`.
The fixture has three common prefixes under `logs/2026/`; a result limit of four lets traversal establish their ends.
This is fixture-specific: a full page ending on a common prefix can leave that entry's end unknown.
A listing `End` inside a common prefix's keys can also leave its end unknown, even without a full result page.
Cursor-clipped starts identify the first remaining candidate row; they do not include earlier rows under the prefix.
Both row cursors and prefix cursors can clip a start inside a common prefix, and the current reader establishes that start.
Unknown boundaries must never be treated as empty spans or zero counts; use the [key-range fallback](#resolve-unknown-rows).
The listing itself reads storage; only the subsequent column-free `PlanRows` and `Count` calls cost zero reads.
There is no promise of an exact zero-request count for every listing entry.

`PlanRows` unions overlapping spans, counting each row once in dataset order.
`Count` returns both the number and its exactness; do not present a provisional count as exact.
Even with known positions, a column-free plan's count is inexact if any contributing part was unchecked at plan construction.
This includes a span crossing an unopened part between the listing's established boundaries.
Count exactness is a snapshot; checking more parts later does not update an existing plan's result.
Planning with columns checks those parts and loads projection metadata, so the resulting count is exact.
Count exactness does not certify unread keys or values.

`InputRowSpans()` returns the caller's original spans, including overlaps and empties, in input order, rather than the normalized union.
For a key plan, these are the resolved input selections; a `Split` shard has one input per span of the plan's rows in it.
`SelectRows` uses the parent plan's projection and performs no I/O.
Its spans must have known boundaries from the same dataset; each nonempty span must fit within the parent's selected rows.
Unknown or foreign boundaries and reversed spans return `ErrInvalidRequest`, as do nonempty spans outside the parent's selected rows.

`ScanValues` yields borrowed, read-only batches in dataset order; each batch stays within a row group, projected pages, and a normalized selected span.
A batch may cross adjacent input-span boundaries after union normalization.
For per-input aggregation, distribute each row by ordinal as the example does; overlapping inputs each receive their matching rows.
Only scan-produced batches carry data; a zero `ValueBatch` is empty.
`ValueBatch.Start()` supplies the first row's independently owned position, and `Len()` supplies the row count.
Ordinals and counts are `int64`; batch indexes and lengths are `int` on both 32-bit and 64-bit systems.
Compute `start + int64(i)`; converting a dataset ordinal to `int` can truncate it on 32-bit systems.
`Column(c)` uses the projection order returned by `Plan.Columns()`.
Copying a `ValueBatch` or `ColumnView` does not retain values.
Use `Column(c).Value(i).Clone()` before the callback returns to retain an owning value.
The scan does not decode key pages unless the key column is explicitly projected.

## Resolve unknown rows

Fall back to a key-range plan when either boundary's `Ordinal()` reports `known == false`.

**Go**

<!-- snippet: go/example_docs_test.go#Example_countPrefixUnknownRows -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
entries, err := d.List(ctx, seekparquet.ListQuery{
	Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: 1,
}, nil)
if err != nil {
	log.Fatal(err)
}
entry := entries[0]
_, known := entry.Rows.End.Ordinal()
fmt.Println("end known:", known)

// This listing has no End or Cursor to restrict the prefix's rows.
// Plan resolves its key boundaries; unlike PlanRows without columns,
// this fallback can read storage.
p, err := d.Plan(ctx, []seekparquet.Selection{
	{Span: seekparquet.PrefixSpan(entry.CommonPrefix)},
}, nil, nil)
if err != nil {
	log.Fatal(err)
}
n, exact := p.Count()
fmt.Printf("%s: %d rows, exact=%t\n", entry.Bytes(), n, exact)
fmt.Println("resolved spans:", len(p.InputRowSpans()))
// Output:
// end known: false
// logs/2026/01/: 30 rows, exact=true
// resolved spans: 1
```
<!-- /snippet -->

For a common prefix, use `PrefixSpan(entry.CommonPrefix)` as shown.
For a row entry, `Rows` is already known and selects that single row, including with repeated keys.
`KeySpan(entry.Row.Key)` would select its entire key group instead.
When reproducing a restricted listing's candidates, intersect the prefix span with `ListQuery.End` and preserve any row cursor in the selection.
For a prefix cursor, also raise the span's inclusive `Start` to the byte successor of the cursor's prefix if greater.
If there is no successor, no candidates remain.
This excludes every key beginning with the cursor's prefix, which may clip a returned common prefix when the cursor is arbitrary.
A prefix cursor cannot be passed as a `Selection.Cursor`.
The fallback may read key pages to establish boundaries, so it has no zero-read promise.
Check its count exactness too: resolving boundaries can still leave contributing parts unchecked.
For an exact count, plan with a projected column, or call `Verify` and rebuild the column-free plan.
Its `InputRowSpans()` can then be reused with `PlanRows` on the same dataset handle.

## Estimate the work

Count-only row planning needs no storage requests or rounds once both positions are known, but its count may still be inexact.
Planning with columns checks selected parts and loads projected OffsetIndexes that the reader has not retained.
It does not re-resolve row positions through keys or read projected data pages.
`SelectRows` reuses the parent plan's retained projection metadata without storage I/O.
`ScanValues` reads projected pages and dictionaries; chunks without an OffsetIndex require walking page headers from the chunk's start.
Storage can merge or split logical reads into physical requests, and dependent reads add rounds.
Use [read statistics](../reference/read-stats.md) to measure your Source and cache state.

The counting test store pins `browse/count-prefix` at **0 physical requests, 0 rounds**, after an unrecorded listing.
It pins `browse/sum-prefix`, including projected planning and scanning, at **1 physical request, 1 round, 6,153 bytes**.
Both the scalar and capable store tables assert these numbers in [`TestReadBudgets`](../../go/iobudget_test.go).
They describe that test's fixture and setup, not the small example above or arbitrary prefixes.

`ProjectedBytes()` counts unique compressed projected page and dictionary extents, once each for the selected union.
It excludes footers, indexes, and implicit key reads; it is not a physical transfer estimate or a request count.
Without an OffsetIndex, it omits that chunk and reports `exact == false`.
After `SelectRows`, discarded page locations can also make the estimate inexact, even if the parent estimate was exact.
Only page locations retained within `Limits.OffsetIndexMemory` remain available to price the selection.
Check the boolean before using the byte estimate as a complete projected extent total.

`ScanValues` honors the context's cache mode: `CacheDefault` allows Source-cache lookup and admission under storage policy.
`Plan.Scan` uses `CacheBypass` when the context mode is `CacheDefault`.
Bypass still allows reader-owned metadata reuse.
Choose a mode with `WithCacheMode`; permitted caching does not promise a cache hit.

## Respect the dataset lifetime

Keep the dataset open for `Dataset.PlanRows`, scans, and `Plan.Split`.
`Split` can read key pages for repeated keys and requires the open Dataset even when it needs no reads.
Positions belong to that handle and cannot be transferred to another handle opened on the same Source.
Positions remain inspectable after `Close`.
`Count`, `ProjectedBytes`, `Columns`, `InputRowSpans`, and `SelectRows` remain usable as in-memory plan operations after close.
Their results do not reopen storage; scanning a plan or calling `Dataset.PlanRows` then fails with `ErrClosed` for valid arguments.

`ScanValues` follows `Plan.Scan` for cancellation, callback errors, and `Stop`.
Return `Stop` to end successfully; other callback errors are returned.
Read statistics are filled on failure too.
Both scans hold a dataset lease during callbacks.
Do not call `Close` from a callback or wait there for a goroutine calling it; close after the scan returns.
