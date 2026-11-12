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
// first two entries without I/O. RowSpans preserves the input order.
all, err := d.PlanRows(ctx, spans, []string{"size"}, nil)
if err != nil {
	log.Fatal(err)
}
selected, err := all.SelectRows(all.RowSpans()[:2])
if err != nil {
	log.Fatal(err)
}
var sum int64
var scan seekparquet.ReadStats
err = selected.ScanValues(ctx, &scan, func(b *seekparquet.ValueBatch) error {
	column := b.Column(0)
	if !column.Present() || column.Kind() != seekparquet.KindInt64 {
		return fmt.Errorf("size must be an INT64 column")
	}
	for i := range b.Len() {
		if !column.IsNull(i) {
			sum += column.Int64(i)
		}
	}
	return nil
})
if err != nil {
	log.Fatal(err)
}
fmt.Printf("selected size sum: %d; key pages decoded: %d\n", sum, scan.KeyPagesDecoded)
// Output:
// logs/2026/01/: 30 rows, exact=true, reads=0
// logs/2026/02/: 40 rows, exact=true, reads=0
// logs/2026/03/: 30 rows, exact=true, reads=0
// selected size sum: 74822517816; key pages decoded: 0
```
<!-- /snippet -->

**Java:** pending; see the [parity table](../reference/parity.md#planning).

Run the compiled examples from the repository root with `go -C go test -run '^Example_count' .`.
The fixture has three common prefixes under `logs/2026/`; a result limit of four lets traversal establish their ends.
This is fixture-specific: a full page ending on a common prefix can leave that entry's end unknown.
The listing itself reads storage; only the subsequent column-free `PlanRows` and `Count` calls cost zero reads.

`PlanRows` unions overlapping spans, counting each row once in dataset order.
`Count` returns both the number and its exactness; do not present a provisional count as exact.
With no columns, unchecked contributing parts can leave the count provisional.
Planning with columns checks those parts and loads projection metadata, so the resulting count is exact.
Count exactness does not certify unread keys or values.

`RowSpans()` returns one span per input, in input order, before the plan unions overlaps.
`SelectRows` uses the parent plan's projection and performs no I/O.
Its spans must have known boundaries from the same dataset; each nonempty span must fit within the parent's selected rows.
Unknown or foreign boundaries and reversed spans return `ErrInvalidRequest`, as do nonempty spans outside the parent's selected rows.

`ScanValues` yields borrowed, read-only batches in dataset order; each batch stays within a row group, projected pages, and a selected span.
`ValueBatch.Start()` supplies the first row's owned position, and `Len()` supplies the row count.
`Column(c)` uses the projection order returned by `Plan.Columns()`.
Copy retained values with `Column(c).Value(i).Clone()` before the callback returns.
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
fmt.Println("resolved spans:", len(p.RowSpans()))
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
A prefix cursor excludes whole prefixes; it cannot be passed as a `Selection.Cursor`.
The fallback may read key pages to establish boundaries, so it has no zero-read promise.
Its `RowSpans()` can then be reused with `PlanRows` on the same dataset handle.

## Estimate the work

Count-only row planning needs no storage requests or rounds once the positions are available.
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

Keep the dataset open while planning or scanning with its positions.
Positions belong to that handle and cannot be transferred to another handle opened on the same Source.
After `Close`, `Count`, `ProjectedBytes`, `Columns`, `RowSpans`, and `SelectRows` remain usable as in-memory plan operations.
Their results do not reopen storage; scanning a plan or calling `Dataset.PlanRows` then fails with `ErrClosed` for valid arguments.

`ScanValues` follows `Plan.Scan` for cancellation, callback errors, and `Stop`.
Return `Stop` to end successfully; other callback errors are returned.
Read statistics are filled on failure too.
Both scans hold a dataset lease during callbacks.
Do not call `Close` from a callback or wait there for a goroutine calling it; close after the scan returns.
