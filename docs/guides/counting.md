# Count and aggregate by prefix

Use sorted hierarchical keys to select rows, then aggregate their columns; the keys can represent any ordered hierarchy.
This guide uses the small [multipart fixture](../../conformance/datasets/multipart/manifest.json) shared by the Go documentation examples.
The example treats its `size` column as an INT64 measure and skips null values.
An absent column or another physical type is an error in this example.
See [row positions and spans](../concepts.md#row-positions-and-spans) for the model.

## Reduce named columns

Use [`Dataset.Aggregate`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.Aggregate) for independent results over overlapping selections.

**Go**

<!-- snippet: go/example_generic_test.go#ExampleDataset_Aggregate -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
if err := d.Verify(ctx, nil); err != nil {
	log.Fatal(err)
}
results, err := d.Aggregate(ctx, []seekparquet.Selection{
	{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))},
	{Span: seekparquet.PrefixSpan([]byte("logs/2026/02/"))},
}, []seekparquet.Reduction{
	{Op: seekparquet.CountRows},
	{Op: seekparquet.Sum, Column: "size"},
	{Op: seekparquet.CountNulls, Column: "size"},
}, seekparquet.AggregateOptions{ValueBudgetBytes: 4 << 20}, nil)
if err != nil {
	log.Fatal(err)
}
for _, r := range results {
	if r[0].Status == seekparquet.AggregateExact && r[1].Status == seekparquet.AggregateExact {
		fmt.Printf("rows=%d size=%d nulls=%d\n", r[0].Value.Int64(), r[1].Value.Int64(), r[2].Value.Int64())
	}
}
// Output:
// rows=30 size=31543127898 nulls=0
// rows=40 size=43279389918 nulls=0
```
<!-- /snippet -->

**Java:** not available; the generic aggregation mirror is planned.

[`AggregateRows`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.AggregateRows) accepts established spans without locating their keys again.
Unfiltered `CountRows` over known, checked spans reads no values.
Without `CheckKeys`, unchecked contributing parts leave results `AggregateUnknown`.
With `CheckKeys`, selected keys and contributing parts are checked, and failures return no results.
Counts include every selected row, including repeated keys.
`Sum` and `Max` skip null and absent values; request `CountNulls` and `CountAbsent` to distinguish them.
Empty sums are zero; empty maxima are null.
Signed and unsigned integer sums report `AggregateOverflow` only when the final widened sum does not fit.
Timestamp maxima retain their column's physical value and declared unit.

`ValueBudgetBytes` charges unique compressed projected pages and dictionaries across accepted inputs.
A value of zero permits no value I/O; `-1` permits unlimited value work.
Positive budgets admit each input's value reductions together, leaving free unfiltered counts available.
`AggregateBudgetExceeded` and `AggregateUnpriced` describe skipped value work, without partial totals.
Planning metadata and implicit key checks do not consume the value budget.
Set `CheckKeys: true` when every selected key needs checking, including order across shards.
The result's exactness alone does not check unread keys.

## Filter a union batch

Declare predicate columns in [`BatchFilter`](https://pkg.go.dev/github.com/varveio/seekparquet/go#BatchFilter) and modify its borrowed keep mask.

**Go**

<!-- snippet: go/example_generic_test.go#ExampleBatchFilter -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
if err := d.Verify(ctx, nil); err != nil {
	log.Fatal(err)
}
filter := &seekparquet.BatchFilter{
	Columns: []string{"size"},
	Apply: func(b *seekparquet.ValueBatch, keep []bool) error {
		sizes := b.Column(0)
		for i := range keep {
			keep[i] = sizes.Present() && !sizes.IsNull(i) && sizes.Int64(i) >= 0
		}
		return nil
	},
}
results, err := d.Aggregate(ctx, []seekparquet.Selection{{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))}},
	[]seekparquet.Reduction{{Op: seekparquet.CountRows}},
	seekparquet.AggregateOptions{ValueBudgetBytes: -1, Filter: filter}, nil)
if err != nil {
	log.Fatal(err)
}
fmt.Println("selected rows:", results[0][0].Value.Int64())
// Output:
// selected rows: 30
```
<!-- /snippet -->

**Java:** not available.

The filter sees only its declared columns, in order, and runs once per scanned union batch.
Its columns consume the same value budget as reduction columns.
The keep mask starts true and is reused after the callback returns.
With multiple workers, callbacks must support concurrent calls.
Any callback error, including `Stop`, fails aggregation without results.
Application predicates can select file payload rows without changing listing or exact-row retrieval.

## Fold rows with your own accumulator

Use [`FoldRows`](https://pkg.go.dev/github.com/varveio/seekparquet/go#FoldRows) when the built-in reductions do not fit, for example to sum a column by the value of another.

**Go**

<!-- snippet: go/example_generic_test.go#ExampleFoldRows -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
plan, err := d.Plan(ctx, []seekparquet.Selection{
	{Span: seekparquet.PrefixSpan([]byte("logs/2026/"))},
	{Span: seekparquet.PrefixSpan([]byte("logs/2026/02/"))},
}, nil, nil)
if err != nil {
	log.Fatal(err)
}

// Sum size by the value of another column. Each worker folds into maps
// of its own, so Add takes no lock. Merge copies: from may be merged
// into both overlapping results.
sumBy := seekparquet.Fold[map[string]int64]{
	Columns: []string{"size", "storage_class"},
	New:     func() map[string]int64 { return map[string]int64{} },
	Add: func(acc *map[string]int64, rows *seekparquet.ValueBatch) error {
		sizes, labels := rows.Column(0), rows.Column(1)
		if sizes.Kind() != seekparquet.KindInt64 || labels.Present() && labels.Kind() != seekparquet.KindByteArray {
			return fmt.Errorf("unexpected column types")
		}
		for i := range rows.Len() {
			label := "(null)"
			if labels.Present() && !labels.IsNull(i) {
				label = string(labels.Bytes(i))
			}
			(*acc)[label] += sizes.Int64(i)
		}
		return nil
	},
	Merge: func(into, from *map[string]int64) {
		for label, size := range *from {
			(*into)[label] += size
		}
	},
}
results, err := seekparquet.FoldRows(ctx, d, plan.InputRowSpans(), sumBy,
	seekparquet.FoldOptions{ValueBudgetBytes: -1, Workers: 2, CheckKeys: true}, nil)
if err != nil {
	log.Fatal(err)
}
for _, r := range results {
	fmt.Println(r.Status == seekparquet.AggregateExact, r.Value)
}
// Output:
// true map[(null):25240398209 GLACIER:31427457570 STANDARD:46326142539]
// true map[(null):9347359785 GLACIER:13075496879 STANDARD:20856533254]
```
<!-- /snippet -->

**Java:** not available.

A [`Fold`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Fold) declares its columns and three functions.
`New` makes an empty accumulator, `Add` folds a borrowed run of rows into one, and `Merge` folds one accumulator into another.
The reduction must be associative and commutative.
Each run lies in the same spans among those whose value work was admitted, so `Add` needs no ordinal arithmetic; it can cross a boundary of a span the budget refused.
Each worker keeps its own accumulators, so `Add` takes no lock.
`Merge` runs on the calling goroutine after the scan.
It must copy what it takes, because one accumulator can reach several overlapping results.
Spans, budgets, key checks and statuses are those of `AggregateRows`.
A span with no rows is exact, with an empty accumulator.
Any `Add` error, including `Stop`, fails the fold without results.
`Add` checks its column types itself, with `ColumnView.Present` and `Kind`.

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

Run the examples from the repository root with `go -C go test -run '^Example_count' .`.

The listing itself reads storage; the column-free `PlanRows` and `Count` calls after it read nothing. A row
span's boundaries can be unknown, and unknown never means empty or zero:

- A common prefix's end can be unknown when a full page ends on it, or when the listing's `End` cuts through it.
  This fixture's three prefixes have known ends only because a result limit of four lets the traversal pass
  them.
- A start clipped by a row or prefix cursor is known, and identifies the first remaining candidate row.
- Use the [key-range fallback](#resolve-unknown-rows) for an unknown boundary. No listing promises a
  zero-read count for every entry.

`PlanRows` unions overlapping spans, counting each row once in dataset order. `Count` returns the number and
whether it is exact. A column-free plan's count is inexact if any contributing part was unchecked when the plan
was built, including a part between the listing's established boundaries that no read has opened; checking
more parts later does not update the plan. Planning with columns checks those parts, so its count is exact.
Exactness does not certify unread keys or values.

`InputRowSpans()` returns the original spans in input order, overlaps and empties included; a `Split` shard
has one input per span of the plan's rows in it. `SelectRows` selects spans of the parent's rows with the
parent's projection and no I/O. Its spans must have known boundaries from the same dataset, and each nonempty
span must lie within the parent's selected rows; anything else is `ErrInvalidRequest`. An empty span is
accepted anywhere in the dataset.

`ScanValues` yields borrowed, read-only `ValueBatch`es in dataset order, each within a row group, projected
pages and a normalized span; a batch may cross adjacent input spans, so distribute rows by ordinal as the
example does. `Start()` is the first row's position (independently owned), `Len()` the row count, and
`Column(c)` follows `Plan.Columns()` order. Ordinals are `int64` and batch indexes `int`: compute
`start + int64(i)`, never convert an ordinal to `int`. Copying a batch or a `ColumnView` keeps no values;
`Column(c).Value(i).Clone()` does. Only scan-produced batches carry data. The scan reads no key page for
positioning, but selected parts and pages get their usual checks, and a projected key column is read as a
value column; success does not certify unread key pages.

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

Once both positions are known, a count-only row plan needs no storage requests, though its count may be
inexact. Planning with columns checks the selected parts and loads the projected OffsetIndexes the reader has
not kept, without reading data pages. `SelectRows` reuses the parent's kept projection metadata.
`ScanValues` reads projected pages and dictionaries; a chunk without an OffsetIndex has its page headers
walked from its start. Measure your Source and cache state with [read statistics](../reference/read-stats.md).

The counting test store pins `browse/count-prefix` at **0 physical requests, 0 rounds**, after an unrecorded
listing, and `browse/sum-prefix`, including projected planning and scanning, at **1 physical request,
1 round, 6,153 bytes**, for both its scalar and capable stores ([`TestReadBudgets`](../../go/iobudget_test.go)).
[`TestReadBudgetsGeneric`](../../go/generic_iobudget_test.go) pins the same counts and single-prefix sum for `AggregateRows`; its 64 KiB
budget admits exactly nine sums from more complete prefixes than fit, and adding counts costs no request.
These describe that test's fixture, not arbitrary prefixes.

`ProjectedBytes()` counts unique compressed projected page and dictionary extents, once each for the selected
union. It excludes footers, indexes and implicit key reads, and is not a transfer estimate or a request count.
A chunk without an OffsetIndex, or page locations a `SelectRows` child lacks because they were not
kept within `Limits.OffsetIndexMemory`, make it inexact: check the boolean. Byte caching is the storage
layer's, for `ScanValues` and `Plan.Scan` alike.

## Respect the dataset lifetime

Positions belong to the `Dataset` handle that made them, not to another handle on the same Source, and stay
inspectable after `Close`. `Count`, `ProjectedBytes`, `Columns`, `InputRowSpans` and `SelectRows` work after
close; `Dataset.PlanRows`, scans and `Plan.Split` (which can read key pages for repeated keys) need the open
`Dataset` and fail with `ErrClosed`. `ScanValues` follows `Plan.Scan` for cancellation, callback errors and
`Stop`, fills read statistics on failure too, and holds a dataset lease during callbacks: close after the
scan returns, never from a callback.
