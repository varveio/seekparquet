# Count and fold by prefix

Sorted hierarchical keys put the rows of a prefix, or of any key selection, next to each other. Count those
rows, or reduce their columns with a function of your own:

- A **count** plans the rows and reads no values: `Dataset.PlanRows` on the row spans a listing returned, or
  `Dataset.Plan` on key selections, then `Plan.Count`.
- A **fold** reads the columns you name and reduces them with your [`Fold`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Fold):
  [`FoldRows`](https://pkg.go.dev/github.com/varveio/seekparquet/go#FoldRows), one result per span.

The examples read the small [multipart fixture](../../conformance/datasets/multipart/manifest.json). See
[row positions and spans](../concepts.md#row-positions-and-spans) for the model.

## Count the common prefixes of a listing

List with a delimiter, plan each common prefix's row span with no columns, and read its count.

**Go**

<!-- snippet: go/example_docs_test.go#Example_countPrefixes -->
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
}
// Output:
// logs/2026/01/: 30 rows, exact=true, reads=0
// logs/2026/02/: 40 rows, exact=true, reads=0
// logs/2026/03/: 30 rows, exact=true, reads=0
```
<!-- /snippet -->

**Java:** not available; Java has no row positions ([parity](../reference/parity.md#planning)).

The listing reads storage. [`Dataset.PlanRows`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.PlanRows)
with no columns, and `Count`, read nothing. Plan each prefix on its own: a plan of several spans counts their
union, each row once.

`Count` also says whether the count is exact. It is not when a part holding the rows had not been checked
when the plan was built, and a later check does not update the plan. For an exact count, plan with a column,
which checks those parts, or call `Verify` first. An exact count does not certify keys or values.

An exact count comes from row positions and the row counts of checked footers. It decodes no key between a
span's ends, so it does not check their order; [check keys](#check-keys) shows a count that does.

A boundary of a row span can be unknown, and unknown never means empty or zero:

- A listing that reaches its result limit on a common prefix can leave the prefix's end unknown. Listing
  past the prefix establishes its end, unless the listing's `End` cuts into the prefix; here a result limit
  of four passes all three prefixes.
- `PlanRows` refuses an unknown boundary as an invalid request. [Resolve it](#resolve-unknown-rows) with a
  key-range plan.
- A listing's `Cursor` or `End` can cut into a common prefix. Its span then holds only the rows that listing
  covers; [pagination](pagination.md#count-and-fold-the-prefixes-of-a-page) says how to tell.

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

**Java:** `Dataset.plan(selections, columns)` plans key selections; `Plan.count()` returns an `Estimate`.

For a common prefix, plan `PrefixSpan(entry.CommonPrefix)` as shown. For a row entry, `Rows` is known
and selects that one row; `KeySpan(entry.Row.Key)` would select its whole key group.

To reproduce a clipped listing's candidates, intersect the prefix span with `ListQuery.End`, and pass a row
cursor as the `Selection.Cursor`. A prefix cursor cannot be a `Selection.Cursor`: raise the span's inclusive
`Start` to the byte successor of the cursor's prefix instead, when that is greater. Without a successor, no
candidates remain.

`Dataset.Plan` can read key pages to place the boundaries. Check its count's exactness too, since parts
between the boundaries can stay unchecked. `InputRowSpans()` returns the resolved spans in input order,
overlaps and empties included, for `PlanRows` and `FoldRows` on the same dataset handle.

## Fold the values of selected rows

Give `FoldRows` the row spans and a `Fold` that reads its columns; each span gets its own result.

**Go**

<!-- snippet: go/example_generic_test.go#ExampleFold -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()

// A fold sees physical values, whatever their annotation. Check that
// every part stores size as a plain or signed INT64 before summing it.
for part := range d.Metadata().Parts {
	columns, err := d.Columns(ctx, part, nil)
	if err != nil {
		log.Fatal(err)
	}
	i := slices.IndexFunc(columns, func(c seekparquet.Column) bool { return c.Name == "size" })
	if i < 0 || columns[i].Type != seekparquet.KindInt64 {
		log.Fatalf("part %d: size is not an INT64 column", part)
	}
	if l := columns[i].Logical; l.Kind != seekparquet.LogicalNone && (l.Kind != seekparquet.LogicalInt || !l.Signed) {
		log.Fatalf("part %d: size is annotated %s", part, l)
	}
}

plan, err := d.Plan(ctx, []seekparquet.Selection{
	{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))},
	{Span: seekparquet.PrefixSpan([]byte("logs/2026/02/"))},
}, nil, nil)
if err != nil {
	log.Fatal(err)
}

// Count rows, and sum and count the nulls of size. The sum is exact
// however FoldRows groups the rows, so check its range on the result.
type totals struct {
	rows, nulls int64
	size        *big.Int
}
fold := seekparquet.Fold[totals]{
	Columns: []string{"size"},
	New:     func() totals { return totals{size: new(big.Int)} },
	Add: func(acc *totals, rows *seekparquet.ValueBatch) error {
		sizes := rows.Column(0)
		acc.rows += int64(rows.Len())
		var n big.Int
		for i := range rows.Len() {
			if sizes.IsNull(i) {
				acc.nulls++
			} else {
				acc.size.Add(acc.size, n.SetInt64(sizes.Int64(i)))
			}
		}
		return nil
	},
	Merge: func(into, from *totals) {
		into.rows += from.rows
		into.nulls += from.nulls
		into.size.Add(into.size, from.size)
	},
}
results, err := seekparquet.FoldRows(ctx, d, plan.InputRowSpans(), fold,
	seekparquet.FoldOptions{ValueBudgetBytes: 4 << 20, CheckKeys: true}, nil)
if err != nil {
	log.Fatal(err)
}
for _, r := range results {
	switch {
	case r.Status != seekparquet.FoldExact:
		fmt.Println("not exact")
	case !r.Value.size.IsInt64():
		fmt.Println("size overflows int64")
	default:
		fmt.Printf("rows=%d size=%d nulls=%d\n", r.Value.rows, r.Value.size, r.Value.nulls)
	}
}
// Output:
// rows=30 size=31543127898 nulls=0
// rows=40 size=43279389918 nulls=0
```
<!-- /snippet -->

**Java:** not available.

A `Fold` names its `Columns` and three functions:

- `New` returns an empty accumulator; nil means the zero value.
- `Add` folds a borrowed run of rows into an accumulator. Every run lies in the same admitted spans, so
  `Add` needs no ordinal arithmetic. It filters by skipping rows.
- `Merge` folds one accumulator into another, on the calling goroutine after the scan.

The reduction must be associative and commutative. Each worker folds into accumulators of its own, so `Add`
takes no lock. Spans may overlap, and one accumulator can reach several results: `Merge` must copy what it
takes from `from`. A span with no rows is exact, with an empty accumulator. An error from `Add`, `Stop`
included, fails the fold with no results.

The arithmetic is yours:

- `Add` sees physical values. `ColumnView.Kind` is the physical type, and `Int64` returns the stored bits
  whatever the annotation; read an unsigned INT64 with `Uint64`.
- Check each part's annotations with [`Dataset.Columns`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.Columns).
  Parts agree on physical types ([COL-5](../../spec/format-1.md#col-5)), not on annotations. An annotation
  the reader cannot describe is `LogicalUnrecognized`.
- Guard against overflow without breaking associativity. A flag set when a partial sum overflows depends on
  how `FoldRows` groups the rows. Keep the sum exact, as the example does with a `big.Int`, and check its
  range on the result.

## Fold the rows of a listing

Give `FoldRows` the row spans of listed common prefixes; it reads their values by position.

**Go**

<!-- snippet: go/example_docs_test.go#Example_foldPrefixes -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
entries, err := d.List(ctx, seekparquet.ListQuery{
	Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: 3,
}, nil)
if err != nil {
	log.Fatal(err)
}
// The third entry establishes the end of the second.
var spans []seekparquet.RowSpan
for _, entry := range entries[:2] {
	spans = append(spans, entry.Rows)
}

// Sum size. Each run of rows Add receives lies in the same spans, so
// Add needs no ordinal arithmetic.
sum := seekparquet.Fold[int64]{
	Columns: []string{"size"},
	Add: func(acc *int64, rows *seekparquet.ValueBatch) error {
		column := rows.Column(0)
		if !column.Present() || column.Kind() != seekparquet.KindInt64 {
			return fmt.Errorf("size must be an INT64 column")
		}
		for i := range rows.Len() {
			if !column.IsNull(i) {
				*acc += column.Int64(i)
			}
		}
		return nil
	},
	Merge: func(into, from *int64) { *into += *from },
}
var scan seekparquet.ReadStats
results, err := seekparquet.FoldRows(ctx, d, spans, sum, seekparquet.FoldOptions{}, &scan)
if err != nil {
	log.Fatal(err)
}
for i, r := range results {
	if r.Status != seekparquet.FoldExact {
		log.Fatal("the span covers a part no read has checked")
	}
	fmt.Printf("%s: size %d\n", entries[i].Bytes(), r.Value)
}
fmt.Println("key pages decoded:", scan.KeyPagesDecoded)
// Output:
// logs/2026/01/: size 31543127898
// logs/2026/02/: size 43279389918
// key pages decoded: 0
```
<!-- /snippet -->

**Java:** not available.

Without `CheckKeys`, a fold trusts the positions: it reads the projected columns only, and decodes key pages
only when the key column is one of them, as a value column. The parts and pages it reads get their usual
checks. A span that covers a part no read has checked is `FoldUnknown` and admits no work of its own; rows it
shares with an admitted span are still read for that span. The listing checks the parts it reads; `Verify`
or `CheckKeys` checks the rest.

## Value budgets and statuses

`FoldOptions.ValueBudgetBytes` bounds the compressed extents of the projected pages and dictionaries a fold
reads, each counted once however many spans share it. Zero is unlimited; a negative budget is an invalid
request, refused before any read. Planning metadata and the key pages read to check keys are not charged,
and a fold with no columns is not budgeted.

Spans are taken in ascending order of their starts. Each is admitted whole when its value work fits with
that of the spans admitted before it, so no span is folded in part. A refused span stays refused where an
admitted one overlaps it.

| Status | `Value` | What to do |
|---|---|---|
| `FoldExact` | the reduction of every row of the span | use it |
| `FoldUnknown` | zero | the span covers a part no read has checked; call `Verify`, or fold with `CheckKeys` |
| `FoldBudgetExceeded` | zero | the span's value work did not fit; fold it with a larger budget |
| `FoldUnpriced` | zero | a projected chunk has no OffsetIndex, so the extents of its pages are not known; fold it with an unlimited budget |

A refused span's result carries no count. Count it with `PlanRows` and `Count`, which read no values.

## Check keys

Set `FoldOptions.CheckKeys` to check every key a fold selects; a fold with no columns is then a key-checked
count.

**Go**

<!-- snippet: go/example_generic_test.go#ExampleFoldOptions_checkKeys -->
```go
ctx := context.Background()
// A fixture whose keys decrease inside one page.
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/invalid/key-order-within-page", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
plan, err := d.Plan(ctx, []seekparquet.Selection{{}}, nil, nil)
if err != nil {
	log.Fatal(err)
}

// The count comes from checked row counts: it decodes no key.
n, exact := plan.Count()
fmt.Printf("count: %d, exact=%t\n", n, exact)

// A fold with no columns and CheckKeys counts the rows and checks
// every key it selects.
count := seekparquet.Fold[int64]{
	Add:   func(acc *int64, rows *seekparquet.ValueBatch) error { *acc += int64(rows.Len()); return nil },
	Merge: func(into, from *int64) { *into += *from },
}
_, err = seekparquet.FoldRows(ctx, d, plan.InputRowSpans(), count, seekparquet.FoldOptions{CheckKeys: true}, nil)
fmt.Println("checked count:", seekparquet.RefusalCodeOf(err))
// Output:
// count: 12, exact=true
// checked count: key_order
```
<!-- /snippet -->

**Java:** not available.

With `CheckKeys`, `FoldRows` checks every part the spans touch and every selected key, including the order
across the shards its workers scan, and including the keys of spans the budget refused. A fault fails the
fold with no results. Without it, an exact result does not certify the keys of its rows.

## Estimate the work

Once both boundaries are known, a count-only row plan makes no storage request, though its count may be
inexact. Planning with columns checks the selected parts and loads the projected OffsetIndexes the reader has
not kept, without reading data pages. `FoldRows` reads projected pages and dictionaries; a chunk without an
OffsetIndex has its page headers walked from its start. Measure your Source and cache state with
[read statistics](../reference/read-stats.md).

The counting test store pins counting nine listed prefixes at **0 physical requests**, and folding one
prefix's `size` at **1 physical request, 1 round, 6,153 bytes**, for both its scalar and capable stores
([`TestReadBudgetsGeneric`](../../go/generic_iobudget_test.go)). In the same test, a 64 KiB budget given more
complete prefixes than fit admits exactly nine. These describe that test's fixture, not arbitrary prefixes.

`ProjectedBytes()` counts unique compressed projected page and dictionary extents, once each for the selected
union. It excludes footers, page indexes and the key reads a scan makes for its own keys; a projected key
column counts like any other. It is not a transfer estimate or a request count. A chunk without an OffsetIndex
makes it inexact: check the boolean. Byte caching is the storage layer's, for `FoldRows` and `Plan.Scan` alike.

## Respect the dataset lifetime

Positions belong to the `Dataset` handle that made them, not to another handle on the same Source, and stay
inspectable after `Close`. `Count`, `ProjectedBytes`, `Columns` and `InputRowSpans` work after close;
`Dataset.PlanRows`, scans, `FoldRows` and `Plan.Split` (which can read key pages for repeated keys) need the
open `Dataset` and fail with `ErrClosed`. `FoldRows` fills read statistics on failure too, and holds a
dataset lease during `Add`: close after it returns, never from a callback.
