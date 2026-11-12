# Read a dataset

Use this guide to read keys and projected columns from a local directory or a caller-supplied Source.
[Concepts](../concepts.md) defines the data model; [counting](counting.md) covers row spans and aggregation.

> Pending: WP-G Java examples wait for Java API alignment (`claude/java-api-alignment`).

<!-- java: pending alignment -->

Examples use `context`, `fmt`, `log`, and the import
`seekparquet "github.com/varveio/seekparquet/go"`.
Run them from `go/` with `go test -run Example ./...`.
Paths beginning `../conformance/` refer to checked-in fixtures.
Examples using `exampleDataset` share the small fixture builder in
[`go/example_test.go`](../../go/example_test.go).

## Open a dataset

Pass a local directory to [`OpenDir`](https://pkg.go.dev/github.com/varveio/seekparquet/go#OpenDir); set resource limits through `OpenOptions.Limits` and logical-read policy through `OpenOptions.Read`.

**Go**

<!-- snippet: go/example_reading_test.go#ExampleOpenDir -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
fmt.Println("key:", d.Metadata().Key.Column)
fmt.Println("parts:", len(d.Metadata().Parts))
// Output:
// key: key
// parts: 3
```
<!-- /snippet -->

For other storage, pass your `Source` to [`OpenDataset`](https://pkg.go.dev/github.com/varveio/seekparquet/go#OpenDataset).
The [Source guide](storage-sources.md#open-an-in-memory-dataset) includes a runnable in-memory example.
Open checks the manifest; part checks happen on first touch
([CHECK-1](../../spec/format-1.md#check-1)).
Use [validation](validating.md) to check the dataset beyond the read path.

## Read a key range

Pass an inclusive start and exclusive end in a `Query.Selection.Span`.

**Go**

<!-- snippet: go/example_test.go#ExampleDataset_Rows -->
```go
dir, cleanup := exampleDataset()
defer cleanup()
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
rows, err := d.Rows(ctx, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.Span{Start: []byte("logs/2026/01/b.gz"), End: []byte("photos/dog.jpg")}}, Limit: 10}, nil)
if err != nil {
	log.Fatal(err)
}
for _, row := range rows {
	fmt.Println(string(row.Key))
}
// Output:
// logs/2026/01/b.gz
// logs/2026/02/a.gz
// logs/readme
// photos/cat.jpg
```
<!-- /snippet -->

Set `StartExclusive` to exclude the entire key group at the lower bound
([`Span`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Span)). A nil bound is unbounded; a non-nil empty `End` selects nothing.

**Go**

<!-- snippet: go/example_reading_test.go#ExampleSpan_exclusive -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
q := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.Span{
	Start: []byte("logs/2026/01/f000.gz"), StartExclusive: true,
	End: []byte("logs/2026/01/f003.gz"),
}}, Limit: 10}
rows, err := d.Rows(ctx, q, nil)
if err != nil {
	log.Fatal(err)
}
for _, row := range rows {
	fmt.Println(string(row.Key))
}
// Output:
// logs/2026/01/f001.gz
// logs/2026/01/f002.gz
```
<!-- /snippet -->

## Read a prefix

Use `PrefixSpan` and carry the last row's cursor when paging.

**Go**

<!-- snippet: go/example_test.go#ExamplePrefixSpan -->
```go
dir, cleanup := exampleDataset()
defer cleanup()
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
rng := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.PrefixSpan([]byte("logs/2026/"))}, Limit: 2}
for page := 1; ; page++ {
	rows, err := d.Rows(ctx, rng, nil)
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		fmt.Printf("page %d: %s\n", page, row.Key)
	}
	if len(rows) == 0 {
		break
	}
	rng.Cursor = rows[len(rows)-1].Cursor()
}
// Output:
// page 1: logs/2026/01/a.gz
// page 1: logs/2026/01/b.gz
// page 2: logs/2026/02/a.gz
```
<!-- /snippet -->

A prefix intersects any `Start` and `End` bounds you also supply ([`Span`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Span)).

## Look up a key

Use [`Dataset.Get`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.Get) for a key group that fits `Limits.GetRows`.

**Go**

<!-- snippet: go/example_test.go#ExampleDataset_Get -->
```go
dir, cleanup := exampleDataset()
defer cleanup()
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
rows, err := d.Get(ctx, []byte("photos/dog.jpg"), nil, "size")
if err != nil {
	log.Fatal(err)
}
for _, row := range rows {
	fmt.Println(string(row.Key), row.Values[0].Int64())
}
// Output:
// photos/dog.jpg 1
```
<!-- /snippet -->

`GetRows` bounds row count, not returned bytes. A key with no rows returns an empty result.
For larger key groups, page `KeySpan` with `Rows` after recognizing `LimitGetRows`:

**Go**

<!-- snippet: go/example_reading_test.go#ExampleDataset_Get_largeGroup -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/one-large-group", seekparquet.OpenOptions{Limits: seekparquet.Limits{GetRows: 8}})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
_, err = d.Get(ctx, []byte("only"), nil, "size")
var limit *seekparquet.LimitError
if !errors.As(err, &limit) || limit.Limit != seekparquet.LimitGetRows {
	log.Fatal(err)
}
fmt.Println("lookup exceeds:", limit.Limit)
q := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.KeySpan([]byte("only"))}, Limit: 10, Columns: []string{"size"}}
for {
	rows, err := d.Rows(ctx, q, nil)
	if err != nil {
		log.Fatal(err)
	}
	if len(rows) == 0 {
		break
	}
	fmt.Printf("%d rows, offsets %d..%d\n", len(rows), rows[0].Offset, rows[len(rows)-1].Offset)
	q.Cursor = rows[len(rows)-1].Cursor()
}
// Output:
// lookup exceeds: GetRows
// 10 rows, offsets 0..9
// 10 rows, offsets 10..19
// 4 rows, offsets 20..23
```
<!-- /snippet -->

## List with a delimiter

Pass a byte delimiter to `List`; inspect `IsPrefix` to distinguish common prefixes from row entries.

**Go**

<!-- snippet: go/example_test.go#ExampleDataset_List -->
```go
dir, cleanup := exampleDataset()
defer cleanup()
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
req := seekparquet.ListQuery{Prefix: []byte("logs/"), Delimiter: '/', Limit: 1}
for page := 1; ; page++ {
	entries, err := d.List(ctx, req, nil)
	if err != nil {
		log.Fatal(err)
	}
	if len(entries) == 0 {
		break
	}
	for _, e := range entries {
		if e.IsPrefix() {
			fmt.Printf("page %d: prefix %s\n", page, e.CommonPrefix)
		} else {
			fmt.Printf("page %d: row %s\n", page, e.Row.Key)
		}
	}
	req.Cursor = entries[len(entries)-1].Cursor()
}
// Output:
// page 1: prefix logs/2026/
// page 2: row logs/readme
```
<!-- /snippet -->

A common prefix can also carry `Exact`, a row whose key equals that prefix
([LIST-8](../../spec/format-1.md#list-8)). Projected values for that row accompany the entry:

**Go**

<!-- snippet: go/example_reading_test.go#ExampleListEntry_exact -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/delimiter", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
entries, err := d.List(ctx, seekparquet.ListQuery{Prefix: []byte("a/"), Delimiter: '/', Limit: 10, Columns: []string{"size"}}, nil)
if err != nil {
	log.Fatal(err)
}
for _, entry := range entries {
	if entry.IsPrefix() && entry.Exact != nil {
		fmt.Printf("prefix %s, exact row %s\n", entry.CommonPrefix, entry.Exact.Key)
	}
}
// Output:
// prefix a//, exact row a//
// prefix a/b/, exact row a/b/
```
<!-- /snippet -->

`ListEntry.Rows` describes the candidate rows represented by the entry, subject to the query's bounds and cursor.
Its boundaries can be unknown. Use the [counting guide](counting.md) to resolve and count them.
A common prefix appears when any candidate key below `End` rolls up to it,
even if other keys under it are at or above `End` ([LIST-7](../../spec/format-1.md#list-7)).

## Paginate a listing

Continue from the last entry's `Cursor()` and stop on an empty page.

**Go**

<!-- snippet: go/example_reading_test.go#Example_paginate -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
q := seekparquet.ListQuery{Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: 2}
for {
	entries, err := d.List(ctx, q, nil)
	if err != nil {
		log.Fatal(err)
	}
	if len(entries) == 0 {
		break
	}
	for _, entry := range entries {
		fmt.Println(string(entry.Bytes()))
	}
	// Carry the token through your service response and next request.
	token, err := entries[len(entries)-1].Cursor().Encode()
	if err != nil {
		log.Fatal(err)
	}
	q.Cursor, err = seekparquet.ParseCursor(token)
	if err != nil {
		log.Fatal(err)
	}
}
// Output:
// logs/2026/01/
// logs/2026/02/
// logs/2026/03/
```
<!-- /snippet -->

A short page is not the general termination rule. See [pagination](pagination.md) for row cursors,
prefix cursors, tokens, and binding a token to a snapshot.

## Project columns

List projected names in `Query.Columns`; an empty projection reads keys only.

**Go**

<!-- snippet: go/example_test.go#ExampleQuery_projection -->
```go
dir, cleanup := exampleDataset()
defer cleanup()
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
rows, err := d.Rows(ctx, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.Span{Prefix: []byte("logs/")}}, Limit: 3, Columns: []string{"size", "name", "owner"}}, nil)
if err != nil {
	log.Fatal(err)
}
for _, row := range rows {
	fmt.Print(string(row.Key))
	for i, v := range row.Values {
		switch {
		case v.Absent():
			fmt.Printf(" %s=absent", row.Columns[i])
		case v.IsNull():
			fmt.Printf(" %s=null", row.Columns[i])
		default:
			fmt.Printf(" %s=%v", row.Columns[i], v)
		}
	}
	fmt.Println()
}
// Output:
// logs/2026/01/a.gz size=0 name=name-0 owner=absent
// logs/2026/01/b.gz size=1 name=name-1 owner=absent
// logs/2026/02/a.gz size=2 name=null owner=absent
```
<!-- /snippet -->

Check absent, then null, then the physical value's kind before using a typed accessor
([`Value`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Value)). This example uses the `projection` fixture:

**Go**

<!-- snippet: go/example_reading_test.go#ExampleValue_absent -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/projection", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
rows, err := d.Rows(ctx, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.KeySpan([]byte("row-04"))}, Limit: 1, Columns: []string{"extra", "i32", "req"}}, nil)
if err != nil {
	log.Fatal(err)
}
for i, value := range rows[0].Values {
	switch {
	case value.Absent():
		fmt.Println(rows[0].Columns[i], "absent")
	case value.IsNull():
		fmt.Println(rows[0].Columns[i], "null")
	default:
		fmt.Println(rows[0].Columns[i], value.Kind())
	}
}
// Output:
// extra absent
// i32 null
// req INT64
```
<!-- /snippet -->

An absent column is distinct from a stored null ([COL-2](../../spec/format-1.md#col-2)).
A name containing a dot is a literal top-level name, not a nested field path ([COL-6](../../spec/format-1.md#col-6)).
Nested or repeated projection reports `UnsupportedError` ([`Query`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Query), [`Dataset.Rows`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.Rows)).

## Stream borrowed batches

Use `Scan` for serial callbacks over borrowed batches; a zero result limit scans to the span's end.

**Go**

<!-- snippet: go/example_test.go#ExampleDataset_Scan -->
```go
dir, cleanup := exampleDataset()
defer cleanup()
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, dir, seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
q := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.PrefixSpan([]byte("logs/"))}, Columns: []string{"name"}}
var st seekparquet.ReadStats
err = d.Scan(ctx, q, &st, func(b *seekparquet.Batch) error {
	names := b.Column(0)
	for i := range b.Len() {
		if names.IsNull(i) {
			fmt.Printf("%s: no name\n", b.Key(i))
			return seekparquet.Stop
		}
		fmt.Printf("%s: %s\n", b.Key(i), names.Bytes(i))
	}
	return nil
})
if err != nil {
	log.Fatal(err)
}
fmt.Println("key pages decoded:", st.KeyPagesDecoded)
// Output:
// logs/2026/01/a.gz: name-0
// logs/2026/01/b.gz: name-1
// logs/2026/02/a.gz: no name
// key pages decoded: 1
```
<!-- /snippet -->

Use `ColumnView` to read projected values in place:

**Go**

<!-- snippet: go/example_reading_test.go#ExampleColumnView -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
err = d.Scan(ctx, seekparquet.Query{Limit: 3, Columns: []string{"size"}}, nil, func(b *seekparquet.Batch) error {
	sizes := b.Column(0)
	if !sizes.Present() || sizes.Kind() != seekparquet.KindInt64 {
		return fmt.Errorf("size must be INT64")
	}
	for i := range b.Len() {
		if !sizes.IsNull(i) {
			fmt.Println(string(b.Key(i)), sizes.Int64(i))
		}
	}
	return nil
})
if err != nil {
	log.Fatal(err)
}
// Output:
// README 2052263231
// app/v1/obj-000 572045545
// app/v1/obj-001 2107371739
```
<!-- /snippet -->

> The batch, keys, views, and byte values are borrowed until the callback returns.
> Copy anything you retain; `Batch.Row` and `Batch.Cursor` make owning copies.
> See [`Dataset.Scan`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.Scan) for callback shutdown restrictions.

Not guaranteed: a fixed batch size, delivery of remaining rows after `Stop`, or rollback of callbacks
already delivered before a read error. See [`Batch`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Batch) and [`Dataset.Scan`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.Scan).

## Plan and admit a read

Use `Plan.Count` and `Plan.ProjectedBytes` to apply an application admission policy before scanning projected values.

**Go**

<!-- snippet: go/example_reading_test.go#ExampleDataset_Plan -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
p, err := d.Plan(ctx, []seekparquet.Selection{{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))}}, []string{"size"}, nil)
if err != nil {
	log.Fatal(err)
}
rows, rowsExact := p.Count()
bytes, bytesExact := p.ProjectedBytes()
// These are application admission thresholds, not reader defaults.
if !rowsExact || !bytesExact || rows > 100 || bytes > 1<<20 {
	log.Fatal("read exceeds admission policy or needs a separate estimate")
}
fmt.Println("rows:", rows, "exact:", rowsExact)
fmt.Println("projected bytes exact:", bytesExact)
var scanned int
err = p.Scan(ctx, nil, func(b *seekparquet.Batch) error { scanned += b.Len(); return nil })
if err != nil {
	log.Fatal(err)
}
fmt.Println("scanned:", scanned)
// Output:
// rows: 30 exact: true
// projected bytes exact: true
// scanned: 30
```
<!-- /snippet -->

Check both exactness flags. A count without sufficient checked-part evidence is provisional
([`Plan.Count`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Plan.Count)). Projected bytes omit chunks without usable OffsetIndexes;
they exclude footer, page index, and implicit key-page reads ([`Plan.ProjectedBytes`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Plan.ProjectedBytes)).
Planning itself can perform I/O and decode boundary key pages ([`Dataset.Plan`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.Plan)).

Not guaranteed: total wire bytes, decoded heap usage, or exact counts for unchecked parts.
These estimates do not replace resource limits.
For `PlanRows`, `InputRowSpans`, `SelectRows`, and value-only `ScanValues`, see [counting](counting.md).

## Split work across goroutines

Call `Split`, then scan each returned shard with separate statistics and result storage.

**Go**

<!-- snippet: go/example_reading_test.go#ExamplePlan_Split -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
p, err := d.Plan(ctx, []seekparquet.Selection{{}}, []string{"size"}, nil)
if err != nil {
	log.Fatal(err)
}
shards, err := p.Split(ctx, 4, nil)
if err != nil {
	log.Fatal(err)
}
counts := make([]int, len(shards))
failures := make([]error, len(shards))
stats := make([]seekparquet.ReadStats, len(shards))
var wg sync.WaitGroup
for i, shard := range shards {
	wg.Add(1)
	go func() {
		defer wg.Done()
		failures[i] = shard.Scan(ctx, &stats[i], func(b *seekparquet.Batch) error { counts[i] += b.Len(); return nil })
	}()
}
wg.Wait()
var total int
for i, err := range failures {
	if err != nil {
		log.Fatal(err)
	}
	total += counts[i]
}
fmt.Println("scanned:", total)
// Output:
// scanned: 134
```
<!-- /snippet -->

The example also imports `sync`. Shards share the dataset's metadata.
Consume results by shard index if you need dataset order across concurrent scans
([`Plan.Split`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Plan.Split)).

Not guaranteed: the requested shard count or equal shard sizes. Key groups remain whole at shard boundaries;
splitting repeated keys can read boundary pages ([`Plan.Split`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Plan.Split)).

## Describe columns

Call `Columns` for the part whose schema you need, including its `Logical` annotations.

**Go**

<!-- snippet: go/example_reading_test.go#ExampleDataset_Columns -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/projection", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
columns, err := d.Columns(ctx, 0)
if err != nil {
	log.Fatal(err)
}
for _, column := range columns {
	if column.Key {
		fmt.Println(column.String())
	}
}
// Output:
// key BYTE_ARRAY REQUIRED STRING key
```
<!-- /snippet -->

Physical compatibility across checked parts does not compare logical annotations
([`Dataset.Columns`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.Columns), [COL-5](../../spec/format-1.md#col-5)).
Inspect each part if your application depends on those annotations or on a column being present everywhere.

Physical schema compatibility does not prove semantic consistency.
Before stable adoption, decide whether producer `schema_id` governs logical meaning or stronger annotation checks are needed,
while preserving the physical-value API ([COL-1](../../spec/format-1.md#col-1)).

## Measure a read

Start with `SourceReads`, `KeyPagesDecoded`, and `ValuePagesDecoded` to separate logical I/O from decoding work.

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

See [`ReadStats`](https://pkg.go.dev/github.com/varveio/seekparquet/go#ReadStats) for operation accounting and concurrency rules,
and the [remote I/O guide](remote-io.md#diagnose-a-costly-listing) for interpretation.

Not guaranteed: identical counts across versions, languages, geometries, or storage implementations.
Physical requests, retries, wire bytes, and byte-cache hits belong to your Source's instrumentation.

## Cancel and close

See [operating](operating.md#coordinate-concurrency-cancellation-and-close) to coordinate concurrency, cancellation, and close.

## Classify errors

Use `errors.As` for typed errors and `errors.Is` for sentinels; a refusal is distinct from a storage failure.

**Go**

<!-- snippet: go/example_docs_test.go#Example_errors -->
```go
ctx := context.Background()
// A fixture whose keys decrease inside one page: opening succeeds,
// because the fault is in a page, and the read that decodes it refuses.
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/invalid/key-order-within-page", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
_, err = d.Rows(ctx, seekparquet.Query{Limit: 1000}, nil)

var refusal *seekparquet.RefusalError
var unsupported *seekparquet.UnsupportedError
var limit *seekparquet.LimitError
switch {
case errors.As(err, &refusal):
	// A fault in the data, or a version pair this reader does not
	// support (unsupported_version). The code is stable.
	fmt.Println("refused:", refusal.Code, "in", refusal.Part)
case errors.As(err, &unsupported):
	// A feature this reader does not implement. It says nothing
	// about the validity of the rest of the dataset.
	fmt.Println("unsupported:", unsupported.Code)
case errors.As(err, &limit):
	// Your Limits, or a capacity of the reader; not a fault of the data.
	fmt.Println("limit:", limit.Limit)
case errors.Is(err, seekparquet.ErrInvalidRequest):
	fmt.Println("invalid request:", err)
case err != nil:
	// Storage, cancellation, or a closed Dataset.
	fmt.Println("environment:", err)
}
fmt.Println("code:", seekparquet.RefusalCodeOf(err))
// Output:
// refused: key_order in data/part-00000.parquet
// code: key_order
```
<!-- /snippet -->

The example also imports `errors`. Use [troubleshooting](troubleshooting.md) to turn each code into an action.

## Scan with queued workers

Use [`Plan.ScanParallel`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Plan.ScanParallel) to process borrowed batches concurrently.

**Go**

<!-- snippet: go/example_generic_test.go#ExamplePlan_ScanParallel -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()
plan, err := d.Plan(ctx, []seekparquet.Selection{{Span: seekparquet.PrefixSpan([]byte("logs/2026/01/"))}}, []string{"size"}, nil)
if err != nil {
	log.Fatal(err)
}
var rows atomic.Int64
err = plan.ScanParallel(ctx, 2, nil, func(b *seekparquet.Batch) error {
	rows.Add(int64(b.Len()))
	return nil
})
if err != nil {
	log.Fatal(err)
}
fmt.Println("rows:", rows.Load())
// Output:
// rows: 30
```
<!-- /snippet -->

**Java:** not available; the parallel scheduler mirror is planned.

Callbacks follow key order within a shard; callbacks from different shards are unordered.
The worker count bounds active scans and callbacks.
One worker scans one shard; multiple workers take queued shards as they finish.
Equal-key groups stay inside a shard, and complete scans check key order across shard boundaries.
Borrowed batches follow the lifetime rules of `Plan.Scan`.
Return `Stop` for successful early termination; workers are canceled and joined before return.
Early termination does not check the complete selection.
Other callback errors retain identity, and external cancellation fails the scan.
Statistics include split and scan failures; add the original plan's statistics separately with `ReadStats.Add`.
