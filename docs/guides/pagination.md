# Paginate reads and listings

Keep the same query and immutable dataset when carrying a cursor between pages
([PAGE-1](../../spec/format-1.md#page-1), [PAGE-2](../../spec/format-1.md#page-2)).
The examples are Go; in Java, `Query.next(row)` and `ListQuery.next(entry)` build the next page's request,
`Cursor.encode()` and `Cursor.parse(token)` carry it as text, and each reader reads the other's tokens.

## Resume from the last result

Use the final row's or listing entry's `Cursor()`; repeat the query until it returns an empty page.

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

A row cursor identifies a key and its zero-based offset in the key group.
Span bounds select whole key groups, but a result limit can end a page inside one
([RANGE-1](../../spec/format-1.md#range-1), [PAGE-1](../../spec/format-1.md#page-1)).
See [paging a large key group](reading.md#look-up-a-key) for the row loop.
A prefix cursor skips the subtree represented by the common prefix, including its exact row
([LIST-6](../../spec/format-1.md#list-6)). Prefix cursors apply to listings; range reads require row cursors.
Use `ListEntry.Cursor()` rather than constructing a row cursor from `Exact` when advancing past a common prefix.

Not guaranteed: a short page means the query is exhausted, or a cursor checks which dataset produced it.
The cursor contract applies to the same query and dataset ([`Cursor`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Cursor)).

## Encode and parse tokens

Use `Cursor.Encode` and `ParseCursor` to carry a cursor as text.

**Go**

<!-- snippet: go/example_reading_test.go#ExampleCursor_Encode -->
```go
row, err := seekparquet.RowCursor([]byte("logs/a"), 2).Encode()
if err != nil {
	log.Fatal(err)
}
prefix, err := seekparquet.PrefixCursor([]byte("logs/")).Encode()
if err != nil {
	log.Fatal(err)
}
fmt.Println(row)
fmt.Println(prefix)
_, err = seekparquet.ParseCursor("spc1.r.bad=.0")
fmt.Println("invalid request:", errors.Is(err, seekparquet.ErrInvalidRequest))
// Output:
// spc1.r.bG9ncy9h.2
// spc1.p.bG9ncy8
// invalid request: true
```
<!-- /snippet -->

Token version 1 is stable. Its row form is `spc1.r.KEY.OFFSET`; its prefix form is `spc1.p.PREFIX`.
Payload bytes use canonical unpadded base64url, not a UTF-8 interpretation
([cursor token specification](../../spec/cursor-token-1.md)).

**Go**

<!-- snippet: go/example_test.go#ExampleParseCursor -->
```go
token, err := seekparquet.RowCursor([]byte("logs/readme"), 0).Encode()
if err != nil {
	log.Fatal(err)
}
fmt.Println(token)
c, err := seekparquet.ParseCursor(token)
if err != nil {
	log.Fatal(err)
}
fmt.Printf("%s %d\n", c.Key, c.Offset)
// Output:
// spc1.r.bG9ncy9yZWFkbWU.0
// logs/readme 0
```
<!-- /snippet -->

The Go implementation bounds token length with [`MaxCursorToken`](https://pkg.go.dev/github.com/varveio/seekparquet/go#MaxCursorToken).
Overlong or malformed tokens, including unknown versions, report `ErrInvalidRequest`
([`ParseCursor`](https://pkg.go.dev/github.com/varveio/seekparquet/go#ParseCursor)). Reject them as invalid client input, rather than as a refusal of stored data.
An empty token payload still identifies an empty key or prefix; it does not mean the cursor is absent.

## Bind service tokens to a snapshot and query

Wrap the library token in an application envelope before exposing a paginated service.
Include these fields and check them on the next call:

| Field | What to compare |
|---|---|
| Snapshot | Your immutable snapshot identifier; optionally an opaque `Metadata.ManifestIdentity` from `ManifestReader` |
| Manifest digest | A digest of `Metadata.Manifest` when the Source supplies no suitable snapshot identity |
| Query | Operation, span or prefix, delimiter, end bound, and other parameters affecting membership or order |
| Cursor token | The result of `Cursor.Encode` |
| Authentication | A signature or MAC over the envelope when clients must not alter it |

A manifest identity is diagnostic and grants no authority over part bytes ([`Metadata`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Metadata)).
Use an application snapshot whose parts satisfy the immutability obligations
([COMMIT-2](../../spec/format-1.md#commit-2), [CHECK-6](../../spec/format-1.md#check-6)).
If you use a manifest digest, hash the exact manifest bytes and retain the associated parts for the token's lifetime.
The library does not implement signing, expiry, authorization, snapshot retention, or query binding.
Encoding is not authentication.

A syntactically valid cursor applied to another dataset is not a dataset-mismatch check.
It is interpreted against that dataset's keys; results can differ or be empty
([PAGE-1](../../spec/format-1.md#page-1), [`Cursor`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Cursor)).
Reject mismatched envelopes in your service before asking the reader to resume.

## Count the common prefixes of a page

List one entry beyond the page with [`Dataset.List`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.List), then count each common prefix it keeps with [`Dataset.PlanRows`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.PlanRows).

**Go**

<!-- snippet: go/example_generic_test.go#ExampleDataset_List_pages -->
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
// Pages of two entries. Each listing asks for one entry more, to learn
// whether another page follows and to establish the rows of the last
// common prefix it keeps.
const pageSize = 2
q := seekparquet.ListQuery{Prefix: []byte("logs/2026/"), Delimiter: '/', Limit: pageSize + 1}
for {
	entries, err := d.List(ctx, q, nil)
	if err != nil {
		log.Fatal(err)
	}
	more := len(entries) > pageSize
	if more {
		entries = entries[:pageSize]
	}
	for _, entry := range entries {
		_, startKnown := entry.Rows.Start.Ordinal()
		_, endKnown := entry.Rows.End.Ordinal()
		if !startKnown || !endKnown {
			log.Fatal("use a key-range plan for an unknown boundary")
		}
		// Counting known rows reads nothing.
		p, err := d.PlanRows(ctx, []seekparquet.RowSpan{entry.Rows}, nil, nil)
		if err != nil {
			log.Fatal(err)
		}
		n, _ := p.Count()
		fmt.Printf("%s: %d rows\n", entry.Bytes(), n)
	}
	if !more {
		break
	}
	q.Cursor = entries[len(entries)-1].Cursor()
}
// Output:
// logs/2026/01/: 30 rows
// logs/2026/02/: 40 rows
// logs/2026/03/: 30 rows
```
<!-- /snippet -->

**Java:** not available.

The entry beyond the page shows whether another page follows; the cursor of the last kept entry continues the listing.
Listing past the last kept common prefix, when an entry follows it, also establishes the end of its rows.
A count without storage reads needs both boundaries known.
`End` inside a prefix leaves its end unknown, and a cursor inside a prefix starts its rows at the first remaining candidate.
See [resolve unknown rows](counting.md#resolve-unknown-rows) for the fallback, and [`FoldRows`](counting.md#count-and-sum-selections) for other totals.
