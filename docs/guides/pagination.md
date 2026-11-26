# Paginate reads and listings

Keep the same query and immutable dataset when carrying a cursor between pages
([PAGE-1](../../spec/format-1.md#page-1), [PAGE-2](../../spec/format-1.md#page-2)).
The examples are Go; in Java, `Query.next(row)` and `ListQuery.next(entry)` build the next page's request,
`Cursor.encode()` and `Cursor.parse(token)` carry it as text, and each reader reads the other's tokens.

## Page a listing

List one entry more than the page size, keep the page, and continue from the cursor of the last entry you keep.

**Go**

<!-- snippet: go/example_reading_test.go#Example_paginate -->
```go
ctx := context.Background()
d, err := seekparquet.OpenDir(ctx, "../conformance/datasets/multipart", seekparquet.OpenOptions{})
if err != nil {
	log.Fatal(err)
}
defer d.Close()

// page lists one entry more than size: that entry tells whether
// another page follows. It returns the cursor of the next page, nil
// after the last.
page := func(q seekparquet.ListQuery, size int) ([]seekparquet.ListEntry, *seekparquet.Cursor, error) {
	if size < 1 || size == math.MaxInt {
		return nil, nil, fmt.Errorf("page size %d is out of range", size)
	}
	q.Limit = size + 1
	entries, err := d.List(ctx, q, nil)
	if err != nil || len(entries) <= size {
		return entries, nil, err
	}
	entries = entries[:size]
	return entries, entries[size-1].Cursor(), nil
}

q := seekparquet.ListQuery{Prefix: []byte("logs/2026/"), Delimiter: '/'}
for n := 1; ; n++ {
	entries, next, err := page(q, 2)
	if err != nil {
		log.Fatal(err)
	}
	for _, entry := range entries {
		fmt.Printf("page %d: %s\n", n, entry.Bytes())
	}
	if next == nil {
		break
	}
	// Carry the token through your service response and next request.
	token, err := next.Encode()
	if err != nil {
		log.Fatal(err)
	}
	q.Cursor, err = seekparquet.ParseCursor(token)
	if err != nil {
		log.Fatal(err)
	}
}
// Output:
// page 1: logs/2026/01/
// page 1: logs/2026/02/
// page 2: logs/2026/03/
```
<!-- /snippet -->

**Java:** the same, with `limit(size + 1)` and `ListQuery.next(entry)` on the last entry kept.

The entry beyond the page tells you that another page follows. The Go reader fills a listing to its `Limit`
unless the candidates run out, so a listing of `Limit` entries or fewer is the last page, with no empty page
after it. The format allows a shorter listing ([LIST-4](../../spec/format-1.md#list-4)); a reader of this
repository returns one only at the end.

Listing one entry more also establishes the end of the last common prefix you keep, so every common prefix of
the page has known rows to [count or fold](#count-and-fold-the-prefixes-of-a-page).

`ListQuery.Limit` is an `int`. Refuse a page size of `math.MaxInt` before you add one, as the example does.

Continue from the kept entry's `Cursor()`: a row cursor after a row, a prefix cursor after a common prefix. A
prefix cursor skips the subtree represented by the common prefix, including its exact row
([LIST-6](../../spec/format-1.md#list-6)). Use `ListEntry.Cursor()` rather than building a row cursor from
`Exact` when you continue after a common prefix. The pages, joined, equal the listing read in one call
([PAGE-2](../../spec/format-1.md#page-2)).

## Row cursors

A range read pages the same way: a `Query.Limit` one above the page size, and the `Cursor()` of the last row
you keep. A row cursor identifies a key and its zero-based offset in the key group. Span bounds select whole
key groups, but a result limit can end a page inside one ([RANGE-1](../../spec/format-1.md#range-1),
[PAGE-1](../../spec/format-1.md#page-1)). See [paging a large key group](reading.md#look-up-a-key) for the row
loop. Prefix cursors apply to listings; a range read takes row cursors only.

Not guaranteed: a cursor checks which dataset or query produced it. The cursor contract applies to the same
query and dataset ([`Cursor`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Cursor)).

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

## Count and fold the prefixes of a page

Check that a common prefix's row span is the whole prefix before you present its count or fold as the prefix's
total.

**Go**

<!-- snippet: go/example_reading_test.go#ExampleListEntry_clipped -->
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
// logs/2026/02/ holds 40 rows. A cursor inside it starts its rows
// after the cursor; an End inside it leaves their end unknown.
for _, q := range []seekparquet.ListQuery{
	{Cursor: seekparquet.RowCursor([]byte("logs/2026/02/f009.gz"), 0)},
	{End: []byte("logs/2026/02/f010.gz")},
} {
	q.Prefix, q.Delimiter, q.Limit = []byte("logs/2026/"), '/', 10
	entries, err := d.List(ctx, q, nil)
	if err != nil {
		log.Fatal(err)
	}
	for _, entry := range entries {
		_, startKnown := entry.Rows.Start.Ordinal()
		_, endKnown := entry.Rows.End.Ordinal()
		if !startKnown || !endKnown {
			fmt.Printf("%s: rows not established\n", entry.Bytes())
			continue
		}
		p, err := d.PlanRows(ctx, []seekparquet.RowSpan{entry.Rows}, nil, nil)
		if err != nil {
			log.Fatal(err)
		}
		n, _ := p.Count()
		fmt.Printf("%s: %d rows\n", entry.Bytes(), n)
	}
}
// Output:
// logs/2026/02/: 30 rows
// logs/2026/03/: 30 rows
// logs/2026/01/: 30 rows
// logs/2026/02/: rows not established
```
<!-- /snippet -->

**Java:** not available.

`List` reports the rows of a common prefix as it finds them: `ListEntry.Rows` covers the prefix's candidate
rows in this listing, not the whole prefix
([`ListEntry`](https://pkg.go.dev/github.com/varveio/seekparquet/go#ListEntry)).

- A `Cursor` inside the prefix starts `Rows` at the first candidate after it. Both boundaries can be known, and
  a count of them is the count of the remaining rows only. When the cursor follows every row stored at the
  prefix's own key, `Exact` is nil.
- An `End` inside the prefix leaves `Rows.End` unknown, and `PlanRows` refuses the span.

The pages of one listing do not cut a common prefix: each continues after a whole entry, and every kept prefix
has the rows the listing read in one call reports. A cut comes from an `End` below the prefix's byte
successor, or from a cursor the listing did not return. Show such a prefix's count as the rows listed, or plan
`PrefixSpan(entry.CommonPrefix)` for its total ([resolve unknown rows](counting.md#resolve-unknown-rows)). Fold
the rows of whole prefixes with [`FoldRows`](counting.md#fold-the-rows-of-a-listing).
