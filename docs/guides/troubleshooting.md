# Troubleshooting

Find your error by its kind and code. Every failure the reader reports is one of four kinds
([concepts](../concepts.md#four-kinds-of-error)): a refusal of the data, an unsupported feature, a limit, or
an error of the environment. A refusal reports a fault in the data, or a dataset version pair the reader does
not support (`unsupported_version`). An unsupported feature does not certify that the rest of the dataset is
valid. Limits and errors of the environment establish no fault in the data.

## Classify the error

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

**Java**

<!-- snippet: java/seekparquet/src/test/java/io/varve/seekparquet/docs/DocSnippetsTest.java#errors -->
```java
try (Dataset dataset = Dataset.open(dir)) {
    dataset.rows(Query.builder(Span.all()).limit(1000).build());
} catch (RefusalException e) {
    // A fault in the data, or a version pair this reader does not
    // support (unsupported_version). The code is stable.
    out.println("refused: " + e.code().id() + " in " + e.part().orElse("the manifest"));
} catch (UnsupportedFeatureException e) {
    // A feature this reader does not implement. It says nothing
    // about the validity of the rest of the dataset.
    out.println("unsupported: " + e.code().id());
} catch (LimitExceededException e) {
    // Your Limits, or a capacity of the reader; not a fault of the data.
    out.println("limit: " + e.limit());
} catch (StorageException e) {
    // Reading storage failed; says nothing about the data.
    out.println("storage: " + e.getMessage());
}
// IllegalArgumentException: an invalid request.
// IllegalStateException: the dataset is closed, or closed from inside its own request.
// CancellationException also ends a read that close() interrupts.
// CancellationException: the read was cancelled.
```
<!-- /snippet -->

In Go, `RefusalCodeOf(err)` returns the code of a refusal and `""` for anything else. `errors.Is(err,
seekparquet.ErrLimitExceeded)` matches every limit error. In Java, every failure the data, a limit or storage
causes is a `SeekParquetException`, a sealed class whose four subclasses are the four kinds above.

A refusal or unsupported outcome from a part's first-touch checks is retained while the dataset is open: every
later request that touches the part ends the same way ([CHECK-1](../../spec/format-1.md#check-1)). A failure in
page data ends that request; another request that avoids the faulty data may succeed
([CHECK-2](../../spec/format-1.md#check-2)).

## Refusal codes

The code is stable; the error text is not. Each code below gives the usual cause and the fix. The same table
in one place is the [refusal code reference](../reference/refusal-codes.md).

<!-- generated: troubleshooting-refusals -->
### `no_manifest`

The dataset root has no manifest.json. Detected at the latest on open ([LAYOUT-1](../../spec/format-1.md#layout-1)).

- **Usual cause:** The path is not a dataset root, or the producer has not committed the dataset yet: the manifest is the last file written (COMMIT-1).
- **What to do:** Check the path points at the directory or prefix that holds manifest.json. If a producer is still writing, wait for its commit.

### `invalid_manifest`

The manifest is not valid: unparseable JSON, duplicate member names (including inside extensions), a missing, extra or ill-typed member, an extensions member that is not an object, a wrong format name, malformed base64, a part with no rows, or a part path that breaks PART-1. Also part bounds in the manifest that are not ascending and disjoint (PART-4). A missing or malformed key declaration is also invalid_manifest. Detected at the latest on open ([MAN-1](../../spec/format-1.md#man-1), [MAN-2](../../spec/format-1.md#man-2), [MAN-3](../../spec/format-1.md#man-3), [MAN-4](../../spec/format-1.md#man-4), [PART-1](../../spec/format-1.md#part-1), [PART-2](../../spec/format-1.md#part-2), [PART-3](../../spec/format-1.md#part-3), [MAN-6](../../spec/format-1.md#man-6), [MAN-7](../../spec/format-1.md#man-7), [PART-4](../../spec/format-1.md#part-4), [MAN-8](../../spec/format-1.md#man-8), [EXT-4](../../spec/format-1.md#ext-4)).

- **Usual cause:** A hand-written or hand-edited manifest: a missing or misspelled member, unpadded or URL-safe base64 in a key bound, a part path outside the root, part bounds out of order or overlapping.
- **What to do:** Check the manifest against spec/manifest.schema.json, then the rules of §3. The error detail may identify a member or describe a manifest-wide parsing problem.

### `unsupported_version`

The manifest's format_version and draft_revision pair is not one the reader supports. Detected at the latest on open ([MAN-4](../../spec/format-1.md#man-4), [MAN-5](../../spec/format-1.md#man-5), [VER-1](../../spec/format-1.md#ver-1)).

- **Usual cause:** The dataset was written for a different draft revision or format version than this reader supports.
- **What to do:** Use a reader release that supports the dataset's version pair, or rewrite the dataset with a producer of this reader's version.

### `missing_part`

A part the manifest lists does not exist. Detected at the latest on first touch of the part ([LAYOUT-1](../../spec/format-1.md#layout-1), [CHECK-1](../../spec/format-1.md#check-1)).

- **Usual cause:** A part was deleted or never published, or the manifest names a path that differs from the file (case, directory).
- **What to do:** Restore the part, or rewrite the dataset. A producer must make every part durable before it commits the manifest (COMMIT-1).

### `part_mismatch`

A part's size in bytes, or its footer's row count, differs from its manifest entry. Detected at the latest on first touch of the part ([CHECK-1](../../spec/format-1.md#check-1)).

- **Usual cause:** A part was replaced or rewritten after the manifest was committed, or the producer recorded the size or row count before the file was finished.
- **What to do:** Treat the dataset as damaged and rewrite it. Parts are immutable once committed (COMMIT-2); a producer records each part's size and rows from the sealed file.

### `invalid_footer`

A part is shorter than 12 bytes, has invalid magic under CHECK-1 after applying its encryption classification, its Parquet footer cannot be read, its row groups' row counts do not sum to the footer's row count, or a column chunk refers to another file (file_path set). Detected at the latest on first touch of the part ([CHECK-1](../../spec/format-1.md#check-1), [FILE-1](../../spec/format-1.md#file-1)).

- **Usual cause:** A truncated or partially copied part, a file that is not Parquet, or a writer that puts column chunks in other files (file_path).
- **What to do:** Compare the part's size and checksum with the source. Copy parts whole and only after they are complete.

### `stamp_mismatch`

A part's seekparquet.* footer metadata is missing, duplicated, forbidden (a draft_revision stamp on a non-draft file), not canonical, or different from the manifest. Detected at the latest on first touch of the part ([STAMP-1](../../spec/format-1.md#stamp-1), [STAMP-2](../../spec/format-1.md#stamp-2)).

- **Usual cause:** The part lacks the five seekparquet.* footer stamps or disagrees with the manifest. pyarrow drops them when called with store_schema=False.
- **What to do:** Write the stamps into the part's footer key-value metadata (STAMP-1), exactly as the manifest states the version, schema id and key. See the producing guide.

### `invalid_key_column`

A part has no top-level REQUIRED BYTE_ARRAY column named by manifest key.column, the key column carries an annotation other than STRING or UTF8, or column_orders does not give it TYPE_ORDER. Detected at the latest on first touch of the part ([KEY-1](../../spec/format-1.md#key-1), [KEY-5](../../spec/format-1.md#key-5)).

- **Usual cause:** The key column is nullable, not BYTE_ARRAY, carries an annotation other than STRING, or lacks TYPE_ORDER. DuckDB's COPY TO writes the key OPTIONAL.
- **What to do:** Declare the key a non-nullable binary or string column (Parquet REQUIRED BYTE_ARRAY) with column_orders set (KEY-1, KEY-5).

### `inconsistent_column`

A column appears in several parts with different physical types, repetition or nesting. Detected at the latest on first touch of the part ([COL-5](../../spec/format-1.md#col-5)).

- **Usual cause:** Parts written with different schemas: a column changed type, nullability or nesting between parts.
- **What to do:** Write every part with one schema. A part may omit a column (COL-2), but a column it has must match the other parts.

### `missing_sorting_columns`

A non-empty row group does not declare key ascending as its first sorting column. Detected at the latest on first touch of the part ([RG-1](../../spec/format-1.md#rg-1)).

- **Usual cause:** The writer was not told the rows are sorted, so it wrote no sorting_columns, or wrote the key descending or not first.
- **What to do:** Declare the key as the first sorting column, ascending, on every non-empty row group (RG-1). In pyarrow: sorting_columns=[pq.SortingColumn(i)].

### `missing_page_index`

A non-empty row group's key column lacks a ColumnIndex or an OffsetIndex. Detected at the latest on read ([RG-2](../../spec/format-1.md#rg-2), [RG-7](../../spec/format-1.md#rg-7)).

- **Usual cause:** The writer did not write a page index. pyarrow writes none unless called with write_page_index=True; some writers drop the ColumnIndex for very long keys.
- **What to do:** Enable the page index for the key column (RG-2). In pyarrow: write_page_index=True.

### `invalid_page_index`

A key column's page index is inconsistent with itself or with the physical pages: list lengths differ, a null page, an empty list, row positions or locations out of range, an entry that does not locate exactly one data page, a page whose row count differs from its indexed interval, a bound that is not valid UTF-8 where KEY-6 applies, or decoded keys outside the page's bounds. Detected at the latest on read ([RG-3](../../spec/format-1.md#rg-3), [RG-4](../../spec/format-1.md#rg-4), [RG-5](../../spec/format-1.md#rg-5), [RG-7](../../spec/format-1.md#rg-7), [KEY-6](../../spec/format-1.md#key-6)).

- **Usual cause:** A writer that shortens page bounds incorrectly (for example keys beginning with 0xFF bytes), or a page index edited or damaged after writing.
- **What to do:** Rewrite the part with a writer whose bounds hold every key of the page (RG-5). Validate at the profile level to detect invalid key-page bounds; validation stops when it encounters a fault.

### `invalid_key`

An empty key, or a key that is not valid UTF-8 in a key column annotated STRING or UTF8. Detected at the latest on read ([KEY-2](../../spec/format-1.md#key-2), [KEY-6](../../spec/format-1.md#key-6)).

- **Usual cause:** An empty key, or bytes that are not UTF-8 in a key column annotated STRING.
- **What to do:** Drop or encode empty keys before writing (KEY-2). Store keys that are not UTF-8 in an unannotated binary key column (KEY-6).

### `key_order`

Keys decrease within a page or across pages, row groups or parts, or equal keys appear when manifest key.unique is true. Detected at the latest on read ([KEY-3](../../spec/format-1.md#key-3)).

- **Usual cause:** Rows sorted as signed bytes, by a string collation or locale, or by UTF-16 code units; or duplicate keys in a dataset whose manifest declares keys unique.
- **What to do:** Sort keys by unsigned byte order (KEY-3), and declare key.unique false when keys repeat.

### `invalid_parquet`

Corruption in the Parquet data itself: a page that does not decompress or decode, a malformed dictionary, or a value count that does not match the rows. Detected at the latest on validation, full level ([CHECK-5](../../spec/format-1.md#check-5)).

- **Usual cause:** A damaged page: bit rot, a bad copy, or a writer bug in compression or encoding.
- **What to do:** Rewrite or restore the part. Validate at the full level to check all stored columns. A failed run reports the fault encountered, not every damaged page.

### `bounds_mismatch`

A part's actual first or last key differs from its manifest bounds. Detected at the latest on validation, profile level ([PART-3](../../spec/format-1.md#part-3), [PART-4](../../spec/format-1.md#part-4)).

- **Usual cause:** The manifest's min_key or max_key was taken from intended bounds or from shortened page index bounds, not from the part's actual first and last keys.
- **What to do:** Write the manifest's bounds from the first and last keys actually written to each part (PART-3).

### `digest_mismatch`

A part's sha256 differs from the manifest's. Detected at the latest on validation, digest level ([PART-5](../../spec/format-1.md#part-5)).

- **Usual cause:** A part changed after its digest was recorded, or the digest was computed over different bytes.
- **What to do:** Treat the part as damaged and restore or rewrite it. Compute the digest over the sealed file's bytes.
<!-- /generated -->

## Unsupported codes

This reader encountered a feature it cannot handle. That outcome does not certify the dataset's validity.
The feature alone does not block a request that does not need it.

<!-- generated: troubleshooting-unsupported -->
### `unsupported_encoding`

A codec or encoding outside what the reader implements (never one of the ENC-1 baseline; for example BROTLI).

- **Usual cause:** A codec or encoding outside what this reader decodes, such as BROTLI in the Java reader.
- **What to do:** Inspect the error detail to identify the unsupported codec or encoding, then rewrite the affected column using a supported codec and encoding, preferably the ENC-1 baseline. An unprojected value column’s unsupported encoding or codec does not itself block the request.

### `unsupported_projection`

A request to project a nested or repeated column.

- **Usual cause:** The request projects a nested or repeated column.
- **What to do:** Project only top-level, non-repeated columns (COL-3), or read the part with a general Parquet reader.

### `unsupported_encryption`

An encrypted part: PARE magic at either end of a part of at least 12 bytes (CHECK-1), or readable footer metadata declaring encryption (FILE-2).

- **Usual cause:** The part uses Parquet modular encryption.
- **What to do:** Read the part with a Parquet reader that decrypts, or write the dataset unencrypted (FILE-2).
<!-- /generated -->

## Limits

A limit error names the limit, the amount found and the maximum in force: Go `LimitError.Limit`,
`Value` and `Max`; Java `limit()`, `declared()` and `maximum()`. A limit is your policy or a capacity of the
reader, never a fault of the data ([CHECK-5](../../spec/format-1.md#check-5)). Raise a limit only for data
you trust: the limits are what stop a hostile file from making the reader allocate what it declares. Defaults
and meanings are in the [limits reference](../reference/limits.md).

<!-- generated: troubleshooting-limits -->
### `ManifestBytes`

Go `LimitManifestBytes`, Java `LimitName.MANIFEST_BYTES`. [Default and meaning](../reference/limits.md#manifestbytes).

Raise it only for a manifest you trust: it is read whole into memory. A manifest this large usually lists more parts than a dataset should have; consider fewer, larger parts.

### `Parts`

Go `LimitParts`, Java `LimitName.PARTS`. [Default and meaning](../reference/limits.md#parts).

Raise it for a trusted dataset with many parts. Each part costs one first touch and one open file, so also check your file-handle limit.

### `FooterBytes`

Go `LimitFooterBytes`, Java `LimitName.FOOTER_BYTES`. [Default and meaning](../reference/limits.md#footerbytes).

Footers grow with row groups times columns. Raise it for a trusted part, or have the producer write fewer row groups or columns per part.

### `PageIndexBytes`

Go `LimitPageIndexBytes`, Java `LimitName.PAGE_INDEX_BYTES`. [Default and meaning](../reference/limits.md#pageindexbytes).

The key column's page index grows with key pages and key length. Raise it for a trusted part, or have the producer write fewer, larger key pages per row group.

### `MetadataMemory`

Go `LimitMetadataMemory`, Java `LimitName.METADATA_MEMORY`. [Default and meaning](../reference/limits.md#metadatamemory).

Raise it for a trusted part whose footer or page index decodes large. It bounds one structure, not the whole Dataset.

### `PageBytes`

Go `LimitPageBytes`, Java `LimitName.PAGE_BYTES`. [Default and meaning](../reference/limits.md#pagebytes).

A page this large usually holds very large values. Raise it if you trust the data and have the memory; it also raises the default PageMemory.

### `PageValues`

Go `LimitPageValues`, Java `LimitName.PAGE_VALUES`. [Default and meaning](../reference/limits.md#pagevalues).

Raise it if you trust the data; it also raises the default PageMemory. Producers should keep pages near 1,024 rows (GEO-1).

### `PageMemory`

Go `LimitPageMemory`, Java `LimitName.PAGE_MEMORY`. [Default and meaning](../reference/limits.md#pagememory).

The page’s decoding estimate exceeds PageMemory. Raise the limit for trusted data if sufficient memory is available, or rewrite the data with smaller pages. Lowering concurrency reduces aggregate memory use but does not make this page pass the limit.

### `IndexMemory`

Go `LimitIndexMemory`, Java `LimitName.INDEX_MEMORY`. [Default and meaning](../reference/limits.md#indexmemory).

The Dataset keeps every key page index it loads. Raise it, open fewer parts per Dataset, or reopen the Dataset to drop what it retains.

### `GetRows`

Go `LimitGetRows`, Java `LimitName.GET_ROWS`. [Default and meaning](../reference/limits.md#getrows).

The key has more rows than Get returns at once. Page the group with Rows over KeySpan(key) (Java: rows over Span.key(key)), or raise it.

### `ManifestNesting`

Go `LimitManifestNesting`, Java `LimitName.MANIFEST_NESTING`. The reader's capacity: 512 nested JSON objects and arrays, the manifest object counting 1.

Fixed; it cannot be raised. Flatten the manifest's extensions objects.

### `DatasetRows`

Go `LimitDatasetRows`, Java `LimitName.DATASET_ROWS`. The reader's capacity: a dataset row total of 2^63-1.

Fixed; it cannot be raised. Split the dataset.
<!-- /generated -->

## Errors of the environment

| Error | Meaning | What to do |
|---|---|---|
| Go `ErrInvalidRequest`, Java `IllegalArgumentException` | the request is malformed: a result limit that is not positive, a malformed cursor or cursor token, a prefix cursor in a range read ([QUERY-1](../../spec/format-1.md#query-1)); or a negative `Limits` setting, which Go rejects when the dataset is opened and Java when `Limits.Builder.build()` runs | fix the request; nothing was read |
| Go `ErrClosed`, Java `IllegalStateException` | the dataset was already closed | do not use a dataset after `Close` |
| Go `ErrClosed` (wrapping the read's error), Java `CancellationException` | `Close` was called while the read ran, and ended it | close only after the reads you need have returned |
| Java `IllegalStateException` from `close()` | `close()` was called from inside one of the dataset's own requests on the same thread, such as a scan visitor; nothing changed | end the request, then close |
| Go context error, Java `CancellationException` | the read was cancelled or timed out | retry with a longer deadline if needed |
| Go: the storage's error, wrapped; Java `StorageException` | reading storage failed | retry; this says nothing about the data |
| Go: an error from `OpenDir` naming the path | the directory does not exist or cannot be opened; a directory without `manifest.json` is the refusal `no_manifest` instead | check the path |
| Java `StorageException` caused by `storage.PartChangedException` | a part's stored object changed while it was open ([CHECK-6](../../spec/format-1.md#check-6)) | reopen the dataset; parts must not change after commit |

## Symptoms

**`Close` never returns (Go), or `close()` throws `IllegalStateException` (Java).** Go `Close` waits for every
read in progress, so calling it from the scan's own callback deadlocks. Java `close()` rejects a call from
inside one of the dataset's own requests on the same thread. In either language, a callback must not wait for
another thread that is closing the dataset. End the scan by returning Go `Stop` or Java `false`, or by
cancelling, then close.

**"Too many open files", or memory growing with use.** Implementation behavior of both readers: an open
dataset keeps a part open, with its checked footer, for every part it has touched, until it closes; `Verify`
touches every part. Through Go `OpenDir`, each open part holds one file descriptor. Size the process's
file-descriptor limit and memory for the most parts one dataset touches.

**When to stop paging.** The examples continue until an empty page, which is a safe termination rule: the
format allows a page with fewer entries than the result limit. The current Go and Java readers fill a
successful page to the result limit unless the query is exhausted.

**A listing returns a common prefix that sorts before my cursor.** That is correct: a common prefix appears
whenever a candidate row after the cursor rolls up to it ([LIST-5](../../spec/format-1.md#list-5)). It then
carries an exact row only if the first such candidate row has the prefix as its key
([LIST-8](../../spec/format-1.md#list-8)). Resume from a
common prefix with its prefix cursor, which skips everything under it.

**A value is neither a value nor null.** It is absent: the row's part does not have that column
([COL-2](../../spec/format-1.md#col-2)). Go `Value.Absent()` is true and `IsNull()` false; Java returns
`Value.Absent`.

**A read succeeded on a dataset that validation fails.** Reads check only what they decode
([CHECK-4](../../spec/format-1.md#check-4)). [Validation](validating.md) checks the scope of the level you ask
for.
