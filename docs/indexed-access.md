# Indexed-access qualification status

The tests below are deterministic regression evidence for the current readers.
They do **not** complete the mandatory indexed-access qualification gate in
[GOVERNANCE.md](../GOVERNANCE.md). The common regression runner below exercises both readers and emits JSON
observations, but a normative cross-reader qualification protocol and complete
coverage remain open. Shared semantic conformance alone also does not establish
access efficiency.

All new scenarios generate temporary Parquet files from readable source rows.
Expected keys, projected values and prefixes come from those rows, independently
of the reader's routing decisions. The Go writer is parquet-go; the Java test
writer emits PLAIN, uncompressed V1 pages. These access tests are not independent
encoding interoperability evidence.

| Scenario | Go evidence | Java evidence | Scope or remaining gap |
| --- | --- | --- | --- |
| Cold/warm point and five-row range; 16 parts × 16 groups; 128-byte shared stem | `TestIndexedAccessColdWarmLongPrefix` counts `SourceFile.ReadAt` calls and requested bytes through public `Range` | `SeekCostTest.manyPartsWithLongPrefixesHaveBoundedColdAndWarmShortRangesAndLists` counts `PartSource.read` through public `range` | Each range size starts on a fresh handle; warm repeats reuse it. Open counters are reset separately. Local logical reads, not remote requests. |
| Cold/warm delimiter listing on the same geometry | Same Go test, public `List`, exact 16 directory prefixes | Same Java test, public `list`, exact 16 directory prefixes | One directory per group; budgets permit index loading and key-page routing per emitted prefix. |
| Many groups | `TestIndexedAccessNonMonotoneScaling`: 10/100/1,000 groups; above test: 256 groups across 16 parts | `coldFinalKeyUsesLogarithmicProbesAndBoundedIndexOverfetch`: 10/100/1,000 groups, read and byte budgets | No 100,000-part open or remote workload qualification. |
| Broad and nonmonotone bounds | `TestIndexedAccessNonMonotoneScaling` measures reads/bytes/decodes on alternating broad bounds; `TestLongKeys` checks overlapping-bound results | Alternating broad nonmonotone bounds above; `nonMonotoneBoundsKeepThePredecessorOfARepeatedGroup` checks complete repeated group | Broad bounds can require scanning within a group; no universal constant-read promise. The scaling case holds candidate group size constant at 16 rows. |
| Large repeated group and adaptive row continuation | `TestLargeGroupResumeIsPageBounded` and `TestIndexedAccessRepeatedListingResume`: 5,000 equal keys, actual prior-row cursors, maximum decoded pages/request; listing also checks projection | `repeatedPaginationJumpsToPhysicalRowsForRangeAndList`: 10,000 equal keys, range and list, per-request and aggregate read/byte budgets | Both APIs check every expected offset; counts are data-page decodes in Go and logical source reads in Java. |
| Sparse projection with/without value OffsetIndex | `TestSparseListingDecodesValuePageOnce`: exactly two decoded data pages; `TestSparseProjectionSkipsPages`: exact distinct selected pages, null/dictionary/absent values | Projected ranges above exercise value pages with OffsetIndex | Java sparse delimiter projection and missing-OffsetIndex counted decoder coverage remain open. Go's direct sparse projection test additionally exercises the internal production projector. |
| Large keys and retained metadata | `TestFirstKeysShareIndexBudget`: 32 × 128 KiB keys, exact retained-index/first-key charge and one-byte-short refusal | `retainedExactFirstKeysAreChargedOnceBeforePublication`: exact budget including aligned retained first-key copy | Resource accounting tests, not total heap measurements. Java lacks the same 128 KiB multi-group geometry. |

The source wrappers count every requested range, including overlapping requests
and failed requests; requested bytes are not bytes fetched from a network or
physical disk. The Go `stats.pages` counter is incremented by the production
decoder and counts repeated data-page decoding, excluding dictionary pages,
header walking and index parsing. The sparse tests derive the distinct selected
data pages from generated OffsetIndexes, then compare that geometry with decode
counts. Source reads and decoder counts measure different work and should not
be combined into one metric.

The new source budgets allow one index load and routing/key-page reads per
visited group or emitted directory, plus selected value work. Warm ranges allow
three source reads because locating/projecting a physical row can require a
key-page reread as well as the selected value page. Byte ceilings account for
16 rows of roughly 150-byte keys per page and metadata; they are fixture-specific
regression bounds, not format limits or latency estimates. Existing Java scaling
tests additionally bound logarithmic routing probes and index overfetch.

Retained index and first-key charges exclude transient decoding buffers, parsed
footers, handles and returned results. Completing qualification still requires a
complete cross-reader report of reads, requested bytes, distinct pages, repeated
decodes and retained metadata, the gaps in the table, and
measurements using the actual remote adapter with cache/prefetch behavior stated.
See [read costs](read-costs.md) for cold-open and producer geometry guidance.

Run both readers' selected access suites and save the machine-readable report:

```sh
python3 conformance/harness/check_indexed_access.py --output /tmp/indexed-access.json
```

The runner requires the declared Go/JDK toolchains and uses the Gradle wrapper.
Use `--language go` or `--language java` in jobs with a single reader toolchain;
the default runs both. Reports name the readers actually selected.
It forces test execution, requires nonempty passing results from each selected reader,
uses two Go processors/Gradle workers, and sets finite per-reader timeouts
(default 900 seconds, configurable with `--timeout`). A failed reader produces a
nonzero exit; tests are never silently skipped. The JSON includes per-test status
and selected fixture-specific read/byte/decode observations, with
`freeze_qualified: false`. Existing assertions supply the regression budgets;
this report is a repository tool, not a new normative format contract. It does
not yet aggregate every metric for every scenario.

Run the full affected suites with `go test -race -p 1 ./...` from `go/` and
`./gradlew --max-workers=2 build` from `java/` using the declared toolchains.
