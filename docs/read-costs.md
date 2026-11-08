# Read costs and writer guidance

SeekParquet defines results and validity, not a constant read count. The shared semantic
conformance suite does not yet certify indexed-access efficiency. These are considerations for
choosing and measuring a writer configuration, not a qualified performance preset.
See the [indexed-access coverage matrix](indexed-access.md) for deterministic regression
evidence and the remaining qualification gaps.

## Measure three phases separately

1. **Cold open:** the manifest and every part's footer must be checked, even if the next query
   returns one row. Part count therefore matters independently of page size. Optional verification
   reads more key pages on open. Opening a fresh handle for every request repeats startup work.
2. **Cold query:** routing information and page indexes needed by the query must be loaded and
   checked, followed by relevant key and projected value pages. Row-group geometry, index placement
   and bound quality matter here.
3. **Warm query:** a reused handle can retain routing information and checked indexes, subject to
   the reader's limits. Data pages may still need reading and decoding. Do not assume that caching
   indexes means caching all data pages or results.

The all-part open checks are [CHECK-1](../spec/format-1.md#7-what-readers-check-and-when), not merely
a tuning choice. Reusing long-lived handles can amortize them, but also retains file handles and
metadata. A query that succeeds does not validate skipped data (CHECK-4).

## Keep key bounds useful

[RG-5/RG-6](../spec/format-1.md#52-row-groups-and-the-page-index) permit conservative shortened
page bounds, including overlapping or nonmonotone bounds. A reader cannot assume they are exact.
Many consecutive pages can appear to contain a sought key even when only the last one does.
In the worst case, a selective query can decode an entire row group.

For example, if keys share `logs/customer-0001/2026/` and differ only after that prefix, bounds
limited to the first eight bytes cannot distinguish those suffixes. Smaller data pages alone do
not repair that lack of discrimination.

- Prefer exact key-page bounds when their index size is acceptable, or retain enough bytes to
  distinguish neighboring pages for the actual key distribution.
- Inspect the resulting ColumnIndex, rather than assuming a writer's configured size means
  bounds are selective. Measure long shared prefixes and arbitrary byte keys as well as short keys.
- Preserve the bound rules when shortening: a maximum must remain at least the page's last key.
  If shortening cannot preserve an upper bound, the full maximum is required. Do not simply
  slice both minima and maxima to a byte budget.

These recommendations do not make a slower, correctly bounded file invalid.

## Choose geometry as a whole

- The [geometry guidance](../spec/format-1.md#55-geometry-and-encoding) recommends at most 1,024
  rows per key page. More/smaller pages also mean larger indexes, headers and potentially more reads.
- More parts increase open work. More row groups increase footer and routing metadata. Larger
  groups can increase index-loading and page-decoding costs. Test the combination, not just one knob.
- Include OffsetIndexes for value columns where the writer supports them (COL-4). Readers must
  accept their absence, but locating sparse projected rows may then require walking page headers.
- Measure index placement and coalescing in the chosen storage adapter. Combining nearby reads
  can save calls but fetch extra or overlapping bytes.
- Repeated-key groups cannot cross parts (PART-4/KEY-7). A single large group can exceed a desired
  part-size target, so that target must be best-effort unless the producer limits group sizes.
- Test sparse delimiter listings with projected values and deep repeated-key continuations, not
  only consecutive key-only ranges. Those exercises stress different paths.

## Report what was actually measured

For each phase, record logical source reads, requested bytes, distinct pages, total page decodes,
latency, retained metadata and open file handles. Distinguish repeated decoding from distinct data.
For a remote experiment, also record actual network requests and transferred bytes: source reads
can be cached, retried or coalesced, so they are not automatically HTTP requests.

Name the reader/version, writer options, file geometry, key distribution, projections, result
limit and cache state. Compare the same workload against a suitable baseline. Tiny synthetic
probes expose an access pattern; they do not establish production throughput or cloud latency.

The [quickstart](quickstart.md) is a semantic example using checked-in fixtures. The [local producer and validator](../tools/seekparquet/README.md) support a bounded development workflow;
representative remote qualification remains separate work.
