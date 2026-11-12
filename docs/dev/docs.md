# How the documentation works

This page is for anyone changing the documentation. It covers where content goes, what is generated, the
checks CI runs, and the style rules.

## Where content goes

| Content | Its one home | Everywhere else |
|---|---|---|
| Format rules, query results, check obligations | [spec/format-1.md](../../spec/format-1.md) | cite the requirement ID, linked to its anchor (`format-1.md#check-2`); never restate the rule |
| What a Go or Java symbol does | its godoc or Javadoc comment | guides show it in use |
| Limit defaults and meanings | the `Limits` code and comments | [reference/limits.md](../reference/limits.md), generated |
| Refusal and unsupported codes | [spec/refusal-codes.json](../../spec/refusal-codes.json), plus causes and actions in [docs/data/refusal-actions.json](../data/refusal-actions.json) | [reference/refusal-codes.md](../reference/refusal-codes.md) and the troubleshooting sections, generated |
| `ReadStats` fields | the Go struct comments and the Java record | [reference/read-stats.md](../reference/read-stats.md), generated |
| Go and Java names | [docs/data/parity.json](../data/parity.json) | [reference/parity.md](../reference/parity.md), generated |
| The pre-release status | [spec/status.json](../../spec/status.json) | the status box in the root README, generated; no other page repeats it |
| History ("previously", "renamed from") | the component changelog | nowhere else |
| The conformance protocol | [conformance/protocol.md](../../conformance/protocol.md) | not restated |

Guides are organized by task. Under each task heading: one sentence, a **Go** block, a **Java** block, then at
most one line on how Java behaves differently. When a language cannot do the task, say so in place of its
block ("**Java:** not available.").

## Generated pages, regions and snippets

`go/internal/tools/docgen` renders everything generated. From the repository root:

```sh
go -C go run ./internal/tools/docgen      # check; exits 1 when a file is out of date
go -C go run ./internal/tools/docgen -w   # rewrite the files that differ
```

It renders:

- the whole pages under `docs/reference/` except `compatibility.md`; they start with a "Generated" comment;
- **generated regions** of hand-written pages, between these markers, from the data files:

  ```markdown
  <!-- generated: troubleshooting-refusals -->
  <!-- /generated -->
  ```

  The regions are `status`, `troubleshooting-refusals`, `troubleshooting-unsupported` and
  `troubleshooting-limits`;
- **snippets**, between these markers:

  ```markdown
  <!-- snippet: go/example_docs_test.go#Example_readme -->
  <!-- /snippet -->
  ```

  For a `.go` file, the snippet is the body of the named function: write it as an `Example` function with
  an `// Output:` comment, so `go test` runs it and checks what it prints. For any other file, the snippet
  is the lines between comments containing `tag::NAME[]` and `end::NAME[]`. Java snippets live in
  `java/seekparquet/src/test/java/io/varve/seekparquet/docs/DocSnippetsTest.java`, a JUnit test that
  checks their output. Without `#NAME`, the snippet is the whole file.

Write the markers with nothing between them; `-w` fills them in. Never edit generated text by hand: change its
source and regenerate. Markers inside fenced code blocks are left alone.

docgen also checks the data against the code, in both directions:

- every exported `Limits` field and every `LimitName` has an entry in `docs/data/limits.json`, and every
  default stated in a field comment equals the default the code sets;
- every `ReadStats` field has an entry in `docs/data/read-stats.json`;
- every refusal and unsupported code of `spec/refusal-codes.json` has a Go constant, a Java constant and an
  entry in `docs/data/refusal-actions.json`;
- every Go and Java name in `docs/data/parity.json`, `limits.json` and `read-stats.json` exists, and Java's
  limit defaults equal Go's.

Java names are matched to public declarations on their owner type, including nested types and record
components. The complete builder-setting and statistics-component lists are compared both ways. A default
that cannot be extracted fails the check; derived defaults require a recognized implementation in each
language. Go's `TestLimitsDoc` and Java's `LimitsDocTest` also compare the rendered defaults with the
running implementations.

Both Markdown scanners inspect tracked files in a checkout, excluding untracked scratch files. Outside
Git they scan Markdown files under the supplied root.

When Go or Java gains a field, a limit or a code, the check fails until its entry exists. To skip the Java
checks for a parity row while Java lags behind Go, set that row's `java_pending` in `parity.json` to the
PR or branch that will catch up; its rendered note then says so. Other Java names, defaults and statistics
remain checked. A top-level `java_pending` skips all Java checks and marks every generated page that shows
Java names. `-java DIR` checks all Java names, including pending rows, against another checkout.

## Checks

| Check | Command | What fails |
|---|---|---|
| generated docs and snippets | `go -C go run ./internal/tools/docgen` | a generated file differs from its sources; data and code disagree |
| examples | `go -C go test ./...`; `java/gradlew -p java test` | an example's output differs from its `// Output:` comment or assertion |
| links, anchors, requirement IDs, terminology | `python3 scripts/docs/check_docs.py` | a relative link or `#anchor` that does not resolve; a requirement without its anchor; a cited ID the specification does not define; a denied term |
| the pyarrow recipe | the conformance CI job | the recipe no longer writes a dataset that validates at the full level |

CI runs the first and third in the `docs` job on every pull request, and the rest in the Go, Java and
conformance jobs. Links to other sites are not checked.

## Pending work

A page that describes work not yet merged carries one line naming the PR or branch:

```markdown
> Pending: the guide to custom schema checks waits for the schema checks branch (`feature/schema-checks`).
```

Search for `Pending:` before a release; none may remain.

## Style

- Second person, present tense: "Pass a `*ReadStats` to see what a read did." Say "the reader" for the
  library's behavior.
- Task headings are imperative ("Validate from the command line"); concept and reference headings are nouns.
- One claim per sentence. Aim for under 25 words; split anything over 40.
- Lists for three or more parallel items; tables for items with two or more attributes.
- No history: no "now", "previously", "new", "no longer". Changelogs hold history.
- Do not restate a default outside the generated reference, except in an example whose output shows it.
- A number that measures time or bytes on real hardware links its receipt: the dated record of machine,
  commit and store. A request count from the counting test store says so and names the test it comes from.
  No latency numbers until real-store measurements exist.
- "Always", "never" and "guarantees" only for a requirement of the specification (cite the ID) or a tested
  library contract. Never "fast", "efficient" or "cheap" without a number and its receipt.
- Requirement IDs in parentheses, linked to their anchors.
- Code identifiers in backticks. Qualify the language when it is ambiguous: Go `Dataset.Rows`, Java
  `Dataset.rows`.
- Binary units (KiB, MiB, GiB); thousands separators in prose ("1,000"), not in code.
- American spelling.
- Diagrams in Mermaid.

### Terms

[terms.txt](terms.txt) lists the denied terms that `check_docs.py` enforces on the pages in its scope. This
page quotes them, so it is not in the scope. Add a
page to the scope when you rewrite it. A line containing `terms: ok`, for example in an HTML comment, is
exempt.

| Concept | Write | Not |
|---|---|---|
| draft version | "format 1, draft revision 1 (on disk `format_version` 0)" | "draft 1" |
| equal keys | **key group** | bare "group" |
| Parquet's slice of a file | **row group** | "group" |
| key uniqueness | "unique keys", "repeated keys" | "unique mode", "grouped mode" |
| the specification's range operation | "range read"; Go `Rows`/`Scan`, Java `rows`/`scan` | "range" for the API |
| one key's rows | "lookup"; Go `Get`, Java `get` | "point query" |
| where a page ended | **cursor** (row cursor, prefix cursor); **cursor token** for its text form | "continuation", "position" |
| listing entries | **common prefix**, **row entry**, **exact row** | "folder", "directory", except to explain the S3 analogy |
| most results per query | **result limit** | bare "limit" |
| resource bounds | **limit** (fails the operation), **cache budget** (never fails) | "cap", "budget" for a limit |
| checks | **check** (as a read goes), **verify** (every part now), **validate** (§8) | "certify", except quoting the specification |
| a part's first use | **first touch** | "first read routes to" |
| data the reader rejects | **refusal**, with its code | "rejected", "invalid" (except "invalid request") |
| a malformed request | **invalid request** | "malformed request" |
| storage interface | Go **Source**, Java **storage** (`DatasetStorage`); "a Source implementation" | "adapter", which means the conformance adapter |
| one range asked of storage | **logical read** | "source read", "storage call" |
| what goes on the network | **physical request** | bare "request", "GET" |
| a dependent chain of requests | **round** | "round trip" |
| reading before it is needed | **speculative read** (read-ahead, tail read) | "prefetch" for read-ahead |
| one read of a part's end | **tail read** | "footer window", "metadata prefetch" |
| indexes | **page index** (ColumnIndex and OffsetIndex); **key page index** for the key column's | "key index", "page-index" |
| the optional listing accelerator | **list index** | "boundary index", "slash listing index" |
| a column a part lacks | **absent** | "missing column", "null" |
| fresh and reused | **cold** (first operation on a fresh dataset), **warm** (repeated on the same dataset) | "first visit", "revisit" |

### Tested contracts cited by the Go guides

Guides cite the canonical godoc or implementation alongside a behavior claim.
This register names existing evidence; it does not require broad runtime suites for a docs-only change.
Run Example functions and the documentation checks for snippet changes.

| Contract and canonical source | Regression evidence |
|---|---|
| `Dataset.Get`: GetRows failure and paging with KeySpan (`go/dataset.go`) | `TestGet`, `TestGetCapOne`, `ExampleDataset_Get_largeGroup` |
| Cursor tokens and same-query pagination (`go/cursor.go`, `go/query.go`; PAGE-1/PAGE-2) | `TestCursorTokenVectors`, `TestCursorTokenLength`, `TestCursorTokenPagination`, `Example_paginate` |
| Scan borrowing and owning copies (`go/batch.go`, `go/dataset.go`) | `TestScanBatchesPoisonRetiredStorage`, `TestScanBatchesBoundariesAndValues` |
| Close joins operations; callback restrictions (`go/dataset.go`) | `TestCloseWhileReading`, `TestRequestInsideScanWhileClosing` |
| Independent scalar contexts (`go/dataset.go`: ContextBinder) | `TestContextBinding` |
| Plan exactness and projected extents (`go/selection.go`) | `TestPlanCountsUseOnlyCheckedParts`, `TestPlanSharedDictionary`, `ExampleDataset_Plan` |
| Split union, key-group boundaries, independent scans (`go/split.go`) | `TestSplitConformanceOracle`, `TestSplitGeometries`, `TestSplitConcurrentScans`, `ExamplePlan_Split` |
| Known positions, no-I/O selection, value-only scans (`go/rows.go`) | `TestScanValuesReadsNoKeyPage`, examples in the counting guide |
| List-index accounting and option reuse (`go/listindex.go`) | `TestListIndexSharedMemoryIsolation`, `TestListIndexEmptyCacheAndEffectiveOptions`, `ExampleDataset_BuildListIndex` |
| Storage batch outcomes, fallback, and cancellation (`go/storage.go`, `go/storageio.go`) | `TestReadRangesBatched`, `TestReadRangesFallback`, `TestReadRangesCancelled`, `TestSourceErrorClassification`, `TestOnlyEOFBounded` |

