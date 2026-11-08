# Conformance runner protocol

Every implementation provides a small **adapter**: an executable that answers requests about datasets, one
JSON object per line. The **harness** (`harness/run.py`, Python standard library only) reads the cases, sends
requests to the adapter without their expected answers, compares the responses, and reports. No
implementation needs another implementation's toolchain to run the suite.

## Transport

- The harness starts the adapter as a child process (`run.py --adapter "COMMAND ..."`).
- Each request is one line of JSON on the adapter's stdin; each response is one line of JSON on its stdout,
  in the same order. Lines are UTF-8 without a byte order mark; objects have unique member names. The adapter
  writes nothing else to stdout. Diagnostics go to stderr.
- Every request has an `id`, a string; the response repeats it.
- The adapter exits with status 0 when stdin closes. A non-zero exit status, a crash, or failing to exit
  shortly after stdin closes is a run error, even if every response was correct.

## Encoding

- Byte strings (keys, prefixes, delimiters, `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY` values) are standard
  base64 with padding, in members whose names end in `_base64`.
- Projected values are **physical** Parquet values, one per requested column in request order. Logical
  annotations (string, decimal, date, timestamp, unsigned integers) are ignored: an `INT32` annotated as
  unsigned is still reported as its signed 32-bit value. Floating-point values are reported by their exact
  bits, so NaN payloads and the sign of zero are preserved. Column names match top-level field names exactly
  (spec COL-6).

  | JSON | Meaning |
  |---|---|
  | `{"absent": true}` | the row's part has no such column (COL-2) |
  | `{"null": true}` | the value is null |
  | `{"bool": true}` | `BOOLEAN` |
  | `{"int": "-42"}` | `INT32` or `INT64`, as a decimal string |
  | `{"float32": "3fc00000"}` | `FLOAT`, as the 8 lowercase hex digits of its IEEE 754 bits |
  | `{"float64": "3ff8000000000000"}` | `DOUBLE`, as the 16 lowercase hex digits of its bits |
  | `{"bytes_base64": "..."}` | `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` |

- Dataset paths are absolute local paths.
- This revision of the protocol defines no representation for nested or repeated values, so the suite has no
  case that projects one successfully.

## Requests

### `hello`

```json
{"id": "1", "op": "hello"}
```

```json
{"id": "1", "status": "ok", "implementation": "seekparquet-go", "version": "v0.0.0-dev",
 "supports": [{"format_version": 0, "draft_revision": 1}],
 "targets": ["reader"],
 "capabilities": ["open", "range", "list", "projection"]}
```

`capabilities` and `targets` use the names in [capabilities.json](capabilities.json). A **target** is a
conformance claim (for example `reader`, the semantic reader target; `validator-profile`); each target
lists the capabilities it needs. For every target an adapter claims, every case of that target runs:
skipping one is a failure. A target may depend on other targets (`target_dependencies` in capabilities.json,
for example `validator-full` on `validator-profile` on `validator-structural`): claiming it runs the cases
of its dependencies as well, and it qualifies only if they qualify. Cases outside the claimed targets and
their dependencies are skipped and reported as skipped. The run fails if a claimed target's capability is
not reported, or if the adapter does not support the suite's format version and draft revision.

**Case files.** Cases live in `cases/core/` (about 150 hand-reviewed examples, `generated: false`, each with
a description of what distinguishes a correct answer) and `cases/generated/` (generated families,
`generated: true`, with `family`, and where relevant `fault_id`, `scenario_id` and `step`). Both are mandatory
for qualification; the split is about authorship, not optionality. Case ids are stable and never encode the
split; retired ids are listed in `suite.json` and never reused.

**Qualification.** A target is qualified only by a complete run: every case of the target and of its
dependencies ran and passed, with no run error. A run narrowed by a case filter or by selecting fewer
targets than the adapter claims, or limited to a selection (`--subset core`, or a named subset such as
`subsets/smoke.json`), is a partial run: it reports whether the selected cases passed, and never
reports a target as qualified. The harness records the selection in its report.

### `open`

```json
{"id": "2", "op": "open", "dataset": "/abs/path/datasets/basic"}
```

Opens the dataset as a reader would (CHECK-1). The adapter may keep it open for later requests on the same
path.

### `range`

```json
{"id": "3", "op": "range", "dataset": "/abs/path/datasets/basic", "prefix_base64": "",
 "start_base64": "YQ==", "start_exclusive": false, "end_base64": "Yw==", "limit": 10, "columns": ["size"]}
```

`prefix_base64`, `start_base64` and `end_base64` are optional (absent or null) and may be empty strings (spec QUERY-2); `start_exclusive` defaults to false; `columns` defaults to `[]`.
The adapter opens the dataset first if it has not.

### `list`

```json
{"id": "4", "op": "list", "dataset": "/abs/path/datasets/basic",
 "prefix_base64": "", "delimiter_base64": "Lw==", "after_base64": "YS8=", "continuation": true,
 "end_base64": null, "limit": 100, "columns": []}
```

`after_base64` and `end_base64` are optional (absent or null); `prefix_base64` defaults to empty; `continuation` defaults to false and has no effect without `after_base64`; `columns` defaults to `[]`.

### `validate`

```json
{"id": "5", "op": "validate", "dataset": "/abs/path/datasets/basic", "level": "profile"}
```

Validation runs the requested level and its prerequisites (spec §8) and reports each level it attempted:

```json
{"id": "5", "status": "ok", "levels": {
  "structural": {"result": "complete"},
  "profile": {"result": "failed", "code": "key_order"}}}
```

`result` is `complete`, `failed` (with `code`) or `incomplete` (with `reason`, a non-empty string; the
harness rejects an `incomplete` level without one, but never compares its wording). A `validate` request never
answers `refused`: a dataset that cannot be opened reports `structural` as `failed` with the refusal code.
Validator targets therefore include the open-stage faults. The `digest` level adds
`checked`, the number of digests it compared. `status` is `ok` whenever validation ran, whatever it found.

## Responses

| `status` | Members | Meaning |
|---|---|---|
| `ok` | `entries` for `range` and `list`; `levels` for `validate` | success |
| `refused` | `code` | the data breaks the specification; `code` is from `spec/refusal-codes.json` |
| `invalid_request` | `message` (optional) | the request is malformed (QUERY-1, LIST-1) |
| `unsupported` | `code`, `message` (optional) | the data is valid but uses an unsupported feature, or the adapter does not implement the operation (`unsupported_operation`) |
| `error` | `message` | anything else; the harness counts it as a failure |

Entries (a row always has `values`, empty when no columns were requested; a prefix never has them):

```json
{"kind": "row", "key_base64": "Yg==", "offset": 0, "values": [{"int": "12"}]}
{"kind": "prefix", "key_base64": "YS8="}
```

## How the harness judges refusals

A refusal case has a `detect` stage, the latest point at which the refusal must appear (see
`spec/refusal-codes.json`), and an expected `code` (or a list of acceptable `codes`). The harness runs the
stages in order and stops at the first refusal:

1. **open.** It sends `open`. A refusal with an acceptable code completes the case (whatever its `detect`). A
   refusal with another code fails it. If `detect` is `open` and `open` succeeds, the case fails.
2. **read.** If `detect` is `read`, it sends the case's request, which must be refused with an acceptable
   code.
3. **validate.** If `detect` is `validate:<level>`, it sends the case's request if it has one (success, or
   a refusal with an acceptable code, which completes the case), then `validate` at that level, whose
   report must show that level `failed` with an acceptable code.

A `read` case's request must decode the faulty data under every correct access path: for example a range
that starts before the faulty page and whose limit covers it. Faults that an index-guided reader may
legitimately skip (CHECK-4) belong in `validate` cases. Fixtures carry one fault each where practical;
otherwise the case lists every acceptable code.

## Malformed requests

Cases that test `invalid_request` use `raw_request` instead of `request`: an object the harness sends as it
is, adding only `id` and `dataset`, so that a fractional limit or malformed base64 can be expressed.

## Optional features

A case on a dataset that uses a feature outside the mandatory baseline (for example the LZ4_RAW or BROTLI
codec, ENC-1) may list `accept_unsupported` codes in its expectation: the expected `ok` result passes, and so
does `unsupported` with one of those codes. A refusal never passes. In a validation expectation a level may
list further acceptable results in `accept`, for example a `full` level that is `complete` or, for a
validator without the codec, `incomplete` (never `failed`). Full validation of datasets with nested or
repeated columns accepts `incomplete` for the same reason (VAL-1); baseline-only datasets never do.

Nested and repeated columns: COL-3 allows a reader to support projecting them, but this protocol has no
value encoding for nested data, so the suite can only expect `unsupported_projection`. That reflects the
protocol's representational limit, not a requirement of the specification to reject such projections.

## Suites

- **Semantic** (this protocol): results and refusals. A reader that scans every row can pass it.
- **Indexed access** (planned): fixtures and an instrumented source that fail a reader which reads pages it
  should have skipped. Passing the semantic suite does not claim indexed access.
- **Benchmarks**: outside the conformance suite.

### Tagged continuation

Both range and list accept `after = {"key_base64": "...", "offset": n}` after a row,
or `after = {"prefix_base64": "..."}` after a common prefix (PAGE-1/PAGE-2).
Each row response includes `offset`, its zero-based position within its group in file order;
it is always zero when the manifest declares unique keys. Offsets are non-negative integers
at most 2^63-1. Tags have exactly the members shown, and base64 is canonical.
A row cursor resumes at offset n+1 of that key, then later keys. A prefix cursor suppresses
that common prefix and all earlier prefixes. Query bounds still apply to whole groups;
a page limit may split a group. Repeating pages must equal the unpaginated result.
`after_base64` remains a listing byte floor, with `continuation` controlling LIST-5 suppression;
it is not a row cursor. It must not be combined with `after`.
Lookup uses range with start k inclusive and end k followed by a zero byte; no new operation is needed.
