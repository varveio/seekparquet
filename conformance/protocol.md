# Conformance runner protocol

Every implementation provides a small **adapter**: an executable that answers requests about datasets, one
JSON object per line. The **harness** (`harness/run.py`, Python standard library only) reads the cases, sends
requests to the adapter without their expected answers, compares the responses, and reports. No
implementation needs another implementation's toolchain to run the suite.

## Transport

- The harness starts the adapter as a child process (`run.py --adapter "COMMAND ..."`).
- Each request is one line of JSON on the adapter's stdin; each response is one line of JSON on its stdout,
  in the same order. The adapter writes nothing else to stdout. Diagnostics go to stderr.
- Every request has an `id`; the response repeats it.
- The adapter exits when stdin closes.

## Encoding

- Byte strings (keys, prefixes, delimiters, `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY` values) are standard
  base64 with padding, in members whose names end in `_base64`.
- Projected values, one per requested column in request order:

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

## Requests

### `hello`

```json
{"id": "1", "op": "hello"}
```

```json
{"id": "1", "status": "ok", "implementation": "seekparquet-go", "version": "v0.0.0-dev",
 "supports": [{"format_version": 0, "draft_revision": 1}],
 "capabilities": ["open", "range", "list", "projection", "validate:structural", "validate:profile"]}
```

The harness skips cases whose `requires` lists a capability the adapter does not report, and fails the run
if a mandatory capability (`open`, `range`, `list`, `projection`) is missing.

### `open`

```json
{"id": "2", "op": "open", "dataset": "/abs/path/datasets/basic"}
```

Opens the dataset as a reader would (CHECK-1). The adapter may keep it open for later requests on the same
path.

### `range`

```json
{"id": "3", "op": "range", "dataset": "/abs/path/datasets/basic",
 "start_base64": "YQ==", "start_inclusive": true, "end_base64": "Yw==", "limit": 10, "columns": ["size"]}
```

`start_base64` and `end_base64` are optional; `start_inclusive` defaults to true; `columns` defaults to `[]`.
The adapter opens the dataset first if it has not.

### `list`

```json
{"id": "4", "op": "list", "dataset": "/abs/path/datasets/basic",
 "prefix_base64": "", "delimiter_base64": "Lw==", "after_base64": "YS8=", "continuation": true,
 "end_base64": null, "limit": 100, "columns": []}
```

`after_base64` and `end_base64` are optional (absent or null); `continuation` defaults to false.

### `validate`

```json
{"id": "5", "op": "validate", "dataset": "/abs/path/datasets/basic", "level": "profile"}
```

## Responses

| `status` | Members | Meaning |
|---|---|---|
| `ok` | `entries` for `range` and `list`; `levels_completed` for `validate` | success |
| `refused` | `code` | the data breaks the specification; `code` is from `spec/refusal-codes.json` |
| `invalid_request` | `message` (optional) | the request is malformed (QUERY-1, LIST-1) |
| `unsupported` | `code`, `message` (optional) | the data is valid but uses an unsupported feature |
| `error` | `message` | anything else; the harness counts it as a failure |

Entries:

```json
{"kind": "row", "key_base64": "Yg==", "values": [{"int": "12"}]}
{"kind": "prefix", "key_base64": "YS8="}
```

## How the harness judges refusals

A case's `detect` field says when its refusal must appear at the latest (see `spec/refusal-codes.json`):

- `open`: the `open` response must be `refused` with the expected code.
- `read`: `open` may succeed or be refused with the expected code; if it succeeds, the case's request must be
  refused with the expected code.
- `validate:<level>`: `open` and the request may succeed, or be refused with the expected code; a `validate`
  request at that level must be refused with it.

Where a case lists several acceptable codes (`codes`), any of them passes.

## Suites

- **Semantic** (this protocol): results and refusals. A reader that scans every row can pass it.
- **Indexed access** (planned): fixtures and an instrumented source that fail a reader which reads pages it
  should have skipped. Passing the semantic suite does not claim indexed access.
- **Benchmarks**: outside the conformance suite.
