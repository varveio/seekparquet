# SeekParquet conformance suite

A language-neutral suite for checking SeekParquet implementations against
[format 1, draft revision 1](../spec/format-1.md). It contains small synthetic datasets, case files with the
expected result of each request (entries, or a refusal code and the latest point at which it must appear),
and a harness that drives any implementation through a small adapter.

| Path | What it is |
|---|---|
| [protocol.md](protocol.md) | The adapter protocol: JSON lines on stdin and stdout. |
| [capabilities.json](capabilities.json) | Capability and conformance-target names. |
| [case.schema.json](case.schema.json) | JSON Schema of the case files. |
| [suite.json](suite.json) | Suite revision and digest, spec revision, case files, subsets, mandatory target, retired case ids, renamed datasets. |
| [cases/core/](cases/core/) | About 150 hand-reviewed cases, one file per group. Each says what distinguishes a correct answer. Authored, never regenerated: `make_cases.py` checks them against the oracle and stops on a disagreement. |
| [cases/generated/](cases/generated/) | Generated families (`*.generated.json`): boundary sweeps, parameter variants, pagination expansions, one validation case per fault at its lowest level, selected prerequisite and digest cases. |
| [subsets/smoke.json](subsets/smoke.json) | Thirty core cases for adapter bring-up. Never qualifies. |
| [datasets/](datasets/) | Fixture datasets: `manifest.json`, Parquet parts and a textual `STRUCTURE.txt` each. Invalid fixtures are under `datasets/invalid/`. |
| [fixtures.lock.json](fixtures.lock.json) | Every fixture file with SHA-256, size, producer, provenance and generator command, and each invalid fixture's fault. |
| [oracle.py](oracle.py) | Sequential implementation of the query semantics (spec §6) over source rows; computes every expected entry. |
| [generators/](generators/) | Source rows, pinned generators and the metadata mutator ([README](generators/README.md)). |
| [harness/run.py](harness/run.py) | The harness (Python standard library only). |
| [harness/check_suite.py](harness/check_suite.py) | Static checks of the suite (schema, operation-specific payloads, requirement ids, codes, names, lock, suite digest). |
| [harness/test_run.py](harness/test_run.py) | Self-tests of the harness with deliberately deficient adapters (transport faults, and oracle-backed adapters with one injected bug each: signed byte order, inclusive end, shortened bounds treated as exact, a prefix cursor read as a byte floor, a seek from the last row group with an equal first key, group offsets reset at row groups, ignored cursor offsets, a common prefix without its exact row or with the group's first row as it, absent reported as null, validation skipping prerequisites, incomplete without a reason); qualification of a target without cases; offset spelling in the parsers; and the reference adapter's validation levels. |
| [vectors/cursor-token-1.json](vectors/cursor-token-1.json) | Golden vectors of the cursor token ([spec/cursor-token-1.md](../spec/cursor-token-1.md)): valid tokens with their cursors, malformed tokens, and cursors an encoder refuses. Not adapter cases. |
| [validator/](validator/VALIDATOR.md) | A Python validator at the four validation levels, kept as an independent oracle for CI. Conformance tooling, not a supported validator. |
| [COVERAGE.md](COVERAGE.md) | Generated: each spec requirement id and the cases that cite it, or why none do. |
| [harness/reference_adapter.py](harness/reference_adapter.py) | A full-scan semantic reference adapter used to test the suite itself. It is **not** a SeekParquet reader. |

## Running the suite against an implementation

Write an adapter (see [protocol.md](protocol.md)) and run, from this directory:

```sh
python3 harness/run.py --adapter "path/to/adapter --flags"
python3 harness/run.py --adapter "go run ./internal/cmd/conformance" --filter '^range/'   # a subset
python3 harness/run.py --adapter "..." --targets validator-profile                    # a validator only
python3 harness/run.py --adapter "..." --subset smoke                                 # 30 cases, bring-up
python3 harness/run.py --adapter "..." --subset core                                  # the reviewed examples
python3 harness/run.py --adapter "..." --json > report.json
```

The Go adapter (`go/internal/cmd/conformance`) claims the reader target and all four validator targets, backed
by the Go module's `Validate`; CI runs it over the full suite, so every validator case runs against it.

Three selections: **smoke** (30 core ids) for bring-up, **core** (about 150) for learning and ordinary
development, and **all** (core plus generated) for CI and qualification. Smoke and core runs, like filtered
runs, are partial and never qualify a target. The report counts results by group, by family, by part
(core/generated) and by target, and records the selection.

The harness sends `hello`, checks the reported targets, capabilities and supported version, then runs
every case of every claimed target, sending requests without their expected answers. Case files are
grouped by target (`reader`, `validator-structural`, `validator-profile`, `validator-full`,
`validator-digest`); cases of unclaimed targets are skipped and reported as such, while skipping a case of a
claimed target is a failure. Without `--targets`, the adapter must claim the suite's mandatory target
(`reader`). The run fails on a wrong answer, a wrong refusal code, a refusal later than the case's `detect`
stage, a missing, duplicate or out-of-order response, an adapter crash or timeout, an unknown capability or
target name, or an unsupported format version. The exit status is 0 only when everything passed.

A fault declaration's `outcome` defaults to `refused`; `unsupported` selects an unsupported-feature response
instead. Both outcomes use the declared detection stage and the same open/read/verify lifecycle. An acceptable
outcome during open completes that path; otherwise a `touch` case checks the first routed read and, when
claimed, verification on a fresh handle. Validation represents unsupported features as `incomplete` with a
non-empty reason, never as a failed level.

Refusal cases follow the state machine in protocol.md: the harness sends `open`; an acceptable refusal
there completes the case; for `detect: read` it then sends the case's request (an unbounded range whose limit
covers every row, so every correct access path decodes the faulty data); for `detect: touch` (a fault in a
part, which a reader need not detect before a read routes to the part) it sends the same kind of range, and
then, if the adapter claims the optional `verify` capability, `verify` on a freshly opened dataset; for `detect: validate:<level>` it
sends `validate` and expects that level to fail with an acceptable code.

**Qualification.** A target qualifies only on a complete run: it has cases of its own, and every case of
the target and of the targets it depends on (`target_dependencies` in capabilities.json; `validator-full` → `validator-profile` →
`validator-structural`) ran and passed, with no run error. A run with `--filter`, or with `--targets`
narrower than what the adapter claims, is a partial run: the harness reports whether the selected cases
passed, records the selection (`selection` in `--json` output), and never reports qualification. The exit
status is 0 when every selected case passed and there was no run error (including a non-zero adapter exit).

**Optional capabilities.** `verify` and `planning` are capabilities no target requires. The `planning` cases
(`cases/generated/planning.generated.json`) run only against an adapter that claims it, and are otherwise
skipped without counting against `reader` qualification. Their rows come from the oracle; their projected
bytes are computed by `make_cases.py` from the fixtures' OffsetIndex page locations, never from a reader.


**Optional features.** Cases on `optional-codecs` that project its BROTLI value column accept the values or
`unsupported` (`unsupported_encoding`); its LZ4_RAW column is in the ENC-1 baseline, and requests that do not
need the BROTLI column must succeed. Full validation of it may be `complete` or `incomplete`, never `failed`.
`brotli-key` has a BROTLI key: requests may answer `unsupported`. Structural validation completes. If the
key's BROTLI codec is unavailable, profile is incomplete with code `unsupported_encoding` and a non-empty
reason; full is then incomplete because its prerequisite did not complete. A level cannot complete unless its
prerequisite completed. Encrypted parts (`PARE` magic at either end of a part of at least 12
bytes, or encryption declared in the footer: `invalid/encrypted-*`, faults whose `outcome` is `unsupported`)
are answered `unsupported` (`unsupported_encryption`), and their structural validation is `incomplete` with
that code.

**Implementation limits, not cases.** A manifest nested deeper than 512 JSON objects and arrays (the manifest
object counting 1) is a resource limit of both the Go and the Java reader, not `invalid_manifest`; depth 512
is read (CHECK-5). The suite has no case for it, since a limit is the implementation's, and so does not
pin it.

Passing this semantic suite does not claim indexed access (the planned indexed-access suite, which
has no target until its cases exist) or performance.

## Versions

The suite revision is in `suite.json` (`revision`); it moves independently of the specification's draft
revision, which the suite also records (`format_version`, `draft_revision`). `content_sha256` is a digest of
protocol.md, case.schema.json, capabilities.json, fixtures.lock.json and every case file (recomputed by
`check_suite.py`), so `spec/status.json` can pin a suite by revision and digest. A published suite revision
is immutable.

## Maintaining the suite

How expected results are made, the fixture policy, regeneration and the suite's own checks are in
[MAINTAINING.md](MAINTAINING.md).
