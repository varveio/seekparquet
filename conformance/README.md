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
| [suite.json](suite.json) | Suite revision, spec revision, case files, mandatory target. |
| [cases/](cases/) | The cases, one file per group and target. |
| [datasets/](datasets/) | Fixture datasets: `manifest.json`, Parquet parts and a textual `STRUCTURE.txt` each. Invalid fixtures are under `datasets/invalid/`. |
| [fixtures.lock.json](fixtures.lock.json) | Every fixture file with SHA-256, size, producer, provenance and generator command, and each invalid fixture's fault. |
| [oracle.py](oracle.py) | Sequential implementation of the query semantics (spec §6) over source rows; computes every expected entry. |
| [generators/](generators/) | Source rows, pinned generators and the metadata mutator ([README](generators/README.md)). |
| [harness/run.py](harness/run.py) | The harness (Python standard library only). |
| [harness/check_suite.py](harness/check_suite.py) | Static checks of the suite (schema, operation-specific payloads, requirement ids, codes, names, lock, suite digest). |
| [harness/test_run.py](harness/test_run.py) | Self-tests of the harness with deliberately deficient adapters. |
| [COVERAGE.md](COVERAGE.md) | Generated: each spec requirement id and the cases that cite it, or why none do. |
| [harness/reference_adapter.py](harness/reference_adapter.py) | A full-scan semantic reference adapter used to test the suite itself. It is **not** a SeekParquet reader. |

## Running the suite against an implementation

Write an adapter (see [protocol.md](protocol.md)) and run, from this directory:

```sh
python3 harness/run.py --adapter "path/to/adapter --flags"
python3 harness/run.py --adapter "go run ./internal/cmd/conformance" --filter '^range/'   # a subset
python3 harness/run.py --adapter "..." --targets validator-profile                    # a validator only
python3 harness/run.py --adapter "..." --json > report.json
```

The harness sends `hello`, checks the reported targets, capabilities and supported version, then runs
every case of every claimed target, sending requests without their expected answers. Case files are
grouped by target (`reader`, `validator-structural`, `validator-profile`, `validator-full`,
`validator-digest`); cases of unclaimed targets are skipped and reported as such, while skipping a case of a
claimed target is a failure. Without `--targets`, the adapter must claim the suite's mandatory target
(`reader`). The run fails on a wrong answer, a wrong refusal code, a refusal later than the case's `detect`
stage, a missing, duplicate or out-of-order response, an adapter crash or timeout, an unknown capability or
target name, or an unsupported format version. The exit status is 0 only when everything passed.

Refusal cases follow the state machine in protocol.md: the harness sends `open`; an acceptable refusal
there completes the case; for `detect: read` it then sends the case's request (an unbounded range whose limit
covers every row, so every correct access path decodes the faulty data); for `detect: validate:<level>` it
sends `validate` and expects that level to fail with an acceptable code.

**Qualification.** A target qualifies only on a complete run: every case of the target and of the targets
it depends on (`target_dependencies` in capabilities.json; `validator-full` → `validator-profile` →
`validator-structural`) ran and passed, with no run error. A run with `--filter`, or with `--targets`
narrower than what the adapter claims, is a partial run: the harness reports whether the selected cases
passed, records the selection (`selection` in `--json` output), and never reports qualification. The exit
status is 0 when every selected case passed and there was no run error (including a non-zero adapter exit).

**Optional features.** Cases on `optional-codecs` (LZ4_RAW and BROTLI value columns) accept the values or
`unsupported` (`unsupported_encoding`); full validation of it may be `complete` or `incomplete`, never
`failed`.

Passing this semantic suite does not claim indexed access (the planned indexed-access suite) or
performance.

To check the suite itself:

```sh
python3 harness/check_suite.py                                     # needs jsonschema
python3 -m unittest discover -s harness -p 'test_*.py'             # harness self-tests, stdlib only
python3 -m unittest discover -s generators -p 'test_*.py'          # parquet_meta malformed-input tests
python3 harness/run.py --adapter "python3 harness/reference_adapter.py"   # needs generators/requirements.txt
python3 generators/check_reproducible.py                           # regenerate in a temp dir and compare
```

## Where expected results come from

Every valid dataset is generated from a rows document in [generators/rows/](generators/rows/): keys in
base64 and column values typed as in the protocol. [oracle.py](oracle.py) computes each expected entry
from those rows by walking every row in key order, with no index. Expected refusals come from the fault
recorded for each invalid fixture in `fixtures.lock.json`. No expected result is a recording of any reader's
output. The reference adapter, which shares no query code with the oracle, must pass 100%.

## Fixture admission policy

- **Synthetic data only.** No real object names, bucket names, customer data or internal identifiers.
- **Small.** Normally at most 1 MiB per fixture file and at most 10 MiB for the whole corpus (enforced by
  `regenerate.py` and `check_suite.py`). Large or performance datasets are generated on demand or published
  as checksummed release assets, never added here.
- **Reproducible and explained.** Every fixture needs its generator or mutation source in `generators/`,
  pinned dependencies (`generators/requirements.txt`), its SHA-256 in `fixtures.lock.json`, and a textual
  structural summary (`STRUCTURE.txt`: row groups, pages per key chunk, `first_row_index`, bound lengths,
  `sorting_columns`, stamps, and for derived fixtures exactly what was changed).
- **Changing fixture bytes needs an explanation** in the pull request: which fixtures, why, and the
  `STRUCTURE.txt` diff. A generator or dependency upgrade that changes bytes is a fixture change.
  Checked-in bytes stay authoritative; a regression fixture is never silently replaced by regeneration.
- **No Git LFS.** Fixtures are ordinary files, so the suite works offline and from a plain checkout.
- **One fault per invalid fixture** where practical; otherwise the case lists every acceptable code.

## Provenance categories

Recorded per file in `fixtures.lock.json` and per dataset in `STRUCTURE.txt`:

| Category | Meaning |
|---|---|
| `native` | Output of an independent Parquet writer (pyarrow 26.0.1), including the profile metadata, which pyarrow writes itself (footer stamps via schema metadata, `sorting_columns`, page index). Some invalid fixtures are native too: rows written out of order with sorting claimed, an empty key, a nullable or INT64 key, missing or wrong stamps, a key long enough that pyarrow drops the ColumnIndex. |
| `native-with-profile-metadata` | Independent-writer output to which profile metadata was added afterwards. No fixture needs this yet. |
| `mutated` | Deliberately edited by [generators/mutate_metadata.py](generators/mutate_metadata.py): manifests, footers or page indexes, with data pages preserved byte for byte (one fixture overwrites one data page body, and says so). Both valid fixtures (for example conservatively shortened, overlapping or non-monotone page bounds, which pyarrow never writes) and invalid ones. |
| `generated` | A text file (manifest) written by a generator. |

## Regenerating

```sh
python3 -m venv .venv && .venv/bin/pip install -r generators/requirements.txt
.venv/bin/python generators/regenerate.py
```

With the pinned pyarrow, regeneration is byte-reproducible: the fixtures, `fixtures.lock.json`, the case
files and `STRUCTURE.txt` come out identical. `generators/check_reproducible.py` proves it by regenerating
in a temporary directory and comparing; CI should run it whenever `generators/` or its requirements change.
The lock records Python as major.minor only. If a future writer is not byte-reproducible, keep the
committed bytes and require identical `STRUCTURE.txt` output instead.

## Versions

The suite revision is in `suite.json` (`revision`); it moves independently of the specification's draft
revision, which the suite also records (`format_version`, `draft_revision`). `content_sha256` is a digest of
protocol.md, case.schema.json, capabilities.json, fixtures.lock.json and every case file (recomputed by
`check_suite.py`), so `spec/status.json` can pin a suite by revision and digest. A published suite revision
is immutable.
