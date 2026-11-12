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

The Go and Java adapters also accept `SEEKPARQUET_LIST_INDEX=1`: before a list request they build the
reader's optional boundary index, then answer the same existing cases. Run both modes to check that the
optimization changes no conformance answer. This is an adapter test setting, not a protocol capability;
it adds no query semantics or fixture expectations. Index budgets, caps and page-count guards live in
each implementation's tests and benchmarks.

**Optional features.** Cases on `optional-codecs` (LZ4_RAW and BROTLI value columns) accept the values or
`unsupported` (`unsupported_encoding`); full validation of it may be `complete` or `incomplete`, never
`failed`.

Passing this semantic suite does not claim indexed access (the planned indexed-access suite, which
has no target until its cases exist) or performance.

To check the suite itself:

```sh
python3 harness/check_suite.py                                     # needs jsonschema
python3 -m unittest harness/test_run.py generators/test_parquet_meta.py \
    generators/test_oracle.py generators/test_case_generation.py   # stdlib; reference adapter tests need requirements.txt
python3 harness/run.py --adapter "python3 harness/reference_adapter.py"   # needs generators/requirements.txt
python3 generators/check_reproducible.py                           # regenerate in a temp dir and compare
```

## Where expected results come from

Invalid fixtures' faults are declared by hand in [generators/faults.json](generators/faults.json): the
fault id (the dataset name), when a reader must detect it (`open`, `touch`, `read`, or only by validation), its
lowest validation level, its codes, the requirements it breaks, its location, and any unavoidable secondary
fault with a justification. Refusal and validation cases are built from that file; nothing is inferred
from codes. Validation is one case per fault at its lowest detecting level, plus 15 prerequisite cases
(six structural faults with distinct entry paths requested at profile and full, and three profile faults
requested at full) and six distinct digest behaviours.


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
- **One fault per invalid fixture** where practical; otherwise faults.json names the secondary fault and
  justifies it, and the case accepts its code too.
- **Preserved regressions.** A fixture that must never be regenerated (a minimized reproducer, or bytes
  from a writer no longer pinned) is listed in [generators/preserved.json](generators/preserved.json) with
  its file hashes; `regenerate.py` verifies it and never deletes or rewrites it.
- **Renames keep case ids.** `truncated-bounds` and `truncated-bounds-2` became
  `binary-bounds-truncated-16-byte` and `binary-bounds-truncated-2-byte`; the mapping is in `suite.json`
  (`renamed_datasets`), and case ids still say `truncated-bounds…`. Exact duplicate cases are retired
  (`retired_case_ids`, with the id each duplicated), as are cases that tested only a removed request member
  (with a `reason`); retired ids are never reused.

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

### Optional list-index accounting parity

The implementation tests can compare build statistics on the same checked-in Parquet bytes without
changing conformance answers. From the repository root (Go 1.26+ and JDK 21+):

```sh
mkdir -p java/seekparquet/build
export SEEKPARQUET_LIST_INDEX_PARITY="$PWD/java/seekparquet/build/list-index-parity.tsv"
go -C go test -run '^TestListIndexParity$' -count=1
java/gradlew -p java :seekparquet:test --tests '*ListIndexResourcesTest.sharedFixtureAccountingParity' --offline --max-workers=1
```

The export covers four shared datasets, six budgets and four page-change caps. Java independently
builds each index and compares indexed groups, entries, retained bytes and budget exhaustion. This
optional test skips when the environment variable is absent; it adds no protocol operation or spec rule.

### Optional random list-index differential

The differential harness compares complete listing responses from Go and Java, each with and
without the index, on eight seeded random datasets containing the same Parquet bytes. It covers
unique/repeated keys, multiple parts, sparse and dense pages, exact folder objects, byte prefixes,
arbitrary prefix/row cursors, end bounds, projections and pagination. This supplements the normative
suite; agreement between implementations is not conformance qualification.

```sh
export SEEKPARQUET_LIST_RANDOM_FIXTURES="$PWD/java/seekparquet/build/list-index-random"
go -C go test -run '^TestListIndexExportRandomFixtures$' -count=1
java/gradlew -p java :conformance:installDist --offline --max-workers=1
python3 conformance/harness/list_index_differential.py \
  --fixtures "$SEEKPARQUET_LIST_RANDOM_FIXTURES" \
  --go-adapter "go -C go run ./internal/cmd/conformance" \
  --java-adapter java/conformance/build/install/seekparquet-conformance/bin/seekparquet-conformance
```

Fixture export skips when the environment variable is absent. Generated files belong in the ignored
build directory and are not checked into the suite.
