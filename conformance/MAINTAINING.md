# Maintaining the conformance suite

This page is for people who change the suite itself: its cases, fixtures, generators and harness. To run the
suite against an implementation, see the [README](README.md).

## Checking the suite

From this directory:

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
from codes. Validation is one case per fault at its lowest detecting level, plus prerequisite cases
(six structural faults with distinct entry paths requested at profile and full, three profile faults
requested at full, and three unsupported parts requested at profile and full) and six distinct digest
behaviors. Proposal [0001](../spec/proposals/0001-prerelease-compatibility-pass.md) part B admits its
exact 16-reader/five-validator matrix in `rulings_group`: three unsupported-data full validations and two
structural annotation validations. Its other reader faults remain declared in `faults.json`; this admission
exception does not add validation cases for them. `test_case_generation.py` checks that matrix explicitly.

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
  from a writer that is not pinned any more) is listed in [generators/preserved.json](generators/preserved.json) with
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

## Implementation test settings

These are settings and recipes of the Go and Java implementations' own tests. They add no protocol operation,
case or spec rule, and agreement between implementations is not conformance qualification.

The Go and Java adapters also accept `SEEKPARQUET_LIST_INDEX=1`: before a list request they build the
reader's optional list index, then answer the same existing cases. Run both modes to check that the
optimization changes no conformance answer. This is an adapter test setting, not a protocol capability;
it adds no query semantics or fixture expectations. Index budgets, caps and page-count guards live in
each implementation's tests and benchmarks.

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
