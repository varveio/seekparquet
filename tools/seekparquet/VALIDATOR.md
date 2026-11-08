# Local dataset validator

`seekparquet-validate DATASET --level full` prints the per-level report from
`seekparquet.validate.validate_dataset(path, level='full', limits=Limits())`.
Levels are `structural`, `profile`, `full`, and the independent `digest`.
Exit codes are 0 for complete, 1 for a failed level, and 2 for incomplete.
A failed level names the stable refusal code. Incomplete results distinguish
unsupported features, storage errors, configured limits and prerequisites.
Invalid API level/limit arguments raise `ValueError`; the CLI rejects malformed
arguments through argparse.

The validator walks physical column-chunk page headers using footer chunk
extents before inspecting key OffsetIndex entries. It checks physical offsets,
page sizes, counts and required-key row positions against the index; a plausible
index omitting a physical page fails. Profile validation decodes all keys in row
group order using PyArrow without filters, then checks byte ordering, page bounds,
UTF-8 when applicable, and exact manifest bounds. Full validation enumerates all
stored column chunks, checks physical value totals against column metadata, and
asks PyArrow to decode every complete row group with page CRC verification.
Digest streams the declared parts without opening them as Parquet, including
when structural validation would fail. No declared hashes means complete with
zero checked, explicitly reported.

This implementation shares the bounded compact Thrift codec's provenance with
`conformance/generators/parquet_meta.py`. Its validation control flow and checks
are implemented separately. The shipped modules import neither the reference
adapter nor the oracle, expected fixture outcomes, or producer source rows.
PyArrow supplies decoding, so the validator is independent of the Go and Java
reader implementations, but is not an independent implementation of PyArrow's
Parquet decoding or of the fixture metadata codec.

Only immutable local snapshots and format 0 draft revision 1 are supported.
Validate a producer output before publishing it. The validator does not establish
atomic publication, source completeness, remote object identity, or concurrent
mutation safety. A digest does not establish those properties either.

`Limits` caps manifest, footer/index, part, individual page bytes, page counts,
part counts, row group rows and aggregate declared uncompressed page bytes per
selected row group. Limit exhaustion is incomplete. The low-level codec also
bounds nesting, collection lengths and binary field lengths. These limits bound
the inspected sizes and declared decompression work; they are **not a total heap
budget**. PyArrow's table allocation, dictionary expansion, logical type
conversion and transient buffers can exceed the sum of uncompressed page sizes.
Run untrusted files under an OS memory limit if a strict process bound is needed.

LZO, unavailable build codecs, and legacy GROUP_VAR_INT/BIT_PACKED value page
encodings produce incomplete at the level needing them. Arrow reports additional
unimplemented features as incomplete when it raises ArrowNotImplementedError.
Unknown codec/encoding enums are corrupt data. A decoder error for a supported
feature is `invalid_parquet`; storage mapping/open errors are incomplete storage.
The supported feature boundary may need extension as independent producers expose
additional valid Parquet features. No format freeze or universal Parquet feature
coverage is claimed.

Source-checkout checks:

```sh
python -m unittest discover -s tools/seekparquet/tests -v
python conformance/harness/run.py \
  --adapter 'python tools/seekparquet/tests/validator_adapter.py' \
  --targets validator-structural,validator-profile,validator-full,validator-digest
```

The test adapter translates the public API to the conformance transport; it is
not a reader. The corpus test runs every validator case, every valid dataset at
full level, and every invalid dataset at full/digest levels. Generated temporary
fixtures additionally cover page V1/V2 and the four mandatory codecs, omitted
physical pages, corruption confined to a value page, digest independence, limits,
unsupported codecs and malformed manifests. No new binary fixtures are committed.
