# Python validator oracle

`validate.py` is a second, independent check of a dataset against the four validation levels of
[§8 of the specification](../../spec/format-1.md). It exists so that CI can examine every dataset a writer
under test produces with code that shares nothing with the Go and Java implementations. It is conformance
tooling, not a supported validator: it is not packaged, versioned or documented for users. The supported
validator is the one in the Go module.

```sh
python conformance/validator/validate.py DATASET --level full    # prints the per-level report as JSON
```

Exit status is 0 when every reported level is complete, 1 when one failed, 2 when one is incomplete.

## What each level checks

- **structural**: the manifest, and for every part its footer and page indexes. It walks the physical page
  headers of each column chunk from the extents in the footer, and only then compares the key column's
  OffsetIndex against what it found: offsets, page sizes, value counts and first-row positions. A plausible
  index that omits a physical page fails.
- **profile**: decodes every key in row group order through PyArrow without filters, then checks byte order,
  page bounds against the ColumnIndex, UTF-8 where the key is annotated so, and the manifest's exact bounds.
- **full**: enumerates every stored column chunk, checks physical value totals against the column metadata,
  and has PyArrow decode every row group with page CRC verification.
- **digest**: streams the declared parts without opening them as Parquet, so it runs even when structural
  validation fails. With no declared hashes it reports complete with zero checked.

Resource limits (`Limits`) and unsupported features (LZO, build codecs PyArrow lacks, legacy
GROUP_VAR_INT and BIT_PACKED value encodings) make a level incomplete, not failed. Unknown codec or encoding
enums are corrupt data; a decoder error on a supported feature is `invalid_parquet`.

## Independence

Decoding is PyArrow's, and the page walk uses the Thrift codec in
[`../generators/parquet_meta.py`](../generators/parquet_meta.py), so the validator shares no code with
parquet-go or the Java readers, and does not trust the OffsetIndex under examination. It imports neither the
reference adapter, the oracle, the expected case outcomes nor source rows.

## Known limits

- The compact Thrift codec is shared with the fixture tooling, so a codec bug would affect both the fixtures
  and this validator. The validator's control flow and checks are separate. It is not an independent
  implementation of PyArrow's Parquet decoding.
- The limits bound inspected sizes and declared decompression work. They are not a total memory bound:
  PyArrow's table allocation, dictionary expansion and transient buffers can exceed the sum of declared page
  sizes. Run untrusted files under an OS memory limit.
- Only immutable local snapshots of format 0, draft revision 1 are supported. Atomic publication, source
  completeness and concurrent mutation are not examined.

## Tests

```sh
python -m unittest discover -s conformance/validator -v
python conformance/harness/run.py --adapter 'python conformance/validator/validator_adapter.py' \
  --targets validator-structural,validator-profile,validator-full,validator-digest
```

`test_validator_corpus.py` runs every validator case, validates every valid corpus dataset at every level
(all complete), and requires each invalid one to fail at some level, except a fault whose outcome is
unsupported (an encrypted part), whose structural level must be incomplete. `test_validate.py` builds
temporary fixtures for page V1 and V2, four of the mandatory codecs, omitted physical pages, corruption confined
to a value page, digest independence, limits, unsupported codecs and malformed manifests. No binary fixtures
are committed.
