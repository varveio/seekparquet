# Fixture generators

Everything under `../datasets/`, `../fixtures.lock.json`, `../cases/` and `../suite.json` is produced by:

```sh
python generators/regenerate.py      # from conformance/, with generators/requirements.txt installed
```

| File | Role |
|---|---|
| `requirements.txt` | Pinned dependencies (pyarrow 26.0.1, jsonschema), Python 3.13. |
| `rows/*.json` | Source rows of each pyarrow-written dataset, as text: keys in base64, values typed as in `protocol.md` (physical values; floats by their exact bits), plus part layout (row group sizes, rows per key page) and writer settings. |
| `pyarrow_fixtures.py` | Writes `rows/*.json` from its definitions, then writes each dataset from those documents with pyarrow, and asserts the resulting physical structure. |
| `parquet_meta.py` | Pure-stdlib Thrift compact-protocol reader/writer for FileMetaData, ColumnIndex, OffsetIndex and PageHeader; prints structural summaries (`python generators/parquet_meta.py FILE...`). Decode-then-encode is asserted to reproduce pyarrow's bytes exactly. |
| `mutate_metadata.py` | Derives fixtures by editing manifests, footers and page indexes, preserving data pages, recomputing index offsets, file sizes and manifest sizes, and recording every change. |
| `faults.json` | The authored declaration of every invalid fixture's fault (reader detection, lowest validation level, codes, requirements, location, justified secondary faults). |
| `preserved.json` | Preserved regression fixtures that regeneration must never touch (empty so far). |
| `make_cases.py` | Writes `cases/generated/`, `suite.json` and `COVERAGE.md` from the rows (through `../oracle.py`) and `faults.json`. It checks `cases/core/` against its own candidates and the oracle, never rewrites it, and retires exact duplicates and cases of removed request members (`REMOVED`). |
| `test_oracle.py` | Oracle property tests and hand-specified examples. |
| `test_case_generation.py` | Generator tests: determinism, unique ids, no exact duplicates, boundary families present, one validation case per fault, the prerequisite and digest selections, honest "miss" labels. |
| `check_reproducible.py` | Regenerates everything in a temporary directory and compares with the checked-in files. |
| `test_parquet_meta.py` | Bounded malformed-input regression tests for `parquet_meta.py`. |
| `regenerate.py` | Runs the above, writes `STRUCTURE.txt` per dataset and `fixtures.lock.json`, and removes stale dataset directories. |
| `common.py` | Shared helpers. |

## What pyarrow 26.0.1 does (asserted, not assumed)

- **Page rows.** `max_rows_per_page=N` gives key pages of exactly N rows (the last page of a row group holds
  the remainder), for data page v1 and v2, PLAIN, dictionary and `DELTA_BYTE_ARRAY`. `data_page_size` and
  `write_batch_size` are not needed.
- **Row groups.** Each `ParquetWriter.write_table` call with `row_group_size >= len(table)` writes one row
  group; writing an empty table writes a 0-row row group (with a column chunk whose offsets are not
  meaningful). `multipart` has leading, interior and trailing empty row groups.
- **Stamps.** Arrow schema metadata reaches the footer key-value metadata only with `store_schema=True`
  (which also adds `ARROW:schema`); `store_schema=False` silently drops the stamps.
- **sorting_columns** is written as given, for every row group including empty ones; pyarrow neither sorts
  nor checks the rows, which is how the native `key_order` fixtures are made.
- **Bounds are never shortened.** ColumnIndex and chunk statistics hold exact page minima and maxima (a
  1001-byte key gives a 1001-byte bound). There is no truncation-length option in the Python API. Shortened
  bounds therefore come from `mutate_metadata.py` (`truncated-bounds`, `truncated-bounds-2`).
- **Large values drop the ColumnIndex.** If any key in a column chunk is 4096 bytes or longer (the
  statistics size limit), pyarrow omits that chunk's entire ColumnIndex but keeps its OffsetIndex: a
  `missing_page_index` fault produced natively (`invalid/missing-column-index-native`).
- **Physical values.** Unsigned Arrow integers are written as INT32/INT64 with an unsigned annotation;
  float columns built from bit patterns keep NaN payloads and signed zeros.
- **Encodings.** Writer `version="1.0"` with dictionaries gives PLAIN_DICTIONARY dictionary and data pages;
  `column_encoding` gives DELTA_BINARY_PACKED (INT32, INT64) and DELTA_LENGTH_BYTE_ARRAY (needs
  dictionaries off for those columns). Asserted from the page headers (`expect_encodings`).
- **Codecs.** Per-column `compression` works; `"lz4"` writes LZ4_RAW, `"brotli"` BROTLI.
- **Annotations.** `store_decimal_as_integer=True` writes DECIMAL(≤9) as INT32 and DECIMAL(≤18) as INT64;
  larger precisions are FIXED_LEN_BYTE_ARRAY of the minimum width (13 bytes for precision 30). Python's
  default `decimal` context (28 digits) silently rounds wider values; the generator uses an explicit one.
- **Determinism.** Output is byte-identical across runs (no timestamps in the footer).

The `repeated-keys`, `one-large-group` and `named-key` fixtures exercise equal keys across pages and
row groups, whole-group bounds, exact key names and tagged row-offset/prefix cursors. Every source
document and manifest declares the key column and uniqueness. Invalid fixtures declare key-order,
split-group, manifest-key and footer key-stamp faults explicitly in `faults.json`.

`check_boolean.py --adapter COMMAND` generates temporary PyArrow 26.0.1
V1/V2 Boolean datasets and compares complete real-adapter responses against
source rows, also checking PyArrow's full-scan values. It covers true/false
run boundaries, transitions, nulls, required/optional columns, four of the
mandatory codecs (UNCOMPRESSED, SNAPPY, GZIP, ZSTD), seven-row page boundaries and nineteen-row row-group boundaries.
Both reader CI jobs run it; no generated binary is retained or committed.

### Generated independent encoding and continuation qualification

`python conformance/generators/check_independent.py --adapter COMMAND` creates
16 temporary datasets using pinned PyArrow 26.0.1: data pages V1/V2, each of
UNCOMPRESSED/SNAPPY/GZIP/ZSTD, and dictionary fallback or explicit delta encoding.
Each dataset has three 1,025-row groups. Physical page headers must demonstrate
both RLE_DICTIONARY and PLAIN fallback in key/byte columns, or DELTA_BYTE_ARRAY
keys, DELTA_BINARY_PACKED integers and DELTA_LENGTH_BYTE_ARRAY values. Every
column spans multiple pages in every row group; optional binary values are all
null and some required byte values exceed 5 KiB. The PyArrow full scan must equal
the source table before adapter queries run. These generated files are discarded.

The qualifier compares each page to a direct source-row model and concatenates
results through exhaustion. Cursors come from the actual preceding response,
including repeated-key ordinals and delimiter prefixes. It uses both one-entry
pages and a changing 7/2/129/3/17 limit schedule, with prefix/end bounds and
repeated delimiters. A listing with delimiter `b` rolls the repeated `b` rows up
to the prefix `b`, which carries its exact row (LIST-8), and a request after the
row cursor `(b, 8)` must carry `(b, 9)` with its values. Hand-derived model tests
and deliberately broken adapters check duplicate-offset, prefix-repeat, missing
exact row and group-start exact row failure sensitivity independently of the
existing conformance oracle. Both real adapters execute this check in CI.

This qualifies these producer/version/geometry combinations, not every legal
Parquet encoding, dictionary width, delta block geometry, codec, or optional-page
layout. PyArrow chooses the dictionary/index bitstream; this check does not claim
exhaustive RLE-width coverage. The separate Boolean qualifier covers small RLE/
bit-packing boundaries. No malformed dictionary-free all-null page is fabricated.
This is semantic interoperability evidence, not validator or indexed-access
qualification, remote-storage performance evidence, or a production writer.
