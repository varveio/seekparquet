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
| `make_cases.py` | Writes the case files and `suite.json` from the rows (through `../oracle.py`) and the faults in the lock. |
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
- **Determinism.** Output is byte-identical across runs (no timestamps in the footer).
