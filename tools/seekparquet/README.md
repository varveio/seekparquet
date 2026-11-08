# Python producer and validator (draft revision 1)

This small supported producer writes a **new local immutable dataset** from an
already sorted, in-memory `pyarrow.Table`. It supports Python 3.11+ and pins
PyArrow 26.0.1. It stamps format version 0, draft revision 1; it does not stamp
the reserved final format version 1.

```sh
python -m pip install -e tools/seekparquet
seekparquet-write sorted.parquet /new/dataset --schema-id example/v1 --key path --hash-parts
```

```python
import pyarrow as pa
from seekparquet import write_dataset

rows = pa.table({"path": ["a/one", "a/two", "b/one"], "size": [10, 20, 30]})
manifest = write_dataset(rows, "/new/dataset", schema_id="example/v1", key="path",
                         target_rows_per_part=2, hash_parts=True)
```

The named top-level key must be Arrow `binary` or `string`, have no null or empty
values, and be sorted in unsigned lexicographic byte order (UTF-8 bytes for
strings). The producer persists it as a required Parquet BYTE_ARRAY column.
Unique mode is the default; use `unique=False` in Python or `--repeated` on the
CLI to allow adjacent duplicates. It never sorts input or changes order within
a repeated group. Duplicate column names are refused. Other columns are passed
to PyArrow; reader projection support still follows COL-3 of the format.

`target_rows_per_part` (default 1,000,000) is a best-effort row target. If its
boundary lands inside a repeated group, that part extends through the complete
group, even if the group alone is larger than the target. Row groups (default
65,536 rows) and pages may split groups. Parts use ZSTD, plain values, Data Page
V2, page indexes and ascending sorting declarations. Pages target at most 1,024
rows. Counts, byte sizes and exact first/last bounds come from the produced
parts and input; `hash_parts=True` adds SHA-256. Empty input publishes a manifest
with no parts. There is no format key-length maximum: if this pinned writer
omits required indexes (for example for very long keys), the producer raises
`ValueError` and leaves the output unpublished.

The destination directory must not exist, including as an empty directory or
symlink. The producer never replaces a dataset or deletes existing files.
It closes and fsyncs every part, fsyncs containing directories, then writes and
fsyncs a temporary manifest and atomically renames it to `manifest.json` before
fsyncing the root again. This requires a local POSIX filesystem with directory
fsync and atomic same-directory rename; unsupported storage fails with an I/O
error. The caller must keep the destination private while writing and preserve
published parts unchanged. File permissions do not provide immutable storage.

A failure before publication leaves an inspectable directory without a manifest
and therefore no committed dataset; use a new output path for a retry. An error
from the final directory fsync can occur after the manifest becomes visible, so
an exception alone does not establish absence of publication. Replacement,
version retention, garbage collection, object stores and streaming/external
sorting are outside this producer's scope. It retains the input plus a Python
copy of all key bytes in memory. A successful write is not a claim of complete
independent validation or source completeness.

Run tests from the repository root:

```sh
python -m unittest discover -s tools/seekparquet/tests -v
```

Set `SEEKPARQUET_TEST_ADAPTERS` to a JSON array of Go/Java adapter commands to run
input-derived interoperability checks. CI runs both real readers. Test files
are generated in temporary directories; no binary fixtures are committed.

## Validate the stored output

Run `seekparquet-validate /new/dataset --level full` and, when hashes are present,
`seekparquet-validate /new/dataset --level digest` before exposing the snapshot to consumers.
See [validator behavior and limits](VALIDATOR.md) for per-level reports, unsupported
features, resource bounds, and codec provenance. Validation does not prove source completeness.
