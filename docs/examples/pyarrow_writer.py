# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Write a one-part SeekParquet dataset with pyarrow: the recipe in docs/guides/producing.md.

Usage: python3 docs/examples/pyarrow_writer.py DIR   (DIR must not exist)

CI runs it with the pinned pyarrow and validates the result at the full level.
"""
# tag::recipe[]
import base64, json, os, sys
import pyarrow as pa
import pyarrow.parquet as pq

root = sys.argv[1]  # the dataset root to create
schema_id = "example/objects/v1"
keys = sorted(f"logs/2026/{i:05d}.json".encode() for i in range(5000))  # bytes sort unsigned
stamps = {
    "seekparquet.format_version": "0",
    "seekparquet.draft_revision": "1",
    "seekparquet.schema_id": schema_id,
    "seekparquet.key_column": "key",
    "seekparquet.key_unique": "true",
}
schema = pa.schema(
    [pa.field("key", pa.binary(), nullable=False), pa.field("size", pa.int64())],
    metadata=stamps,
)
table = pa.table({"key": keys, "size": [len(k) * 100 for k in keys]}, schema=schema)

part = "data/part-00000.parquet"
os.makedirs(os.path.join(root, "data"))
pq.write_table(
    table,
    os.path.join(root, part),
    write_page_index=True,
    sorting_columns=[pq.SortingColumn(0)],  # column 0 is the key
    data_page_size=1,  # with write_batch_size, ends a page every 1,024 rows
    write_batch_size=1024,
)

manifest = {
    "format": "seekparquet",
    "format_version": 0,
    "draft_revision": 1,
    "schema_id": schema_id,
    "key": {"column": "key", "unique": True},
    "parts": [{
        "path": part,
        "rows": table.num_rows,
        "bytes": os.path.getsize(os.path.join(root, part)),
        "min_key_base64": base64.b64encode(keys[0]).decode(),
        "max_key_base64": base64.b64encode(keys[-1]).decode(),
    }],
}
with open(os.path.join(root, "manifest.json"), "w") as f:
    json.dump(manifest, f)
# end::recipe[]
