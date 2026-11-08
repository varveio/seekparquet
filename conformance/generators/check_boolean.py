#!/usr/bin/env python3
"""Generated-only independent Boolean regression. Requires the pinned PyArrow.

Write V1/V2 required/optional Boolean columns, compare PyArrow full scans and
real adapter responses with source rows. No generated artifact enters Git.
"""
import argparse
import base64
import json
from pathlib import Path
import shlex
import subprocess
import tempfile
import threading

import pyarrow as pa
import pyarrow.parquet as pq


def b64(value):
    return base64.b64encode(value).decode()


def check(adapter):
    if pa.__version__ != "26.0.1":
        raise RuntimeError(f"requires PyArrow 26.0.1; got {pa.__version__}")
    patterns = [[bit] * n for n in (1, 2, 7, 8, 9, 15, 16, 17) for bit in (False, True)]
    patterns += [[True] * 3 + [False] * 2 + [True, False] * 8 + [True] * 9,
                 [True, False, None, True, None] * 13, [None] * 17]
    count = 0
    with tempfile.TemporaryDirectory(prefix="seekparquet-boolean-") as directory:
        with subprocess.Popen(shlex.split(adapter), stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, text=True) as process:
            # Bound the adapter's lifetime, including a missing response or exit.
            watchdog = threading.Timer(120, process.kill)
            watchdog.daemon = True
            watchdog.start()
            for version in ("1.0", "2.0"):
                for codec in ("none", "snappy", "gzip", "zstd"):
                    for optional in (False, True):
                        for pattern, page_rows, group_rows in ((p, page, group) for p in patterns
                                                                  for page, group in ((128, 128), (7, 19))):
                            if not optional and None in pattern:
                                continue
                            root = Path(directory) / str(count)
                            root.mkdir()
                            keys = [f"k{i:04d}".encode() for i in range(len(pattern))]
                            stamps = {"format_version": "0", "draft_revision": "1",
                                      "schema_id": "boolean-regression", "key_column": "key", "key_unique": "true"}
                            schema = pa.schema([pa.field("key", pa.binary(), nullable=False),
                                                pa.field("flag", pa.bool_(), nullable=optional)],
                                               metadata={f"seekparquet.{k}": v for k, v in stamps.items()})
                            table = pa.Table.from_arrays([pa.array(keys), pa.array(pattern, type=pa.bool_())], schema=schema)
                            path = root / "part.parquet"
                            pq.write_table(table, path, data_page_version=version, compression=codec,
                                           use_dictionary=False, write_page_index=True,
                                           sorting_columns=[pq.SortingColumn(0)], max_rows_per_page=page_rows,
                                           row_group_size=group_rows, write_batch_size=page_rows)
                            assert pq.read_table(path)["flag"].to_pylist() == pattern
                            manifest = {"format": "seekparquet", "format_version": 0, "draft_revision": 1,
                                        "schema_id": "boolean-regression", "key": {"column": "key", "unique": True},
                                        "parts": [{"path": path.name, "rows": len(keys), "bytes": path.stat().st_size,
                                                   "min_key_base64": b64(keys[0]), "max_key_base64": b64(keys[-1])}]}
                            (root / "manifest.json").write_text(json.dumps(manifest))
                            request = {"id": str(count), "op": "range", "dataset": str(root),
                                       "limit": len(keys) + 1, "columns": ["flag"]}
                            process.stdin.write(json.dumps(request) + "\n")
                            process.stdin.flush()
                            response = json.loads(process.stdout.readline())
                            expected = [{"kind": "row", "key_base64": b64(key), "offset": 0,
                                         "values": [{"null": True} if value is None else {"bool": value}]}
                                        for key, value in zip(keys, pattern)]
                            if response.get("status") != "ok" or response.get("entries") != expected:
                                raise AssertionError((version, codec, optional, pattern, response))
                            count += 1
            process.stdin.close()
            if process.wait(timeout=20) != 0:
                raise RuntimeError("adapter exited unsuccessfully")
            watchdog.cancel()
    print(f"independent Boolean regression: {count} cases passed ({adapter})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adapter", required=True)
    check(parser.parse_args().adapter)
