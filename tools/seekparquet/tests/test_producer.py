import base64
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from seekparquet import write_dataset


class ProducerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "dataset"

    def write(self, keys, **kwargs):
        table = pa.table({"key": pa.array(keys, type=pa.binary()), "n": range(len(keys)),
                          "flag": [True] * len(keys)})
        return write_dataset(table, self.root, schema_id="test/v1", **kwargs)

    def test_invalid_keys_leave_no_output(self):
        for keys in ([b"z", b"a"], [b"a", b"a"], [b""], [None]):
            with self.subTest(keys=keys), self.assertRaises(ValueError):
                self.write(keys)
            self.assertFalse(self.root.exists())

    def test_invalid_schema_and_options(self):
        for table in (pa.table({"other": ["a"]}), pa.table({"key": [1]})):
            with self.assertRaises(ValueError):
                write_dataset(table, self.root, schema_id="test")
        for option in ({"schema_id": "bad\n"}, {"target_rows_per_part": 0}, {"unique": 1}):
            args = {"schema_id": "test", **option}
            with self.assertRaises((ValueError, TypeError)):
                write_dataset(pa.table({"key": ["a"]}), self.root, **args)
        self.assertFalse(self.root.exists())

    def test_group_boundaries_and_large_group(self):
        keys = [b"a"] * 9 + [b"b", b"c", b"d", b"e"]
        manifest = self.write(keys, unique=False, target_rows_per_part=3, row_group_size=2, hash_parts=True)
        self.assertEqual([p["rows"] for p in manifest["parts"]], [9, 3, 1])
        self.assertEqual(json.loads((self.root / "manifest.json").read_text()), manifest)
        actual = []
        for part in manifest["parts"]:
            path = self.root / part["path"]
            rows = pq.read_table(path)
            actual.extend(rows["key"].to_pylist())
            self.assertEqual(part["bytes"], path.stat().st_size)
            self.assertEqual(part["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertEqual(base64.b64decode(part["min_key_base64"]), rows["key"][0].as_py())
            self.assertEqual(base64.b64decode(part["max_key_base64"]), rows["key"][-1].as_py())
            metadata = pq.read_metadata(path)
            self.assertFalse(rows.schema.field("key").nullable)
            self.assertEqual(metadata.metadata[b"seekparquet.key_unique"], b"false")
            for i in range(metadata.num_row_groups):
                group = metadata.row_group(i)
                self.assertEqual(group.sorting_columns[0].column_index, 0)
                self.assertTrue(group.column(0).has_column_index)
                self.assertTrue(group.column(0).has_offset_index)
        self.assertEqual(actual, keys)

    def test_long_key_never_publishes_missing_indexes(self):
        # Writer statistics limits are implementation limits, not format limits.
        try:
            self.write([b"x" * 8192])
        except ValueError as error:
            self.assertIn("indexes", str(error))
            self.assertFalse((self.root / "manifest.json").exists())
        else:
            metadata = pq.read_metadata(self.root / "data/part-00000.parquet")
            self.assertTrue(metadata.row_group(0).column(0).has_column_index)

    def test_metadata_is_replaced(self):
        table = pa.table({"key": ["a"]}).replace_schema_metadata({
            b"seekparquet.schema_id": b"stale", b"seekparquet.unknown": b"stale"})
        write_dataset(table, self.root, schema_id="fresh")
        metadata = pq.read_metadata(self.root / "data/part-00000.parquet").metadata
        self.assertEqual(metadata[b"seekparquet.schema_id"], b"fresh")
        self.assertNotIn(b"seekparquet.unknown", metadata)

    def test_empty(self):
        manifest = self.write([])
        self.assertEqual(manifest["parts"], [])
        self.assertEqual(list((self.root / "data").iterdir()), [])

    def test_string_and_unsigned_bytes(self):
        self.write([b"a", b"\x80", b"\xff"], target_rows_per_part=1)
        other = Path(self.tmp.name) / "strings"
        write_dataset(pa.table({"k": ["a", "é", "中"]}), other, key="k", schema_id="utf8")
        self.assertEqual(pq.read_table(other / "data/part-00000.parquet")["k"].to_pylist(), ["a", "é", "中"])

    def test_existing_directory_is_untouched(self):
        self.root.mkdir()
        sentinel = self.root / "manifest.json"
        sentinel.write_text("user content")
        with self.assertRaises(FileExistsError):
            self.write([b"a"])
        self.assertEqual(sentinel.read_text(), "user content")
        self.assertEqual(len(list(self.root.iterdir())), 1)

    def test_failure_does_not_publish(self):
        with patch("seekparquet.producer.os.rename", side_effect=OSError("injected")):
            with self.assertRaises(OSError):
                self.write([b"a", b"b"])
        self.assertFalse((self.root / "manifest.json").exists())
        self.assertTrue((self.root / "data/part-00000.parquet").exists())

    def test_post_publication_sync_failure_can_leave_manifest(self):
        from seekparquet.producer import _sync_dir

        def injected_sync(path):
            if (self.root / "manifest.json").exists():
                raise OSError("injected post-publication fsync failure")
            _sync_dir(path)

        with patch("seekparquet.producer._sync_dir", side_effect=injected_sync):
            with self.assertRaises(OSError):
                self.write([b"a"])
        self.assertTrue((self.root / "manifest.json").exists())
        self.assertEqual(json.loads((self.root / "manifest.json").read_text())["parts"][0]["rows"], 1)

    def test_part_failure_does_not_publish(self):
        with patch("seekparquet.producer.pq.write_table", side_effect=OSError("injected")):
            with self.assertRaises(OSError):
                self.write([b"a"])
        self.assertFalse((self.root / "manifest.json").exists())

    @unittest.skipUnless(os.environ.get("SEEKPARQUET_TEST_ADAPTERS"), "set JSON array of real Go/Java adapter commands")
    def test_real_readers(self):
        # Expected rows are derived directly from the input, not another adapter.
        sources = ([b"a", b"b", b"\xff"], [b"a"] * 9 + [b"b", b"c"], [])
        for case, keys in enumerate(sources):
            self.root = Path(self.tmp.name) / f"reader-{case}"
            self.write(keys, unique=case != 1, target_rows_per_part=3, row_group_size=2)
            for command in json.loads(os.environ["SEEKPARQUET_TEST_ADAPTERS"]):
                request = {"id": "read", "op": "range", "dataset": str(self.root),
                           "limit": 100, "columns": ["n", "flag"]}
                result = subprocess.run(shlex.split(command), input=json.dumps(request) + "\n",
                                        text=True, capture_output=True, check=True, timeout=60)
                response = json.loads(result.stdout)
                self.assertEqual(response["status"], "ok", response)
                expected = [{"key_base64": base64.b64encode(k).decode(),
                             "values": [{"int": str(i)}, {"bool": True}]} for i, k in enumerate(keys)]
                actual = [{"key_base64": row["key_base64"], "values": row["values"]}
                          for row in response["entries"]]
                self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
