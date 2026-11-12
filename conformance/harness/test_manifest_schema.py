# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0

"""Integer bounds (MAN-6, MAN-7) and extensions (EXT-1 to EXT-5) in the structural manifest schema."""

import json
from pathlib import Path
import unittest

import jsonschema


class ManifestSchemaTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        repo = Path(__file__).resolve().parents[2]
        schema = json.loads((repo / "spec/manifest.schema.json").read_text())
        jsonschema.Draft202012Validator.check_schema(schema)
        cls.validator = jsonschema.Draft202012Validator(schema)

    def manifest(self, **members):
        return {"format": "seekparquet", "format_version": 0, "draft_revision": 1,
                "schema_id": "test", "key": {"column": "key", "unique": True},
                "parts": [], **members}

    def test_draft_revision_integer_bounds(self):
        for revision in (1, 2**63 - 1):
            with self.subTest(revision=revision):
                self.validator.validate(self.manifest(draft_revision=revision))
        for revision in (0, -1, 1.5, "1", 2**63, 10**100):
            with self.subTest(revision=revision):
                self.assertFalse(self.validator.is_valid(self.manifest(draft_revision=revision)))

    def test_format_version_stays_unbounded_for_reader_version_gating(self):
        for version in (2, 2**63, 10**100):
            with self.subTest(version=version):
                manifest = self.manifest(format_version=version)
                del manifest["draft_revision"]
                self.validator.validate(manifest)

    def part(self, **members):
        return {"path": "data/p.parquet", "rows": 1, "bytes": 100, "min_key_base64": "YQ==",
                "max_key_base64": "YQ==", **members}

    def test_extensions_top_level_and_per_part(self):
        values = {"swath": {"a": [1, {"b": None}]}, "seekparquet": {"future": {"version": 9}}, "x": "text"}
        self.validator.validate(self.manifest(extensions=values, parts=[self.part(extensions=values)]))
        self.validator.validate(self.manifest(parts=[self.part(extensions={})]))
        for bad in ("text", [], None, 1, {"": 1}):
            with self.subTest(extensions=bad):
                self.assertFalse(self.validator.is_valid(self.manifest(extensions=bad)))
                self.assertFalse(self.validator.is_valid(self.manifest(parts=[self.part(extensions=bad)])))

    def test_core_stays_closed_beside_extensions(self):
        self.assertFalse(self.validator.is_valid(
            self.manifest(parts=[self.part(extensions={"swath": 1}, checksum="abc")])))
        self.assertFalse(self.validator.is_valid(self.manifest(extensions={}, created_at="2026")))


if __name__ == "__main__":
    unittest.main()
