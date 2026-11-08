# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0

"""Integer bounds in the structural manifest schema (MAN-6 and MAN-7)."""

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


if __name__ == "__main__":
    unittest.main()
