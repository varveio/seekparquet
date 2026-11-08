#!/usr/bin/env python3
"""Tests of the case generator (standard library only; reads the checked-in fixtures.lock.json and core cases).

    python -m unittest generators/test_case_generation.py          # from conformance/
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONF = HERE.parent
sys.path.insert(0, str(HERE))

import make_cases  # noqa: E402


def build():
    lock = json.loads((CONF / "fixtures.lock.json").read_text(encoding="utf-8"))
    return make_cases.build_all(lock)


class Generation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.groups, cls.core, cls.core_files, cls.generated, cls.retired = build()
        cls.allgen = [c for v in cls.generated.values() for c in v]

    def test_deterministic(self):
        _g, _c, _f, generated, retired = build()
        self.assertEqual(json.dumps(generated, sort_keys=True), json.dumps(self.generated, sort_keys=True))
        self.assertEqual(retired, self.retired)

    def test_unique_ids_and_no_core_overlap(self):
        ids = [c["id"] for c in self.allgen] + list(self.core)
        self.assertEqual(len(ids), len(set(ids)))
        self.assertFalse({c["id"] for c in self.allgen} & set(self.core))

    def test_no_exact_duplicates(self):
        seen = {}
        for c in self.allgen + [c for _d, c in self.core.values()]:
            b = make_cases.behaviour(c)
            self.assertNotIn(b, seen, f"{c['id']} duplicates {seen.get(b)}")
            seen[b] = c["id"]

    def test_retired_ids_are_not_active(self):
        active = {c["id"] for c in self.allgen} | set(self.core)
        for r in self.retired:
            self.assertNotIn(r["id"], active)
            if "duplicate_of" in r:
                self.assertIn(r["duplicate_of"], active)
            else:
                self.assertTrue(r["reason"], r["id"])

    def test_boundary_categories_retained(self):
        fams = {c["family"] for c in self.allgen}
        for f in ("range/boundary-page", "range/boundary-row-group", "range/boundary-part", "range/seek-key",
                  "range/start-just-above-key", "prefix/prefix-only", "pagination/list-scenario"):
            self.assertIn(f, fams)
        parts = sum(1 for c in self.allgen if c["family"] == "range/boundary-part")
        self.assertGreaterEqual(parts, 30)

    def test_every_fault_once_at_its_lowest_level(self):
        faults = make_cases.load_faults()
        lowest = {}
        for c in self.allgen + [c for _d, c in self.core.values()]:
            if c.get("fault_id") and c["id"].split("/")[0].startswith("validate-") and "/prerequisite/" not in c["id"] \
                    and not c["id"].startswith("validate-digest/"):
                lowest.setdefault(c["fault_id"], []).append(c["id"])
        for fid, f in faults.items():
            if f["validation_level"] == "digest":
                continue
            self.assertEqual(len(lowest.get(fid, [])), 1, fid)
            self.assertTrue(lowest[fid][0].startswith(f"validate-{f['validation_level']}/"), fid)

    def test_selected_prerequisites_and_digests(self):
        allc = self.allgen + [c for _d, c in self.core.values()]
        prereq = [c for c in allc if "/prerequisite/" in c["id"]]
        self.assertEqual(len(prereq), 15)
        self.assertEqual(len([c for c in allc if c["id"].startswith("validate-digest/")]), 6)

    def test_reader_cases_follow_faults_json(self):
        faults = make_cases.load_faults()
        allc = {c["id"]: c for c in self.allgen + [c for _d, c in self.core.values()]}
        for fid, f in faults.items():
            o, r = allc.get(f"refusals-open/{fid}"), allc.get(f"refusals-read/{fid}")
            self.assertEqual(o is not None, f["reader_detect"] in ("open", "touch"), fid)
            self.assertEqual(r is not None, f["reader_detect"] == "read", fid)

    def test_incomplete_never_broadly_accepted(self):
        allowed = set(make_cases.OPTIONAL) | set(make_cases.NESTED_COLUMNS)
        for c in self.allgen + [c for _d, c in self.core.values()]:
            for lvl, v in c["expect"].get("levels", {}).items():
                if "incomplete" in v.get("accept", []):
                    self.assertEqual(lvl, "full", c["id"])
                    self.assertIn(c["dataset"].split("/")[-1], allowed, c["id"])

    def test_miss_labels_are_misses(self):
        import oracle
        keys = set(oracle.Dataset.load("binary-keys").keys())
        for c in self.allgen:
            if c["family"] == "range/start-just-above-key":
                import base64
                start = base64.b64decode(c["request"]["start_base64"])
                self.assertEqual("which is itself a key" in c["description"], start in keys, c["id"])

    def test_core_files_are_authored(self):
        for path, doc in self.core_files:
            self.assertIs(doc["generated"], False, path)
            for c in doc["cases"]:
                self.assertGreaterEqual(len(c.get("description", "")), 20, c["id"])


if __name__ == "__main__":
    unittest.main()
