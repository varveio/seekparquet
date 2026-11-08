#!/usr/bin/env python3
"""Self-tests of harness/run.py with deliberately deficient adapters (standard library only).

    python -m unittest discover -s harness -p 'test_*.py'      # from conformance/

Each stub adapter answers just enough of the protocol to reach the behaviour under test. A passing
reference adapter alone cannot show that the harness rejects incomplete implementations.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
RUN = HERE / "run.py"

STUB = textwrap.dedent('''
    import json, sys, time
    mode = sys.argv[1]
    caps = ["open", "range", "list", "projection"]
    targets = ["reader"]
    if mode == "lacks-capability":
        caps = ["open"]
    if mode == "unknown-target":
        targets = ["reader", "sorcerer"]
    if mode == "validator-full":
        targets, caps = ["validator-full"], ["open", "validate:structural", "validate:profile", "validate:full"]
    n = 0
    for line in sys.stdin:
        req = json.loads(line)
        n += 1
        if req["op"] == "hello":
            resp = {"status": "ok", "implementation": "stub", "version": "0", "targets": targets,
                    "capabilities": caps, "supports": [{"format_version": 0, "draft_revision": 1}]}
        elif mode == "missing-response" and n >= 2:
            time.sleep(60)
            continue
        elif req["op"] == "open":
            resp = {"status": "ok"}
        elif req["op"] in ("range", "list"):
            resp = {"status": "ok", "entries": []}
        elif req["op"] == "validate":
            resp = {"status": "ok", "levels": {"structural": {"result": "complete"}}}
        else:
            resp = {"status": "invalid_request"}
        resp["id"] = "bogus" if (mode == "wrong-id" and n >= 2) else req["id"]
        out = json.dumps(resp) + "\\n"
        sys.stdout.write(out)
        if mode == "duplicate":
            sys.stdout.write(out)
        sys.stdout.flush()
    if mode == "nonzero-exit":
        sys.exit(7)
    if mode == "no-exit":
        time.sleep(60)
''')


class HarnessSelfTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.stub = Path(cls.tmp.name) / "stub_adapter.py"
        cls.stub.write_text(STUB, encoding="utf-8")

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def run_harness(self, mode, *args, timeout=60):
        cmd = [sys.executable, str(RUN), "--adapter", f"{sys.executable} {self.stub} {mode}", "--json",
               "--timeout", "2", "--shutdown-timeout", "1", *args]
        t0 = time.time()
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        report = json.loads(p.stdout) if p.stdout.strip().startswith("{") else {}
        return p.returncode, report, time.time() - t0

    def test_correct_partial_run_passes_but_does_not_qualify(self):
        rc, rep, _ = self.run_harness("good", "--filter", "^open/basic$")
        self.assertEqual(rc, 0, rep)
        self.assertTrue(rep["selected_cases_passed"])
        self.assertTrue(rep["selection"]["partial"])
        self.assertEqual(rep["qualified_targets"], [])
        self.assertEqual(rep["selection"]["filter"], "^open/basic$")

    def test_wrong_answers_fail(self):
        rc, rep, _ = self.run_harness("good", "--filter", "^range/basic/all$")
        self.assertEqual(rc, 1)
        self.assertEqual(rep["counts"]["fail"], 1)

    def test_full_run_of_incomplete_reader_fails_and_does_not_qualify(self):
        rc, rep, _ = self.run_harness("good")
        self.assertEqual(rc, 1)
        self.assertFalse(rep["selection"]["partial"])
        self.assertEqual(rep["qualified_targets"], [])
        self.assertGreater(rep["counts"]["fail"], 100)

    def test_nonzero_exit_is_a_run_error(self):
        rc, rep, _ = self.run_harness("nonzero-exit", "--filter", "^open/basic$")
        self.assertEqual(rc, 1)
        self.assertTrue(any("status 7" in e for e in rep["errors"]), rep["errors"])

    def test_duplicate_response(self):
        rc, rep, _ = self.run_harness("duplicate", "--filter", "^open/basic$")
        self.assertEqual(rc, 1)
        self.assertTrue(rep["errors"])

    def test_wrong_response_id(self):
        rc, rep, _ = self.run_harness("wrong-id", "--filter", "^open/basic$")
        self.assertEqual(rc, 1)
        self.assertTrue(any("id" in e for e in rep["errors"]), rep["errors"])

    def test_missing_response_times_out_and_is_reaped(self):
        rc, rep, elapsed = self.run_harness("missing-response", "--filter", "^open/(basic|multipart)$")
        self.assertEqual(rc, 1)
        self.assertTrue(any("no response" in e for e in rep["errors"]), rep["errors"])
        self.assertLess(elapsed, 30, "the harness must kill and reap a hung adapter")

    def test_adapter_that_does_not_exit_is_killed(self):
        rc, rep, elapsed = self.run_harness("no-exit", "--filter", "^open/basic$")
        self.assertEqual(rc, 1)
        self.assertTrue(any("did not exit" in e for e in rep["errors"]), rep["errors"])
        self.assertLess(elapsed, 30)

    def test_claimed_target_without_capability(self):
        rc, rep, _ = self.run_harness("lacks-capability")
        self.assertEqual(rc, 1)
        self.assertTrue(any("without capabilities" in e for e in rep["errors"]), rep["errors"])

    def test_unknown_target_name(self):
        rc, rep, _ = self.run_harness("unknown-target")
        self.assertEqual(rc, 1)
        self.assertTrue(any("unknown target" in e for e in rep["errors"]), rep["errors"])

    def test_dependencies_run_with_the_claimed_target(self):
        rc, rep, _ = self.run_harness("validator-full", "--targets", "validator-full")
        self.assertEqual(sorted(rep["selection"]["selected_targets"]),
                         ["validator-full", "validator-profile", "validator-structural"])
        self.assertGreater(rep["targets"]["validator-structural"]["fail"] + rep["targets"]["validator-structural"]["pass"], 0)
        self.assertEqual(rc, 1)  # the stub's validation reports are wrong
        self.assertEqual(rep["qualified_targets"], [])

    def test_narrower_targets_is_partial(self):
        rc, rep, _ = self.run_harness("validator-full", "--targets", "validator-structural")
        self.assertTrue(rep["selection"]["partial"])
        self.assertEqual(rep["qualified_targets"], [])


if __name__ == "__main__":
    unittest.main()
