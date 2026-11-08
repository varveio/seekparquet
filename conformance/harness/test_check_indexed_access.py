# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Failure-path tests for the deterministic access regression runner."""
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_indexed_access as check


class IndexedAccessRunnerTest(unittest.TestCase):
    def test_missing_tool_is_failure(self):
        result, log = check.run(["/nonexistent/access-test-tool"], Path.cwd(), os.environ, 1)
        self.assertEqual(127, result["exit_code"])
        self.assertTrue(log)

    def test_timeout_terminates_process(self):
        result, _ = check.run([sys.executable, "-c", "import time; time.sleep(30)"],
                              Path.cwd(), os.environ, 0.05)
        self.assertTrue(result["timed_out"])
        self.assertNotEqual(0, result["exit_code"])

    def test_one_passing_go_test_cannot_mask_missing_selection(self):
        log = json.dumps({"Action": "pass", "Test": check.GO_TESTS[0]})
        with mock.patch.object(check, "run", return_value=({"exit_code": 0}, log)):
            with contextlib.redirect_stderr(io.StringIO()):
                result = check.run_go(Path.cwd(), os.environ, 1)
        self.assertFalse(result["passed"])
        self.assertEqual(set(check.GO_TESTS[1:]), set(result["missing_tests"]))

    def test_stale_java_reports_cannot_mask_missing_current_reports(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reports = root / "java/seekparquet/build/test-results/test"
            reports.mkdir(parents=True)
            for name in check.JAVA_CLASSES:
                (reports / f"TEST-io.varve.seekparquet.{name}.xml").write_text(
                    '<testsuite><testcase name="oldSuccess"/></testsuite>')
            with mock.patch.object(check, "run", return_value=({"exit_code": 0}, "")):
                with contextlib.redirect_stderr(io.StringIO()):
                    result = check.run_java(root, os.environ, 1)
            self.assertFalse(result["passed"])
            self.assertEqual(list(check.JAVA_CLASSES), result["missing_tests"])
            self.assertEqual([], list(reports.iterdir()))

    def test_skipped_test_does_not_qualify(self):
        result = {"exit_code": 0, "missing_tests": [],
                  "tests": [{"name": "ignored", "status": "skip"}]}
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertFalse(check.finish(result, "")["passed"])


if __name__ == "__main__":
    unittest.main()
