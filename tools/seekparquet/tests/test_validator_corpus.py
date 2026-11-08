"""Qualification uses the public API and the existing conformance protocol.

Case expectations are read only by the harness, never by the shipped validator.
The complete corpus lives in source checkouts; installed wheels omit it.
"""
import json
from pathlib import Path
import subprocess
import sys
import unittest
from seekparquet.validate import validate_dataset

ROOT = Path(__file__).resolve().parents[3]
CONF = ROOT / 'conformance'


@unittest.skipUnless((CONF / 'suite.json').is_file(), 'source conformance corpus required')
class CorpusTest(unittest.TestCase):
    def test_all_validator_cases(self):
        adapter = Path(__file__).with_name('validator_adapter.py')
        run = subprocess.run([sys.executable, str(CONF / 'harness' / 'run.py'), '--adapter',
                              f'{sys.executable} {adapter}', '--targets',
                              'validator-structural,validator-profile,validator-full,validator-digest', '--json'],
                             text=True, capture_output=True, cwd=ROOT, check=False)
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        report = json.loads(run.stdout)
        self.assertEqual(set(report['qualified_targets']),
                         {'validator-' + x for x in ('structural', 'profile', 'full', 'digest')})
    def test_every_valid_dataset(self):
        for path in sorted((CONF / 'datasets').iterdir()):
            if path.is_dir() and path.name != 'invalid':
                with self.subTest(dataset=path.name):
                    report = validate_dataset(path)
                    self.assertTrue(all(x['result'] == 'complete' for x in report.values()), report)
    def test_every_invalid_dataset(self):
        for path in sorted((CONF / 'datasets' / 'invalid').iterdir()):
            if not path.is_dir():
                continue
            with self.subTest(dataset=path.name):
                report = validate_dataset(path)
                digest = validate_dataset(path, 'digest')
                self.assertTrue(any(x['result'] == 'failed' for x in [*report.values(), *digest.values()]),
                                (report, digest))


if __name__ == '__main__':
    unittest.main()
