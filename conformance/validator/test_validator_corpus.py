"""Qualification of validate.py over the suite's corpus, through the conformance protocol.

Case expectations are read only by the harness, never by the validator.
"""
import json
from pathlib import Path
import subprocess
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parent))
from validate import validate_dataset  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
CONF = ROOT / 'conformance'


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
        # Unsupported features make their declared detecting level incomplete; lower levels still complete.
        faults = json.loads((CONF / 'generators' / 'faults.json').read_text(encoding='utf-8'))['faults']
        unsupported = {f['id']: f for f in faults if f.get('outcome') == 'unsupported'}
        for path in sorted((CONF / 'datasets' / 'invalid').iterdir()):
            if not path.is_dir():
                continue
            with self.subTest(dataset=path.name):
                report = validate_dataset(path)
                digest = validate_dataset(path, 'digest')
                if path.name in unsupported:
                    fault = unsupported[path.name]
                    low = fault['validation_level']
                    self.assertEqual(report[low]['result'], 'incomplete', report)
                    self.assertEqual(report[low]['code'], fault['codes'][0], report)
                    self.assertTrue(report[low]['reason'], report)
                    if low == 'structural':
                        self.assertTrue(report[low]['reason'].startswith(fault['codes'][0]), report)
                    levels = ('structural', 'profile', 'full')
                    for level in levels[:levels.index(low)]:
                        self.assertEqual(report[level]['result'], 'complete', report)
                    for level in levels[levels.index(low) + 1:]:
                        self.assertEqual(report[level]['result'], 'incomplete', report)
                    continue
                self.assertTrue(any(x['result'] == 'failed' for x in [*report.values(), *digest.values()]),
                                (report, digest))


if __name__ == '__main__':
    unittest.main()
