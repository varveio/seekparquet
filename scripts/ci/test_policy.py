import copy
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
import policy

spec = importlib.util.spec_from_file_location('shards', Path(__file__).with_name('go-shards.py'))
shards = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shards)


class PolicyTests(unittest.TestCase):
    def test_go_keeps_java_reader_and_windows(self):
        self.assertEqual(policy.select('pull_request', [('M', ['go/query.go'])]),
                         dict(go=True, java=True, conformance=True, macos=False))

    def test_documentation_only(self):
        self.assertFalse(any(policy.select('pull_request', [('M', ['CONTRIBUTING.md'])]).values()))

    def test_full_and_escalation(self):
        for event, paths, label in [
            ('push', [('M', ['README.md'])], False),
            ('schedule', [], False), ('workflow_dispatch', [], False),
            ('pull_request', [], False), ('pull_request', [('M', ['README.md'])], True),
            ('pull_request', [('M', ['go/go.mod'])], False),
            ('pull_request', [('M', ['.github/workflows/ci.yml'])], False),
            ('pull_request', [('M', ['new-unknown-input'])], False),
            ('pull_request', [('R100', ['go/a.go', 'docs/a.md'])], False),
            ('pull_request', [('D', ['go/a.go'])], False),
        ]:
            with self.subTest(event=event, paths=paths):
                self.assertTrue(all(policy.select(event, paths, label).values()))

    def test_java_only(self):
        result = policy.select('pull_request', [('M', ['java/seekparquet/src/Foo.java'])])
        self.assertEqual(result, dict(go=False, java=True, conformance=False, macos=False))

    def test_failed_diff_is_not_empty_diff(self):
        import subprocess
        with patch('policy.subprocess.run') as run, patch('policy.subprocess.check_output', side_effect=subprocess.CalledProcessError(1, 'git')):
            run.return_value.returncode = 0
            with self.assertRaises(subprocess.CalledProcessError):
                policy.changed_paths('abc')

    def test_gate_rejects_every_missing_failed_cancelled_required_job(self):
        outputs = {k: 'true' for k in ('go', 'java', 'conformance', 'macos')}
        jobs = ['go-lint', 'go-race', 'go-windows', 'go-macos', 'java', 'java-windows', 'conformance']
        needs = {'changes': {'result': 'success', 'outputs': outputs}}
        needs.update({job: {'result': 'success'} for job in jobs})
        policy.gate(needs)
        for job in jobs:
            for result in ('failure', 'cancelled', 'skipped'):
                bad = copy.deepcopy(needs)
                bad[job]['result'] = result
                with self.subTest(job=job, result=result), self.assertRaises(ValueError):
                    policy.gate(bad)
        bad = copy.deepcopy(needs)
        bad['changes']['outputs']['go'] = ''
        with self.assertRaises(ValueError):
            policy.gate(bad)
        bad = copy.deepcopy(needs)
        bad['extra-job'] = {'result': 'success'}
        with self.assertRaises(ValueError):
            policy.gate(bad)

    def test_gate_permits_only_intentional_skips(self):
        outputs = {k: 'false' for k in ('go', 'java', 'conformance', 'macos')}
        needs = {'changes': {'result': 'success', 'outputs': outputs}}
        needs.update({job: {'result': 'skipped'} for job in ('go-lint', 'go-race', 'go-windows', 'go-macos', 'java', 'java-windows', 'conformance')})
        policy.gate(needs)

    def test_windows_vet_and_tests_are_separate_steps(self):
        workflow = (Path(__file__).resolve().parents[2] / '.github/workflows/ci.yml').read_text()
        windows = workflow.split('  go-windows:', 1)[1].split('  go-macos:', 1)[0]
        steps = windows.split('      - ')
        vet = [step for step in steps if 'run: go vet ./...' in step]
        tests = [step for step in steps if 'run: go test -count=1' in step]
        self.assertEqual(len(vet), 1)
        self.assertEqual(len(tests), 1)
        self.assertNotEqual(vet[0], tests[0])
        self.assertNotIn('go test', vet[0])

    def test_workflow_gate_dependency_inventory(self):
        workflow = (Path(__file__).resolve().parents[2] / '.github/workflows/ci.yml').read_text()
        import re
        jobs = set(re.findall(r'^  ([\w-]+):$', workflow.split('jobs:\n', 1)[1], re.M))
        gate = re.search(r'    needs: \[([^\]]+)\]', workflow.split('  ci-ok:', 1)[1]).group(1)
        self.assertEqual(set(gate.split(', ')), jobs - {'ci-ok'})


class ShardTests(unittest.TestCase):
    def test_runner_retains_diagnostics_and_runs_after_failure(self):
        import contextlib
        import io
        import json
        import tempfile
        from unittest.mock import Mock
        diagnostics = ['    reader_test.go:12: bad value\n',
                       'WARNING: DATA RACE\n', 'goroutine 12 [running]:\n',
                       'compiler: bad type\n']
        events = [json.dumps({'Action': 'output' if i < 3 else 'build-output',
                              'Output': text}) + '\n'
                  for i, text in enumerate(diagnostics)]
        events.append('unstructured fatal diagnostic\n')
        failed = Mock(stdout=iter(events))
        failed.wait.return_value = 1
        passed = Mock(stdout=iter([json.dumps({'Action': 'output', 'Output': 'other package ran\n'}) + '\n']))
        passed.wait.return_value = 0
        console = io.StringIO()
        with tempfile.TemporaryDirectory() as temp, contextlib.redirect_stdout(console):
            log = Path(temp) / 'race.json'
            with patch.object(shards.subprocess, 'Popen', side_effect=[failed, passed]) as popen:
                status = shards.run_commands([['first'], ['second']], log)
            self.assertEqual(popen.call_count, 2)
            self.assertEqual(status, 1)
            self.assertEqual(log.read_text(), ''.join(events) + json.dumps({'Action': 'output', 'Output': 'other package ran\n'}) + '\n')
        for diagnostic in [*diagnostics, 'unstructured fatal diagnostic', 'other package ran']:
            self.assertIn(diagnostic, console.getvalue())

    def test_race_command_timeouts_fit_job_budget(self):
        commands = shards.commands('rest', 'root', {'root': [], 'root/other': []})
        self.assertEqual(len(commands), 2)
        for command in commands:
            self.assertIn('-timeout=9m', command)
            self.assertIn('-count=1', command)

    def test_inventory_uses_race_build_constraints(self):
        root_tests = '\n'.join([*shards.HEAVY.values(), 'TestOther'])
        with patch.object(shards.subprocess, 'check_output', side_effect=[
                'root\nroot/raceonly\n', 'root\n', root_tests, 'TestRaceOnly\n']) as output:
            root, tests = shards.inventory()
        self.assertEqual(root, 'root')
        self.assertEqual(tests['root/raceonly'], ['TestRaceOnly'])
        for call in output.call_args_list:
            self.assertIn('-race', call.args[0])
        self.assertEqual(output.call_args_list[0].args[0], ['go', 'list', '-race', './...'])
        self.assertEqual(output.call_args_list[2].args[0], ['go', 'test', '-race', '-list', '.', 'root'])

    def test_new_test_fuzz_example_and_package_enter_rest(self):
        tests = {'root': [*shards.HEAVY.values(), 'TestNew', 'FuzzSeed', 'Example'],
                 'root/new': ['TestListIndexDifferential', 'TestNew']}
        shards.check('root', tests)
        self.assertEqual(shards.owner('root/new', 'root', 'TestListIndexDifferential'), 'rest')

    def test_renamed_heavy_test_fails_closed(self):
        with self.assertRaises(ValueError):
            shards.check('root', {'root': ['TestRenamed', 'TestNew']})

    def test_overlapping_or_missing_commands_fail(self):
        tests = {'root': [*shards.HEAVY.values(), 'TestOther']}
        with patch.object(shards, 'commands', return_value=[['go', 'test', 'root']]):
            with self.assertRaises(ValueError):
                shards.check('root', tests)
        with patch.object(shards, 'commands', return_value=[]):
            with self.assertRaises(ValueError):
                shards.check('root', tests)
