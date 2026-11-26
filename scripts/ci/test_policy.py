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

    def test_documentation_recipes_run_in_conformance(self):
        result = policy.select('pull_request', [('M', ['docs/examples/pyarrow_writer.py'])])
        self.assertEqual(result['conformance'], True)

    def test_documentation_only(self):
        self.assertFalse(any(policy.select('pull_request', [('M', ['CONTRIBUTING.md'])]).values()))

    def test_go_markdown_is_documentation(self):
        # The docs job, which always runs, is all that reads it: docgen's snippets and regions, the link
        # and terminology checks (scripts/docs).
        for path in ('go/CHANGELOG.md', 'go/README.md', 'go/write/NOTES.md'):
            with self.subTest(path=path):
                self.assertFalse(any(policy.select('pull_request', [('M', [path])]).values()))
        self.assertEqual(policy.select('pull_request', [('A', ['go/README.md']), ('M', ['go/query.go'])]),
                         dict(go=True, java=True, conformance=True, macos=False))
        self.assertEqual(policy.select('pull_request', [('M', ['go/internal/x/testdata/case.md'])]),
                         dict(go=True, java=True, conformance=True, macos=False))
        for paths in ([('D', ['go/CHANGELOG.md'])], [('R100', ['go/README.md', 'go/OVERVIEW.md'])],
                      [('R095', ['go/README.md', 'docs/go.md'])]):
            with self.subTest(paths=paths):
                self.assertTrue(all(policy.select('pull_request', paths).values()))

    def test_go_sources_embed_no_markdown(self):
        # An embedded Markdown file would be read by Go tests that a Markdown-only change does not select.
        import re
        go = Path(__file__).resolve().parents[2] / 'go'
        for source in go.rglob('*.go'):
            for line in source.read_text(encoding='utf-8').splitlines():
                if line.startswith('//go:embed'):
                    for pattern in line.split()[1:]:
                        with self.subTest(source=str(source), pattern=pattern):
                            self.assertFalse(pattern.endswith('.md') or re.search(r'[*?\[]', pattern)
                                             or (source.parent / pattern).is_dir())

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
        jobs = ['go-lint', 'go-race', 'go-windows', 'go-macos', 'java', 'java-windows', 'conformance', 'docs']
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
        needs['docs'] = {'result': 'success'}
        policy.gate(needs)
        needs['docs'] = {'result': 'skipped'}
        with self.assertRaises(ValueError):
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


def inventory_fixture():
    tests = {'root': [*shards.HEAVY.values(), *(f'TestCase{i}' for i in range(64))], 'root/other': ['TestOther']}
    assert {shards.owner('root', 'root', t) for t in tests['root']} == set(shards.SHARDS) - {shards.PACKAGES}
    return tests


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

    def test_race_commands_fit_job_budget(self):
        tests = inventory_fixture()
        for spec in [*shards.SHARDS, *shards.MACOS]:
            for command in shards.commands(spec, 'root', tests):
                with self.subTest(spec=spec, command=command[:8]):
                    self.assertEqual(command[:6], ['go', 'test', '-race', '-count=1', '-timeout=9m', '-json'])
        self.assertEqual(len(shards.commands('root-1+packages', 'root', tests)), 2)

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

    def test_new_tests_fuzz_examples_and_packages_need_no_edit(self):
        tests = inventory_fixture()
        tests['root'] += ['TestNew', 'TestNewer', 'FuzzSeed', 'Example', 'Example_readme', 'TestCase1x']
        tests['root/new'] = ['TestListIndexDifferential', 'TestNew']
        shards.check('root', tests)
        self.assertEqual(shards.owner('root/new', 'root', 'TestListIndexDifferential'), 'packages')
        for test in ('TestNew', 'FuzzSeed', 'Example_readme'):
            self.assertIn(shards.owner('root', 'root', test), shards.ROOT_GROUPS)

    def test_membership_is_a_function_of_the_name(self):
        tests = inventory_fixture()
        before = {test: shards.owner('root', 'root', test) for test in tests['root']}
        tests['root'] = ['TestAdded', *tests['root'][::-1]]
        self.assertEqual({test: shards.owner('root', 'root', test) for test in before}, before)

    def test_selection_is_anchored(self):
        # A name that prefixes another selects only itself, as Go matches -run.
        tests = inventory_fixture()
        tests['root'] += ['TestReadBudgets', 'TestReadBudgetsGeneric', 'TestReadBudgetsOracle']
        self.assertEqual(len({shards.owner('root', 'root', t) for t in tests['root'][-3:]}), 3)
        shards.check('root', tests)

    def test_unions_merge_one_command_per_package(self):
        tests = inventory_fixture()
        (command,) = shards.commands('+'.join(shards.HEAVY), 'root', tests)
        self.assertEqual(command[-3:-1], ['-run', '^(' + '|'.join(shards.HEAVY.values()) + ')$'])
        for spec in ('rest', 'root-1+root-1', 'root-1+', ''):
            with self.subTest(spec=spec), self.assertRaises(ValueError):
                shards.commands(spec, 'root', tests)

    def test_macos_runs_every_shard_once_and_vets_once(self):
        self.assertEqual(sorted(s for spec in shards.MACOS for s in shards.shards_of(spec)), sorted(shards.SHARDS))
        self.assertLessEqual(len(shards.MACOS), 3)  # macOS runners are scarce; see go-shards.py
        workflow = (Path(__file__).resolve().parents[2] / '.github/workflows/ci.yml').read_text()
        macos = workflow.split('  go-macos:', 1)[1].split('\n  conformance:', 1)[0]
        self.assertIn("go-shards.py --run '${{ matrix.shard }}'", macos)
        self.assertEqual(macos.count('go vet ./...'), 1)
        self.assertIn('if: matrix.vet', macos)

    def test_command_lines_stay_under_the_windows_limit(self):
        tests = {'root': [*shards.HEAVY.values(), *(f'Test{"Long" * 40}{i}' for i in range(2000))]}
        with self.assertRaisesRegex(ValueError, 'command line'):
            shards.commands('root-1', 'root', tests)

    def test_renamed_heavy_test_fails_closed(self):
        tests = inventory_fixture()
        tests['root'].remove('TestFaultMatrix')
        with self.assertRaises(ValueError):
            shards.check('root', tests)
        with self.assertRaises(ValueError):  # and --run of its shard, before go test runs nothing
            shards.commands('faults', 'root', tests)

    def test_overlapping_or_missing_commands_fail(self):
        tests = inventory_fixture()
        with patch.object(shards, 'commands', return_value=[['go', 'test', 'root']]):
            with self.assertRaises(ValueError):
                shards.check('root', tests)
        with patch.object(shards, 'commands', return_value=[]):
            with self.assertRaises(ValueError):
                shards.check('root', tests)
