"""Offline retention policy checks: python3 -m unittest discover -s scripts/ci -p 'test_*.py'."""
import datetime as dt
import unittest

from cache_prune import BUILD, deletion_plan
import pathlib
import json
import subprocess
import sys

NOW = dt.datetime(2026, 11, 10, tzinfo=dt.timezone.utc)


def cache(number, mode='conformance', **overrides):
    result = dict(id=number,
                  key=f'seekparquet-go-v1-build-Linux-X64-go1.26.10-{mode}-' + 'a' * 64 + f'-{number:040x}',
                  ref='refs/heads/main', version='compression-v1',
                  created_at=(NOW - dt.timedelta(days=10 - number)).isoformat(),
                  last_accessed_at=NOW.isoformat())
    result.update(overrides)
    return result


class RetentionTests(unittest.TestCase):
    def ids(self, caches, active=()):
        return [c['id'] for c in deletion_plan(caches, NOW, active)]

    def test_newest_two_by_creation_not_access_and_deduplicated(self):
        entries = [cache(i) for i in (3, 1, 2, 1)]
        self.assertEqual(self.ids(entries), [1])

    def test_modes_platforms_and_cache_versions_are_independent(self):
        entries = [cache(i) for i in (1, 2, 3)]
        entries += [cache(4, mode='java'), cache(5, version='other')]
        entries += [cache(6, key=cache(6)['key'].replace('Linux-X64', 'Windows-X64'))]
        self.assertEqual(self.ids(entries), [1])

    def test_unknown_and_non_main_caches_are_untouched(self):
        entries = [cache(i, ref='refs/pull/12/merge') for i in (1, 2, 3)]
        entries += [cache(i + 10, key='setup-go-old-key') for i in (1, 2, 3)]
        entries += [cache(20, key=cache(20)['key'].replace('conformance', 'unknown'))]
        self.assertEqual(self.ids(entries), [])

    def test_active_revision_and_recent_uploads_are_protected(self):
        entries = [cache(i) for i in (1, 2, 3, 4)]
        for entry, minutes in zip(entries[1:], (3, 2, 1)):
            entry['created_at'] = (NOW - dt.timedelta(minutes=minutes)).isoformat()
        self.assertEqual(self.ids(entries, [f'{1:040x}']), [])

    def test_shared_modules_prune_across_dependency_changes(self):
        entries = [cache(i, key='seekparquet-go-v1-mod-Linux-X64-' + f'{i:064x}') for i in (1, 2, 3)]
        self.assertEqual(self.ids(entries), [1])

    def test_dependencies_do_not_multiply_build_generations(self):
        entries = [cache(i, key=cache(i)['key'].replace('a' * 64, f'{i:064x}')) for i in (1, 2, 3)]
        self.assertEqual(self.ids(entries), [1])

    def test_real_workflow_modes_are_recognized(self):
        root = pathlib.Path(__file__).resolve().parents[2]
        workflow = (root / '.github/workflows/ci.yml').read_text()
        import re
        modes = re.findall(r'^          key: (.+)$', workflow, re.MULTILINE)
        shards = json.loads(subprocess.check_output(
            [sys.executable, str(root / 'scripts/ci/go-shards.py'), '--matrix'], text=True))
        expanded = []
        for mode in modes:
            if mode == 'go-race-${{ matrix.shard }}':
                expanded.extend('go-race-' + shard for shard in shards)
            else:
                expanded.append(mode)
        self.assertTrue(expanded)
        for mode in expanded:
            with self.subTest(mode=mode):
                self.assertIsNotNone(BUILD.fullmatch(cache(1, mode=mode)['key']))

    def test_toolchains_are_not_restoration_or_retention_peers(self):
        entries = [cache(i) for i in (1, 2)]
        entries += [cache(3, key=cache(3)['key'].replace('go1.26.10', 'go1.27.1'))]
        self.assertEqual(self.ids(entries), [])


if __name__ == '__main__':
    unittest.main()
