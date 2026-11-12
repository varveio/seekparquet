#!/usr/bin/env python3
"""Conservative PR selection and the aggregate gate; standard library only."""
import argparse
import json
import os
import subprocess


def select(event, paths=(), full_label=False):
    full = event != 'pull_request' or full_label or not paths
    go = java = contract = full
    macos = full
    for status, names in paths:
        if status != 'M' and status != 'A':
            go = java = contract = macos = True
        for path in names:
            if path.startswith(('.github/', 'scripts/ci/')):
                go = java = contract = macos = True
            elif path.startswith('go/'):
                go = True
                if path in ('go/go.mod', 'go/go.sum') or path.startswith('go/local/') or any(x in path.lower() for x in ('windows', 'darwin', 'unix', 'platform')):
                    macos = True
            elif path.startswith('java/'):
                java = True
            elif path.startswith(('spec/', 'conformance/')):
                contract = True
            elif path not in ('README.md', 'CONTRIBUTING.md', 'CODE_OF_CONDUCT.md', 'LICENSE', 'NOTICE', '.gitignore') and not path.startswith('docs/'):
                go = java = contract = macos = True
    return {'go': go or contract, 'java': java or go or contract,
            'conformance': go or contract, 'macos': macos and (go or contract)}


def changed_paths(base):
    if not base or set(base) == {'0'}:
        return []
    if subprocess.run(['git', 'cat-file', '-e', base + '^{commit}'], stderr=subprocess.DEVNULL).returncode:
        return []
    fields = subprocess.check_output(['git', 'diff', '--name-status', '-z', '--find-renames', base, 'HEAD']).decode().split('\0')
    paths = []
    while fields and fields[0]:
        status = fields.pop(0)
        count = 2 if status[0] in 'RC' else 1
        names, fields = fields[:count], fields[count:]
        if len(names) != count or not all(names):
            raise ValueError('malformed git diff')
        paths.append((status, names))
    return paths


def gate(needs):
    if needs['changes']['result'] != 'success':
        raise ValueError('change detection did not succeed')
    outputs = needs['changes']['outputs']
    for name in ('go', 'java', 'conformance', 'macos'):
        if outputs.get(name) not in ('true', 'false'):
            raise ValueError('missing or invalid selection: ' + name)
    expected = {'go-lint': outputs['go'], 'go-race': outputs['go'],
                'go-windows': outputs['go'], 'go-macos': outputs['macos'],
                'java': outputs['java'], 'java-windows': outputs['java'],
                'conformance': outputs['conformance'],
                'docs': 'true'}  # always selected: cheap, and generated docs follow code
    if set(needs) != set(expected) | {'changes'}:
        raise ValueError('gate dependencies differ from policy')
    failures = []
    for job, needed in expected.items():
        want = 'success' if needed == 'true' else 'skipped'
        actual = needs[job]['result']
        print(f'{job}: {actual} (expected {want})')
        if actual != want:
            failures.append(job)
    if failures:
        raise ValueError(', '.join(failures) + ' did not meet gate policy')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--gate', action='store_true')
    args = parser.parse_args()
    if args.gate:
        gate(json.loads(os.environ['NEEDS']))
    else:
        result = select(os.environ['EVENT_NAME'], changed_paths(os.environ.get('BASE_SHA', '')),
                        'ci:full' in (json.loads(os.environ.get('LABELS', '[]')) or []))
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            for key, value in result.items():
                print(f'{key}={str(value).lower()}', file=out)
