#!/usr/bin/env python3
"""Four race shards. New tests and packages automatically enter rest."""
import argparse
import json
import re
import shlex
import subprocess
from pathlib import Path

GO = Path(__file__).resolve().parents[2] / 'go'
HEAVY = {'differential': 'TestListIndexDifferential',
         'randomized': 'TestListIndexRandomizedDifferential',
         'faults': 'TestFaultMatrix'}
SHARDS = [*HEAVY, 'rest']
EXCLUDED = '^(' + '|'.join(HEAVY.values()) + ')$'


def owner(package, root, test):
    if package == root:
        for shard, name in HEAVY.items():
            if test == name:
                return shard
    return 'rest'


def inventory():
    packages = subprocess.check_output(['go', 'list', '-race', './...'], cwd=GO, text=True).splitlines()
    root = subprocess.check_output(['go', 'list', '-race', '.'], cwd=GO, text=True).strip()
    tests = {}
    for package in packages:
        output = subprocess.check_output(['go', 'test', '-race', '-list', '.', package], cwd=GO, text=True)
        tests[package] = [line for line in output.splitlines() if re.match(r'^(Test|Fuzz|Example)\w*$', line)]
    if not packages or root not in tests or not tests[root]:
        raise ValueError('empty or incomplete test inventory')
    return root, tests


def commands(shard, root, tests):
    common = ['go', 'test', '-race', '-count=1', '-timeout=9m', '-json']
    if shard in HEAVY:
        return [common + ['-run', '^' + HEAVY[shard] + '$', root]]
    return [common + ['-skip', EXCLUDED, root], common + [p for p in tests if p != root]] if len(tests) > 1 else [common + ['-skip', EXCLUDED, root]]


def check(root, tests):
    for name in HEAVY.values():
        if name not in tests[root]:
            raise ValueError('heavy test disappeared or was renamed: ' + name)
    counts = dict.fromkeys(SHARDS, 0)
    for package, names in tests.items():
        for test in names:
            matches = []
            for shard in SHARDS:
                for command in commands(shard, root, tests):
                    if package not in command:
                        continue
                    run = command[command.index('-run') + 1] if '-run' in command else '.'
                    skip = command[command.index('-skip') + 1] if '-skip' in command else r'(?!)'
                    if re.search(run, test) and not re.search(skip, test):
                        matches.append(shard)
            if matches != [owner(package, root, test)]:
                raise ValueError(f'{package}/{test}: invalid coverage {matches}')
            counts[matches[0]] += 1
    if not all(counts.values()):
        raise ValueError('empty shard')
    print(json.dumps(counts))


def run_commands(command_list, log):
    """Keep every diagnostic and execute all packages even after a failure."""
    status = 0
    with log.open('w') as out:
        for command in command_list:
            print('+ ' + shlex.join(command), flush=True)
            process = subprocess.Popen(command, cwd=GO, stdout=subprocess.PIPE, text=True)
            for line in process.stdout:
                out.write(line)
                out.flush()
                try:
                    event = json.loads(line)
                except ValueError:
                    print(line, end='', flush=True)
                    continue
                if event.get('Action') in ('output', 'build-output'):
                    print(event.get('Output', ''), end='', flush=True)
                elif event.get('Action') in ('pass', 'fail', 'skip', 'build-fail') and 'Test' not in event:
                    print(line.rstrip(), flush=True)
            code = process.wait()
            if code:
                status = code if code > 0 else 1
    return status


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--matrix', action='store_true')
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--run', choices=SHARDS)
    args = parser.parse_args()
    if args.matrix:
        print(json.dumps(SHARDS))
    else:
        if args.run:
            packages = subprocess.check_output(['go', 'list', '-race', './...'], cwd=GO, text=True).splitlines()
            root = subprocess.check_output(['go', 'list', '-race', '.'], cwd=GO, text=True).strip()
            tests = dict.fromkeys(packages)
        else:
            root, tests = inventory()
            check(root, tests)
        if args.run:
            log = Path(f'go-race-{args.run}.json').resolve()
            raise SystemExit(run_commands(commands(args.run, root, tests), log))
