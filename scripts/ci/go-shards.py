#!/usr/bin/env python3
"""Race shards: one per heavy root test, the other root tests in groups by a stable hash of their names,
and one for every other package. New tests and packages need no edit here. macOS runs the same shards
in fewer jobs: each of its jobs runs a union of shards, written as their names joined by '+'."""
import argparse
import json
import re
import shlex
import subprocess
import zlib
from pathlib import Path

GO = Path(__file__).resolve().parents[2] / 'go'
HEAVY = {'differential': 'TestListIndexDifferential',
         'randomized': 'TestListIndexRandomizedDifferential',
         'faults': 'TestFaultMatrix',
         'browse-oracle': 'TestBrowseRowsOracle',
         'range-batches': 'TestRangeBatchesMatchesRange'}
# The other root tests took 302 s of Linux race time in 468 tests (November 2026), nine of them 13-18 s
# each, so a hash balances them only on average: eight groups put the largest near a minute, as long as
# the heavy shards; four would leave it near 100 s.
ROOT_GROUPS = [f'root-{i}' for i in range(1, 9)]
PACKAGES = 'packages'
SHARDS = [*HEAVY, *ROOT_GROUPS, PACKAGES]
# macOS runners are scarce, so three jobs: the heavy tests, and the root groups in two halves, the half
# without the other packages also running go vet.
MACOS = ['+'.join(HEAVY), '+'.join([*ROOT_GROUPS[0::2], PACKAGES]), '+'.join(ROOT_GROUPS[1::2])]
MACOS_VET = MACOS[2]
# Below Windows' 32,767 characters, the smallest command-line limit of the runners' systems.
COMMAND_LIMIT = 30000


def owner(package, root, test):
    if package != root:
        return PACKAGES
    for shard, name in HEAVY.items():
        if test == name:
            return shard
    return ROOT_GROUPS[zlib.crc32(test.encode()) % len(ROOT_GROUPS)]


def shards_of(spec):
    names = spec.split('+')
    if len(set(names)) != len(names) or not set(names) <= set(SHARDS):
        raise ValueError('not a shard or a union of distinct shards: ' + spec)
    return names


def packages():
    listed = subprocess.check_output(['go', 'list', '-race', './...'], cwd=GO, text=True).splitlines()
    root = subprocess.check_output(['go', 'list', '-race', '.'], cwd=GO, text=True).strip()
    return root, listed


def list_tests(package):
    output = subprocess.check_output(['go', 'test', '-race', '-list', '.', package], cwd=GO, text=True)
    return [line for line in output.splitlines() if re.match(r'^(Test|Fuzz|Example)\w*$', line)]


def inventory():
    root, listed = packages()
    tests = {package: list_tests(package) for package in listed}
    if not listed or root not in tests or not tests[root]:
        raise ValueError('empty or incomplete test inventory')
    return root, tests


def commands(spec, root, tests):
    """The commands of a shard or union: the root package's tests by name, then the other packages whole."""
    shards = shards_of(spec)
    common = ['go', 'test', '-race', '-count=1', '-timeout=9m', '-json']
    names = [test for test in tests[root] if owner(root, root, test) in shards]
    others = [package for package in tests if package != root] if PACKAGES in shards else []
    if {owner(root, root, test) for test in names} | ({PACKAGES} if others else set()) != set(shards):
        raise ValueError(f'{spec}: a shard selects no tests (renamed heavy test?)')
    result = []
    if names:
        result.append(common + ['-run', '^(' + '|'.join(names) + ')$', root])
    if others:
        result.append(common + others)
    for command in result:
        if len(shlex.join(command)) > COMMAND_LIMIT:
            raise ValueError(f'{spec}: a {len(shlex.join(command))}-byte command line; add root groups')
    return result


def check(root, tests):
    """Every test runs in exactly one Linux shard and in exactly one macOS job."""
    for name in HEAVY.values():
        if name not in tests[root]:
            raise ValueError('heavy test disappeared or was renamed: ' + name)
    if sorted(shard for spec in MACOS for shard in shards_of(spec)) != sorted(SHARDS):
        raise ValueError('macOS jobs do not run every shard exactly once')
    report = []
    for platform, matrix in (('linux', SHARDS), ('macos', MACOS)):
        counts = dict.fromkeys(matrix, 0)
        planned = {spec: commands(spec, root, tests) for spec in matrix}
        for package, names in tests.items():
            for test in names:
                matches = []
                for spec, command_list in planned.items():
                    for command in command_list:
                        run = command[command.index('-run') + 1] if '-run' in command else '.'
                        if package in command and re.search(run, test):
                            matches.append(spec)
                want = [spec for spec in matrix if owner(package, root, test) in shards_of(spec)]
                if matches != want:
                    raise ValueError(f'{platform} {package}/{test}: invalid coverage {matches}')
                counts[matches[0]] += 1
        if not all(counts.values()):
            raise ValueError('empty shard')
        longest = max(len(shlex.join(c)) for command_list in planned.values() for c in command_list)
        report.append(f'{platform}: {json.dumps(counts)}; longest command {longest} bytes')
    print('\n'.join(report))


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
    parser.add_argument('--matrix', nargs='?', const='linux', choices=['linux', 'macos'],
                        help="print the Linux shards, or macOS's jobs with the one that vets")
    parser.add_argument('--check', action='store_true', help='check both matrices against the live inventory')
    parser.add_argument('--run', metavar='SHARD[+SHARD...]', help='run a shard or a union of shards')
    args = parser.parse_args()
    if args.matrix == 'linux':
        print(json.dumps(SHARDS))
    elif args.matrix == 'macos':
        print(json.dumps([{'shard': spec, 'vet': spec == MACOS_VET} for spec in MACOS]))
    elif args.run:
        root, listed = packages()
        tests = dict.fromkeys(listed, [])
        if any(shard != PACKAGES for shard in shards_of(args.run)):
            tests[root] = list_tests(root)
        log = Path(f'go-race-{args.run}.json').resolve()
        raise SystemExit(run_commands(commands(args.run, root, tests), log))
    else:
        check(*inventory())
