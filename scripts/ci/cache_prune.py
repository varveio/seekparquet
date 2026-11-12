#!/usr/bin/env python3
"""Retain two generations of recognized main Go caches; never prune other caches.

--plan reads a JSON array on stdin and prints candidate IDs without network access.
Live mode uses gh and requires actions:write. Entries younger than 15 minutes and
build keys belonging to active main runs are protected in addition to retention.
Only IDs in the initial snapshot are considered, so concurrent new uploads survive.
"""
import argparse
import datetime as dt
import json
import re
import subprocess
import sys

PREFIX = r"seekparquet-go-v1-"
BUILD = re.compile(PREFIX + r"build-(Linux|macOS|Windows)-(X64|ARM64)-(go[0-9.]+)-(go-lint|go-race-(?:differential|randomized|faults|rest)|go-windows|go-macos|conformance|java|docs)-[0-9a-f]{64}-([0-9a-f]{40})$")
MODULE = re.compile(PREFIX + r"mod-(Linux|macOS|Windows)-(X64|ARM64)-[0-9a-f]{64}$")


def deletion_plan(caches, now, active_shas=()):
    families = {}
    for cache in {c['id']: c for c in caches}.values():
        if cache.get('ref') != 'refs/heads/main':
            continue
        build = BUILD.fullmatch(cache['key'])
        module = MODULE.fullmatch(cache['key'])
        if build:
            family = ('build', *build.groups()[:-1], cache['version'])
        elif module:
            family = ('mod', *module.groups(), cache['version'])
        else:
            continue
        families.setdefault(family, []).append((cache, build))
    result = []
    for entries in families.values():
        entries.sort(key=lambda item: (item[0]['created_at'], item[0]['id']), reverse=True)
        for cache, build in entries[2:]:
            created = dt.datetime.fromisoformat(cache['created_at'].replace('Z', '+00:00'))
            if now - created < dt.timedelta(minutes=15):
                continue
            if build and build.group(5) in active_shas:
                continue
            result.append(cache)
    return sorted(result, key=lambda c: c['id'])


def gh(*args):
    return subprocess.check_output(['gh', 'api', *args], text=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', action='store_true')
    args = parser.parse_args()
    now = dt.datetime.now(dt.timezone.utc)
    if args.plan:
        print(json.dumps(deletion_plan(json.load(sys.stdin), now), indent=2))
        return
    import os
    repo = os.environ['GITHUB_REPOSITORY']
    # gh slurp keeps each page separate; sorting by creation avoids moving access order.
    pages = json.loads(gh('--paginate', '--slurp', f'repos/{repo}/actions/caches?sort=created_at&direction=asc&per_page=100'))
    caches = [c for page in pages for c in page['actions_caches']]
    active_shas = set()
    # Query active statuses separately so completed history cannot hide an active run.
    for status in ('queued', 'in_progress', 'waiting', 'pending', 'requested'):
        pages = json.loads(gh('--paginate', '--slurp', f'repos/{repo}/actions/runs?branch=main&status={status}&per_page=100'))
        active_shas.update(run['head_sha'] for page in pages for run in page['workflow_runs'])
    plan = deletion_plan(caches, now, active_shas)
    for cache in plan:
        # A vanished ID is harmless; all other API failures stop the maintenance job.
        deleted = subprocess.run(['gh', 'api', '-X', 'DELETE', f'repos/{repo}/actions/caches/{cache["id"]}'], capture_output=True, text=True)
        if deleted.returncode and 'HTTP 404' not in deleted.stderr:
            raise RuntimeError(deleted.stderr)
        print(f'Deleted {cache["id"]}: {cache["key"]}')
    print(f'{len(caches)} caches inspected; {len(plan)} superseded Go caches deleted.')


if __name__ == '__main__':
    main()
