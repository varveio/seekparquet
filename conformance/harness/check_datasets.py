#!/usr/bin/env python3
"""Check datasets written by a producer, outside the fixtures.

    python harness/check_datasets.py --adapter "python harness/reference_adapter.py" DIR [DIR ...]
    python harness/check_datasets.py --adapter "python validator/validator_adapter.py" DIR [DIR ...]

Each DIR is a dataset root, or a directory whose subdirectories are dataset roots. Every dataset must
validate completely at the full level (structural, profile and full) and at the digest level, with every
part declaring a sha256, through an adapter that claims the validator targets (protocol.md). When the
duckdb module is installed, every part must also read in DuckDB with the manifest's row count, its keys in
the declared order (strictly ascending when keys are unique, across parts too) and its first and last keys
equal to the manifest's bounds. Python standard library only, apart from the optional duckdb.

The Go module's producer writes its sample datasets for this check with
`go -C go run ./internal/cmd/samples DIR`.
"""

from __future__ import annotations

import argparse
import base64
import json
import shlex
import subprocess
import sys
from pathlib import Path


def datasets(paths):
    for p in map(Path, paths):
        if (p / "manifest.json").is_file():
            yield p.resolve()
        else:
            yield from sorted(d.resolve() for d in p.iterdir() if (d / "manifest.json").is_file())


class Adapter:
    def __init__(self, command):
        self.proc = subprocess.Popen(shlex.split(command), stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.next = 0

    def ask(self, req):
        self.next += 1
        req = {"id": str(self.next), **req}
        self.proc.stdin.write(json.dumps(req) + "\n")
        self.proc.stdin.flush()
        resp = json.loads(self.proc.stdout.readline())
        if resp.get("id") != req["id"]:
            raise SystemExit(f"adapter answered {resp.get('id')!r} to request {req['id']!r}")
        return resp

    def close(self):
        self.proc.stdin.close()
        if self.proc.wait(timeout=60) != 0:
            raise SystemExit(f"adapter exited with status {self.proc.returncode}")


def check_validation(adapter, root, manifest):
    problems = []
    resp = adapter.ask({"op": "validate", "dataset": str(root), "level": "full"})
    levels = resp.get("levels", {})
    for level in ("structural", "profile", "full"):
        if levels.get(level, {}).get("result") != "complete":
            problems.append(f"{level}: {levels.get(level) or resp}")
    resp = adapter.ask({"op": "validate", "dataset": str(root), "level": "digest"})
    digest = resp.get("levels", {}).get("digest", {})
    parts = manifest["parts"]
    if digest.get("result") != "complete" or digest.get("checked") != len(parts):
        problems.append(f"digest: {digest or resp}")
    if any("sha256" not in p for p in parts):
        problems.append("a part declares no sha256")
    return problems


def check_duckdb(duckdb, root, manifest):
    problems = []
    unique = manifest["key"]["unique"]
    column = manifest["key"]["column"]
    previous = None
    for part in manifest["parts"]:
        path = str(root / part["path"])
        keys = [r[0] for r in duckdb.execute(f'SELECT "{column}" FROM read_parquet(?)', [path]).fetchall()]
        keys = [k.encode() if isinstance(k, str) else bytes(k) for k in keys]
        if len(keys) != part["rows"]:
            problems.append(f"{part['path']}: DuckDB read {len(keys)} rows, the manifest says {part['rows']}")
            continue
        if keys[0] != base64.b64decode(part["min_key_base64"]) or keys[-1] != base64.b64decode(part["max_key_base64"]):
            problems.append(f"{part['path']}: first or last key differs from the manifest bounds")
        for k in keys:
            if previous is not None and (k < previous or unique and k == previous):
                problems.append(f"{part['path']}: keys out of order")
                break
            previous = k
    return problems


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--adapter", required=True, help="adapter command claiming the validator targets")
    ap.add_argument("dirs", nargs="+")
    args = ap.parse_args()
    try:
        import duckdb  # noqa: PLC0415
    except ImportError:
        duckdb = None
        print("duckdb is not installed: DuckDB reads skipped")
    adapter = Adapter(args.adapter)
    hello = adapter.ask({"op": "hello"})
    if "validator-full" not in hello.get("targets", []) or "validator-digest" not in hello.get("targets", []):
        raise SystemExit(f"the adapter does not claim validator-full and validator-digest: {hello}")
    failed = 0
    roots = list(datasets(args.dirs))
    for root in roots:
        manifest = json.loads((root / "manifest.json").read_text())
        problems = check_validation(adapter, root, manifest)
        if duckdb is not None:
            problems += check_duckdb(duckdb, root, manifest)
        rows = sum(p["rows"] for p in manifest["parts"])
        status = "ok" if not problems else "FAILED"
        print(f"{status}: {root.name}: {len(manifest['parts'])} parts, {rows} rows")
        for p in problems:
            print(f"  {p}")
        failed += bool(problems)
    adapter.close()
    if not roots:
        raise SystemExit("no datasets found")
    print(f"{len(roots) - failed} of {len(roots)} datasets passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
