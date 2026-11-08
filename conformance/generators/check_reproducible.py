#!/usr/bin/env python3
"""Regenerate the whole corpus in a temporary directory and compare it with the checked-in files.

    python generators/check_reproducible.py        # from conformance/, with generators/requirements.txt

Copies the generator sources, contract files and spec/ into a temporary tree, deletes nothing in the real
tree, runs generators/regenerate.py there, and compares every generated file (generators/rows/, datasets/,
cases/, fixtures.lock.json, suite.json, COVERAGE.md) byte for byte. Exits non-zero on any difference.
"""

from __future__ import annotations

import filecmp
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

CONF = Path(__file__).resolve().parents[1]
REPO = CONF.parent
SOURCES = ["generators", "oracle.py", "protocol.md", "case.schema.json", "capabilities.json"]
GENERATED_DIRS = ["generators/rows", "datasets", "cases"]
GENERATED_FILES = ["fixtures.lock.json", "suite.json", "COVERAGE.md"]


def files_under(root: Path, rel_dirs, rel_files):
    out = set()
    for d in rel_dirs:
        base = root / d
        if base.exists():
            out |= {p.relative_to(root).as_posix() for p in base.rglob("*")
                    if p.is_file() and "__pycache__" not in p.parts}
    out |= {f for f in rel_files if (root / f).exists()}
    return out


def main():
    with tempfile.TemporaryDirectory() as tmp:
        troot = Path(tmp) / "repo"
        tconf = troot / "conformance"
        shutil.copytree(REPO / "spec", troot / "spec")
        tconf.mkdir(parents=True)
        for s in SOURCES:
            src = CONF / s
            if src.is_dir():
                shutil.copytree(src, tconf / s, ignore=shutil.ignore_patterns("__pycache__", "rows"))
            else:
                shutil.copy2(src, tconf / s)
        subprocess.run([sys.executable, str(tconf / "generators" / "regenerate.py")], check=True, cwd=tconf,
                       stdout=subprocess.DEVNULL)
        mine = files_under(CONF, GENERATED_DIRS, GENERATED_FILES)
        theirs = files_under(tconf, GENERATED_DIRS, GENERATED_FILES)
        problems = [f"only checked in: {p}" for p in sorted(mine - theirs)]
        problems += [f"only regenerated: {p}" for p in sorted(theirs - mine)]
        for p in sorted(mine & theirs):
            if not filecmp.cmp(CONF / p, tconf / p, shallow=False):
                problems.append(f"differs: {p}")
    for p in problems:
        print(p)
    print(f"{'REPRODUCIBLE' if not problems else 'NOT REPRODUCIBLE'}: {len(mine)} generated files compared, "
          f"{len(problems)} differences")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
