#!/usr/bin/env python3
"""Static checks of the conformance suite (Python standard library + jsonschema).

    python harness/check_suite.py [--suite conformance/suite.json]

Checks that:
- every case file listed in suite.json (and every file in cases/) validates against case.schema.json;
- case ids are unique; every case's dataset exists;
- requirement ids exist as [XXX-N] in spec/format-1.md;
- refusal codes exist in spec/refusal-codes.json, unsupported codes among its non-refusal codes;
- target and capability names exist in capabilities.json, and a case's requires are part of its target;
- every valid dataset's manifest validates against spec/manifest.schema.json;
- fixtures.lock.json lists exactly the fixture files on disk, with matching SHA-256 and size, within the
  size policy (1 MiB per file, 10 MiB in total).
Exits non-zero on any problem.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

import jsonschema

HERE = Path(__file__).resolve().parent


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", default=str(HERE.parent / "suite.json"))
    args = ap.parse_args(argv)
    suite_path = Path(args.suite).resolve()
    conf = suite_path.parent
    repo = conf.parent
    problems = []

    def bad(msg):
        problems.append(msg)

    suite = json.loads(suite_path.read_text(encoding="utf-8"))
    schema = json.loads((conf / "case.schema.json").read_text(encoding="utf-8"))
    caps = json.loads((conf / "capabilities.json").read_text(encoding="utf-8"))
    codes_doc = json.loads((repo / "spec" / "refusal-codes.json").read_text(encoding="utf-8"))
    mschema = json.loads((repo / "spec" / "manifest.schema.json").read_text(encoding="utf-8"))
    spec_ids = set(re.findall(r"\[([A-Z]+-[0-9]+)\]", (repo / "spec" / "format-1.md").read_text(encoding="utf-8")))
    refusal_codes = {c["code"] for c in codes_doc["codes"]}
    unsupported_codes = {c["code"] for n in codes_doc["non_refusals"] for c in n.get("codes", [])}
    validator = jsonschema.Draft202012Validator(schema)
    mvalidator = jsonschema.Draft202012Validator(mschema)

    for t, tcaps in caps["targets"].items():
        for c in tcaps:
            if c not in caps["capabilities"]:
                bad(f"capabilities.json: target {t} names unknown capability {c}")
    for t in suite.get("mandatory_targets", []):
        if t not in caps["targets"]:
            bad(f"suite.json: unknown mandatory target {t}")
    for c in suite.get("mandatory_capabilities", []):
        if c not in caps["capabilities"]:
            bad(f"suite.json: unknown mandatory capability {c}")

    listed = set(suite["case_files"])
    on_disk = {p.relative_to(conf).as_posix() for p in (conf / "cases").glob("*.json")}
    for f in sorted(on_disk - listed):
        bad(f"{f} is not listed in suite.json")
    ids = set()
    n_cases = 0
    for f in suite["case_files"]:
        path = conf / f
        if not path.exists():
            bad(f"suite.json lists missing file {f}")
            continue
        doc = json.loads(path.read_text(encoding="utf-8"))
        for e in validator.iter_errors(doc):
            bad(f"{f}: schema: {e.message[:200]} at {'/'.join(map(str, e.absolute_path))}")
        target = doc.get("target")
        if target not in caps["targets"]:
            bad(f"{f}: unknown target {target!r}")
        for c in doc.get("cases", []):
            n_cases += 1
            cid = c.get("id")
            where = f"{f}: {cid}"
            if cid in ids:
                bad(f"{where}: duplicate case id")
            ids.add(cid)
            for r in c.get("requirements", []):
                if r not in spec_ids:
                    bad(f"{where}: requirement {r} is not defined in spec/format-1.md")
            for cap in c.get("requires", []):
                if cap not in caps["capabilities"]:
                    bad(f"{where}: unknown capability {cap!r}")
                elif target in caps["targets"] and cap not in caps["targets"][target]:
                    bad(f"{where}: requires {cap!r}, which target {target!r} does not include")
            if not (conf / c.get("dataset", "")).is_dir():
                bad(f"{where}: dataset {c.get('dataset')} does not exist")
            exp = c.get("expect", {})
            codes = exp.get("codes") or ([exp["code"]] if "code" in exp else [])
            for lv in exp.get("levels", {}).values():
                codes += lv.get("codes") or ([lv["code"]] if "code" in lv else [])
            allowed = unsupported_codes if exp.get("status") == "unsupported" else refusal_codes
            for code in codes:
                if code not in allowed:
                    bad(f"{where}: unknown code {code!r} for status {exp.get('status')}")
            det = c.get("detect", "")
            if det.startswith("validate:") and f"{det}" not in c.get("requires", []):
                bad(f"{where}: detect {det} but requires does not list {det}")

    lock = json.loads((conf / "fixtures.lock.json").read_text(encoding="utf-8"))
    locked = {}
    total = 0
    for d in lock["datasets"]:
        ddir = conf / d["dir"]
        if not ddir.is_dir():
            bad(f"fixtures.lock.json: dataset {d['dir']} missing")
            continue
        if d["valid"]:
            mp = ddir / "manifest.json"
            try:
                m = json.loads(mp.read_text(encoding="utf-8"))
                for e in mvalidator.iter_errors(m):
                    bad(f"{d['dir']}/manifest.json: {e.message[:200]}")
            except (OSError, ValueError) as e:
                bad(f"{d['dir']}/manifest.json: {e}")
        if not d["valid"] and "fault" not in d:
            bad(f"fixtures.lock.json: invalid dataset {d['name']} has no fault")
        if not (ddir / "STRUCTURE.txt").exists():
            bad(f"{d['dir']}: STRUCTURE.txt missing")
        for fe in d["files"]:
            locked[fe["path"]] = fe
            total += fe["bytes"]
            p = conf / fe["path"]
            if not p.is_file():
                bad(f"fixtures.lock.json: {fe['path']} missing")
                continue
            data = p.read_bytes()
            if len(data) != fe["bytes"] or hashlib.sha256(data).hexdigest() != fe["sha256"]:
                bad(f"{fe['path']}: SHA-256 or size differs from fixtures.lock.json")
            if len(data) > 1024 * 1024:
                bad(f"{fe['path']}: larger than 1 MiB")
            for k in ("producer", "provenance", "command"):
                if not fe.get(k):
                    bad(f"fixtures.lock.json: {fe['path']} has no {k}")
    for p in sorted((conf / "datasets").rglob("*")):
        if p.is_file() and p.name != "STRUCTURE.txt" and p.relative_to(conf).as_posix() not in locked:
            bad(f"{p.relative_to(conf)} is not listed in fixtures.lock.json")
    if total > 10 * 1024 * 1024:
        bad(f"fixture corpus is {total} bytes, over 10 MiB")

    for p in problems:
        print(f"PROBLEM {p}")
    print(f"{'OK' if not problems else 'FAILED'}: {len(suite['case_files'])} case files, {n_cases} cases, "
          f"{len(lock['datasets'])} datasets, {len(locked)} fixture files, {total} bytes, {len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
