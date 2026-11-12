#!/usr/bin/env python3
"""Static checks of the conformance suite (Python standard library + jsonschema).

    python harness/check_suite.py [--suite conformance/suite.json]

Checks that:
- every case file listed in suite.json (and every file in cases/) validates against case.schema.json;
- case ids are unique; every case's dataset exists;
- requirement ids exist as [XXX-N] in spec/format-1.md;
- refusal codes exist in spec/refusal-codes.json, unsupported codes among its non-refusal codes;
- target and capability names exist in capabilities.json, and a case's requires are part of its target or optional;
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
    faults = {x["id"]: x for x in json.loads((conf / "generators" / "faults.json").read_text(encoding="utf-8"))["faults"]}
    mvalidator = jsonschema.Draft202012Validator(mschema)

    for t, tcaps in caps["targets"].items():
        for c in tcaps:
            if c not in caps["capabilities"]:
                bad(f"capabilities.json: target {t} names unknown capability {c}")
    for t, ds in caps.get("target_dependencies", {}).items():
        if t == "description":
            continue
        if t not in caps["targets"] or any(d not in caps["targets"] for d in ds):
            bad(f"capabilities.json: target_dependencies names an unknown target ({t}: {ds})")
    for c in caps.get("optional_capabilities", {}).get("capabilities", []):
        if c not in caps["capabilities"] or any(c in tcaps for tcaps in caps["targets"].values()):
            bad(f"capabilities.json: optional capability {c} is unknown or required by a target")
    for t in suite.get("mandatory_targets", []):
        if t not in caps["targets"]:
            bad(f"suite.json: unknown mandatory target {t}")
    for c in suite.get("mandatory_capabilities", []):
        if c not in caps["capabilities"]:
            bad(f"suite.json: unknown mandatory capability {c}")

    listed = set(suite["case_files"])
    on_disk = {p.relative_to(conf).as_posix() for p in (conf / "cases").rglob("*.json")}
    for f in sorted(on_disk - listed):
        bad(f"{f} is not listed in suite.json")
    for f in suite["case_files"]:
        core_path, gen_path = f.startswith("cases/core/"), f.startswith("cases/generated/") and f.endswith(".generated.json")
        if not (core_path or gen_path):
            bad(f"{f}: case files live in cases/core/*.json or cases/generated/*.generated.json")
    core_ids = set()
    retired = {r["id"] for r in suite.get("retired_case_ids", [])}
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
        if doc.get("generated") is not f.startswith("cases/generated/"):
            bad(f"{f}: generated must be {f.startswith('cases/generated/')} for this location")
        fams = doc.get("families", {})
        for c in doc.get("cases", []):
            n_cases += 1
            cid = c.get("id")
            where = f"{f}: {cid}"
            if cid in ids:
                bad(f"{where}: duplicate case id")
            ids.add(cid)
            if cid in retired:
                bad(f"{where}: uses a retired case id")
            if not doc.get("generated"):
                core_ids.add(cid)
            elif c.get("family") not in fams:
                bad(f"{where}: family {c.get('family')!r} is not described in the file's families")
            if c.get("fault_id") and c["fault_id"] not in faults:
                bad(f"{where}: unknown fault_id {c['fault_id']!r}")
            for r in c.get("requirements", []):
                if r not in spec_ids:
                    bad(f"{where}: requirement {r} is not defined in spec/format-1.md")
            for cap in c.get("requires", []):
                if cap not in caps["capabilities"]:
                    bad(f"{where}: unknown capability {cap!r}")
                elif (target in caps["targets"] and cap not in caps["targets"][target]
                      and cap not in caps.get("optional_capabilities", {}).get("capabilities", [])):
                    bad(f"{where}: requires {cap!r}, which target {target!r} does not include")
            if not (conf / c.get("dataset", "")).is_dir():
                bad(f"{where}: dataset {c.get('dataset')} does not exist")
            exp = c.get("expect", {})
            codes = exp.get("codes") or ([exp["code"]] if "code" in exp else [])
            allowed = unsupported_codes if exp.get("status") == "unsupported" else refusal_codes
            for code in codes:
                if code not in allowed:
                    bad(f"{where}: unknown code {code!r} for status {exp.get('status')}")
            # A failed level names a refusal code; an incomplete one, an unsupported code.
            for name, lv in exp.get("levels", {}).items():
                optional = lv.get("result") == "complete" and "incomplete" in lv.get("accept", [])
                allowed = unsupported_codes if lv.get("result") == "incomplete" or optional else refusal_codes
                for code in lv.get("codes") or ([lv["code"]] if "code" in lv else []):
                    if code not in allowed:
                        bad(f"{where}: unknown code {code!r} for a {lv.get('result')} {name} level")
            for code in exp.get("accept_unsupported", []):
                if code not in unsupported_codes:
                    bad(f"{where}: accept_unsupported names unknown code {code!r}")
            det = c.get("detect", "")
            if det.startswith("validate:") and f"{det}" not in c.get("requires", []):
                bad(f"{where}: detect {det} but requires does not list {det}")
            op = c.get("request", {}).get("op")
            if exp.get("status") == "ok" and op in ("range", "list", "plan") and "entries" not in exp:
                bad(f"{where}: an ok {op} expectation needs entries")
            if op == "validate" and (exp.get("status") != "ok" or not exp.get("levels")):
                bad(f"{where}: a validate expectation must be status ok with levels")
            if op == "validate" and c["request"].get("level") not in exp.get("levels", {}):
                bad(f"{where}: the expected levels do not include the requested level")

    # Subsets: core ids only; never qualify.
    for sp in suite.get("subsets", []):
        sub = json.loads((conf / sp).read_text(encoding="utf-8"))
        if sub.get("qualifies") is not False:
            bad(f"{sp}: a subset must say qualifies: false")
        for cid in sub.get("case_ids", []):
            if cid not in core_ids:
                bad(f"{sp}: {cid} is not a core case")
    # Behaviour duplicates: no two active cases assert the same thing.
    seen = {}
    for f in suite["case_files"]:
        for c in json.loads((conf / f).read_text(encoding="utf-8")).get("cases", []):
            key = json.dumps([c.get("dataset"), c.get("request"), c.get("raw_request"), c.get("expect"),
                              c.get("detect")], sort_keys=True)
            if key in seen:
                bad(f"{c['id']}: exact duplicate of {seen[key]}")
            seen.setdefault(key, c["id"])

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
        if not d["valid"]:
            fid = d.get("fault_id")
            if fid not in faults:
                bad(f"fixtures.lock.json: invalid dataset {d['name']} has no fault in generators/faults.json")
            elif faults[fid]["dataset"] != d["dir"]:
                bad(f"generators/faults.json: {fid} names dataset {faults[fid]['dataset']}, the lock says {d['dir']}")
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
    locked_faults = {d.get("fault_id") for d in lock["datasets"]}
    for fid in sorted(set(faults) - locked_faults):
        bad(f"generators/faults.json: fault {fid} has no fixture in fixtures.lock.json")
    if total > 10 * 1024 * 1024:
        bad(f"fixture corpus is {total} bytes, over 10 MiB")

    sys.path.insert(0, str(conf / "generators"))
    try:
        import make_cases
        if suite.get("content_sha256") != make_cases.suite_digest(suite["case_files"] + suite.get("subsets", [])):
            bad("suite.json content_sha256 does not match the contract and case files")
    except ImportError as e:
        bad(f"cannot import generators/make_cases.py to check the suite digest: {e}")
    if not (conf / "COVERAGE.md").exists():
        bad("COVERAGE.md missing")

    for p in problems:
        print(f"PROBLEM {p}")
    print(f"{'OK' if not problems else 'FAILED'}: {len(suite['case_files'])} case files, {n_cases} cases, "
          f"{len(lock['datasets'])} datasets, {len(locked)} fixture files, {total} bytes, {len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
