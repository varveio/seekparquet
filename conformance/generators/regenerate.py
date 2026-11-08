#!/usr/bin/env python3
"""Regenerate every fixture, STRUCTURE.txt, fixtures.lock.json and the case files.

    python generators/regenerate.py            # from conformance/

Steps:
1. pyarrow_fixtures.build(): rows documents, then pyarrow-written datasets.
2. mutate_metadata.build(): fixtures derived by metadata edits.
3. A textual STRUCTURE.txt per dataset (provenance, changes, manifest, and the
   parquet_meta summary of every Parquet file).
4. fixtures.lock.json: every fixture file with SHA-256, size, producer,
   provenance category and command, plus each dataset's fault and expected
   refusal.
5. make_cases.main(): the case files.

Directories under datasets/ that no generator produced are removed.
"""

from __future__ import annotations

import json
import platform
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyarrow as pa  # noqa: E402

import make_cases  # noqa: E402
import mutate_metadata  # noqa: E402
import parquet_meta as pm  # noqa: E402
import pyarrow_fixtures  # noqa: E402
from common import CONF, DATASETS, b64d, dumps, sha256, write_text  # noqa: E402

LOCK = CONF / "fixtures.lock.json"
FAULTS = CONF / "generators" / "faults.json"
PRESERVED = CONF / "generators" / "preserved.json"


def load_faults():
    doc = json.loads(FAULTS.read_text(encoding="utf-8"))
    out = {}
    for f in doc["faults"]:
        assert f["id"] not in out, f"duplicate fault id {f['id']}"
        out[f["id"]] = f
    return out


def accepted_codes(f):
    return f["codes"] + [s["code"] for s in f.get("secondary_faults", [])]


def fmt_key(s):
    b = b64d(s)
    if len(b) > 40:
        return f"{b[:16].hex()}..({len(b)} bytes)"
    return repr(b)[1:]


def structure(rec):
    d = CONF / rec["dir"]
    L = [f"# {rec['name']}", "", f"valid: {'yes' if rec['valid'] else 'no'}", f"description: {rec['description']}",
         f"provenance: {rec['provenance']}", f"producer: {rec['producer']}"]
    if rec.get("rows"):
        L.append(f"source rows: {rec['rows']}")
    if rec.get("derived_from"):
        L.append(f"derived from: {rec['derived_from']}")
    if rec.get("fault"):
        f = rec["fault"]
        L.append(f"fault (generators/faults.json): {f['id']}: {' or '.join(accepted_codes(f))}; reader detects on "
                 f"{f['reader_detect']}; lowest validation level {f['validation_level']} "
                 f"({', '.join(f['requirements'])}); location: {f['location']}")
        for sec in f.get("secondary_faults", []):
            L.append(f"secondary fault: {sec['code']}: {sec['justification']}")
        if f.get("note"):
            L.append(f"note: {f['note']}")
    if rec.get("changes"):
        L.append("changes:")
        L += [f"  - {c}" for c in rec["changes"]]
    L.append("")
    mpath = d / "manifest.json"
    if mpath.exists():
        try:
            m = json.loads(mpath.read_text(encoding="utf-8"))
            parts = m.get("parts", []) if isinstance(m, dict) else []
            L.append(f"manifest: format_version {m.get('format_version') if isinstance(m, dict) else '?'}, "
                     f"{len(parts)} parts")
            for i, p in enumerate(parts):
                try:
                    bounds = f"{fmt_key(p['min_key_base64'])} .. {fmt_key(p['max_key_base64'])}"
                except Exception:  # noqa: BLE001 - invalid on purpose
                    bounds = f"{p.get('min_key_base64')!r} .. {p.get('max_key_base64')!r} (not decodable)"
                L.append(f"  part {i}: {p.get('path')!r}, rows {p.get('rows')}, bytes {p.get('bytes')}, keys {bounds}")
        except Exception as e:  # noqa: BLE001
            L.append(f"manifest: not parseable as intended ({type(e).__name__})")
    else:
        L.append("manifest: absent")
    L.append("")
    for f in sorted(d.rglob("*.parquet")):
        rel = f.relative_to(d).as_posix()
        try:
            L.append(pm.summarize(f.read_bytes(), rel).rstrip("\n"))
        except Exception as e:  # noqa: BLE001
            L.append(f"file {rel}: {f.stat().st_size} bytes; footer unreadable: {e}")
        L.append("")
    write_text(d / "STRUCTURE.txt", "\n".join(L).rstrip("\n") + "\n")


def file_entries(rec):
    d = CONF / rec["dir"]
    src = CONF / rec["derived_from"] if rec.get("derived_from") else None
    out = []
    for f in sorted(p for p in d.rglob("*") if p.is_file() and p.name != "STRUCTURE.txt"):
        rel = f.relative_to(d).as_posix()
        data = f.read_bytes()
        e = {"path": f.relative_to(CONF).as_posix(), "bytes": len(data), "sha256": sha256(data)}
        is_parquet = f.suffix == ".parquet"
        if rec["provenance"] == "native":
            e["producer"] = rec["producer"] if is_parquet else "generators/pyarrow_fixtures.py"
            e["provenance"] = "native" if is_parquet else "generated"
        else:
            same = src is not None and (src / rel).exists() and (src / rel).read_bytes() == data
            if same:
                e["producer"] = pyarrow_fixtures.PRODUCER if is_parquet else "generators/pyarrow_fixtures.py"
                e["provenance"] = ("native" if is_parquet else "generated") + f" (copied unchanged from {rec['derived_from']})"
            else:
                e["producer"] = (f"{pyarrow_fixtures.PRODUCER} + generators/mutate_metadata.py" if is_parquet
                                 else "generators/mutate_metadata.py")
                e["provenance"] = "mutated"
        e["command"] = rec["command"]
        out.append(e)
    return out


def main():
    (DATASETS / "invalid").mkdir(parents=True, exist_ok=True)
    preserved = json.loads(PRESERVED.read_text(encoding="utf-8"))["datasets"]
    protected = {CONF / p["dir"] for p in preserved}
    records = pyarrow_fixtures.build() + mutate_metadata.build()
    names = [r["name"] for r in records]
    assert len(names) == len(set(names)), "duplicate dataset names"
    dirs = {CONF / r["dir"] for r in records}
    assert not dirs & protected, "a generator wrote into a preserved regression fixture"
    faults = load_faults()
    for r in records:
        if not r["valid"]:
            assert r["name"] in faults, f"invalid dataset {r['name']} has no entry in generators/faults.json"
            r["fault"] = faults[r["name"]]
            assert faults[r["name"]]["dataset"] == r["dir"], f"faults.json dataset path for {r['name']}"
    for p in preserved:
        if not p["valid"]:
            assert p["fault_id"] in faults, f"preserved dataset {p['name']} has no fault declaration"
    declared = {r["name"] for r in records if not r["valid"]} | {p.get("fault_id") for p in preserved}
    assert set(faults) <= declared, f"faults.json declares faults with no fixture: {sorted(set(faults) - declared)}"
    dirs |= protected
    for d in sorted(list(DATASETS.iterdir()) + list((DATASETS / "invalid").iterdir())):
        if d.is_dir() and d != DATASETS / "invalid" and d not in dirs:
            shutil.rmtree(d)  # stale generator output
    total = 0
    datasets = []
    for r in records:
        structure(r)
        files = file_entries(r)
        total += sum(f["bytes"] for f in files)
        entry = {k: r[k] for k in ("name", "dir", "valid", "description", "provenance", "producer", "rows",
                                   "derived_from", "changes") if r.get(k) is not None}
        if r.get("fault"):
            entry["fault_id"] = r["fault"]["id"]
        entry["files"] = files
        datasets.append(entry)
    for p in preserved:  # verified, never regenerated
        for fe in p["files"]:
            data = (CONF / fe["path"]).read_bytes()
            assert sha256(data) == fe["sha256"] and len(data) == fe["bytes"], f"preserved file changed: {fe['path']}"
            total += fe["bytes"]
        datasets.append(dict(p, preserved=True))
    biggest = max((f["bytes"] for d in datasets for f in d["files"]), default=0)
    assert total <= 10 * 1024 * 1024, f"fixture corpus {total} bytes exceeds 10 MiB"
    assert biggest <= 1024 * 1024, "a fixture file exceeds 1 MiB"
    lock = {
        "description": ("Every fixture file under datasets/ with its SHA-256, size, producer, provenance category "
                        "and generator command. Provenance categories: 'native' = written by an independent "
                        "Parquet writer (pyarrow) including the profile metadata; 'native-with-profile-metadata' = "
                        "independent-writer output to which profile metadata was added afterwards (unused so far: "
                        "pyarrow writes stamps, sorting_columns and the page index itself); 'mutated' = deliberately "
                        "edited by generators/mutate_metadata.py (data pages preserved unless a change says "
                        "otherwise); 'generated' = a text file (manifest) written by a generator."),
        "generator": {"command": "python generators/regenerate.py",
                      "python": ".".join(platform.python_version_tuple()[:2]),
                      "pyarrow": pa.__version__, "requirements": "generators/requirements.txt"},
        "reproducibility": ("Byte-reproducible with the pinned pyarrow on Python 3.13: "
                            "generators/check_reproducible.py regenerates everything in a temporary directory and "
                            "compares it with the checked-in files (pyarrow 26.0.1 footers carry no timestamps). If a future writer is not "
                            "byte-reproducible, the checked-in bytes stay authoritative and STRUCTURE.txt must "
                            "remain identical."),
        "total_bytes": total,
        "largest_file_bytes": biggest,
        "datasets": datasets,
    }
    write_text(LOCK, dumps(lock))
    make_cases.main()
    print(f"{len(records)} datasets, {sum(len(d['files']) for d in datasets)} files, {total} bytes")


if __name__ == "__main__":
    main()
