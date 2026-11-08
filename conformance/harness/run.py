#!/usr/bin/env python3
"""SeekParquet conformance harness (Python standard library only).

    python harness/run.py --adapter "COMMAND ..." [--suite conformance/suite.json]
                          [--filter REGEX] [--targets reader,validator-profile] [--json] [--timeout SECONDS]

Starts the adapter, sends ``hello``, then sends each case's request(s) without
the expected answer and judges the responses as conformance/protocol.md
describes. Exits 0 when every case of every claimed target passes, 1 on any
failure, and 2 on a usage error.

The harness contains no SeekParquet read logic: it compares responses with the
expectations written in the case files.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import os
import queue
import re
import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_SUITE = HERE.parent / "suite.json"
STATUSES = {"ok", "refused", "invalid_request", "unsupported", "error"}
VALUE_KINDS = {"absent", "null", "bool", "int", "float32", "float64", "bytes_base64"}
INT_RE = re.compile(r"^-?(0|[1-9][0-9]*)$")
F32_RE = re.compile(r"^[0-9a-f]{8}$")
F64_RE = re.compile(r"^[0-9a-f]{16}$")


class Fatal(Exception):
    """The adapter broke the transport (crash, timeout, bad id): the run cannot continue."""


class CaseFail(Exception):
    pass


def strict_object(pairs):
    out = {}
    for k, v in pairs:
        if k in out:
            raise ValueError(f"duplicate member {k!r}")
        out[k] = v
    return out


def canon_b64(s, what):
    if not isinstance(s, str):
        raise CaseFail(f"{what}: not a string")
    try:
        b = base64.b64decode(s, validate=True)
    except (binascii.Error, ValueError):
        raise CaseFail(f"{what}: not valid base64: {s!r}") from None
    if base64.b64encode(b).decode("ascii") != s:
        raise CaseFail(f"{what}: not canonical padded base64: {s!r}")
    return b


class Adapter:
    def __init__(self, cmd, timeout):
        self.timeout = timeout
        self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None)
        self.q = queue.Queue()
        self.n = 0
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self):
        for line in self.proc.stdout:
            self.q.put(line)
        self.q.put(None)

    def call(self, req):
        self.n += 1
        rid = f"r{self.n}"
        msg = dict(req)
        msg["id"] = rid
        line = json.dumps(msg, ensure_ascii=False) + "\n"
        try:
            self.proc.stdin.write(line.encode("utf-8"))
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError):
            raise Fatal("adapter closed its stdin (crashed?)") from None
        try:
            raw = self.q.get(timeout=self.timeout)
        except queue.Empty:
            raise Fatal(f"no response to request {rid} within {self.timeout}s") from None
        if raw is None:
            raise Fatal(f"adapter exited without answering request {rid} (exit code {self.proc.poll()})")
        try:
            text = raw.decode("utf-8")
            if text.startswith("﻿"):
                raise ValueError("byte order mark")
            resp = json.loads(text, object_pairs_hook=strict_object)
        except ValueError as e:
            raise Fatal(f"response to {rid} is not a JSON object line: {e}: {raw[:200]!r}") from None
        if not isinstance(resp, dict):
            raise Fatal(f"response to {rid} is not a JSON object")
        if resp.get("id") != rid:
            raise Fatal(f"expected a response with id {rid!r}, got id {resp.get('id')!r} "
                        "(missing, duplicate or out-of-order response)")
        st = resp.get("status")
        if st not in STATUSES:
            raise CaseFail(f"unknown status {st!r}")
        if st in ("refused", "unsupported") and not isinstance(resp.get("code"), str):
            raise CaseFail(f"status {st} without a code")
        return resp

    def close(self):
        extra = []
        try:
            self.proc.stdin.close()
        except OSError:
            pass
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                line = self.q.get(timeout=max(0.01, deadline - time.time()))
            except queue.Empty:
                break
            if line is None:
                break
            extra.append(line)
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            extra.append(b"<adapter did not exit after stdin closed>")
        return extra


# --------------------------------------------------------------------------
# Comparison

def check_value(v, where):
    if not isinstance(v, dict) or len(v) != 1:
        raise CaseFail(f"{where}: a value must be an object with one member, got {v!r}")
    (k, x), = v.items()
    if k not in VALUE_KINDS:
        raise CaseFail(f"{where}: unknown value kind {k!r}")
    if k in ("absent", "null") and x is not True:
        raise CaseFail(f"{where}: {k} must be true")
    if k == "bool" and not isinstance(x, bool):
        raise CaseFail(f"{where}: bool must be true or false")
    if k == "int" and not (isinstance(x, str) and INT_RE.match(x)):
        raise CaseFail(f"{where}: int must be a canonical decimal string, got {x!r}")
    if k == "float32" and not (isinstance(x, str) and F32_RE.match(x)):
        raise CaseFail(f"{where}: float32 must be 8 lowercase hex digits, got {x!r}")
    if k == "float64" and not (isinstance(x, str) and F64_RE.match(x)):
        raise CaseFail(f"{where}: float64 must be 16 lowercase hex digits, got {x!r}")
    if k == "bytes_base64":
        return (k, canon_b64(x, where))
    return (k, x)


def norm_entries(entries, ncols, where):
    if not isinstance(entries, list):
        raise CaseFail(f"{where}: entries must be an array")
    out = []
    for i, e in enumerate(entries):
        w = f"{where}[{i}]"
        if not isinstance(e, dict) or e.get("kind") not in ("row", "prefix"):
            raise CaseFail(f"{w}: entry must have kind row or prefix: {e!r}")
        key = canon_b64(e.get("key_base64"), f"{w}.key_base64")
        if e["kind"] == "prefix":
            if "values" in e:
                raise CaseFail(f"{w}: a prefix entry must not have values")
            if set(e) - {"kind", "key_base64"}:
                raise CaseFail(f"{w}: unexpected members {sorted(set(e) - {'kind', 'key_base64'})}")
            out.append(("prefix", key, None))
        else:
            vals = e.get("values")
            if not isinstance(vals, list):
                raise CaseFail(f"{w}: a row entry must have values (an array, empty without columns)")
            if ncols is not None and len(vals) != ncols:
                raise CaseFail(f"{w}: {len(vals)} values for {ncols} requested columns")
            if set(e) - {"kind", "key_base64", "values"}:
                raise CaseFail(f"{w}: unexpected members")
            out.append(("row", key, tuple(check_value(v, f"{w}.values[{j}]") for j, v in enumerate(vals))))
    return out


def show_entry(t):
    kind, key, vals = t
    k = key.decode("latin-1") if all(0x20 <= c < 0x7F for c in key) else "0x" + key.hex()
    return f"{kind} {k!r}" + ("" if vals is None else f" {list(vals)!r}")


def compare_entries(got, exp):
    for i, (g, e) in enumerate(zip(got, exp)):
        if g != e:
            raise CaseFail(f"entry {i}: expected {show_entry(e)}, got {show_entry(g)}")
    if len(got) != len(exp):
        more = got[len(exp)] if len(got) > len(exp) else exp[len(got)]
        raise CaseFail(f"expected {len(exp)} entries, got {len(got)} (first difference at {min(len(got), len(exp))}: "
                       f"{'unexpected' if len(got) > len(exp) else 'missing'} {show_entry(more)})")


def acceptable(exp):
    return exp["codes"] if "codes" in exp else [exp["code"]]


# --------------------------------------------------------------------------

class Runner:
    def __init__(self, adapter, suite_dir):
        self.a = adapter
        self.dir = suite_dir

    def full(self, case, req):
        r = dict(req)
        r["dataset"] = str((self.dir / case["dataset"]).resolve())
        return r

    def expect_ok(self, case, req, resp, exp):
        if resp["status"] != exp["status"]:
            extra = f" (code {resp.get('code')})" if resp.get("code") else ""
            msg = f": {resp.get('message')}" if resp.get("message") else ""
            raise CaseFail(f"expected status {exp['status']}, got {resp['status']}{extra}{msg}")
        if exp["status"] == "unsupported":
            if resp["code"] not in acceptable(exp):
                raise CaseFail(f"expected unsupported code {acceptable(exp)}, got {resp['code']!r}")
            return
        if exp["status"] != "ok":
            return
        op = req.get("op")
        if op in ("range", "list"):
            ncols = len(req.get("columns") or [])
            got = norm_entries(resp.get("entries"), ncols, "response.entries")
            want = norm_entries(exp.get("entries", []), ncols, "expected.entries")
            compare_entries(got, want)
        elif op == "validate":
            self.compare_levels(resp, exp.get("levels", {}))

    def compare_levels(self, resp, want):
        levels = resp.get("levels")
        if not isinstance(levels, dict):
            raise CaseFail("validate response without levels")
        for lvl, w in want.items():
            g = levels.get(lvl)
            if not isinstance(g, dict):
                raise CaseFail(f"validate response has no result for level {lvl}")
            if g.get("result") != w["result"]:
                raise CaseFail(f"level {lvl}: expected {w['result']}, got {g.get('result')!r}"
                               f"{' (' + str(g.get('code') or g.get('reason')) + ')' if g.get('code') or g.get('reason') else ''}")
            if w["result"] == "failed" and g.get("code") not in acceptable(w):
                raise CaseFail(f"level {lvl}: expected code {acceptable(w)}, got {g.get('code')!r}")
            if "checked" in w and g.get("checked") != w["checked"]:
                raise CaseFail(f"level {lvl}: expected checked {w['checked']}, got {g.get('checked')!r}")

    def refusal(self, case):
        exp = case["expect"]
        ok_codes = acceptable(exp)
        detect = case["detect"]
        req = case.get("request", {})

        def refused_ok(resp, stage):
            if resp["status"] == "refused":
                if resp["code"] in ok_codes:
                    return True
                raise CaseFail(f"{stage}: refused with {resp['code']!r}, expected {' or '.join(ok_codes)}")
            if resp["status"] != "ok":
                raise CaseFail(f"{stage}: status {resp['status']}{': ' + str(resp.get('message')) if resp.get('message') else ''}"
                               f"{' (' + resp['code'] + ')' if resp.get('code') else ''}, expected success or "
                               f"refusal with {' or '.join(ok_codes)}")
            return False

        # 1. open
        if refused_ok(self.a.call(self.full(case, {"op": "open"})), "open"):
            return
        if detect == "open":
            raise CaseFail(f"open succeeded; expected refusal with {' or '.join(ok_codes)}")
        # 2. read
        if detect == "read":
            resp = self.a.call(self.full(case, req))
            if resp["status"] == "refused" and resp["code"] in ok_codes:
                return
            raise CaseFail(f"request: expected refusal with {' or '.join(ok_codes)}, got status {resp['status']}"
                           f"{' code ' + resp['code'] if resp.get('code') else ''}")
        # 3. validate
        level = detect.split(":", 1)[1]
        if req and not (req.get("op") == "validate"):
            if refused_ok(self.a.call(self.full(case, req)), "request"):
                return
        vreq = req if req.get("op") == "validate" and req.get("level") == level else {"op": "validate", "level": level}
        resp = self.a.call(self.full(case, vreq))
        if resp["status"] != "ok":
            raise CaseFail(f"validate: status {resp['status']}{' (' + resp['code'] + ')' if resp.get('code') else ''}; "
                           "expected status ok with the level failed")
        self.compare_levels(resp, {level: {"result": "failed", "codes": ok_codes}})

    def run_case(self, case):
        exp = case["expect"]
        if exp["status"] == "refused":
            self.refusal(case)
            return
        req = case.get("request") or case.get("raw_request")
        resp = self.a.call(self.full(case, req))
        self.expect_ok(case, req, resp, exp)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--adapter", required=True, help="adapter command line")
    ap.add_argument("--suite", default=str(DEFAULT_SUITE))
    ap.add_argument("--filter", help="regular expression searched in case ids")
    ap.add_argument("--targets", help="comma-separated targets to run (default: every target the adapter claims; "
                                      "the suite's mandatory targets must then be claimed)")
    ap.add_argument("--json", action="store_true", help="print a JSON report instead of text")
    ap.add_argument("--timeout", type=float, default=120.0, help="seconds to wait for each response")
    ap.add_argument("--verbose", "-v", action="store_true", help="list passing cases too")
    args = ap.parse_args(argv)

    suite_path = Path(args.suite).resolve()
    suite = json.loads(suite_path.read_text(encoding="utf-8"))
    sdir = suite_path.parent
    caps_doc = json.loads((sdir / "capabilities.json").read_text(encoding="utf-8"))
    known_caps, known_targets = set(caps_doc["capabilities"]), caps_doc["targets"]
    files = [(f, json.loads((sdir / f).read_text(encoding="utf-8"))) for f in suite["case_files"]]
    pattern = re.compile(args.filter) if args.filter else None

    results, fatal, run_errors = [], None, []
    adapter = Adapter(shlex.split(args.adapter), args.timeout)
    hello = {}
    try:
        try:
            hello = adapter.call({"op": "hello"})
        except CaseFail as e:
            raise Fatal(f"hello: {e}") from None
        if hello.get("status") != "ok":
            raise Fatal(f"hello failed: {hello}")
        caps = hello.get("capabilities")
        targets = hello.get("targets")
        supports = hello.get("supports")
        if not isinstance(caps, list) or not isinstance(targets, list) or not isinstance(supports, list):
            raise Fatal("hello must report capabilities, targets and supports as arrays")
        for c in caps:
            if c not in known_caps:
                run_errors.append(f"hello: unknown capability {c!r}")
        for t in targets:
            if t not in known_targets:
                run_errors.append(f"hello: unknown target {t!r}")
            else:
                missing = [c for c in known_targets[t] if c not in caps]
                if missing:
                    run_errors.append(f"hello: target {t!r} claimed without capabilities {missing}")
        if {"format_version": suite["format_version"], "draft_revision": suite["draft_revision"]} not in supports:
            run_errors.append(f"hello: the adapter does not support format_version {suite['format_version']} "
                              f"draft_revision {suite['draft_revision']}")
        selected = set(targets)
        if args.targets:
            want = [t.strip() for t in args.targets.split(",") if t.strip()]
            for t in want:
                if t not in targets:
                    run_errors.append(f"--targets: the adapter does not claim target {t!r}")
            selected &= set(want)
        else:
            for t in suite.get("mandatory_targets", []):
                if t not in targets:
                    run_errors.append(f"the adapter does not claim mandatory target {t!r} "
                                      f"(use --targets to run a subset deliberately)")
        if run_errors:
            raise Fatal("; ".join(run_errors))
        runner = Runner(adapter, sdir)
        for fname, doc in files:
            for case in doc["cases"]:
                if pattern and not pattern.search(case["id"]):
                    continue
                rec = {"id": case["id"], "file": fname, "group": doc["group"], "target": doc["target"]}
                if fatal:
                    rec.update(result="fail", message=f"not run: {fatal}")
                elif doc["target"] not in selected:
                    rec.update(result="skip", message=f"target {doc['target']} not claimed or not selected")
                else:
                    lacking = [c for c in case.get("requires", []) if c not in caps]
                    if lacking:
                        rec.update(result="fail", message=f"target {doc['target']} claimed but capabilities "
                                                          f"{lacking} not reported")
                    else:
                        try:
                            runner.run_case(case)
                            rec.update(result="pass")
                        except CaseFail as e:
                            rec.update(result="fail", message=str(e))
                        except Fatal as e:
                            fatal = str(e)
                            rec.update(result="fail", message=fatal)
                results.append(rec)
    except Fatal as e:
        fatal = str(e)
    extra = adapter.close()
    if extra:
        run_errors.append(f"adapter wrote {len(extra)} unexpected line(s) after its last response: {extra[0][:200]!r}")
    if fatal and fatal not in run_errors:
        run_errors.append(fatal)

    counts = {"pass": 0, "fail": 0, "skip": 0}
    by_group = {}
    for r in results:
        counts[r["result"]] += 1
        g = by_group.setdefault(r["group"], {"pass": 0, "fail": 0, "skip": 0})
        g[r["result"]] += 1
    ok = counts["fail"] == 0 and not run_errors and counts["pass"] > 0
    if args.json:
        print(json.dumps({"suite": suite.get("revision"), "adapter": {k: hello.get(k) for k in
                          ("implementation", "version", "targets", "capabilities", "supports")},
                          "passed": ok, "counts": counts, "groups": by_group, "errors": run_errors,
                          "results": results}, indent=2))
    else:
        print(f"suite {suite.get('revision')}  adapter {hello.get('implementation')} {hello.get('version')}  "
              f"targets {hello.get('targets')}")
        for r in results:
            if r["result"] == "fail" or (args.verbose and r["result"] == "pass"):
                print(f"{r['result'].upper():5} {r['id']}" + (f": {r['message']}" if r.get("message") else ""))
        for g, c in by_group.items():
            print(f"  {g:22} pass {c['pass']:4}  fail {c['fail']:4}  skip {c['skip']:4}")
        for e in run_errors:
            print(f"ERROR {e}")
        print(f"{'PASSED' if ok else 'FAILED'}: {counts['pass']} passed, {counts['fail']} failed, "
              f"{counts['skip']} skipped")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
