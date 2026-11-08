#!/usr/bin/env python3
"""Self-tests of harness/run.py with deliberately deficient adapters (standard library only).

    python -m unittest discover -s harness -p 'test_*.py'      # from conformance/

Each stub adapter answers just enough of the protocol to reach the behaviour under test. A passing
reference adapter alone cannot show that the harness rejects incomplete implementations.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
RUN = HERE / "run.py"

STUB = textwrap.dedent('''
    import json, sys, time
    mode = sys.argv[1]
    caps = ["open", "range", "list", "projection"]
    targets = ["reader"]
    if mode == "lacks-capability":
        caps = ["open"]
    if mode == "unknown-target":
        targets = ["reader", "sorcerer"]
    if mode == "validator-full":
        targets, caps = ["validator-full"], ["open", "validate:structural", "validate:profile", "validate:full"]
    n = 0
    for line in sys.stdin:
        req = json.loads(line)
        n += 1
        if req["op"] == "hello":
            resp = {"status": "ok", "implementation": "stub", "version": "0", "targets": targets,
                    "capabilities": caps, "supports": [{"format_version": 0, "draft_revision": 1}]}
        elif mode == "missing-response" and n >= 2:
            time.sleep(60)
            continue
        elif req["op"] == "open":
            resp = {"status": "ok"}
        elif req["op"] in ("range", "list"):
            resp = {"status": "ok", "entries": []}
        elif req["op"] == "validate":
            resp = {"status": "ok", "levels": {"structural": {"result": "complete"}}}
        else:
            resp = {"status": "invalid_request"}
        resp["id"] = "bogus" if (mode == "wrong-id" and n >= 2) else req["id"]
        out = json.dumps(resp) + "\\n"
        sys.stdout.write(out)
        if mode == "duplicate":
            sys.stdout.write(out)
        sys.stdout.flush()
    if mode == "nonzero-exit":
        sys.exit(7)
    if mode == "no-exit":
        time.sleep(60)
''')


ORACLE_ADAPTER = textwrap.dedent(r'''
    """An adapter built on oracle.py with one deliberate bug (argv[2]); "none" is the correct control."""
    import base64, bisect, hashlib, json, sys
    from pathlib import Path
    CONF = Path(sys.argv[1]); BUG = sys.argv[2]
    sys.path[:0] = [str(CONF), str(CONF / "generators")]
    import oracle, parquet_meta as pm
    lock = {d["dir"]: d for d in json.loads((CONF / "fixtures.lock.json").read_text())["datasets"]}
    faults = {f["dataset"]: f for f in json.loads((CONF / "generators/faults.json").read_text())["faults"]}
    LEVELS = ["structural", "profile", "full"]

    def rows_name(d):
        e = lock[d]
        return Path(e["rows"]).stem if e.get("rows") else rows_name(e["derived_from"])

    def b64(req, k):
        v = req.get(k)
        if v is None:
            return None
        b = base64.b64decode(v, validate=True)
        if base64.b64encode(b).decode() != v:
            raise ValueError("non-canonical")
        return b

    def skey(k):  # BUG signed: compare bytes as signed integers
        return tuple(x - 256 if x > 127 else x for x in k)

    def page_starts(d, ds):
        """(min bound, first global row) of every key page, from the real ColumnIndex/OffsetIndex."""
        m = json.loads((CONF / d / "manifest.json").read_text())
        out, base = [], 0
        for part in m["parts"]:
            pf = pm.read_parquet((CONF / d / part["path"]).read_bytes())
            for ri, rg in enumerate(pf.row_groups):
                n = rg.get(pm.RG_NUM_ROWS)
                if not n:
                    continue
                cc = pm.column_chunk(pf, ri, 0)
                ci, oi = pm.read_index(pf, cc, "column"), pm.read_index(pf, cc, "offset")
                for mn, loc in zip(ci.items_of(pm.CI_MIN), oi.items_of(pm.OI_PAGE_LOCATIONS)):
                    out.append((mn, base + loc.get(pm.PL_FIRST_ROW)))
                base += n
        return out

    def do_range(d, ds, req, cols):
        prefix, start, end = b64(req, "prefix_base64"), b64(req, "start_base64"), b64(req, "end_base64")
        excl, limit = req.get("start_exclusive", False), req["limit"]
        if BUG not in ("signed", "inclusive-end", "shortened-exact"):
            return ds.range(prefix=prefix, start=start, start_exclusive=excl, end=end, limit=limit, columns=cols)
        if not isinstance(limit, int) or limit < 1:
            raise oracle.InvalidRequest()
        rows = list(ds.rows)
        kf = skey if BUG == "signed" else (lambda k: k)
        rows.sort(key=lambda r: kf(r[0]))
        first = 0
        if BUG == "shortened-exact" and start is not None:
            # treat each page minimum as that page's exact first key and jump to the last page with min <= start
            ps = page_starts(d, ds)
            i = bisect.bisect_right([p[0] for p in ps], start) - 1
            first = ps[i][1] if i >= 0 else 0
        out = []
        for k, v, present in rows[first:]:
            if prefix is not None and not k.startswith(prefix):
                continue
            if start is not None and (kf(k) < kf(start) or (excl and k == start)):
                continue
            if end is not None and (kf(k) > kf(end) if BUG == "inclusive-end" else kf(k) >= kf(end)):
                continue
            out.append({"kind": "row", "key_base64": base64.b64encode(k).decode(), "offset": 0, "values": ds._values(k, v, present, cols)})
            if len(out) == limit:
                break
        return out

    def validate(d, level):
        f = faults.get(d)
        if level == "digest":
            m = json.loads((CONF / d / "manifest.json").read_text())
            n, bad = 0, False
            for part in m["parts"]:
                if "sha256" in part:
                    n += 1
                    bad |= hashlib.sha256((CONF / d / part["path"]).read_bytes()).hexdigest() != part["sha256"]
            return {"digest": {"result": "failed", "code": "digest_mismatch"} if bad else {"result": "complete", "checked": n}}
        out = {}
        flvl = f["validation_level"] if f else None
        for lvl in LEVELS[: LEVELS.index(level) + 1]:
            if flvl is None or flvl not in LEVELS or LEVELS.index(lvl) < LEVELS.index(flvl):
                out[lvl] = {"result": "complete"}
            elif lvl == flvl:
                out[lvl] = {"result": "failed", "code": f["codes"][0]}
            elif BUG == "skip-prereq":
                out[lvl] = {"result": "complete"}  # BUG: dependent levels reported without their prerequisite
            else:
                out[lvl] = {"result": "incomplete"} if BUG == "no-reason" else \
                    {"result": "incomplete", "reason": "a prerequisite level did not complete"}
        return out

    def handle(req):
        if req["op"] == "hello":
            return {"status": "ok", "implementation": "oracle-stub", "version": BUG,
                    "supports": [{"format_version": 0, "draft_revision": 1}],
                    "targets": ["reader", "validator-structural", "validator-profile", "validator-full", "validator-digest"],
                    "capabilities": ["open", "range", "list", "projection", "validate:structural", "validate:profile",
                                     "validate:full", "validate:digest"]}
        d = Path(req["dataset"]).relative_to(CONF).as_posix()
        if req["op"] == "validate":
            return {"status": "ok", "levels": validate(d, req["level"])}
        f = faults.get(d)
        if f and (f["reader_detect"] == "open" or req["op"] != "open" and f["reader_detect"] == "read"):
            return {"status": "refused", "code": f["codes"][0]}
        if req["op"] == "open":
            return {"status": "ok"}
        ds = oracle.Dataset.load(rows_name(d))
        cols = req.get("columns", [])
        try:
            if req["op"] == "range":
                entries = do_range(d, ds, req, cols)
            else:
                after = b64(req, "after_base64")
                cont = req.get("continuation", False) and BUG != "continuation"  # BUG: continuation ignored
                entries = ds.list(prefix=b64(req, "prefix_base64") or b"", delimiter=b64(req, "delimiter_base64"),
                                  after=after, continuation=cont, end=b64(req, "end_base64"), limit=req.get("limit"),
                                  columns=cols)
        except (oracle.InvalidRequest, ValueError, KeyError, TypeError):
            return {"status": "invalid_request"}
        except oracle.Unsupported as u:
            return {"status": "unsupported", "code": u.code}
        if BUG == "absent-null":  # BUG: a missing column reported as null
            for e in entries:
                if "values" in e:
                    e["values"] = [{"null": True} if v == {"absent": True} else v for v in e["values"]]
        return {"status": "ok", "entries": entries}

    for line in sys.stdin:
        req = json.loads(line)
        sys.stdout.write(json.dumps({"id": req["id"], **handle(req)}) + "\n")
        sys.stdout.flush()
''')


class HarnessSelfTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.stub = Path(cls.tmp.name) / "stub_adapter.py"
        cls.stub.write_text(STUB, encoding="utf-8")
        cls.oracle_adapter = Path(cls.tmp.name) / "oracle_adapter.py"
        cls.oracle_adapter.write_text(ORACLE_ADAPTER, encoding="utf-8")

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def run_harness(self, mode, *args, timeout=60):
        cmd = [sys.executable, str(RUN), "--adapter", f"{sys.executable} {self.stub} {mode}", "--json",
               "--timeout", "2", "--shutdown-timeout", "1", *args]
        t0 = time.time()
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        report = json.loads(p.stdout) if p.stdout.strip().startswith("{") else {}
        return p.returncode, report, time.time() - t0

    def run_oracle(self, bug, pattern):
        cmd = [sys.executable, str(RUN), "--adapter", f"{sys.executable} {self.oracle_adapter} {HERE.parent} {bug}",
               "--json", "--filter", pattern, "--timeout", "30"]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        rep = json.loads(p.stdout)
        self.assertFalse(rep["errors"], rep["errors"])
        return rep["counts"]

    def assert_detected(self, bug, pattern):
        control = self.run_oracle("none", pattern)
        self.assertGreater(control["pass"], 0, pattern)
        self.assertEqual(control["fail"], 0, f"the correct control adapter fails {pattern}")
        broken = self.run_oracle(bug, pattern)
        self.assertGreater(broken["fail"], 0, f"no case detects the {bug} bug")

    def test_detects_signed_byte_ordering(self):
        self.assert_detected("signed", r"^range/binary-keys/(all|seek-)")

    def test_detects_inclusive_upper_bound(self):
        self.assert_detected("inclusive-end", r"^range/basic/(end-hit|end-first|start-equals-end|start-end)$")

    def test_detects_shortened_bounds_treated_as_exact(self):
        self.assert_detected("shortened-exact", r"^range/truncated-bounds-2/seek-")

    def test_exact_bounds_do_not_trigger_the_shortened_bounds_bug(self):
        # the same bug is harmless where bounds are exact, so the detection comes from the shortened layout
        self.assertEqual(self.run_oracle("shortened-exact", r"^range/binary-keys/seek-")["fail"], 0)

    def test_detects_wrong_continuation(self):
        self.assert_detected("continuation", r"^(list/delimiter/after-inside-prefix-continuation|pagination/list/)")

    def test_detects_absent_null_conflation(self):
        self.assert_detected("absent-null", r"^projection/(column/f64|columns/span-parts|col-7/)")

    def test_detects_validation_skipping_prerequisites(self):
        self.assert_detected("skip-prereq", r"^validate-(profile|full)/prerequisite/")

    def test_detects_incomplete_without_reason(self):
        self.assert_detected("no-reason", r"^validate-(profile|full)/prerequisite/")

    def test_smoke_subset_never_qualifies(self):
        cmd = [sys.executable, str(RUN), "--adapter", f"{sys.executable} {self.oracle_adapter} {HERE.parent} none",
               "--json", "--subset", "smoke"]
        rep = json.loads(subprocess.run(cmd, capture_output=True, text=True, timeout=300).stdout)
        self.assertEqual(rep["counts"]["pass"] + rep["counts"]["fail"], 30)
        self.assertTrue(rep["selection"]["partial"])
        self.assertEqual(rep["qualified_targets"], [])

    def test_correct_partial_run_passes_but_does_not_qualify(self):
        rc, rep, _ = self.run_harness("good", "--filter", "^open/basic$")
        self.assertEqual(rc, 0, rep)
        self.assertTrue(rep["selected_cases_passed"])
        self.assertTrue(rep["selection"]["partial"])
        self.assertEqual(rep["qualified_targets"], [])
        self.assertEqual(rep["selection"]["filter"], "^open/basic$")

    def test_wrong_answers_fail(self):
        rc, rep, _ = self.run_harness("good", "--filter", "^range/basic/all$")
        self.assertEqual(rc, 1)
        self.assertEqual(rep["counts"]["fail"], 1)

    def test_full_run_of_incomplete_reader_fails_and_does_not_qualify(self):
        rc, rep, _ = self.run_harness("good")
        self.assertEqual(rc, 1)
        self.assertFalse(rep["selection"]["partial"])
        self.assertEqual(rep["qualified_targets"], [])
        self.assertGreater(rep["counts"]["fail"], 100)

    def test_nonzero_exit_is_a_run_error(self):
        rc, rep, _ = self.run_harness("nonzero-exit", "--filter", "^open/basic$")
        self.assertEqual(rc, 1)
        self.assertTrue(any("status 7" in e for e in rep["errors"]), rep["errors"])

    def test_duplicate_response(self):
        rc, rep, _ = self.run_harness("duplicate", "--filter", "^open/basic$")
        self.assertEqual(rc, 1)
        self.assertTrue(rep["errors"])

    def test_wrong_response_id(self):
        rc, rep, _ = self.run_harness("wrong-id", "--filter", "^open/basic$")
        self.assertEqual(rc, 1)
        self.assertTrue(any("id" in e for e in rep["errors"]), rep["errors"])

    def test_missing_response_times_out_and_is_reaped(self):
        rc, rep, elapsed = self.run_harness("missing-response", "--filter", "^open/(basic|multipart)$")
        self.assertEqual(rc, 1)
        self.assertTrue(any("no response" in e for e in rep["errors"]), rep["errors"])
        self.assertLess(elapsed, 30, "the harness must kill and reap a hung adapter")

    def test_adapter_that_does_not_exit_is_killed(self):
        rc, rep, elapsed = self.run_harness("no-exit", "--filter", "^open/basic$")
        self.assertEqual(rc, 1)
        self.assertTrue(any("did not exit" in e for e in rep["errors"]), rep["errors"])
        self.assertLess(elapsed, 30)

    def test_claimed_target_without_capability(self):
        rc, rep, _ = self.run_harness("lacks-capability")
        self.assertEqual(rc, 1)
        self.assertTrue(any("without capabilities" in e for e in rep["errors"]), rep["errors"])

    def test_unknown_target_name(self):
        rc, rep, _ = self.run_harness("unknown-target")
        self.assertEqual(rc, 1)
        self.assertTrue(any("unknown target" in e for e in rep["errors"]), rep["errors"])

    def test_dependencies_run_with_the_claimed_target(self):
        rc, rep, _ = self.run_harness("validator-full", "--targets", "validator-full")
        self.assertEqual(sorted(rep["selection"]["selected_targets"]),
                         ["validator-full", "validator-profile", "validator-structural"])
        self.assertGreater(rep["targets"]["validator-structural"]["fail"] + rep["targets"]["validator-structural"]["pass"], 0)
        self.assertEqual(rc, 1)  # the stub's validation reports are wrong
        self.assertEqual(rep["qualified_targets"], [])

    def test_narrower_targets_is_partial(self):
        rc, rep, _ = self.run_harness("validator-full", "--targets", "validator-structural")
        self.assertTrue(rep["selection"]["partial"])
        self.assertEqual(rep["qualified_targets"], [])


if __name__ == "__main__":
    unittest.main()
