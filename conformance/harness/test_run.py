#!/usr/bin/env python3
"""Self-tests of harness/run.py with deliberately deficient adapters (standard library only).

    python -m unittest discover -s harness -p 'test_*.py'      # from conformance/

Each stub adapter answers just enough of the protocol to reach the behaviour under test. A passing
reference adapter alone cannot show that the harness rejects incomplete implementations. The tests of the
reference adapter itself run only where its dependencies (generators/requirements.txt) are installed.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
RUN = HERE / "run.py"
sys.path.insert(0, str(HERE))

import run  # noqa: E402

try:
    import reference_adapter as ra  # noqa: E402
except ImportError:  # pyarrow or jsonschema missing
    ra = None

STUB = textwrap.dedent('''
    import json, sys, time
    mode = sys.argv[1]
    caps = ["open", "range", "list", "projection"]
    targets = ["reader"]
    if mode == "lacks-capability":
        caps = ["open"]
    if mode == "unknown-target":
        targets = ["reader", "sorcerer"]
    if mode == "planned":
        targets = ["reader", "planned"]
    if mode == "planner":
        caps = caps + ["planning"]
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
        elif req["op"] == "plan":
            resp = {"status": "ok", "entries": [], "count": 0, "count_exact": True, "projected_bytes": 0,
                    "projected_bytes_exact": True}
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

    def row_group_starts(d):
        """The global index of the first row of every non-empty row group, from the rows document."""
        doc = json.loads((CONF / "generators" / "rows" / f"{rows_name(d)}.json").read_text())
        starts, base = [], 0
        for part in doc["parts"]:
            for n in part["row_groups"]:
                if n:
                    starts.append(base)
                base += n
        return starts

    def install_bug(d, ds, req):
        """Apply the cursor and row-position bugs to ds; return the cursor to use."""
        cursor = req.get("after")
        if BUG == "ignore-offset" and isinstance(cursor, dict) and "offset" in cursor:  # BUG: (k, n) read as after k
            cursor = dict(cursor, offset=(1 << 63) - 1)
        if BUG == "prefix-cursor-floor" and isinstance(cursor, dict) and set(cursor) == {"prefix_base64"}:
            cursor = {"key_base64": cursor["prefix_base64"], "offset": (1 << 63) - 1}  # BUG: c read as keys > c
        if BUG in ("reset-offsets", "last-equal-row-group"):
            rows, starts = list(ds.positioned()), row_group_starts(d)
            if BUG == "reset-offsets":  # BUG: group offsets restart at every row group
                out = []
                for i, (k, n, v, p) in enumerate(rows):
                    out.append((k, 0 if i in starts or not out or out[-1][0] != k else out[-1][1] + 1, v, p))
                rows = out
            else:  # BUG: a seek starts at the last row group whose first key is <= the sought key
                seek = b64(req, "start_base64") or (b64(cursor, "key_base64") if isinstance(cursor, dict) else None)
                if seek is not None and starts:
                    i = max([j for j, s in enumerate(starts) if rows[s][0] <= seek], default=0)
                    rows = rows[starts[i]:]
            ds.positioned = lambda: iter(rows)
        return cursor

    def do_range(d, ds, req, cols, cursor):
        prefix, start, end = b64(req, "prefix_base64"), b64(req, "start_base64"), b64(req, "end_base64")
        excl, limit = req.get("start_exclusive", False), req["limit"]
        if BUG not in ("signed", "inclusive-end", "shortened-exact"):
            return ds.range(prefix=prefix, start=start, start_exclusive=excl, end=end, limit=limit, columns=cols,
                            cursor=cursor)
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
        if d == "datasets/brotli-key" and BUG in ("prereq-broken", "optional-no-code", "optional-wrong-code"):
            # BUGs: a validator without BROTLI that completes full after an incomplete profile, or reports the
            # incomplete profile without its unsupported code, or with the wrong one
            code = {"optional-no-code": None, "optional-wrong-code": "unsupported_encryption"}.get(BUG, "unsupported_encoding")
            out["structural"] = {"result": "complete"}
            if level in ("profile", "full"):
                out["profile"] = {"result": "incomplete", "reason": "BROTLI unavailable"}
                if code:
                    out["profile"]["code"] = code
            if level == "full":
                out["full"] = {"result": "complete"} if BUG == "prereq-broken" else \
                    {"result": "incomplete", "reason": "a prerequisite level did not complete"}
            return out
        flvl = f["validation_level"] if f else None
        for lvl in LEVELS[: LEVELS.index(level) + 1]:
            if flvl is None or flvl not in LEVELS or LEVELS.index(lvl) < LEVELS.index(flvl):
                out[lvl] = {"result": "complete"}
            elif lvl == flvl and f.get("outcome") == "unsupported":
                out[lvl] = {"result": "incomplete", "reason": "an unsupported feature"}
                if BUG != "no-unsupported-code":  # BUG: the incomplete level does not name the unsupported code
                    out[lvl]["code"] = f["codes"][0]
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
                    "capabilities": ["open", "range", "list", "projection", "verify", "validate:structural",
                                     "validate:profile", "validate:full", "validate:digest"]}
        d = Path(req["dataset"]).relative_to(CONF).as_posix()
        if req["op"] == "validate":
            return {"status": "ok", "levels": validate(d, req["level"])}
        f = faults.get(d)
        status = "unsupported" if f and f.get("outcome") == "unsupported" else "refused"
        if f and status == "unsupported" and req["op"] == "verify" and BUG == "verify-ok":
            return {"status": "ok"}  # BUG: verify does not check the part the range found unsupported
        if f and (f["reader_detect"] == "open" or req["op"] != "open" and f["reader_detect"] in ("touch", "read")):
            return {"status": status, "code": f["codes"][0]}
        if req["op"] in ("open", "verify"):
            return {"status": "ok"}
        ds = oracle.Dataset.load(rows_name(d))
        cols = req.get("columns", [])
        try:
            cursor = install_bug(d, ds, req)
            if req["op"] == "range":
                entries = do_range(d, ds, req, cols, cursor)
            else:
                entries = ds.list(prefix=b64(req, "prefix_base64") or b"", delimiter=b64(req, "delimiter_base64"),
                                  cursor=cursor, end=b64(req, "end_base64"), limit=req.get("limit"), columns=cols)
        except (oracle.InvalidRequest, ValueError, KeyError, TypeError):
            return {"status": "invalid_request"}
        except oracle.Unsupported as u:
            return {"status": "unsupported", "code": u.code}
        for e in entries:
            if BUG == "no-exact":  # BUG: a common prefix never carries its exact row
                e.pop("exact", None)
            if BUG == "exact-group-start" and "exact" in e:  # BUG: the exact row is the group's first row
                e["exact"]["offset"] = 0
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

    def test_detects_prefix_cursor_read_as_byte_floor(self):
        self.assert_detected("prefix-cursor-floor", r"^(list/delimiter/after-inside-prefix-continuation|pagination/list/)")

    def test_detects_seek_from_last_equal_row_group(self):
        self.assert_detected("last-equal-row-group", r"^repeated/(core/|one-large-group/seek-)")

    def test_detects_offsets_reset_at_row_groups(self):
        self.assert_detected("reset-offsets", r"^repeated/(core/|repeated-keys/cursor-row-)")

    def test_detects_ignored_cursor_offsets(self):
        self.assert_detected("ignore-offset", r"^repeated/(core/resume-|repeated-keys/cursor-row-)")

    def test_detects_missing_exact_rows(self):
        self.assert_detected("no-exact", r"^list/(exact/|delimiter/a-slash$)")

    def test_detects_exact_row_taken_from_the_group_start(self):
        self.assert_detected("exact-group-start", r"^repeated/core/listing-exact-row-inside-group$")

    def test_detects_absent_null_conflation(self):
        self.assert_detected("absent-null", r"^projection/(column/f64|columns/span-parts|col-7/)")

    def test_detects_validation_skipping_prerequisites(self):
        self.assert_detected("skip-prereq", r"^validate-(profile|full)/prerequisite/")

    def test_detects_unsupported_part_passing_verify(self):
        # an unsupported part follows the refusal lifecycle: verify on a fresh handle must answer unsupported too
        self.assert_detected("verify-ok", r"^refusals-open/encrypted-")

    def test_detects_unsupported_incomplete_without_its_code(self):
        self.assert_detected("no-unsupported-code", r"^validate-(structural|profile|full)/(prerequisite/)?encrypted-")

    def test_detects_level_completed_after_its_incomplete_prerequisite(self):
        self.assert_detected("prereq-broken", r"^validate-(profile|full)/brotli-key$")

    def test_detects_optional_incomplete_without_or_with_the_wrong_code(self):
        self.assert_detected("optional-no-code", r"^validate-(profile|full)/brotli-key$")
        self.assert_detected("optional-wrong-code", r"^validate-(profile|full)/brotli-key$")

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

    def test_unclaimed_optional_capability_is_skipped(self):
        rc, rep, _ = self.run_harness("good", "--filter", "^planning/basic/")
        self.assertEqual(rep["counts"]["fail"], 0, rep)
        self.assertGreater(rep["counts"]["skip"], 0)
        self.assertTrue(all(r["result"] == "skip" and r.get("optional") for r in rep["results"]))

    def test_plan_answers_are_compared(self):
        rc, rep, _ = self.run_harness("planner", "--filter", "^planning/basic/(all|empty)$")
        self.assertEqual(rc, 1)
        results = {r["id"]: r["result"] for r in rep["results"]}
        self.assertEqual(results, {"planning/basic/all": "fail", "planning/basic/empty": "pass"})

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

    def test_target_without_cases_of_its_own_never_qualifies(self):
        # "planned" depends on reader and has no cases: passing the reader cases must not qualify it.
        suite = Path(self.tmp.name) / "planned-suite"
        suite.mkdir(exist_ok=True)
        files = {
            "suite.json": {"format_version": 0, "draft_revision": 1, "case_files": ["cases.json"],
                           "mandatory_targets": ["reader"]},
            "capabilities.json": {"capabilities": {c: c for c in ("open", "range", "list", "projection")},
                                  "target_dependencies": {"planned": ["reader"]},
                                  "targets": {"reader": ["open", "range", "list", "projection"], "planned": ["open"]}},
            "cases.json": {"group": "open", "target": "reader", "cases": [
                {"id": "open/any", "dataset": "datasets/any", "request": {"op": "open"}, "expect": {"status": "ok"}}]},
        }
        for name, doc in files.items():
            (suite / name).write_text(json.dumps(doc), encoding="utf-8")
        rc, rep, _ = self.run_harness("planned", "--suite", str(suite / "suite.json"))
        self.assertEqual(rc, 0, rep)
        self.assertFalse(rep["selection"]["partial"])
        self.assertEqual(rep["qualified_targets"], ["reader"])


class ResponseNumbers(unittest.TestCase):
    """Offsets keep their JSON spelling: the object-valued cases cannot carry -0, so the parser is tested directly."""

    def entries(self, offset):
        return run.parse_response('[{"kind": "row", "key_base64": "YQ==", "offset": %s, "values": []}]' % offset)

    def test_plain_integer_offsets(self):
        for text, value in (("0", 0), ("7", 7), ("9223372036854775807", (1 << 63) - 1)):
            self.assertEqual(run.norm_entries(self.entries(text), 0, "r"), [("row", b"a", (value, ()))])

    def test_other_spellings_are_rejected(self):
        for text in ("-0", "0.0", "0e0", "1E2", "-1", "9223372036854775808", "NaN"):
            with self.assertRaises(run.CaseFail, msg=text):
                run.norm_entries(self.entries(text), 0, "r")


@unittest.skipIf(ra is None, "the reference adapter needs generators/requirements.txt")
class ReferenceAdapter(unittest.TestCase):
    """The reference adapter's request parsing and VAL-1 dispatch, with stubbed decoders where needed."""

    BASIC = str(HERE.parent / "datasets" / "basic")

    def setUp(self):
        ra.DATASETS.clear()

    def range_status(self, offset):
        line = '{"op": "range", "dataset": %s, "limit": 1, "after": {"key_base64": "YQ==", "offset": %s}}' % (
            json.dumps(self.BASIC), offset)
        return ra.handle(ra.parse_request(line))["status"]

    def test_offset_spellings(self):
        self.assertEqual(self.range_status("0"), "ok")
        for text in ("-0", "0.0", "0e0", "-1"):
            self.assertEqual(self.range_status(text), "invalid_request", text)

    def test_prefix_cursor_only_for_list(self):
        req = {"dataset": self.BASIC, "limit": 1, "after": {"prefix_base64": "YS8="}}
        self.assertEqual(ra.handle(dict(req, op="range"))["status"], "invalid_request")
        self.assertEqual(ra.handle(dict(req, op="list", delimiter_base64="Lw=="))["status"], "ok")

    def test_unsupported_full_decoding_is_incomplete(self):
        with mock.patch.object(ra.pq, "read_table", side_effect=ra.pa.ArrowNotImplementedError("stub codec")):
            levels = ra.handle({"op": "validate", "dataset": self.BASIC, "level": "full"})["levels"]
        self.assertEqual(levels["structural"], {"result": "complete"})
        self.assertEqual(levels["profile"], {"result": "complete"})
        self.assertEqual(levels["full"]["result"], "incomplete")
        self.assertTrue(levels["full"]["reason"])

    def test_unsupported_profile_decoding_keeps_structural_and_blocks_full(self):
        with mock.patch.object(ra.Part, "key_checks", side_effect=ra.NotSupported("unsupported_encoding", "stub")):
            levels = ra.handle({"op": "validate", "dataset": self.BASIC, "level": "full"})["levels"]
        self.assertEqual(levels["structural"], {"result": "complete"})
        self.assertEqual([levels[x]["result"] for x in ("profile", "full")], ["incomplete", "incomplete"])
        self.assertTrue(levels["profile"]["reason"] and levels["full"]["reason"])

    def test_digest_of_a_structurally_invalid_part(self):
        data = b"not a parquet file"
        manifest = {"format": "seekparquet", "format_version": 0, "draft_revision": 1, "schema_id": "s",
                    "key": {"column": "key", "unique": True},
                    "parts": [{"path": "p.parquet", "rows": 1, "bytes": len(data), "min_key_base64": "YQ==",
                               "max_key_base64": "YQ==", "sha256": hashlib.sha256(data).hexdigest()}]}
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "p.parquet").write_bytes(data)
            (Path(tmp) / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            self.assertEqual(ra.do_validate(tmp, "digest"), {"digest": {"result": "complete", "checked": 1}})
            self.assertEqual(ra.do_validate(tmp, "structural"),
                             {"structural": {"result": "failed", "code": "invalid_footer"}})


@unittest.skipIf(ra is None, "the reference adapter needs generators/requirements.txt")
class ReferenceRulings(unittest.TestCase):
    def setUp(self):
        sys.path.insert(0, str(HERE.parent))
        from validator import test_validate as fixtures
        self.f = fixtures
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        ra.DATASETS.clear()

    def tearDown(self):
        self.temp.cleanup()

    def request(self, **args):
        return ra.handle({"dataset": str(self.root), "op": "range", "limit": 10000, **args})

    def test_timestamp_fixed_carrier(self):
        self.f.timestamp_fixed_fixture(self.root)
        answer = self.request(columns=['value'])
        self.assertEqual(answer, {'status': 'ok', 'entries': [{'kind': 'row', 'key_base64': 'YQ==',
                          'offset': 0, 'values': [{'bytes_base64': 'AQAAAAAAAAAAAAAA'}]}]})

    def test_unknown_value_features_are_projection_dependent(self):
        p = self.f.p
        for version in ('1.0', '2.0'):
            for feature in ('encoding', 'codec'):
                with self.subTest(version=version, feature=feature):
                    ra.DATASETS.clear()
                    m = self.f.fixture(self.root, version=version)
                    if feature == 'codec':
                        self.f.rewrite_test_footer(self.root, m, lambda meta: meta.items_of(p.FMD_ROW_GROUPS)[0]
                                                  .items_of(p.RG_COLUMNS)[1].get(p.CC_META).set(p.CMD_CODEC, p.CT_I32, 99))
                    else:
                        self.f.rewrite_test_header(self.root, 1, lambda h: h.get(p.PH_DATA_V1 if version == '1.0'
                                                   else p.PH_DATA_V2).set(2 if version == '1.0' else 4, p.CT_I32, 99))
                    self.assertEqual(self.request()['status'], 'ok')
                    answer = self.request(columns=['value'])
                    self.assertEqual((answer['status'], answer['code']), ('unsupported', 'unsupported_encoding'))
                    # A failed projection must not poison subsequent key-only queries.
                    self.assertEqual(self.request()['status'], 'ok')
                    levels = self.request(op='validate', level='full')['levels']
                    self.assertEqual(levels['profile']['result'], 'complete')
                    self.assertEqual(levels['full']['result'], 'incomplete')
                    self.assertEqual(levels['full']['code'], 'unsupported_encoding')

    def test_unknown_key_features_only_when_needed(self):
        p = self.f.p
        for feature in ('encoding', 'codec'):
            ra.DATASETS.clear()
            m = self.f.fixture(self.root, version='1.0')
            if feature == 'codec':
                self.f.rewrite_test_footer(self.root, m, lambda meta: meta.items_of(p.FMD_ROW_GROUPS)[0]
                                          .items_of(p.RG_COLUMNS)[0].get(p.CC_META).set(p.CMD_CODEC, p.CT_I32, 99))
            else:
                self.f.rewrite_test_header(self.root, 0, lambda h: h.get(p.PH_DATA_V1).set(2, p.CT_I32, 99))
            self.assertEqual(self.request(start_base64='eg=='), {'status': 'ok', 'entries': []})
            answer = self.request()
            self.assertEqual((answer['status'], answer['code']), ('unsupported', 'unsupported_encoding'))
            levels = self.request(op='validate', level='full')['levels']
            self.assertEqual(levels['structural']['result'], 'complete')
            self.assertEqual(levels['profile']['code'], 'unsupported_encoding')

    def test_ignored_values_and_required_fields(self):
        p = self.f.p
        m = self.f.fixture(self.root, version='1.0')
        for ci in (0, 1):
            self.f.rewrite_test_header(self.root, ci, lambda h: h.get(p.PH_DATA_V1).set(4, p.CT_I32, 99))
        self.f.rewrite_indexes(self.root / 'part.parquet', lambda indexes: indexes[0][1].set(p.CI_BOUNDARY, p.CT_I32, 99))
        self.f.save(self.root, m)
        self.assertEqual(self.request(columns=['value'])['status'], 'ok')
        self.f.rewrite_test_header(self.root, 1, lambda h: h.get(p.PH_DATA_V1).remove(4))
        ra.DATASETS.clear()
        self.assertEqual(self.request(columns=['value'])['code'], 'invalid_parquet')
        m = self.f.fixture(self.root)
        self.f.rewrite_indexes(self.root / 'part.parquet', lambda indexes: indexes[0][1].remove(p.CI_BOUNDARY))
        self.f.save(self.root, m)
        ra.DATASETS.clear()
        self.assertEqual(self.request()['code'], 'invalid_page_index')

    def test_valueless_metadata_and_stamps(self):
        p = self.f.p
        m = self.f.fixture(self.root)
        def ignored(meta):
            for key in (b'ordinary', b'seekparquet.future'):
                for _ in range(2):
                    meta.get(p.FMD_KV).items.append(p.Struct([(p.KV_KEY, p.CT_BINARY, key)]))
        self.f.rewrite_test_footer(self.root, m, ignored)
        self.assertEqual(self.request()['status'], 'ok')
        self.f.rewrite_test_footer(self.root, m, lambda meta: meta.get(p.FMD_KV).items.append(
                                   p.Struct([(p.KV_KEY, p.CT_BINARY, b'seekparquet.schema_id')])))
        ra.DATASETS.clear()
        self.assertEqual(self.request()['code'], 'stamp_mismatch')
        m = self.f.fixture(self.root)
        self.f.rewrite_test_footer(self.root, m, lambda meta: meta.get(p.FMD_KV).items.append(p.Struct()))
        ra.DATASETS.clear()
        self.assertEqual(self.request()['code'], 'invalid_footer')

    def test_header_and_annotation_classifications(self):
        p = self.f.p
        for version, mutate, code in (
            ('1.0', lambda h: h.get(p.PH_DATA_V1).set(1, p.CT_I32, -5), 'invalid_parquet'),
            ('2.0', lambda h: h.get(p.PH_DATA_V2).set(5, p.CT_I32, -1), 'invalid_parquet'),
            ('1.0', lambda h: h.set(p.PH_COMPRESSED, p.CT_I32, 1000000), 'invalid_page_index'),
        ):
            ra.DATASETS.clear()
            self.f.fixture(self.root, version=version)
            self.f.rewrite_test_header(self.root, 0, mutate)
            self.assertEqual(self.request()['code'], code)
        m = self.f.fixture(self.root)
        self.f.rewrite_test_footer(self.root, m, lambda meta: meta.items_of(p.FMD_SCHEMA)[2].set(
                                  10, p.CT_STRUCT, p.Struct([(6, p.CT_STRUCT, p.Struct())])))
        ra.DATASETS.clear()
        self.assertEqual(self.request()['code'], 'invalid_footer')
        for lt in (p.Struct([(99, p.CT_STRUCT, p.Struct())]), p.Struct([(8, p.CT_STRUCT,
                   p.Struct([(1, p.CT_BOOL, False), (2, p.CT_STRUCT, p.Struct([(99, p.CT_STRUCT, p.Struct())]))]))])):
            m = self.f.fixture(self.root)
            self.f.rewrite_test_footer(self.root, m, lambda meta: meta.items_of(p.FMD_SCHEMA)[2].set(10, p.CT_STRUCT, lt))
            ra.DATASETS.clear()
            answer = self.request(columns=['value'])
            self.assertEqual(answer['status'], 'ok', answer)


if __name__ == "__main__":
    unittest.main()
