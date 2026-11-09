#!/usr/bin/env python3
"""Generate conformance/cases/generated/*.generated.json, suite.json and COVERAGE.md, deterministically.

Cases are of two kinds:

* ``cases/core/*.json``: about 150 hand-reviewed examples, each with a description of the observation that
  distinguishes a correct implementation. They are authored, never written by this script. This script
  checks every core case against the generator's own candidate (or the oracle) and stops on a mismatch.
* ``cases/generated/*.generated.json``: the generated families (boundary sweeps, parameter variants,
  pagination expansions, one validation case per fault at its lowest detecting level, a few prerequisite
  and digest cases), excluding every case owned by core and every retired id.

Expected entries come only from ``oracle.py`` applied to the source rows in ``generators/rows/``. Expected
refusals and validation levels come from ``generators/faults.json``. No reader output is ever used.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONF = HERE.parent
sys.path.insert(0, str(CONF))
sys.path.insert(0, str(HERE))

import oracle  # noqa: E402
from common import b64e, dumps, load_rows, write_text  # noqa: E402

SUITE_REVISION = "0.1.0-draft.4"
GENERATOR = "generators/make_cases.py"
# Datasets renamed for clarity: old label (kept in case ids, which are stable) -> dataset directory.
RENAMED = {"truncated-bounds": "binary-bounds-truncated-16-byte",
           "truncated-bounds-2": "binary-bounds-truncated-2-byte"}
# Valid datasets with nested or repeated columns (projection fixtures): a validator that does not support
# such columns reports the full level incomplete (VAL-1), so full validation accepts complete or incomplete.
NESTED_COLUMNS = {"projection": "meta (struct) and tags (list)", "key-legacy-utf8": "meta (struct) and tags (list)"}
GROUP_ORDER = ["open", "range", "prefix", "list", "pagination", "projection", "requests", "refusals-open",
               "refusals-read", "validate-structural", "validate-profile", "validate-full", "validate-digest"]
# Datasets that use optional features (outside ENC-1): a reader may support them or answer unsupported.
OPTIONAL = {"optional-codecs": ["unsupported_encoding"]}
BIG = 100000  # a limit at least the number of rows of every fixture (QUERY-3)
# Retired cases that tested only removed request members; their ids are never reused.
REMOVED = {"list/delimiter/continuation-without-after": "tested the removed continuation member"}


class Group:
    def __init__(self, name, target, description):
        self.name, self.target, self.description = name, target, description
        self.cases = []
        self.ids = set()
        self.families = {}
        self.family = None

    def fam(self, name, description):
        """Set the family of the cases added next."""
        self.family = f"{self.name}/{name}"
        self.families[self.family] = description

    def add(self, cid, reqs, dataset, expect, request=None, raw_request=None, desc=None, requires=None, detect=None,
            meta=None):
        cid = f"{self.name}/{cid}"
        assert cid not in self.ids, cid
        assert self.family, f"{cid}: no family"
        self.ids.add(cid)
        c = {"id": cid}
        if desc:
            c["description"] = desc
        c["family"] = self.family
        for k, v in (meta or {}).items():
            c[k] = v
        if any("exact" in e for e in expect.get("entries", [])):
            reqs = reqs + ["LIST-8"]  # a common prefix carries its exact row
        c["requirements"] = sorted(set(reqs), key=reqs.index)
        if requires:
            c["requires"] = requires
        if not dataset.startswith("datasets/"):
            dataset = f"datasets/{RENAMED.get(dataset, dataset)}"
        c["dataset"] = dataset
        if detect:
            c["detect"] = detect
        if request is not None:
            c["request"] = request
        if raw_request is not None:
            c["raw_request"] = raw_request
        c["expect"] = expect
        self.cases.append(c)

    def doc(self, cases):
        fams = {f: d for f, d in self.families.items() if any(c["family"] == f for c in cases)}
        return {"group": self.name, "target": self.target, "generated": True, "generator": GENERATOR,
                "description": self.description, "families": fams, "cases": cases}


def B(b):
    return None if b is None else b64e(b)


def rq_range(start=None, start_exclusive=None, end=None, limit=BIG, columns=None, null_bounds=False, prefix=None):
    r = {"op": "range"}
    if prefix is not None:
        r["prefix_base64"] = B(prefix)
    if start is not None or null_bounds:
        r["start_base64"] = B(start)
    if start_exclusive is not None:
        r["start_exclusive"] = start_exclusive
    if end is not None or null_bounds:
        r["end_base64"] = B(end)
    r["limit"] = limit
    if columns:
        r["columns"] = list(columns)
    return r


def row_cursor(key, offset=0):
    return {"key_base64": B(key), "offset": offset}


def prefix_cursor(prefix):
    return {"prefix_base64": B(prefix)}


def cursor_after(entry):
    """The cursor of a returned entry (PAGE-1)."""
    if entry["kind"] == "row":
        return {"key_base64": entry["key_base64"], "offset": entry["offset"]}
    return {"prefix_base64": entry["key_base64"]}


def rq_list(prefix=b"", delimiter=b"/", after=None, end=None, limit=BIG, columns=None):
    r = {"op": "list"}
    if prefix:
        r["prefix_base64"] = B(prefix)
    r["delimiter_base64"] = B(delimiter)
    if after is not None:
        r["after"] = after
    if end is not None:
        r["end_base64"] = B(end)
    r["limit"] = limit
    if columns:
        r["columns"] = list(columns)
    return r


def run(ds, req):
    """Apply the oracle to a request; return the expect object."""
    args = {k: req.get(k) for k in req}
    cols = args.get("columns") or []
    try:
        if req["op"] == "range":
            e = ds.range(prefix=dec(args.get("prefix_base64")), start=dec(args.get("start_base64")), start_exclusive=args.get("start_exclusive", False),
                         end=dec(args.get("end_base64")), limit=args.get("limit"), columns=cols, cursor=args.get("after"))
        else:
            e = ds.list(prefix=dec(args.get("prefix_base64")) or b"", delimiter=dec(args["delimiter_base64"]),
                        cursor=args.get("after"), end=dec(args.get("end_base64")), limit=args.get("limit"), columns=cols)
    except oracle.InvalidRequest:
        return {"status": "invalid_request"}
    except oracle.Unsupported as u:
        return {"status": "unsupported", "code": u.code}
    return {"status": "ok", "entries": e}


def dec(s):
    import base64
    return None if s is None else base64.b64decode(s, validate=True)


def pages(doc):
    """[(part index, row group index, page index, keys)] from a rows document's layout."""
    out = []
    for pi, part in enumerate(doc["parts"]):
        keys = [dec(r["key_base64"]) for r in part["rows"]]
        mrpp = part["writer"]["max_rows_per_page"]
        off = 0
        for ri, n in enumerate(part["row_groups"]):
            for gi, s in enumerate(range(0, n, mrpp)):
                out.append((pi, ri, gi, keys[off + s: off + min(s + mrpp, n)]))
            off += n
    return out


def show(b):
    if len(b) > 24:
        return f"{b[:8].hex()}..({len(b)}B)"
    if all(0x21 <= c <= 0x7E for c in b):
        return b.decode()
    return "0x" + b.hex()


# --------------------------------------------------------------------------

def open_group(lock):
    g = Group("open", "reader", "Opening every valid dataset succeeds (CHECK-1), including datasets with "
                                "unusual but valid layouts.")
    g.fam("valid-dataset", "open each valid dataset; it must succeed")
    for d in lock["datasets"]:
        if d["valid"]:
            reqs = ["CHECK-1", "LAYOUT-1", "MAN-1", "MAN-3"]
            if d["name"] == "multipart":
                reqs += ["LAYOUT-3", "EXT-1", "ENC-1", "PART-5"]
            if d["name"] == "empty":
                reqs += ["MAN-3"]
            if d["name"] in ("projection", "key-legacy-utf8"):
                reqs += ["KEY-1", "KEY-6"]
            if d["name"] == "extensions-numbers":
                reqs += ["EXT-1", "MAN-6"]
            if d["name"] == "encodings":
                reqs += ["ENC-1"]
            exp = {"status": "ok"}
            if d["name"] in OPTIONAL:
                reqs += ["ENC-1"]
                exp["accept_unsupported"] = OPTIONAL[d["name"]]
            label = {v: k for k, v in RENAMED.items()}.get(d["name"], d["name"])
            g.add(label, reqs, d["dir"], exp, request={"op": "open"}, desc=d["description"])
    g.fam("empty-dataset", "queries on the dataset with no parts")
    e = oracle.Dataset.load("empty")
    g.add("empty-range", ["RANGE-1", "MAN-3"], "empty", run(e, rq_range()), request=rq_range())
    g.add("empty-list", ["LIST-4", "MAN-3"], "empty", run(e, rq_list()), request=rq_list())
    return g


def range_group():
    g = Group("range", "reader", "Range reads (RANGE-1, RANGE-2, QUERY-2): bounds, limits and boundaries "
                                 "of pages, row groups and parts. Expected rows come from oracle.py.")

    def add(cid, reqs, dsname, rows_name, req, desc=None):
        ds = oracle.Dataset.load(rows_name)
        g.add(cid, reqs, dsname, run(ds, req), request=req, desc=desc)

    R = ["RANGE-1"]

    def miss(rows_name, *keys):
        ks = set(oracle.Dataset.load(rows_name).keys())
        for k in keys:
            assert k not in ks, f"a case labelled as a miss uses an existing key {k!r}"

    g.fam("bounds", "start, end, exclusivity, empty and null bounds, and limits on basic")
    miss("basic", b"alp", b"bb", b"c", b"A", b"zulu")
    add("basic/all", R, "basic", "basic", rq_range())
    add("basic/all-columns", R + ["RANGE-2", "COL-3"], "basic", "basic", rq_range(columns=["size", "etag"]))
    add("basic/start-inclusive-hit", R, "basic", "basic", rq_range(start=b"alpha"))
    add("basic/start-exclusive-hit", R, "basic", "basic", rq_range(start=b"alpha", start_exclusive=True))
    add("basic/start-inclusive-explicit", R, "basic", "basic", rq_range(start=b"alpha", start_exclusive=False))
    add("basic/start-miss", R, "basic", "basic", rq_range(start=b"alp"), "start falls between two keys")
    add("basic/start-exclusive-miss", R, "basic", "basic", rq_range(start=b"alp", start_exclusive=True))
    add("basic/start-before-all", R, "basic", "basic", rq_range(start=b"A"))
    add("basic/start-after-all", R, "basic", "basic", rq_range(start=b"zulu"), "empty result")
    add("basic/start-exclusive-last", R, "basic", "basic", rq_range(start=b"foxtrot", start_exclusive=True))
    add("basic/end-hit", R, "basic", "basic", rq_range(end=b"charlie"), "end is exclusive")
    add("basic/end-miss", R, "basic", "basic", rq_range(end=b"c"))
    add("basic/end-first", R, "basic", "basic", rq_range(end=b"a/1.txt"), "end equals the first key: no rows")
    add("basic/start-end", R, "basic", "basic", rq_range(start=b"b", end=b"delta"))
    add("basic/start-equals-end", R, "basic", "basic", rq_range(start=b"b", end=b"b"))
    add("basic/start-after-end", R, "basic", "basic", rq_range(start=b"delta", end=b"b"))
    add("basic/exclusive-start-end", R, "basic", "basic", rq_range(start=b"b", start_exclusive=True, end=b"c/d"))
    add("basic/empty-start", R + ["QUERY-2"], "basic", "basic", rq_range(start=b""), "an empty start includes every key")
    add("basic/empty-start-exclusive", R + ["QUERY-2"], "basic", "basic", rq_range(start=b"", start_exclusive=True))
    add("basic/empty-end", R + ["QUERY-2"], "basic", "basic", rq_range(end=b""), "an empty end includes no key")
    add("basic/null-bounds", R, "basic", "basic", rq_range(null_bounds=True), "start and end given as null")
    add("basic/limit-1", R, "basic", "basic", rq_range(start=b"b", limit=1))
    add("basic/limit-1-miss", R, "basic", "basic", rq_range(start=b"bb", limit=1), "exact miss: the next key")
    add("basic/limit-3", R, "basic", "basic", rq_range(start=b"alpha", limit=3))
    add("basic/limit-exceeds", R, "basic", "basic", rq_range(start=b"echo", limit=50))

    # boundaries of pages, row groups and parts, from the layout of the source rows
    for kind_name, kind_desc in (("page", "page"), ("row-group", "row group"), ("part", "part")):
        g.families[f"range/boundary-{kind_name}"] = (
            f"five probes around each {kind_desc} boundary of the written layout (start at the first key after it, "
            "exclusive and inclusive start at the last key before it, start just above that key, end at the first "
            "key after it); at most four page boundaries per dataset")
    for name in ("basic", "multipart", "delimiter", "binary-keys", "projection"):
        doc = load_rows(name)
        pg = pages(doc)
        n_page = 0
        for (p0, r0, g0, k0), (p1, r1, g1, k1) in zip(pg, pg[1:]):
            kind = "part" if p1 != p0 else ("row-group" if r1 != r0 else "page")
            if kind == "page":
                n_page += 1
                if n_page > 4:
                    continue
            reqs = R + (["KEY-3"] if kind != "page" else []) + (["LAYOUT-1"] if kind == "part" else [])
            tag = f"{name}/{kind}-{p1}.{r1}.{g1}"
            g.family = f"range/boundary-{kind}"
            add(f"{tag}/start-at-first", reqs, name, name, rq_range(start=k1[0], limit=2),
                f"start at the first key of the {kind} beginning at {show(k1[0])}")
            add(f"{tag}/exclusive-at-last", reqs, name, name, rq_range(start=k0[-1], start_exclusive=True, limit=2),
                f"start after the last key of the previous {kind}")
            add(f"{tag}/inclusive-at-last", reqs, name, name, rq_range(start=k0[-1], limit=2))
            add(f"{tag}/between", reqs, name, name, rq_range(start=k0[-1] + b"\x00", limit=1),
                "start just above the last key before the boundary")
            add(f"{tag}/end-at-first", reqs, name, name, rq_range(start=k0[-1], end=k1[0]),
                "end at the first key after the boundary")
    # multipart: the whole dataset (empty row groups, stray file, three codecs)
    g.fam("whole-dataset", "every row of a dataset, with columns")
    add("multipart/all", R + ["RANGE-2", "LAYOUT-1", "LAYOUT-3", "ENC-1"], "multipart", "multipart",
        rq_range(columns=["size", "storage_class", "mtime"]),
        "every row; the stray file under the root and empty row groups contribute nothing")
    add("multipart/stray-not-read", R + ["LAYOUT-1"], "multipart", "multipart",
        rq_range(start=b"logs/2026/02/s", end=b"logs/2026/03/"),
        "the unlisted file shards/stray.parquet holds keys in this range; they must not appear")

    # binary keys and conservative bounds
    g.families["range/seek-key"] = ("start exactly at every key of binary-keys, on the exact-bounds layout and on "
                                    "both shortened-bounds layouts; limit 1")
    g.families["range/start-just-above-key"] = (
        "start at key + 0x00 for every key, limit 1: the first key at or after it, which may be an existing key "
        "(the case ids say seek-miss for historical reasons; the probe is not necessarily a miss)")
    g.families["range/end-at-key"] = "end exactly at every key on the 2-byte shortened bounds, start at its first byte"
    for name in ("binary-keys", "truncated-bounds", "truncated-bounds-2"):
        ds = oracle.Dataset.load("binary-keys")
        keys = set(ds.keys())
        reqs = R + ["KEY-3", "KEY-4"] + (["RG-5", "RG-6"] if name != "binary-keys" else [])
        g.family = "range/whole-dataset"
        add(f"{name}/all", reqs, name, "binary-keys", rq_range(columns=["v"]))
        for i, k in enumerate(ds.keys()):
            g.family = "range/seek-key"
            add(f"{name}/seek-{i:02d}", reqs, name, "binary-keys", rq_range(start=k, limit=1),
                f"start exactly at {show(k)}")
            if name != "truncated-bounds":
                g.family = "range/start-just-above-key"
                above = k + b"\x00"
                add(f"{name}/seek-miss-{i:02d}", reqs, name, "binary-keys", rq_range(start=above, limit=1),
                    f"start at {show(k)} + 0x00, " + ("which is itself a key" if above in keys else "between keys"))
            if name == "truncated-bounds-2":
                g.family = "range/end-at-key"
                add(f"{name}/end-{i:02d}", reqs, name, "binary-keys", rq_range(start=k[:1], end=k, limit=3),
                    f"end exactly at {show(k)}")
    for name, src, cols, extra in (
            ("non-monotone-bounds", "basic", ["size", "etag"], ["RG-5", "RG-6"]),
            ("no-value-page-index", "basic", ["size", "etag"], ["COL-4", "RANGE-2"]),
            ("non-utf8-keys", "non-utf8-keys", ["size"], ["KEY-6"]),
            ("extensions-numbers", "basic", ["size", "etag"], ["EXT-1"]),
            ("encodings", "encodings", ["i32", "i64", "s"], ["ENC-1", "RANGE-2"]),
            ("annotated", "annotated", ["d", "ts_ms", "ts_utc", "dec9", "dec18", "dec30", "s"], ["RANGE-2", "COL-3"])):
        ds = oracle.Dataset.load(src)
        reqs = R + extra
        g.family = "range/whole-dataset"
        add(f"{name}/all", reqs, name, src, rq_range(columns=cols))
        g.fam("seek-with-columns", "start at every key with limit 2 and all scalar columns, on layouts with unusual "
                                   "indexes, encodings or annotations")
        for i, k in enumerate(ds.keys()):
            add(f"{name}/seek-{i:02d}", reqs, name, src, rq_range(start=k, limit=2, columns=cols))
    return g


def prefix_group():
    g = Group("prefix", "reader", "Prefix reads: range requests with prefix_base64 (RANGE-1), alone and "
                                  "intersected with start and end. A prefix read of p equals the range from p to "
                                  "successor(p), or unbounded above when p has no successor (section 6.2).")
    cases = [
        ("multipart", [b"logs/2026/02/", b"logs/", b"logs/2026/", b"app/v1/obj-00", b"tmp/", b"zz", b"nothing", b""]),
        ("binary-keys", [b"a", b"a\x00", b"\x00", b"\x7f", b"\x7f\xff", b"\xc3", b"p", b"pq", b"L" * 30, b"\xff",
                         b"\xff\xff", b"\xff" * 80, b"\xff" * 81, b"\xff" * 82, b"\xfe\xff", b"M" * 70 + b"\xff" * 10]),
        ("truncated-bounds-2", [b"\xff", b"\xff\xff", b"\xff" * 80, b"L" * 30, b"M" * 70 + b"\xff" * 10, b"a"]),
        ("delimiter", [b"a/b/", b"a/", b"caf\xc3\xa9", b"x\xff", b"\xff", b"\xff\xff", b"\xff\xff\xff"]),
    ]
    g.fam("prefix-only", "a range with only prefix_base64; asserted equal to the range from p to successor(p)")
    for name, prefixes in cases:
        src = "binary-keys" if name.startswith("truncated") else name
        ds = oracle.Dataset.load(src)
        for i, p in enumerate(prefixes):
            s = oracle.successor(p)
            req = rq_range(prefix=p)
            exp = run(ds, req)
            # The definition: the same rows as the range p .. successor(p).
            assert exp == run(ds, rq_range(start=p, end=s)), (name, p)
            g.add(f"{name}/{i:02d}", ["RANGE-1", "KEY-3"] + (["QUERY-2"] if p == b"" else []), name, exp,
                  request=req, desc=f"prefix {show(p) or '(empty)'}" +
                  ("" if s is not None or p == b"" else " (all 0xFF bytes: no successor)") +
                  (" (ends in 0xFF)" if p.endswith(b"\xff") and p.strip(b"\xff") else ""))
    combos = [
        ("multipart", b"logs/2026/02/", dict(start=b"logs/2026/02/f010")),
        ("multipart", b"logs/2026/02/", dict(start=b"logs/2026/02/f010", start_exclusive=True, limit=3)),
        ("multipart", b"logs/2026/02/", dict(end=b"logs/2026/02/f007")),
        ("multipart", b"logs/2026/02/", dict(start=b"app/", end=b"logs/2026/02/f002"), "start below the prefix"),
        ("multipart", b"logs/2026/02/", dict(start=b"logs/2026/03/"), "start above every key with the prefix"),
        ("multipart", b"logs/2026/02/", dict(end=b"logs/2026/02/"), "end equal to the prefix: nothing"),
        ("multipart", b"logs/2026/02/", dict(start=b"logs/2026/02/", start_exclusive=True, limit=2),
         "exclusive start equal to the prefix"),
        ("multipart", b"logs/", dict(start=b"logs/2026/02/f038", end=b"logs/2026/03/f002"), "spans a part boundary"),
        ("binary-keys", b"\xff", dict(start=b"\xff\xfe")),
        ("binary-keys", b"\xff", dict(start=b"\xff\xff", start_exclusive=True, end=b"\xff" * 81)),
        ("binary-keys", b"\xff\xff", dict(end=b"\xff\xff\xff\xff")),
        ("truncated-bounds-2", b"\xff", dict(start=b"\xff" * 80 + b"\x01", start_exclusive=True)),
        ("binary-keys", b"a", dict(start=b"", end=b"")),
        ("binary-keys", b"", dict(start=b"\x7f", limit=4)),
    ]
    g.fam("prefix-with-bounds", "prefix intersected with start and end")
    for i, c in enumerate(combos):
        name, p, kw = c[0], c[1], dict(c[2])
        src = "binary-keys" if name.startswith("truncated") else name
        ds = oracle.Dataset.load(src)
        kw.setdefault("limit", BIG)
        req = rq_range(prefix=p, **kw)
        g.add(f"{name}/with-bounds-{i:02d}", ["RANGE-1"] + (["QUERY-2"] if b"" in (kw.get("start"), kw.get("end")) else []),
              name, run(ds, req), request=req, desc=(c[3] + "; " if len(c) > 3 else "") + "prefix intersected with start/end")
    return g


def list_group():
    g = Group("list", "reader", "Delimited listing (LIST-2 to LIST-8). Expected entries come from oracle.py.")

    def add(cid, reqs, name, req, desc=None, src=None):
        ds = oracle.Dataset.load(src or name)
        g.add(cid, reqs, name, run(ds, req), request=req, desc=desc)

    L = ["LIST-2", "LIST-3", "LIST-4"]
    D = "delimiter"
    g.fam("delimiter-semantics", "rollups, rows, delimiters and prefixes on the folder-like delimiter dataset")
    add("delimiter/root", L, D, rq_list(), "empty prefix; keys beginning with the delimiter roll up to '/'")
    add("delimiter/a", L, D, rq_list(prefix=b"a"), "prefix without a trailing delimiter; key a is a row")
    add("delimiter/a-slash", L, D, rq_list(prefix=b"a/"), "key equal to the prefix is a row; a// and a/b/ roll up")
    add("delimiter/a-slash-slash", L, D, rq_list(prefix=b"a//"), "repeated delimiters")
    add("delimiter/a-b", L, D, rq_list(prefix=b"a/b/"), "a/b/ spans parts 0 and 1")
    add("delimiter/a-b-c", L, D, rq_list(prefix=b"a/b/c"))
    add("delimiter/slash", L, D, rq_list(prefix=b"/"), "delimiter at the start of keys")
    add("delimiter/slash-slash", L, D, rq_list(prefix=b"//"))
    add("delimiter/no-match", L, D, rq_list(prefix=b"q"), "no key has the prefix")
    add("delimiter/ff-delim-root", L + ["QUERY-1"], D, rq_list(delimiter=b"\xff"), "non-ASCII delimiter 0xFF")
    add("delimiter/ff-delim-x", L + ["QUERY-1"], D, rq_list(prefix=b"x", delimiter=b"\xff"))
    add("delimiter/ff-delim-xff", L + ["QUERY-1"], D, rq_list(prefix=b"x\xff", delimiter=b"\xff"),
        "x\\xff\\xff rolls up to itself")
    add("delimiter/utf8-lead-byte", L + ["QUERY-1"], D, rq_list(prefix=b"caf", delimiter=b"\xc3"),
        "delimiter equal to a UTF-8 lead byte")
    add("delimiter/utf8-cont-byte", L + ["QUERY-1"], D, rq_list(prefix=b"caf\xc3", delimiter=b"\xa9"),
        "delimiter equal to a UTF-8 continuation byte")
    add("delimiter/prefix-ff", L, D, rq_list(prefix=b"\xff"), "prefix 0xFF has no successor")
    add("delimiter/prefix-ff-ff", L, D, rq_list(prefix=b"\xff\xff"))
    add("delimiter/delim-a", L, D, rq_list(delimiter=b"a"), "a letter as delimiter")
    add("delimiter/limit-1", L, D, rq_list(prefix=b"a/", limit=1))
    add("delimiter/limit-2", L, D, rq_list(limit=2))
    add("delimiter/columns", L + ["RANGE-2"], D, rq_list(prefix=b"a/", columns=["size"]))
    add("delimiter/columns-root", L + ["RANGE-2"], D, rq_list(columns=["size"]))
    E = L + ["LIST-7"]
    g.fam("bounds-and-after", "end, row cursors and prefix cursors applied to candidate keys")
    add("delimiter/end-inside-prefix", E, D, rq_list(end=b"a/b"), "end inside the range of a/")
    add("delimiter/end-at-prefix", E, D, rq_list(prefix=b"a/", end=b"a/b/"),
        "end equal to a common prefix: the key a/b/ is not a candidate")
    add("delimiter/end-inside-a-b", E, D, rq_list(prefix=b"a/", end=b"a/b/c/"))
    add("delimiter/end-empty", E + ["QUERY-2"], D, rq_list(end=b""), "an empty end: nothing")
    add("delimiter/end-below-prefix", E, D, rq_list(prefix=b"b", end=b"a"))
    C = E + ["LIST-5", "PAGE-1"]
    add("delimiter/after-row", C, D, rq_list(prefix=b"a/", after=row_cursor(b"a/")))
    add("delimiter/after-inside-prefix", C, D, rq_list(prefix=b"a/", after=row_cursor(b"a/b/c")),
        "row cursor a/b/c: a/b/ is returned because a/b/c/ and later keys roll up to it")
    add("delimiter/after-inside-prefix-continuation", C + ["LIST-6"], D,
        rq_list(prefix=b"a/", after=prefix_cursor(b"a/b/")), "prefix cursor a/b/: nothing under a/b/ is returned")
    # after-equals-prefix-continuation (the byte floor a/b/ with continuation) became the same prefix cursor as
    # after-inside-prefix-continuation, and is retired as its duplicate.
    add("delimiter/after-equals-prefix-continuation", C + ["LIST-6"], D,
        rq_list(prefix=b"a/", after=prefix_cursor(b"a/b/")))
    add("delimiter/after-past-prefix", C, D, rq_list(after=row_cursor(b"a/zz")))
    add("delimiter/after-empty", C + ["QUERY-2"], D, rq_list(after=row_cursor(b"")),
        "every key is greater than the empty cursor key")
    add("delimiter/after-and-end", C, D, rq_list(after=row_cursor(b"a//"), end=b"a/c"))
    LB = "list-bounds"
    add("list-bounds/worked-example", E, LB, rq_list(end=b"a/b"), "LIST-7: a/a is a candidate, so a/ appears")
    add("list-bounds/worked-example-without-a-a", E, LB, rq_list(after=row_cursor(b"a/a"), end=b"a/b"),
        "LIST-7: with a/a excluded by the cursor, a/z alone (beyond end) does not make a/ appear")
    add("list-bounds/after-a-a", C, LB, rq_list(after=row_cursor(b"a/a")),
        "row cursor a/a: a/ is returned because a/z rolls up to it")
    add("list-bounds/after-a-a-continuation", C + ["LIST-6"], LB, rq_list(after=prefix_cursor(b"a/")))
    add("list-bounds/end-c", E, LB, rq_list(end=b"c"))
    M = "multipart"
    g.fam("other-datasets", "listings over the multipart, binary-keys, shortened-bounds and empty datasets")
    add("multipart/root", L, M, rq_list())
    add("multipart/logs", L, M, rq_list(prefix=b"logs/"))
    add("multipart/logs-2026", L, M, rq_list(prefix=b"logs/2026/"), "logs/2026/02/ spans parts 0 and 1")
    add("multipart/logs-2026-02", L + ["RANGE-2"], M, rq_list(prefix=b"logs/2026/02/", columns=["size", "mtime"]),
        "rows from two parts")
    add("multipart/logs-2026-02-limit", L, M, rq_list(prefix=b"logs/2026/02/", limit=5))
    add("multipart/tmp", L, M, rq_list(prefix=b"tmp/"))
    add("multipart/delim-dot", L, M, rq_list(prefix=b"logs/2027/01/", delimiter=b"."))
    BK = "binary-keys"
    add("binary-keys/nul-delim", L + ["QUERY-1"], BK, rq_list(delimiter=b"\x00"))
    add("binary-keys/nul-delim-a", L + ["QUERY-1"], BK, rq_list(prefix=b"a", delimiter=b"\x00"))
    add("binary-keys/ff-delim", L + ["QUERY-1"], BK, rq_list(delimiter=b"\xff"))
    add("binary-keys/ff-delim-ff", L + ["QUERY-1"], BK, rq_list(prefix=b"\xff", delimiter=b"\xff"))
    add("binary-keys/upper-l-delim", L + ["QUERY-1"], BK, rq_list(delimiter=b"L"))
    add("truncated-bounds-2/ff-delim-ff", L + ["QUERY-1", "RG-6"], "truncated-bounds-2",
        rq_list(prefix=b"\xff", delimiter=b"\xff"), src=BK)
    add("truncated-bounds-2/upper-m-prefix", L + ["RG-6"], "truncated-bounds-2", rq_list(prefix=b"M", delimiter=b"\xff"),
        src=BK)
    add("empty/root", L, "empty", rq_list())
    return g


def pagination_group():
    g = Group("pagination", "reader", "Paginated listings and ranges (LIST-6, QUERY-3): each page is a case; "
                                      "make_cases.py asserts that the pages concatenate to the unpaginated result.")
    configs = [("delimiter", b"", b"/", 1), ("delimiter", b"", b"/", 3), ("delimiter", b"a/", b"/", 2),
               ("delimiter", b"", b"\xff", 4), ("multipart", b"", b"/", 2), ("multipart", b"logs/2026/02/", b"/", 7),
               ("multipart", b"logs/", b"/", 1), ("binary-keys", b"", b"\xff", 5), ("list-bounds", b"", b"/", 1)]
    g.fam("list-scenario", "a listing read page by page: after = the cursor of the previous page's last entry; the "
                           "pages concatenate to the unpaginated listing")
    for name, prefix, delim, n in configs:
        ds = oracle.Dataset.load(name)
        full = ds.list(prefix=prefix, delimiter=delim, limit=BIG)
        scenario = f"list/{name}/{show(prefix) or 'root'}-{delim.hex()}-limit{n}"
        got, after, i = [], None, 0
        while True:
            req = rq_list(prefix=prefix, delimiter=delim, after=after, limit=n)
            exp = run(ds, req)
            g.add(f"{scenario}/page-{i:02d}", ["PAGE-1", "PAGE-2", "LIST-6", "QUERY-3"], name, exp, request=req,
                  meta={"scenario_id": scenario, "step": i})
            got += exp["entries"]
            if len(exp["entries"]) < n:
                break
            after = cursor_after(exp["entries"][-1])
            i += 1
        assert got == full, f"pagination of {name} does not concatenate to the full listing"
    g.fam("range-scenario", "a range read page by page with an exclusive start at the previous page's last key")
    for name, n in (("multipart", 25), ("binary-keys", 7), ("truncated-bounds-2", 4)):
        src = "binary-keys" if name.startswith("truncated") else name
        ds = oracle.Dataset.load(src)
        full = ds.range(limit=BIG)
        got, start, i = [], None, 0
        while True:
            req = rq_range(start=start, start_exclusive=True if start is not None else None, limit=n)
            exp = run(ds, req)
            g.add(f"range/{name}-limit{n}/page-{i:02d}", ["RANGE-1", "QUERY-3"], name, exp, request=req,
                  meta={"scenario_id": f"range/{name}-limit{n}", "step": i})
            got += exp["entries"]
            if len(exp["entries"]) < n:
                break
            start = dec(exp["entries"][-1]["key_base64"])
            i += 1
        assert got == full
    return g


def projection_group():
    g = Group("projection", "reader", "Projection (RANGE-2, COL-2, COL-3, COL-6): physical values of every "
                                      "mandatory type, null versus absent, request order, nested columns.")
    P = "projection"
    ds = oracle.Dataset.load(P)
    scalar = ["flag", "i32", "i64", "f32", "f64", "u32", "u64", "a.b", "name", "blob", "fx", "req", "extra"]
    g.fam("column-type", "each scalar column of the projection dataset over every row")
    for c in scalar:
        req = rq_range(columns=[c])
        reqs = ["RANGE-2", "COL-3"] + (["COL-2"] if c in ("f64", "req", "extra") else []) + \
               (["COL-6"] if c == "a.b" else [])
        g.add(f"column/{c}", reqs, P, run(ds, req), request=req)
    g.fam("column-combination", "several columns: request order, absent versus null, rows across parts")
    for cid, cols in (("all", scalar), ("reversed", list(reversed(scalar))), ("absent-mix", ["extra", "req", "f64"]),
                      ("repeat-key-order", ["fx", "flag"])):
        req = rq_range(columns=cols)
        g.add(f"columns/{cid}", ["RANGE-2", "COL-2", "COL-3"], P, run(ds, req), request=req)
    req = rq_range(start=b"row-05", end=b"row-13", columns=["f64", "req", "extra", "i32"])
    g.add("columns/span-parts", ["RANGE-2", "COL-2"], P, run(ds, req), request=req,
          desc="rows from all three parts: absent in some, null in others")
    req = rq_list(prefix=b"row-1", delimiter=b"-", columns=["i64", "f64"])
    g.add("list/columns", ["LIST-3", "RANGE-2", "COL-2"], P, run(ds, req), request=req,
          desc="the delimiter occurs only inside the prefix, so every candidate is a row")
    g.fam("absent-and-unknown", "columns that no part has (COL-7) and literal dotted names (COL-6)")
    for cid, cols in (("unknown", ["no_such_column"]), ("unknown-mixed", ["i32", "no_such_column", "req"])):
        req = rq_range(columns=cols)
        g.add(f"col-7/{cid}", ["COL-7", "RANGE-2"], P, run(ds, req), request=req,
              desc="a column no part has is absent in every row, not an error")
    req = rq_list(delimiter=b"-", columns=["no_such_column"])
    g.add("col-7/list", ["COL-7", "LIST-3"], P, run(ds, req), request=req)
    req = rq_range(columns=["a"])
    g.add("col-6/no-path-lookup", ["COL-6", "COL-7"], P, run(ds, req), request=req,
          desc="'a' is not a field; the column 'a.b' does not make it one")
    # The key column is projected like any other column: its physical BYTE_ARRAY bytes.
    key_cases = [
        ("basic", "basic", rq_range(columns=["key"]), "unannotated key alone"),
        ("basic", "basic", rq_range(start=b"b", limit=5, columns=["size", "key", "etag", "key"]),
         "key with other columns, requested twice"),
        ("basic", "basic", rq_list(prefix=b"a/", columns=["key", "size"]), "list rows with the key projected"),
        ("projection", P, rq_range(columns=["key", "i32"]), "key annotated STRING"),
        ("projection", P, rq_list(prefix=b"row-1", delimiter=b"-", columns=["key", "key"]), "list, key twice"),
        ("key-legacy-utf8", P, rq_range(start=b"row-10", limit=3, columns=["key"]), "key annotated UTF8 only"),
        ("binary-keys", "binary-keys", rq_range(columns=["v", "key"]), "keys that are not UTF-8"),
        ("non-utf8-keys", "non-utf8-keys", rq_range(columns=["key"]), None),
        ("multipart", "multipart", rq_list(prefix=b"logs/2026/02/", columns=["key"], limit=6), "rows from two parts"),
    ]
    g.fam("key-column", "projecting key returns its physical bytes")
    for i, (name, src, req, desc) in enumerate(key_cases):
        g.add(f"key/{name}-{i:02d}", ["RANGE-2", "KEY-1"] + (["LIST-3"] if req["op"] == "list" else []), name,
              run(oracle.Dataset.load(src), req), request=req, desc=desc)
    an = oracle.Dataset.load("annotated")
    acols = ["d", "ts_ms", "ts_utc", "dec9", "dec18", "dec30", "s"]
    g.fam("annotated", "physical values of annotated columns (DATE, TIMESTAMP, DECIMAL, STRING)")
    for c in acols:
        req = rq_range(columns=[c])
        g.add(f"annotated/{c}", ["RANGE-2", "COL-3"], "annotated", run(an, req), request=req,
              desc="the physical value, whatever the logical annotation")
    req = rq_range(columns=["key"])
    g.add("annotated/keys-only", ["RANGE-2"], "annotated", run(an, req), request=req,
          desc="unrequested annotated columns must not affect a keys-only request")
    req = rq_list(prefix=b"ann-", delimiter=b"/", columns=list(reversed(acols)))
    g.add("annotated/list-all", ["RANGE-2", "LIST-3"], "annotated", run(an, req), request=req)
    en = oracle.Dataset.load("encodings")
    g.fam("encodings", "values from PLAIN_DICTIONARY, DELTA_BINARY_PACKED and DELTA_LENGTH_BYTE_ARRAY pages")
    for c in ("i32", "i64", "s"):
        req = rq_range(columns=[c, "key"])
        g.add(f"encodings/{c}", ["RANGE-2", "ENC-1"], "encodings", run(en, req), request=req,
              desc="PLAIN_DICTIONARY pages in part 0; DELTA_BINARY_PACKED / DELTA_LENGTH_BYTE_ARRAY in part 1")
    oc = oracle.Dataset.load("optional-codecs")
    g.fam("optional-codec", "columns compressed with optional codecs: the values, or unsupported_encoding")
    req = rq_range(columns=["size"])
    g.add("optional-codecs/baseline-column", ["RANGE-2", "ENC-1"], "optional-codecs",
          dict(run(oc, req), accept_unsupported=["unsupported_encoding"]), request=req,
          desc="only baseline codecs are decoded, but a reader that checks codecs eagerly may answer unsupported")
    for c in ("lz4col", "brcol"):
        req = rq_range(columns=[c])
        g.add(f"optional-codecs/{c}", ["RANGE-2", "ENC-1"], "optional-codecs",
              dict(run(oc, req), accept_unsupported=["unsupported_encoding"]), request=req,
              desc="a codec outside the baseline: the values, or unsupported (never wrong values or a refusal)")
    g.fam("nested", "nested or repeated columns: unsupported_projection. COL-3 permits supporting them, but the "
                    "runner protocol has no encoding for nested values, so the suite can only accept the rejection; "
                    "it must never be answered as absent")
    for c in ("meta", "tags"):
        req = rq_range(columns=["i32", c])
        g.add(f"nested/{c}", ["COL-3"], P, run(ds, req), request=req,
              desc="projecting a nested or repeated column is rejected (the protocol cannot express its values), "
                   "never answered as absent")
    req = rq_list(prefix=b"row-0", delimiter=b"/", columns=["tags"])
    g.add("nested/list-tags", ["COL-3"], P, run(ds, req), request=req)
    lu = oracle.Dataset.load(P)
    g.fam("other-datasets", "projection on other datasets")
    req = rq_range(columns=scalar)
    g.add("key-legacy-utf8/all", ["RANGE-2", "KEY-1"], "key-legacy-utf8", run(lu, req), request=req)
    m = oracle.Dataset.load("multipart")
    req = rq_range(start=b"logs/2026/01/f025", limit=30, columns=["mtime", "storage_class", "size"])
    g.add("multipart/span", ["RANGE-2", "ENC-1"], "multipart", run(m, req), request=req,
          desc="values across codecs, dictionary pages and data page v2, crossing a part boundary")
    b = oracle.Dataset.load("basic")
    req = rq_range(start=b"bravo", limit=4, columns=["etag", "size", "etag"])
    g.add("basic/repeated-column", ["RANGE-2"], "basic", run(b, req), request=req,
          desc="a column requested twice is reported twice, in request order")
    return g


def requests_group():
    g = Group("requests", "reader", "Malformed requests are invalid requests, not empty results (QUERY-1). "
                                    "raw_request is sent as is, plus id and dataset.")
    bad = [
        ("range-limit-0", {"op": "range", "limit": 0}),
        ("range-limit-negative", {"op": "range", "limit": -1}),
        ("range-limit-fraction", {"op": "range", "limit": 1.5}),
        ("range-limit-string", {"op": "range", "limit": "10"}),
        ("range-limit-missing", {"op": "range"}),
        ("range-limit-boolean", {"op": "range", "limit": True}),
        ("range-start-malformed-base64", {"op": "range", "start_base64": "YQ", "limit": 10}),
        ("range-start-bad-alphabet", {"op": "range", "start_base64": "Y!==", "limit": 10}),
        ("range-end-noncanonical-base64", {"op": "range", "end_base64": "YR==", "limit": 10}),
        ("range-prefix-malformed-base64", {"op": "range", "prefix_base64": "YS8", "limit": 10}),
        ("list-limit-0", {"op": "list", "delimiter_base64": "Lw==", "limit": 0}),
        ("list-limit-fraction", {"op": "list", "delimiter_base64": "Lw==", "limit": 2.5}),
        ("list-delimiter-two-bytes", {"op": "list", "delimiter_base64": "Ly8=", "limit": 10}),
        ("list-delimiter-empty", {"op": "list", "delimiter_base64": "", "limit": 10}),
        ("list-delimiter-missing", {"op": "list", "limit": 10}),
        ("list-prefix-malformed-base64", {"op": "list", "prefix_base64": "YS8", "delimiter_base64": "Lw==", "limit": 10}),
        ("list-after-malformed-base64", {"op": "list", "after": {"key_base64": "@@@@", "offset": 0},
                                         "delimiter_base64": "Lw==", "limit": 10}),
    ]
    g.fam("malformed", "a malformed request is invalid_request")
    for cid, raw in bad:
        g.add(f"basic/{cid}", ["QUERY-1"], "basic", {"status": "invalid_request"}, raw_request=raw)
    g.add("empty/range-limit-0", ["QUERY-1"], "empty", {"status": "invalid_request"},
          raw_request={"op": "range", "limit": 0}, desc="invalid even when the dataset is empty")
    return g


# Refusals and validation ----------------------------------------------------

LEVELS = ["structural", "profile", "full"]
# Prerequisite matrix (VAL-1): six structural faults with distinct entry paths requested at profile and full,
# and three profile faults requested at full. Every other fault is tested only at its lowest level.
STRUCTURAL_SENTINELS = ["manifest-unknown-member", "missing-part", "invalid-footer-magic", "stamp-wrong-schema-id",
                        "inconsistent-column-type", "missing-column-index"]
PROFILE_SENTINELS = ["key-order-across-pages", "bounds-last-key-differs", "page-index-min-above-first-key"]
# Six distinct digest behaviours (VAL-2): (dataset, expected digest level, description).
DIGEST_CASES = [
    ("empty", {"result": "complete", "checked": 0}, "the empty dataset: nothing to check, complete"),
    ("basic", {"result": "complete", "checked": 0}, "a non-empty dataset that declares no sha256: complete, 0 checked"),
    ("multipart", {"result": "complete", "checked": 3}, "every part declares a matching sha256: 3 checked"),
    ("digest-mixed", {"result": "complete", "checked": 2}, "two of three parts declare sha256: exactly 2 checked"),
    ("invalid/digest-mismatch", {"result": "failed", "code": "digest_mismatch"},
     "a declared sha256 does not match the part's bytes"),
    ("invalid/digest-on-profile-invalid", {"result": "complete", "checked": 1},
     "the dataset has a profile fault (key_order) but its declared digest matches: digest validation is "
     "independent of the other levels and completes"),
]


def load_faults():
    doc = json.loads((HERE / "faults.json").read_text(encoding="utf-8"))
    return {f["id"]: f for f in doc["faults"]}


def accepted(f):
    return f["codes"] + [x["code"] for x in f.get("secondary_faults", [])]


def expect_refusal(codes):
    return {"status": "refused", "code": codes[0]} if len(codes) == 1 else {"status": "refused", "codes": codes}


def failed_level(codes):
    return {"result": "failed", **({"code": codes[0]} if len(codes) == 1 else {"codes": codes})}


def refusal_groups(lock):
    faults = load_faults()
    g_open = Group("refusals-open", "reader", "Faults in the manifest, which a reader must detect when opening the "
                                              "dataset (reader_detect open in generators/faults.json), and faults in a "
                                              "part, which it must detect no later than the first read that routes to "
                                              "the part, or on verify (reader_detect touch). Each touch request is an "
                                              "unbounded range whose limit covers every row, so it routes to every part.")
    g_read = Group("refusals-read", "reader", "Faults a reader must detect no later than decoding the faulty "
                                              "data. Each request is an unbounded range whose limit covers every "
                                              "row (projecting the faulty column where the fault is in values), so "
                                              "it decodes the faulty page under any correct access path.")
    g_open.fam("fault", "one case per fault that a reader must refuse on open, or when a read first touches its part")
    g_read.fam("fault", "one case per fault that a reader must refuse no later than the read that decodes it")
    gv = {lvl: Group(f"validate-{lvl}", f"validator-{lvl}",
                     f"Validation at the {lvl} level (section 8). validate always answers status ok with levels; "
                     "an invalid dataset shows the level failed with the refusal code, and a dataset that cannot "
                     "be opened shows structural failed.")
          for lvl in ("structural", "profile", "full", "digest")}
    for lvl in LEVELS:
        gv[lvl].fam("valid-dataset", "every valid dataset completes this level and its prerequisites")
    for d in lock["datasets"]:
        if not d["valid"]:
            continue
        label = {v: k for k, v in RENAMED.items()}.get(d["name"], d["name"])
        for lvl in LEVELS:
            levels = {x: {"result": "complete"} for x in LEVELS[: LEVELS.index(lvl) + 1]}
            reqs, desc = ["VAL-1"], None
            if lvl == "full" and d["name"] in OPTIONAL:
                levels["full"] = {"result": "complete", "accept": ["incomplete"]}
                reqs.append("ENC-1")
                desc = ("value columns use LZ4_RAW and BROTLI (outside ENC-1): full is complete, or incomplete with a "
                        "reason for a validator without those codecs; structural and profile must complete")
            if lvl == "full" and d["name"] in NESTED_COLUMNS:
                levels["full"] = {"result": "complete", "accept": ["incomplete"]}
                desc = (f"the dataset has nested/repeated columns ({NESTED_COLUMNS[d['name']]}); a validator that "
                        "does not decode them reports full incomplete with a reason (VAL-1); lower levels complete")
            gv[lvl].add(label, reqs, d["dir"], {"status": "ok", "levels": levels},
                        request={"op": "validate", "level": lvl}, requires=[f"validate:{lvl}"], desc=desc)
    for lvl in LEVELS:
        gv[lvl].fam("fault-at-lowest-level", "each fault at the lowest validation level that detects it "
                                             "(validation_level in generators/faults.json)")
    for fid, f in faults.items():
        codes = accepted(f)
        ddir = f["dataset"]
        desc = next(d["description"] for d in lock["datasets"] if d["dir"] == ddir)
        if f.get("secondary_faults"):
            desc += " Secondary fault: " + "; ".join(f"{x['code']}: {x['justification']}" for x in f["secondary_faults"])
        exp = expect_refusal(codes)
        meta = {"fault_id": fid}
        if f["reader_detect"] == "open":
            g_open.add(fid, f["requirements"], ddir, exp, request={"op": "open"}, detect="open", desc=desc, meta=meta)
        elif f["reader_detect"] == "touch":
            g_open.add(fid, f["requirements"], ddir, exp, request=rq_range(limit=BIG), detect="touch", desc=desc, meta=meta)
        elif f["reader_detect"] == "read":
            req = rq_range(limit=BIG, columns=f.get("read_columns"))
            g_read.add(fid, f["requirements"], ddir, exp, request=req, detect="read", desc=desc, meta=meta)
        lvl = f["validation_level"]
        if lvl == "digest":
            continue  # covered by the digest behaviours below
        levels = {x: {"result": "complete"} for x in LEVELS[: LEVELS.index(lvl)]}
        levels[lvl] = failed_level(codes)
        gv[lvl].add(fid, f["requirements"] + ["VAL-1"], ddir, {"status": "ok", "levels": levels},
                    request={"op": "validate", "level": lvl}, requires=[f"validate:{lvl}"], desc=desc, meta=meta)
    for lvl in ("profile", "full"):
        gv[lvl].fam("prerequisite", "a lower-level fault with validation requested at a higher level: the failing "
                                    "level is reported failed and every dependent level incomplete with a reason")
    for fid in STRUCTURAL_SENTINELS + PROFILE_SENTINELS:
        f = faults[fid]
        low = f["validation_level"]
        for higher in LEVELS[LEVELS.index(low) + 1:]:
            if fid in PROFILE_SENTINELS and higher != "full":
                continue
            levels = {x: {"result": "complete"} for x in LEVELS[: LEVELS.index(low)]}
            levels[low] = failed_level(accepted(f))
            for x in LEVELS[LEVELS.index(low) + 1: LEVELS.index(higher) + 1]:
                levels[x] = {"result": "incomplete"}
            gv[higher].add(f"prerequisite/{fid}", f["requirements"] + ["VAL-1"], f["dataset"],
                           {"status": "ok", "levels": levels}, request={"op": "validate", "level": higher},
                           requires=[f"validate:{higher}"], meta={"fault_id": fid},
                           desc=f"a {low} fault ({fid}) with validation requested at {higher}")
    gv["digest"].fam("digest", "six distinct digest behaviours (VAL-2, PART-5)")
    for name, level, desc in DIGEST_CASES:
        meta = {"fault_id": name.split("/", 1)[1]} if name.startswith("invalid/") else None
        gv["digest"].add(name.split("/")[-1], ["VAL-2", "PART-5"], f"datasets/{name}",
                         {"status": "ok", "levels": {"digest": level}}, request={"op": "validate", "level": "digest"},
                         requires=["validate:digest"], desc=desc, meta=meta)
    return [g_open, g_read] + [gv[x] for x in ("structural", "profile", "full", "digest")]


def repeated_group():
    g = Group("repeated", "reader", "Repeated groups, exact key column names, whole-group bounds and tagged continuation.")
    names = ("repeated-keys", "one-large-group", "named-key", "basic")
    g.fam("boundaries", "Inclusive and exclusive seeks at every group key, plus exact-group lookups and prefix reads")
    g.fam("pagination", "Every page size 1, 2, 5, 7; row cursors resume inside groups, prefix cursors skip rollups")
    for name in names:
        ds = oracle.Dataset.load(name)
        g.family = "repeated/boundaries"
        for i, key in enumerate(dict.fromkeys(ds.keys())):
            for exclusive in (False, True):
                req = rq_range(start=key, start_exclusive=exclusive, limit=BIG, columns=[ds.key_column, "size"])
                g.add(f"{name}/seek-{i}-{int(exclusive)}", ["MAN-8", "KEY-1", "KEY-3", "KEY-7", "ROUTE-1", "RANGE-1"], name, run(ds, req), request=req)
            for label, req in (("lookup", rq_range(start=key, end=key+b"\x00", columns=["size"])),
                               ("prefix", rq_range(prefix=key, columns=["size"]))):
                g.add(f"{name}/{label}-{i}", ["RANGE-1", "KEY-7", "ROUTE-1"], name, run(ds, req), request=req)
        g.family = "repeated/pagination"
        for op in ("range", "list"):
            for size in (1, 2, 5, 7):
                req = rq_range(limit=size, columns=["size"]) if op == "range" else rq_list(limit=size, columns=["size"])
                step = 0
                while True:
                    exp = run(ds, req)
                    g.add(f"{name}/{op}-{size}/{step}", ["PAGE-1", "PAGE-2", "LIST-6", "KEY-7"], name, exp, request=dict(req),
                          meta={"scenario_id": f"{name}/{op}-{size}", "step": step})
                    if len(exp["entries"]) < size:
                        break
                    req["after"] = cursor_after(exp["entries"][-1])
                    step += 1
    g.fam("cursor-every-row", "A row cursor at every row of the repeated layouts, limit 6: resumption at every page, "
                              "row-group, group and part boundary, with group offsets counted across row groups")
    for name in ("repeated-keys", "one-large-group"):
        ds = oracle.Dataset.load(name)
        for i, (key, offset, _v, _p) in enumerate(ds.positioned()):
            req = rq_range(limit=6, columns=["size"])
            req["after"] = row_cursor(key, offset)
            g.add(f"{name}/cursor-row-{i:02d}", ["PAGE-1", "KEY-7", "ROUTE-1"], name, run(ds, req), request=req)
    g.fam("invalid-cursors", "Malformed tags, offsets and base64 are rejected")
    for i, cursor in enumerate(({}, {"key_base64": "YQ=="}, {"key_base64": "YQ==", "offset": -1},
                                {"key_base64": "YQ==", "offset": True}, {"key_base64": "YQ==", "offset": 1.5},
                                {"key_base64": "@@@@", "offset": 0}, {"prefix_base64": "YQ==", "offset": 0})):
        g.add(f"invalid-cursor-{i}", ["QUERY-1", "PAGE-1"], "basic", {"status": "invalid_request"},
              raw_request={"op": "range", "limit": 2, "after": cursor})
    return g


# Assembly --------------------------------------------------------------------

def behaviour(c):
    """What a case asserts, independent of its id and documentation."""
    return json.dumps([c["dataset"], c.get("request"), c.get("raw_request"), c["expect"], c.get("detect")],
                      sort_keys=True)


def load_core():
    core = {}
    files = []
    for path in sorted((CONF / "cases" / "core").glob("*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        assert doc.get("generated") is False, f"{path}: core files must say generated: false"
        files.append((path.relative_to(CONF).as_posix(), doc))
        for c in doc["cases"]:
            assert c["id"] not in core, f"duplicate core id {c['id']}"
            core[c["id"]] = (doc, c)
    return core, files


def check_core_case(c, candidate):
    """Core expectations are authoritative: check them, never rewrite them."""
    if candidate is not None:
        if behaviour(c) != behaviour(candidate):
            raise SystemExit(f"core case {c['id']} disagrees with the generator/oracle; review the core case "
                             f"(it is not rewritten)")
        return
    req = c.get("request") or c.get("raw_request") or {}
    rows = c["dataset"].split("/", 1)[1]
    if req.get("op") in ("range", "list") and (HERE / "rows" / f"{rows}.json").exists():
        if run(oracle.Dataset.load(rows), req) != {k: v for k, v in c["expect"].items() if k != "accept_unsupported"}:
            raise SystemExit(f"core case {c['id']} disagrees with the oracle")
        return
    raise SystemExit(f"core case {c['id']} has no generated counterpart and cannot be checked by the oracle")


def build_all(lock):
    groups = [repeated_group(), open_group(lock), range_group(), prefix_group(), list_group(), pagination_group(),
              projection_group(), requests_group()] + refusal_groups(lock)
    core, core_files = load_core()
    cand = {c["id"]: c for g in groups for c in g.cases}
    for cid, (_doc, c) in core.items():
        check_core_case(c, cand.get(cid))
    # Exact duplicates (same dataset, request and expectation): keep the first (core first), retire the rest.
    seen, retired = {}, []
    for cid, (_doc, c) in core.items():
        seen.setdefault(behaviour(c), cid)
    # The "start just above a key" probes are retired in favour of an identical exact-hit or boundary probe.
    ordered = [c for g in groups for c in sorted(g.cases, key=lambda c: c["family"] == "range/start-just-above-key")]
    for c in ordered:
        if c["id"] in core:
            continue
        first = seen.setdefault(behaviour(c), c["id"])
        if first != c["id"]:
            retired.append({"id": c["id"], "duplicate_of": first})
    for cid, reason in REMOVED.items():
        assert cid not in cand and cid not in core, f"{cid} is retired"
        retired.append({"id": cid, "reason": reason})
    retired_ids = {r["id"] for r in retired}
    generated = {g.name: [c for c in g.cases if c["id"] not in core and c["id"] not in retired_ids] for g in groups}
    return groups, core, core_files, generated, retired


def main():
    lock = json.loads((CONF / "fixtures.lock.json").read_text(encoding="utf-8"))
    groups, core, core_files, generated, retired = build_all(lock)
    for old in (CONF / "cases").glob("*.json"):
        old.unlink()  # superseded flat layout
    gen_files = []
    for g in groups:
        path = f"cases/generated/{g.name}.generated.json"
        if generated[g.name]:
            write_text(CONF / path, dumps(g.doc(generated[g.name])))
            gen_files.append(path)
        elif (CONF / path).exists():
            (CONF / path).unlink()
    case_files = [p for p, _d in core_files] + gen_files
    counts = {}
    for p, d in core_files:
        counts[p] = len(d["cases"])
    for p in gen_files:
        counts[p] = len(generated[p.split("/")[-1].split(".")[0]])
    suite = {
        "suite": "seekparquet-conformance-semantic",
        "revision": SUITE_REVISION,
        "status": "draft",
        "specification": "spec/format-1.md",
        "format_version": 0,
        "draft_revision": 1,
        "mandatory_targets": ["reader"],
        "mandatory_capabilities": ["open", "range", "list", "projection"],
        "case_files": case_files,
        "subsets": ["subsets/smoke.json"],
        "case_counts": counts,
        "totals": {"core": sum(len(d["cases"]) for _p, d in core_files),
                   "generated": sum(len(v) for v in generated.values())},
        "renamed_datasets": {f"datasets/{k}": f"datasets/{v}" for k, v in RENAMED.items()},
        "retired_case_ids": retired,
    }
    suite["content_sha256"] = suite_digest(case_files + suite["subsets"])
    write_text(CONF / "suite.json", dumps(suite))
    write_coverage(core, generated, groups)
    print(f"{suite['totals']['core']} core + {suite['totals']['generated']} generated cases; "
          f"{len(retired)} retired ids")


DIGEST_FILES = ["protocol.md", "case.schema.json", "capabilities.json", "fixtures.lock.json", "generators/faults.json"]


def suite_digest(files):
    """SHA-256 over the suite's contract files, case files and subsets (path, NUL, bytes, NUL; sorted by path).

    harness/check_suite.py recomputes it, so spec/status.json can pin a suite revision by revision and digest.
    """
    import hashlib
    h = hashlib.sha256()
    for path in sorted(DIGEST_FILES + list(files)):
        h.update(path.encode() + b"\0" + (CONF / path).read_bytes() + b"\0")
    return h.hexdigest()


UNTESTABLE = {
    "COMMIT-1": "publication property (atomic, durable publication): not testable from stored bytes",
    "COMMIT-2": "publication property (parts immutable while referenced): not testable from stored bytes",
    "COMMIT-3": "scope statement (one snapshot per manifest): not testable",
    "VER-2": "process rule for the specification itself: not testable",
    "VAL-3": "statement of what validation cannot show: not testable",
    "EXT-2": "API recommendation (expose extensions unchanged): outside the runner protocol",
    "GEO-1": "writer recommendation affecting speed, not validity; reader acceptance of any page size is "
             "exercised by every fixture with a different page size",
    "GEO-2": "writer's choice: not a reader obligation",
    "MAN-5": "reader configuration (accepting a draft only when configured): adapters declare supported versions "
             "in hello; the unsupported-version cases cover refusal",
    "LAYOUT-2": "paths are relative to the root: exercised implicitly by every dataset",
    "COL-1": "producer freedom: no reader obligation",
}
GAPS = {
    "FILE-2": "gap: no encrypted fixture yet",
    "CHECK-3": "gap: permission for earlier checks; the harness accepts early refusals but no case targets it",
    "CHECK-4": "gap: needs the planned indexed-access suite (a reader must not certify rows it skipped)",
    "RG-3": "partial: no fixture whose OffsetIndex omits a data page while every listed entry is consistent",
    "KEY-5": "partial: a column_orders entry that is present but not TYPE_ORDER is not tested",
    "STAMP-2": "partial: a draft_revision stamp on a format_version 1 file needs a reader that supports version 1",
    "COL-3": "partial: nested projection can only be tested as rejected (no protocol encoding)",
}


def write_coverage(core, generated, groups):
    import re
    spec = (CONF.parent / "spec" / "format-1.md").read_text(encoding="utf-8")
    ids = list(dict.fromkeys(re.findall(r"\[([A-Z]+-[0-9]+)\]", spec)))
    by_req = {r: {"core": [], "families": {}, "pos": 0, "neg": 0} for r in ids}

    def negative(c):
        e = c["expect"]
        return e["status"] != "ok" or any(v.get("result") == "failed" for v in e.get("levels", {}).values())

    allcases = [c for _d, c in core.values()] + [c for v in generated.values() for c in v]
    for c in allcases:
        for r in c["requirements"]:
            if r not in by_req:
                continue
            b = by_req[r]
            if c["id"] in core:
                b["core"].append(c["id"])
            else:
                b["families"][c["family"]] = b["families"].get(c["family"], 0) + 1
            b["neg" if negative(c) else "pos"] += 1
    L = ["# Requirement coverage", "",
         "Generated by `generators/make_cases.py`. For each requirement in `spec/format-1.md`: representative",
         "core cases (hand-reviewed, `cases/core/`), the generated families that exercise it (with case counts),",
         "the number of positive cases (valid data, results expected) and negative cases (refusals, invalid",
         "requests, unsupported features or failed validation levels), and known gaps. A citation count is not",
         "an argument for completeness: the core descriptions and family descriptions say what each case observes.",
         ""]
    for r in ids:
        b = by_req[r]
        L.append(f"## {r}")
        L.append("")
        if not b["core"] and not b["families"]:
            L.append(f"No case. {UNTESTABLE.get(r) or GAPS.get(r) or 'gap: not yet covered'}.")
            L.append("")
            continue
        L.append(f"- Evidence: {b['pos']} positive, {b['neg']} negative.")
        if b["core"]:
            shown = b["core"][:5]
            L.append("- Core: " + ", ".join(f"`{x}`" for x in shown) + (f" and {len(b['core']) - 5} more"
                                                                          if len(b["core"]) > 5 else ""))
        if b["families"]:
            L.append("- Generated: " + ", ".join(f"`{f}` ({n})" for f, n in sorted(b["families"].items())))
        note = UNTESTABLE.get(r) or GAPS.get(r)
        if note:
            L.append(f"- Note: {note}.")
        L.append("")
    L += ["## Known gaps", ""] + [f"- {r}: {g}" for r, g in GAPS.items()] + [
        "- Adaptive pagination driven by an adapter's own responses (pagination cases use fixed requests).",
        "- Validators that let the indexes under test decide what to visit (needs dedicated fixtures).",
        "- A second independent writer's fixtures.", ""]
    write_text(CONF / "COVERAGE.md", "\n".join(L).rstrip("\n") + "\n")


if __name__ == "__main__":
    main()
