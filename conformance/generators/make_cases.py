#!/usr/bin/env python3
"""Generate conformance/cases/*.json and suite.json, deterministically.

Expected entries come only from ``oracle.py`` applied to the source rows in
``generators/rows/``. Expected refusals come from the fault recorded for each
fixture in ``fixtures.lock.json``. No reader output is ever used.
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

SUITE_REVISION = "0.1.0-draft.1"
BIG = 100000  # a limit at least the number of rows of every fixture (QUERY-3)


class Group:
    def __init__(self, name, target, description):
        self.name, self.target, self.description = name, target, description
        self.cases = []
        self.ids = set()

    def add(self, cid, reqs, dataset, expect, request=None, raw_request=None, desc=None, requires=None, detect=None):
        cid = f"{self.name}/{cid}"
        assert cid not in self.ids, cid
        self.ids.add(cid)
        c = {"id": cid}
        if desc:
            c["description"] = desc
        c["requirements"] = sorted(set(reqs), key=reqs.index)
        if requires:
            c["requires"] = requires
        c["dataset"] = f"datasets/{dataset}" if not dataset.startswith("datasets/") else dataset
        if detect:
            c["detect"] = detect
        if request is not None:
            c["request"] = request
        if raw_request is not None:
            c["raw_request"] = raw_request
        c["expect"] = expect
        self.cases.append(c)

    def doc(self):
        return {"group": self.name, "target": self.target, "description": self.description, "cases": self.cases}


def B(b):
    return None if b is None else b64e(b)


def rq_range(start=None, start_exclusive=None, end=None, limit=BIG, columns=None, null_bounds=False):
    r = {"op": "range"}
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


def rq_list(prefix=b"", delimiter=b"/", after=None, continuation=None, end=None, limit=BIG, columns=None):
    r = {"op": "list"}
    if prefix:
        r["prefix_base64"] = B(prefix)
    r["delimiter_base64"] = B(delimiter)
    if after is not None:
        r["after_base64"] = B(after)
    if continuation is not None:
        r["continuation"] = continuation
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
            e = ds.range(start=dec(args.get("start_base64")), start_exclusive=args.get("start_exclusive", False),
                         end=dec(args.get("end_base64")), limit=args["limit"], columns=cols)
        else:
            e = ds.list(prefix=dec(args.get("prefix_base64")) or b"", delimiter=dec(args["delimiter_base64"]),
                        after=dec(args.get("after_base64")), continuation=args.get("continuation", False),
                        end=dec(args.get("end_base64")), limit=args["limit"], columns=cols)
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
    for d in lock["datasets"]:
        if d["valid"]:
            reqs = ["CHECK-1", "LAYOUT-1"]
            if d["name"] == "multipart":
                reqs += ["LAYOUT-3", "EXT-1", "ENC-1", "PART-5"]
            if d["name"] == "empty":
                reqs += ["MAN-3"]
            if d["name"] in ("projection", "key-legacy-utf8"):
                reqs += ["KEY-1", "KEY-6"]
            g.add(d["name"], reqs, d["dir"], {"status": "ok"}, request={"op": "open"},
                  desc=d["description"])
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
    # basic: bounds
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
    add("multipart/all", R + ["RANGE-2", "LAYOUT-1", "LAYOUT-3", "ENC-1"], "multipart", "multipart",
        rq_range(columns=["size", "storage_class", "mtime"]),
        "every row; the stray file under the root and empty row groups contribute nothing")
    add("multipart/stray-not-read", R + ["LAYOUT-1"], "multipart", "multipart",
        rq_range(start=b"logs/2026/02/s", end=b"logs/2026/03/"),
        "the unlisted file shards/stray.parquet holds keys in this range; they must not appear")

    # binary keys and conservative bounds
    for name in ("binary-keys", "truncated-bounds", "truncated-bounds-2"):
        ds = oracle.Dataset.load("binary-keys")
        reqs = R + ["KEY-3", "KEY-4"] + (["RG-5", "RG-6"] if name != "binary-keys" else [])
        add(f"{name}/all", reqs, name, "binary-keys", rq_range(columns=["v"]))
        for i, k in enumerate(ds.keys()):
            add(f"{name}/seek-{i:02d}", reqs, name, "binary-keys", rq_range(start=k, limit=1),
                f"exact hit at {show(k)}")
            if name != "truncated-bounds":
                add(f"{name}/seek-miss-{i:02d}", reqs, name, "binary-keys", rq_range(start=k + b"\x00", limit=1),
                    f"exact miss just above {show(k)}")
            if name == "truncated-bounds-2":
                add(f"{name}/end-{i:02d}", reqs, name, "binary-keys", rq_range(start=k[:1], end=k, limit=3),
                    f"end exactly at {show(k)}")
    for name, src in (("non-monotone-bounds", "basic"), ("no-value-page-index", "basic"),
                      ("non-utf8-keys", "non-utf8-keys")):
        ds = oracle.Dataset.load(src)
        reqs = R + {"non-monotone-bounds": ["RG-5", "RG-6"], "no-value-page-index": ["COL-4", "RANGE-2"],
                    "non-utf8-keys": ["KEY-6"]}[name]
        cols = ["size"] + (["etag"] if src == "basic" else [])
        add(f"{name}/all", reqs, name, src, rq_range(columns=cols))
        for i, k in enumerate(ds.keys()):
            add(f"{name}/seek-{i:02d}", reqs, name, src, rq_range(start=k, limit=2, columns=cols))
    return g


def prefix_group():
    g = Group("prefix", "reader", "Prefix reads: the range from p to successor(p), or unbounded above when p "
                                  "has no successor (section 6.2). The protocol has no prefix operation, so "
                                  "these are range requests whose end is computed by the suite.")
    cases = [
        ("multipart", [b"logs/2026/02/", b"logs/", b"logs/2026/", b"app/v1/obj-00", b"tmp/", b"zz", b"nothing"]),
        ("binary-keys", [b"a", b"a\x00", b"\x00", b"\x7f", b"\xc3", b"p", b"pq", b"L" * 30, b"\xff", b"\xff\xff",
                         b"\xff" * 80, b"\xfe\xff"]),
        ("truncated-bounds-2", [b"\xff", b"\xff\xff", b"L" * 30, b"M" * 70 + b"\xff" * 10, b"a"]),
        ("delimiter", [b"a/b/", b"a/", b"caf\xc3\xa9", b"x\xff", b"\xff", b"\xff\xff"]),
    ]
    for name, prefixes in cases:
        src = "binary-keys" if name.startswith("truncated") else name
        ds = oracle.Dataset.load(src)
        for i, p in enumerate(prefixes):
            s = oracle.successor(p)
            req = rq_range(start=p, end=s)
            reqs = ["RANGE-1", "KEY-3"]
            g.add(f"{name}/{i:02d}", reqs, name, run(ds, req), request=req,
                  desc=f"prefix {show(p)}" + ("" if s is not None else " (no successor: unbounded above)"))
    return g


def list_group():
    g = Group("list", "reader", "Delimited listing (LIST-1 to LIST-7). Expected entries come from oracle.py.")

    def add(cid, reqs, name, req, desc=None, src=None):
        ds = oracle.Dataset.load(src or name)
        g.add(cid, reqs, name, run(ds, req), request=req, desc=desc)

    L = ["LIST-2", "LIST-3", "LIST-4"]
    D = "delimiter"
    add("delimiter/root", L, D, rq_list(), "empty prefix; keys beginning with the delimiter roll up to '/'")
    add("delimiter/a", L, D, rq_list(prefix=b"a"), "prefix without a trailing delimiter; key a is a row")
    add("delimiter/a-slash", L, D, rq_list(prefix=b"a/"), "key equal to the prefix is a row; a// and a/b/ roll up")
    add("delimiter/a-slash-slash", L, D, rq_list(prefix=b"a//"), "repeated delimiters")
    add("delimiter/a-b", L, D, rq_list(prefix=b"a/b/"), "a/b/ spans parts 0 and 1")
    add("delimiter/a-b-c", L, D, rq_list(prefix=b"a/b/c"))
    add("delimiter/slash", L, D, rq_list(prefix=b"/"), "delimiter at the start of keys")
    add("delimiter/slash-slash", L, D, rq_list(prefix=b"//"))
    add("delimiter/no-match", L, D, rq_list(prefix=b"q"), "no key has the prefix")
    add("delimiter/ff-delim-root", L + ["LIST-1"], D, rq_list(delimiter=b"\xff"), "non-ASCII delimiter 0xFF")
    add("delimiter/ff-delim-x", L + ["LIST-1"], D, rq_list(prefix=b"x", delimiter=b"\xff"))
    add("delimiter/ff-delim-xff", L + ["LIST-1"], D, rq_list(prefix=b"x\xff", delimiter=b"\xff"),
        "x\\xff\\xff rolls up to itself")
    add("delimiter/utf8-lead-byte", L + ["LIST-1"], D, rq_list(prefix=b"caf", delimiter=b"\xc3"),
        "delimiter equal to a UTF-8 lead byte")
    add("delimiter/utf8-cont-byte", L + ["LIST-1"], D, rq_list(prefix=b"caf\xc3", delimiter=b"\xa9"),
        "delimiter equal to a UTF-8 continuation byte")
    add("delimiter/prefix-ff", L, D, rq_list(prefix=b"\xff"), "prefix 0xFF has no successor")
    add("delimiter/prefix-ff-ff", L, D, rq_list(prefix=b"\xff\xff"))
    add("delimiter/delim-a", L, D, rq_list(delimiter=b"a"), "a letter as delimiter")
    add("delimiter/limit-1", L, D, rq_list(prefix=b"a/", limit=1))
    add("delimiter/limit-2", L, D, rq_list(limit=2))
    add("delimiter/columns", L + ["RANGE-2"], D, rq_list(prefix=b"a/", columns=["size"]))
    add("delimiter/columns-root", L + ["RANGE-2"], D, rq_list(columns=["size"]))
    E = L + ["LIST-7"]
    add("delimiter/end-inside-prefix", E, D, rq_list(end=b"a/b"), "end inside the range of a/")
    add("delimiter/end-at-prefix", E, D, rq_list(prefix=b"a/", end=b"a/b/"),
        "end equal to a common prefix: the key a/b/ is not a candidate")
    add("delimiter/end-inside-a-b", E, D, rq_list(prefix=b"a/", end=b"a/b/c/"))
    add("delimiter/end-empty", E + ["QUERY-2"], D, rq_list(end=b""), "an empty end: nothing")
    add("delimiter/end-below-prefix", E, D, rq_list(prefix=b"b", end=b"a"))
    add("delimiter/after-row", E + ["LIST-5"], D, rq_list(prefix=b"a/", after=b"a/"))
    add("delimiter/after-inside-prefix", E + ["LIST-5"], D, rq_list(prefix=b"a/", after=b"a/b/c"),
        "continuation false: a/b/ is returned because a/b/c/ and later keys roll up to it")
    add("delimiter/after-inside-prefix-continuation", E + ["LIST-5", "LIST-6"], D,
        rq_list(prefix=b"a/", after=b"a/b/c", continuation=True), "continuation true: a/b/ is not returned")
    add("delimiter/after-equals-prefix-continuation", E + ["LIST-5", "LIST-6"], D,
        rq_list(prefix=b"a/", after=b"a/b/", continuation=True))
    add("delimiter/after-past-prefix", E + ["LIST-5"], D, rq_list(after=b"a/zz"))
    add("delimiter/after-empty", E + ["QUERY-2"], D, rq_list(after=b""), "every key is greater than the empty after")
    add("delimiter/continuation-without-after", L + ["LIST-5"], D, rq_list(prefix=b"a/", continuation=True),
        "continuation has no effect without after")
    add("delimiter/after-and-end", E + ["LIST-5"], D, rq_list(after=b"a//", end=b"a/c"))
    LB = "list-bounds"
    add("list-bounds/worked-example", E, LB, rq_list(end=b"a/b"), "LIST-7: a/a is a candidate, so a/ appears")
    add("list-bounds/worked-example-without-a-a", E, LB, rq_list(after=b"a/a", end=b"a/b"),
        "LIST-7: with a/a excluded by after, a/z alone (beyond end) does not make a/ appear")
    add("list-bounds/after-a-a", E + ["LIST-5"], LB, rq_list(after=b"a/a"),
        "continuation false: a/ is returned because a/z rolls up to it")
    add("list-bounds/after-a-a-continuation", E + ["LIST-5"], LB, rq_list(after=b"a/a", continuation=True))
    add("list-bounds/end-c", E, LB, rq_list(end=b"c"))
    M = "multipart"
    add("multipart/root", L, M, rq_list())
    add("multipart/logs", L, M, rq_list(prefix=b"logs/"))
    add("multipart/logs-2026", L, M, rq_list(prefix=b"logs/2026/"), "logs/2026/02/ spans parts 0 and 1")
    add("multipart/logs-2026-02", L + ["RANGE-2"], M, rq_list(prefix=b"logs/2026/02/", columns=["size", "mtime"]),
        "rows from two parts")
    add("multipart/logs-2026-02-limit", L, M, rq_list(prefix=b"logs/2026/02/", limit=5))
    add("multipart/tmp", L, M, rq_list(prefix=b"tmp/"))
    add("multipart/delim-dot", L, M, rq_list(prefix=b"logs/2027/01/", delimiter=b"."))
    BK = "binary-keys"
    add("binary-keys/nul-delim", L + ["LIST-1"], BK, rq_list(delimiter=b"\x00"))
    add("binary-keys/nul-delim-a", L + ["LIST-1"], BK, rq_list(prefix=b"a", delimiter=b"\x00"))
    add("binary-keys/ff-delim", L + ["LIST-1"], BK, rq_list(delimiter=b"\xff"))
    add("binary-keys/ff-delim-ff", L + ["LIST-1"], BK, rq_list(prefix=b"\xff", delimiter=b"\xff"))
    add("binary-keys/upper-l-delim", L + ["LIST-1"], BK, rq_list(delimiter=b"L"))
    add("truncated-bounds-2/ff-delim-ff", L + ["LIST-1", "RG-6"], "truncated-bounds-2",
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
    for name, prefix, delim, n in configs:
        ds = oracle.Dataset.load(name)
        full = ds.list(prefix=prefix, delimiter=delim, limit=BIG)
        got, after, i = [], None, 0
        while True:
            req = rq_list(prefix=prefix, delimiter=delim, after=after, continuation=True if after is not None else None,
                          limit=n)
            exp = run(ds, req)
            reqs = ["LIST-6", "QUERY-3", "LIST-5"]
            g.add(f"list/{name}/{show(prefix) or 'root'}-{delim.hex()}-limit{n}/page-{i:02d}", reqs, name, exp,
                  request=req)
            got += exp["entries"]
            if len(exp["entries"]) < n:
                break
            after = dec(exp["entries"][-1]["key_base64"])
            i += 1
        assert got == full, f"pagination of {name} does not concatenate to the full listing"
    for name, n in (("multipart", 25), ("binary-keys", 7), ("truncated-bounds-2", 4)):
        src = "binary-keys" if name.startswith("truncated") else name
        ds = oracle.Dataset.load(src)
        full = ds.range(limit=BIG)
        got, start, i = [], None, 0
        while True:
            req = rq_range(start=start, start_exclusive=True if start is not None else None, limit=n)
            exp = run(ds, req)
            g.add(f"range/{name}-limit{n}/page-{i:02d}", ["RANGE-1", "QUERY-3"], name, exp, request=req)
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
    for c in scalar:
        req = rq_range(columns=[c])
        reqs = ["RANGE-2", "COL-3"] + (["COL-2"] if c in ("f64", "req", "extra") else []) + \
               (["COL-6"] if c == "a.b" else [])
        g.add(f"column/{c}", reqs, P, run(ds, req), request=req)
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
    for c in ("meta", "tags"):
        req = rq_range(columns=["i32", c])
        g.add(f"nested/{c}", ["COL-3"], P, run(ds, req), request=req,
              desc="projecting a nested or repeated column is rejected, never answered as absent")
    req = rq_list(prefix=b"row-0", delimiter=b"/", columns=["tags"])
    g.add("nested/list-tags", ["COL-3"], P, run(ds, req), request=req)
    lu = oracle.Dataset.load(P)
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
        ("list-limit-0", {"op": "list", "delimiter_base64": "Lw==", "limit": 0}),
        ("list-limit-fraction", {"op": "list", "delimiter_base64": "Lw==", "limit": 2.5}),
        ("list-delimiter-two-bytes", {"op": "list", "delimiter_base64": "Ly8=", "limit": 10}),
        ("list-delimiter-empty", {"op": "list", "delimiter_base64": "", "limit": 10}),
        ("list-delimiter-missing", {"op": "list", "limit": 10}),
        ("list-prefix-malformed-base64", {"op": "list", "prefix_base64": "YS8", "delimiter_base64": "Lw==", "limit": 10}),
        ("list-after-malformed-base64", {"op": "list", "after_base64": "@@@@", "delimiter_base64": "Lw==", "limit": 10}),
    ]
    for cid, raw in bad:
        g.add(f"basic/{cid}", ["QUERY-1"], "basic", {"status": "invalid_request"}, raw_request=raw)
    g.add("empty/range-limit-0", ["QUERY-1"], "empty", {"status": "invalid_request"},
          raw_request={"op": "range", "limit": 0}, desc="invalid even when the dataset is empty")
    return g


# Refusals ------------------------------------------------------------------

STRUCTURAL_READ = {"missing_page_index"}  # plus invalid_page_index faults citing RG-3/RG-4 only


def fault_level(f):
    """The validation level that must find a fault (spec section 8)."""
    codes, det = set(f["codes"]), f["detect"]
    if det.startswith("validate:"):
        return det.split(":")[1]
    if det == "open":
        return "structural"
    if codes <= STRUCTURAL_READ or ("invalid_page_index" in codes and set(f["requirements"]) & {"RG-3", "RG-4"}
                                    and not set(f["requirements"]) & {"RG-5", "KEY-6"}):
        return "structural"
    if "invalid_parquet" in codes:
        return "full"
    return "profile"


def expect_refusal(codes):
    return {"status": "refused", "code": codes[0]} if len(codes) == 1 else {"status": "refused", "codes": codes}


def read_request(f):
    req = rq_range(limit=BIG)
    r = f.get("request", "")
    if r.startswith("range-columns:"):
        req["columns"] = r.split(":", 1)[1].split(",")
    return req


def refusal_groups(lock):
    g_open = Group("refusals-open", "reader", "Faults a reader must detect when opening the dataset "
                                              "(detect: open).")
    g_read = Group("refusals-read", "reader", "Faults a reader must detect no later than decoding the faulty "
                                              "data (detect: read). Each request is an unbounded range whose limit "
                                              "covers every row, so it decodes the faulty page under any correct "
                                              "access path.")
    gv = {lvl: Group(f"validate-{lvl}", f"validator-{lvl}", f"Validation at the {lvl} level (section 8): valid "
                                                            "datasets complete; faults in scope fail with their code.")
          for lvl in ("structural", "profile", "full", "digest")}
    order = ["structural", "profile", "full"]
    for d in lock["datasets"]:
        if d["valid"]:
            n_sha = 0
            m = json.loads((CONF / d["dir"] / "manifest.json").read_text())
            n_sha = sum(1 for p in m["parts"] if "sha256" in p)
            for lvl in order:
                levels = {x: {"result": "complete"} for x in order[: order.index(lvl) + 1]}
                gv[lvl].add(d["name"], ["VAL-1"], d["dir"], {"status": "ok", "levels": levels},
                            request={"op": "validate", "level": lvl}, requires=[f"validate:{lvl}"])
            gv["digest"].add(d["name"], ["VAL-2", "PART-5"], d["dir"],
                             {"status": "ok", "levels": {"digest": {"result": "complete", "checked": n_sha}}},
                             request={"op": "validate", "level": "digest"}, requires=["validate:digest"])
            continue
        f = d["fault"]
        codes = f["codes"]
        exp = expect_refusal(codes)
        desc = d["description"] + (f" Note: {f['note']}" if f.get("note") else "")
        if f["detect"] == "open":
            g_open.add(d["name"], f["requirements"], d["dir"], exp, request={"op": "open"}, detect="open", desc=desc)
        elif f["detect"] == "read" or "invalid_parquet" in codes:
            g_read.add(d["name"], f["requirements"], d["dir"], exp, request=read_request(
                {"request": "range-columns:size"} if "invalid_parquet" in codes else f), detect="read", desc=desc)
        lvl = fault_level(f)
        if "inconsistent_column" in codes:
            continue  # COL-5 is only required for projected columns; validators SHOULD check it
        reqs = f["requirements"] + ["VAL-1"]
        if lvl == "digest":
            reqs = f["requirements"] + ["VAL-2"]
        gv[lvl].add(d["name"], reqs, d["dir"], exp, request={"op": "validate", "level": lvl},
                    detect=f"validate:{lvl}", requires=[f"validate:{lvl}"], desc=desc)
    return [g_open, g_read] + [gv[x] for x in ("structural", "profile", "full", "digest")]


def main():
    lock = json.loads((CONF / "fixtures.lock.json").read_text(encoding="utf-8"))
    groups = [open_group(lock), range_group(), prefix_group(), list_group(), pagination_group(),
              projection_group(), requests_group()] + refusal_groups(lock)
    files = []
    for g in groups:
        path = f"cases/{g.name}.json"
        write_text(CONF / path, dumps(g.doc()))
        files.append(path)
    suite = {
        "suite": "seekparquet-conformance-semantic",
        "revision": SUITE_REVISION,
        "status": "draft",
        "specification": "spec/format-1.md",
        "format_version": 0,
        "draft_revision": 1,
        "mandatory_targets": ["reader"],
        "mandatory_capabilities": ["open", "range", "list", "projection"],
        "case_files": files,
        "case_counts": {g.name: len(g.cases) for g in groups},
    }
    write_text(CONF / "suite.json", dumps(suite))
    print(f"{sum(len(g.cases) for g in groups)} cases in {len(groups)} files")


if __name__ == "__main__":
    main()
