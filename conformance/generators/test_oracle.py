#!/usr/bin/env python3
"""Property and example tests for oracle.py (standard library only).

    python -m unittest generators/test_oracle.py          # from conformance/

The properties hold for every valid dataset's source rows. The examples are small datasets whose expected
answers are written out by hand from the specification, so the oracle is not only checked against itself.
"""

from __future__ import annotations

import base64
import json
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE.parent), str(HERE)]

import oracle  # noqa: E402
from common import DEFAULT_KEY as KEY  # noqa: E402

BIG = 10 ** 6
VALID = ["basic", "multipart", "binary-keys", "projection", "delimiter", "list-bounds", "non-utf8-keys", "annotated",
         "encodings", "optional-codecs"]


def keys_of(entries):
    return [(e["kind"], base64.b64decode(e["key_base64"])) for e in entries]


def mini(keys, values=None):
    rows = [{"key_base64": base64.b64encode(k).decode(), "values": (values or {}).get(k, {"v": {"int": str(i)}})}
            for i, k in enumerate(sorted(keys))]
    return oracle.Dataset({"dataset": "mini", "key": KEY, "columns": [{"name": "v", "type": "int64"}],
                           "parts": [{"path": "p", "rows": rows}]})


def row(key, offset=0):
    return {"key_base64": base64.b64encode(key).decode(), "offset": offset}


def prefix(p):
    return {"prefix_base64": base64.b64encode(p).decode()}


def cursor_of(entry):
    if entry["kind"] == "row":
        return {"key_base64": entry["key_base64"], "offset": entry["offset"]}
    return {"prefix_base64": entry["key_base64"]}


class Properties(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ds = {n: oracle.Dataset.load(n) for n in VALID}

    def probes(self, ds):
        ks = ds.keys()
        out = {b"", b"\x00", b"\xff", b"\xff\xff", b"zz"}
        for k in ks:
            for n in (1, 2, 3, len(k) - 1, len(k)):
                if 0 < n <= len(k):
                    out.add(k[:n])
        return sorted(out)

    def test_prefix_equals_successor_range(self):
        for name, ds in self.ds.items():
            for p in self.probes(ds):
                self.assertEqual(ds.range(prefix=p, limit=BIG),
                                 ds.range(start=p, end=oracle.successor(p), limit=BIG), (name, p))

    def test_limit_is_a_prefix_of_the_full_result(self):
        for name, ds in self.ds.items():
            full = ds.range(limit=BIG)
            for n in (1, 2, 5, 17):
                self.assertEqual(ds.range(limit=n), full[:n], name)
            for d in (b"/", b"\xff", b"\x00"):
                fl = ds.list(delimiter=d, limit=BIG)
                for n in (1, 3):
                    self.assertEqual(ds.list(delimiter=d, limit=n), fl[:n], (name, d))

    def test_results_sorted_and_unique(self):
        for name, ds in self.ds.items():
            for p in self.probes(ds)[:40]:
                for d in (b"/", b"\xff", b"-"):
                    ks = [k for _kind, k in keys_of(ds.list(prefix=p, delimiter=d, limit=BIG))]
                    self.assertEqual(ks, sorted(set(ks)), (name, p, d))

    def test_projection_preserves_keys(self):
        ds = self.ds["projection"]
        bare = keys_of(ds.range(limit=BIG))
        with_cols = ds.range(limit=BIG, columns=["i32", "no_such", "key"])
        self.assertEqual(keys_of(with_cols), bare)
        for e in with_cols:
            self.assertEqual(e["values"][2], {"bytes_base64": e["key_base64"]})
            self.assertEqual(e["values"][1], {"absent": True})

    def test_common_prefix_witness(self):
        for name, ds in self.ds.items():
            ks = ds.keys()
            for p in self.probes(ds)[:30]:
                for d in (b"/", b"\xff"):
                    for kind, cp in keys_of(ds.list(prefix=p, delimiter=d, limit=BIG)):
                        if kind != "prefix":
                            continue
                        self.assertTrue(cp.startswith(p) and cp.endswith(d))
                        self.assertNotIn(d, cp[len(p):-1], (name, p, cp))
                        self.assertTrue(any(k.startswith(cp) for k in ks), (name, p, cp))

    def test_pagination_concatenates(self):
        for name in ("delimiter", "list-bounds", "basic", "binary-keys"):
            ds = self.ds[name]
            for p in (b"", b"a", b"a/"):
                for d in (b"/", b"\xff"):
                    full = ds.list(prefix=p, delimiter=d, limit=BIG)
                    for n in (1, 2, 3):
                        got, cursor = [], None
                        while True:
                            page = ds.list(prefix=p, delimiter=d, cursor=cursor, limit=n)
                            got += page
                            if len(page) < n:
                                break
                            cursor = cursor_of(page[-1])
                        self.assertEqual(got, full, (name, p, d, n))


class HandWrittenExamples(unittest.TestCase):
    """Expected answers derived from the specification by hand, not from the oracle."""

    def test_unsigned_byte_order(self):
        ds = mini([b"\x80", b"\x7f", b"\x7f\xff", b"a", b"\x00"])
        self.assertEqual([k for _k, k in keys_of(ds.range(limit=10))], [b"\x00", b"a", b"\x7f", b"\x7f\xff", b"\x80"])

    def test_range_bounds(self):
        ds = mini([b"a", b"b", b"c"])
        self.assertEqual(keys_of(ds.range(start=b"a", start_exclusive=True, end=b"c", limit=10)), [("row", b"b")])
        self.assertEqual(ds.range(end=b"", limit=10), [])  # QUERY-2
        self.assertEqual(len(ds.range(start=b"", limit=10)), 3)
        with self.assertRaises(oracle.InvalidRequest):
            ds.range(limit=0)

    def test_successor(self):
        self.assertEqual(oracle.successor(b"a\xff"), b"b")
        self.assertEqual(oracle.successor(b"ab"), b"ac")
        self.assertIsNone(oracle.successor(b"\xff\xff"))
        self.assertIsNone(oracle.successor(b""))

    def test_list_rollups(self):
        ds = mini([b"a", b"a/", b"a/b", b"a/b/", b"a/b/c", b"a//", b"b"])
        self.assertEqual(keys_of(ds.list(delimiter=b"/", limit=10)), [("row", b"a"), ("prefix", b"a/"), ("row", b"b")])
        self.assertEqual(keys_of(ds.list(prefix=b"a/", delimiter=b"/", limit=10)),
                         [("row", b"a/"), ("prefix", b"a//"), ("row", b"a/b"), ("prefix", b"a/b/")])

    def test_list7_worked_example(self):
        ds = mini([b"a/a", b"a/z"])
        self.assertEqual(keys_of(ds.list(delimiter=b"/", end=b"a/b", limit=10)), [("prefix", b"a/")])
        self.assertEqual(ds.list(delimiter=b"/", cursor=row(b"a/a"), end=b"a/b", limit=10), [])

    def test_list5_row_and_prefix_cursors(self):
        ds = mini([b"a/b/c", b"a/b/d", b"a/c"])
        # a row cursor filters rows only: a/b/d still rolls up to a/b/, which sorts before the cursor key
        self.assertEqual(keys_of(ds.list(prefix=b"a/", delimiter=b"/", cursor=row(b"a/b/c"), limit=10)),
                         [("prefix", b"a/b/"), ("row", b"a/c")])
        # a prefix cursor excludes every key under the prefix
        self.assertEqual(keys_of(ds.list(prefix=b"a/", delimiter=b"/", cursor=prefix(b"a/b/"), limit=10)),
                         [("row", b"a/c")])
        # a prefix with no successor excludes everything after it
        self.assertEqual(mini([b"\xff/a", b"\xff\xff"]).list(delimiter=b"/", cursor=prefix(b"\xff\xff"), limit=10), [])

    def test_prefix_cursor_is_for_listing_only(self):
        with self.assertRaises(oracle.InvalidRequest):
            mini([b"a/b"]).range(cursor=prefix(b"a/"), limit=10)

    def test_absent_versus_null(self):
        doc = {"dataset": "m", "key": KEY, "columns": [{"name": "v", "type": "int64"}, {"name": "w", "type": "int64"}],
               "parts": [{"path": "p0", "columns": ["v"], "rows": [{"key_base64": "YQ==", "values": {"v": {"null": True}}}]},
                         {"path": "p1", "rows": [{"key_base64": "Yg==", "values": {"v": {"int": "1"}, "w": {"int": "2"}}}]}]}
        e = oracle.Dataset(doc).range(limit=10, columns=["w", "v"])
        self.assertEqual([x["values"] for x in e], [[{"absent": True}, {"null": True}], [{"int": "2"}, {"int": "1"}]])

    def test_delimiter_must_be_one_byte(self):
        with self.assertRaises(oracle.InvalidRequest):
            mini([b"a"]).list(delimiter=b"//", limit=1)


class RepeatedGroups(unittest.TestCase):
    def test_handwritten_offsets_and_values(self):
        doc = {"dataset": "grouped", "key": {"column": "id", "unique": False},
               "columns": [{"name": "v", "type": "int64"}],
               "parts": [{"rows": [{"key_base64": "YQ==", "values": {"v": {"int": str(n)}}} for n in (9, 7, 8)]}]}
        ds = oracle.Dataset(doc)
        e = ds.range(cursor={"key_base64": "YQ==", "offset": 0}, limit=10, columns=["v", "id", "key"])
        self.assertEqual([x["offset"] for x in e], [1, 2])
        self.assertEqual([x["values"] for x in e], [[{"int": "7"}, {"bytes_base64": "YQ=="}, {"absent": True}],
                                                   [{"int": "8"}, {"bytes_base64": "YQ=="}, {"absent": True}]])
        self.assertEqual(ds.range(start=b"a", start_exclusive=True, limit=10), [])
        self.assertEqual(ds.range(end=b"a", limit=10), [])
        # an offset beyond the group skips the group; it is not an error
        self.assertEqual(ds.range(cursor={"key_base64": "YQ==", "offset": 9}, limit=10), [])

    def test_tagged_pagination_all_group_offsets(self):
        for name in ("repeated-keys", "one-large-group", "named-key"):
            ds = oracle.Dataset.load(name)
            for op in ("range", "list"):
                for limit in (1, 2, 5, 7):
                    args = {"limit": BIG, "columns": ["size"]}
                    if op == "list":
                        args["delimiter"] = b"/"
                    query = getattr(ds, op)
                    full = query(**args)
                    args["limit"] = limit
                    got = []
                    while True:
                        page = query(**args)
                        got += page
                        if len(page) < limit:
                            break
                        e = page[-1]
                        args["cursor"] = cursor_of(e)
                        self.assertLessEqual(len(got), len(ds.rows))
                    self.assertEqual(got, full, (name, op, limit))

    def test_malformed_cursors_even_on_empty(self):
        ds = oracle.Dataset({"dataset": "empty", "key": KEY, "columns": [], "parts": []})
        for cursor in ({}, {"key_base64": "YQ==", "offset": True}, {"key_base64": "YQ==", "offset": -1},
                       {"key_base64": "YQ==", "offset": 1 << 63}, {"key_base64": "YR==", "offset": 0},
                       {"prefix_base64": "@@@@"}):
            for query, args in ((ds.range, {}), (ds.list, {"delimiter": b"/"})):
                with self.assertRaises(oracle.InvalidRequest):
                    query(cursor=cursor, limit=1, **args)


if __name__ == "__main__":
    unittest.main()
