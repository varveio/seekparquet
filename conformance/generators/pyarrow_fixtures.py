#!/usr/bin/env python3
"""Write the pyarrow-produced SeekParquet fixtures.

Every dataset starts as a rows document in ``generators/rows/<name>.json``
(keys in base64, column values typed as in ``protocol.md``, plus the part
layout and writer settings). This script first (re)writes those documents from
the deterministic definitions below, then reads them back and writes the
Parquet parts and ``manifest.json`` with pyarrow, so the bytes on disk derive
from the text rows. Expected query answers are later computed from the same
rows by ``oracle.py``.

pyarrow writes everything the profile needs natively: the footer key-value
stamps (via Arrow schema metadata), ``sorting_columns``, the page index and
small key pages (``max_rows_per_page``). The resulting physical structure is
asserted after each write rather than assumed.

Some invalid datasets are also written natively here (rows out of order with
sorting metadata claimed, an empty key, a nullable or INT64 key, missing or
wrong stamps, missing sorting metadata, a key long enough that pyarrow drops
the ColumnIndex). Their fault and expected refusal are recorded in
``fixtures.lock.json``.

Run through ``generators/regenerate.py``.
"""

from __future__ import annotations

import io
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

import parquet_meta as pm
from common import (CONF, DATASETS, DRAFT_REVISION, FORMAT_VERSION, ROWS, b64d, b64e, dumps, hex_f32, hex_f64,
                    load_rows, manifest, part_entry, write_json, write_text)

PRODUCER = f"pyarrow {pa.__version__}"
COMMAND = "python generators/regenerate.py"


class LCG:
    """Tiny deterministic generator, so fixtures do not depend on the random module."""

    def __init__(self, seed):
        self.s = seed & 0xFFFFFFFFFFFFFFFF

    def next(self, n):
        self.s = (self.s * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
        return (self.s >> 33) % n


def W(mrpp, compression="none", dictionary=False, page_version="1.0", key_encoding=None):
    w = {"max_rows_per_page": mrpp, "compression": compression, "use_dictionary": dictionary,
         "data_page_version": page_version}
    if key_encoding:
        w["key_encoding"] = key_encoding
    return w


def I(n):
    return {"int": str(n)}


def S(s):
    return {"bytes_base64": b64e(s.encode("utf-8") if isinstance(s, str) else s)}


NULL = {"null": True}


def split_parts(rows, sizes):
    out, off = [], 0
    for n in sizes:
        out.append(rows[off:off + n])
        off += n
    assert off == len(rows)
    return out


def rows_of(keys, values_fn):
    keys = sorted(keys)
    assert len(set(keys)) == len(keys)
    return [{"key_base64": b64e(k), "values": values_fn(i, k)} for i, k in enumerate(keys)]


# --------------------------------------------------------------------------
# Dataset definitions (source rows)

def def_basic():
    keys = [b"a/1.txt", b"a/2.txt", b"a/b/3.txt", b"alpha", b"b", b"bravo", b"c/", b"c/d", b"charlie",
            b"delta", b"echo", b"foxtrot"]
    rows = rows_of(keys, lambda i, k: {"size": I(100 + i * 7),
                                       "etag": NULL if i % 4 == 3 else S(f"etag-{i:02d}")})
    return {
        "dataset": "basic",
        "description": "One part, two row groups of six rows, key pages of two rows; uncompressed PLAIN v1 pages.",
        "schema_id": "conformance/objects/v1",
        "key_type": "binary",
        "columns": [{"name": "size", "type": "int64", "nullable": False},
                    {"name": "etag", "type": "string", "nullable": True}],
        "parts": [{"path": "data/part-00000.parquet", "row_groups": [6, 6], "writer": W(2), "rows": rows}],
    }


def def_multipart():
    keys = [b"README"]
    keys += [b"app/v1/obj-%03d" % i for i in range(10)]
    for month, n in ((1, 30), (2, 40), (3, 30)):
        keys += [b"logs/2026/%02d/f%03d.gz" % (month, i) for i in range(n)]
    keys += [b"logs/2027/01/f%03d.gz" % i for i in range(20)]
    keys += [b"tmp/", b"tmp/x", b"zz-last"]
    rng = LCG(7)
    classes = ["STANDARD", "STANDARD", "GLACIER", None]

    def vals(i, k):
        sc = classes[rng.next(4)]
        return {"size": I(rng.next(1 << 40)),
                "storage_class": NULL if sc is None else S(sc),
                "mtime": NULL if rng.next(9) == 0 else I(1767225600 + rng.next(10**7))}

    rows = rows_of(keys, vals)
    p0, p1, p2 = split_parts(rows, [45, 45, len(rows) - 90])
    return {
        "dataset": "multipart",
        "description": ("Three parts listed out of file-name order, several row groups and pages each, "
                        "leading/interior/trailing empty row groups, SNAPPY/ZSTD/GZIP, dictionary and "
                        "DELTA_BYTE_ARRAY keys, data page v1 and v2, sha256 in the manifest, an extension, "
                        "and a stray Parquet file under the root that is not part of the dataset."),
        "schema_id": "conformance/objects/v1",
        "key_type": "binary",
        "columns": [{"name": "size", "type": "int64", "nullable": False},
                    {"name": "storage_class", "type": "string", "nullable": True},
                    {"name": "mtime", "type": "int64", "nullable": True}],
        "manifest": {"sha256": True, "extensions": {"conformance": {"synthetic": True, "note": "ignored by readers"}}},
        "parts": [
            {"path": "shards/c.parquet", "row_groups": [0, 16, 16, 0, 13],
             "writer": W(4, "snappy", True, "1.0"), "rows": p0},
            {"path": "shards/a.parquet", "row_groups": [15, 15, 15],
             "writer": W(5, "zstd", False, "2.0", key_encoding="DELTA_BYTE_ARRAY"), "rows": p1},
            {"path": "shards/b.parquet", "row_groups": [20, len(p2) - 20, 0],
             "writer": W(3, "gzip", True, "2.0"), "rows": p2},
        ],
    }


LONG_L = b"L" * 120
FF80 = b"\xff" * 80


def def_binary_keys():
    keys = [b"\x00", b"\x00\x00", b"\x00\x01", b"\x01", b"\x1f", b" ", b"A", b"a", b"a\x00", b"a\x00\x00",
            b"a\x01", b"ab", b"abc", b"abd", b"p", b"pq", b"pqr", b"pqrs", b"\x7e", b"\x7f", b"\x7f\xff",
            b"\x80", b"\x80\x00", b"\xc3\x28", b"\xc3\xa9", b"\xe2\x82", b"\xef\xbd\x9e",
            b"\xf0\x9f\x98\x80", b"\xfe", b"\xff", b"\xff\x00", b"\xff\xfe", b"\xff\xff", b"\xff\xff\xff",
            FF80 + b"\x01", FF80 + b"\x02", b"\xff" * 81]
    keys += [LONG_L + b"%02d" % i for i in range(7)]
    keys += [b"M" * 70 + b"\xff" * 10 + b"\x01", b"M" * 70 + b"\xff" * 10 + b"\x02"]
    rows = rows_of(keys, lambda i, k: {"v": I(i)})
    n0 = 24
    return {
        "dataset": "binary-keys",
        "description": ("Keys exercising unsigned byte order: NUL, 0x7F/0x80, 0xFF, invalid UTF-8, "
                        "UTF-8 whose UTF-16 order differs, prefix chains, keys longer than 64 bytes "
                        "(pyarrow does not shorten bounds), and long keys of 0xFF bytes. Pages of 3 rows."),
        "schema_id": "conformance/binary/v1",
        "key_type": "binary",
        "columns": [{"name": "v", "type": "int32", "nullable": False}],
        "parts": [
            {"path": "data/part-00000.parquet", "row_groups": [12, 12],
             "writer": W(3, "zstd", False, "1.0", key_encoding="DELTA_BYTE_ARRAY"), "rows": rows[:n0]},
            {"path": "data/part-00001.parquet", "row_groups": [9, len(rows) - n0 - 9],
             "writer": W(3, "snappy", True, "2.0"), "rows": rows[n0:]},
        ],
    }


def def_projection():
    keys = [b"row-%02d" % i for i in range(18)]
    # Exact IEEE 754 bits: 1.5, -0.0, +inf, -inf, quiet NaN, NaN with payload, signalling NaN, negative NaN,
    # largest finite, smallest subnormal.
    f32s = ["3fc00000", "80000000", "7f800000", "ff800000", "7fc00000", "7fc00001", "7f800001", "ffc00000",
            "7f7fffff", "00000001"]
    f64s = ["3ff8000000000000", "8000000000000000", "7ff0000000000000", "7ff8000000000000",
            "7ff8000000000001", "7ff0000000000001", "fff8000000000000", "7fefffffffffffff",
            "0000000000000001", "3fb999999999999a"]
    u32s = [0, 1, 2147483648, 4294967295]
    u64s = [0, 1, 9223372036854775808, 18446744073709551615]
    i32s = [0, -1, 2147483647, -2147483648, 42]
    i64s = [0, -1, 9223372036854775807, -9223372036854775808, 1 << 40]

    def vals(i, k):
        nul = i % 5 == 4
        allnull = i == 9
        return {
            "flag": NULL if nul or allnull else {"bool": i % 3 == 0},
            "i32": NULL if nul or allnull else I(i32s[i % len(i32s)]),
            "i64": NULL if (i % 7 == 6) or allnull else I(i64s[i % len(i64s)]),
            "f32": NULL if nul or allnull else {"float32": f32s[i % len(f32s)]},
            "f64": NULL if (i % 6 == 5) or allnull else {"float64": f64s[i % len(f64s)]},
            # Unsigned annotations are ignored: values are reported as the signed physical integer.
            "u32": I(u32s[i % 4] - (1 << 32) if u32s[i % 4] >= 1 << 31 else u32s[i % 4]),
            "u64": NULL if allnull else I(u64s[i % 4] - (1 << 64) if u64s[i % 4] >= 1 << 63 else u64s[i % 4]),
            "a.b": I(i * 3),
            "name": NULL if nul or allnull else S(["", "plain", "café", "日本", "x" * 40][i % 5]),
            "blob": NULL if (i % 4 == 2) or allnull else S(bytes([0, 255, i, 128])[: 1 + i % 4]),
            "fx": NULL if nul or allnull else S(bytes([i, 0, 255, 127])),
            "req": I(1000 - i),
            "extra": I(-i),
            "meta": NULL if nul else {"json": {"a": i}},
            "tags": NULL if nul else {"json": ["t%d" % j for j in range(i % 3)]},
        }

    rows = rows_of(keys, vals)
    allc = ["flag", "i32", "i64", "f32", "f64", "u32", "u64", "a.b", "name", "blob", "fx", "req", "meta", "tags"]
    return {
        "dataset": "projection",
        "description": ("Projected values of every mandatory physical type with nulls, special floats given by exact bits "
                        "(signed zero, infinities, NaN payloads), unsigned-annotated INT32/INT64 reported as signed "
                        "physical values, and a top-level column literally named a.b; "
                        "the key column carries the STRING annotation; part 1 lacks columns f64 and req, "
                        "only part 2 has column extra; meta (struct) and tags (list) are nested."),
        "schema_id": "conformance/projection/v1",
        "key_type": "string",
        "columns": [{"name": "flag", "type": "bool", "nullable": True},
                    {"name": "i32", "type": "int32", "nullable": True},
                    {"name": "i64", "type": "int64", "nullable": True},
                    {"name": "f32", "type": "float32", "nullable": True},
                    {"name": "f64", "type": "float64", "nullable": True},
                    {"name": "u32", "type": "uint32", "nullable": False},
                    {"name": "u64", "type": "uint64", "nullable": True},
                    {"name": "a.b", "type": "int64", "nullable": False},
                    {"name": "name", "type": "string", "nullable": True},
                    {"name": "blob", "type": "binary", "nullable": True},
                    {"name": "fx", "type": "fixed:4", "nullable": True},
                    {"name": "req", "type": "int64", "nullable": False},
                    {"name": "extra", "type": "int32", "nullable": False},
                    {"name": "meta", "type": "struct<a:int32>", "nullable": True},
                    {"name": "tags", "type": "list<string>", "nullable": True}],
        "parts": [
            {"path": "data/part-00000.parquet", "columns": allc, "row_groups": [3, 3],
             "writer": W(2, "gzip", False, "1.0"), "rows": rows[0:6]},
            {"path": "data/part-00001.parquet", "columns": [c for c in allc if c not in ("f64", "req")],
             "row_groups": [3, 3], "writer": W(2, "snappy", True, "2.0"), "rows": rows[6:12]},
            {"path": "data/part-00002.parquet", "columns": allc + ["extra"], "row_groups": [6],
             "writer": W(2, "none", False, "1.0"), "rows": rows[12:18]},
        ],
    }


def def_delimiter():
    keys = [b"/", b"//", b"//x", b"/a", b"/a/b", b"a", b"a/", b"a//", b"a//b", b"a///", b"a/b", b"a/b/",
            b"a/b/c", b"a/b/c/", b"a/b/c/d", b"a/b/d", b"a/bc", b"a/c", b"a0", b"ab", b"b", b"b/", b"b/x",
            b"caf\xc3\xa9", b"caf\xc3\xa9/x", b"caf\xc3\xa9\xc3\xa9", b"caf\xc3\xa9\xc3\xa9z",
            b"x\xffy", b"x\xffy\xffz", b"x\xff\xff", b"x\xffz",
            b"\xff", b"\xff/a", b"\xff/b/c", b"\xff\xff", b"\xff\xff/a", b"\xff\xff\xff/x"]
    rows = rows_of(keys, lambda i, k: {"size": I(i * 10)})
    sk = sorted(keys)
    cut0 = sk.index(b"a/b/c") + 1  # the common prefix a/b/ spans parts 0 and 1
    cut1 = sk.index(b"caf\xc3\xa9\xc3\xa9") + 1
    return {
        "dataset": "delimiter",
        "description": ("Folder-like keys: keys equal to prefixes, trailing and repeated delimiters, a delimiter "
                        "at the start, non-ASCII bytes used as delimiters, keys beginning with 0xFF, and the "
                        "common prefix a/b/ spanning a part boundary. Pages of 2 rows."),
        "schema_id": "conformance/objects/v1",
        "key_type": "binary",
        "columns": [{"name": "size", "type": "int64", "nullable": False}],
        "parts": [
            {"path": "data/part-00000.parquet", "row_groups": [cut0], "writer": W(2), "rows": rows[:cut0]},
            {"path": "data/part-00001.parquet", "row_groups": [4, cut1 - cut0 - 4], "writer": W(2, "snappy"),
             "rows": rows[cut0:cut1]},
            {"path": "data/part-00002.parquet", "row_groups": [len(rows) - cut1], "writer": W(2),
             "rows": rows[cut1:]},
        ],
    }


def def_list_bounds():
    rows = rows_of([b"a/a", b"a/z", b"b/1", b"b/2", b"c"], lambda i, k: {"size": I(i)})
    return {"dataset": "list-bounds",
            "description": "Keys a/a, a/z, b/1, b/2, c: the worked example of LIST-7 (bounds apply to candidate keys).",
            "schema_id": "conformance/objects/v1", "key_type": "binary",
            "columns": [{"name": "size", "type": "int64", "nullable": False}],
            "parts": [{"path": "data/part-00000.parquet", "row_groups": [5], "writer": W(2), "rows": rows}]}


def def_non_utf8_keys():
    keys = [b"k00", b"k00\xc3", b"k01", b"k02", b"k02\x80", b"k03", b"k04", b"k05", b"k06"]
    rows = rows_of(keys, lambda i, k: {"size": I(i)})
    return {"dataset": "non-utf8-keys",
            "description": ("An unannotated key column holding keys that are not valid UTF-8 (allowed, KEY-6). "
                            "Each invalid key is in the middle of a 3-row page, so every page bound is valid UTF-8."),
            "schema_id": "conformance/objects/v1", "key_type": "binary",
            "columns": [{"name": "size", "type": "int64", "nullable": False}],
            "parts": [{"path": "data/part-00000.parquet", "row_groups": [9], "writer": W(3), "rows": rows}]}


def def_inconsistent(name, desc, override):
    r0 = [{"key_base64": b64e(b"k%02d" % i), "values": {"size": I(i)}} for i in range(4)]
    r1 = [{"key_base64": b64e(b"k%02d" % i), "values": {"size": I(i)}} for i in range(4, 8)]
    return {"dataset": name, "description": desc, "schema_id": "conformance/objects/v1", "key_type": "binary",
            "columns": [{"name": "size", "type": "int64", "nullable": False}],
            "parts": [{"path": "data/part-00000.parquet", "row_groups": [4], "writer": W(2), "rows": r0},
                      {"path": "data/part-00001.parquet", "row_groups": [4], "writer": W(2), "rows": r1,
                       "column_overrides": {"size": override}}]}


def def_empty():
    return {"dataset": "empty", "description": "A committed dataset with no parts.",
            "schema_id": "conformance/objects/v1", "key_type": "binary",
            "columns": [{"name": "size", "type": "int64", "nullable": False}], "parts": []}


# Invalid data written natively ------------------------------------------------

def _simple(name, desc, keys, row_groups, mrpp=2, ordered=False):
    """A one-part dataset whose rows are stored exactly in the given order."""
    if ordered:
        keys = sorted(keys)
    rows = [{"key_base64": b64e(k), "values": {"size": I(i)}} for i, k in enumerate(keys)]
    return {"dataset": name, "description": desc, "schema_id": "conformance/objects/v1", "key_type": "binary",
            "columns": [{"name": "size", "type": "int64", "nullable": False}],
            "parts": [{"path": "data/part-00000.parquet", "row_groups": row_groups, "writer": W(mrpp),
                       "rows": rows}]}


def def_invalid_native():
    base = [b"k%02d" % i for i in range(12)]
    docs = []
    # Pages hold 2 rows: rows 0-1, 2-3, 4-5, ...; row groups hold 6 rows.
    docs.append(_simple("key-order-within-page", "Rows 2 and 3 (one page) are stored in descending order.",
                        [b"k00", b"k01", b"k03", b"k02"] + base[4:], [6, 6]))
    docs.append(_simple("key-order-duplicate", "Key k04 is stored twice in one page (rows 4 and 5).",
                        base[:4] + [b"k04", b"k04"] + base[6:], [6, 6]))
    docs.append(_simple("key-order-across-pages",
                        "Page 1 ends with k04 and page 2 starts with k03; each page is internally ascending.",
                        [b"k00", b"k01", b"k02", b"k04", b"k03", b"k05"] + base[6:], [6, 6]))
    docs.append(_simple("key-order-across-row-groups",
                        "Row group 0 ends with k06 and row group 1 starts with k05.",
                        [b"k00", b"k01", b"k02", b"k03", b"k04", b"k06", b"k05", b"k07"] + base[8:], [6, 6]))
    docs.append(_simple("key-order-duplicate-across-row-groups",
                        "The last key of row group 0 equals the first key of row group 1.",
                        base[:6] + [b"k05"] + base[7:], [6, 6]))
    docs.append(_simple("invalid-key-empty",
                        "The first key of the only part is empty (it still sorts first).",
                        [b""] + base[1:], [6, 6]))
    long_key = b"m/" + b"x" * 5000
    docs.append(_simple("missing-column-index-native",
                        "Row group 1 holds a 5002-byte key; pyarrow omits the whole ColumnIndex of that key "
                        "chunk (statistics larger than its 4096-byte limit) but keeps the OffsetIndex.",
                        sorted(base[:9] + [long_key]), [5, 5], ordered=True))
    return docs


def def_key_order_across_parts():
    """Two parts whose keys interleave (part 0 ends after part 1 starts)."""
    keys0 = [b"k00", b"k01", b"k02", b"k03", b"k04", b"k09"]
    keys1 = [b"k05", b"k06", b"k07", b"k08", b"k10", b"k11"]
    r0 = [{"key_base64": b64e(k), "values": {"size": I(i)}} for i, k in enumerate(keys0)]
    r1 = [{"key_base64": b64e(k), "values": {"size": I(6 + i)}} for i, k in enumerate(keys1)]
    return {"dataset": "key-order-across-parts",
            "description": ("Part 0 ends with k09, part 1 starts with k05. The manifest hides this by "
                            "declaring part 0's max key as k04 (so the manifest bounds look disjoint)."),
            "schema_id": "conformance/objects/v1", "key_type": "binary",
            "columns": [{"name": "size", "type": "int64", "nullable": False}],
            "manifest": {"override_bounds": {0: [None, b"k04"]}},
            "parts": [{"path": "data/part-00000.parquet", "row_groups": [6], "writer": W(2), "rows": r0},
                      {"path": "data/part-00001.parquet", "row_groups": [6], "writer": W(2), "rows": r1}]}


# Native variants of basic: (name, description, fault options, refusal) ----------

BASIC_VARIANTS = [
    ("stamp-missing", "Parts carry no seekparquet.* footer metadata.", {"stamps": {}}),
    ("stamp-wrong-schema-id", "Footer seekparquet.schema_id differs from the manifest's.",
     {"stamps": {"seekparquet.schema_id": "conformance/other/v1"}}),
    ("stamp-wrong-revision", "Footer seekparquet.draft_revision is 2; the manifest says 1.",
     {"stamps": {"seekparquet.draft_revision": "2"}}),
    ("stamp-noncanonical-revision", "Footer seekparquet.draft_revision is \"01\" (not canonical decimal).",
     {"stamps": {"seekparquet.draft_revision": "01"}}),
    ("stamp-missing-revision", "Footer lacks seekparquet.draft_revision although format_version is 0.",
     {"stamps": {"seekparquet.draft_revision": None}}),
    ("key-nullable", "The key column is OPTIONAL (no nulls are stored).", {"key": "nullable"}),
    ("key-int64", "The key column is INT64.", {"key": "int64"}),
    ("key-missing", "No column is named key (it is named id).", {"key": "missing"}),
    ("key-nested", "key is a group column (struct with one BYTE_ARRAY child).", {"key": "struct"}),
    ("sorting-columns-missing", "Row groups carry no sorting_columns.", {"sorting": "none"}),
    ("sorting-columns-descending", "sorting_columns declares key descending.", {"sorting": "descending"}),
    ("sorting-columns-other-first", "sorting_columns names column size first, then key.",
     {"sorting": "other_first"}),
    ("unsupported-version-draft-2", "Manifest and stamps say format_version 0, draft_revision 2.",
     {"version": (0, 2)}),
    ("unsupported-version-1", "Manifest and stamps say format_version 1 (reserved until format 1 is final).",
     {"version": (1, None)}),
]


# --------------------------------------------------------------------------
# Writing

def arrow_type(t):
    simple = {"bool": pa.bool_(), "int32": pa.int32(), "int64": pa.int64(), "float32": pa.float32(),
              "float64": pa.float64(), "string": pa.string(), "binary": pa.binary(), "uint32": pa.uint32(),
              "uint64": pa.uint64()}
    if t in simple:
        return simple[t]
    if t.startswith("fixed:"):
        return pa.binary(int(t.split(":")[1]))
    if t == "struct<a:int32>":
        return pa.struct([pa.field("a", pa.int32())])
    if t == "list<string>":
        return pa.list_(pa.string())
    raise ValueError(t)


def py_value(t, v):
    if "null" in v:
        return None
    if "json" in v:
        return v["json"]
    if "bool" in v:
        return v["bool"]
    if "int" in v:
        return int(v["int"])
    if "float32" in v:
        return hex_f32(v["float32"])
    if "float64" in v:
        return hex_f64(v["float64"])
    b = b64d(v["bytes_base64"])
    return b.decode("utf-8") if t == "string" else b


def stamps_for(doc, version=(FORMAT_VERSION, DRAFT_REVISION), override=None):
    fv, dr = version
    st = {"seekparquet.format_version": str(fv)}
    if dr is not None:
        st["seekparquet.draft_revision"] = str(dr)
    st["seekparquet.schema_id"] = doc["schema_id"]
    if override is not None:
        if override == {}:
            return {}
        for k, v in override.items():
            if v is None:
                st.pop(k, None)
            else:
                st[k] = v
    return st


def build_table(doc, part, fault):
    names = part.get("columns") or [c["name"] for c in doc["columns"]]
    cols = {c["name"]: c for c in doc["columns"]}
    keys = [b64d(r["key_base64"]) for r in part["rows"]]
    key_mode = fault.get("key")
    if key_mode == "int64":
        kfield, karr = pa.field("key", pa.int64(), nullable=False), pa.array(range(len(keys)), pa.int64())
    elif key_mode == "struct":
        st = pa.struct([pa.field("k", pa.binary(), nullable=False)])
        kfield, karr = pa.field("key", st, nullable=False), pa.array([{"k": k} for k in keys], st)
    else:
        kt = pa.string() if doc["key_type"] == "string" else pa.binary()
        kv = [k.decode("utf-8") for k in keys] if doc["key_type"] == "string" else keys
        kname = "id" if key_mode == "missing" else "key"
        kfield, karr = pa.field(kname, kt, nullable=key_mode == "nullable"), pa.array(kv, kt)
    fields, arrays = [kfield], [karr]
    for n in names:
        c = dict(cols[n], **part.get("column_overrides", {}).get(n, {}))
        t = arrow_type(c["type"])
        fields.append(pa.field(n, t, nullable=c["nullable"]))
        arrays.append(physical_array(c["type"], t, [r["values"][n] for r in part["rows"]]))
    return fields, arrays


BITS = {"float32": (pa.uint32(), 32), "float64": (pa.uint64(), 64), "uint32": (pa.uint32(), 32),
        "uint64": (pa.uint64(), 64)}


def physical_array(tname, t, values):
    """Build an Arrow array whose Parquet physical values are exactly the typed protocol values."""
    if tname in ("float32", "float64"):
        ut, _ = BITS[tname]
        bits = [None if "null" in v else int(v[tname], 16) for v in values]
        return pa.array(bits, ut).view(t)
    if tname in ("uint32", "uint64"):
        _, w = BITS[tname]
        return pa.array([None if "null" in v else int(v["int"]) % (1 << w) for v in values], t)
    return pa.array([py_value(tname, v) for v in values], t)


def physical_values(tname, arr):
    """Inverse of physical_array, for checking what pyarrow reads back."""
    out = []
    if tname in ("float32", "float64"):
        bits = arr.view(BITS[tname][0]).to_pylist()
        w = BITS[tname][1] // 4
        return [{"null": True} if b is None else {tname: format(b, "0%dx" % w)} for b in bits]
    for x in arr.to_pylist():
        if x is None:
            out.append({"null": True})
        elif tname in ("uint32", "uint64"):
            w = BITS[tname][1]
            out.append({"int": str(x - (1 << w) if x >= 1 << (w - 1) else x)})
        elif tname == "bool":
            out.append({"bool": x})
        elif tname in ("int32", "int64"):
            out.append({"int": str(x)})
        elif tname in ("string", "binary") or tname.startswith("fixed:"):
            out.append({"bytes_base64": b64e(x.encode("utf-8") if isinstance(x, str) else x)})
        else:
            out.append({"json": x})
    return out


def write_part(doc, part, fault=None) -> bytes:
    fault = fault or {}
    fields, arrays = build_table(doc, part, fault)
    st = stamps_for(doc, fault.get("version", (FORMAT_VERSION, DRAFT_REVISION)), fault.get("stamps"))
    schema = pa.schema(fields, metadata=st or None)
    table = pa.Table.from_arrays(arrays, schema=schema)
    w = part["writer"]
    sorting = fault.get("sorting", "key")
    sc = {"key": [pq.SortingColumn(0)], "none": None, "descending": [pq.SortingColumn(0, descending=True)],
          "other_first": [pq.SortingColumn(1), pq.SortingColumn(0)]}[sorting]
    kwargs = dict(version="2.6", write_statistics=True, write_page_index=True, store_schema=True,  # store_schema=False drops all schema metadata, including the stamps
                  max_rows_per_page=w["max_rows_per_page"], compression=w["compression"],
                  data_page_version=w["data_page_version"], sorting_columns=sc)
    if w.get("key_encoding"):
        others = [f.name for f in fields[1:]] if w["use_dictionary"] else False
        kwargs.update(use_dictionary=others, column_encoding={"key": w["key_encoding"]})
    else:
        kwargs.update(use_dictionary=w["use_dictionary"])
    buf = io.BytesIO()
    writer = pq.ParquetWriter(buf, schema, **kwargs)
    off = 0
    for n in part["row_groups"]:
        writer.write_table(table.slice(off, n), row_group_size=max(n, 1))
        off += n
    assert off == len(part["rows"]), "row groups do not cover the rows"
    writer.close()
    data = buf.getvalue()
    pm.roundtrip_check(data)
    return data


def assert_structure(data, doc, part, fault=None):
    """Assert the physical structure pyarrow produced (do not assume option semantics)."""
    fault = fault or {}
    pf = pm.read_parquet(data)
    rgs = pf.row_groups
    assert len(rgs) == len(part["row_groups"]), "row group count"
    assert pf.meta.get(pm.FMD_NUM_ROWS) == len(part["rows"])
    kv = pf.kv()
    want = stamps_for(doc, fault.get("version", (FORMAT_VERSION, DRAFT_REVISION)), fault.get("stamps"))
    got = {k: v for k, v in kv.items() if k.startswith("seekparquet.")}
    assert got == want, f"stamps in footer {got} != {want}"
    leaves = pf.leaf_columns()
    if fault.get("key") is None:
        path, el, d, r = leaves[0]
        assert path == ("key",) and el.get(pm.SE_TYPE) == 6 and el.get(pm.SE_REPETITION) == 0
    keys = [b64d(r["key_base64"]) for r in part["rows"]]
    mrpp = part["writer"]["max_rows_per_page"]
    off = 0
    for ri, (rg, n) in enumerate(zip(rgs, part["row_groups"])):
        assert rg.get(pm.RG_NUM_ROWS) == n
        if n == 0:
            off += n
            continue
        sc = rg.get(pm.RG_SORTING)
        if fault.get("sorting", "key") == "key":
            assert sc is not None and sc.items[0].get(pm.SC_COLUMN_IDX) == 0 and not sc.items[0].get(pm.SC_DESCENDING)
        cc = pm.column_chunk(pf, ri, 0)
        pages = pm.data_pages(pf, cc)
        expect = [mrpp] * (n // mrpp) + ([n % mrpp] if n % mrpp else [])
        got_rows = [p.num_rows if p.num_rows is not None else p.num_values for p in pages]
        assert got_rows == expect, f"key pages {got_rows} != {expect} (row group {ri})"
        oi = pm.read_index(pf, cc, "offset")
        ci = pm.read_index(pf, cc, "column")
        assert oi is not None, "offset index missing"
        firsts = [p.get(pm.PL_FIRST_ROW) for p in oi.items_of(pm.OI_PAGE_LOCATIONS)]
        cum = [sum(expect[:i]) for i in range(len(expect))]
        assert firsts == cum, f"first_row_index {firsts} != {cum}"
        if fault.get("expect_no_column_index_rg") == ri:
            assert ci is None, "expected pyarrow to omit the ColumnIndex"
        elif fault.get("key") is None:
            assert ci is not None, "column index missing"
            assert not any(ci.items_of(pm.CI_NULL_PAGES))
            mins, maxs = ci.items_of(pm.CI_MIN), ci.items_of(pm.CI_MAX)
            assert len(mins) == len(expect)
            for pi, cnt in enumerate(expect):
                pk = keys[off + cum[pi]: off + cum[pi] + cnt]
                assert mins[pi] == min(pk) and maxs[pi] == max(pk), "pyarrow bounds are exact page min/max"
        off += n
    # Values read back by pyarrow equal the source rows.
    if fault.get("key") is None:
        t = pq.read_table(io.BytesIO(data))
        names = part.get("columns") or [c["name"] for c in doc["columns"]]
        assert t.column_names == ["key"] + names
        got_keys = [k if isinstance(k, bytes) else k.encode("utf-8") for k in t.column("key").to_pylist()]
        assert got_keys == keys
        cols = {c["name"]: dict(c, **part.get("column_overrides", {}).get(c["name"], {})) for c in doc["columns"]}
        for n in names:
            exp = [r["values"][n] for r in part["rows"]]
            got_v = physical_values(cols[n]["type"], t.column(n).combine_chunks())
            assert exp == got_v, (n, exp, got_v)


def manifest_for(doc, datas, version=(FORMAT_VERSION, DRAFT_REVISION)):
    mopt = doc.get("manifest", {})
    over = mopt.get("override_bounds", {})
    parts = []
    for i, (part, data) in enumerate(zip(doc["parts"], datas)):
        keys = [b64d(r["key_base64"]) for r in part["rows"]]
        mn, mx = keys[0], keys[-1]
        if i in over or str(i) in over:
            o = over.get(i, over.get(str(i)))
            mn = o[0] if o[0] is not None else mn
            mx = o[1] if o[1] is not None else mx
        if not mn:  # an empty first key cannot be expressed in a manifest (minLength 4)
            mn = keys[1]
        parts.append(part_entry(part["path"], len(part["rows"]), data, mn, mx, mopt.get("sha256", False)))
    return manifest(parts, doc["schema_id"], format_version=version[0], draft_revision=version[1],
                    extensions=mopt.get("extensions"))


def jsonable_rows_doc(doc):
    d = dict(doc)
    if "manifest" in d and "override_bounds" in d["manifest"]:
        m = dict(d["manifest"])
        m["override_bounds"] = {str(k): [None if x is None else b64e(x) for x in v]
                                for k, v in m["override_bounds"].items()}
        d["manifest"] = m
    return d


def restore_rows_doc(d):
    m = d.get("manifest", {})
    if "override_bounds" in m:
        m["override_bounds"] = {int(k): [None if x is None else b64d(x) for x in v]
                                for k, v in m["override_bounds"].items()}
    return d


def write_dataset(doc, out_dir, fault=None, check=True, extra_files=None):
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    fault = fault or {}
    datas = []
    for part in doc["parts"]:
        data = write_part(doc, part, fault)
        if check:
            assert_structure(data, doc, part, fault)
        p = out_dir / part["path"]
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        datas.append(data)
    m = manifest_for(doc, datas, fault.get("version", (FORMAT_VERSION, DRAFT_REVISION)))
    write_json(out_dir / "manifest.json", m)
    for rel, data in (extra_files or {}).items():
        (out_dir / rel).parent.mkdir(parents=True, exist_ok=True)
        (out_dir / rel).write_bytes(data)
    return m


VALID = [def_empty, def_basic, def_multipart, def_binary_keys, def_projection, def_delimiter, def_list_bounds,
         def_non_utf8_keys]


def def_inconsistent_all():
    return [def_inconsistent("inconsistent-column-type", "Column size is INT64 in part 0 and INT32 in part 1.",
                             {"type": "int32"}),
            def_inconsistent("inconsistent-column-repetition",
                             "Column size is REQUIRED in part 0 and OPTIONAL in part 1.", {"nullable": True})]


def rec(name, out_dir, rows_name, valid, desc, provenance="native", fault=None, extra=None):
    r = {"name": name, "dir": str(out_dir.relative_to(CONF)), "valid": valid, "description": desc,
         "rows": f"generators/rows/{rows_name}.json" if rows_name else None,
         "producer": PRODUCER, "provenance": provenance, "command": COMMAND}
    if fault:
        r["fault"] = fault
    if extra:
        r.update(extra)
    return r


def build():
    records = []
    # 1. rows documents
    docs = [f() for f in VALID] + def_invalid_native() + [def_key_order_across_parts()] + def_inconsistent_all()
    for d in docs:
        write_text(ROWS / f"{d['dataset']}.json", dumps(jsonable_rows_doc(d)))

    # 2. valid datasets, from the rows documents as written
    for f in VALID:
        name = f()["dataset"]
        doc = restore_rows_doc(load_rows(name))
        out = DATASETS / name
        extra = None
        if name == "multipart":
            # A stray Parquet file under the root that the manifest does not list (LAYOUT-1).
            stray = {"dataset": "multipart-stray", "schema_id": doc["schema_id"], "key_type": "binary",
                     "columns": doc["columns"],
                     "parts": [{"path": "x", "row_groups": [2], "writer": W(2),
                                "rows": [{"key_base64": b64e(b"logs/2026/02/stray-%d.gz" % i),
                                          "values": {"size": I(1), "storage_class": NULL, "mtime": NULL}}
                                         for i in range(2)]}]}
            extra = {"shards/stray.parquet": write_part(stray, stray["parts"][0])}
        write_dataset(doc, out, extra_files=extra)
        records.append(rec(name, out, name, True, doc["description"]))

    # 3. native invalid datasets with their own rows
    native_faults = {
        "key-order-within-page": ({"codes": ["key_order"], "detect": "read", "requirements": ["KEY-3", "CHECK-2"]}, {}),
        "key-order-duplicate": ({"codes": ["key_order"], "detect": "read", "requirements": ["KEY-3", "CHECK-2"]}, {}),
        "key-order-across-pages": ({"codes": ["key_order"], "detect": "read", "requirements": ["KEY-3", "CHECK-2"]}, {}),
        "key-order-across-row-groups": ({"codes": ["key_order"], "detect": "read", "requirements": ["KEY-3", "CHECK-2"]}, {}),
        "key-order-duplicate-across-row-groups": ({"codes": ["key_order"], "detect": "read", "requirements": ["KEY-3", "CHECK-2"]}, {}),
        "invalid-key-empty": ({"codes": ["invalid_key", "bounds_mismatch"], "detect": "read",
                               "requirements": ["KEY-2", "CHECK-2"],
                               "note": "The manifest cannot express an empty min key, so it names the second key; "
                                       "a reader that compares first keys on open may report bounds_mismatch."}, {}),
        "missing-column-index-native": ({"codes": ["missing_page_index"], "detect": "read",
                                         "requirements": ["RG-2", "RG-7", "CHECK-2"]},
                                        {"expect_no_column_index_rg": 1}),
        "inconsistent-column-type": ({"codes": ["inconsistent_column"], "detect": "read", "requirements": ["COL-5", "CHECK-1"],
                                      "request": "range-columns:size",
                                      "note": "COL-5 requires the check no later than the first request that projects "
                                              "the column, so the read case projects size; structural validation checks all columns."}, {}),
        "inconsistent-column-repetition": ({"codes": ["inconsistent_column"], "detect": "read", "requirements": ["COL-5", "CHECK-1"],
                                            "request": "range-columns:size"}, {}),
        "key-order-across-parts": ({"codes": ["key_order", "bounds_mismatch"], "detect": "read",
                                    "requirements": ["KEY-3", "CHECK-2", "PART-3"],
                                    "note": "The manifest's max key for part 0 is not the actual last key, so "
                                            "bounds_mismatch is also a correct report."}, {}),
    }
    for name, (fault, wf) in native_faults.items():
        doc = restore_rows_doc(load_rows(name))
        out = DATASETS / "invalid" / name
        write_dataset(doc, out, fault=wf, check=True)
        records.append(rec(name, out, name, False, doc["description"], fault=fault))

    # 4. native variants of basic (same rows, different writer options or stamps)
    basic = restore_rows_doc(load_rows("basic"))
    for name, desc, opts in BASIC_VARIANTS:
        out = DATASETS / "invalid" / name
        write_dataset(basic, out, fault=opts, check=True)
        if name.startswith("stamp-"):
            fault = {"codes": ["stamp_mismatch"], "detect": "open", "requirements": ["STAMP-1", "STAMP-2", "CHECK-1"]}
        elif name.startswith("key-"):
            fault = {"codes": ["invalid_key_column"], "detect": "open", "requirements": ["KEY-1", "CHECK-1"]}
        elif name.startswith("sorting-"):
            fault = {"codes": ["missing_sorting_columns"], "detect": "open", "requirements": ["RG-1", "CHECK-1"]}
        else:
            fault = {"codes": ["unsupported_version"], "detect": "open", "requirements": ["MAN-4", "MAN-5", "VER-1"]}
        records.append(rec(name, out, "basic", False, desc, fault=fault))
    return records


if __name__ == "__main__":
    import json
    print(json.dumps(build(), indent=2))
