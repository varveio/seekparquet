#!/usr/bin/env python3
"""Full-scan semantic reference adapter for the SeekParquet conformance suite.

    python harness/run.py --adapter "python harness/reference_adapter.py"

THIS IS NOT A SEEKPARQUET READER AND MAKES NO READER CLAIM. It exists to test
the suite itself: it answers the runner protocol (protocol.md) by decoding every
row of every part with pyarrow and filtering in memory, never using the page
index to choose what to read. It performs the open-time checks it can (manifest
rules, sizes, footers, stamps, key column shape, column consistency,
sorting_columns) and, on the first read or validation of a dataset, the
read-time checks over the whole dataset (page index consistency with the
physical pages, key order, empty and non-UTF-8 keys, page bounds, Parquet
decoding), using generators/parquet_meta.py for the metadata. Because it checks
everything on first read, it detects faults earlier than an index-guided reader
must, which the protocol allows.

It shares no query code with oracle.py, so agreement between the two is a check
on the expected answers. It needs pyarrow and jsonschema
(generators/requirements.txt).
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONF = HERE.parent
sys.path.insert(0, str(CONF / "generators"))

import jsonschema  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

import parquet_meta as pm  # noqa: E402

SCHEMA = json.loads((CONF.parent / "spec" / "manifest.schema.json").read_text(encoding="utf-8"))
SUPPORTED = [(0, 1)]
MAX_INT = (1 << 63) - 1
LEVELS = ["structural", "profile", "full"]


class Refusal(Exception):
    def __init__(self, code, msg=""):
        super().__init__(f"{code}: {msg}")
        self.code = code


class BadRequest(Exception):
    pass


class NotSupported(Exception):
    def __init__(self, code, msg=""):
        super().__init__(msg)
        self.code = code


# --------------------------------------------------------------------------
# Manifest (section 3)

class Num:
    """A JSON number written with a fraction or exponent (MAN-6 forbids it for integers)."""

    def __init__(self, text):
        self.text = text


def _pairs(pairs):
    seen = {}
    for k, v in pairs:
        if k in seen:
            raise Refusal("invalid_manifest", f"duplicate member {k!r}")
        seen[k] = v
    return seen


def _walk_strings(x):
    if isinstance(x, str):
        yield x
    elif isinstance(x, dict):
        for k, v in x.items():
            yield k
            yield from _walk_strings(v)
    elif isinstance(x, list):
        for v in x:
            yield from _walk_strings(v)


def _is_int(x):
    return isinstance(x, int) and not isinstance(x, bool)


def canonical_b64(s):
    if not isinstance(s, str):
        raise ValueError("not a string")
    b = base64.b64decode(s, validate=True)
    if base64.b64encode(b).decode("ascii") != s:
        raise ValueError("not canonical")
    return b


def path_ok(p):
    if not isinstance(p, str) or not p or p.startswith("/"):
        return False
    for seg in p.split("/"):
        if seg in ("", ".", "..") or "\\" in seg or any(ord(c) < 0x20 or ord(c) == 0x7F for c in seg):
            return False
    return True


def load_manifest(root: Path):
    mp = root / "manifest.json"
    if not mp.is_file():
        raise Refusal("no_manifest")
    raw = mp.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raise Refusal("invalid_manifest", "byte order mark")
    try:
        text = raw.decode("utf-8")
        m = json.loads(text, object_pairs_hook=_pairs, parse_float=Num,
                       parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)))
    except Refusal:
        raise
    except ValueError as e:
        raise Refusal("invalid_manifest", f"not JSON: {e}") from None
    if not isinstance(m, dict):
        raise Refusal("invalid_manifest", "not an object")
    for s in _walk_strings(m):
        try:
            s.encode("utf-8")
        except UnicodeEncodeError:
            raise Refusal("invalid_manifest", "unpaired surrogate") from None
    # MAN-7: the version members are checked for well-formedness first, then for support.
    fv, dr = m.get("format_version"), m.get("draft_revision", None)
    if m.get("format") != "seekparquet":
        raise Refusal("invalid_manifest", "format")
    if not _is_int(fv) or not 0 <= fv <= MAX_INT:
        raise Refusal("invalid_manifest", "format_version is not an integer in range (MAN-6, MAN-7)")
    if fv == 0:
        if not _is_int(dr) or not 1 <= dr <= MAX_INT:
            raise Refusal("invalid_manifest", "format_version 0 needs a draft_revision in 1..2^63-1")
    elif "draft_revision" in m:
        raise Refusal("invalid_manifest", "draft_revision is only allowed with format_version 0")
    if (fv, dr) not in SUPPORTED:
        raise Refusal("unsupported_version", f"({fv}, {dr})")
    # MAN-6 applies to the defined integer members only: a number spelled with a fraction or exponent is a
    # Num, which the schema's "integer" type rejects there; numbers inside extensions are ordinary JSON.
    errs = list(jsonschema.Draft202012Validator(SCHEMA).iter_errors(m))
    if errs:
        raise Refusal("invalid_manifest", errs[0].message)
    paths = set()
    bounds = []
    for p in m["parts"]:
        if not path_ok(p["path"]) or p["path"] in paths:
            raise Refusal("invalid_manifest", f"path {p['path']!r}")
        paths.add(p["path"])
        try:
            lo, hi = canonical_b64(p["min_key_base64"]), canonical_b64(p["max_key_base64"])
        except (ValueError, binascii.Error):
            raise Refusal("invalid_manifest", "base64") from None
        if not lo or not hi:
            raise Refusal("invalid_manifest", "empty key")
        bounds.append((lo, hi))
    for i, (lo, hi) in enumerate(bounds):
        if lo > hi or (i and bounds[i - 1][1] >= lo):
            raise Refusal("invalid_manifest", f"part bounds not ascending and disjoint at part {i} (PART-4)")
    return m, bounds


# --------------------------------------------------------------------------
# Parts

def top_level(pf):
    """[(name, SchemaElement, signature, leaf indexes)] for each top-level field."""
    sch = pf.schema
    out = []
    idx = 1
    leaf = 0

    def sig(i):
        nonlocal leaf
        el = sch[i]
        kids = el.get(pm.SE_NUM_CHILDREN) or 0
        parts = [el.get(pm.SE_NAME), el.get(pm.SE_REPETITION, 0)]
        j = i + 1
        leaves = []
        if kids:
            sub = []
            for _ in range(kids):
                s, j, lv = sig(j)
                sub.append(s)
                leaves += lv
            parts.append(tuple(sub))
        else:
            parts += [el.get(pm.SE_TYPE), el.get(pm.SE_TYPE_LENGTH)]
            leaves = [leaf]
            leaf += 1
        return tuple(parts), j, leaves

    for _ in range(sch[0].get(pm.SE_NUM_CHILDREN, 0)):
        s, nxt, leaves = sig(idx)
        out.append((sch[idx].get(pm.SE_NAME).decode("utf-8", "replace"), sch[idx], s, leaves))
        idx = nxt
    return out


def part_path(root, rel):
    """The file of a manifest part path, confined to the dataset root (PART-1)."""
    full = (root / rel).resolve()
    if os.path.commonpath([str(full), str(root.resolve())]) != str(root.resolve()):
        raise Refusal("invalid_manifest", "path outside root")
    if not full.is_file():
        raise Refusal("missing_part", rel)
    return full


@contextmanager
def decoding():
    """Arrow decoding: a feature Arrow does not implement is unsupported (CHECK-5, VAL-1); other failures are
    invalid_parquet."""
    try:
        yield
    except pa.ArrowNotImplementedError as e:
        raise NotSupported("unsupported_encoding", str(e)) from None
    except (OSError, pa.ArrowException) as e:
        raise Refusal("invalid_parquet", str(e)) from None


class Part:
    def __init__(self, root, entry, manifest):
        self.entry = entry
        self.key_name = manifest["key"]["column"]
        self.unique = manifest["key"]["unique"]
        rel = entry["path"]
        self.path = part_path(root, rel)
        self.data = self.path.read_bytes()
        if len(self.data) != entry["bytes"]:
            raise Refusal("part_mismatch", f"{rel}: size {len(self.data)} != {entry['bytes']}")
        try:
            self.pf = pm.read_parquet(self.data)
            self.fields = top_level(self.pf)
        except (pm.ParquetMetaError, pm.ThriftError, AttributeError, IndexError, TypeError) as e:
            raise Refusal("invalid_footer", f"{rel}: {e}") from None
        for rg in self.pf.row_groups:
            for cc in rg.items_of(pm.RG_COLUMNS):
                if cc.has(pm.CC_FILE_PATH):
                    raise Refusal("invalid_footer", "file_path set (FILE-1)")
        rows = sum(rg.get(pm.RG_NUM_ROWS, 0) for rg in self.pf.row_groups)
        num_rows = self.pf.meta.get(pm.FMD_NUM_ROWS)
        if rows != num_rows:
            raise Refusal("invalid_footer", f"{rel}: row groups hold {rows} rows, footer says {num_rows}")
        if num_rows != entry["rows"]:
            raise Refusal("part_mismatch", f"{rel}: rows {num_rows} != {entry['rows']}")
        self.check_stamps(manifest)
        self.check_key()
        for ri, rg in enumerate(self.pf.row_groups):
            if not rg.get(pm.RG_NUM_ROWS):
                continue
            sc = rg.get(pm.RG_SORTING)
            if sc is None or not sc.items or sc.items[0].get(pm.SC_COLUMN_IDX) != self.key_leaf \
                    or sc.items[0].get(pm.SC_DESCENDING):
                raise Refusal("missing_sorting_columns", f"{rel} row group {ri}")

    def check_stamps(self, m):
        want = {"seekparquet.format_version": str(m["format_version"]), "seekparquet.schema_id": m["schema_id"],
                "seekparquet.key_column": m["key"]["column"], "seekparquet.key_unique": str(m["key"]["unique"]).lower()}
        if m["format_version"] == 0:
            want["seekparquet.draft_revision"] = str(m["draft_revision"])
        got = {}
        for e in self.pf.meta.items_of(pm.FMD_KV):
            k = e.get(pm.KV_KEY, b"")
            if k.startswith(b"seekparquet."):
                ks = k.decode("utf-8", "replace")
                if ks in got:
                    raise Refusal("stamp_mismatch", f"duplicate {ks}")
                v = e.get(pm.KV_VALUE)
                got[ks] = None if v is None else v.decode("utf-8", "replace")
        for k, v in want.items():
            if got.get(k) != v:
                raise Refusal("stamp_mismatch", f"{k}: {got.get(k)!r} != {v!r}")
        if m["format_version"] != 0 and "seekparquet.draft_revision" in got:
            raise Refusal("stamp_mismatch", "draft_revision on a non-draft file")

    def check_key(self):
        f = next((x for x in self.fields if x[0] == self.key_name), None)
        if f is None:
            raise Refusal("invalid_key_column", "no key column")
        _n, el, _sig, leaves = f
        if el.get(pm.SE_NUM_CHILDREN) or el.get(pm.SE_TYPE) != 6 or el.get(pm.SE_REPETITION, 0) != 0:
            raise Refusal("invalid_key_column", "key is not a REQUIRED BYTE_ARRAY leaf")
        ct, lt = el.get(6), el.get(10)
        lt_kinds = [fid for fid, _t, _v in lt.fields] if lt is not None else []
        if ct not in (None, 0) or lt_kinds not in ([], [1]):
            raise Refusal("invalid_key_column", "key annotation other than STRING/UTF8")
        self.utf8 = ct == 0 or lt_kinds == [1]
        self.key_leaf = leaves[0]
        orders = self.pf.meta.get(7)
        nleaves = len(self.pf.leaf_columns())
        if orders is None or len(orders.items) != nleaves or not orders.items[self.key_leaf].has(1):
            raise Refusal("invalid_key_column", "column_orders missing or not TYPE_ORDER for key")

    # --- read-time checks (CHECK-2), done over the whole part --------------
    def index_checks(self, ri):
        """RG-2 to RG-4, including the header-based parts of RG-3. Returns page row intervals."""
        rg = self.pf.row_groups[ri]
        n = rg.get(pm.RG_NUM_ROWS)
        cc = rg.items_of(pm.RG_COLUMNS)[self.key_leaf]
        try:
            ci = pm.read_index(self.pf, cc, "column")
            oi = pm.read_index(self.pf, cc, "offset")
        except pm.ParquetMetaError as e:
            raise Refusal("invalid_page_index", str(e)) from None
        if ci is None or oi is None:
            raise Refusal("missing_page_index", f"row group {ri}")
        locs = oi.items_of(pm.OI_PAGE_LOCATIONS)
        if not locs:
            raise Refusal("invalid_page_index", "empty OffsetIndex")
        firsts = [x.get(pm.PL_FIRST_ROW) for x in locs]
        if firsts[0] != 0 or any(b <= a for a, b in zip(firsts, firsts[1:])) or firsts[-1] >= n:
            raise Refusal("invalid_page_index", f"first_row_index {firsts}")
        md = cc.get(pm.CC_META)
        start = md.get(pm.CMD_DICT_PAGE_OFFSET) or md.get(pm.CMD_DATA_PAGE_OFFSET)
        start = min(start, md.get(pm.CMD_DATA_PAGE_OFFSET))
        end = start + md.get(pm.CMD_TOTAL_COMPRESSED)
        for x in locs:
            off, size = x.get(pm.PL_OFFSET), x.get(pm.PL_SIZE)
            if off is None or size is None or off < start or size <= 0 or off + size > end or end > self.pf.footer_start:
                raise Refusal("invalid_page_index", "page location outside the column chunk or file")
        try:
            pages = pm.data_pages(self.pf, cc)
        except pm.ParquetMetaError as e:
            raise Refusal("invalid_parquet", str(e)) from None
        if [(p.offset, p.header_len + p.compressed_size) for p in pages] != \
                [(x.get(pm.PL_OFFSET), x.get(pm.PL_SIZE)) for x in locs]:
            raise Refusal("invalid_page_index", "OffsetIndex does not locate exactly the data pages")
        intervals = [(a, b) for a, b in zip(firsts, firsts[1:] + [n])]
        for p, (a, b) in zip(pages, intervals):
            rows = p.num_rows if p.num_rows is not None else p.num_values
            if rows != b - a:
                raise Refusal("invalid_page_index", f"page holds {rows} rows, index says {b - a}")
        nulls, mins, maxs = (ci.items_of(f) for f in (pm.CI_NULL_PAGES, pm.CI_MIN, pm.CI_MAX))
        if not (len(nulls) == len(mins) == len(maxs) == len(locs)):
            raise Refusal("invalid_page_index", "ColumnIndex list lengths")
        if any(nulls):
            raise Refusal("invalid_page_index", "null page")
        return intervals, mins, maxs

    def key_checks(self, ri, intervals, mins, maxs, prev):
        """KEY-2, KEY-3, KEY-6, RG-5 over the decoded keys of one row group. Returns the last key."""
        with decoding():
            col = pq.ParquetFile(self.path).read_row_group(ri, columns=[self.key_name]).column(0).combine_chunks()
        if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
            col = col.view(pa.binary() if pa.types.is_string(col.type) else pa.large_binary())
        keys = col.to_pylist()
        for (a, b), lo, hi in zip(intervals, mins, maxs):
            if self.utf8:
                for bound in (lo, hi):
                    try:
                        bound.decode("utf-8")
                    except UnicodeDecodeError:
                        raise Refusal("invalid_page_index", "bound is not UTF-8 (KEY-6)") from None
            for k in keys[a:b]:
                if not k:
                    raise Refusal("invalid_key", "empty key")
                if self.utf8:
                    try:
                        k.decode("utf-8")
                    except UnicodeDecodeError:
                        raise Refusal("invalid_key", "key is not UTF-8 (KEY-6)") from None
                if prev is not None and (k < prev or self.unique and k == prev):
                    raise Refusal("key_order", f"{k!r} after {prev!r}")
                prev = k
            if keys[a:b] and (lo > keys[a] or hi < keys[b - 1]):
                raise Refusal("invalid_page_index", "keys outside page bounds (RG-5)")
        return prev, keys


class Dataset:
    def __init__(self, root):
        self.root = Path(root)
        self.state = None  # Refusal or None after open
        self.read_state = None
        self.rows = None
        self.manifest, self.bounds = load_manifest(self.root)
        self.parts = [Part(self.root, e, self.manifest) for e in self.manifest["parts"]]
        sigs = {}
        for p in self.parts:
            for name, _el, sig, _l in p.fields:
                if name in sigs and sigs[name] != sig[1:]:
                    raise Refusal("inconsistent_column", name)
                sigs[name] = sig[1:]
        self.nested = {}
        for p in self.parts:
            for name, el, _sig, _l in p.fields:
                self.nested[name] = bool(el.get(pm.SE_NUM_CHILDREN)) or el.get(pm.SE_REPETITION, 0) == 2

    def structural(self):
        for p in self.parts:
            for ri, rg in enumerate(p.pf.row_groups):
                if rg.get(pm.RG_NUM_ROWS):
                    p.index_checks(ri)

    def profile(self):
        """Read every key page in dataset order (also used to serve queries)."""
        prev = None
        all_rows = []
        for pi, p in enumerate(self.parts):
            part_keys = []
            for ri, rg in enumerate(p.pf.row_groups):
                if not rg.get(pm.RG_NUM_ROWS):
                    continue
                intervals, mins, maxs = p.index_checks(ri)
                prev, keys = p.key_checks(ri, intervals, mins, maxs, prev)
                part_keys += keys
            all_rows.append(part_keys)
        return all_rows

    def first_last(self, all_rows):
        for (lo, hi), keys in zip(self.bounds, all_rows):
            if keys[0] != lo or keys[-1] != hi:
                raise Refusal("bounds_mismatch", "manifest bounds differ from the part's keys")

    def full(self):
        tables = []
        for p in self.parts:
            with decoding():
                t = pq.read_table(p.path)
                for c in t.columns:
                    c.combine_chunks()
            if any(len(c) != p.entry["rows"] for c in t.columns):
                raise Refusal("invalid_parquet", "column row count")
            tables.append(t)
        return tables

    def load(self):
        """Read-time checks over everything, then the rows (cached)."""
        if self.read_state is not None:
            raise self.read_state
        if self.rows is not None:
            return self.rows
        try:
            self.profile()
            tables = self.full()
        except Refusal as r:
            self.read_state = r
            raise
        self.tables = tables
        self.colcache = {}
        rows = []
        for pi, t in enumerate(tables):
            keys = t.column(self.manifest["key"]["column"]).combine_chunks()
            if pa.types.is_string(keys.type) or pa.types.is_large_string(keys.type):
                keys = keys.view(pa.binary() if pa.types.is_string(keys.type) else pa.large_binary())
            for ri, k in enumerate(keys.to_pylist()):
                rows.append((k, (pi, ri)))
        previous, offset = None, 0
        positioned = []
        for key, pos in rows:
            offset = offset + 1 if key == previous else 0
            previous = key
            positioned.append((key, offset, pos))
        self.rows = positioned
        return positioned

    def value(self, key, pos, col):
        """The protocol value of one projected column, converted lazily per (part, column)."""
        pi, ri = pos
        if col == self.manifest["key"]["column"]:
            return {"bytes_base64": base64.b64encode(key).decode("ascii")}
        p = self.parts[pi]
        field = next((f for f in p.fields if f[0] == col), None)
        if field is None:
            return {"absent": True}  # COL-2 / COL-7
        vals = self.colcache.get((pi, col))
        if vals is None:
            el = field[1]
            vals = to_protocol(self.tables[pi].column(col).combine_chunks(), el.get(pm.SE_TYPE),
                               el.get(pm.SE_TYPE_LENGTH))
            self.colcache[(pi, col)] = vals
        return vals[ri]


def to_protocol(arr, ptype, type_length=None):
    """Physical Parquet values as protocol values (protocol.md, Encoding).

    Arrow presents annotated columns as logical values (dates, timestamps, decimals, unsigned integers);
    they are mapped back to the footer's physical type here.
    """
    from decimal import Context
    t = arr.type
    if ptype == 4:
        bits = arr.view(pa.uint32()).to_pylist()
        return [{"null": True} if b is None else {"float32": f"{b:08x}"} for b in bits]
    if ptype == 5:
        bits = arr.view(pa.uint64()).to_pylist()
        return [{"null": True} if b is None else {"float64": f"{b:016x}"} for b in bits]
    if ptype in (1, 2) and (pa.types.is_temporal(t)):
        arr = arr.view(pa.int32() if t.bit_width == 32 else pa.int64())
    out = []
    dec = pa.types.is_decimal(t)
    scale = t.scale if dec else 0
    ctx = Context(prec=100)
    for x in arr.to_pylist():
        if x is None:
            out.append({"null": True})
        elif ptype == 0:
            out.append({"bool": bool(x)})
        elif ptype in (1, 2):
            if dec:
                x = int(x.scaleb(scale, ctx))
            w = 32 if ptype == 1 else 64
            x %= 1 << w
            out.append({"int": str(x - (1 << w) if x >= 1 << (w - 1) else x)})
        elif ptype in (6, 7):
            if dec:
                u = int(x.scaleb(scale, ctx))
                if ptype == 7:
                    b = u.to_bytes(type_length, "big", signed=True)
                else:
                    b = u.to_bytes(max(1, (u.bit_length() + 8) // 8), "big", signed=True)
            else:
                b = x.encode("utf-8") if isinstance(x, str) else bytes(x)
            out.append({"bytes_base64": base64.b64encode(b).decode("ascii")})
        else:
            raise NotSupported("unsupported_encoding", f"physical type {ptype}")
    return out


# --------------------------------------------------------------------------
# Requests

DATASETS = {}


def get_dataset(path):
    d = DATASETS.get(path)
    if d is None:
        try:
            d = Dataset(path)
        except Refusal as r:
            d = r
        DATASETS[path] = d
    if isinstance(d, Refusal):
        raise d
    return d


def b64arg(req, name, required=False):
    v = req.get(name)
    if v is None:
        if required:
            raise BadRequest(f"{name} is required")
        return None
    try:
        return canonical_b64(v)
    except (ValueError, binascii.Error):
        raise BadRequest(f"{name} is not canonical base64") from None


def parse_request(line):
    """A request line. Number spellings are kept where they matter: -0, fractions and exponents are never integers."""
    return json.loads(line, parse_float=Num, parse_constant=Num, parse_int=lambda s: Num(s) if s == "-0" else int(s))


def cursor_arg(req):
    """PAGE-1: None, ("row", key, offset), or ("prefix", bytes, None), which only list accepts."""
    if "after" not in req:
        return None
    a = req["after"]
    if not isinstance(a, dict):
        raise BadRequest("cursor must be an object")
    if set(a) == {"prefix_base64"}:
        if req.get("op") != "list":
            raise BadRequest("a prefix cursor is valid only for list")
        return ("prefix", b64arg(a, "prefix_base64", True), None)
    if set(a) == {"key_base64", "offset"} and _is_int(a["offset"]) and 0 <= a["offset"] <= MAX_INT:
        return ("row", b64arg(a, "key_base64", True), a["offset"])
    raise BadRequest("malformed cursor")


def past_cursor(cursor, key, offset):
    if cursor is None:
        return True
    tag, c, n = cursor
    if tag == "prefix":
        return key > c and not key.startswith(c)
    return key > c or key == c and offset > n


def common_args(req):
    cursor_arg(req)
    limit = req.get("limit")
    if not _is_int(limit) or limit < 1:
        raise BadRequest("limit must be a positive integer")
    cols = req.get("columns", [])
    if not isinstance(cols, list) or not all(isinstance(c, str) for c in cols):
        raise BadRequest("columns must be an array of strings")
    if "start_exclusive" in req and not isinstance(req["start_exclusive"], bool):
        raise BadRequest("start_exclusive must be a boolean")
    return limit, cols


def project(ds, key, pos, cols):
    return [ds.value(key, pos, c) for c in cols]


def do_range(ds, req):
    limit, cols = common_args(req)
    prefix = b64arg(req, "prefix_base64")
    start, end = b64arg(req, "start_base64"), b64arg(req, "end_base64")
    excl = req.get("start_exclusive", False)
    for c in cols:
        if ds.nested.get(c):
            raise NotSupported("unsupported_projection", c)
    rows = ds.load()
    entries = []
    cursor = cursor_arg(req)
    for k, offset, v in rows:
        if len(entries) >= limit:
            break
        if not past_cursor(cursor, k, offset):
            continue
        if start is not None and (k < start or (excl and k == start)):
            continue
        if end is not None and k >= end:
            break
        if prefix is not None and not k.startswith(prefix):
            continue
        entries.append({"kind": "row", "key_base64": base64.b64encode(k).decode(), "offset": offset, "values": project(ds, k, v, cols)})
    return entries


def do_list(ds, req):
    limit, cols = common_args(req)
    prefix = b64arg(req, "prefix_base64") or b""
    d = b64arg(req, "delimiter_base64", required=True)
    if len(d) != 1:
        raise BadRequest("delimiter must be one byte")
    end = b64arg(req, "end_base64")
    for c in cols:
        if ds.nested.get(c):
            raise NotSupported("unsupported_projection", c)
    rows = ds.load()
    entries = []
    last_prefix = None
    cursor = cursor_arg(req)
    for k, offset, v in rows:
        if len(entries) >= limit:
            break
        if k[: len(prefix)] != prefix or (end is not None and k >= end) or not past_cursor(cursor, k, offset):
            continue
        rest = k[len(prefix):]
        if d in rest:
            cp = k[: len(prefix) + rest.index(d) + 1]
            if cp == last_prefix:
                continue
            last_prefix = cp
            entries.append({"kind": "prefix", "key_base64": base64.b64encode(cp).decode()})
        else:
            entries.append({"kind": "row", "key_base64": base64.b64encode(k).decode(), "offset": offset, "values": project(ds, k, v, cols)})
    return entries


def do_digest(path):
    """VAL-2: hash the manifest-listed bytes of each part that declares sha256, without opening any part as
    Parquet (digest has no prerequisite level)."""
    root = Path(path)
    checked = 0
    try:
        manifest, _bounds = load_manifest(root)
        for e in manifest["parts"]:
            if "sha256" in e:
                checked += 1
                if hashlib.sha256(part_path(root, e["path"]).read_bytes()).hexdigest() != e["sha256"]:
                    return {"digest": {"result": "failed", "code": "digest_mismatch", "checked": checked}}
    except Refusal as r:
        return {"digest": {"result": "incomplete", "reason": f"the part files cannot be identified ({r.code})"}}
    return {"digest": {"result": "complete", "checked": checked}}


def do_validate(path, level):
    """VAL-1: each level completes, fails with a code, or is incomplete with a reason; a level whose
    prerequisite did not complete is incomplete, and lower levels keep their results."""
    if level == "digest":
        return do_digest(path)
    out = {}
    blocked = False
    for lvl in LEVELS[: LEVELS.index(level) + 1]:
        if blocked:
            out[lvl] = {"result": "incomplete", "reason": "a prerequisite level did not complete"}
            continue
        try:
            if lvl == "structural":
                ds = get_dataset(path)
                ds.structural()
            elif lvl == "profile":
                ds.first_last(ds.profile())
            else:
                ds.full()
            out[lvl] = {"result": "complete"}
        except Refusal as r:
            out[lvl] = {"result": "failed", "code": r.code}
            blocked = True
        except NotSupported as u:
            out[lvl] = {"result": "incomplete", "reason": f"unsupported ({u.code}): {u}"}
            blocked = True
    return out


def handle(req):
    op = req.get("op")
    if op == "hello":
        return {"status": "ok", "implementation": "seekparquet-conformance-reference (full scan, not a reader)",
                "version": "0.1.0", "supports": [{"format_version": f, "draft_revision": d} for f, d in SUPPORTED],
                "targets": ["reader", "validator-structural", "validator-profile", "validator-full",
                            "validator-digest"],
                "capabilities": ["open", "range", "list", "projection", "validate:structural", "validate:profile",
                                 "validate:full", "validate:digest"]}
    path = req.get("dataset")
    if not isinstance(path, str):
        return {"status": "invalid_request", "message": "dataset is required"}
    try:
        if op == "open":
            get_dataset(path)
            return {"status": "ok"}
        if op == "range":
            common_args(req)
            b64arg(req, "prefix_base64"), b64arg(req, "start_base64"), b64arg(req, "end_base64")
            return {"status": "ok", "entries": do_range(get_dataset(path), req)}
        if op == "list":
            common_args(req)
            for n in ("prefix_base64", "end_base64"):
                b64arg(req, n)
            dl = b64arg(req, "delimiter_base64", required=True)
            if len(dl) != 1:
                raise BadRequest("delimiter must be one byte")
            return {"status": "ok", "entries": do_list(get_dataset(path), req)}
        if op == "validate":
            if req.get("level") not in LEVELS + ["digest"]:
                raise BadRequest("level")
            return {"status": "ok", "levels": do_validate(path, req["level"])}
        return {"status": "invalid_request", "message": f"unknown op {op!r}"}
    except BadRequest as e:
        return {"status": "invalid_request", "message": str(e)}
    except Refusal as r:
        return {"status": "refused", "code": r.code, "message": str(r)}
    except NotSupported as u:
        return {"status": "unsupported", "code": u.code, "message": str(u)}


def main():
    for line in sys.stdin.buffer:
        try:
            req = parse_request(line)
        except ValueError:
            continue
        try:
            resp = handle(req)
        except Exception as e:  # noqa: BLE001
            resp = {"status": "error", "message": f"{type(e).__name__}: {e}"}
        resp = {"id": req.get("id"), **resp}
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
