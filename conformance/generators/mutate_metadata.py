#!/usr/bin/env python3
"""Derive fixtures from the pyarrow datasets by editing metadata only.

Every derived fixture starts from a valid dataset written by
``pyarrow_fixtures.py``. A mutation edits the manifest, or the Parquet footer
and page index through ``parquet_meta.py``. Data pages are preserved byte for
byte: a rewritten part keeps bytes [0, start of the page index) unchanged and
re-serializes the page indexes and footer after them, recomputing index
offsets and lengths, and the manifest's ``bytes`` (and ``sha256`` when
present) are updated to the new file. Each change is recorded in
``fixtures.lock.json`` and ``STRUCTURE.txt``.

Most derived fixtures are invalid (one fault each, with its expected refusal).
A few are valid ("mutated positive" fixtures), for example conservatively
shortened page bounds, which pyarrow never writes.

Run through ``generators/regenerate.py``.
"""

from __future__ import annotations

import copy
import json
import shutil

import parquet_meta as pm
from common import CONF, DATASETS, b64d, b64e, dumps, sha256, write_text

INVALID = DATASETS / "invalid"
COMMAND = "python generators/regenerate.py"
PRODUCER = "generators/mutate_metadata.py"


def fmt_bytes(b):
    if b is None:
        return "None"
    if len(b) > 24:
        return f"{b[:12].hex()}..({len(b)} bytes)"
    return b.hex() if any(c < 0x20 or c > 0x7E for c in b) else repr(b.decode("ascii"))


class Part:
    """A part file loaded for editing."""

    def __init__(self, data):
        self.pf = pm.read_parquet(data)
        self.meta = copy.deepcopy(self.pf.meta)
        self.idx = pm.load_indexes(self.pf)
        self.changes = []

    def key_ci(self, rg):
        return self.idx[(rg, 0)][0]

    def key_oi(self, rg):
        return self.idx[(rg, 0)][1]

    def build(self):
        return pm.rebuild(self.pf, self.meta, self.idx)


def read_manifest(src):
    return json.loads((src / "manifest.json").read_text(encoding="utf-8"))


def derive(name, src_name, desc, fault, *, parts=None, manifest=None, raw_manifest=None, valid=False,
           drop_manifest=False, out=None, raw=None):
    """Copy dataset ``src_name`` to ``out`` applying part and manifest edits.

    parts: {part path: function(Part) -> None} editing a part in place (append to Part.changes).
    manifest: function(dict, changes) -> None editing the manifest after sizes are updated.
    raw_manifest: function(text) -> (text, [changes]) producing manifest bytes directly.
    raw: {part path: function(bytes) -> (bytes, [changes])}, a same-size byte edit applied last.
    """
    src = DATASETS / src_name
    out = out or (INVALID / name)
    if out.exists():
        shutil.rmtree(out)
    m = read_manifest(src)
    changes = []
    out.mkdir(parents=True)
    for p in m["parts"]:
        data = (src / p["path"]).read_bytes()
        if parts and p["path"] in parts:
            part = Part(data)
            parts[p["path"]](part)
            new = part.build()
            pm.read_parquet(new)  # still a parseable file unless the footer itself was mangled later
            changes += [f"{p['path']}: {c}" for c in part.changes]
            if not part.changes:
                raise AssertionError("mutation recorded no change")
            changes.append(f"{p['path']}: data pages preserved; page indexes and footer re-serialized, "
                           f"file size {len(data)} -> {len(new)}")
            if len(new) != p["bytes"]:
                changes.append(f"manifest: parts[{m['parts'].index(p)}].bytes {p['bytes']} -> {len(new)}")
                p["bytes"] = len(new)
            if "sha256" in p:
                p["sha256"] = sha256(new)
            data = new
        if raw and p["path"] in raw:
            new, ch = raw[p["path"]](data)
            assert len(new) == len(data)
            changes += [f"{p['path']}: {c}" for c in ch]
            if "sha256" in p:
                p["sha256"] = sha256(new)
            data = new
        dst = out / p["path"]
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(data)
    if manifest:
        manifest(m, changes)
    text = dumps(m)
    if raw_manifest:
        text, ch = raw_manifest(text)
        changes += ch
    if drop_manifest:
        changes.append("manifest.json removed")
    else:
        (out / "manifest.json").write_bytes(text.encode("utf-8") if isinstance(text, str) else text)
    r = {"name": name, "dir": str(out.relative_to(CONF)), "valid": valid, "description": desc,
         "rows": None, "producer": PRODUCER, "provenance": "mutated", "command": COMMAND,
         "derived_from": f"datasets/{src_name}", "changes": changes}
    if fault:
        r["fault"] = fault
    return r


def F(code, detect, reqs, request=None, codes=None, note=None):
    f = {"codes": codes or [code], "detect": detect, "requirements": reqs}
    if request:
        f["request"] = request
    if note:
        f["note"] = note
    return f


# --------------------------------------------------------------------------
# Manifest edits

def set_member(path, value):
    def fn(m, changes):
        obj = m
        for k in path[:-1]:
            obj = obj[k]
        old = obj.get(path[-1], "<absent>") if isinstance(obj, dict) else obj[path[-1]]
        obj[path[-1]] = value
        changes.append(f"manifest: {'.'.join(map(str, path))} {json.dumps(old)} -> {json.dumps(value)}")
    return fn


def del_member(path):
    def fn(m, changes):
        obj = m
        for k in path[:-1]:
            obj = obj[k]
        del obj[path[-1]]
        changes.append(f"manifest: {'.'.join(map(str, path))} removed")
    return fn


def text_replace(old, new, count=1):
    def fn(text):
        assert old in text, old
        return text.replace(old, new, count), [f"manifest text: {old!r} -> {new!r}"]
    return fn


def add_part_copy(path, min_key, max_key):
    def fn(m, changes):
        p = dict(m["parts"][0])
        p.update(path=path, min_key_base64=b64e(min_key), max_key_base64=b64e(max_key))
        m["parts"].append(p)
        changes.append(f"manifest: appended parts[{len(m['parts']) - 1}] path {path!r} with bounds "
                       f"{fmt_bytes(min_key)}..{fmt_bytes(max_key)}")
    return fn


# --------------------------------------------------------------------------
# Part edits

def remove_index(rg, which):
    def fn(part):
        pos = 0 if which == "column" else 1
        assert part.idx[(rg, 0)][pos] is not None
        part.idx[(rg, 0)][pos] = None
        part.changes.append(f"row group {rg} key chunk: {which} index removed "
                            f"({'column_index' if pos == 0 else 'offset_index'}_offset/_length dropped)")
    return fn


def remove_value_indexes(part):
    for (rg, col), pair in part.idx.items():
        if col != 0 and (pair[0] is not None or pair[1] is not None):
            pair[0] = pair[1] = None
            part.changes.append(f"row group {rg} column {col}: column index and offset index removed")


def null_page(rg, page):
    def fn(part):
        ci = part.key_ci(rg)
        ci.get(pm.CI_NULL_PAGES).items[page] = True
        old_min, old_max = ci.get(pm.CI_MIN).items[page], ci.get(pm.CI_MAX).items[page]
        ci.get(pm.CI_MIN).items[page] = b""
        ci.get(pm.CI_MAX).items[page] = b""
        part.changes.append(f"row group {rg} key ColumnIndex: null_pages[{page}] false -> true, "
                            f"min {fmt_bytes(old_min)} -> '', max {fmt_bytes(old_max)} -> ''")
    return fn


def drop_last_ci_entry(rg):
    def fn(part):
        ci = part.key_ci(rg)
        n = len(ci.get(pm.CI_MIN).items)
        for fid, _t, v in ci.fields:
            if isinstance(v, pm.TList) and len(v.items) == n:
                v.items.pop()
            elif isinstance(v, pm.TList) and fid in (6, 7):  # level histograms: one run per page
                per = len(v.items) // n
                del v.items[-per:]
        part.changes.append(f"row group {rg} key ColumnIndex: last page entry removed ({n} -> {n - 1} pages; "
                            f"OffsetIndex still lists {n})")
    return fn


def set_first_row(rg, page, value):
    def fn(part):
        loc = part.key_oi(rg).get(pm.OI_PAGE_LOCATIONS).items[page]
        old = loc.get(pm.PL_FIRST_ROW)
        loc.set(pm.PL_FIRST_ROW, pm.CT_I64, value)
        part.changes.append(f"row group {rg} key OffsetIndex: page_locations[{page}].first_row_index {old} -> {value}")
    return fn


def set_location_offset(rg, page, value_fn):
    def fn(part):
        loc = part.key_oi(rg).get(pm.OI_PAGE_LOCATIONS).items[page]
        old = loc.get(pm.PL_OFFSET)
        new = value_fn(part)
        loc.set(pm.PL_OFFSET, pm.CT_I64, new)
        part.changes.append(f"row group {rg} key OffsetIndex: page_locations[{page}].offset {old} -> {new}")
    return fn


def set_bound(rg, page, which, fn_value, why):
    def fn(part):
        ci = part.key_ci(rg)
        lst = ci.get(pm.CI_MIN if which == "min" else pm.CI_MAX).items
        old = lst[page]
        lst[page] = fn_value(old)
        part.changes.append(f"row group {rg} key ColumnIndex: {which}_values[{page}] {fmt_bytes(old)} -> "
                            f"{fmt_bytes(lst[page])} ({why})")
    return fn


def remove_sorting(rg):
    def fn(part):
        g = part.meta.items_of(pm.FMD_ROW_GROUPS)[rg]
        assert g.has(pm.RG_SORTING)
        g.remove(pm.RG_SORTING)
        part.changes.append(f"footer: row_groups[{rg}].sorting_columns removed")
    return fn


def set_num_rows(delta):
    def fn(part):
        old = part.meta.get(pm.FMD_NUM_ROWS)
        part.meta.set(pm.FMD_NUM_ROWS, pm.CT_I64, old + delta)
        part.changes.append(f"footer: FileMetaData.num_rows {old} -> {old + delta} "
                            f"(row groups still sum to {old})")
    return fn


def set_key_annotation(kind):
    def fn(part):
        el = part.meta.items_of(pm.FMD_SCHEMA)[1]
        assert el.get(pm.SE_NAME) == b"key"
        if kind == "json":
            el.set(6, pm.CT_I32, 19)  # ConvertedType JSON
            el.set(10, pm.CT_STRUCT, pm.Struct([(12, pm.CT_STRUCT, pm.Struct())]))  # LogicalType JSON
            what = "converted_type JSON and logicalType JSON added"
        elif kind == "string":
            el.set(6, pm.CT_I32, 0)  # ConvertedType UTF8
            el.set(10, pm.CT_STRUCT, pm.Struct([(1, pm.CT_STRUCT, pm.Struct())]))  # LogicalType STRING
            what = "converted_type UTF8 and logicalType STRING added"
        elif kind == "legacy-utf8":
            assert el.has(10)
            el.remove(10)
            what = "logicalType STRING removed; legacy converted_type UTF8 kept"
        part.changes.append(f"footer: schema element key: {what}")
    return fn


def remove_column_orders(part):
    assert part.meta.has(7)
    part.meta.remove(7)
    part.changes.append("footer: FileMetaData.column_orders removed")


def set_file_path(rg, col, value):
    def fn(part):
        cc = part.meta.items_of(pm.FMD_ROW_GROUPS)[rg].items_of(pm.RG_COLUMNS)[col]
        cc.set(pm.CC_FILE_PATH, pm.CT_BINARY, value)
        part.changes.append(f"footer: row_groups[{rg}].columns[{col}].file_path set to {value.decode()!r}")
    return fn


def duplicate_stamp(key):
    def fn(part):
        kv = part.meta.get(pm.FMD_KV).items
        e = next(x for x in kv if x.get(pm.KV_KEY) == key)
        kv.append(copy.deepcopy(e))
        part.changes.append(f"footer: key-value entry {key.decode()!r} appended a second time (same value)")
    return fn


def shorten_list(rg, fid, label):
    def fn(part):
        lst = part.key_ci(rg).get(fid).items
        lst.pop()
        part.changes.append(f"row group {rg} key ColumnIndex: last entry of {label} removed "
                            f"({len(lst) + 1} -> {len(lst)}; other lists unchanged)")
    return fn


def empty_indexes(rg):
    def fn(part):
        ci, oi = part.key_ci(rg), part.key_oi(rg)
        for fid, _t, v in ci.fields:
            if isinstance(v, pm.TList):
                v.items.clear()
        oi.get(pm.OI_PAGE_LOCATIONS).items.clear()
        oi.remove(2)
        part.changes.append(f"row group {rg} key ColumnIndex and OffsetIndex: every list emptied")
    return fn


def loosen_bounds(part):
    """Valid but non-monotone bounds: a lower minimum and a higher maximum than needed."""
    ci = part.key_ci(0)
    mins, maxs = ci.get(pm.CI_MIN).items, ci.get(pm.CI_MAX).items
    old = (mins[1], maxs[0])
    mins[1] = b"a"
    maxs[0] = b"\xff\xff"
    ci.set(pm.CI_BOUNDARY, pm.CT_I32, 0)
    part.changes.append(f"row group 0 key ColumnIndex: min_values[1] {fmt_bytes(old[0])} -> 'a' (below "
                        f"min_values[0]); max_values[0] {fmt_bytes(old[1])} -> ffff (above max_values[1]); "
                        f"boundary_order -> UNORDERED. Both are still true bounds.")


def corrupt_page(rg, col, page_no):
    def fn(data):
        pf = pm.read_parquet(data)
        cc = pm.column_chunk(pf, rg, col)
        pages = pm.data_pages(pf, cc)
        p = pages[page_no]
        body = p.offset + p.header_len
        new = data[:body] + b"\xff" * p.compressed_size + data[body + p.compressed_size:]
        return new, [f"row group {rg} column {col}: body of data page {page_no} ({p.compressed_size} bytes at "
                     f"offset {body}) overwritten with 0xFF; its header and every other byte unchanged"]
    return fn


# Conservative bound shortening, as parquet-java's BinaryTruncator does it ------

TRUNC = 16


def shorten_min(b, trunc=TRUNC):
    return b if len(b) <= trunc else b[:trunc]


def shorten_max(b, trunc=TRUNC):
    if len(b) <= trunc:
        return b
    p = b[:trunc].rstrip(b"\xff")
    if not p:
        return b  # retained bytes are all 0xFF: keep the full maximum (RG-5)
    return p[:-1] + bytes([p[-1] + 1])


def shorten_all(force_unordered_rg=None, trunc=TRUNC):
    TRUNC = trunc  # noqa: N806

    def fn(part):
        for (rg, col), (ci, _oi) in sorted(part.idx.items()):
            if col != 0 or ci is None:
                continue
            mins, maxs = ci.get(pm.CI_MIN).items, ci.get(pm.CI_MAX).items
            for i in range(len(mins)):
                nm, nx = shorten_min(mins[i], TRUNC), shorten_max(maxs[i], TRUNC)
                if nm != mins[i]:
                    part.changes.append(f"row group {rg} key ColumnIndex: min_values[{i}] {fmt_bytes(mins[i])} -> "
                                        f"{fmt_bytes(nm)} (shortened lower bound)")
                if nx != maxs[i]:
                    part.changes.append(f"row group {rg} key ColumnIndex: max_values[{i}] {fmt_bytes(maxs[i])} -> "
                                        f"{fmt_bytes(nx)} (shortened upper bound)")
                elif len(maxs[i]) > TRUNC:
                    part.changes.append(f"row group {rg} key ColumnIndex: max_values[{i}] kept full "
                                        f"({len(maxs[i])} bytes): its first {TRUNC} bytes are all 0xFF")
                mins[i], maxs[i] = nm, nx
            asc = all(mins[i] <= mins[i + 1] and maxs[i] <= maxs[i + 1] for i in range(len(mins) - 1))
            bo = 0 if (rg == force_unordered_rg or not asc) else 1
            old = ci.get(pm.CI_BOUNDARY)
            if bo != old:
                ci.set(pm.CI_BOUNDARY, pm.CT_I32, bo)
                part.changes.append(f"row group {rg} key ColumnIndex: boundary_order "
                                    f"{pm.BOUNDARY_ORDER[old]} -> {pm.BOUNDARY_ORDER[bo]}")
            overl = [i for i in range(len(mins) - 1) if maxs[i] >= mins[i + 1]]
            if overl:
                part.changes.append(f"row group {rg} key ColumnIndex: bounds of pages {overl} overlap the next page")
    return fn


def find_page(src_name, part_path, pred):
    """Locate (row group, page, page keys) in a dataset part by decoding keys with the source rows."""
    from common import load_rows
    doc = load_rows(src_name)
    part = next(p for p in doc["parts"] if p["path"] == part_path)
    keys = [b64d(r["key_base64"]) for r in part["rows"]]
    mrpp = part["writer"]["max_rows_per_page"]
    off = 0
    for rg, n in enumerate(part["row_groups"]):
        for pg, s in enumerate(range(0, n, mrpp)):
            pk = keys[off + s: off + min(s + mrpp, n)]
            if pred(pk):
                return rg, pg, pk
        off += n
    raise LookupError("no such page")


# --------------------------------------------------------------------------

def build():
    R = []
    B = "data/part-00000.parquet"
    open_ = "open"

    # ---- valid, mutated -------------------------------------------------------
    R.append(derive("truncated-bounds", "binary-keys",
                    "binary-keys with every key-page bound longer than 16 bytes shortened conservatively "
                    "(lower bounds to a 16-byte prefix, upper bounds to an incremented prefix, except that an "
                    "upper bound whose first 16 bytes are all 0xFF is kept full). Adjacent page bounds overlap. "
                    "Row group 1 of part 0 is marked UNORDERED.",
                    None, valid=True, out=DATASETS / "truncated-bounds",
                    parts={"data/part-00000.parquet": shorten_all(force_unordered_rg=1),
                           "data/part-00001.parquet": shorten_all()}))
    R.append(derive("truncated-bounds-2", "binary-keys",
                    "binary-keys with every key-page bound longer than 2 bytes shortened to 2 bytes the same way: "
                    "e.g. a page ending at ff ff ff keeps its full maximum while its siblings are shortened, and "
                    "many adjacent bounds overlap.",
                    None, valid=True, out=DATASETS / "truncated-bounds-2",
                    parts={"data/part-00000.parquet": shorten_all(trunc=2),
                           "data/part-00001.parquet": shorten_all(trunc=2)}))
    R.append(derive("non-monotone-bounds", "basic",
                    "basic with valid but non-monotone key page bounds (a looser minimum and maximum) and "
                    "boundary_order UNORDERED.", None, valid=True, out=DATASETS / "non-monotone-bounds",
                    parts={B: loosen_bounds}))
    R.append(derive("key-legacy-utf8", "projection",
                    "projection with the key annotated only by the legacy converted type UTF8 (KEY-1 allows it).",
                    None, valid=True, out=DATASETS / "key-legacy-utf8",
                    parts={p: set_key_annotation("legacy-utf8") for p in
                           ("data/part-00000.parquet", "data/part-00001.parquet", "data/part-00002.parquet")}))
    R.append(derive("no-value-page-index", "basic",
                    "basic with the ColumnIndex and OffsetIndex of every non-key column removed (allowed: COL-4).",
                    None, valid=True, out=DATASETS / "no-value-page-index", parts={B: remove_value_indexes}))

    # ---- manifest -------------------------------------------------------------
    R.append(derive("no-manifest", "basic", "The part file is present but manifest.json is not.",
                    F("no_manifest", open_, ["LAYOUT-1", "CHECK-1"]), drop_manifest=True))
    mi = lambda reqs, **kw: F("invalid_manifest", open_, reqs + ["CHECK-1"], **kw)  # noqa: E731
    R.append(derive("manifest-not-json", "basic", "manifest.json is truncated mid-object.", mi(["MAN-1"]),
                    raw_manifest=lambda t: (t[: len(t) // 2], ["manifest text truncated to half its length"])))
    R.append(derive("manifest-bom", "basic", "manifest.json begins with a UTF-8 byte order mark.", mi(["MAN-1"]),
                    raw_manifest=lambda t: ("﻿" + t, ["manifest text: UTF-8 BOM prepended"])))
    R.append(derive("manifest-not-object", "basic", "manifest.json is a JSON array holding the manifest object.",
                    mi(["MAN-1"]), raw_manifest=lambda t: ("[" + t.rstrip("\n") + "]\n", ["manifest wrapped in [ ]"])))
    R.append(derive("manifest-duplicate-member", "basic", "The top-level member schema_id appears twice.",
                    mi(["MAN-2"]), raw_manifest=text_replace('"schema_id": "conformance/objects/v1",',
                                                             '"schema_id": "conformance/objects/v1",\n'
                                                             '  "schema_id": "conformance/objects/v1",')))
    R.append(derive("manifest-duplicate-part-member", "basic", "A part object has the member rows twice.",
                    mi(["MAN-2"]), raw_manifest=text_replace('"rows": 12,', '"rows": 12,\n      "rows": 12,')))
    R.append(derive("manifest-unknown-member", "basic", "Top-level member created_at is not defined.",
                    mi(["MAN-3"]), manifest=set_member(["created_at"], "2026-01-01T00:00:00Z")))
    R.append(derive("manifest-unknown-part-member", "basic", "Part member checksum is not defined.",
                    mi(["MAN-3"]), manifest=set_member(["parts", 0, "checksum"], "abc")))
    R.append(derive("manifest-extension-not-object", "basic", "extensions is a string, not an object.",
                    mi(["MAN-3"]), manifest=set_member(["extensions"], "producer-metadata")))
    R.append(derive("manifest-missing-schema-id", "basic", "The required member schema_id is absent.",
                    mi(["MAN-3"]), manifest=del_member(["schema_id"])))
    R.append(derive("manifest-missing-parts", "basic", "The required member parts is absent.",
                    mi(["MAN-3"]), manifest=del_member(["parts"])))
    R.append(derive("manifest-rows-string", "basic", "A part's rows is the string \"12\".",
                    mi(["MAN-3"]), manifest=set_member(["parts", 0, "rows"], "12")))
    R.append(derive("manifest-wrong-format", "basic", "format is \"parquet\".",
                    mi(["MAN-4"]), manifest=set_member(["format"], "parquet")))
    R.append(derive("manifest-draft-without-revision", "basic", "format_version is 0 but draft_revision is absent.",
                    mi(["MAN-3", "MAN-4", "MAN-7"]),
                    manifest=del_member(["draft_revision"])))
    R.append(derive("manifest-bad-base64", "basic", "min_key_base64 contains characters outside the base64 alphabet.",
                    mi(["PART-3"]), manifest=set_member(["parts", 0, "min_key_base64"], "YS8x!nR4dA==")))
    R.append(derive("manifest-base64-unpadded", "basic", "min_key_base64 omits its padding.",
                    mi(["PART-3"]), manifest=set_member(["parts", 0, "min_key_base64"], "YS8xLnR4dA")))
    R.append(derive("manifest-base64-url-alphabet", "binary-keys",
                    "Part 1's max_key_base64 (0xFF*81) is written in the URL-safe alphabet ('_' for '/').",
                    mi(["PART-3"]),
                    manifest=lambda m, ch: set_member(["parts", 1, "max_key_base64"],
                                                      m["parts"][1]["max_key_base64"].replace("/", "_"))(m, ch)))
    R.append(derive("manifest-path-escape", "basic",
                    "The part path climbs out of the dataset root to a real, valid part of another dataset.",
                    mi(["PART-1"]), manifest=set_member(["parts", 0, "path"], "../../basic/data/part-00000.parquet")))
    R.append(derive("manifest-path-absolute", "basic", "The part path begins with '/'.",
                    mi(["PART-1"]), manifest=set_member(["parts", 0, "path"], "/data/part-00000.parquet")))
    R.append(derive("manifest-path-dot-segment", "basic",
                    "The part path has a '.' segment (it would resolve to the real part).",
                    mi(["PART-1"]), manifest=set_member(["parts", 0, "path"], "data/./part-00000.parquet")))
    R.append(derive("manifest-path-empty-segment", "basic", "The part path has an empty segment.",
                    mi(["PART-1"]), manifest=set_member(["parts", 0, "path"], "data//part-00000.parquet")))
    R.append(derive("manifest-path-backslash", "basic", "The part path contains a backslash.",
                    mi(["PART-1"]), manifest=set_member(["parts", 0, "path"], "data\\part-00000.parquet")))
    R.append(derive("manifest-path-duplicate", "basic",
                    "Two parts name the same path (their declared bounds are disjoint and ascending).",
                    mi(["PART-1"]), manifest=add_part_copy(B, b"zz0", b"zz9")))
    R.append(derive("manifest-rows-zero", "basic", "A part declares rows 0.", mi(["PART-2"]),
                    manifest=set_member(["parts", 0, "rows"], 0)))
    R.append(derive("manifest-rows-fraction", "basic", "A part's rows is written 12.0.", mi(["MAN-6"]),
                    raw_manifest=text_replace('"rows": 12,', '"rows": 12.0,')))
    R.append(derive("manifest-rows-exponent", "basic", "A part's rows is written 1.2e1.", mi(["MAN-6"]),
                    raw_manifest=text_replace('"rows": 12,', '"rows": 1.2e1,')))
    R.append(derive("manifest-bytes-too-large", "basic", "A part's bytes is 2^63, beyond the allowed range.",
                    mi(["MAN-6"]), manifest=set_member(["parts", 0, "bytes"], 1 << 63)))
    R.append(derive("manifest-format-version-fraction", "basic", "format_version is written 0.0.",
                    mi(["MAN-6", "MAN-7"]), raw_manifest=text_replace('"format_version": 0,', '"format_version": 0.0,')))
    R.append(derive("manifest-base64-noncanonical", "basic",
                    "min_key_base64 is YS8xLnR4dB==: it decodes to the right key but its unused bits are not zero.",
                    mi(["PART-3"]), manifest=set_member(["parts", 0, "min_key_base64"], "YS8xLnR4dB==")))
    R.append(derive("manifest-path-control-character", "basic", "The part path contains U+0001.",
                    mi(["PART-1"]), manifest=set_member(["parts", 0, "path"], "data/part\u0001-00000.parquet")))
    R.append(derive("manifest-extensions-duplicate-member", "basic",
                    "An object inside extensions has the member a twice.", mi(["MAN-2"]),
                    raw_manifest=text_replace('  "parts": [', '  "extensions": {"conformance": {"a": 1, "a": 2}},\n  "parts": [')))
    R.append(derive("manifest-duplicate-member-escaped", "basic",
                    "schema_id appears twice, once spelled with an escape (schema\\u005fid).", mi(["MAN-2"]),
                    raw_manifest=text_replace('"schema_id": "conformance/objects/v1",',
                                              '"schema_id": "conformance/objects/v1",\n'
                                              '  "schema\\u005fid": "conformance/objects/v1",')))
    R.append(derive("manifest-unpaired-surrogate", "basic", "schema_id ends with the escaped lone surrogate \\ud800.",
                    mi(["MAN-1"]), raw_manifest=text_replace('"schema_id": "conformance/objects/v1"',
                                                            '"schema_id": "conformance/objects/v1\\ud800"')))
    uv = lambda reqs: F("unsupported_version", open_, reqs)  # noqa: E731
    R.append(derive("unsupported-version-2", "basic", "format_version is 2 (well formed, not defined by this draft).",
                    uv(["MAN-4", "MAN-7", "VER-1"]),
                    manifest=lambda m, ch: (del_member(["draft_revision"])(m, ch), set_member(["format_version"], 2)(m, ch))))
    R.append(derive("unsupported-version-2-unknown-member", "basic",
                    "format_version 2 with a member draft 1 does not define: the version is decided first (MAN-7).",
                    uv(["MAN-7", "VER-1"]),
                    manifest=lambda m, ch: (del_member(["draft_revision"])(m, ch), set_member(["format_version"], 2)(m, ch),
                                            set_member(["created_at"], "2026-01-01T00:00:00Z")(m, ch))))
    R.append(derive("missing-part", "basic", "The manifest lists a second part that does not exist.",
                    F("missing_part", open_, ["LAYOUT-1", "CHECK-1"]),
                    manifest=add_part_copy("data/part-00001.parquet", b"zz0", b"zz9")))
    R.append(derive("part-size-larger", "basic", "The manifest's bytes is one more than the file's size.",
                    F("part_mismatch", open_, ["CHECK-1"]),
                    manifest=lambda m, ch: set_member(["parts", 0, "bytes"], m["parts"][0]["bytes"] + 1)(m, ch)))
    R.append(derive("part-size-smaller", "basic", "The manifest's bytes is one less than the file's size.",
                    F("part_mismatch", open_, ["CHECK-1"]),
                    manifest=lambda m, ch: set_member(["parts", 0, "bytes"], m["parts"][0]["bytes"] - 1)(m, ch)))
    R.append(derive("part-rows-mismatch", "basic", "The manifest's rows is 13; the part holds 12.",
                    F("part_mismatch", open_, ["CHECK-1"]),
                    manifest=set_member(["parts", 0, "rows"], 13)))
    R.append(derive("bounds-min-above-max", "basic", "The part's declared min key is greater than its max key.",
                    F("invalid_manifest", open_, ["PART-4", "CHECK-1"]),
                    manifest=set_member(["parts", 0, "min_key_base64"], b64e(b"zulu"))))
    R.append(derive("bounds-overlap", "multipart",
                    "The first part's declared max key is raised to the second part's min key (manifest bounds overlap).",
                    F("invalid_manifest", open_, ["PART-4", "CHECK-1"]),
                    manifest=lambda m, ch: set_member(["parts", 0, "max_key_base64"], m["parts"][1]["min_key_base64"])(m, ch)))
    R.append(derive("bounds-parts-out-of-order", "multipart",
                    "The manifest lists the parts with the last part first (bounds not ascending).",
                    F("invalid_manifest", open_, ["PART-4", "CHECK-1"]),
                    manifest=lambda m, ch: (m["parts"].insert(0, m["parts"].pop()),
                                            ch.append("manifest: parts[2] moved to parts[0]"))))
    R.append(derive("bounds-first-key-differs", "multipart",
                    "Part 1's declared min key is below its actual first key (still above part 0's max).",
                    F("bounds_mismatch", "validate:profile", ["PART-3", "VAL-1"], request="validate:profile"),
                    manifest=set_member(["parts", 1, "min_key_base64"], b64e(b"logs/2026/02/f004"))))
    R.append(derive("bounds-last-key-differs", "basic", "The part's declared max key is above its actual last key.",
                    F("bounds_mismatch", "validate:profile", ["PART-3"], request="validate:profile"),
                    manifest=set_member(["parts", 0, "max_key_base64"], b64e(b"golf"))))
    R.append(derive("digest-mismatch", "multipart", "Part 2's sha256 is wrong (all zeros).",
                    F("digest_mismatch", "validate:digest", ["PART-5"], request="validate:digest"),
                    manifest=set_member(["parts", 2, "sha256"], "0" * 64)))

    # ---- footer ---------------------------------------------------------------
    R.append(derive_raw_part("invalid-footer-magic", "The part's trailing magic is PAR0 instead of PAR1.",
                             lambda d: d[:-4] + b"PAR0", "trailing magic PAR1 -> PAR0"))
    R.append(derive_raw_part("invalid-footer-length", "The footer length field exceeds the file size.",
                             lambda d: d[:-8] + (len(d) + 10).to_bytes(4, "little") + d[-4:],
                             "footer length field -> file size + 10"))
    R.append(derive_raw_part("invalid-footer-thrift", "The footer bytes are overwritten with 0xFF (same length).",
                             lambda d: _ff_footer(d), "FileMetaData bytes replaced by 0xFF"))
    R.append(derive("invalid-footer-row-count", "basic",
                    "FileMetaData.num_rows is 13 while its row groups hold 12 rows; the manifest says 13, matching "
                    "the footer's num_rows.",
                    F("invalid_footer", open_, ["CHECK-1"]), parts={B: set_num_rows(1)},
                    manifest=set_member(["parts", 0, "rows"], 13)))
    R.append(derive("invalid-footer-file-path", "basic",
                    "Row group 0's key column chunk sets file_path (a reference to another file).",
                    F("invalid_footer", open_, ["FILE-1", "CHECK-1"]), parts={B: set_file_path(0, 0, b"other.parquet")}))
    R.append(derive("stamp-duplicate", "basic", "The footer holds seekparquet.schema_id twice (same value).",
                    F("stamp_mismatch", open_, ["STAMP-2", "CHECK-1"]),
                    parts={B: duplicate_stamp(b"seekparquet.schema_id")}))
    R.append(derive("key-annotation-json", "basic", "The key column is annotated JSON.",
                    F("invalid_key_column", open_, ["KEY-1", "CHECK-1"]), parts={B: set_key_annotation("json")}))
    R.append(derive("column-orders-missing", "basic", "The footer has no column_orders.",
                    F("invalid_key_column", open_, ["KEY-5", "CHECK-1"]), parts={B: remove_column_orders}))
    R.append(derive("invalid-key-utf8", "non-utf8-keys",
                    "non-utf8-keys with the key annotated STRING: two keys (each mid-page) are not valid UTF-8; "
                    "every page bound is.",
                    F("invalid_key", "read", ["KEY-6", "CHECK-2"]), parts={B: set_key_annotation("string")}))
    MP = "shards/c.parquet"
    R.append(derive("invalid-parquet-page", "multipart",
                    "The first data page of column size in row group 1 of shards/c.parquet does not decompress.",
                    F("invalid_parquet", "validate:full", ["CHECK-5", "VAL-1"], request="validate:full"),
                    raw={MP: corrupt_page(1, 1, 0)}))
    R.append(derive("sorting-columns-missing-second-row-group", "basic",
                    "Only row group 1 lacks sorting_columns.",
                    F("missing_sorting_columns", open_, ["RG-1", "CHECK-1"]), parts={B: remove_sorting(1)}))

    # ---- page index -----------------------------------------------------------
    pi = lambda reqs, **kw: F("invalid_page_index", "read", reqs + ["RG-7", "CHECK-2"], **kw)  # noqa: E731
    R.append(derive("missing-column-index", "basic", "Row group 1's key chunk has no ColumnIndex.",
                    F("missing_page_index", "read", ["RG-2", "RG-7", "CHECK-2"]), parts={B: remove_index(1, "column")}))
    R.append(derive("missing-offset-index", "basic", "Row group 1's key chunk has no OffsetIndex.",
                    F("missing_page_index", "read", ["RG-2", "RG-7", "CHECK-2"]), parts={B: remove_index(1, "offset")}))
    R.append(derive("page-index-null-page", "basic", "Row group 1's key ColumnIndex marks page 1 as a null page.",
                    pi(["RG-4"]), parts={B: null_page(1, 1)}))
    R.append(derive("page-index-count-mismatch", "basic",
                    "Row group 0's key ColumnIndex has one entry fewer than its OffsetIndex.",
                    pi(["RG-4"]), parts={B: drop_last_ci_entry(0)}))
    R.append(derive("page-index-first-row-nonzero", "basic",
                    "Row group 1's key OffsetIndex starts at first_row_index 1.", pi(["RG-3"]),
                    parts={B: set_first_row(1, 0, 1)}))
    R.append(derive("page-index-first-row-not-increasing", "basic",
                    "Row group 0's key OffsetIndex repeats first_row_index 2 for pages 1 and 2.", pi(["RG-3"]),
                    parts={B: set_first_row(0, 2, 2)}))
    R.append(derive("page-index-first-row-out-of-range", "basic",
                    "Row group 1's last key page claims first_row_index 6, equal to the row group's row count.",
                    pi(["RG-3"]), parts={B: set_first_row(1, 2, 6)}))
    R.append(derive("page-index-location-outside-file", "basic",
                    "Row group 1's last key page location points past the end of the file.", pi(["RG-3"]),
                    parts={B: set_location_offset(1, 2, lambda part: len(part.pf.data) + 100)}))
    R.append(derive("page-index-rows-mismatch", "basic",
                    "Row group 0's key OffsetIndex says first_row_index [0, 1, 4] for pages of 2 rows each: page 0 "
                    "holds 2 rows but its indexed interval is 1 row.", pi(["RG-3"]),
                    parts={B: set_first_row(0, 1, 1)}))
    R.append(derive("page-index-list-length-mismatch", "basic",
                    "Row group 1's key ColumnIndex max_values has one entry fewer than null_pages and min_values.",
                    pi(["RG-4"]), parts={B: shorten_list(1, pm.CI_MAX, "max_values")}))
    R.append(derive("page-index-location-mismatch", "basic",
                    "Row group 0's key OffsetIndex locates page 1 one byte after its real header.", pi(["RG-3"]),
                    parts={B: set_location_offset(0, 1, lambda part: part.key_oi(0).get(pm.OI_PAGE_LOCATIONS)
                                                  .items[1].get(pm.PL_OFFSET) + 1)}))
    R.append(derive("page-index-empty", "basic", "Row group 0's key ColumnIndex and OffsetIndex have empty lists.",
                    pi(["RG-3", "RG-4"]), parts={B: empty_indexes(0)}))
    rg, pg, pk = find_page("projection", B, lambda k: k[-1] == b"row-01")
    R.append(derive("page-index-bound-not-utf8", "projection",
                    "The STRING key's ColumnIndex max for the page ending at row-01 is row-\\xff: still an upper "
                    "bound, but not valid UTF-8 (KEY-6).", pi(["KEY-6"]),
                    parts={B: set_bound(rg, pg, "max", lambda b: b"row-\xff", "not valid UTF-8")}))
    rg, pg, pk = find_page("basic", B, lambda k: k[0] == b"c/")
    R.append(derive("page-index-min-above-first-key", "basic",
                    f"The key ColumnIndex min of the page starting at {pk[0]!r} is above that first key.",
                    pi(["RG-5"]), parts={B: set_bound(rg, pg, "min", lambda b: b + b"\x00", "now above the first key")}))
    rg, pg, pk = find_page("basic", B, lambda k: k[-1] == b"delta")
    R.append(derive("page-index-max-below-last-key", "basic",
                    f"The key ColumnIndex max of the page ending at {pk[-1]!r} is shortened to below that key.",
                    pi(["RG-5"]), parts={B: set_bound(rg, pg, "max", lambda b: b[:-1], "truncated without increment")}))
    P1 = "data/part-00001.parquet"
    rg, pg, pk = find_page("binary-keys", P1, lambda k: k[-1] == b"\xff" * 80 + b"\x02")
    R.append(derive("page-index-shortened-ff-max", "binary-keys",
                    "The key ColumnIndex max of the page ending at 0xFF*80 0x02 is shortened to 0xFF*16, which is "
                    "below the page's keys (the all-0xFF truncation fault RG-5 forbids).",
                    pi(["RG-5"]), parts={P1: set_bound(rg, pg, "max", lambda b: b"\xff" * TRUNC,
                                                         "16 bytes of 0xFF: below the page's last key")}))
    return R


def _ff_footer(d):
    flen = int.from_bytes(d[-8:-4], "little")
    start = len(d) - 8 - flen
    return d[:start] + b"\xff" * flen + d[-8:]


def derive_raw_part(name, desc, fn, what):
    """Copy basic and apply a byte-level edit to the part's tail (footer), keeping the manifest size exact."""
    src = DATASETS / "basic"
    out = INVALID / name
    if out.exists():
        shutil.rmtree(out)
    m = read_manifest(src)
    p = m["parts"][0]
    data = (src / p["path"]).read_bytes()
    new = fn(data)
    assert len(new) == len(data)
    dst = out / p["path"]
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(new)
    write_text(out / "manifest.json", dumps(m))
    return {"name": name, "dir": str(out.relative_to(CONF)), "valid": False, "description": desc, "rows": None,
            "producer": PRODUCER, "provenance": "mutated", "command": COMMAND, "derived_from": "datasets/basic",
            "changes": [f"{p['path']}: {what}; size unchanged; data pages untouched"],
            "fault": F("invalid_footer", "open", ["CHECK-1"])}


if __name__ == "__main__":
    print(json.dumps(build(), indent=2))
