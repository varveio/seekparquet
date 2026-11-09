#!/usr/bin/env python3
"""Sequential oracle for SeekParquet query semantics (format-1.md section 6).

The oracle computes expected entries from a dataset's *source rows*
(``generators/rows/<name>.json``), never from Parquet files and never from any
reader's output. It deliberately uses no index and no clever search: every
query walks every row in key order and applies the definitions literally.

Standard library only. Used by ``generators/make_cases.py``.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROWS = HERE / "generators" / "rows"

NESTED_TYPES = ("struct<", "list<")


class InvalidRequest(Exception):
    """QUERY-1: the request is malformed."""


class Unsupported(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


class Dataset:
    """The rows of a dataset, in key order, with per-part column presence."""

    def __init__(self, doc):
        self.name = doc["dataset"]
        self.key_column, self.unique = doc["key"]["column"], doc["key"]["unique"]
        self.types = {c["name"]: c["type"] for c in doc["columns"]}
        allcols = [c["name"] for c in doc["columns"]]
        self.rows = []  # (key bytes, {column: value}, set of columns present in the row's part)
        for part in doc["parts"]:
            present = set(part.get("columns") or allcols)
            for r in part["rows"]:
                self.rows.append((base64.b64decode(r["key_base64"], validate=True), r["values"], present))
        keys = [k for k, _v, _p in self.rows]
        # Source rows obey the declared ordering mode (KEY-2, KEY-3).
        assert all(keys), f"{self.name}: empty key in source rows"
        assert all(a < b if self.unique else a <= b for a, b in zip(keys, keys[1:])), f"{self.name}: source rows violate declared key order"

    def positioned(self):
        previous, offset = None, 0
        for key, values, present in self.rows:
            offset = offset + 1 if key == previous else 0
            previous = key
            yield key, offset, values, present

    @classmethod
    def load(cls, name):
        with open(ROWS / f"{name}.json", encoding="utf-8") as f:
            return cls(json.load(f))

    def keys(self):
        return [k for k, _v, _p in self.rows]

    def _values(self, key, values, present, columns):
        out = []
        for c in columns:
            if c == self.key_column:  # the key column is projected like any other BYTE_ARRAY column
                out.append({"bytes_base64": b64(key)})
            elif c not in present:  # COL-2, and COL-7 for a column no part has
                out.append({"absent": True})
            else:
                out.append(values[c])
        return out

    def _check(self, limit, columns):
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise InvalidRequest("limit must be a positive integer")
        for c in columns:
            if c in self.types and self.types[c].startswith(NESTED_TYPES):
                raise Unsupported("unsupported_projection")

    def range(self, prefix=None, start=None, start_exclusive=False, end=None, limit=None, columns=(), cursor=None):
        """RANGE-1, RANGE-2, QUERY-2, PAGE-1 (row cursors only)."""
        columns = list(columns)
        self._check(limit, columns)
        after = cursor_filter(cursor, prefix_ok=False)  # validated even on an empty dataset
        out = []
        for k, offset, v, present in self.positioned():
            if not after(k, offset):
                continue
            if prefix is not None and not k.startswith(prefix):
                continue
            if start is not None and (k < start or (start_exclusive and k == start)):
                continue
            if end is not None and not k < end:
                continue
            out.append({"kind": "row", "key_base64": b64(k), "offset": offset, "values": self._values(k, v, present, columns)})
            if len(out) == limit:
                break
        return out

    def list(self, prefix=b"", delimiter=None, cursor=None, end=None, limit=None, columns=()):
        """LIST-2 to LIST-8, PAGE-1."""
        columns = list(columns)
        if delimiter is None or len(delimiter) != 1:
            raise InvalidRequest("delimiter must be exactly one byte")
        self._check(limit, columns)
        after = cursor_filter(cursor, prefix_ok=True)
        out, seen = [], set()
        for k, offset, v, present in self.positioned():
            # candidate rows (section 6.3): begin with prefix, after the cursor, < end
            if not k.startswith(prefix) or not after(k, offset):
                continue
            if end is not None and not k < end:
                continue
            i = k.find(delimiter, len(prefix))
            if i >= 0:
                cp = k[: i + 1]  # LIST-2; returned once if any candidate rolls up to it (LIST-5)
                if cp in seen:
                    continue
                seen.add(cp)
                entry = {"kind": "prefix", "key_base64": b64(cp)}
                if k == cp:  # LIST-8: the first candidate under cp is its exact row
                    entry["exact"] = {"offset": offset, "values": self._values(k, v, present, columns)}
                out.append(entry)
            else:  # LIST-3
                out.append({"kind": "row", "key_base64": b64(k), "offset": offset, "values": self._values(k, v, present, columns)})
            if len(out) == limit:
                break
        # LIST-4: prefixes are distinct; row entries retain file order.
        eb = [base64.b64decode(e["key_base64"]) for e in out]
        assert all(a <= b for a, b in zip(eb, eb[1:])), "oracle: entries not ascending"
        return out


def b64(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def successor(p: bytes):
    q = p.rstrip(b"\xff")
    if not q:
        return None
    return q[:-1] + bytes([q[-1] + 1])


def cursor_filter(cursor, prefix_ok):
    """PAGE-1: a predicate on (key, offset) that is true for rows after the cursor.

    A row cursor (k, n) admits keys > k, and key k at offsets > n. A prefix cursor c (listings only) admits keys
    > c that do not begin with c.
    """
    if cursor is None:
        return lambda key, offset: True
    if not isinstance(cursor, dict):
        raise InvalidRequest("cursor must be tagged")
    if set(cursor) == {"key_base64", "offset"}:
        n = cursor["offset"]
        if type(n) is not int or n < 0 or n > (1 << 63) - 1:
            raise InvalidRequest("invalid cursor offset")
        k = cursor_bytes(cursor["key_base64"])
        return lambda key, offset: key > k or key == k and offset > n
    if set(cursor) == {"prefix_base64"} and prefix_ok:
        c = cursor_bytes(cursor["prefix_base64"])
        return lambda key, offset: key > c and not key.startswith(c)
    raise InvalidRequest("invalid cursor")


def cursor_bytes(text):
    try:
        raw = base64.b64decode(text, validate=True)
        if b64(raw) != text:
            raise ValueError("noncanonical base64")
        return raw
    except (ValueError, TypeError):
        raise InvalidRequest("cursor bytes must be canonical base64") from None
