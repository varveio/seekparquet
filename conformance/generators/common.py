"""Shared helpers for the conformance generators (standard library only)."""

from __future__ import annotations

import base64
import hashlib
import json
import struct
from pathlib import Path

CONF = Path(__file__).resolve().parents[1]
REPO = CONF.parent
ROWS = CONF / "generators" / "rows"
DATASETS = CONF / "datasets"

FORMAT_VERSION = 0
DRAFT_REVISION = 1


def b64e(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def b64d(s: str) -> bytes:
    return base64.b64decode(s, validate=True)


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def dumps(obj) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=True) + "\n"


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def write_json(path: Path, obj) -> None:
    write_text(path, dumps(obj))


def successor(p: bytes):
    """successor(p) from format-1.md section 1, or None when p has no successor."""
    q = p.rstrip(b"\xff")
    if not q:
        return None
    return q[:-1] + bytes([q[-1] + 1])


# Typed values as in protocol.md -------------------------------------------

def f32_hex(x: float) -> str:
    return struct.pack(">f", x).hex()


def f64_hex(x: float) -> str:
    return struct.pack(">d", x).hex()


def hex_f32(h: str) -> float:
    return struct.unpack(">f", bytes.fromhex(h))[0]


def hex_f64(h: str) -> float:
    return struct.unpack(">d", bytes.fromhex(h))[0]


def load_rows(name: str) -> dict:
    with open(ROWS / f"{name}.json", encoding="utf-8") as f:
        return json.load(f)


def manifest(parts, schema_id, *, format_version=FORMAT_VERSION, draft_revision=DRAFT_REVISION,
             extensions=None) -> dict:
    m = {"format": "seekparquet", "format_version": format_version}
    if draft_revision is not None:
        m["draft_revision"] = draft_revision
    m["schema_id"] = schema_id
    m["parts"] = parts
    if extensions is not None:
        m["extensions"] = extensions
    return m


def part_entry(path: str, rows: int, data: bytes, min_key: bytes, max_key: bytes, with_sha=False) -> dict:
    e = {"path": path, "rows": rows, "bytes": len(data),
         "min_key_base64": b64e(min_key), "max_key_base64": b64e(max_key)}
    if with_sha:
        e["sha256"] = sha256(data)
    return e
