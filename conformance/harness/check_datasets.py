#!/usr/bin/env python3
"""Check datasets written by a producer, outside the fixtures.

    python harness/check_datasets.py --adapter "python harness/reference_adapter.py" DIR [DIR ...]
    python harness/check_datasets.py --adapter "python validator/validator_adapter.py" DIR [DIR ...]
    python harness/check_datasets.py --target reader --adapter ADAPTER DIR [DIR ...]

Each DIR is a dataset root, or a directory whose subdirectories are dataset roots. Every dataset must
validate completely at the full level (structural, profile and full) and at the digest level, with every
part declaring a sha256, through an adapter that claims the validator targets (protocol.md). When the
duckdb module is installed, every part must also read in DuckDB with the manifest's row count, its keys in
the declared order (strictly ascending when keys are unique, across parts too) and its first and last keys
equal to the manifest's bounds. Python standard library only, apart from the optional duckdb.

With `--target reader`, the adapter must claim the reader target instead, and every dataset is queried
through it: range and list requests over the whole dataset and over bounds, prefixes and a delimiter,
projecting every column, paged with small and changing limits through their cursors. The expected answers
come from the source rows, read from the part files with PyArrow (an independent reader, never either
implementation's output): group offsets, delimiter rollups and LIST-8 exact rows follow from them. This
mode needs pyarrow (conformance/generators/requirements.txt).

The Go module's producer writes its sample datasets for this check with
`go -C go run ./internal/cmd/samples DIR`.
"""

from __future__ import annotations

import argparse
import base64
import json
import shlex
import struct
import subprocess
import sys
from pathlib import Path


def datasets(paths):
    for p in map(Path, paths):
        if (p / "manifest.json").is_file():
            yield p.resolve()
        else:
            yield from sorted(d.resolve() for d in p.iterdir() if (d / "manifest.json").is_file())


class Adapter:
    def __init__(self, command):
        self.proc = subprocess.Popen(shlex.split(command), stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.next = 0

    def ask(self, req):
        self.next += 1
        req = {"id": str(self.next), **req}
        self.proc.stdin.write(json.dumps(req) + "\n")
        self.proc.stdin.flush()
        resp = json.loads(self.proc.stdout.readline())
        if resp.get("id") != req["id"]:
            raise SystemExit(f"adapter answered {resp.get('id')!r} to request {req['id']!r}")
        return resp

    def close(self):
        self.proc.stdin.close()
        if self.proc.wait(timeout=60) != 0:
            raise SystemExit(f"adapter exited with status {self.proc.returncode}")


def check_validation(adapter, root, manifest):
    problems = []
    resp = adapter.ask({"op": "validate", "dataset": str(root), "level": "full"})
    levels = resp.get("levels", {})
    for level in ("structural", "profile", "full"):
        if levels.get(level, {}).get("result") != "complete":
            problems.append(f"{level}: {levels.get(level) or resp}")
    resp = adapter.ask({"op": "validate", "dataset": str(root), "level": "digest"})
    digest = resp.get("levels", {}).get("digest", {})
    parts = manifest["parts"]
    if digest.get("result") != "complete" or digest.get("checked") != len(parts):
        problems.append(f"digest: {digest or resp}")
    if any("sha256" not in p for p in parts):
        problems.append("a part declares no sha256")
    return problems


def check_duckdb(duckdb, root, manifest):
    problems = []
    unique = manifest["key"]["unique"]
    column = manifest["key"]["column"]
    previous = None
    for part in manifest["parts"]:
        path = str(root / part["path"])
        keys = [r[0] for r in duckdb.execute(f'SELECT "{column}" FROM read_parquet(?)', [path]).fetchall()]
        keys = [k.encode() if isinstance(k, str) else bytes(k) for k in keys]
        if len(keys) != part["rows"]:
            problems.append(f"{part['path']}: DuckDB read {len(keys)} rows, the manifest says {part['rows']}")
            continue
        if keys[0] != base64.b64decode(part["min_key_base64"]) or keys[-1] != base64.b64decode(part["max_key_base64"]):
            problems.append(f"{part['path']}: first or last key differs from the manifest bounds")
        for k in keys:
            if previous is not None and (k < previous or unique and k == previous):
                problems.append(f"{part['path']}: keys out of order")
                break
            previous = k
    return problems


def protocol_value(physical, v):
    """Encodes a source value, already in its physical form (see physical_values), as the adapter protocol
    does (protocol.md)."""
    if v is None:
        return {"null": True}
    if physical == "BOOLEAN":
        return {"bool": bool(v)}
    if physical in ("INT32", "INT64"):
        return {"int": str(v)}
    if physical == "FLOAT":
        return {"float32": format(v, "08x")}
    if physical == "DOUBLE":
        return {"float64": format(v, "016x")}
    return {"bytes_base64": b64(v)}


def physical_values(column, physical, type_length):
    """Returns a PyArrow column's values in their physical form, the form the protocol reports (protocol.md:
    annotations are ignored): floating-point values as their exact bits (read through an integer view, so
    that a NaN keeps its payload), dates, times, timestamps, unsigned and small integers as the signed
    INT32 or INT64 they are stored as, decimals as their unscaled integer or big-endian bytes, strings as
    their UTF-8 bytes. A type it cannot map exits rather than expect a wrong answer."""
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.types as pt  # noqa: PLC0415

    column = column.combine_chunks() if hasattr(column, "combine_chunks") else column
    if isinstance(column.type, pa.ExtensionType):
        column = column.storage
    t = column.type
    if physical == "BOOLEAN":
        return column.to_pylist()
    if physical in ("FLOAT", "DOUBLE"):
        if pt.is_float16(t):
            raise SystemExit("a FLOAT16 column: not mapped by this check")
        return column.view(pa.uint32() if physical == "FLOAT" else pa.uint64()).to_pylist()
    if physical in ("INT32", "INT64"):
        signed = pa.int32() if physical == "INT32" else pa.int64()
        if pt.is_decimal(t):
            return [None if v is None else int(v.scaleb(t.scale)) for v in column.to_pylist()]
        if pt.is_integer(t) and t.bit_width == signed.bit_width:
            return column.view(signed).to_pylist()  # unsigned: the stored two's complement
        if pt.is_integer(t):
            return column.cast(signed).to_pylist()  # 8 and 16 bits fit
        if pt.is_temporal(t) and t.bit_width == signed.bit_width:
            return column.view(signed).to_pylist()  # dates, times, timestamps: their stored count
        raise SystemExit(f"a {physical} column read as {t}: not mapped by this check")
    if physical in ("BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"):
        if pt.is_decimal(t):
            if physical != "FIXED_LEN_BYTE_ARRAY":
                raise SystemExit("a BYTE_ARRAY decimal: its stored width is not recoverable by this check")
            return [None if v is None else int(v.scaleb(t.scale)).to_bytes(type_length, "big", signed=True)
                    for v in column.to_pylist()]
        return [None if v is None else v.encode() if isinstance(v, str) else bytes(v) for v in column.to_pylist()]
    raise SystemExit(f"a {physical} column: not a type the reader target projects")


def b64(b):
    return base64.b64encode(b).decode()


def source_rows(root, manifest):
    """Reads every part with PyArrow, in manifest order: the value columns of all parts, in the order they
    first appear, and every row's key and values, a column its part lacks reported absent (COL-2)."""
    import pyarrow.parquet as pq  # noqa: PLC0415

    key_column = manifest["key"]["column"]
    columns, keys, parts = [], [], []
    for part in manifest["parts"]:
        f = pq.ParquetFile(root / part["path"])
        leaves = {f.schema.column(i).path: f.schema.column(i) for i in range(len(f.schema))}
        table = f.read()
        values = {}
        for name in table.column_names:
            if name == key_column:
                continue
            if name not in columns:
                columns.append(name)
            leaf = leaves[name]
            values[name] = [protocol_value(leaf.physical_type, v)
                            for v in physical_values(table.column(name), leaf.physical_type, leaf.length)]
        parts.append((table.num_rows, values))
        keys += [k.encode() if isinstance(k, str) else bytes(k) for k in table.column(key_column).to_pylist()]
    rows = []
    for count, values in parts:
        for i in range(count):
            rows.append([values[name][i] if name in values else {"absent": True} for name in columns])
    return columns, keys, rows


def expected(keys, values, prefix=b"", start=None, exclusive=False, end=None, delimiter=None):
    """The answer a reader owes, from the source rows: a row's offset is its position in its group of equal
    keys, counted before any filter (KEY-7); with a delimiter, keys roll up to their first common prefix,
    which carries its first row as its exact row when that row's key is the prefix itself (LIST-8)."""
    out, seen, offset = [], set(), 0
    for i, (key, row) in enumerate(zip(keys, values)):
        offset = offset + 1 if i > 0 and keys[i - 1] == key else 0
        if not key.startswith(prefix) or end is not None and key >= end:
            continue
        if start is not None and (key < start or exclusive and key == start):
            continue
        split = key.find(delimiter, len(prefix)) if delimiter else -1
        if split < 0:
            out.append({"kind": "row", "key_base64": b64(key), "offset": offset, "values": row})
            continue
        folder = key[: split + len(delimiter)]
        if folder in seen:
            continue
        seen.add(folder)
        entry = {"kind": "prefix", "key_base64": b64(folder)}
        if key == folder:
            entry["exact"] = {"offset": offset, "values": row}
        out.append(entry)
    return out


def paged(adapter, request, want):
    """Pages through request with small and changing limits, following each response's cursor, and
    returns a problem, or None when the entries equal want."""
    got, after = [], None
    for step in range(len(want) + 2):
        limit = (1000, 7, 1, 129, 2)[step % 5]
        req = dict(request, limit=limit)
        if after is not None:
            req["after"] = after
        resp = adapter.ask(req)
        if resp.get("status") != "ok":
            return f"{req}: {resp}"
        entries = resp.get("entries", [])
        if entries != want[len(got) : len(got) + limit]:
            return f"{req}: the entries differ from the source rows from entry {len(got)}"
        if not entries:
            return None if got == want else f"{req}: ended after {len(got)} of {len(want)} entries"
        got += entries
        last = entries[-1]
        after = {"prefix_base64": last["key_base64"]} if last["kind"] == "prefix" else {
            "key_base64": last["key_base64"], "offset": last["offset"]}
    return f"{request}: the cursors did not end"


def check_reader(adapter, root, manifest):
    columns, keys, values = source_rows(root, manifest)
    if len(keys) != sum(p["rows"] for p in manifest["parts"]):
        return [f"PyArrow read {len(keys)} rows, the manifest says {sum(p['rows'] for p in manifest['parts'])}"]
    base = {"dataset": str(root), "columns": columns}
    queries = [("range", {}, {})]
    if keys:
        lo, hi = keys[len(keys) // 3], keys[2 * len(keys) // 3]
        cut = keys[len(keys) // 2].find(b"/")
        head = keys[len(keys) // 2][: cut + 1] if cut >= 0 else keys[len(keys) // 2][:4]
        queries += [
            ("range", {"start_base64": b64(lo), "end_base64": b64(hi)}, {"start": lo, "end": hi}),
            ("range", {"start_base64": b64(lo), "start_exclusive": True}, {"start": lo, "exclusive": True}),
            ("range", {"prefix_base64": b64(head)}, {"prefix": head}),
            ("list", {"delimiter_base64": b64(b"/")}, {"delimiter": b"/"}),
            ("list", {"prefix_base64": b64(head), "delimiter_base64": b64(b"/")}, {"prefix": head, "delimiter": b"/"}),
            ("list", {"delimiter_base64": b64(b"/"), "end_base64": b64(hi)}, {"delimiter": b"/", "end": hi}),
        ]
        # An end just above a rollup prefix, below every key under it: the end bounds the candidate keys,
        # so the prefix is listed only if a key equal to it exists (LIST-7), not because the prefix itself
        # is below the end.
        under = next((k for k in keys if 0 <= k.find(b"/") < len(k) - 1), None)
        if under is not None:
            end = under[: under.find(b"/") + 1] + b"\x00"
            queries.append(("list", {"delimiter_base64": b64(b"/"), "end_base64": b64(end)}, {"delimiter": b"/", "end": end}))
    problems = []
    for op, fields, model in queries:
        problem = paged(adapter, {"op": op, **base, **fields}, expected(keys, values, **model))
        if problem:
            problems.append(problem)
    return problems


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--adapter", required=True, help="adapter command claiming the validator targets, or the reader target")
    ap.add_argument("--target", choices=("validator", "reader"), default="validator",
                    help="validate every dataset (default), or query it as a reader and compare with its source rows")
    ap.add_argument("dirs", nargs="+")
    args = ap.parse_args()
    reader = args.target == "reader"
    duckdb = None
    if not reader:
        try:
            import duckdb  # noqa: PLC0415
        except ImportError:
            print("duckdb is not installed: DuckDB reads skipped")
    adapter = Adapter(args.adapter)
    hello = adapter.ask({"op": "hello"})
    if reader:
        if "reader" not in hello.get("targets", []):
            raise SystemExit(f"the adapter does not claim the reader target: {hello}")
    elif "validator-full" not in hello.get("targets", []) or "validator-digest" not in hello.get("targets", []):
        raise SystemExit(f"the adapter does not claim validator-full and validator-digest: {hello}")
    failed = 0
    roots = list(datasets(args.dirs))
    for root in roots:
        manifest = json.loads((root / "manifest.json").read_text())
        problems = check_reader(adapter, root, manifest) if reader else check_validation(adapter, root, manifest)
        if duckdb is not None:
            problems += check_duckdb(duckdb, root, manifest)
        rows = sum(p["rows"] for p in manifest["parts"])
        status = "ok" if not problems else "FAILED"
        print(f"{status}: {root.name}: {len(manifest['parts'])} parts, {rows} rows")
        for p in problems:
            print(f"  {p}")
        failed += bool(problems)
    adapter.close()
    if not roots:
        raise SystemExit("no datasets found")
    print(f"{len(roots) - failed} of {len(roots)} datasets passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
