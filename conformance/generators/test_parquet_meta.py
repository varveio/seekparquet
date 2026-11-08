#!/usr/bin/env python3
"""Regression tests for parquet_meta.py on malformed input (standard library only).

    python -m unittest discover -s generators -p 'test_*.py'      # from conformance/

Every test runs under an alarm, so a regression that loops forever fails instead of hanging.
"""

from __future__ import annotations

import signal
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parquet_meta as pm  # noqa: E402
from parquet_meta import CT_I32, CT_I64, CT_STRUCT, Struct  # noqa: E402


def header(ptype, usize, csize, sub=None, sub_fid=None, drop=()):
    fields = [(1, CT_I32, ptype), (2, CT_I32, usize), (3, CT_I32, csize)]
    if sub is not None:
        fields.append((sub_fid, CT_STRUCT, sub))
    return pm.encode_struct(Struct([f for f in fields if f[0] not in drop]))


def v1(nv, enc=0):
    return Struct([(1, CT_I32, nv), (2, CT_I32, enc), (3, CT_I32, 3), (4, CT_I32, 3)])


def v2(nv, nulls, rows, dl=0, rl=0):
    return Struct([(1, CT_I32, nv), (2, CT_I32, nulls), (3, CT_I32, rows), (4, CT_I32, 0), (5, CT_I32, dl),
                   (6, CT_I32, rl)])


def chunk(body, total=None):
    """A fake file: PAR1, the chunk bytes, and nothing after (footer_start = end of chunk)."""
    data = b"PAR1" + body
    md = Struct([(9, CT_I64, 4), (7, CT_I64, len(body) if total is None else total)])
    cc = Struct([(3, CT_STRUCT, md)])
    return pm.ParquetFile(data, Struct(), len(data), 0), cc


class Bounded(unittest.TestCase):
    def setUp(self):
        if hasattr(signal, "alarm"):
            signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError("did not terminate")))
            signal.alarm(5)

    def tearDown(self):
        if hasattr(signal, "alarm"):
            signal.alarm(0)

    def bad(self, body, total=None):
        pf, cc = chunk(body, total)
        with self.assertRaises(pm.ParquetMetaError):
            pm.chunk_pages(pf, cc)

    def test_valid_pages(self):
        body = header(0, 3, 3, v1(1), 5) + b"abc" + header(3, 2, 2, v2(2, 0, 2), 8) + b"de"
        pf, cc = chunk(body)
        pages = pm.chunk_pages(pf, cc)
        self.assertEqual([p.type for p in pages], [0, 3])
        self.assertEqual([p.num_values for p in pages], [1, 2])

    def test_negative_compressed_size_index_page(self):
        # The reported case: an INDEX_PAGE header whose compressed size moves the cursor backwards.
        h = header(1, 0, -7)
        self.bad(h + b"\x00" * 16)

    def test_negative_compressed_size_data_page(self):
        self.bad(header(0, 3, -1, v1(1), 5) + b"abc")

    def test_zero_progress(self):
        h = pm.encode_struct(Struct([(1, CT_I32, 1), (2, CT_I32, 0), (3, CT_I32, 0)]))
        # header length h, compressed size -len(h) would return to the same position
        self.bad(header(1, 0, -len(h)) + b"\x00" * 8)

    def test_missing_sizes(self):
        self.bad(header(0, 3, 3, v1(1), 5, drop=(3,)) + b"abc")
        self.bad(header(0, 3, 3, v1(1), 5, drop=(2,)) + b"abc")
        self.bad(header(0, 3, 3, v1(1), 5, drop=(1,)) + b"abc")

    def test_page_overruns_chunk(self):
        self.bad(header(0, 3, 30, v1(1), 5) + b"abc")

    def test_missing_subtype_headers(self):
        self.bad(header(0, 3, 3) + b"abc")
        self.bad(header(3, 3, 3) + b"abc")
        self.bad(header(2, 3, 3) + b"abc")

    def test_negative_counts(self):
        self.bad(header(0, 3, 3, v1(-1), 5) + b"abc")
        self.bad(header(3, 3, 3, v2(1, -1, 1), 8) + b"abc")
        self.bad(header(3, 3, 3, v2(1, 2, 1), 8) + b"abc")  # more nulls than values
        self.bad(header(3, 3, 3, v2(1, 0, 1, dl=2, rl=2), 8) + b"abc")  # level bytes beyond the page

    def test_unknown_page_type(self):
        self.bad(header(9, 3, 3) + b"abc")

    def test_truncated_header(self):
        h = header(0, 3, 3, v1(1), 5)
        self.bad(h[:-3])

    def test_chunk_beyond_file(self):
        self.bad(header(0, 3, 3, v1(1), 5) + b"abc", total=1000)

    def test_huge_list_count_in_footer(self):
        # A footer whose schema list claims ~2^31 elements in a few bytes must fail fast.
        w = bytearray([0x19, 0xFC])  # field 1 (list), header 0xF0|struct, then a varint count
        w += bytes([0xFF, 0xFF, 0xFF, 0xFF, 0x07, 0x00])
        data = b"PAR1" + bytes(w) + len(w).to_bytes(4, "little") + b"PAR1"
        with self.assertRaises(pm.ParquetMetaError):
            pm.read_parquet(data)

    def test_deep_nesting(self):
        body = bytes([0x1C]) * 200 + b"\x00" * 200  # nested struct fields
        data = b"PAR1" + body + len(body).to_bytes(4, "little") + b"PAR1"
        with self.assertRaises(pm.ParquetMetaError):
            pm.read_parquet(data)

    def test_fixture_round_trip(self):
        f = Path(__file__).resolve().parents[1] / "datasets" / "multipart" / "shards" / "c.parquet"
        if f.exists():
            pm.roundtrip_check(f.read_bytes())


if __name__ == "__main__":
    unittest.main()
