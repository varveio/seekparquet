#!/usr/bin/env python3
"""Minimal Parquet metadata reader/writer (Python standard library only).

This module decodes and re-encodes the Thrift compact protocol structures that
SeekParquet cares about: the footer (FileMetaData), the page index (ColumnIndex
and OffsetIndex) and data page headers (PageHeader). It is deliberately small
and generic: a struct is kept as an ordered list of (field id, compact type,
value) triples, so a decode followed by an encode reproduces the input bytes
exactly (this is asserted by ``roundtrip_check``), and a mutation changes only
the fields it names.

It is used by:

* ``mutate_metadata.py`` to derive invalid fixtures by editing metadata while
  preserving every data page byte for byte;
* ``pyarrow_fixtures.py`` to assert the physical structure pyarrow produced;
* ``harness/reference_adapter.py`` to check page-index rules;
* ``python parquet_meta.py FILE...`` to print a textual structural summary.

Field ids follow parquet-format's ``parquet.thrift``.
"""

from __future__ import annotations

import struct as _struct
import sys
from dataclasses import dataclass, field

# --------------------------------------------------------------------------
# Thrift compact protocol

CT_STOP = 0
CT_TRUE = 1
CT_FALSE = 2
CT_BYTE = 3
CT_I16 = 4
CT_I32 = 5
CT_I64 = 6
CT_DOUBLE = 7
CT_BINARY = 8
CT_LIST = 9
CT_SET = 10
CT_MAP = 11
CT_STRUCT = 12
CT_BOOL = CT_TRUE  # canonical type code used when storing a bool field


class ThriftError(ValueError):
    pass


@dataclass
class TList:
    etype: int
    items: list


@dataclass
class TMap:
    ktype: int
    vtype: int
    items: list  # list of (k, v)


@dataclass
class Struct:
    fields: list = field(default_factory=list)  # [(fid, ctype, value)]

    # --- accessors -----------------------------------------------------
    def get(self, fid, default=None):
        for f, _t, v in self.fields:
            if f == fid:
                return v
        return default

    def has(self, fid):
        return any(f == fid for f, _t, _v in self.fields)

    def set(self, fid, ctype, value):
        if ctype in (CT_TRUE, CT_FALSE):
            ctype = CT_BOOL
        for i, (f, _t, _v) in enumerate(self.fields):
            if f == fid:
                self.fields[i] = (fid, ctype, value)
                return
        self.fields.append((fid, ctype, value))
        self.fields.sort(key=lambda x: x[0])

    def remove(self, fid):
        self.fields = [x for x in self.fields if x[0] != fid]

    def items_of(self, fid):
        v = self.get(fid)
        return [] if v is None else v.items


def _uvarint(buf, pos):
    shift = 0
    out = 0
    while True:
        if pos >= len(buf):
            raise ThriftError("truncated varint")
        b = buf[pos]
        pos += 1
        out |= (b & 0x7F) << shift
        if not b & 0x80:
            return out, pos
        shift += 7
        if shift > 70:
            raise ThriftError("varint too long")


def _zigzag_dec(n):
    return (n >> 1) ^ -(n & 1)


def _zigzag_enc(n):
    return (n << 1) ^ (n >> 63)


def _put_uvarint(out, n):
    if n < 0:
        raise ThriftError("negative uvarint")
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return


MAX_DEPTH = 64


class Reader:
    def __init__(self, buf, pos=0, limit=None):
        self.buf = buf
        self.pos = pos
        self.limit = len(buf) if limit is None else limit
        self.depth = 0

    def _need(self, n):
        if self.pos + n > self.limit:
            raise ThriftError("truncated input")

    def byte(self):
        self._need(1)
        b = self.buf[self.pos]
        self.pos += 1
        return b

    def varint(self):
        v, self.pos = _uvarint(self.buf, self.pos)
        if self.pos > self.limit:
            raise ThriftError("truncated input")
        return v

    def value(self, ctype):
        if ctype == CT_TRUE:
            return True
        if ctype == CT_FALSE:
            return False
        if ctype == CT_BYTE:
            b = self.byte()
            return b - 256 if b > 127 else b
        if ctype in (CT_I16, CT_I32, CT_I64):
            return _zigzag_dec(self.varint())
        if ctype == CT_DOUBLE:
            self._need(8)
            v = _struct.unpack_from("<d", self.buf, self.pos)[0]
            self.pos += 8
            return v
        if ctype == CT_BINARY:
            n = self.varint()
            self._need(n)
            v = bytes(self.buf[self.pos:self.pos + n])
            self.pos += n
            return v
        if ctype in (CT_LIST, CT_SET):
            self._enter()
            h = self.byte()
            size = h >> 4
            etype = h & 0x0F
            if size == 15:
                size = self.varint()
            if size > self.limit - self.pos:
                raise ThriftError("list larger than input")
            items = []
            for _ in range(size):
                if etype in (CT_TRUE, CT_FALSE):
                    items.append(self.byte() == CT_TRUE)
                else:
                    items.append(self.value(etype))
            self.depth -= 1
            return TList(etype, items)
        if ctype == CT_MAP:
            self._enter()
            size = self.varint()
            if size == 0:
                self.depth -= 1
                return TMap(0, 0, [])
            if size > self.limit - self.pos:
                raise ThriftError("map larger than input")
            kv = self.byte()
            kt, vt = kv >> 4, kv & 0x0F
            items = [(self.value(kt), self.value(vt)) for _ in range(size)]
            self.depth -= 1
            return TMap(kt, vt, items)
        if ctype == CT_STRUCT:
            return self.struct()
        raise ThriftError(f"unknown compact type {ctype}")

    def _enter(self):
        """Every struct, list, set and map is one nesting level; deep nesting is an error, not a RecursionError."""
        self.depth += 1
        if self.depth > MAX_DEPTH:
            raise ThriftError("nesting too deep")

    def struct(self):
        self._enter()
        s = Struct()
        last = 0
        while True:
            h = self.byte()
            if h == CT_STOP:
                break
            ctype = h & 0x0F
            delta = h >> 4
            if delta:
                fid = last + delta
            else:
                fid = _zigzag_dec(self.varint())
            v = self.value(ctype)
            if ctype in (CT_TRUE, CT_FALSE):
                ctype = CT_BOOL
            s.fields.append((fid, ctype, v))
            last = fid
        self.depth -= 1
        return s


class Writer:
    def __init__(self):
        self.out = bytearray()

    def value(self, ctype, v):
        o = self.out
        if ctype == CT_BYTE:
            o.append(v & 0xFF)
        elif ctype in (CT_I16, CT_I32, CT_I64):
            _put_uvarint(o, _zigzag_enc(v))
        elif ctype == CT_DOUBLE:
            o += _struct.pack("<d", v)
        elif ctype == CT_BINARY:
            _put_uvarint(o, len(v))
            o += v
        elif ctype in (CT_LIST, CT_SET):
            n = len(v.items)
            et = v.etype
            if n < 15:
                o.append((n << 4) | et)
            else:
                o.append(0xF0 | et)
                _put_uvarint(o, n)
            for it in v.items:
                if et in (CT_TRUE, CT_FALSE):
                    o.append(CT_TRUE if it else CT_FALSE)
                else:
                    self.value(et, it)
        elif ctype == CT_MAP:
            _put_uvarint(o, len(v.items))
            if v.items:
                o.append((v.ktype << 4) | v.vtype)
                for k, x in v.items:
                    self.value(v.ktype, k)
                    self.value(v.vtype, x)
        elif ctype == CT_STRUCT:
            self.struct(v)
        else:
            raise ThriftError(f"cannot write type {ctype}")

    def struct(self, s):
        last = 0
        for fid, ctype, v in s.fields:
            wtype = (CT_TRUE if v else CT_FALSE) if ctype in (CT_TRUE, CT_FALSE) else ctype
            delta = fid - last
            if 0 < delta <= 15:
                self.out.append((delta << 4) | wtype)
            else:
                self.out.append(wtype)
                _put_uvarint(self.out, _zigzag_enc(fid))
            if wtype not in (CT_TRUE, CT_FALSE):
                self.value(ctype, v)
            last = fid
        self.out.append(CT_STOP)


def decode_struct(buf, pos=0, limit=None):
    r = Reader(buf, pos, limit)
    s = r.struct()
    return s, r.pos


def encode_struct(s):
    w = Writer()
    w.struct(s)
    return bytes(w.out)


# --------------------------------------------------------------------------
# Parquet constants and field ids

TYPES = ["BOOLEAN", "INT32", "INT64", "INT96", "FLOAT", "DOUBLE", "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"]
REPETITION = ["REQUIRED", "OPTIONAL", "REPEATED"]
CODECS = ["UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "BROTLI", "LZ4", "ZSTD", "LZ4_RAW"]
ENCODINGS = {0: "PLAIN", 2: "PLAIN_DICTIONARY", 3: "RLE", 4: "BIT_PACKED", 5: "DELTA_BINARY_PACKED",
             6: "DELTA_LENGTH_BYTE_ARRAY", 7: "DELTA_BYTE_ARRAY", 8: "RLE_DICTIONARY", 9: "BYTE_STREAM_SPLIT"}
PAGE_TYPES = ["DATA_PAGE", "INDEX_PAGE", "DICTIONARY_PAGE", "DATA_PAGE_V2"]
BOUNDARY_ORDER = ["UNORDERED", "ASCENDING", "DESCENDING"]

# FileMetaData
FMD_VERSION, FMD_SCHEMA, FMD_NUM_ROWS, FMD_ROW_GROUPS, FMD_KV, FMD_CREATED_BY = 1, 2, 3, 4, 5, 6
# SchemaElement
SE_TYPE, SE_TYPE_LENGTH, SE_REPETITION, SE_NAME, SE_NUM_CHILDREN = 1, 2, 3, 4, 5
# RowGroup
RG_COLUMNS, RG_TOTAL_BYTE_SIZE, RG_NUM_ROWS, RG_SORTING, RG_FILE_OFFSET, RG_TOTAL_COMPRESSED = 1, 2, 3, 4, 5, 6
# SortingColumn
SC_COLUMN_IDX, SC_DESCENDING, SC_NULLS_FIRST = 1, 2, 3
# ColumnChunk
CC_FILE_PATH, CC_FILE_OFFSET, CC_META = 1, 2, 3
CC_OI_OFFSET, CC_OI_LENGTH, CC_CI_OFFSET, CC_CI_LENGTH = 4, 5, 6, 7
# ColumnMetaData
CMD_TYPE, CMD_ENCODINGS, CMD_PATH, CMD_CODEC, CMD_NUM_VALUES = 1, 2, 3, 4, 5
CMD_TOTAL_UNCOMPRESSED, CMD_TOTAL_COMPRESSED, CMD_DATA_PAGE_OFFSET, CMD_DICT_PAGE_OFFSET = 6, 7, 9, 11
CMD_STATISTICS = 12
# KeyValue
KV_KEY, KV_VALUE = 1, 2
# ColumnIndex
CI_NULL_PAGES, CI_MIN, CI_MAX, CI_BOUNDARY, CI_NULL_COUNTS = 1, 2, 3, 4, 5
# OffsetIndex / PageLocation
OI_PAGE_LOCATIONS = 1
PL_OFFSET, PL_SIZE, PL_FIRST_ROW = 1, 2, 3
# PageHeader
PH_TYPE, PH_UNCOMPRESSED, PH_COMPRESSED = 1, 2, 3
PH_DATA_V1, PH_DICT, PH_DATA_V2 = 5, 7, 8
DPH_NUM_VALUES, DPH_ENCODING = 1, 2
DPH2_NUM_VALUES, DPH2_NUM_NULLS, DPH2_NUM_ROWS, DPH2_ENCODING = 1, 2, 3, 4

MAGIC = b"PAR1"


class ParquetMetaError(ValueError):
    pass


# --------------------------------------------------------------------------
# Footer

@dataclass
class ParquetFile:
    data: bytes
    meta: Struct
    footer_start: int
    footer_len: int

    # convenience views ---------------------------------------------------
    @property
    def row_groups(self):
        return self.meta.items_of(FMD_ROW_GROUPS)

    @property
    def schema(self):
        return self.meta.items_of(FMD_SCHEMA)

    def kv(self):
        out = {}
        for e in self.meta.items_of(FMD_KV):
            k = e.get(KV_KEY, b"").decode("utf-8", "replace")
            v = e.get(KV_VALUE)
            out[k] = None if v is None else v.decode("utf-8", "replace")
        return out

    def leaf_columns(self):
        """Return [(path tuple, SchemaElement, max_def, max_rep)] for leaf columns."""
        sch = self.schema
        if not sch:
            raise ParquetMetaError("empty schema")
        out = []
        idx = 1

        def walk(prefix, n, d, r):
            nonlocal idx
            for _ in range(n):
                if idx >= len(sch):
                    raise ParquetMetaError("schema truncated")
                el = sch[idx]
                idx += 1
                rep = el.get(SE_REPETITION, 0)
                dd = d + (1 if rep in (1, 2) else 0)
                rr = r + (1 if rep == 2 else 0)
                name = el.get(SE_NAME, b"").decode("utf-8", "replace")
                kids = el.get(SE_NUM_CHILDREN)
                if kids:
                    walk(prefix + (name,), kids, dd, rr)
                else:
                    out.append((prefix + (name,), el, dd, rr))

        walk((), sch[0].get(SE_NUM_CHILDREN, 0), 0, 0)
        return out


def read_parquet(data: bytes) -> ParquetFile:
    if len(data) < 12 or data[:4] != MAGIC or data[-4:] != MAGIC:
        raise ParquetMetaError("missing PAR1 magic")
    flen = int.from_bytes(data[-8:-4], "little")
    start = len(data) - 8 - flen
    if flen <= 0 or start < 4:
        raise ParquetMetaError("footer length out of range")
    try:
        meta, end = decode_struct(data, start, len(data) - 8)
    except ThriftError as e:
        raise ParquetMetaError(f"footer: {e}") from e
    if end != len(data) - 8:
        raise ParquetMetaError("footer length does not match encoded FileMetaData")
    return ParquetFile(data, meta, start, flen)


def column_chunk(pf: ParquetFile, rg: int, col: int) -> Struct:
    return pf.row_groups[rg].items_of(RG_COLUMNS)[col]


def read_index(pf: ParquetFile, cc: Struct, which: str):
    off_f, len_f = (CC_CI_OFFSET, CC_CI_LENGTH) if which == "column" else (CC_OI_OFFSET, CC_OI_LENGTH)
    off, ln = cc.get(off_f), cc.get(len_f)
    if off is None or ln is None:
        return None
    if off < 4 or ln <= 0 or off + ln > pf.footer_start:
        raise ParquetMetaError(f"{which} index location out of range")
    try:
        s, end = decode_struct(pf.data, off, off + ln)
    except ThriftError as e:
        raise ParquetMetaError(f"{which} index: {e}") from e
    if end != off + ln:
        raise ParquetMetaError(f"{which} index length mismatch")
    return s


@dataclass
class Page:
    offset: int
    header_len: int
    compressed_size: int
    uncompressed_size: int
    type: int
    num_values: int
    num_rows: int | None
    encoding: int | None


def _int_field(st, fid, what, required=True, minimum=0):
    v = st.get(fid) if st is not None else None
    if v is None:
        if required:
            raise ParquetMetaError(f"{what} missing")
        return None
    if not isinstance(v, int) or isinstance(v, bool) or v < minimum:
        raise ParquetMetaError(f"{what} invalid: {v!r}")
    return v


def chunk_pages(pf: ParquetFile, cc: Struct, max_pages: int = 1 << 20):
    """Walk the page headers of a column chunk (dictionary and data pages).

    Every size and count must be present and non-negative, each page type must carry its own header
    struct, and each step must move forward inside the chunk (pos < next <= end), so a malformed chunk
    raises ParquetMetaError after a bounded amount of work.
    """
    md = cc.get(CC_META)
    if md is None:
        raise ParquetMetaError("column chunk without meta_data")
    dpo = _int_field(md, CMD_DATA_PAGE_OFFSET, "data_page_offset")
    start = md.get(CMD_DICT_PAGE_OFFSET)
    if not isinstance(start, int) or isinstance(start, bool) or start <= 0 or start > dpo:
        start = dpo
    total = _int_field(md, CMD_TOTAL_COMPRESSED, "total_compressed_size")
    end = start + total
    if start < 4 or end > pf.footer_start:
        raise ParquetMetaError("column chunk out of range")
    pages = []
    pos = start
    while pos < end:
        if len(pages) >= max_pages:
            raise ParquetMetaError("too many pages")
        try:
            h, hend = decode_struct(pf.data, pos, end)
        except ThriftError as e:
            raise ParquetMetaError(f"page header at {pos}: {e}") from e
        ptype = _int_field(h, PH_TYPE, f"page header at {pos}: type")
        csize = _int_field(h, PH_COMPRESSED, f"page header at {pos}: compressed_page_size")
        usize = _int_field(h, PH_UNCOMPRESSED, f"page header at {pos}: uncompressed_page_size")
        nxt = hend + csize
        if not (pos < nxt <= end):
            raise ParquetMetaError(f"page at {pos} does not end inside the column chunk")
        nr = enc = None
        if ptype == 0:
            d = h.get(PH_DATA_V1)
            if not isinstance(d, Struct):
                raise ParquetMetaError(f"DATA_PAGE at {pos} without data_page_header")
            nv = _int_field(d, DPH_NUM_VALUES, "data_page_header.num_values")
            enc = _int_field(d, DPH_ENCODING, "data_page_header.encoding")
        elif ptype == 3:
            d = h.get(PH_DATA_V2)
            if not isinstance(d, Struct):
                raise ParquetMetaError(f"DATA_PAGE_V2 at {pos} without data_page_header_v2")
            nv = _int_field(d, DPH2_NUM_VALUES, "data_page_header_v2.num_values")
            nn = _int_field(d, DPH2_NUM_NULLS, "data_page_header_v2.num_nulls")
            nr = _int_field(d, DPH2_NUM_ROWS, "data_page_header_v2.num_rows")
            enc = _int_field(d, DPH2_ENCODING, "data_page_header_v2.encoding")
            dl = _int_field(d, 5, "data_page_header_v2.definition_levels_byte_length")
            rl = _int_field(d, 6, "data_page_header_v2.repetition_levels_byte_length")
            if nn > nv or dl + rl > csize:
                raise ParquetMetaError(f"DATA_PAGE_V2 at {pos}: inconsistent counts or level lengths")
        elif ptype == 2:
            d = h.get(PH_DICT)
            if not isinstance(d, Struct):
                raise ParquetMetaError(f"DICTIONARY_PAGE at {pos} without dictionary_page_header")
            nv = _int_field(d, 1, "dictionary_page_header.num_values")
            enc = _int_field(d, 2, "dictionary_page_header.encoding")
        elif ptype == 1:
            nv = 0  # INDEX_PAGE: not used by Parquet writers; skipped
        else:
            raise ParquetMetaError(f"unknown page type {ptype} at {pos}")
        pages.append(Page(pos, hend - pos, csize, usize, ptype, nv, nr, enc))
        pos = nxt
    return pages


def data_pages(pf, cc):
    return [p for p in chunk_pages(pf, cc) if p.type in (0, 3)]


# --------------------------------------------------------------------------
# Rewriting the file tail (page indexes + footer), preserving data bytes.

def tail_start(pf: ParquetFile) -> int:
    """Offset where the page indexes (or, without them, the footer) begin."""
    t = pf.footer_start
    for rg in pf.row_groups:
        for cc in rg.items_of(RG_COLUMNS):
            for f in (CC_CI_OFFSET, CC_OI_OFFSET):
                v = cc.get(f)
                if v is not None:
                    t = min(t, v)
    return t


def load_indexes(pf: ParquetFile):
    """Return {(rg, col): [ColumnIndex|None, OffsetIndex|None]}."""
    out = {}
    for r, rg in enumerate(pf.row_groups):
        for c, cc in enumerate(rg.items_of(RG_COLUMNS)):
            out[(r, c)] = [read_index(pf, cc, "column"), read_index(pf, cc, "offset")]
    return out


def rebuild(pf: ParquetFile, meta: Struct, indexes: dict, prefix_end: int | None = None,
            raw_footer: bytes | None = None) -> bytes:
    """Write data bytes [0, prefix_end), then column indexes, offset indexes and the footer.

    Column chunk index offsets/lengths are recomputed; an index given as None is
    omitted and its offset/length fields are removed from the footer. This is the
    same layout Arrow C++ uses (all column indexes, then all offset indexes).
    """
    if prefix_end is None:
        prefix_end = tail_start(pf)
    out = bytearray(pf.data[:prefix_end])
    rgs = meta.items_of(FMD_ROW_GROUPS)
    for kind, (of, lf) in ((0, (CC_CI_OFFSET, CC_CI_LENGTH)), (1, (CC_OI_OFFSET, CC_OI_LENGTH))):
        for r, rg in enumerate(rgs):
            for c, cc in enumerate(rg.items_of(RG_COLUMNS)):
                idx = indexes.get((r, c), [None, None])[kind]
                if idx is None:
                    cc.remove(of)
                    cc.remove(lf)
                    continue
                enc = idx if isinstance(idx, (bytes, bytearray)) else encode_struct(idx)
                cc.set(of, CT_I64, len(out))
                cc.set(lf, CT_I32, len(enc))
                out += enc
    footer = encode_struct(meta) if raw_footer is None else raw_footer
    out += footer
    out += len(footer).to_bytes(4, "little")
    out += MAGIC
    return bytes(out)


def roundtrip_check(data: bytes) -> None:
    """Assert that decode+encode of footer and indexes reproduces the file exactly."""
    pf = read_parquet(data)
    idx = load_indexes(pf)
    again = rebuild(pf, pf.meta, idx)
    if again != data:
        raise AssertionError("parquet_meta round trip changed bytes")


# --------------------------------------------------------------------------
# Structural summary

def _hexlen(b):
    return "-" if b is None else str(len(b))


def summarize(data: bytes, name: str = "") -> str:
    pf = read_parquet(data)
    lines = []
    lines.append(f"file {name}: {len(data)} bytes, footer {pf.footer_len} bytes")
    lines.append(f"  num_rows {pf.meta.get(FMD_NUM_ROWS)}, row_groups {len(pf.row_groups)}, "
                 f"created_by {pf.meta.get(FMD_CREATED_BY, b'').decode('utf-8', 'replace')!r}")
    kv = pf.kv()
    stamps = {k: v for k, v in sorted(kv.items()) if k.startswith("seekparquet.")}
    other = sorted(k for k in kv if not k.startswith("seekparquet."))
    lines.append(f"  stamps {stamps}")
    if other:
        lines.append(f"  other key-value keys {other}")
    leaves = pf.leaf_columns()
    for path, el, d, r in leaves:
        t = el.get(SE_TYPE)
        tl = el.get(SE_TYPE_LENGTH)
        lines.append(f"  column {'.'.join(path)}: {TYPES[t] if t is not None else '?'}"
                     f"{'(' + str(tl) + ')' if t == 7 else ''} {REPETITION[el.get(SE_REPETITION, 0)]}"
                     f" def={d} rep={r}")
    key_col = next((i for i, (p, *_rest) in enumerate(leaves) if p == ("key",)), None)
    for ri, rg in enumerate(pf.row_groups):
        sc = rg.get(RG_SORTING)
        sc_txt = "absent" if sc is None else "[" + ", ".join(
            f"col{s.get(SC_COLUMN_IDX)}{' desc' if s.get(SC_DESCENDING) else ' asc'}" for s in sc.items) + "]"
        lines.append(f"  row_group {ri}: rows {rg.get(RG_NUM_ROWS)}, sorting_columns {sc_txt}")
        if not rg.get(RG_NUM_ROWS):
            lines.append("    (no rows; ignored by SeekParquet, RG-1..RG-7 do not apply)")
            continue
        for ci, cc in enumerate(rg.items_of(RG_COLUMNS)):
            if key_col is not None and ci != key_col:
                continue
            md = cc.get(CC_META)
            encs = ",".join(ENCODINGS.get(e, str(e)) for e in md.items_of(CMD_ENCODINGS))
            lines.append(f"    key chunk: codec {CODECS[md.get(CMD_CODEC)]}, encodings {encs}")
            try:
                pages = chunk_pages(pf, cc)
                lines.append("    page headers: " + " ".join(
                    f"{PAGE_TYPES[p.type]}({p.num_values})" for p in pages))
            except ParquetMetaError as e:
                lines.append(f"    page headers: unreadable ({e})")
            try:
                cix = read_index(pf, cc, "column")
                oix = read_index(pf, cc, "offset")
            except ParquetMetaError as e:
                lines.append(f"    page index: unreadable ({e})")
                continue
            if oix is None:
                lines.append("    offset index: absent")
            else:
                locs = oix.items_of(OI_PAGE_LOCATIONS)
                lines.append(f"    offset index: {len(locs)} pages, first_row_index "
                             f"{[p.get(PL_FIRST_ROW) for p in locs]}")
            if cix is None:
                lines.append("    column index: absent")
            else:
                nulls = cix.items_of(CI_NULL_PAGES)
                mins = cix.items_of(CI_MIN)
                maxs = cix.items_of(CI_MAX)
                bo = cix.get(CI_BOUNDARY)
                lines.append(f"    column index: {len(mins)} pages, boundary_order "
                             f"{BOUNDARY_ORDER[bo] if bo is not None and bo < 3 else bo}, null_pages "
                             f"{'none' if not any(nulls) else [i for i, n in enumerate(nulls) if n]}")
                lines.append("      min lengths " + str([len(m) for m in mins]))
                lines.append("      max lengths " + str([len(m) for m in maxs]))
    return "\n".join(lines) + "\n"


def main(argv):
    if not argv:
        print("usage: parquet_meta.py FILE.parquet...", file=sys.stderr)
        return 2
    for p in argv:
        with open(p, "rb") as f:
            sys.stdout.write(summarize(f.read(), p))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
