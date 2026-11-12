"""Raw Parquet checks shared by the Python reference and validator.

Classification precedes Arrow decoding. This module contains no query semantics.
Annotation restrictions follow parquet-format ec90dd4661ff1c4d77f06b5b5232c5aedf68ffc5.
"""
import copy
import math

import pyarrow as pa

import parquet_meta as p


class RuleError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def need(ok, code, message):
    if not ok:
        raise RuleError(code, message)


def field(st, fid, kind, code):
    need(isinstance(st, p.Struct), code, 'missing required struct')
    found = [(t, v) for f, t, v in st.fields if f == fid]
    need(len(found) == 1 and found[0][0] == kind, code, f'missing or ill-typed field {fid}')
    return found[0][1]


def annotations(pf):
    for kv in pf.meta.items_of(p.FMD_KV):
        field(kv, p.KV_KEY, p.CT_BINARY, 'invalid_footer')
        if kv.has(p.KV_VALUE):
            field(kv, p.KV_VALUE, p.CT_BINARY, 'invalid_footer')
    for group in pf.row_groups:
        for cc in group.items_of(p.RG_COLUMNS):
            field(cc.get(p.CC_META), p.CMD_CODEC, p.CT_I32, 'invalid_footer')
    for el in pf.schema[1:]:
        if el.has(6):
            field(el, 6, p.CT_I32, 'invalid_footer')
        if el.get(p.SE_NUM_CHILDREN, 0):
            continue
        code = 'invalid_footer'
        t = field(el, p.SE_TYPE, p.CT_I32, code)
        length = el.get(p.SE_TYPE_LENGTH)
        need(t in range(8) and (t != 7 or type(length) is int and length > 0), code, 'physical leaf type')
        def decimal(precision, scale):
            need(type(precision) is int and type(scale) is int and precision > 0 and 0 <= scale <= precision,
                 code, 'decimal parameters')
            ceiling = {1: 9, 2: 18, 6: precision, 7: math.floor((8 * length - 1) * math.log10(2)) if t == 7 else 0}
            need(t in ceiling and precision <= ceiling[t], code, 'decimal carrier precision')
        lt = el.get(10)
        if lt is not None:
            need(isinstance(lt, p.Struct) and len(lt.fields) == 1, code, 'empty or invalid logical union')
            kind, wire, params = lt.fields[0]
            need(wire == p.CT_STRUCT, code, 'logical union wire type')
            if kind in (1, 4, 12, 13, 17, 18):
                need(t == 6, code, 'binary annotation carrier')
            elif kind in (2, 3, 16):
                need(False, code, 'group annotation on leaf')
            elif kind == 5:
                decimal(field(params, 2, p.CT_I32, code), field(params, 1, p.CT_I32, code))
            elif kind == 6:
                need(t == 1, code, 'DATE carrier')
            elif kind in (7, 8):
                field(params, 1, p.CT_BOOL, code)
                unit = field(params, 2, p.CT_STRUCT, code)
                need(len(unit.fields) == 1, code, 'empty or invalid time unit union')
                u, wire, _ = unit.fields[0]
                need(wire == p.CT_STRUCT, code, 'time unit wire type')
                if u in (1, 2, 3):
                    carrier_ok = t == (1 if u == 1 else 2) if kind == 7 else (t == 2 or t == 7 and length == 12)
                    need(carrier_ok, code, 'time/timestamp carrier')
            elif kind == 10:
                width = field(params, 1, p.CT_BYTE, code)
                field(params, 2, p.CT_BOOL, code)
                need(width in (8, 16, 32, 64) and t == (2 if width == 64 else 1), code, 'integer width/carrier')
            elif kind in (14, 15):
                need(t == 7 and length == (16 if kind == 14 else 2), code, 'fixed annotation length')
        ct = el.get(6)
        if ct in (0, 4, 19, 20):
            need(t == 6, code, 'converted binary carrier')
        elif ct in (1, 2, 3):
            need(False, code, 'converted group annotation on leaf')
        elif ct == 5:
            decimal(field(el, 8, p.CT_I32, code), el.get(7, 0))
        elif ct in (6, 7, 11, 12, 13, 15, 16, 17):
            need(t == 1, code, 'converted INT32 carrier')
        elif ct in (8, 9, 10, 14, 18):
            need(t == 2, code, 'converted INT64 carrier')
        elif ct == 21:
            need(t == 7 and length == 12, code, 'INTERVAL carrier')


def page_header(pf, offset, end, *, indexed=False):
    code = 'invalid_page_index' if indexed else 'invalid_parquet'
    try:
        h, stop = p.decode_struct(pf.data, offset, end)
        typ = field(h, p.PH_TYPE, p.CT_I32, code)
        compressed = field(h, p.PH_COMPRESSED, p.CT_I32, code)
        uncompressed = field(h, p.PH_UNCOMPRESSED, p.CT_I32, code)
        counts, levels = [], []
        rows = None
        if typ == 0:
            d = field(h, p.PH_DATA_V1, p.CT_STRUCT, code)
            rows = field(d, p.DPH_NUM_VALUES, p.CT_I32, code)
            counts = [rows]
            for f in (2, 3, 4):  # encoding, definition encoding, repetition encoding
                field(d, f, p.CT_I32, code)
        elif typ == 3:
            d = field(h, p.PH_DATA_V2, p.CT_STRUCT, code)
            counts = [field(d, f, p.CT_I32, code) for f in (1, 2, 3)]
            rows = counts[2]
            field(d, 4, p.CT_I32, code)
            levels = [field(d, f, p.CT_I32, code) for f in (5, 6)]
        elif typ == 2:
            d = field(h, p.PH_DICT, p.CT_STRUCT, code)
            counts = [field(d, 1, p.CT_I32, code)]
            field(d, 2, p.CT_I32, code)
    except (p.ThriftError, IndexError, TypeError) as e:
        raise RuleError(code, str(e)) from None
    need(all(n >= 0 for n in [compressed, uncompressed, *counts, *levels]),
         'invalid_parquet', 'negative page size, count or level length')
    need(not levels or sum(levels) <= min(compressed, uncompressed),
         'invalid_parquet', 'invalid V2 level lengths')
    return h, stop - offset, rows


def physical_pages(pf, cc, max_pages=1 << 20):
    """Enumerate the whole chunk independently of its indexes, retaining raw enum integers."""
    md = cc.get(p.CC_META)
    start = field(md, p.CMD_DATA_PAGE_OFFSET, p.CT_I64, 'invalid_parquet')
    dictionary = md.get(p.CMD_DICT_PAGE_OFFSET)
    if type(dictionary) is int and 0 < dictionary < start:
        start = dictionary
    end = start + field(md, p.CMD_TOTAL_COMPRESSED, p.CT_I64, 'invalid_parquet')
    need(4 <= start < end <= pf.footer_start, 'invalid_parquet', 'column chunk extent')
    pages, offset = [], start
    while offset < end:
        if len(pages) >= max_pages:
            raise p.ResourceLimit('configured pages_per_chunk limit exceeded')
        h, length, rows = page_header(pf, offset, end)
        typ, compressed, uncompressed = h.get(p.PH_TYPE), h.get(p.PH_COMPRESSED), h.get(p.PH_UNCOMPRESSED)
        stop = offset + length + compressed
        need(offset < stop <= end, 'invalid_parquet', 'physical page extent')
        need(typ in (0, 1, 2, 3), 'invalid_parquet', 'unknown page type')
        d = h.get(p.PH_DATA_V1 if typ == 0 else p.PH_DATA_V2 if typ == 3 else p.PH_DICT)
        nv, enc = (0, None) if typ == 1 else (d.get(1), d.get(4 if typ == 3 else 2))
        if typ == 3:
            need(d.get(2) <= nv, 'invalid_parquet', 'V2 null count')
        pages.append(p.Page(offset, length, compressed, uncompressed, typ, nv, rows if typ == 3 else None, enc))
        offset = stop
    return pages


def indexed_headers(pf, cc, locations, total_rows):
    md = cc.get(p.CC_META)
    start = min(md.get(p.CMD_DATA_PAGE_OFFSET), md.get(p.CMD_DICT_PAGE_OFFSET, md.get(p.CMD_DATA_PAGE_OFFSET)))
    end = start + md.get(p.CMD_TOTAL_COMPRESSED)
    firsts = [x.get(p.PL_FIRST_ROW) for x in locations]
    need(bool(firsts) and all(type(x) is int for x in firsts) and firsts[0] == 0
         and all(a < b for a, b in zip(firsts, firsts[1:])) and firsts[-1] < total_rows,
         'invalid_page_index', 'indexed row intervals')
    for i, loc in enumerate(locations):
        off, size = loc.get(p.PL_OFFSET), loc.get(p.PL_SIZE)
        need(type(off) is int and type(size) is int and start <= off < end and size > 0
             and off + size <= end <= pf.footer_start, 'invalid_page_index', 'indexed extent')
        h, length, rows = page_header(pf, off, end, indexed=True)
        need(h.get(p.PH_TYPE) in (0, 3) and length + h.get(p.PH_COMPRESSED) == size
             and rows == (firsts[i + 1] if i + 1 < len(firsts) else total_rows) - firsts[i],
             'invalid_page_index', 'indexed page type, extent or row count')


def encoding_support(pf, cc, pages):
    codec = field(cc.get(p.CC_META), p.CMD_CODEC, p.CT_I32, 'invalid_footer')
    if codec not in (0, 1, 2, 4, 6, 7):
        raise RuleError('unsupported_encoding', f'compression codec {codec}')
    codec_names = {1: 'snappy', 2: 'gzip', 4: 'brotli', 6: 'zstd', 7: 'lz4_raw'}
    if codec and not pa.Codec.is_available(codec_names[codec]):
        raise RuleError('unsupported_encoding', f'compression codec {codec}')
    for page in pages:
        # Includes raw required-field checks for ignored repetition encoding.
        page_header(pf, page.offset, pf.footer_start)
        if page.encoding is not None and page.encoding not in (0, 2, 3, 5, 6, 7, 8, 9):
            raise RuleError('unsupported_encoding', f'page encoding {page.encoding}')


def arrow_bytes(pf, selected):
    """Hide unsupported unselected codecs and annotations Arrow cannot represent.

    Raw metadata has already been checked. Bodies, offsets and indexes stay intact.
    Recognized converted annotations remain the fallback for unknown logical types.
    """
    meta = copy.deepcopy(pf.meta)
    for group in meta.items_of(p.FMD_ROW_GROUPS):
        for i, cc in enumerate(group.items_of(p.RG_COLUMNS)):
            if i not in selected:
                cc.get(p.CC_META).set(p.CMD_CODEC, p.CT_I32, 0)
    for el in meta.items_of(p.FMD_SCHEMA):
        lt = el.get(10)
        if lt is not None and lt.fields:
            kind, _, params = lt.fields[0]
            unknown_unit = kind in (7, 8) and params.get(2).fields[0][0] not in (1, 2, 3)
            if kind not in (1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18):
                el.remove(10)
            elif unknown_unit or kind == 8 and el.get(p.SE_TYPE) == 7:
                el.remove(10)
                el.remove(6)
        if el.get(6) is not None and el.get(6) not in range(22):
            el.remove(6)
    raw = p.encode_struct(meta)
    return bytes(pf.data[:pf.footer_start]) + raw + len(raw).to_bytes(4, 'little') + b'PAR1'
