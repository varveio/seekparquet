#!/usr/bin/env python3
"""Proposal 0001 part B: isolated synthetic classification fixtures.

Success expectations use these source rows, never a reader. Fault declarations are
in faults.json; the exact admitted request/validation matrix is in make_cases.py.
"""
from __future__ import annotations

import parquet_meta as p
from common import DATASETS, ROWS, DEFAULT_KEY, b64e, dumps, write_text

NAMES = {
    'unknown-key-encoding-99-v1', 'unknown-value-encoding-99', 'unknown-codec-99',
    'ignored-enums-99', 'missing-boundary-order', 'missing-repetition-encoding-value',
    'metadata-no-value', 'stamp-duplicate-one-valueless', 'key-header-negative-num-values-v1',
    'key-index-undecodable-header', 'annotation-physical-fallback-controls',
    'key-index-valid-row-count-mismatch', 'annotation-decimal-int64-p19-unprojected',
    'annotation-timestamp-empty-unit',
}
VALID = {'ignored-enums-99', 'metadata-no-value', 'annotation-physical-fallback-controls'}


def logical(member, params=None):
    return p.Struct([(member, p.CT_STRUCT, params if params is not None else p.Struct())])


def temporal(unit):
    return p.Struct([(1, p.CT_BOOL, False), (2, p.CT_STRUCT, unit)])


def decimal(precision):
    return logical(5, p.Struct([(1, p.CT_I32, 0), (2, p.CT_I32, precision)]))


def source(controls=False):
    import pyarrow_fixtures as native
    columns = [('value', 'int64')]
    if controls:
        columns += [('decimal_i32', 'int32'), ('decimal_i64', 'int64'), ('decimal_flba1', 'fixed:1'),
                    ('timestamp_flba12', 'fixed:12'), ('future_logical', 'int64'),
                    ('future_converted', 'int64'), ('future_time', 'int64')]
    rows = []
    for i, key in enumerate((b'a', b'b', b'c', b'd')):
        values = {name: (native.S(bytes([i + 1]) + b'\0' * (int(typ[6:]) - 1))
                         if typ.startswith('fixed:') else native.I(i + 1)) for name, typ in columns}
        if i == 1:
            values['value'] = native.NULL
        rows.append({'key_base64': b64e(key), 'values': values})
    return {'dataset': 'rulings-controls' if controls else 'rulings',
            'description': 'Synthetic physical source rows for proposal 0001 part B; V1 PLAIN, four rows in two pages.',
            'schema_id': 'conformance/rulings/v1', 'key_type': 'binary', 'key': dict(DEFAULT_KEY),
            'columns': [{'name': name, 'type': typ, 'nullable': True} for name, typ in columns],
            'parts': [{'path': 'data/part-00000.parquet', 'row_groups': [4], 'writer': native.W(2), 'rows': rows}]}


def rewrite_pages(part, edits):
    """Rebuild physical pages with their payloads unchanged, repairing all dependent offsets/sizes.

    edits maps (column, page) to a header mutation. This permits the extra byte in
    compact I32(99), and a missing required field, without introducing extent faults.
    """
    data = bytearray(b'PAR1')
    group = part.meta.items_of(p.FMD_ROW_GROUPS)[0]
    total_uncompressed = 0
    for col, cc in enumerate(group.items_of(p.RG_COLUMNS)):
        original = p.column_chunk(part.pf, 0, col)
        md = cc.get(p.CC_META)
        start = len(data)
        delta = 0
        locations = part.idx[(0, col)][1].items_of(p.OI_PAGE_LOCATIONS)
        for page_no, page in enumerate(p.data_pages(part.pf, original)):
            header, _ = p.decode_struct(part.pf.data, page.offset)
            if (col, page_no) in edits:
                edits[(col, page_no)](header)
            encoded = p.encode_struct(header)
            locations[page_no].set(p.PL_OFFSET, p.CT_I64, len(data))
            locations[page_no].set(p.PL_SIZE, p.CT_I32, len(encoded) + page.compressed_size)
            data += encoded
            data += part.pf.data[page.offset + page.header_len:page.offset + page.header_len + page.compressed_size]
            delta += len(encoded) - page.header_len
        md.set(p.CMD_DATA_PAGE_OFFSET, p.CT_I64, start)
        md.set(p.CMD_TOTAL_COMPRESSED, p.CT_I64, len(data) - start)
        md.set(p.CMD_TOTAL_UNCOMPRESSED, p.CT_I64, md.get(p.CMD_TOTAL_UNCOMPRESSED) + delta)
        total_uncompressed += md.get(p.CMD_TOTAL_UNCOMPRESSED)
    group.set(p.RG_TOTAL_BYTE_SIZE, p.CT_I64, total_uncompressed)
    if group.has(p.RG_TOTAL_COMPRESSED):
        group.set(p.RG_TOTAL_COMPRESSED, p.CT_I64, len(data) - 4)
    part.pf.data = bytes(data)
    # No old indexes/footer remain in the rewritten prefix.
    part.pf.footer_start = len(data)
    for rg in part.pf.row_groups:
        for cc in rg.items_of(p.RG_COLUMNS):
            for fid in (p.CC_CI_OFFSET, p.CC_OI_OFFSET):
                cc.remove(fid)


def build():
    import pyarrow_fixtures as native
    from mutate_metadata import Part
    records = []
    for controls in (False, True):
        doc = source(controls)
        write_text(ROWS / f"{doc['dataset']}.json", dumps(native.jsonable_rows_doc(doc)))
    for name in sorted(NAMES):
        controls = name == 'annotation-physical-fallback-controls'
        doc = source(controls)
        raw = native.write_part(doc, doc['parts'][0])
        part = Part(raw)
        description = ''
        edit = None
        schema = part.meta.items_of(p.FMD_SCHEMA)
        if name.startswith('unknown-') and 'encoding' in name:
            col = 0 if 'key' in name else 1
            edit = {(col, 0): lambda h: h.get(p.PH_DATA_V1).set(2, p.CT_I32, 99)}
            # Metadata advertises the same future encoding as the page.
            part.meta.items_of(p.FMD_ROW_GROUPS)[0].items_of(p.RG_COLUMNS)[col].get(p.CC_META).get(p.CMD_ENCODINGS).items.append(99)
            description = f'Column {col} first V1 data-page value encoding is 99; column encodings also includes 99.'
        elif name == 'unknown-codec-99':
            part.meta.items_of(p.FMD_ROW_GROUPS)[0].items_of(p.RG_COLUMNS)[1].get(p.CC_META).set(p.CMD_CODEC, p.CT_I32, 99)
            description = 'Optional value column CompressionCodec is 99; key codec is unchanged.'
        elif name == 'ignored-enums-99':
            part.key_ci(0).set(p.CI_BOUNDARY, p.CT_I32, 99)
            edit = {(col, page): lambda h: h.get(p.PH_DATA_V1).set(4, p.CT_I32, 99)
                    for col in (0, 1) for page in (0, 1)}
            description = 'Key boundary_order and every non-repeated key/value repetition_level_encoding are 99.'
        elif name == 'missing-boundary-order':
            part.key_ci(0).remove(p.CI_BOUNDARY)
            description = 'Required boundary_order absent from the key ColumnIndex.'
        elif name == 'missing-repetition-encoding-value':
            edit = {(1, 0): lambda h: h.get(p.PH_DATA_V1).remove(4)}
            description = 'Required repetition_level_encoding absent from projected optional value column first V1 page.'
        elif name in ('metadata-no-value', 'stamp-duplicate-one-valueless'):
            keys = ['ordinary', 'ordinary', 'seekparquet.future', 'seekparquet.future'] if name == 'metadata-no-value' else ['seekparquet.schema_id']
            part.meta.get(p.FMD_KV).items += [p.Struct([(p.KV_KEY, p.CT_BINARY, key.encode())]) for key in keys]
            description = 'Append valueless metadata entries: ' + ', '.join(keys) + '.'
        elif name == 'key-header-negative-num-values-v1':
            edit = {(0, 0): lambda h: h.get(p.PH_DATA_V1).set(1, p.CT_I32, -5)}
            description = 'First indexed key V1 header num_values is -5; index row interval unchanged.'
        elif name == 'key-index-valid-row-count-mismatch':
            edit = {(0, 0): lambda h: h.get(p.PH_DATA_V1).set(1, p.CT_I32, 3)}
            description = 'First indexed key V1 header num_values is positive 3; index still declares 2 rows.'
        elif name == 'key-index-undecodable-header':
            edit = {(0, 0): lambda h: h.fields.clear()}
            description = 'First indexed key header replaced with an empty Thrift struct (STOP); required fields absent.'
        elif name == 'annotation-decimal-int64-p19-unprojected':
            schema[2].set(10, p.CT_STRUCT, decimal(19))
            description = 'Optional INT64 value leaf is DECIMAL(19,0), exceeding INT64 precision ceiling 18; query does not project it.'
        elif name == 'annotation-timestamp-empty-unit':
            schema[2].set(10, p.CT_STRUCT, logical(8, temporal(p.Struct())))
            description = 'Optional INT64 value leaf TIMESTAMP has an empty required TimeUnit union.'
        elif controls:
            for idx, precision in ((3, 9), (4, 18), (5, 2)):
                schema[idx].set(10, p.CT_STRUCT, decimal(precision))
            schema[6].set(10, p.CT_STRUCT, logical(8, temporal(logical(2))))
            schema[7].set(10, p.CT_STRUCT, logical(99))
            schema[8].set(6, p.CT_I32, 99)
            schema[9].set(10, p.CT_STRUCT, logical(8, temporal(logical(99))))
            description = ('Legal DECIMAL INT32(9), INT64(18), FLBA(1)(2); TIMESTAMP(MICROS) FLBA(12); '
                           'unknown logical 99, converted 99 and timestamp unit 99 retain physical values.')
        else:
            raise AssertionError(name)
        if edit:
            rewrite_pages(part, edit)
        data = part.build()
        out = DATASETS / (name if name in VALID else f'invalid/{name}')
        path = out / doc['parts'][0]['path']
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        m = native.manifest_for(doc, [data])
        write_text(out / 'manifest.json', dumps(m))
        records.append({'name': name, 'dir': str(out.relative_to(DATASETS.parent)), 'valid': name in VALID,
                        'description': description, 'rows': f"generators/rows/{doc['dataset']}.json",
                        'producer': native.PRODUCER + ' + generators/mutate_metadata.py (rulings.py)',
                        'provenance': 'mutated', 'command': native.COMMAND,
                        'changes': [description, 'Data payloads preserved byte for byte; page header lengths, dependent '
                                    'chunk/index offsets and sizes, footer and manifest byte size repaired as needed.']})
    return records
