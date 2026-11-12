"""Generated physical fixtures; no committed Parquet or expected-row oracle."""
import base64
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "generators"))
import parquet_meta as p  # noqa: E402
from validate import Limits, validate_dataset  # noqa: E402


def fixture(root, *, version='2.0', codec='NONE', unique=True, keys=None, values=None):
    keys = keys if keys is not None else [f'k{i:05}'.encode() for i in range(4000)]
    values = values if values is not None else list(range(len(keys)))
    stamps = {b'seekparquet.format_version': b'0', b'seekparquet.draft_revision': b'1',
              b'seekparquet.schema_id': b'test', b'seekparquet.key_column': b'key',
              b'seekparquet.key_unique': str(unique).lower().encode()}
    schema = pa.schema([pa.field('key', pa.binary(), nullable=False), pa.field('value', pa.int64())], metadata=stamps)
    table = pa.Table.from_arrays([pa.array(keys), pa.array(values, type=pa.int64())], schema=schema)
    path = root / 'part.parquet'
    pq.write_table(table, path, data_page_version=version, compression=codec, use_dictionary=False,
                   data_page_size=1024, write_batch_size=128, write_page_index=True,
                   sorting_columns=[pq.SortingColumn(0)], write_page_checksum=True)
    entry = {'path': path.name, 'bytes': path.stat().st_size, 'rows': len(keys),
             'min_key_base64': base64.b64encode(keys[0]).decode(),
             'max_key_base64': base64.b64encode(keys[-1]).decode()}
    m = {'format': 'seekparquet', 'format_version': 0, 'draft_revision': 1, 'schema_id': 'test',
         'key': {'column': 'key', 'unique': unique}, 'parts': [entry]}
    (root / 'manifest.json').write_text(json.dumps(m))
    return m


def save(root, m):
    m['parts'][0]['bytes'] = (root / 'part.parquet').stat().st_size
    (root / 'manifest.json').write_text(json.dumps(m))


def rewrite_indexes(path, mutate):
    pf = p.read_parquet(path.read_bytes())
    indexes = []
    start = pf.footer_start
    for group in pf.row_groups:
        for cc in group.items_of(p.RG_COLUMNS):
            ci, oi = p.read_index(pf, cc, 'column'), p.read_index(pf, cc, 'offset')
            indexes.append((cc, ci, oi))
            for f in (p.CC_CI_OFFSET, p.CC_OI_OFFSET):
                if cc.get(f) is not None:
                    start = min(start, cc.get(f))
    mutate(indexes)
    data = bytearray(pf.data[:start])
    for cc, ci, oi in indexes:
        for index, off, size in ((ci, p.CC_CI_OFFSET, p.CC_CI_LENGTH), (oi, p.CC_OI_OFFSET, p.CC_OI_LENGTH)):
            if index is not None:
                raw = p.encode_struct(index)
                cc.set(off, p.CT_I64, len(data))
                cc.set(size, p.CT_I32, len(raw))
                data.extend(raw)
    footer = p.encode_struct(pf.meta)
    path.write_bytes(data + footer + len(footer).to_bytes(4, 'little') + b'PAR1')


class ValidatorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
    def tearDown(self):
        self.temp.cleanup()
    def test_valid_page_versions_and_codecs(self):
        for version in ('1.0', '2.0'):
            for codec in ('NONE', 'snappy', 'gzip', 'zstd'):
                with self.subTest(version=version, codec=codec):
                    fixture(self.root, version=version, codec=codec)
                    self.assertEqual({x: {'result': 'complete'} for x in ('structural', 'profile', 'full')},
                                     validate_dataset(self.root))
    def test_plausible_index_omits_physical_middle_page(self):
        m = fixture(self.root)
        def omit(indexes):
            _, ci, oi = indexes[0]
            locs = oi.items_of(p.OI_PAGE_LOCATIONS)
            self.assertGreater(len(locs), 2)
            # Drop a physical page, stretch predecessor's indexed row interval.
            # Every remaining location is real, sorted, in bounds; list lengths agree.
            del locs[1]
            for f in (p.CI_NULL_PAGES, p.CI_MIN, p.CI_MAX):
                del ci.get(f).items[1]
        rewrite_indexes(self.root / 'part.parquet', omit)
        save(self.root, m)
        result = validate_dataset(self.root)
        self.assertEqual(result['structural']['code'], 'invalid_page_index')
        self.assertEqual(result['profile']['result'], 'incomplete')
    def test_profile_order_and_bounds(self):
        fixture(self.root, keys=[b'b', b'a', b'c'])
        self.assertEqual(validate_dataset(self.root)['profile']['code'], 'key_order')
        m = fixture(self.root)
        m['parts'][0]['max_key_base64'] = base64.b64encode(b'zz').decode()
        save(self.root, m)
        self.assertEqual(validate_dataset(self.root)['profile']['code'], 'bounds_mismatch')
    def test_digest_has_no_parquet_prerequisite(self):
        m = fixture(self.root)
        path = self.root / 'part.parquet'
        path.write_bytes(b'not a parquet file')
        m['parts'][0]['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        save(self.root, m)
        self.assertEqual(validate_dataset(self.root, 'digest')['digest']['result'], 'complete')
        self.assertEqual(validate_dataset(self.root)['structural']['code'], 'invalid_footer')
        m['parts'][0]['sha256'] = '0' * 64
        save(self.root, m)
        self.assertEqual(validate_dataset(self.root, 'digest')['digest']['code'], 'digest_mismatch')
    def test_digest_nothing_checked(self):
        fixture(self.root)
        result = validate_dataset(self.root, 'digest')['digest']
        self.assertEqual(result['checked'], 0)
        self.assertIn('nothing checked', result['message'])
    def test_limit_and_unsupported_keep_lower_results(self):
        fixture(self.root)
        result = validate_dataset(self.root, limits=Limits(rows_per_group=2))
        self.assertEqual(result['structural']['result'], 'complete')
        self.assertEqual(result['profile']['category'], 'limit')
        with patch('validate.pq.ParquetFile', side_effect=pa.ArrowNotImplementedError('test codec')):
            result = validate_dataset(self.root)
        self.assertEqual(result['structural']['result'], 'complete')
        self.assertEqual(result['profile']['category'], 'unsupported')
        self.assertEqual(result['full']['category'], 'prerequisite')
    def test_physical_and_decode_limits(self):
        fixture(self.root)
        result = validate_dataset(self.root, limits=Limits(pages_per_chunk=1))
        self.assertEqual(result['structural']['category'], 'limit')
        result = validate_dataset(self.root, limits=Limits(uncompressed_bytes_per_group=1))
        self.assertEqual(result['structural']['result'], 'complete')
        self.assertEqual(result['profile']['category'], 'limit')

    def test_declared_unsupported_codec(self):
        m = fixture(self.root)
        def lzo(indexes):
            indexes[0][0].get(p.CC_META).set(p.CMD_CODEC, p.CT_I32, 3)
        rewrite_indexes(self.root / 'part.parquet', lzo)
        save(self.root, m)
        result = validate_dataset(self.root)
        self.assertEqual(result['structural']['result'], 'complete')
        self.assertEqual(result['profile']['category'], 'unsupported')

    def test_unvisited_value_page_crc_corruption(self):
        m = fixture(self.root)
        path = self.root / 'part.parquet'
        data = bytearray(path.read_bytes())
        pf = p.read_parquet(data)
        value = pf.row_groups[0].items_of(p.RG_COLUMNS)[1]
        page = p.data_pages(pf, value)[-1]
        data[page.offset + page.header_len + page.compressed_size - 1] ^= 1
        path.write_bytes(data)
        result = validate_dataset(self.root)
        self.assertEqual(result['profile']['result'], 'complete')
        self.assertEqual(result['full']['code'], 'invalid_parquet')

    def test_storage_and_invalid_request(self):
        fixture(self.root)
        with patch('pathlib.Path.open', side_effect=PermissionError('denied')):
            self.assertEqual(validate_dataset(self.root)['structural']['category'], 'storage')
        with self.assertRaises(ValueError):
            validate_dataset(self.root, 'other')
        with self.assertRaises(ValueError):
            Limits(page_bytes=0)
    def test_malformed_manifest(self):
        (self.root / 'manifest.json').write_text('{"format":1,"format":2}')
        self.assertEqual(validate_dataset(self.root)['structural']['code'], 'invalid_manifest')
        fixture(self.root)
        manifest_path = self.root / 'manifest.json'
        raw = manifest_path.read_text().replace('"format_version": 0', '"format_version": 0.0')
        manifest_path.write_text(raw)
        self.assertEqual(validate_dataset(self.root)['structural']['code'], 'invalid_manifest')


def rewrite_test_footer(root, m, mutate):
    """Local synthetic mutations; never edit the shared corpus."""
    path = root / 'part.parquet'
    pf = p.read_parquet(path.read_bytes())
    mutate(pf.meta)
    raw = p.encode_struct(pf.meta)
    path.write_bytes(pf.data[:pf.footer_start] + raw + len(raw).to_bytes(4, 'little') + b'PAR1')
    save(root, m)


def rewrite_test_header(root, column, mutate):
    """Keep page extents intact using an ignored header padding field, replacing the CRC.

    The payload is unchanged; removing the optional CRC avoids a checksum fault.
    """
    path = root / 'part.parquet'
    pf = p.read_parquet(path.read_bytes())
    cc = pf.row_groups[0].items_of(p.RG_COLUMNS)[column]
    page = p.data_pages(pf, cc)[0]
    header, _ = p.decode_struct(pf.data, page.offset)
    mutate(header)
    header.remove(4)  # optional CRC
    for n in range(32):
        header.set(15, p.CT_BINARY, b'\0' * n)
        raw = p.encode_struct(header)
        if len(raw) == page.header_len:
            break
    else:
        raise AssertionError('could not preserve synthetic header length')
    data = bytearray(pf.data)
    data[page.offset:page.offset + page.header_len] = raw
    path.write_bytes(data)


class RulingsTest(unittest.TestCase):
    setUp = ValidatorTest.setUp
    tearDown = ValidatorTest.tearDown
    def test_timestamp_fixed_carrier(self):
        timestamp_fixed_fixture(self.root)
        self.assertTrue(all(x['result'] == 'complete' for x in validate_dataset(self.root).values()))

    def test_unknown_encoding_and_codec_levels(self):
        for version in ('1.0', '2.0'):
            for column in (0, 1):
                for feature in ('encoding', 'codec'):
                    with self.subTest(version=version, column=column, feature=feature):
                        m = fixture(self.root, version=version)
                        if feature == 'codec':
                            rewrite_test_footer(self.root, m, lambda meta: meta.items_of(p.FMD_ROW_GROUPS)[0]
                                                .items_of(p.RG_COLUMNS)[column].get(p.CC_META)
                                                .set(p.CMD_CODEC, p.CT_I32, 99))
                        else:
                            rewrite_test_header(self.root, column, lambda h: h.get(p.PH_DATA_V1 if version == '1.0'
                                                else p.PH_DATA_V2).set(2 if version == '1.0' else 4, p.CT_I32, 99))
                        report = validate_dataset(self.root)
                        self.assertEqual(report['structural']['result'], 'complete', report)
                        level = 'profile' if column == 0 else 'full'
                        self.assertEqual(report[level]['result'], 'incomplete', report)
                        self.assertEqual(report[level]['code'], 'unsupported_encoding', report)
                        if column == 1:
                            self.assertEqual(report['profile']['result'], 'complete', report)

    def test_ignored_enums_and_required_presence(self):
        m = fixture(self.root, version='1.0')
        for column in (0, 1):
            rewrite_test_header(self.root, column, lambda h: h.get(p.PH_DATA_V1).set(4, p.CT_I32, 99))
        rewrite_indexes(self.root / 'part.parquet', lambda indexes: indexes[0][1].set(p.CI_BOUNDARY, p.CT_I32, 99))
        save(self.root, m)
        self.assertTrue(all(x['result'] == 'complete' for x in validate_dataset(self.root).values()))
        rewrite_test_header(self.root, 1, lambda h: h.get(p.PH_DATA_V1).remove(4))
        self.assertEqual(validate_dataset(self.root)['full']['code'], 'invalid_parquet')
        m = fixture(self.root)
        rewrite_indexes(self.root / 'part.parquet', lambda indexes: indexes[0][1].remove(p.CI_BOUNDARY))
        save(self.root, m)
        self.assertEqual(validate_dataset(self.root)['structural']['code'], 'invalid_page_index')

    def test_optional_metadata_values(self):
        def optional(meta):
            for key in (b'ordinary', b'seekparquet.future'):
                for _ in range(2):
                    meta.get(p.FMD_KV).items.append(p.Struct([(p.KV_KEY, p.CT_BINARY, key)]))
        m = fixture(self.root)
        rewrite_test_footer(self.root, m, optional)
        self.assertTrue(all(x['result'] == 'complete' for x in validate_dataset(self.root).values()))
        for stamp in ('format_version', 'draft_revision', 'schema_id', 'key_column', 'key_unique'):
            for duplicate in (False, True):
                with self.subTest(stamp=stamp, duplicate=duplicate):
                    m = fixture(self.root)
                    def missing(meta):
                        kv = next(x for x in meta.items_of(p.FMD_KV) if x.get(p.KV_KEY) == b'seekparquet.' + stamp.encode())
                        if duplicate:
                            meta.get(p.FMD_KV).items.append(p.Struct([(p.KV_KEY, p.CT_BINARY, kv.get(p.KV_KEY))]))
                        else:
                            kv.remove(p.KV_VALUE)
                    rewrite_test_footer(self.root, m, missing)
                    self.assertEqual(validate_dataset(self.root)['structural']['code'], 'stamp_mismatch')
        m = fixture(self.root)
        rewrite_test_footer(self.root, m, lambda meta: meta.get(p.FMD_KV).items.append(p.Struct()))
        self.assertEqual(validate_dataset(self.root)['structural']['code'], 'invalid_footer')

    def test_indexed_header_precedence(self):
        cases = [
            ('1.0', lambda h: h.get(p.PH_DATA_V1).set(1, p.CT_I32, -5), 'invalid_parquet'),
            ('2.0', lambda h: h.get(p.PH_DATA_V2).set(1, p.CT_I32, -5), 'invalid_parquet'),
            ('2.0', lambda h: h.get(p.PH_DATA_V2).set(5, p.CT_I32, -1), 'invalid_parquet'),
            ('1.0', lambda h: h.set(p.PH_COMPRESSED, p.CT_I32, -1), 'invalid_parquet'),
            ('1.0', lambda h: h.set(p.PH_COMPRESSED, p.CT_I32, 1000000), 'invalid_page_index'),
            ('1.0', lambda h: h.get(p.PH_DATA_V1).set(1, p.CT_I32, 129), 'invalid_page_index'),
            ('1.0', lambda h: h.set(p.PH_TYPE, p.CT_I32, 1), 'invalid_page_index'),
        ]
        for version, mutate, code in cases:
            with self.subTest(version=version, code=code):
                fixture(self.root, version=version)
                rewrite_test_header(self.root, 0, mutate)
                self.assertEqual(validate_dataset(self.root)['structural']['code'], code)
        # Point an index at a complete but invalid Thrift header, leaving physical pages intact.
        m = fixture(self.root)
        rewrite_indexes(self.root / 'part.parquet', lambda indexes: indexes[0][2].items_of(p.OI_PAGE_LOCATIONS)[0]
                        .set(p.PL_OFFSET, p.CT_I64, indexes[0][2].items_of(p.OI_PAGE_LOCATIONS)[0].get(p.PL_OFFSET) + 1))
        save(self.root, m)
        self.assertEqual(validate_dataset(self.root)['structural']['code'], 'invalid_page_index')

    def test_annotations_on_unprojected_leaves(self):
        def logical(kind, params=None):
            return p.Struct([(kind, p.CT_STRUCT, params or p.Struct())])
        def temporal(unit):
            return p.Struct([(1, p.CT_BOOL, False), (2, p.CT_STRUCT, unit)])
        cases = [
            (2, None, logical(6)), (1, None, logical(1)), (7, 15, logical(14)),
            (2, None, logical(5, p.Struct([(1, p.CT_I32, 0), (2, p.CT_I32, 19)]))),
            (1, None, logical(5, p.Struct([(1, p.CT_I32, 0), (2, p.CT_I32, 10)]))),
            (7, 1, logical(5, p.Struct([(1, p.CT_I32, 0), (2, p.CT_I32, 3)]))),
            (2, None, logical(7, temporal(logical(1)))), (1, None, logical(7, temporal(logical(2)))),
            (2, None, logical(8, temporal(p.Struct()))), (2, None, logical(8, p.Struct([(1, p.CT_BOOL, False)]))),
            (2, None, logical(10, p.Struct([(1, p.CT_BYTE, 7), (2, p.CT_BOOL, True)]))),
            (2, None, logical(3)),
        ]
        for typ, length, annotation in cases:
            with self.subTest(typ=typ, annotation=annotation):
                m = fixture(self.root)
                def annotate(meta):
                    el = meta.items_of(p.FMD_SCHEMA)[2]
                    el.set(p.SE_TYPE, p.CT_I32, typ)
                    if length:
                        el.set(p.SE_TYPE_LENGTH, p.CT_I32, length)
                    el.set(10, p.CT_STRUCT, annotation)
                rewrite_test_footer(self.root, m, annotate)
                self.assertEqual(validate_dataset(self.root)['structural']['code'], 'invalid_footer')
        m = fixture(self.root)
        def nested(meta):
            leaf = meta.items_of(p.FMD_SCHEMA)[2]
            leaf.set(p.SE_NAME, p.CT_BINARY, b'child')
            leaf.set(10, p.CT_STRUCT, logical(6))  # DATE on nested INT64
            meta.get(p.FMD_SCHEMA).items.insert(2, p.Struct([
                (p.SE_NAME, p.CT_BINARY, b'value'), (p.SE_REPETITION, p.CT_I32, 0),
                (p.SE_NUM_CHILDREN, p.CT_I32, 1)]))
        rewrite_test_footer(self.root, m, nested)
        self.assertEqual(validate_dataset(self.root)['structural']['code'], 'invalid_footer')
        for annotation in (logical(99), logical(8, temporal(logical(99)))):
            m = fixture(self.root)
            rewrite_test_footer(self.root, m, lambda meta: meta.items_of(p.FMD_SCHEMA)[2].set(10, p.CT_STRUCT, annotation))
            self.assertTrue(all(x['result'] == 'complete' for x in validate_dataset(self.root).values()))


def timestamp_fixed_fixture(root):
    """A legal TIMESTAMP on FLBA(12), whose logical values Arrow cannot represent."""
    m = fixture(root, keys=[b'a'], values=[1])
    path = root / 'part.parquet'
    metadata = pq.read_schema(path).metadata
    schema = pa.schema([pa.field('key', pa.binary(), nullable=False), pa.field('value', pa.binary(12))], metadata=metadata)
    table = pa.Table.from_arrays([pa.array([b'a']), pa.array([b'\x01' + b'\0' * 11], type=pa.binary(12))], schema=schema)
    pq.write_table(table, path, use_dictionary=False, write_page_index=True, sorting_columns=[pq.SortingColumn(0)])
    logical = p.Struct([(8, p.CT_STRUCT, p.Struct([(1, p.CT_BOOL, False),
                       (2, p.CT_STRUCT, p.Struct([(2, p.CT_STRUCT, p.Struct())]))]))])
    rewrite_test_footer(root, m, lambda meta: meta.items_of(p.FMD_SCHEMA)[2].set(10, p.CT_STRUCT, logical))
    return m


if __name__ == '__main__':
    unittest.main()
