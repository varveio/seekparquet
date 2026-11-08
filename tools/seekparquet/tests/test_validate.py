"""Generated physical fixtures; no committed Parquet or expected-row oracle."""
import base64
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from seekparquet import _parquet as p
from seekparquet.validate import Limits, validate_dataset


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
        with patch('seekparquet.validate.pq.ParquetFile', side_effect=pa.ArrowNotImplementedError('test codec')):
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


if __name__ == '__main__':
    unittest.main()
