#!/usr/bin/env python3
"""Generated-only PyArrow encoding and adaptive continuation qualification."""
import argparse
import base64
import itertools
import json
from pathlib import Path
import shlex
import subprocess
import tempfile
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import parquet_meta as pm


def b64(value):
    return base64.b64encode(value).decode()


def expected(keys, values, prefix=b'', end=None, delimiter=None):
    """Direct source-row model: group ordinals precede all query filtering."""
    result = []
    seen = set()
    for key, group in itertools.groupby(zip(keys, values), key=lambda item: item[0]):
        for offset, (_, value) in enumerate(group):
            if not key.startswith(prefix) or (end is not None and key >= end):
                continue
            split = key.find(delimiter, len(prefix)) if delimiter else -1
            if split >= 0:
                folder = key[:split + len(delimiter)]
                if folder not in seen:
                    entry = {'kind': 'prefix', 'key_base64': b64(folder)}
                    if key == folder:  # LIST-8: the folder's first row is its exact row
                        entry['exact'] = {'offset': offset, 'values': value}
                    result.append(entry)
                    seen.add(folder)
            else:
                result.append({'kind': 'row', 'key_base64': b64(key), 'offset': offset,
                               'values': value})
    return result


def paginate(send, root, model, prefix=b'', end=None, delimiter=None):
    base = {'op': 'list' if delimiter else 'range', 'dataset': str(root),
            'prefix_base64': b64(prefix), 'columns': ['n', 'blob', 'nil']}
    if end is not None:
        base['end_base64'] = b64(end)
    if delimiter:
        base['delimiter_base64'] = b64(delimiter)
    # Exercise every response boundary with small and changing limits, including
    # a repeated-key group split over pages and an actual emitted prefix cursor.
    for sizes in ((1,), (7, 2, 129, 3, 17)):
        collected, after = [], None
        for step in range(len(model) + 2):
            limit = sizes[step % len(sizes)]
            request = dict(base, limit=limit)
            if after is not None:
                request['after'] = after
            response = send(request)
            assert response.get('status') == 'ok', response
            entries = response.get('entries')
            assert entries == model[len(collected):len(collected) + limit], (request, response)
            if not entries:
                assert collected == model
                break
            collected.extend(entries)
            last = entries[-1]
            after = ({'prefix_base64': last['key_base64']} if last['kind'] == 'prefix' else
                     {'key_base64': last['key_base64'], 'offset': last['offset']})
        else:
            raise AssertionError('continuation failed to terminate')


def fixture(root, version, codec, mode):
    # 1,025 rows per group crosses DELTA_BINARY_PACKED's 128-value block and
    # miniblock boundaries. Repeated groups cross pages and row groups, never parts.
    keys = sorted([b'a'] * 131 + [b'a/' + f'{i:04d}'.encode() for i in range(600)] +
                  [b'b'] * 329 + [b'c//x'] * 17 +
                  [b'd/' + f'{i:04d}'.encode() for i in range(600)] +
                  [b'z' + f'{i:04d}'.encode() for i in range(1398)])
    nums = [i * i - 100000 for i in range(len(keys))]
    blobs = [(b'long-shared-prefix-' + bytes([i % 251])) * (257 if i % 127 == 0 else 3)
             + str(i).encode() for i in range(len(keys))]
    metadata = {f'seekparquet.{k}': v for k, v in
                {'format_version': '0', 'draft_revision': '1', 'schema_id': 'independent',
                 'key_column': 'key', 'key_unique': 'false'}.items()}
    schema = pa.schema([pa.field('key', pa.binary(), nullable=False),
                        pa.field('n', pa.int64(), nullable=False),
                        pa.field('blob', pa.binary(), nullable=False),
                        pa.field('nil', pa.binary(), nullable=True)], metadata=metadata)
    table = pa.Table.from_arrays([pa.array(keys), pa.array(nums), pa.array(blobs),
                                 pa.nulls(len(keys), type=pa.binary())], schema=schema)
    path = root / 'part.parquet'
    settings = ({'use_dictionary': ['key', 'blob'], 'dictionary_pagesize_limit': 128}
                if mode == 'fallback' else
                {'use_dictionary': False, 'column_encoding':
                 {'key': 'DELTA_BYTE_ARRAY', 'n': 'DELTA_BINARY_PACKED',
                  'blob': 'DELTA_LENGTH_BYTE_ARRAY', 'nil': 'PLAIN'}})
    pq.write_table(table, path, compression=codec, data_page_version=version,
                   write_page_index=True, sorting_columns=[pq.SortingColumn(0)],
                   max_rows_per_page=257, write_batch_size=129, row_group_size=1025,
                   data_page_size=65536, **settings)
    assert pq.read_table(path).equals(table)
    physical = pm.read_parquet(path.read_bytes())
    group_start = 0
    repeated_page_boundary = repeated_group_boundary = False
    for rg in range(len(physical.row_groups)):
        if group_start:
            repeated_group_boundary |= keys[group_start - 1] == keys[group_start]
        index = pm.read_index(physical, pm.column_chunk(physical, rg, 0), 'offset')
        for location in index.items_of(pm.OI_PAGE_LOCATIONS)[1:]:
            boundary = group_start + location.get(pm.PL_FIRST_ROW)
            repeated_page_boundary |= keys[boundary - 1] == keys[boundary]
        for col in range(4):
            pages = pm.data_pages(physical, pm.column_chunk(physical, rg, col))
            assert len(pages) >= 2, (rg, col, pages)
            assert all(p.type == (0 if version == '1.0' else 3) for p in pages)
            encodings = {p.encoding for p in pages}
            if mode == 'fallback' and col in (0, 2):
                assert {0, 8} <= encodings, (rg, col, encodings)
            if mode == 'delta' and col < 3:
                assert any(p.num_values > 128 for p in pages), (rg, col, pages)
                assert encodings == [{7}, {5}, {6}][col], (rg, col, encodings)
        group_start += physical.row_groups[rg].get(pm.RG_NUM_ROWS)
    assert repeated_page_boundary and repeated_group_boundary
    manifest = {'format': 'seekparquet', 'format_version': 0, 'draft_revision': 1,
                'schema_id': 'independent', 'key': {'column': 'key', 'unique': False},
                'parts': [{'path': path.name, 'rows': len(keys), 'bytes': path.stat().st_size,
                           'min_key_base64': b64(keys[0]), 'max_key_base64': b64(keys[-1])}]}
    (root / 'manifest.json').write_text(json.dumps(manifest))
    values = [[{'int': str(n)}, {'bytes_base64': b64(blob)}, {'null': True}]
              for n, blob in zip(nums, blobs)]
    return keys, values


def check(adapter):
    if pa.__version__ != '26.0.1':
        raise RuntimeError('requires PyArrow 26.0.1')
    count = 0
    with tempfile.TemporaryDirectory(prefix='seekparquet-independent-') as directory:
        with subprocess.Popen(shlex.split(adapter), stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, text=True) as process:
            watchdog = threading.Timer(240, process.kill)
            watchdog.daemon = True
            watchdog.start()
            def send(request):
                nonlocal count
                count += 1
                request = dict(request, id=str(count))
                process.stdin.write(json.dumps(request) + '\n')
                process.stdin.flush()
                response = json.loads(process.stdout.readline())
                assert response.get('id') == str(count), response
                return response
            try:
                for version, codec, mode in itertools.product(('1.0', '2.0'),
                                                              ('none', 'snappy', 'gzip', 'zstd'),
                                                              ('fallback', 'delta')):
                    root = Path(directory) / f'{version}-{codec}-{mode}'
                    root.mkdir()
                    keys, values = fixture(root, version, codec, mode)
                    for prefix, end, delimiter in ((b'', None, None), (b'a', b'd', None),
                                                    (b'', None, b'/'), (b'c/', None, b'/'),
                                                    (b'a/', b'a/0300', b'/')):
                        model = expected(keys, values, prefix, end, delimiter)
                        paginate(send, root, model, prefix, end, delimiter)
                process.stdin.close()
                assert process.wait(timeout=20) == 0
            finally:
                watchdog.cancel()
                if process.poll() is None:
                    process.kill()
                    process.wait()
    print(f'independent encoding and adaptive pagination: {count} responses passed ({adapter})')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--adapter', required=True)
    check(parser.parse_args().adapter)
