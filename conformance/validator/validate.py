"""SeekParquet §8 validator, independent of the indexed reader seek path.

Physical pages are walked independently from column-chunk extents. Indexed
headers are first classified under CHECK-5, without trusting the index to enumerate pages.
PyArrow decodes complete row groups without page-index filtering. The compact
Thrift codec shares provenance with the fixture tooling; validation control flow
and checks are implemented here. Only immutable local datasets are supported.
"""
from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import mmap
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "generators"))
import parquet_meta as p  # noqa: E402
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import parquet_rules as rules  # noqa: E402

LEVELS = ('structural', 'profile', 'full')
MAX_INT = (1 << 63) - 1


@dataclass(frozen=True)
class Limits:
    """Implementation limits; exhaustion is incomplete, never a format refusal."""
    manifest_bytes: int = 16 << 20
    footer_bytes: int = 32 << 20
    part_bytes: int = 1 << 30
    page_bytes: int = 64 << 20
    pages_per_chunk: int = 1 << 20
    parts: int = 100000
    rows_per_group: int = 10000000
    uncompressed_bytes_per_group: int = 256 << 20

    def __post_init__(self):
        if any(type(v) is not int or v < 1 for v in vars(self).values()):
            raise ValueError('limits must be positive integers')


class Invalid(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


class Incomplete(Exception):
    pass


def require(ok, code, message):
    if not ok:
        raise Invalid(code, message)


def integer(x, minimum=0, maximum=MAX_INT):
    return type(x) is int and minimum <= x <= maximum


def pairs(items):
    out = {}
    for k, v in items:
        require(k not in out, 'invalid_manifest', f'duplicate member: {k}')
        out[k] = v
    return out


class Fraction:
    def __init__(self, text):
        self.text = text


def clean_strings(value):
    if isinstance(value, str):
        value.encode('utf-8')
    elif isinstance(value, dict):
        for k, v in value.items():
            clean_strings(k)
            clean_strings(v)
    elif isinstance(value, list):
        for v in value:
            clean_strings(v)


def members(value, needed, allowed):
    require(isinstance(value, dict) and needed <= value.keys() <= allowed,
            'invalid_manifest', 'missing, extra or invalid object members')


def check_extensions(obj):
    """EXT-4: an extensions object (top-level or a part's) with non-empty namespace names; its values are
    ignored (EXT-3)."""
    if 'extensions' in obj:
        require(isinstance(obj['extensions'], dict) and all(obj['extensions']), 'invalid_manifest', 'extensions')


def name(value):
    return isinstance(value, str) and bool(value) and not any(ord(c) < 32 or ord(c) == 127 for c in value)


def key_bound(value):
    try:
        decoded = base64.b64decode(value, validate=True)
        require(bool(decoded) and base64.b64encode(decoded).decode('ascii') == value,
                'invalid_manifest', 'noncanonical or empty key bound')
        return decoded
    except (ValueError, TypeError, binascii.Error):
        raise Invalid('invalid_manifest', 'invalid base64 key bound') from None


def manifest(root, limits):
    path = root / 'manifest.json'
    try:
        with path.open('rb') as f:
            raw = f.read(limits.manifest_bytes + 1)
    except FileNotFoundError:
        raise Invalid('no_manifest', 'manifest.json does not exist') from None
    if len(raw) > limits.manifest_bytes:
        raise Incomplete('configured manifest_bytes limit exceeded')
    try:
        require(not raw.startswith(b'\xef\xbb\xbf'), 'invalid_manifest', 'BOM')
        def constant(s):
            raise ValueError(s)
        m = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                       parse_float=Fraction, parse_constant=constant)
        clean_strings(m)
    except (ValueError, UnicodeError, RecursionError) as e:
        raise Invalid('invalid_manifest', str(e)) from None
    require(isinstance(m, dict), 'invalid_manifest', 'manifest must be an object')
    require(m.get('format') == 'seekparquet' and integer(m.get('format_version')),
            'invalid_manifest', 'format/version')
    fv, rev = m['format_version'], m.get('draft_revision')
    require(integer(rev, 1) if fv == 0 else 'draft_revision' not in m,
            'invalid_manifest', 'draft revision')
    if (fv, rev) != (0, 1):
        raise Invalid('unsupported_version', f'({fv}, {rev})')
    members(m, {'format', 'format_version', 'schema_id', 'key', 'parts', 'draft_revision'},
            {'format', 'format_version', 'schema_id', 'key', 'parts', 'draft_revision', 'extensions'})
    require(name(m['schema_id']), 'invalid_manifest', 'schema_id')
    members(m['key'], {'column', 'unique'}, {'column', 'unique'})
    require(name(m['key']['column']) and type(m['key']['unique']) is bool,
            'invalid_manifest', 'key declaration')
    check_extensions(m)
    require(isinstance(m['parts'], list), 'invalid_manifest', 'parts must be an array')
    if len(m['parts']) > limits.parts:
        raise Incomplete('configured parts limit exceeded')
    seen, previous = set(), None
    for entry in m['parts']:
        needed = {'path', 'rows', 'bytes', 'min_key_base64', 'max_key_base64'}
        members(entry, needed, needed | {'sha256', 'extensions'})
        check_extensions(entry)
        rel = entry['path']
        require(name(rel) and not rel.startswith('/') and '\\' not in rel
                and all(s not in ('', '.', '..') for s in rel.split('/')) and rel not in seen,
                'invalid_manifest', 'invalid or duplicate part path')
        seen.add(rel)
        require(integer(entry['rows'], 1) and integer(entry['bytes'], 1), 'invalid_manifest', 'part counts')
        lo, hi = key_bound(entry['min_key_base64']), key_bound(entry['max_key_base64'])
        require(lo <= hi and (previous is None or previous < lo), 'invalid_manifest', 'part bounds overlap or decrease')
        previous = hi
        require('sha256' not in entry or isinstance(entry['sha256'], str)
                and re.fullmatch('[0-9a-f]{64}', entry['sha256']) is not None,
                'invalid_manifest', 'sha256')
    return m


def part_path(root, entry):
    candidate = (root / entry['path']).resolve()
    require(candidate.is_relative_to(root.resolve()), 'invalid_manifest', 'part escapes dataset root')
    return candidate


def field_tree(schema):
    """Return top-level physical signatures and leaf positions, ignoring annotations."""
    require(bool(schema), 'invalid_footer', 'empty schema')
    cursor, leaf = 1, 0
    def visit(depth=0):
        nonlocal cursor, leaf
        require(depth < 64 and cursor < len(schema), 'invalid_footer', 'invalid schema tree')
        el = schema[cursor]
        cursor += 1
        children = el.get(p.SE_NUM_CHILDREN, 0)
        require(integer(children), 'invalid_footer', 'invalid schema children')
        leaves, signatures = [], []
        for _ in range(children):
            sig, indexes = visit(depth + 1)
            signatures.append(sig)
            leaves.extend(indexes)
        if not children:
            leaves = [leaf]
            leaf += 1
        return ((el.get(p.SE_NAME), el.get(p.SE_REPETITION, 0), el.get(p.SE_TYPE),
                 el.get(p.SE_TYPE_LENGTH), tuple(signatures)), leaves)
    out = []
    for _ in range(schema[0].get(p.SE_NUM_CHILDREN, 0)):
        el = schema[cursor]
        signature, leaves = visit()
        out.append((el, signature, leaves))
    require(cursor == len(schema), 'invalid_footer', 'extra schema elements')
    return out, leaf


class Part:
    def __init__(self, root, entry, m, limits):
        self.entry, self.limits = entry, limits
        self.path = part_path(root, entry)
        self.file = self.mapping = None
        try:
            self.file = self.path.open('rb')
        except FileNotFoundError:
            raise Invalid('missing_part', entry['path']) from None
        try:
            size = self.path.stat().st_size
            require(size == entry['bytes'], 'part_mismatch', 'part byte count')
            if size > limits.part_bytes:
                raise Incomplete('configured part_bytes limit exceeded')
            require(size >= 12, 'invalid_footer', 'part too short')
            self.mapping = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_READ)
            if b'PARE' in (self.mapping[:4], self.mapping[-4:]):  # CHECK-1: both magics, FILE-2
                raise Incomplete('unsupported_encryption: PARE magic (an encrypted part)')
            require(self.mapping[:4] == b'PAR1' and self.mapping[-4:] == b'PAR1',
                    'invalid_footer', 'missing Parquet magic')
            footer_len = int.from_bytes(self.mapping[-8:-4], 'little')
            if footer_len > limits.footer_bytes:
                raise Incomplete('configured footer_bytes limit exceeded')
            try:
                self.pf = p.read_parquet(self.mapping)
            except p.SignedPlaintextFooter as e:
                raise Incomplete(f'unsupported_encryption: {e}') from None
            rules.annotations(self.pf)
            self.fields, self.leaves = field_tree(self.pf.schema)
            self._footer(m)
        except Exception:
            self.close()
            raise

    def close(self):
        if self.mapping is not None:
            self.mapping.close()
        if self.file is not None:
            self.file.close()

    def _footer(self, m):
        if self.pf.meta.has(8) or self.pf.meta.has(9):  # FILE-2: encryption declared in the footer
            raise Incomplete('unsupported_encryption: encryption declared in the footer')
        groups = self.pf.row_groups
        counts = [g.get(p.RG_NUM_ROWS) for g in groups]
        require(all(integer(n) for n in counts), 'invalid_footer', 'negative/missing row group count')
        require(sum(counts) == self.pf.meta.get(p.FMD_NUM_ROWS), 'invalid_footer', 'footer row total')
        require(sum(counts) == self.entry['rows'], 'part_mismatch', 'manifest row count')
        want = {'format_version': str(m['format_version']), 'draft_revision': str(m['draft_revision']),
                'schema_id': m['schema_id'], 'key_column': m['key']['column'],
                'key_unique': str(m['key']['unique']).lower()}
        stamps = {}
        for kv in self.pf.meta.items_of(p.FMD_KV):
            k = kv.get(p.KV_KEY)
            if k in [b'seekparquet.' + s.encode() for s in want]:
                require(k not in stamps, 'stamp_mismatch', 'duplicate stamp')
                stamps[k] = kv.get(p.KV_VALUE)
        require(all(stamps.get(b'seekparquet.' + k.encode()) == v.encode() for k, v in want.items()),
                'stamp_mismatch', 'missing or differing stamp')
        matches = [f for f in self.fields if f[0].get(p.SE_NAME) == m['key']['column'].encode()]
        require(len(matches) == 1, 'invalid_key_column', 'missing/duplicate key field')
        key, _, leaves = matches[0]
        require(not key.get(p.SE_NUM_CHILDREN) and key.get(p.SE_TYPE) == 6
                and key.get(p.SE_REPETITION, 0) == 0, 'invalid_key_column', 'key must be required BYTE_ARRAY')
        logical = key.get(10)
        require(key.get(6) in (None, 0) and (logical is None or [f[0] for f in logical.fields] == [1]),
                'invalid_key_column', 'invalid key annotation')
        self.utf8 = key.get(6) == 0 or logical is not None
        self.key_leaf = leaves[0]
        orders = self.pf.meta.get(7)
        require(orders is not None and len(orders.items) == self.leaves
                and orders.items[self.key_leaf].has(1), 'invalid_key_column', 'key column_orders')
        for group in groups:
            columns = group.items_of(p.RG_COLUMNS)
            require(len(columns) == self.leaves, 'invalid_footer', 'column chunk count')
            for cc in columns:
                require(not cc.has(p.CC_FILE_PATH), 'invalid_footer', 'external column chunk')
                if cc.has(8) or cc.has(9):
                    raise Incomplete('unsupported_encryption: encrypted column metadata')
            if group.get(p.RG_NUM_ROWS):
                sort = group.items_of(p.RG_SORTING)
                require(bool(sort) and sort[0].get(p.SC_COLUMN_IDX) == self.key_leaf
                        and sort[0].get(p.SC_DESCENDING) is False,
                        'missing_sorting_columns', 'key sorting declaration')

    def physical(self, cc):
        pages = rules.physical_pages(self.pf, cc, self.limits.pages_per_chunk)
        for page in pages:
            if max(page.compressed_size, page.uncompressed_size) > self.limits.page_bytes:
                raise Incomplete('configured page_bytes limit exceeded')
        return pages

    def key_index(self, group):
        cc = group.items_of(p.RG_COLUMNS)[self.key_leaf]
        for field in (p.CC_CI_LENGTH, p.CC_OI_LENGTH):
            length = cc.get(field)
            if integer(length) and length > self.limits.footer_bytes:
                raise Incomplete('configured footer_bytes limit exceeded by page index')
        try:
            ci, oi = p.read_index(self.pf, cc, 'column'), p.read_index(self.pf, cc, 'offset')
        except (p.ParquetMetaError, p.ThriftError) as e:
            raise Invalid('invalid_page_index', str(e)) from None
        require(ci is not None and oi is not None, 'missing_page_index', 'key page indexes required')
        rules.field(ci, p.CI_BOUNDARY, p.CT_I32, 'invalid_page_index')
        locations = oi.items_of(p.OI_PAGE_LOCATIONS)
        rules.indexed_headers(self.pf, cc, locations, group.get(p.RG_NUM_ROWS))
        # Independently enumerate the entire physical chunk: indexes cannot hide pages.
        physical = [page for page in self.physical(cc) if page.type in (0, 3)]
        require(bool(locations) and len(locations) == len(physical), 'invalid_page_index', 'physical/index page count')
        positions, row = [], 0
        for page, loc in zip(physical, locations):
            require(loc.get(p.PL_OFFSET) == page.offset
                    and loc.get(p.PL_SIZE) == page.header_len + page.compressed_size
                    and loc.get(p.PL_FIRST_ROW) == row,
                    'invalid_page_index', 'page location, size or first row differs from physical headers')
            n = page.num_values if page.type == 0 else page.num_rows
            require(integer(n, 1) and page.num_values == n, 'invalid_page_index', 'required key page row count')
            positions.append((row, row + n))
            row += n
        require(row == group.get(p.RG_NUM_ROWS), 'invalid_page_index', 'physical key row total')
        lists = [ci.items_of(f) for f in (p.CI_NULL_PAGES, p.CI_MIN, p.CI_MAX)]
        require(all(len(xs) == len(physical) for xs in lists) and not any(lists[0]),
                'invalid_page_index', 'ColumnIndex parallel lists or null page')
        require(all(isinstance(v, bytes) for xs in lists[1:] for v in xs), 'invalid_page_index', 'nonbinary bound')
        return positions, lists[1], lists[2]

    def decoded(self, ri, columns=None):
        n = self.pf.row_groups[ri].get(p.RG_NUM_ROWS)
        if n > self.limits.rows_per_group:
            raise Incomplete('configured rows_per_group limit exceeded')
        selected = {self.key_leaf} if columns is not None else range(self.leaves)
        budget = 0
        if n:
            for ci in selected:
                try:
                    pages = self.physical(self.pf.row_groups[ri].items_of(p.RG_COLUMNS)[ci])
                except (p.ParquetMetaError, p.ThriftError) as e:
                    raise Invalid('invalid_parquet', str(e)) from None
                rules.encoding_support(self.pf, self.pf.row_groups[ri].items_of(p.RG_COLUMNS)[ci], pages)
                budget += sum(page.uncompressed_size for page in pages)
                if budget > self.limits.uncompressed_bytes_per_group:
                    raise Incomplete('configured uncompressed_bytes_per_group limit exceeded')
        # The checked footer is normalized for Arrow without changing stored
        # page bodies. Unsupported unselected codecs must not block profile.
        # Decoding errors on supported features remain Parquet corruption.
        with pa.BufferReader(rules.arrow_bytes(self.pf, selected)) as source:
            try:
                # No filter, no page-index-directed seek, no partition inference.
                table = pq.ParquetFile(source, page_checksum_verification=True).read_row_group(
                    ri, columns=columns, use_threads=False)
                table.validate(full=columns is None)
            except pa.ArrowNotImplementedError as e:
                raise Incomplete(f'unsupported_encoding: {e}') from None
            except (pa.ArrowException, OSError) as e:
                raise Invalid('invalid_parquet', str(e)) from None
        require(table.num_rows == n and all(len(c) == n for c in table.columns),
                'invalid_parquet', 'decoded row group count')
        return table


def _run_level(action):
    try:
        extra = action() or {}
        return {'result': 'complete', **extra}
    except rules.RuleError as e:
        if e.code.startswith('unsupported_'):
            return {'result': 'incomplete', 'reason': str(e), 'category': 'unsupported', 'code': e.code}
        return {'result': 'failed', 'code': e.code, 'message': str(e)}
    except Invalid as e:
        return {'result': 'failed', 'code': e.code, 'message': str(e)}
    except p.ResourceLimit as e:
        return {'result': 'incomplete', 'reason': str(e), 'category': 'limit'}
    except Incomplete as e:
        if str(e).startswith('unsupported_'):  # an unsupported feature names its code
            return {'result': 'incomplete', 'reason': str(e), 'category': 'unsupported', 'code': str(e).split(':')[0]}
        return {'result': 'incomplete', 'reason': str(e), 'category': 'limit'}
    except OSError as e:
        return {'result': 'incomplete', 'reason': str(e), 'category': 'storage'}
    except (p.ParquetMetaError, p.ThriftError, AttributeError, IndexError, TypeError, ValueError) as e:
        return {'result': 'failed', 'code': 'invalid_footer', 'message': str(e)}


def validate_dataset(path, level='full', *, limits=None):
    """Return per-level §8 results. Invalid level/limits raise ValueError.

    Storage failures and configured limits have distinct incomplete categories.
    Digest reads declared bytes independently of Parquet and all other levels.
    """
    if level not in (*LEVELS, 'digest'):
        raise ValueError('level must be structural, profile, full or digest')
    limits, root = limits or Limits(), Path(path)
    parts = []
    state = {}
    def structural():
        m = state['manifest'] = manifest(root, limits)
        signatures = {}
        for entry in m['parts']:
            try:
                part = Part(root, entry, m, limits)
            except (p.ParquetMetaError, p.ThriftError) as e:
                raise Invalid('invalid_footer', str(e)) from None
            parts.append(part)
            for el, signature, _ in part.fields:
                k = el.get(p.SE_NAME)
                require(k not in signatures or signatures[k] == signature,
                        'inconsistent_column', 'physical schema differs across parts')
                signatures[k] = signature
            for group in part.pf.row_groups:
                if group.get(p.RG_NUM_ROWS):
                    try:
                        part.key_index(group)
                    except (p.ParquetMetaError, p.ThriftError) as e:
                        raise Invalid('invalid_parquet', str(e)) from None
    def profile():
        m = state['manifest']
        previous = None
        for part in parts:
            first, last, count = None, None, 0
            for ri, group in enumerate(part.pf.row_groups):
                if not group.get(p.RG_NUM_ROWS):
                    continue
                positions, lower, upper = part.key_index(group)
                array = part.decoded(ri, [m['key']['column']]).column(0).combine_chunks()
                if pa.types.is_string(array.type) or pa.types.is_large_string(array.type):
                    array = array.view(pa.binary() if pa.types.is_string(array.type) else pa.large_binary())
                keys = array.to_pylist()
                for key in keys:
                    require(isinstance(key, bytes) and bool(key), 'invalid_key', 'null or empty key')
                    if part.utf8:
                        try:
                            key.decode('utf-8')
                        except UnicodeError:
                            raise Invalid('invalid_key', 'non-UTF8 key') from None
                    require(previous is None or key > previous or (key == previous and not m['key']['unique']),
                            'key_order', 'keys decrease or duplicate unique key')
                    previous = last = key
                    if first is None:
                        first = key
                for (a, b), lo, hi in zip(positions, lower, upper):
                    if part.utf8:
                        try:
                            lo.decode('utf-8'); hi.decode('utf-8')
                        except UnicodeError:
                            raise Invalid('invalid_page_index', 'non-UTF8 page bound') from None
                    require(lo <= keys[a] and keys[b - 1] <= hi, 'invalid_page_index', 'page bound excludes keys')
                count += len(keys)
            require(count == part.entry['rows'], 'part_mismatch', 'actual part row count')
            require(first == key_bound(part.entry['min_key_base64']) and last == key_bound(part.entry['max_key_base64']),
                    'bounds_mismatch', 'exact manifest bounds differ from stored keys')
    def full():
        for part in parts:
            for ri, group in enumerate(part.pf.row_groups):
                for cc in group.items_of(p.RG_COLUMNS):
                    if not group.get(p.RG_NUM_ROWS):
                        continue
                    try:
                        pages = part.physical(cc)
                        values = sum(page.num_values for page in pages if page.type in (0, 3))
                        require(values == cc.get(p.CC_META).get(p.CMD_NUM_VALUES),
                                'invalid_parquet', 'physical page value count differs from column metadata')
                    except (p.ParquetMetaError, p.ThriftError) as e:
                        raise Invalid('invalid_parquet', str(e)) from None
                part.decoded(ri)
    def digest():
        m = manifest(root, limits)
        checked = 0
        for entry in m['parts']:
            if 'sha256' not in entry:
                continue
            digest_hash = hashlib.sha256()
            try:
                with part_path(root, entry).open('rb') as f:
                    for block in iter(lambda: f.read(1 << 20), b''):
                        digest_hash.update(block)
            except FileNotFoundError:
                raise Invalid('missing_part', entry['path']) from None
            checked += 1
            require(digest_hash.hexdigest() == entry['sha256'], 'digest_mismatch', entry['path'])
        return {'checked': checked, 'message': 'no declared digests; nothing checked' if not checked else 'all declared digests match'}
    if level == 'digest':
        return {'digest': _run_level(digest)}
    out = {}
    try:
        for current, action in zip(LEVELS, (structural, profile, full)):
            if out and list(out.values())[-1]['result'] != 'complete':
                out[current] = {'result': 'incomplete', 'reason': 'prerequisite level did not complete', 'category': 'prerequisite'}
            else:
                out[current] = _run_level(action)
            if current == level:
                break
    finally:
        for part in parts:
            part.close()
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('dataset')
    parser.add_argument('--level', choices=(*LEVELS, 'digest'), default='full')
    args = parser.parse_args(argv)
    results = validate_dataset(args.dataset, args.level)
    print(json.dumps(results, sort_keys=True))
    return 1 if any(v['result'] == 'failed' for v in results.values()) else 2 if any(v['result'] == 'incomplete' for v in results.values()) else 0


if __name__ == '__main__':
    raise SystemExit(main())
