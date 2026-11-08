"""In-memory, sorted-table producer for SeekParquet draft revision 1."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def _name(value, label):
    if not isinstance(value, str) or not value or any(ord(c) < 32 or ord(c) == 127 or 0xD800 <= ord(c) <= 0xDFFF for c in value):
        raise ValueError(f"{label} must be nonempty Unicode without controls or surrogates")


def _sync_dir(path):
    # Directory fsync is required: unsupported filesystems/platforms fail closed.
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_dataset(table, output_dir, *, schema_id, key="key", unique=True,
                  target_rows_per_part=1_000_000, row_group_size=65_536,
                  hash_parts=False):
    """Write a new immutable dataset; return its published manifest.

    Input must already be sorted in unsigned byte order. Repeated groups retain
    input order and never span parts. Before publication, failures leave an
    unpublished directory for inspection. The final directory fsync may fail
    after publication; an exception does not prove the manifest is absent.
    This function never removes or reuses user paths.
    """
    if not isinstance(table, pa.Table):
        raise TypeError("table must be a pyarrow.Table")
    _name(key, "key")
    _name(schema_id, "schema_id")
    if type(unique) is not bool or type(hash_parts) is not bool:
        raise TypeError("unique and hash_parts must be bool")
    for n in (target_rows_per_part, row_group_size):
        if type(n) is not int or n <= 0:
            raise ValueError("row targets must be positive integers")
    if len(set(table.column_names)) != len(table.column_names):
        raise ValueError("duplicate column names")
    index = table.schema.get_field_index(key)
    if index < 0:
        raise ValueError("missing key column")
    field = table.schema.field(index)
    if field.type not in (pa.binary(), pa.string()):
        raise ValueError("key must have binary or string type")
    if table.column(index).null_count:
        raise ValueError("null key")
    keys = [v.encode("utf-8") if isinstance(v, str) else v for v in table.column(index).to_pylist()]
    previous = None
    for value in keys:
        if not value:
            raise ValueError("empty key")
        if previous is not None and (value < previous or (unique and value == previous)):
            raise ValueError("keys must be ascending (strictly ascending in unique mode)")
        previous = value
    # Null-free input can have a nullable Arrow field: persist the required key.
    fields = list(table.schema)
    fields[index] = pa.field(key, field.type, nullable=False, metadata=field.metadata)
    metadata = {k: v for k, v in (table.schema.metadata or {}).items()
                if not k.startswith(b"seekparquet.")}
    metadata.update({
        b"seekparquet.format_version": b"0",
        b"seekparquet.draft_revision": b"1",
        b"seekparquet.schema_id": schema_id.encode("utf-8"),
        b"seekparquet.key_column": key.encode("utf-8"),
        b"seekparquet.key_unique": str(unique).lower().encode("ascii"),
    })
    table = pa.Table.from_arrays(table.columns, schema=pa.schema(fields, metadata=metadata))
    # SortingColumn uses physical leaf ordinals, including leaves preceding key.
    leaf_index = 0
    for preceding in fields[:index]:
        def leaves(t):
            if pa.types.is_struct(t):
                return sum(leaves(f.type) for f in t)
            if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
                return leaves(t.value_type)
            if pa.types.is_map(t):
                return leaves(t.key_type) + leaves(t.item_type)
            return 1
        leaf_index += leaves(preceding.type)
    root = Path(output_dir)
    root.mkdir()  # Exclusive creation; even an empty existing directory is refused.
    _sync_dir(root.parent)
    data = root / "data"
    data.mkdir()
    manifest = {"format": "seekparquet", "format_version": 0, "draft_revision": 1,
                "schema_id": schema_id, "key": {"column": key, "unique": unique}, "parts": []}
    start = 0
    while start < len(keys):
        end = min(start + target_rows_per_part, len(keys))
        while end < len(keys) and keys[end] == keys[end - 1]:
            end += 1
        relative = f"data/part-{len(manifest['parts']):05d}.parquet"
        path = root / relative
        with path.open("xb") as stream:
            pq.write_table(table.slice(start, end - start), stream,
                           row_group_size=row_group_size, compression="zstd",
                           use_dictionary=False, data_page_version="2.0",
                           write_statistics=True, write_page_index=True,
                           data_page_size=64 * 1024, write_batch_size=1024,
                           max_rows_per_page=1024,
                           sorting_columns=[pq.SortingColumn(leaf_index, descending=False)])
            stream.flush()
            os.fsync(stream.fileno())
        footer = pq.read_metadata(path)
        if footer.num_rows != end - start:
            raise ValueError("written part row count differs from input")
        for group_index in range(footer.num_row_groups):
            column = footer.row_group(group_index).column(leaf_index)
            if not column.has_column_index or not column.has_offset_index:
                raise ValueError("PyArrow omitted required key page indexes; output is unpublished")
        part = {"path": relative, "rows": end - start, "bytes": path.stat().st_size,
                "min_key_base64": base64.b64encode(keys[start]).decode("ascii"),
                "max_key_base64": base64.b64encode(keys[end - 1]).decode("ascii")}
        if hash_parts:
            with path.open("rb") as stream:
                part["sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
        manifest["parts"].append(part)
        start = end
    _sync_dir(data)
    _sync_dir(root)
    temporary = root / ".manifest.pending"
    with temporary.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(manifest, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.rename(temporary, root / "manifest.json")
    _sync_dir(root)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="sorted Parquet input")
    parser.add_argument("output", help="new dataset directory")
    parser.add_argument("--schema-id", required=True)
    parser.add_argument("--key", default="key")
    parser.add_argument("--repeated", action="store_true")
    parser.add_argument("--target-rows-per-part", type=int, default=1_000_000)
    parser.add_argument("--row-group-size", type=int, default=65_536)
    parser.add_argument("--hash-parts", action="store_true")
    args = parser.parse_args()
    write_dataset(pq.read_table(args.input), args.output, schema_id=args.schema_id,
                  key=args.key, unique=not args.repeated,
                  target_rows_per_part=args.target_rows_per_part,
                  row_group_size=args.row_group_size, hash_parts=args.hash_parts)


if __name__ == "__main__":
    main()
