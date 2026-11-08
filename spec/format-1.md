# SeekParquet format 1

**Status: draft 1.** Files written to this draft carry `format_version` 0 and `draft_revision` 1 (§9). The
number 1 is reserved for the frozen format; nothing may be stamped `format_version` 1 until this document is
final (see [README](README.md)).

SeekParquet is a sorted-key profile for Apache Parquet. A SeekParquet dataset is a set of ordinary Parquet
files, sorted by one byte-string key and split into key-disjoint parts, plus a manifest that lists the parts
in key order and commits them. Any Parquet reader can read the files as an ordinary table. A reader that
knows the profile can, in addition, find the first row at or after a key, read a key range, and list a key
prefix with delimiter rollups, by reading a manifest, footers, a page index and a few pages, without a query
engine or a catalog.

This document defines the files, the manifest, what readers check, and the meaning of the queries. It does
not define a writer's encoding choices, a storage service, or how a producer obtains its rows.

## 1. Conventions

The key words MUST, MUST NOT, REQUIRED, SHOULD, SHOULD NOT and MAY are to be interpreted as in RFC 2119 and
RFC 8174 when, and only when, they appear in capitals.

Each requirement has an identifier in brackets, for example [KEY-1]. Conformance cases cite these
identifiers. Identifiers are never reused for a different requirement. Retired identifiers: LIST-1 (the
one-byte delimiter rule, now part of QUERY-1).

**Keys and byte order.** A key is a non-empty sequence of bytes. Keys compare in unsigned lexicographic byte
order: the first differing byte decides, compared as an unsigned integer from 0 to 255, and a key that is a
proper prefix of another sorts before it. Keys need not be UTF-8. Every comparison in this document uses this
order.

**Successor of a prefix.** `successor(p)` is the least byte string greater than every string that begins
with `p`: `p` with its trailing `0xFF` bytes removed and its last remaining byte incremented by one. If `p`
is empty or consists only of `0xFF` bytes, it has no successor, and the range of keys beginning with `p` is
unbounded above.

**Parquet terms** (row group, column chunk, page, ColumnIndex, OffsetIndex, `sorting_columns`, key-value
metadata) are those of the [Parquet format specification](https://github.com/apache/parquet-format), whose
definitions this document does not repeat.

## 2. Dataset layout

A dataset is a **dataset root** (a directory, or an object-store prefix that behaves like one) holding a file
named `manifest.json` and the Parquet files the manifest lists, which are its **parts**.

- [LAYOUT-1] The manifest is the commit. A dataset root without `manifest.json` holds no dataset. A dataset
  consists of exactly the parts its manifest lists, in the manifest's order; other files under the root are
  not part of it and readers MUST ignore them.
- [LAYOUT-2] Part paths in the manifest are relative to the dataset root (§3.3).
- [LAYOUT-3] A writer MAY place parts anywhere under the root. The convention `data/part-NNNNN.parquet`
  (zero-padded, in manifest order) is RECOMMENDED but not required, and readers MUST NOT infer order from
  file names.

## 3. The manifest

### 3.1 Encoding

- [MAN-1] `manifest.json` is a single JSON object (RFC 8259) encoded in UTF-8 without a byte order mark.
  Every string, including member names and strings inside `extensions`, is a sequence of Unicode scalar
  values: an escaped unpaired surrogate is not allowed.
- [MAN-2] An object in the manifest, including any object inside `extensions`, MUST NOT contain the same
  member name twice, compared after decoding escapes. A reader MUST refuse a manifest with duplicate names
  (`invalid_manifest`).
- [MAN-3] The manifest MUST conform to [manifest.schema.json](manifest.schema.json). Members not defined by
  this document are not allowed, at the top level or in a part, except inside `extensions` (§3.4). A reader
  MUST refuse a manifest with an undefined member (`invalid_manifest`). New members arrive only with a new
  format version or draft revision.
- [MAN-6] Integer members are written as JSON numbers with no fraction, exponent or leading zeros, and lie
  between 0 and 2^63 − 1. A reader MUST parse them exactly, without rounding, and refuse any other spelling
  or range (`invalid_manifest`).
- [MAN-7] A reader determines the version before anything else. It first reads `format`, `format_version`
  and `draft_revision`. If `format` is not `"seekparquet"`, if `format_version` is missing or not an
  integer, or if `draft_revision` is missing when `format_version` is 0, present when it is not, or not an
  integer of at least 1, the manifest is `invalid_manifest`. If they are well formed but name a version the reader
  does not support, it MUST refuse with `unsupported_version` without applying the rest of this section.
  Only then does it apply the rules of the version it supports. The JSON Schema accepts any
  `format_version` structurally; that does not authorise writing a version this document does not define.

### 3.2 Top-level members

| Member | Type | Required | Meaning |
|---|---|---|---|
| `format` | string | yes | Exactly `"seekparquet"`. |
| `format_version` | integer | yes | `0` while this document is a draft; `1` once it is final. |
| `draft_revision` | integer ≥ 1 | iff `format_version` is 0 | The draft this dataset was written to. This document is draft revision 1. |
| `schema_id` | non-empty string | yes | The producer's identifier for the set of columns and their meaning (§5.3). |
| `parts` | array of part objects | yes | The parts in ascending key order. An empty array is an empty dataset. |
| `extensions` | object | no | Producer metadata (§3.4). |

- [MAN-4] A reader MUST refuse a manifest whose `format` is not `"seekparquet"` (`invalid_manifest`) and
  one whose (`format_version`, `draft_revision`) pair it does not support (`unsupported_version`).
- [MAN-5] A reader that supports format 1 MUST refuse files stamped `format_version` 0 unless it has been
  explicitly configured to accept that exact draft revision.

### 3.3 Parts

| Member | Type | Required | Meaning |
|---|---|---|---|
| `path` | string | yes | The part's path relative to the dataset root. |
| `rows` | integer ≥ 1 | yes | The exact number of rows in the part. |
| `bytes` | integer ≥ 1 | yes | The exact size of the part file in bytes. |
| `min_key_base64` | string | yes | The part's first key, exactly, in standard base64 (RFC 4648 §4, with padding). |
| `max_key_base64` | string | yes | The part's last key, exactly, in the same encoding. |
| `sha256` | string | no | Lowercase hexadecimal SHA-256 of the part file's exact bytes. |

- [PART-1] `path` is a sequence of segments separated by `/`. Each segment is non-empty, is neither `.` nor
  `..`, and contains no `\` and no control character (U+0000 to U+001F and U+007F). The path does not begin
  with `/`. Paths are literal: a reader MUST NOT percent-decode, Unicode-normalise or otherwise reinterpret
  them. Paths are unique within a manifest, compared as exact strings; writers SHOULD NOT produce paths that
  differ only in letter case. A reader MUST refuse a manifest that breaks any of these rules
  (`invalid_manifest`) and MUST NOT read any file outside the dataset root on account of a part path.
- [PART-2] A published part holds at least one row. A part with `rows` 0 is not allowed
  (`invalid_manifest`).
- [PART-3] `min_key_base64` and `max_key_base64` are the exact first and last keys of the part, never
  truncated. A reader decodes them and compares bytes, never base64 text. The encoding is canonical: no
  whitespace, padding present, and unused bits in the last group zero, so that decoding and re-encoding
  gives back the same string. Any other value is `invalid_manifest`.
- [PART-4] Within a part, `min_key` ≤ `max_key` (equal for a one-row part). Across parts, each part's
  `max_key` is strictly less than the next part's `min_key`. A manifest whose bounds break this is refused
  on open (`invalid_manifest`). A part whose actual first or last key differs from its manifest bounds is
  `bounds_mismatch` (§8).
- [PART-5] When `sha256` is present it MUST match the file's bytes. Checking it reads the whole file, so a
  reader MUST NOT be required to check it on open; validation at the `digest` level does (§8).

### 3.4 Extensions

`extensions` is an object whose member names are namespaces chosen by producers (a tool or organisation
name, for example `"swath"`), each holding any JSON value.

- [EXT-1] Extensions carry information the format does not define, such as what a dataset was produced
  from and whether it covers its whole source. They MUST NOT change the meaning of anything this document
  defines. A reader MUST be able to open and query a dataset while ignoring every extension.
- [EXT-2] A reader that exposes the manifest SHOULD expose extensions unchanged, so that consumers can apply
  their own acceptance policies (for example, "accept only a complete listing of the whole bucket").

## 4. Commit and immutability

- [COMMIT-1] A writer MUST make every part durable, complete and immutable before it publishes the manifest
  that lists it, and MUST publish the manifest atomically (for example by an atomic rename, or a conditional
  write of the whole object), so that a reader observes either no manifest or a complete one.
- [COMMIT-2] A part listed by a published manifest MUST NOT be modified while that manifest may still be
  read.
- [COMMIT-3] A manifest describes one immutable snapshot. Replacing a dataset, keeping several versions,
  and deleting parts are outside this draft.

A dataset is a snapshot: every row is a live record. There are no tombstones, versions or duplicate keys, so
an ordinary Parquet reader that reads every part gets exactly the dataset's rows.

## 5. Part files

Each part is a Parquet file that any conforming Parquet reader can read.

- [FILE-1] Every part is self-contained: its column data, dictionaries and indexes are in the part itself.
  No column chunk may set `file_path`. A reader MUST check this on open, before following any reference
  (`invalid_footer`).
- [FILE-2] Parquet modular encryption is outside this draft. A reader MAY report an encrypted part as
  unsupported (`unsupported_encryption`).

### 5.1 The key column

- [KEY-1] The schema has a top-level column named `key` whose physical type is `BYTE_ARRAY`, whose
  repetition is `REQUIRED`, and which is not nested. It is either unannotated or annotated `STRING` (or the
  legacy converted type `UTF8`). Both annotations order values by unsigned bytes, as §1 does; no other
  annotation is allowed.
- [KEY-5] The file's `column_orders` is present, has one entry per leaf column, and gives the `key` column
  `TYPE_ORDER`, without which
  Parquet leaves the meaning of its statistics and page bounds undefined.
- [KEY-6] When `key` is annotated `STRING` or `UTF8`, every key is valid UTF-8, and so is every page bound,
  shortened or not. An unannotated key may hold any bytes.
- [KEY-2] Every key is non-empty.
- [KEY-3] Keys are unique and strictly ascending across the whole dataset: within each page, from page to
  page, from row group to row group, and from part to part in manifest order.
- [KEY-4] The format sets no maximum key length. A producer's schema MAY impose one.

### 5.2 Row groups and the page index

These requirements apply to every row group that holds at least one row. Row groups with no rows MAY appear
and are ignored.

- [RG-1] The row group's `sorting_columns` is present and its first entry names the `key` column with
  `descending` false. Further entries MAY follow and are ignored.
- [RG-2] The `key` column chunk has both a ColumnIndex and an OffsetIndex.
- [RG-3] The OffsetIndex lists every data page of the chunk, in order, and only those: each entry's
  `offset` and `compressed_page_size` locate exactly one data page header and its page within the column
  chunk. The first page's `first_row_index` is 0, `first_row_index` strictly increases, every value is less
  than the row group's row count, and each page holds exactly the rows from its `first_row_index` up to the
  next page's (or the end of the row group). The list is not empty, and every location lies inside the
  column chunk and the file, computed without integer overflow.
- [RG-4] The ColumnIndex has the same number of entries as the OffsetIndex in each of its parallel lists
  (`null_pages`, `min_values`, `max_values`), and no page is marked as a null page.
- [RG-5] For every page, `min_values[i]` is less than or equal to the page's first key, and `max_values[i]` is
  greater than or equal to the page's last key. These are Parquet's own lower and upper bound rules: a writer
  MAY shorten a bound to save space, but a shortened bound MUST still be a bound. In particular, a writer
  that cannot shorten a maximum to a value that is still at least the page's last key (for example because
  its retained bytes are all `0xFF`) MUST write the full maximum.
- [RG-6] Because bounds may be shortened, the bounds of adjacent pages MAY overlap, and the sequences of
  minima and maxima need not be monotone. A reader MUST NOT treat overlapping or non-monotone bounds as an
  error, MUST NOT rely on `boundary_order`, and MUST NOT infer from a bound's length whether it is exact or
  how far it was shortened. A writer SHOULD set `boundary_order` to `ASCENDING` when both bound sequences
  are non-decreasing, and otherwise to `UNORDERED`.
- [RG-7] A reader MUST refuse a row group whose key column lacks either index (`missing_page_index`), and
  one whose indexes break RG-3 or RG-4, or whose decoded keys contradict RG-5 (`invalid_page_index`).

### 5.3 Other columns

- [COL-1] Columns other than `key` are free. Their names, types and meaning are defined by the producer and
  identified by the manifest's `schema_id`. The format defines no schema registry.
- [COL-2] A part MAY lack a column that other parts have. When a reader is asked for such a column, it MUST
  report the value as absent, which is distinct from a null value.
- [COL-3] A reader MUST support projecting top-level, non-repeated columns of the physical types `BOOLEAN`,
  `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY`. A request to project a
  nested or repeated column, or one of a physical type outside this list (`INT96`), MAY be rejected
  (`unsupported_projection`); it MUST NOT be answered as if the column were absent.
- [COL-4] Value columns SHOULD carry an OffsetIndex, so that a reader can seek to the rows it needs. A reader
  MUST still read a value column that lacks one.
- [COL-5] A column that appears in several parts has the same physical type (including `type_length` for
  `FIXED_LEN_BYTE_ARRAY`), repetition and nesting in each.
  A reader MUST check this for a column no later than the first request that projects it, and SHOULD check
  it for all columns on open (`inconsistent_column`). Structural validation checks it for all columns.
- [COL-7] Projecting a column that no part has is not an error: the column is absent in every row.
- [COL-6] Column names are matched exactly against top-level field names; a `.` in a name is a literal
  character, not a path separator.

### 5.4 Footer key-value metadata

- [STAMP-1] Each part's footer key-value metadata contains:

  | Key | Value |
  |---|---|
  | `seekparquet.format_version` | the manifest's `format_version`, in decimal |
  | `seekparquet.draft_revision` | the manifest's `draft_revision`, in decimal; present iff `format_version` is 0 |
  | `seekparquet.schema_id` | the manifest's `schema_id` |

- [STAMP-2] Each of these keys appears exactly once, and `seekparquet.draft_revision` does not appear when
  `format_version` is not 0. Version values are written in canonical decimal (no sign, no leading zeros)
  and compared as exact strings. A reader MUST refuse a part whose stamps are missing, duplicated, forbidden
  or different from its manifest (`stamp_mismatch`). Other key-value entries MAY be present and are ignored.

### 5.5 Geometry and encoding

The following affect speed and size, not validity.

- [GEO-1] Data pages of the `key` column SHOULD hold at most 1,024 rows. A reader MUST accept any page size.
  Small key pages bound how many keys a seek decodes.
- [GEO-2] Row group size, part size, dictionary use, encodings and compression are the writer's choice,
  within the baseline below.
- [ENC-1] A reader MUST support data page versions 1 and 2; the codecs `UNCOMPRESSED`, `SNAPPY`, `GZIP` and
  `ZSTD`; and the encodings `PLAIN`, `RLE`, `RLE_DICTIONARY`, `PLAIN_DICTIONARY`, `DELTA_BINARY_PACKED`,
  `DELTA_LENGTH_BYTE_ARRAY` and `DELTA_BYTE_ARRAY`. A writer SHOULD use only these. A file outside the
  baseline is not invalid, but a reader MAY report it as unsupported (§8). *Open question for format 1:
  whether `LZ4_RAW` and `BROTLI` join the baseline.*

## 6. Queries

These are the results a reader returns. They are defined on the dataset's rows, independent of how a reader
finds them. A reader MAY use any access path, provided its results are these.

### 6.1 Arguments

| Argument | Range | List | Default | Meaning |
|---|---|---|---|---|
| start | ✓ | | unbounded below | lower bound on keys |
| start exclusive | ✓ | | false (inclusive) | whether the start itself is excluded |
| prefix | ✓ | ✓ | empty | keys must begin with it |
| delimiter | | ✓ | required | exactly one byte |
| after | | ✓ | none | keys must be strictly greater |
| continuation | | ✓ | false | see LIST-5; has no effect without `after` |
| end | ✓ | ✓ | unbounded above | keys must be strictly less |
| limit | ✓ | ✓ | required | maximum number of entries; a positive integer |
| columns | ✓ | ✓ | none | columns to project, by exact name (COL-6) |

- [QUERY-1] A limit that is not a positive integer, or a delimiter that is not exactly one byte, is an
  invalid request, not an empty result.
- [QUERY-2] Boundaries are byte strings and MAY be empty. An empty start includes every key; an empty end
  includes none, since every key is non-empty and so greater than the empty string.
- [QUERY-3] "All results" below means the results of the same request with a limit at least the number of
  rows in the dataset.

### 6.2 Range

- [RANGE-1] The result is the rows whose keys begin with the prefix, are at or after the start (strictly
  after, when the start is exclusive) and are strictly before the end, in ascending key order, at most limit
  of them.
- [RANGE-2] Each returned row carries its key and, for each projected column in request order, the value,
  null, or absent (COL-2).

A **prefix** read of prefix `p` is a range with that prefix and no other bounds: equivalently, the range from
`p` inclusive to `successor(p)`, or unbounded above when `p` has no successor.

### 6.3 Delimited listing

The **candidate keys** are the keys `k` that begin with the prefix `p`, are greater than `after` when it is
given, and are less than the end when it is given. Each candidate key yields one entry:

- [LIST-2] If the part of `k` after `p` contains the delimiter `d`, let `i` be the index in `k` of the
  first `d` at or after position `len(p)`. The entry is the **common prefix** `k[0 : i+1]` (half-open: `k`
  up to and including that delimiter byte).
- [LIST-3] Otherwise the entry is the **row** `k`, with its projected values. In particular a key equal to
  `p` is a row, and a key that ends with `d` right after `p` (for example `p` = `a/`, `k` = `a/b/`) rolls up
  to itself as a common prefix.
- [LIST-4] Entries are distinct, and are returned in ascending byte order of the entry's bytes (a common
  prefix compares as its own bytes, a row as its key), at most limit of them. Every key under a common
  prefix sorts at or after the prefix and before any key outside it, so this is the order in which the
  entries' first candidate keys occur.
- [LIST-5] When `continuation` is true and `after` is given, a common prefix that sorts at or before
  `after` is not returned, because the page that ended at `after` already returned it. Otherwise a common
  prefix is returned if any candidate key rolls up to it, even if the prefix itself sorts at or before
  `after`.
- [LIST-7] Bounds apply to candidate keys, not to entries. For example, with `p` empty, `d` = `/` and end
  `a/b`, the keys `a/a` and `a/z` give the common prefix `a/` only because `a/a` is a candidate; if `a/a`
  did not exist, `a/z` alone would not make `a/` appear.

**Pagination.** To read the page after a page whose last entry is `e`, repeat the request with `after` set
to `e`'s bytes and `continuation` true [LIST-6]. Concatenating the pages of a listing gives exactly all
results (QUERY-3), with no duplicates or gaps. After a common prefix `c`, a reader MAY resume directly at
`successor(c)`; when `c` has no successor, the listing is exhausted.

## 7. What readers check, and when

A reader checks what it reads. It does not certify data it skips.

- [CHECK-1] **On open**, a reader MUST check the manifest (§3); that every part file exists and has the
  manifest's exact size; that every part's footer is readable and self-contained (FILE-1), and that its
  row groups' row counts sum to the footer's own row count (`invalid_footer` otherwise), which equals the
  manifest's `rows` (`part_mismatch` otherwise); that its stamps agree (§5.4); that its `key` column meets
  KEY-1 and KEY-5; COL-5 as required there; and RG-1 for every non-empty row group. A failure refuses the
  dataset.
- [CHECK-2] **On read**, for every row group a reader reads, whether or not it uses the page index, it MUST
  first check RG-2 to RG-4 (the parts of RG-3 that need page headers, as it reaches each page). For every key
  page it decodes, it MUST check that the page holds exactly the rows its OffsetIndex entry gives it, that
  every key is non-empty (KEY-2) and valid UTF-8 where KEY-6 applies, that the keys are strictly ascending
  within the page and lie within the page's bounds (RG-5), and that the page's keys are consistent with the
  dataset order of every other page it has decoded in the same request, whatever order it decoded them in. A
  page read again, or served from a cache of already checked pages, needs no new check. A failure is
  reported with its code and ends the request; the reader MAY also mark the dataset unusable.
- [CHECK-3] A reader MAY check more on open, for example by decoding the first key of every row group, or
  each part's first and last key against the manifest. It MUST NOT be required to read every row to open a
  dataset.
- [CHECK-4] A successful query does not certify rows the reader did not decode. Full certification is
  validation (§8).
- [CHECK-5] Corruption in the Parquet data itself (a page that does not decompress or decode, a malformed
  dictionary, a value count that does not match the rows) is reported as `invalid_parquet` when a reader
  meets it. A failure to read storage is not a refusal of the data, and a valid feature the reader does not
  support is reported as unsupported (§10).

## 8. Validation

Validation is a separate procedure. A validator MUST NOT let the indexes it is checking decide which rows it
visits: it enumerates pages and rows independently of the seek path.

| Level | Requires | Checks |
|---|---|---|
| `structural` | – | CHECK-1, plus RG-2 to RG-4 for every non-empty row group, reading page headers but not page data |
| `profile` | `structural` | every page of every `key` column chunk, enumerated from the page headers and compared with the OffsetIndex (offsets, sizes, row positions, count: RG-3); every key (KEY-2, KEY-3, KEY-6); RG-5 for every page; and each part's exact first and last key and row count against the manifest (PART-3, PART-4) |
| `full` | `profile` | every stored column decoded in full: every page decompresses and decodes, and every column holds exactly the row group's row count |
| `digest` | – | `sha256` of every part that declares one (PART-5); independent of the other levels |

- [VAL-1] A validator reports, for each level, whether it completed, failed (with the refusal code), or is
  incomplete (with the reason). A level is complete only if its prerequisite is complete. If a column,
  encoding or codec the level needs is not supported, the level is incomplete, never complete; levels below
  it keep their result.
- [VAL-2] The `digest` level completes when every declared `sha256` matches. When no part declares one it
  completes with nothing checked, and the report says so.
- [VAL-3] Validation checks the dataset as stored. It cannot show that the manifest was published
  atomically, or that a producer captured its whole source; such properties belong to producers and their
  extensions.

## 9. Versions

Several version identities move independently:

| Identity | Where | Meaning |
|---|---|---|
| format version | `format_version`, `seekparquet.format_version` | the on-disk contract; 0 for drafts, 1 for this format once final |
| draft revision | `draft_revision`, `seekparquet.draft_revision` | which draft a `format_version` 0 dataset follows |
| specification release | a `spec/v…` tag | an immutable release of this document and its schemas |
| conformance revision | `conformance/suite.json` | an immutable revision of the shared test suite |
| implementation release | a `go/v…` or `java/v…` tag | an implementation's API and code |
| schema id | `schema_id` | the producer's columns; unrelated to everything above |

- [VER-1] An implementation MUST declare the (`format_version`, `draft_revision`) pairs it supports and MUST
  refuse every other pair (`unsupported_version`).
- [VER-2] A change that alters which bytes are valid or what a query returns requires a new draft revision
  (while in draft) or a new format version (after format 1 is final).

## 10. Refusal codes

When a reader refuses a dataset or ends a request because the data breaks this document, it reports one of
the codes in [refusal-codes.json](refusal-codes.json). Codes are stable; diagnostic text is not part of the
contract. A request that is malformed (QUERY-1) is an invalid request, and a dataset that is valid but uses a
feature the reader does not support is unsupported, with one of the `unsupported_*` codes; neither is a
refusal of the data. A failure to read storage is an error of the environment, not a refusal.

When one dataset has several faults, a reader MAY report any code that applies to a fault it detected.

## 11. Security considerations

A reader handles files from untrusted sources. It MUST bound memory by the sizes the files declare and not
trust them blindly (a page claiming gigabytes, a footer larger than its file), MUST confine part paths to
the dataset root (PART-1), and SHOULD apply limits on manifest size, number of parts and page size that its
users can configure.
