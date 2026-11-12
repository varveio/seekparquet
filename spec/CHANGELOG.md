# Specification changelog

Unreleased draft revision 1 is edited in place; no format has been released.

## Protocol documents

- [cursor-token-1.md](cursor-token-1.md) adds version 1 of the cursor token (`spc1.r.<key>.<offset>`,
  `spc1.p.<prefix>`), a canonical text encoding of PAGE-1 cursors for protocols, with golden vectors in
  `conformance/vectors/`. It affects no valid file and no query result, and needs no draft revision.

## Draft revision 1 (unreleased)

First public draft of format 1. Files carry `format_version` 0, `draft_revision` 1.

- MAN-8 requires the exact key column name and a uniqueness boolean in the manifest.
- STAMP-1/STAMP-2 require matching, exactly-once key column and canonical uniqueness stamps.
- KEY-1/KEY-3/KEY-7 and PART-4 allow repeated-key groups in file order, confined to one part.
- ROUTE-1 preserves the earliest matching row group and page; RG-5/RG-6 bounds rules are unchanged.
- RANGE-1, LIST-3/LIST-4, PAGE-1/PAGE-2 and LIST-6 define whole-group filtering, group lookups,
  and pagination within groups using tagged row-offset or prefix cursors.
- The tagged cursor is the only pagination contract: the listing byte floor (`after` as bytes) and the
  `continuation` flag are removed. A row cursor `(k, n)` admits rows after that position; offsets count
  the complete stored group before filtering, an offset beyond the group skips it, and a nonzero offset
  in unique mode is allowed. A prefix cursor `c` admits keys after `c` that do not begin with `c`, and is
  valid for listing only; on a range it is an invalid request (QUERY-1).
- §6.3 defines candidate rows by the cursor; LIST-5 now states that a common prefix appears exactly when a
  candidate key rolls up to it (the continuation suppression rule is gone); LIST-6 follows from the prefix
  cursor.
- LIST-8: a common prefix carries its exact row, the first candidate row that rolls up to it when that row's
  key is the prefix itself, with its offset and projected values. No change to valid files.
- VAL-1 states that levels requiring an incomplete level are incomplete, and that digest validation reads
  the listed part bytes without opening them as Parquet.
- RG-2 names the key column by `key.column`; KEY-7 says a group never spans parts, as PART-4 does.
- CHECK-2 and profile validation apply the declared ordering mode over decoded rows and every row,
  respectively. Existing refusal codes cover all new faults; no new code is needed.
- The snapshot holds live records, possibly several per key; producer-defined columns carry any
  version, position, identity or direction semantics.

### Pre-release compatibility pass ([proposal 0001](proposals/0001-prerelease-compatibility-pass.md))

- §3.3, §3.4, MAN-3, EXT-1 to EXT-5: each part MAY carry an `extensions` object, with the same schema as the
  top-level one (`$defs/extensions` in the schema). Readers ignore unknown namespaces and members (EXT-3);
  extension metadata never changes validity, query results, required checks or core members (EXT-1); unusable
  accelerator metadata disables the accelerator and never refuses valid data (EXT-3); the JSON rules still
  apply inside extensions (EXT-4); the namespace `seekparquet` is reserved for accelerators the specification
  documents, of which there are none yet (EXT-5). The core manifest stays closed. Manifests with per-part
  extensions become valid; no valid file becomes invalid and no query result changes.
- ENC-1: `LZ4_RAW` joins the required codec baseline, and is distinct from the deprecated `LZ4`. `BROTLI` stays
  valid and outside the baseline (`unsupported_encoding`, never a refusal). An unsupported codec in a column a
  request does not need must not prevent that request. The open question is closed. No file changes validity;
  a reader without `LZ4_RAW` no longer conforms.
- CHECK-1, FILE-2: a reader checks both the leading and the trailing magic when it first touches a part:
  `PARE` at either end is `unsupported_encryption`, anything else but `PAR1` is `invalid_footer`. No file
  changes validity.
- CHECK-5, §11: a limit of the reader's resources is not a refusal of the data. CHECK-6: a reader never
  combines bytes of two versions of a part, such as cached metadata with a same-sized replacement; storage
  identity is the storage layer's to establish.
- §6.1: streaming operations without a result limit are outside QUERY-1's paginated interface; their output
  follows the range semantics and they keep every check. QUERY-1 is unchanged.
- VER-2 and the README state when in-place editing of draft revision 1 ends: at its first release, the first
  specification tag or the first implementation release that writes files of this draft for others to read.
  No version number changes.
