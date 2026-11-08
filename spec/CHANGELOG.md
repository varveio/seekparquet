# Specification changelog

Unreleased draft revision 1 is edited in place; no format has been released.

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
- VAL-1 states that levels requiring an incomplete level are incomplete, and that digest validation reads
  the listed part bytes without opening them as Parquet.
- RG-2 names the key column by `key.column`; KEY-7 says a group never spans parts, as PART-4 does.
- CHECK-2 and profile validation apply the declared ordering mode over decoded rows and every row,
  respectively. Existing refusal codes cover all new faults; no new code is needed.
- The snapshot holds live records, possibly several per key; producer-defined columns carry any
  version, position, identity or direction semantics.
