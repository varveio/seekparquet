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
- CHECK-2 and profile validation apply the declared ordering mode over decoded rows and every row,
  respectively. Existing refusal codes cover all new faults; no new code is needed.
- The snapshot holds live records, possibly several per key; producer-defined columns carry any
  version, position, identity or direction semantics.
