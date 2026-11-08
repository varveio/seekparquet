# Example manifests

These manifests illustrate the structure of `manifest.json`. They are not complete datasets: the part files
they name do not exist here. Complete datasets, valid and invalid, are in [`../../conformance/datasets/`](../../conformance/datasets/).

| File | Structurally | Why |
|---|---|---|
| `empty-manifest.json` | valid | an empty dataset (`parts: []`) |
| `two-part-manifest.json` | valid | two parts, with producer metadata under `extensions` |
| `invalid/unknown-member.json` | invalid (`invalid_manifest`, MAN-3) | `created_at` is not a defined member |
| `invalid/draft-without-revision.json` | invalid (`invalid_manifest`, MAN-7) | `format_version` 0 without `draft_revision` |
| `invalid/path-escape.json` | invalid (`invalid_manifest`, PART-1) | a `..` segment |
| `invalid/empty-part.json` | invalid (`invalid_manifest`, PART-2) | a part with `rows` 0 |

`repeated-keys-manifest.json` declares non-decreasing keys. Groups may span pages and row groups,
but the disjoint part bounds keep every group within one part. All manifests declare the exact key column.
