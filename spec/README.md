# SeekParquet specification

| File | What it is |
|---|---|
| [format-1.md](format-1.md) | The normative specification of format 1. **Draft revision 1.** |
| [cursor-token-1.md](cursor-token-1.md) | The cursor token, version 1: a canonical text encoding of continuation cursors for protocols. Not part of the format; it changes no file or query result. |
| [manifest.schema.json](manifest.schema.json) | JSON Schema for `manifest.json` (structure only; the semantic rules are in the specification). |
| [refusal-codes.json](refusal-codes.json) | The stable refusal codes, what each means, which requirements it covers and when a reader must detect it. |
| [status.json](status.json) | Machine-readable status: draft or final, the draft revision, the on-disk version, the pinned conformance revision. |
| [CHANGELOG.md](CHANGELOG.md) | Changes, and whether each one affects valid files or query results. |
| [examples/](examples/) | Example manifests, valid and invalid. |
| [proposals/](proposals/) | Numbered proposals for substantive changes. |

## Drafts and the freeze

While format 1 is a draft, files are stamped `format_version` 0 with a `draft_revision`, so a draft file can
never be mistaken for a final one. A change that affects valid files or query results increments the draft
revision. Format 1 is frozen, and the number 1 first used on disk, when two independently implemented readers
on different Parquet libraries pass the same pinned conformance revision; the process is in
[GOVERNANCE.md](../GOVERNANCE.md).
