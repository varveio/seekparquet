# SeekParquet specification

| File | What it is |
|---|---|
| [format-1.md](format-1.md) | The normative specification of format 1. **Draft revision 1.** |
| [IMPLEMENTING.md](IMPLEMENTING.md) | A guide for implementers. Not normative. |
| [cursor-token-1.md](cursor-token-1.md) | The cursor token, version 1: a canonical text encoding of continuation cursors for protocols. Not part of the format; it changes no file or query result. |
| [manifest.schema.json](manifest.schema.json) | JSON Schema for `manifest.json` (structure only; the semantic rules are in the specification). |
| [refusal-codes.json](refusal-codes.json) | The stable refusal codes, what each means, which requirements it covers and when a reader must detect it. |
| [status.json](status.json) | Machine-readable status: draft or final, the draft revision, the on-disk version, the pinned conformance revision. |
| [CHANGELOG.md](CHANGELOG.md) | Changes, and whether each one affects valid files or query results. |
| [examples/](examples/) | Example manifests, valid and invalid. |
| [proposals/](proposals/) | Numbered proposals for substantive changes. |

## Drafts and the freeze

While format 1 is a draft, files are stamped `format_version` 0 with a `draft_revision`, so a draft file can
never be mistaken for a final one. [VER-2](format-1.md#ver-2) says when a draft revision stops being edited
in place and when a change needs a new one. The release steps are in [RELEASING.md](../RELEASING.md). Format 1 is frozen,
and the number 1 first used on disk, by the process in [GOVERNANCE.md](../GOVERNANCE.md).

## Implementing

[IMPLEMENTING.md](IMPLEMENTING.md) is a guide for writing a reader, validator or writer in another language:
the reading order, what to check when, the mistakes the conformance suite catches, and how to qualify. Each
requirement has a stable link: `format-1.md#key-3` for KEY-3.
