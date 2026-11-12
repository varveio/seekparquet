# Compatibility and stability

SeekParquet is pre-release. This page says what you can rely on now, what may change, and where each
change is announced. The current versions are in [spec/status.json](../../spec/status.json).

## What is stable before 1.0

| Item | Promise | How it changes | Where announced |
|---|---|---|---|
| Files on disk | A dataset carries its version pair: `format_version` 0 and a `draft_revision` while format 1 is a draft. A reader refuses every pair it does not support ([VER-1](../../spec/format-1.md#ver-1)), but the pair alone does not establish compatibility between main commits before the first release identified by VER-2. | Draft revision 1 is edited in place until its first release. After that, any change to which files are valid, what a query returns, or which code a fault gets is a new draft revision ([VER-2](../../spec/format-1.md#ver-2)). Format 1 becomes final, stored as `format_version` 1, only through the freeze in [GOVERNANCE.md](../../GOVERNANCE.md). | [spec/CHANGELOG.md](../../spec/CHANGELOG.md), [proposals](../../spec/proposals/) |
| Refusal codes | Names and meanings are stable ([§10](../../spec/format-1.md#10-refusal-codes)). Match on the code, never on error text. | Until the first release identified by VER-2, draft revision 1 may change in place. After that release, changes to validity, query semantics or required refusal classifications require a new revision. | [spec/refusal-codes.json](../../spec/refusal-codes.json), spec changelog |
| Unsupported codes | Names are stable. | As refusal codes. A reader may come to support a feature, so an unsupported code can disappear for a dataset. | implementation changelogs |
| Cursor tokens | The current Go and Java readers implement the shared `spc1` draft encoding ([cursor token, version 1](../../spec/cursor-token-1.md)). Readers implementing that encoding interpret its tokens identically; older releases may not support it. | A different encoding gets a different version prefix. | spec changelog |
| Conformance suite | A published suite revision is immutable. The adapter protocol is versioned with the suite. | A new revision, recorded in `conformance/suite.json`. | [conformance/README.md](../../conformance/README.md) |

## What may change in any release

| Item | Status | Where announced |
|---|---|---|
| Go API, module `github.com/varveio/seekparquet/go` | Version 0: any minor version may break the API ([RELEASING.md](../../RELEASING.md)). This documentation describes `main`; the released `go/v0.1.0` has an earlier API. | [go/CHANGELOG.md](../../go/CHANGELOG.md), which lists every break |
| Java API, `io.varve:seekparquet` | Not published to any repository. Build it from source; there is no compatibility promise. | [java/CHANGELOG.md](../../java/CHANGELOG.md) |
| Default limits and cache budgets | May change in any release. Set the [limits](limits.md) you depend on explicitly. | implementation changelogs |
| [Read statistics](read-stats.md) | Diagnostics of one reader's access path. Fields may be added, renamed or removed, and counts for the same read may differ between versions and between Go and Java. | implementation changelogs |
| The list index, read-ahead, caches | Implementation features, not part of the format. They never change a result. | implementation changelogs |
| Error text | For people. Never parse it. | – |

## Using the Go module before its next release

Depend on a commit of `main`:

```sh
go get github.com/varveio/seekparquet/go@main
```

Go records it as a pseudo-version. Pin that version in `go.mod` and update it deliberately: any commit may
change the API.

## Platforms and toolchains

| | Requirement | Platforms |
|---|---|---|
| Go module | Go 1.26 or later | Linux and macOS. Linux is tested in CI; macOS CI runs selectively by changed paths on pull requests and on full runs (main, nightly, manual or `ci:full`). Windows is best-effort: it is tested in CI, but `local` cannot sync a directory there and publishes only with `AllowUnsynced`. |
| Java reader | JDK 21 or later | Linux. macOS is expected to work but is not tested in CI. Windows is best-effort: it is tested in CI; `storage.LocalStorage` takes a checked-path route there because the default file system has no secure directory streams. |

The specification and the conformance suite are platform-independent. The conformance harness needs Python
3.11 or later.
