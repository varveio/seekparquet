# Releasing

Each component is versioned and released on its own. One commit may carry several tags.

| Component | Tag | Notes |
|---|---|---|
| Specification | `spec/v1.0.0-draft.N` while in draft, `spec/v1.0.0` when frozen | `spec/status.json` and `spec/CHANGELOG.md` updated in the same commit |
| Conformance suite | `conformance/vX.Y.Z` | `conformance/suite.json` revision and `fixtures.lock.json` updated |
| Go module | `go/vX.Y.Z` | module `github.com/varveio/seekparquet/go`; consumers request `vX.Y.Z` |
| Java implementation | `java/vX.Y.Z` | `io.varve:seekparquet`, not published to any repository yet: a conformance reference built from source |

## Gates

- An implementation release declares, in its changelog entry, the format versions and draft revisions it
  supports, the conformance revision it passed (`conformance/suite.json`), and the targets it qualified for,
  with the harness's `--json` report attached to the release. A run limited by `--filter` or `--targets` is not
  a qualification.
- A release claiming a new specification draft or format must pass the conformance revision that goes with it.
- Releases are tagged on `main` from a commit whose CI passed. Tags are protected.
- A conformance revision that `spec/status.json` or an implementation changelog pins is tagged
  (`conformance/vX.Y.Z`, the `revision` of `conformance/suite.json`), so the pin resolves to a commit.

## Specification

- A draft revision is edited in place until it is first released: tagged (`spec/v1.0.0-draft.N`), or written by
  an implementation release for others to read, whichever comes first (VER-2). After that, a change that
  affects valid files, query results or refusal codes goes to draft revision N+1, through a proposal, and the
  on-disk `draft_revision` moves with it; readers refuse the pair they do not support (MAN-7). Editorial changes
  stay within the tagged draft.
- Freezing follows GOVERNANCE.md; the frozen document is `spec/v1.0.0`, on disk `format_version` 1, and later
  editorial corrections are `spec/v1.0.x`.

## Go

The module's version is plain semantic versioning for the implementation, independent of the
specification's state. Tags carry the `go/` prefix because the module lives in `go/`; consumers write
`go get github.com/varveio/seekparquet/go@vX.Y.Z`.

- Until v1 there is no compatibility promise: a breaking API change bumps the minor (v0.2.0), a fix the
  patch (v0.1.1). Pre-release suffixes (`-rc.N`) are reserved for candidates before v1.0.0.
- The specification's state is tracked separately, in `spec/status.json` and in the stamps the files
  carry. Each module release names, in its changelog entry, the format draft revision and the conformance
  revision it implements.
- Until the first release, consumers may pin a merged `main` commit as a Go pseudo-version
  (`go get github.com/varveio/seekparquet/go@<sha>`). Tags are cut only for releases, not to hand a
  consumer a build.
- `go/v0.1.0-draft.1` is superseded by `go/v0.1.0` (same commit). It is kept only because tags are
  protected and the module proxy has cached it; `go/go.mod` retracts it, so `go get` and `go list -m -versions`
  skip it. A version that must not be used is always retracted in `go.mod`, never only documented.
- A major version v2 or later changes the module path to `github.com/varveio/seekparquet/go/v2` (a `go.mod`
  edit and import path change in the same commit) and is tagged `go/v2.0.0`; a `go/v2.0.0` tag without the path
  change is unusable by Go tooling.

1. Update `go/CHANGELOG.md`.
2. Tag `go/vX.Y.Z` on the release commit and push the tag.
3. Check that `go list -m github.com/varveio/seekparquet/go@vX.Y.Z` resolves from an unrelated module.
