# Releasing

Each component is versioned and released on its own. One commit may carry several tags.

| Component | Tag | Notes |
|---|---|---|
| Specification | `spec/v1.0.0-draft.N` while in draft, `spec/v1.0.0` when frozen | `spec/status.json` and `spec/CHANGELOG.md` updated in the same commit |
| Conformance suite | `conformance/vX.Y.Z` | `conformance/suite.json` revision and `fixtures.lock.json` updated |
| Go module | `go/vX.Y.Z` | module `github.com/varveio/seekparquet/go`; consumers request `vX.Y.Z` |

## Gates

- An implementation release declares, in its changelog entry, the format versions and draft revisions it
  supports, the conformance revision it passed (`conformance/suite.json`), and the targets it qualified for,
  with the harness's `--json` report attached to the release. A run limited by `--filter` or `--targets` is not
  a qualification.
- A release claiming a new specification draft or format must pass the conformance revision that goes with it.
- Releases are tagged on `main` from a commit whose CI passed. Tags are protected.

## Go

1. Update `go/CHANGELOG.md`.
2. Tag `go/vX.Y.Z` on the release commit and push the tag.
3. Check that `go list -m github.com/varveio/seekparquet/go@vX.Y.Z` resolves from an unrelated module.
