# Governance

SeekParquet is maintained by Varve Systems Ltd and developed in the open. The maintainers are listed in
[MAINTAINERS.md](MAINTAINERS.md).

## Decisions

- Day-to-day changes are decided by maintainer review on pull requests.
- Changes to the specification that affect valid files, query results or refusal codes go through a numbered
  proposal ([spec/proposals/](spec/proposals/)). The decision and its reasons are recorded publicly in the
  proposal.
- A specification change is reviewed by the maintainers of every active implementation before it merges, and
  lands together with the conformance cases that test it.

## Freezing a format

A draft format becomes final, and its version number is first used on disk, when all of the following hold:

1. The candidate specification and a conformance revision are pinned.
2. Two independently implemented readers, built on different Parquet libraries, pass every mandatory case of
   that revision, including listing and refusal cases.
3. Both pass the indexed-access suite: a reader that scans everything does not qualify.
4. The validator accepts every valid fixture and rejects every invalid one.
5. The evidence (implementation commits, Parquet library versions, suite digest, test reports) is recorded
   in the freeze pull request.
6. The maintainers approve that pull request publicly; `spec/status.json` becomes `final` and the
   specification is tagged.

After a freeze, editorial corrections are patch releases of the document. Anything that changes accepted
bytes or query results needs a proposal and a new format version.

## Becoming a maintainer

A contributor with a record of sustained, careful contributions to a component may be invited by the existing
maintainers to maintain it. Maintainers who step back are listed as emeritus.
