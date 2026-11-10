# Contributing to SeekParquet

Thank you for helping. This repository holds the SeekParquet specification, its shared conformance suite and
its implementations. Changes to any of them are welcome.

## Where things go

| Change | How |
|---|---|
| A bug in an implementation | an issue, then a pull request with a test that fails before the fix |
| An editorial fix to the specification (wording, typos, examples) | a pull request |
| A change to which files are valid, what a query returns, or the refusal codes | a proposal first: see [spec/proposals/](spec/proposals/) |
| A new conformance case | a pull request; expected answers must come from the source rows and the specification, never from an implementation's output |
| A new implementation in another language | open an issue first, so we can agree on layout and conformance |

Questions go in GitHub issues for now.

## Development

- **Go** (`go/`): Go 1.26 or later. `cd go && gofmt -l . && go vet ./... && go test -race ./...`
- **Java** (`java/`): JDK 21 or later. `cd java && ./gradlew build :conformance:installDist`, then
  run the reader conformance suite from the repository root:

  ```bash
  python3 conformance/harness/run.py --adapter java/conformance/build/install/seekparquet-conformance/bin/seekparquet-conformance
  ```

- **Conformance** (`conformance/`): Python 3.11 or later. The harness uses only the standard library; the
  fixture generators need the pinned packages in `conformance/generators/requirements.txt`. Run the suite
  against the Go adapter with:

  ```bash
  python3 conformance/harness/run.py --adapter "go -C go run ./internal/cmd/conformance"
  ```

Changing fixture bytes needs a reason in the pull request; see [conformance/README.md](conformance/README.md).

## Continuous integration

`ci-ok` is the required aggregate check. For affected Go code, PRs run format/vet/module checks,
all tests and fuzz seeds in three Linux race shards, and the complete Windows suite without the
race detector. Java still reads Go-written datasets on every Go change. Conformance and Java
checks retain their full assertions when selected; source selection does not select individual tests.

The expensive root-package differential tests each have a shard; the remainder includes every
other root test and every other package. `go-lint` checks the live Go test inventory for exactly
one shard per test. New tests and packages enter the remainder automatically; renaming a dedicated
test fails that check. Reproduce a shard from the repository root with
`python3 scripts/ci/go-shards.py --run differential` (or `randomized`, `rest`), or verify the
inventory with `--check`. Race runs use `-count=1` and keep JSON diagnostics for 14 days.

Full macOS race tests run on every `main` push, daily at 05:17 UTC, and manual workflow dispatch.
PRs also run them for Go dependency/platform paths, CI changes, unknown inputs, deletions/renames,
or the `ci:full` label. Documentation-only PRs may skip runtime suites. Missing base/diff data
selects full validation; a failed diff fails CI. Main, scheduled and manual runs always select
all suites. Label changes reevaluate the policy; superseded PR runs cancel, while main runs
remain independent. Deferring ordinary PR macOS tests trades earlier platform feedback for a
shorter PR loop; a failing main/nightly platform check still requires a fix.

Go module caches are shared per platform and dependency set; build caches are separate
per toolchain, job and race shard. Only main pushes save Go caches, and cache maintenance bounds old generations.
Gradle and pip also cache dependencies, including caches scoped to individual PRs.
Caches never substitute for running Go tests.

## Developer Certificate of Origin

Contributions are accepted under the [Developer Certificate of Origin][dco]. Add a `Signed-off-by` line to
every commit to certify that you wrote the change or otherwise have the right to submit it under the project's
license:

```
Signed-off-by: Your Name <your.email@example.com>
```

`git commit -s` adds it for you.

## Code of conduct

Everyone taking part follows the [code of conduct](CODE_OF_CONDUCT.md).

[dco]: https://developercertificate.org/
