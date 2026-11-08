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
- **Conformance** (`conformance/`): Python 3.11 or later. The harness uses only the standard library; the
  fixture generators need the pinned packages in `conformance/generators/requirements.txt`. Run the suite
  against the Go adapter with:

  ```bash
  python3 conformance/harness/run.py --adapter "go run ./go/internal/cmd/conformance"
  ```

Changing fixture bytes needs a reason in the pull request; see [conformance/README.md](conformance/README.md).

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
