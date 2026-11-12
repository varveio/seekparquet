# Validating datasets

Validation checks the scope of the requested level: structural metadata and the key pages' indexed headers;
all keys at profile; all stored columns at full; or the declared part digests at digest. Successful completion
certifies only that level's scope. Reading checks only what a read touches
([concepts](../concepts.md#what-is-checked-and-when)). Recommendation: validate what you produce before others
read it, and what you receive before you trust it.

Validation is Go only: the `seekparquet` command and the `ValidateDir` and `Validate` functions. The Java
module does not validate.

## Levels

Validation runs at the four levels of [§8](../../spec/format-1.md#8-validation) of the specification.

| Level | Requires | Checks |
|---|---|---|
| `structural` | – | the manifest and every part as a first touch does ([CHECK-1](../../spec/format-1.md#check-1)), and every non-empty row group's key page index, reading page headers but not page data |
| `profile` | `structural` | every key page, found from the page headers and compared with the OffsetIndex; every key's order and bounds; each part's first and last key against the manifest |
| `full` | `profile` | every page of every other column decompresses and decodes to exactly the row group's rows |
| `digest` | – | the SHA-256 of every part whose manifest entry declares one |

Asking for `full`, the default, runs `structural` and `profile` first. `digest` runs alone. Profile and full
enumerate their column chunks independently of the indexes. Profile compares the key indexes with the
pages found; full walks every other stored column chunk.
Structural checks the key indexes and the page headers their entries locate. Digest checks declared hashes,
whether or not the parts open as Parquet ([VAL-1](../../spec/format-1.md#val-1)).

A level stops at the first fault it meets. A failed level reports that fault, not every fault in the dataset;
the levels that require it are then incomplete.

Each level ends in one of three results ([VAL-1](../../spec/format-1.md#val-1)):

| Result | Meaning |
|---|---|
| `complete` | the level checked everything it covers and found no fault |
| `failed` | the level met a fault; the report gives its [refusal code](../reference/refusal-codes.md) and where. A version pair the validator does not support is reported with the refusal code `unsupported_version` |
| `incomplete` | the level could not check everything, for a reason that is not a fault of the data: an unsupported feature, a limit, a storage error, or a prerequisite level that did not complete. It says nothing about validity |

A `digest` level that finds no declared digest is `complete` and says so ([VAL-2](../../spec/format-1.md#val-2)).

## Validate from the command line

Install the command:

```sh
go install github.com/varveio/seekparquet/go/cmd/seekparquet@main
```

Or run it from a checkout, as below: `go -C go run ./cmd/seekparquet`.

```text
usage: seekparquet validate [--level structural|profile|full|digest] [--json] DIR
```

From the repository root, validating the conformance suite's `multipart` fixture:

```sh
seekparquet validate conformance/datasets/multipart
```

```text
dataset conformance/datasets/multipart
structural  complete    3 parts, 8 row groups, 36 key pages located
profile     complete    8 key column chunks, 36 pages, 134 keys
full        complete    24 other column chunks, 108 pages, 134 rows
result: complete
```

The exit status:

| Status | Meaning |
|---|---|
| 0 | every level completed |
| 1 | a level failed: a profile fault or an unsupported dataset version |
| 2 | a level is incomplete, validation could not run, or the command was used wrongly |

With `--json` the report is one JSON object. For a dataset whose keys decrease inside a page:

```sh
seekparquet validate --json conformance/datasets/invalid/key-order-within-page
```

```json
{
  "dataset": "conformance/datasets/invalid/key-order-within-page",
  "result": "failed",
  "levels": [
    {
      "level": "structural",
      "result": "complete",
      "checked": {"parts": 1, "row_groups": 2, "chunks": 0, "pages": 6, "rows": 0, "digests": 0}
    },
    {
      "level": "profile",
      "result": "failed",
      "checked": {"parts": 0, "row_groups": 0, "chunks": 0, "pages": 1, "rows": 2, "digests": 0},
      "code": "key_order",
      "location": {"part": "data/part-00000.parquet", "row_group": 0, "column": "key", "page": 1, "row": 3},
      "detail": "the key breaks the declared key order with the key before it"
    },
    {
      "level": "full",
      "result": "incomplete",
      "checked": {"parts": 0, "row_groups": 0, "chunks": 0, "pages": 0, "rows": 0, "digests": 0},
      "reason": "its prerequisite profile is failed"
    }
  ]
}
```

`code`, `location` and `detail` appear on a failed level, and `reason` on an incomplete one. Fields of
`location` that do not apply are left out. The `detail` and `reason` texts are for people; the code is the
contract.

## Validate from Go

`ValidateDir` validates a local directory. `Validate` validates through any `Source`, the same storage
interface the reader uses.

**Go**

<!-- snippet: go/example_docs_test.go#ExampleValidateDir -->
```go
ctx := context.Background()
report, err := seekparquet.ValidateDir(ctx, "../conformance/datasets/multipart", seekparquet.ValidateOptions{Level: seekparquet.LevelFull})
if err != nil {
	log.Fatal(err) // the directory cannot be opened, or ctx ended
}
for _, level := range report.Levels {
	fmt.Println(level.Level, level.Result)
}
fmt.Println("result:", report.Result())
// Output:
// structural complete
// profile complete
// full complete
// result: complete
```
<!-- /snippet -->

**Java:** not available.

The returned error is not the validation result. It is non-nil only when the directory cannot be opened, the
level is unknown, or the context ends. Read the result from the report: `Report.Result()` for the whole, and
`Report.Levels` for each level's `Result`, `Failure` (a `*RefusalError` with its code and location), `Reason`
and `Err` (the cause of an incomplete level).

`ValidateOptions.Limits` applies the [specific input and decoding limits](../reference/limits.md) used by
the reader; it does not impose an overall memory budget. A limit exceeded leaves the level incomplete; it is not a fault of the data.

## Validation, verify and reading

| | Reading | Verify | Validation |
|---|---|---|---|
| What | what each read touches | every part's footer; each non-empty row group's key page index and first key page, and each part's last key page; each part's first and last key against the manifest | the scope of the level asked for |
| When it runs | as you read | when you call Go `Dataset.Verify` or Java `Dataset.verify` (implementation behavior; [CHECK-3](../../spec/format-1.md#check-3) makes it optional) | when you run it |
| Reads | what the read needs | one footer per part not yet checked, plus key page indexes and a few key pages per row group; reuses prior checks and cached metadata | structural: metadata and page headers; profile: every key page; full: every page; digest: every part with a declared digest, whole |
| Languages | Go, Java | Go, Java | Go |

A successful read does not certify the rows it skipped ([CHECK-4](../../spec/format-1.md#check-4)). Neither
does `Verify`. Validation shows what is stored; it cannot show that the manifest was published atomically or
that the producer captured its whole source ([VAL-3](../../spec/format-1.md#val-3)).

When validation fails, [troubleshooting](troubleshooting.md) says what each code usually means and what to do.
