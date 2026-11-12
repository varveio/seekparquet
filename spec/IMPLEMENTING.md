# Implementing SeekParquet

This guide is for someone writing a SeekParquet reader, validator or writer in a new language. The
[specification](format-1.md) is the only normative text; this guide orders it, lists what readers most often
get wrong, and explains how to qualify. Terms are in [concepts](../docs/concepts.md). Please open an issue
before you start, so we can agree on layout and conformance.

## Reading order

1. [§1 Conventions](format-1.md#1-conventions): unsigned byte order and the successor of a prefix. Every
   comparison in the specification uses them.
2. [§2 Dataset layout](format-1.md#2-dataset-layout) and [§3 The manifest](format-1.md#3-the-manifest).
3. [§5 Part files](format-1.md#5-part-files): the key column, row groups and the page index, other columns,
   footer stamps.
4. [§6 Queries](format-1.md#6-queries): what each query returns, and pagination. This is what the
   conformance suite tests most.
5. [§7 What readers check, and when](format-1.md#7-what-readers-check-and-when).
6. [§10 Refusal codes](format-1.md#10-refusal-codes) with [refusal-codes.json](refusal-codes.json), and
   [§11 Security considerations](format-1.md#11-security-considerations).
7. [§8 Validation](format-1.md#8-validation) only if you are writing a validator, and
   [§4 Commit and immutability](format-1.md#4-commit-and-immutability) if you are writing a producer.

## What to check, and when

A reader checks what it reads and keeps the outcome of a part's checks for as long as the dataset is open.
Each check may happen earlier than listed; none may happen later than its stage in
[refusal-codes.json](refusal-codes.json).

| Stage | Check | Requirements |
|---|---|---|
| Open | the manifest: JSON, members, types, base64, part paths, bounds ascending and disjoint, supported version pair | [CHECK-1](format-1.md#check-1), §3, [VER-1](format-1.md#ver-1) |
| First touch of a part | exists; exact size; length and magic; readable, self-contained footer; row-group row counts sum to the footer's, which equals the manifest's | [CHECK-1](format-1.md#check-1), [FILE-1](format-1.md#file-1), [FILE-2](format-1.md#file-2) |
| | stamps agree with the manifest | [STAMP-1](format-1.md#stamp-1), [STAMP-2](format-1.md#stamp-2) |
| | the key column is a top-level `REQUIRED BYTE_ARRAY`, correctly annotated, with `TYPE_ORDER` | [KEY-1](format-1.md#key-1), [KEY-5](format-1.md#key-5) |
| | columns agree with the parts already checked | [COL-5](format-1.md#col-5) |
| | every non-empty row group declares the key ascending first in `sorting_columns` | [RG-1](format-1.md#rg-1) |
| Each row group read | the key column has both indexes, consistent with each other and the pages | [CHECK-2](format-1.md#check-2), [RG-2](format-1.md#rg-2) to [RG-4](format-1.md#rg-4) |
| Each key page decoded | exactly the rows its OffsetIndex entry gives it; non-empty keys, valid UTF-8 where annotated; ascending within the page and within its bounds; in order with every other page decoded in the request | [CHECK-2](format-1.md#check-2), [KEY-2](format-1.md#key-2), [KEY-3](format-1.md#key-3), [KEY-6](format-1.md#key-6), [RG-5](format-1.md#rg-5) |
| Any page decoded | decompresses and decodes, with the declared counts | [CHECK-5](format-1.md#check-5) |

Bound everything a file declares before you allocate for it ([§11](format-1.md#11-security-considerations)).
A file that exceeds your bounds is a limit of your reader, reported as a limit, not a refusal
([CHECK-5](format-1.md#check-5)). Both readers in this repository also bound manifest nesting at 512 JSON
objects and arrays, the manifest object counting 1; the suite has no case for it.

## Mistakes the suite catches

The harness's self-tests run deliberately broken adapters, each with one bug, and check that the suite fails
them (`conformance/harness/test_run.py`). These are the bugs a reader is most likely to have:

| Bug | What the specification says |
|---|---|
| comparing keys as signed bytes, or as strings | unsigned byte order ([§1](format-1.md#1-conventions)) |
| treating the end bound as inclusive | the end is exclusive ([RANGE-1](format-1.md#range-1)) |
| treating shortened page bounds as exact, and skipping a page that holds the key | bounds may be shortened and overlap; a bound's length says nothing ([RG-5](format-1.md#rg-5), [RG-6](format-1.md#rg-6)) |
| reading a prefix cursor as a byte floor, so keys under the prefix come back | a prefix cursor resumes at the prefix's successor ([PAGE-1](format-1.md#page-1), [LIST-6](format-1.md#list-6)) |
| seeking from the last row group whose first key equals the key, losing the start of its key group | start at the row group before the first first key at or above it ([ROUTE-1](format-1.md#route-1)) |
| restarting row offsets at each row group | an offset counts from the start of the whole key group ([PAGE-1](format-1.md#page-1)) |
| ignoring a row cursor's offset | resume inside the key group after the offset ([PAGE-1](format-1.md#page-1)) |
| a common prefix without its exact row, or with the key group's first row instead of the first candidate row | [LIST-8](format-1.md#list-8) |
| reporting an absent column as null | absent and null are distinct ([COL-2](format-1.md#col-2)) |
| completing a validation level whose prerequisite did not complete | [VAL-1](format-1.md#val-1) |
| an unsupported part passing verification, or an incomplete level without its code or a reason | [VAL-1](format-1.md#val-1), [FILE-2](format-1.md#file-2) |

## Qualify with the conformance suite

1. Write an adapter: an executable that answers JSON requests on stdin, one per line
   ([protocol](../conformance/protocol.md)).
2. Bring it up on the 30-case smoke subset: `python3 conformance/harness/run.py --adapter "..." --subset smoke`.
3. Develop against the reviewed core cases: `--subset core`.
4. Qualify on a full run with no filter. Smoke, core and filtered runs never qualify.

The mandatory target is `reader`. A validator claims `validator-structural`, `validator-profile`,
`validator-full` and `validator-digest`. Details, including how refusal stages are judged, are in the
[conformance README](../conformance/README.md).

Passing the suite shows your results are right. It does not yet show indexed access: a reader that scans
everything also passes. The freeze of format 1 needs two independent readers that also pass an
indexed-access suite ([GOVERNANCE.md](../GOVERNANCE.md)).

## What the specification leaves to you

None of these change a result, so the specification says nothing about them:

- storage access, request batching, caching and retries;
- the order and concurrency of reads, and speculative reads;
- planning, splitting and streaming APIs (a streaming read without a result limit must return what the range
  read without a limit would, and keeps every check);
- accelerators such as a list index, built in memory or carried in `extensions` under the rules of
  [§3.4](format-1.md#34-extensions);
- cursor encoding for your protocol, though [cursor token, version 1](cursor-token-1.md) lets clients move
  between this repository's readers and yours.
