# 0001: Pre-release compatibility pass

- **Status:** draft
- **Author(s):** SeekParquet maintainers
- **Affects:** valid files / refusal codes (detection point) / editorial

Draft revision 1 is unreleased and edited in place (VER-2), so this change keeps `format_version` 0,
`draft_revision` 1. It collects changes that are cheap before the first release and breaking after it.

## Problem

1. The manifest is closed (MAN-3): every object refuses undefined members, so optional metadata about a part,
   such as a future footer-length hint, cannot be added without a new revision. Only the top-level
   `extensions` object is open, and the rules for metadata a reader may ignore are not written down.
2. ENC-1 leaves open whether `LZ4_RAW` and `BROTLI` join the codec baseline. Both readers decode `LZ4_RAW`;
   the suite still lets a reader answer it as unsupported. Promoting a codec after release would leave
   conforming readers behind.
3. CHECK-1 does not say that both Parquet magics are checked. The Go reader checks both; the Java reader
   checks only the trailing one, so a part whose leading magic alone is corrupt is refused by one reader and
   read by the other.
4. `spec/README.md` says a validity change increments the draft revision, without the unreleased exception
   that VER-2 and the changelog state, and nothing says what ends in-place editing. The storage identity
   principle and the difference between a reader's resource limit and invalid data are not reader
   requirements. Streaming reads without a limit are not placed relative to QUERY-1.

## Proposal

1. §3.3 and §3.4: a part MAY have `extensions`, with the top-level object's schema (`$defs/extensions`).
   EXT-1 says extension metadata never changes validity, query results, required checks or core members;
   EXT-3 says readers ignore unknown namespaces, members and accelerator versions, and that unusable
   accelerator metadata disables the accelerator without refusing valid data; EXT-4 keeps the JSON rules of
   §3.1 inside extensions; EXT-5 reserves the namespace `seekparquet` for accelerators the specification
   documents (none yet) and keeps correctness-changing features out of extensions. MAN-3 and VER-2 refer to
   them. The core manifest stays closed.
2. ENC-1: the required codecs are `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `ZSTD` and `LZ4_RAW`, which is distinct from
   the deprecated `LZ4`. `BROTLI` is valid and outside the baseline; a reader MAY answer it
   `unsupported_encoding`, never with a refusal. An unsupported codec in a column a request does not need
   must not prevent the request. Validation levels that need an unavailable codec stay incomplete (VAL-1).
3. CHECK-1: when a reader first touches a part it checks its length and both magics, in one order: shorter
   than 12 bytes is `invalid_footer`; otherwise `PARE` at either end is `unsupported_encryption` (FILE-2,
   now a MUST, as is footer-declared encryption); otherwise anything but `PAR1` at either end is
   `invalid_footer`.
4. VER-2 and the README: in-place editing ends at the first release of draft revision 1, the first
   specification tag or the first implementation release that writes files of this draft for others to read.
   CHECK-6: a reader never combines bytes of two versions of a part; identity is the storage layer's to
   establish, a reader may rely on its source's guarantee, and a reported change of an object used only by
   an accelerator disables the accelerator (EXT-3). CHECK-5 and §11: a reader's resource limit is not a refusal. §6.1: streaming operations without
   a limit are outside QUERY-1, follow the range semantics and keep every check.

## Compatibility

No valid file becomes invalid and no query result changes. Manifests with per-part `extensions` become
valid; readers built before this change refuse them (`invalid_manifest`), which is acceptable before the
first release. `go/v0.1.0` is a reader-only release and does not end in-place editing under VER-2. Its
readers reject manifests containing per-part extensions. Producers needing compatibility with that release
must omit per-part extensions. A reader without `LZ4_RAW` no longer conforms. A part with a corrupt leading magic was already
invalid; a reader now must detect it on first touch. No new refusal code. No new draft revision or format
version: draft revision 1 is unreleased (VER-2).

## Conformance

- Valid datasets `part-extensions` (multipart; per-part extensions with an unknown namespace, nested values,
  and the reserved namespace holding an array, an object with unknown members and a string; the reserved
  namespace at the top level too), `part-extensions-empty` and `part-extensions-null` (basic; a number and
  null under the reserved namespace, an empty per-part envelope), answering exactly as without them.
- Refusals at open: a per-part `extensions` that is an array or null, an empty namespace name, duplicate and
  escaped-duplicate namespace names, a nested duplicate member, a nested unpaired surrogate, and an undefined
  part member beside a valid envelope.
- Valid dataset `lz4-raw` (every column, the key included, `LZ4_RAW`; data page v1 and v2 parts); the
  `optional-codecs` `LZ4_RAW` column must now be answered; queries not projecting the `BROTLI` column must
  succeed; `BROTLI` keeps its dual outcome. Invalid fixtures: an `LZ4_RAW` page that does not decompress, and
  one whose declared uncompressed size is smaller than its output.
- Valid dataset `brotli-key` (a BROTLI key): structural validation completes; requests, profile and full may
  be unsupported or incomplete.
- The full leading/trailing magic matrix over `PAR1`, `PARE` and `PAR0`, parts of 11 and 12 bytes, and a
  footer declaring encryption. Unsupported outcomes follow the refusal lifecycle (open, first read, verify)
  and their validation levels are incomplete with the unsupported code.
- The suite revision moves to `0.1.0-draft.5`.

## Implementation plan

Go and Java parse, keep and expose per-part extensions (Go `PartInfo.Extensions`, through `ParseManifest`
and `MarshalManifest`; Java `ManifestPart.extensions`). Java checks the leading magic in its footer read: free
for a part within its tail window, one 4-byte read for a larger part. The Python validator and reference
adapter follow. Both readers already decode `LZ4_RAW`; Go keeps its `BROTLI` support.

## Alternatives considered

- A path-keyed map under the top-level `extensions.seekparquet` instead of per-part extensions: compatible
  today, but less direct for every per-part accelerator.
- Requiring `BROTLI`: decoder surface and work without a writer that needs it.
- A footer-length hint, a persisted list index, OPTIONAL key columns and multi-byte delimiters are separate
  decisions and not part of this proposal.
