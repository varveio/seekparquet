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

5. **Classification rulings (#37, #38).** Readers disagree on unknown numeric encoding/codec values,
   ignored enum values, valueless footer metadata, intrinsic corruption in indexed key headers, and leaf
   annotation restrictions. The reference adapter also decodes unprojected values and loses the distinction
   between an unsupported encoding and corrupt data.

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

5. **Classification rulings (#37, #38).** ENC-1 classifies unrecognized numeric encodings/codecs as
   `unsupported_encoding` only when needed, and ignores repetition encoding values without repetition
   levels. RG-6 ignores boundary order values; both fields retain required presence and wire types.
   STAMP-2 ignores valueless non-stamp metadata but counts a valueless listed stamp and reports
   `stamp_mismatch`. CHECK-5 (cross-referenced by RG-7) distinguishes no complete indexed header
   (`invalid_page_index`) from decoded negative sizes/counts or invalid V2 level lengths
   (`invalid_parquet`, taking precedence), then valid-header type/extent/row disagreements
   (`invalid_page_index`). CHECK-1 checks recognized annotations on every leaf at first part touch,
   including nested and unprojected leaves (`invalid_footer`), preserving COL-3 physical fallback for
   unrecognized annotations and parameter alternatives.

   The annotation reference is Apache parquet-format revision
   `ec90dd4661ff1c4d77f06b5b5232c5aedf68ffc5`,
   [LogicalTypes.md](https://github.com/apache/parquet-format/blob/ec90dd4661ff1c4d77f06b5b5232c5aedf68ffc5/LogicalTypes.md)
   and [parquet.thrift](https://github.com/apache/parquet-format/blob/ec90dd4661ff1c4d77f06b5b5232c5aedf68ffc5/src/main/thrift/parquet.thrift).
   Its DECIMAL section limits precision to 9 for INT32, 18 for INT64, and
   `floor(log10(2^(8*n - 1) - 1))` for FIXED_LEN_BYTE_ARRAY(n); BYTE_ARRAY has no carrier ceiling.
   Its TIMESTAMP section explicitly permits INT64 and FIXED_LEN_BYTE_ARRAY(12), with a signed
   little-endian 96-bit count for the latter. This permission is for the logical TIMESTAMP annotation;
   converted TIMESTAMP_MILLIS/MICROS still require INT64. INT96 remains the legacy unannotated physical
   carrier. Unknown time-unit alternatives retain physical fallback; absent or empty units are invalid.

## Compatibility

Items 1–4 change no query result. Manifests with per-part `extensions` become
valid; readers built before this change refuse them (`invalid_manifest`), which is acceptable before the
first release. `go/v0.1.0` is a reader-only release and does not end in-place editing under VER-2. Its
readers reject manifests containing per-part extensions. Producers needing compatibility with that release
must omit per-part extensions. A reader without `LZ4_RAW` no longer conforms. A part with a corrupt leading magic was already
invalid; a reader now must detect it on first touch. No new refusal code. No new draft revision or format
version: draft revision 1 is unreleased (VER-2).

5. **Classification rulings (#37, #38).** Recognized annotations violating physical or parameter
   restrictions are explicitly invalid at first touch, independent of projection. Unknown enum values
   gain a required unsupported classification without being certified valid Parquet; ignored enum values
   and valueless non-stamp metadata cannot cause refusal. Intrinsic header corruption retains its data
   classification even where index checks also fail. Valid supported queries keep their rows. No refusal
   code, draft revision, format version or release status changes (VER-2); this proposal remains draft.

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

5. **Classification rulings (#37, #38).** Part B adds the following cases after the Java fixes, without
   changing shared fixtures or cases in part A. Reader `ok` answers come from unchanged source rows.

   | Case name | Target | Expected answer |
   |---|---|---|
   | `unknown-key-encoding-99-v1` | reader; validator-full | reader unsupported with `unsupported_encoding`; structural complete, profile/full incomplete with the code at profile |
   | `unknown-value-encoding-99-projected` | reader; validator-full | reader unsupported with `unsupported_encoding`; structural/profile complete, full incomplete with the code |
   | `unknown-value-encoding-99-unprojected` | reader | `ok` |
   | `unknown-codec-99-projected` | reader; validator-full | reader unsupported with `unsupported_encoding`; structural/profile complete, full incomplete with the code |
   | `unknown-codec-99-unprojected` | reader | `ok` |
   | `ignored-enums-99` | reader | `ok`; boundary order and non-repeated key/value repetition encodings ignored |
   | `missing-boundary-order` | reader | `refused/invalid_page_index` |
   | `missing-repetition-encoding-value` | reader | `refused/invalid_parquet` |
   | `metadata-no-value` | reader | `ok`; ordinary and unknown seekparquet metadata, including duplicates |
   | `stamp-duplicate-one-valueless` | reader | `refused/stamp_mismatch` at touch |
   | `key-header-negative-num-values-v1` | reader | `refused/invalid_parquet` |
   | `key-index-undecodable-header` | reader | `refused/invalid_page_index` |
   | `annotation-physical-fallback-controls` | reader | `ok`; legal DECIMAL ceilings, TIMESTAMP FLBA(12), unknown logical/converted/time-unit alternatives |
   | `key-index-valid-row-count-mismatch` | reader | `refused/invalid_page_index` |
   | `annotation-decimal-int64-p19-unprojected` | reader; validator-structural | reader refused with `invalid_footer` at touch; structural failed with the code |

   Implementation unit tests cover V2 encodings and counts, header extents and valid row disagreement,
   individual valueless stamps, other annotation restrictions, and unknown key codecs without an
   exhaustive shared mutation matrix.

## Implementation plan

Go and Java parse, keep and expose per-part extensions (Go `PartInfo.Extensions`, through `ParseManifest`
and `MarshalManifest`; Java `ManifestPart.extensions`). Java checks the leading magic in its footer read: free
for a part within its tail window, one 4-byte read for a larger part. The Python validator and reference
adapter follow. Both readers already decode `LZ4_RAW`; Go keeps its `BROTLI` support.

5. **Classification rulings (#37, #38).** Part A updates the spec, Go and both Python tools with local
   regression tests. Go preserves optional metadata values across both footer decodes, narrows header
   remapping, and enforces the pinned DECIMAL/TIMESTAMP rules. Python checks raw metadata before Arrow,
   checks all leaves, classifies indexed headers before walking physical pages, and decodes only needed
   query columns. Part B updates Java and admits the named shared cases together so implementation CI
   keeps passing. Proposal 0001 stays draft until implementations and shared expectations agree.

## Alternatives considered

- A path-keyed map under the top-level `extensions.seekparquet` instead of per-part extensions: compatible
  today, but less direct for every per-part accelerator.
- Requiring `BROTLI`: decoder surface and work without a writer that needs it.
- A footer-length hint, a persisted list index, OPTIONAL key columns and multi-byte delimiters are separate
  decisions and not part of this proposal.
