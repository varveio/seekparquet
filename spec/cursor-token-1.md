# SeekParquet cursor token, version 1

**Status: draft.** This is a protocol-layer encoding of the continuation cursors of
[format 1](format-1.md) (PAGE-1) as one line of text, for an API, a URL or a log to carry between pages. It
does not change the format, its draft revision, PAGE-1 or any query result: format-1.md leaves cursor wire
encoding to protocols, and this document is one such encoding, shared by the implementations in this
repository. The conformance adapter protocol keeps its own JSON cursor (`after`). Golden vectors are in
[conformance/vectors/cursor-token-1.json](../conformance/vectors/cursor-token-1.json).

The key words MUST, MUST NOT and MAY are to be interpreted as described in RFC 2119 and RFC 8174 when, and
only when, they appear in capitals.

## Grammar

A token is ASCII text with no whitespace:

```
token        = row-token / prefix-token
row-token    = "spc1" "." "r" "." payload "." offset
prefix-token = "spc1" "." "p" "." payload
payload      = *base64url-char           ; canonical unpadded base64url of the bytes
offset       = "0" / nonzero *9DIGIT     ; up to 19 digits, at most 9223372036854775807
nonzero      = %x31-39
base64url-char = ALPHA / DIGIT / "-" / "_"
```

- `spc1` is the version of the token, independent of the format version and draft revision. A reader MUST
  reject any other version, including other spellings of this one (`SPC1`).
- The tag `r` is a row cursor `(k, n)` and `p` a prefix cursor `c` (PAGE-1). A reader MUST reject any other
  tag. A row token has exactly four segments and a prefix token exactly three, separated by single `.`
  characters; nothing precedes the version or follows the last segment.
- The payload is the cursor's key `k` or prefix `c`: any bytes, encoded in the URL-safe base64 alphabet of
  RFC 4648 §5 without padding. The encoding MUST be canonical: no `=`, no line breaks or other characters
  outside the alphabet, a length that is not 1 modulo 4, and zero bits beyond the last byte, so that
  re-encoding the decoded bytes gives the payload exactly. An empty key or prefix is an empty payload
  (`spc1.p.`), which is distinct from no cursor at all.
- The offset is `n` in canonical decimal: ASCII digits only, no sign, no leading zeros except the single
  digit `0`, from 0 to 2^63 − 1. A prefix token has no offset.

## Meaning

A token denotes exactly the cursor it encodes; its comparison rules are PAGE-1's, unchanged. In particular
an offset beyond the last row of `k`'s group skips that group (2^63 − 1 means after the whole group), and a
nonzero offset on a dataset of unique keys means "after `k`"; neither is an error. A prefix token is accepted
by listings only; on a range it is an invalid request (QUERY-1). A token carries a position, not the query
or the dataset it came from: like the cursor, it is meant for the same query on the same immutable dataset,
and a reader need not detect another use.

## Errors

A token that does not match the grammar, names an unknown version or tag, is not canonical, or has an offset
out of range is a malformed cursor: an invalid request (QUERY-1). It is never a refusal of the data and never
`unsupported_version`, which is about datasets.

An encoder MUST refuse what is not a cursor: neither or both of a key and a prefix, a negative offset, or a
prefix cursor with an offset.

## Length

A reader MAY bound the length of a token before decoding it. Implementations in this repository accept and
produce tokens of up to 1 MiB (1,048,576 bytes), which holds a key or prefix of at least 786,411 bytes, and
refuse a longer token, or a cursor whose token would be longer, as an invalid request.

## Examples

| Cursor | Token |
|---|---|
| row (`a`, 0) | `spc1.r.YQ.0` |
| row (empty key, 7) | `spc1.r..7` |
| row (`00 ff fe 80`, 2^63 − 1) | `spc1.r.AP_-gA.9223372036854775807` |
| prefix `a/` | `spc1.p.YS8` |
| prefix `ff` | `spc1.p._w` |
| empty prefix | `spc1.p.` |
