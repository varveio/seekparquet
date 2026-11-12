// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"encoding/base64"
	"strconv"
	"strings"
)

// MaxCursorToken is the length in bytes of the longest cursor token Encode
// makes and ParseCursor reads (spec/cursor-token-1.md): 1 MiB, a key or
// prefix of at least 786,411 bytes whatever its offset.
const MaxCursorToken = 1 << 20

// The token's version and tags (spec/cursor-token-1.md).
const (
	cursorVersion   = "spc1"
	cursorRowTag    = "r"
	cursorPrefixTag = "p"
)

var cursorBase64 = base64.RawURLEncoding

// Encode returns the cursor as a text token, version 1 of the cursor token
// (spec/cursor-token-1.md): "spc1.r.KEY.OFFSET" for a row cursor and
// "spc1.p.PREFIX" for a prefix cursor, the bytes in unpadded base64url and
// the offset in canonical decimal. An empty key or prefix is an empty
// segment. A cursor no read accepts (neither or both of Key and Prefix, a
// negative Offset, a prefix cursor with an Offset), or one whose token
// would exceed MaxCursorToken bytes, is ErrInvalidRequest. A token carries
// a position only: it applies to the query that produced it, on the same
// dataset, as the Cursor does.
func (c Cursor) Encode() (string, error) {
	if err := c.check(true); err != nil {
		return "", err
	}
	payload := c.Key
	tag := cursorRowTag
	if c.Prefix != nil {
		payload, tag = c.Prefix, cursorPrefixTag
	}
	n := 0
	if len(payload) <= MaxCursorToken {
		n = len(cursorVersion) + 2 + len(tag) + cursorBase64.EncodedLen(len(payload))
		if c.Prefix == nil {
			n += 1 + len(strconv.FormatInt(c.Offset, 10))
		}
	}
	if n == 0 || n > MaxCursorToken {
		return "", invalidRequest("a cursor of %d bytes makes a token beyond %d bytes", len(payload), MaxCursorToken)
	}
	var b strings.Builder
	b.Grow(n)
	b.WriteString(cursorVersion)
	b.WriteByte('.')
	b.WriteString(tag)
	b.WriteByte('.')
	b.WriteString(cursorBase64.EncodeToString(payload))
	if c.Prefix == nil {
		b.WriteByte('.')
		b.WriteString(strconv.FormatInt(c.Offset, 10))
	}
	return b.String(), nil
}

// ParseCursor returns the cursor a token encodes (Cursor.Encode). It
// accepts only the canonical spelling of version 1: the exact version and
// a known tag, exactly the segments of the tag, base64url without padding
// whose bits beyond the bytes are zero, and an offset of decimal digits
// without a sign or leading zeros from 0 to 2^63-1. Anything else, an
// unknown version included, or a token longer than MaxCursorToken bytes, is
// ErrInvalidRequest: a malformed request, never a refusal of the data. An
// empty segment is an empty key or prefix, never no cursor.
func ParseCursor(token string) (*Cursor, error) {
	if len(token) > MaxCursorToken {
		return nil, invalidRequest("a cursor token of %d bytes is beyond %d", len(token), MaxCursorToken)
	}
	version, rest, ok := strings.Cut(token, ".")
	if !ok || version != cursorVersion {
		return nil, invalidRequest("not a version 1 cursor token")
	}
	tag, rest, ok := strings.Cut(rest, ".")
	if !ok {
		return nil, invalidRequest("a cursor token has no tag")
	}
	switch tag {
	case cursorRowTag:
		payload, offset, ok := strings.Cut(rest, ".")
		if !ok {
			return nil, invalidRequest("a row cursor token has no offset")
		}
		key, err := tokenBytes(payload)
		if err != nil {
			return nil, err
		}
		n, err := tokenOffset(offset)
		if err != nil {
			return nil, err
		}
		return &Cursor{Key: key, Offset: n}, nil
	case cursorPrefixTag:
		prefix, err := tokenBytes(rest)
		if err != nil {
			return nil, err
		}
		return &Cursor{Prefix: prefix}, nil
	}
	return nil, invalidRequest("a cursor token's tag %.8q is unknown", tag)
}

// tokenBytes decodes canonical unpadded base64url, non-nil when empty.
func tokenBytes(s string) ([]byte, error) {
	if strings.ContainsAny(s, "=\r\n.") {
		return nil, invalidRequest("a cursor token's bytes are not unpadded base64url")
	}
	b, err := cursorBase64.Strict().DecodeString(s)
	if err != nil || cursorBase64.EncodeToString(b) != s {
		return nil, invalidRequest("a cursor token's bytes are not canonical base64url")
	}
	if b == nil {
		b = []byte{}
	}
	return b, nil
}

// tokenOffset decodes a canonical decimal from 0 to 2^63-1.
func tokenOffset(s string) (int64, error) {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, invalidRequest("a cursor token's offset is not decimal digits")
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != s {
		return 0, invalidRequest("a cursor token's offset is not a canonical decimal from 0 to 2^63-1")
	}
	return n, nil
}
