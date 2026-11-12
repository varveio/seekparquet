// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

// RefusalCode is a refusal code from the specification's
// refusal-codes.json. Codes are stable; the text of an error is not.
type RefusalCode string

// The refusal codes of format 1, draft revision 1. digest_mismatch is
// reported only by validation at the digest level (Validate).
const (
	CodeNoManifest            RefusalCode = "no_manifest"
	CodeInvalidManifest       RefusalCode = "invalid_manifest"
	CodeUnsupportedVersion    RefusalCode = "unsupported_version"
	CodeMissingPart           RefusalCode = "missing_part"
	CodePartMismatch          RefusalCode = "part_mismatch"
	CodeInvalidFooter         RefusalCode = "invalid_footer"
	CodeStampMismatch         RefusalCode = "stamp_mismatch"
	CodeInvalidKeyColumn      RefusalCode = "invalid_key_column"
	CodeInconsistentColumn    RefusalCode = "inconsistent_column"
	CodeMissingSortingColumns RefusalCode = "missing_sorting_columns"
	CodeMissingPageIndex      RefusalCode = "missing_page_index"
	CodeInvalidPageIndex      RefusalCode = "invalid_page_index"
	CodeInvalidKey            RefusalCode = "invalid_key"
	CodeKeyOrder              RefusalCode = "key_order"
	CodeInvalidParquet        RefusalCode = "invalid_parquet"
	CodeBoundsMismatch        RefusalCode = "bounds_mismatch"
	CodeDigestMismatch        RefusalCode = "digest_mismatch"
)

// RefusalError reports data that breaks the specification: a dataset
// refused on open, or a read ended because a page it decoded is invalid.
// Code is the contract; the location fields are facts for diagnosis, and
// Detail is for people.
type RefusalError struct {
	Code RefusalCode
	// Part is the part's path or name, "" when the fault is not one
	// part's (a manifest fault).
	Part string
	// RowGroup is the physical row group in Part, -1 when not one row
	// group's.
	RowGroup int
	// Row is the 0-based row within RowGroup, -1 when not one row's.
	Row int64
	// Key is the offending key (for key_order, the key that is below the
	// key before it, or equal to it when keys are unique), nil when none.
	Key    []byte
	Detail string
}

func (e *RefusalError) Error() string {
	var b bytes.Buffer
	b.WriteString("seekparquet: refused (")
	b.WriteString(string(e.Code))
	b.WriteString(")")
	if e.Part != "" {
		b.WriteString(": part ")
		b.WriteString(e.Part)
		if e.RowGroup >= 0 {
			b.WriteString(" row group ")
			b.WriteString(strconv.Itoa(e.RowGroup))
		}
		if e.Row >= 0 {
			b.WriteString(" row ")
			b.WriteString(strconv.FormatInt(e.Row, 10))
		}
	}
	if e.Detail != "" {
		b.WriteString(": ")
		b.WriteString(e.Detail)
	}
	return b.String()
}

// refuse returns a *RefusalError of no particular part.
func refuse(code RefusalCode, format string, args ...any) *RefusalError {
	return &RefusalError{Code: code, RowGroup: -1, Row: -1, Detail: fmt.Sprintf(format, args...)}
}

// refusePart returns a *RefusalError located at a part and, when group is
// not -1, a row group of it.
func refusePart(code RefusalCode, part string, group int, format string, args ...any) *RefusalError {
	return &RefusalError{Code: code, Part: part, RowGroup: group, Row: -1, Detail: fmt.Sprintf(format, args...)}
}

// keyOrder returns the key_order refusal of a row whose key breaks the
// declared key order with the key decoded before it.
func keyOrder(part string, group int, row int64, key []byte) *RefusalError {
	return &RefusalError{Code: CodeKeyOrder, Part: part, RowGroup: group, Row: row, Key: bytes.Clone(key),
		Detail: "the key breaks the declared key order with the key before it"}
}

// RefusalCodeOf returns err's refusal code, or "" when err is not a refusal.
func RefusalCodeOf(err error) RefusalCode {
	var refusal *RefusalError
	if errors.As(err, &refusal) {
		return refusal.Code
	}
	return ""
}

// ErrInvalidRequest reports a malformed request, such as a limit that is
// not positive (QUERY-1). It is not a refusal of the data.
var ErrInvalidRequest = errors.New("seekparquet: invalid request")

func invalidRequest(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidRequest}, args...)...)
}

// UnsupportedCode is the code of an UnsupportedError, from the
// specification's refusal-codes.json (non_refusals). Codes are stable.
type UnsupportedCode string

// The unsupported codes.
const (
	// UnsupportedProjection: a request projects a nested or repeated
	// column (COL-3).
	UnsupportedProjection UnsupportedCode = "unsupported_projection"
	// UnsupportedEncoding: a file uses an encoding or codec outside what
	// this reader decodes (ENC-1).
	UnsupportedEncoding UnsupportedCode = "unsupported_encoding"
	// UnsupportedEncryption: a part uses Parquet modular encryption
	// (FILE-2).
	UnsupportedEncryption UnsupportedCode = "unsupported_encryption"
)

// UnsupportedError reports a valid dataset or request that uses a feature
// this reader does not support. It is not a refusal of the data.
type UnsupportedError struct {
	Code   UnsupportedCode
	Detail string
}

func (e *UnsupportedError) Error() string {
	return "seekparquet: unsupported (" + string(e.Code) + "): " + e.Detail
}

// ErrClosed reports a read of a closed Dataset.
var ErrClosed = errors.New("seekparquet: closed")

// ErrLimitExceeded reports input beyond one of the Limits (§11), or a
// dataset row total above the reader's signed 64-bit ordinal capacity.
// It is an implementation or caller limit, not a refusal of the data.
var ErrLimitExceeded = errors.New("seekparquet: limit exceeded")

// detach returns a copy of a refusal or unsupported error that shares
// nothing with err, so that a caller changing the error it was handed
// cannot change the copy a part keeps for later reads.
func detach(err error) error {
	switch e := err.(type) {
	case *RefusalError:
		c := *e
		c.Key = bytes.Clone(e.Key)
		return &c
	case *UnsupportedError:
		c := *e
		return &c
	}
	return err
}
