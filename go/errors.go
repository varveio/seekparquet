// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

// Code is a refusal code from the specification's refusal-codes.json. Codes
// are stable; the text of an error is not.
type Code string

// The refusal codes of format 1, draft revision 1, that a reader reports.
// (digest_mismatch belongs to validation, which this package does not do.)
const (
	CodeNoManifest            Code = "no_manifest"
	CodeInvalidManifest       Code = "invalid_manifest"
	CodeUnsupportedVersion    Code = "unsupported_version"
	CodeMissingPart           Code = "missing_part"
	CodePartMismatch          Code = "part_mismatch"
	CodeInvalidFooter         Code = "invalid_footer"
	CodeStampMismatch         Code = "stamp_mismatch"
	CodeInvalidKeyColumn      Code = "invalid_key_column"
	CodeInconsistentColumn    Code = "inconsistent_column"
	CodeMissingSortingColumns Code = "missing_sorting_columns"
	CodeMissingPageIndex      Code = "missing_page_index"
	CodeInvalidPageIndex      Code = "invalid_page_index"
	CodeInvalidKey            Code = "invalid_key"
	CodeKeyOrder              Code = "key_order"
	CodeInvalidParquet        Code = "invalid_parquet"
	CodeBoundsMismatch        Code = "bounds_mismatch"
)

// RefusalError reports data that breaks the specification: a dataset
// refused on open, or a read ended because a page it decoded is invalid.
// Code is the contract; the location fields are facts for diagnosis, and
// Detail is for people.
type RefusalError struct {
	Code Code
	// Part is the part's path or name, "" when the fault is not one
	// part's (a manifest fault).
	Part string
	// RowGroup is the physical row group in Part, -1 when not one row
	// group's.
	RowGroup int
	// Row is the 0-based row within RowGroup, -1 when not one row's.
	Row int64
	// Key is the offending key (for key_order, the key at or below its
	// predecessor), nil when none.
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
func refuse(code Code, format string, args ...any) *RefusalError {
	return &RefusalError{Code: code, RowGroup: -1, Row: -1, Detail: fmt.Sprintf(format, args...)}
}

// refusePart returns a *RefusalError located at a part and, when group is
// not -1, a row group of it.
func refusePart(code Code, part string, group int, format string, args ...any) *RefusalError {
	return &RefusalError{Code: code, Part: part, RowGroup: group, Row: -1, Detail: fmt.Sprintf(format, args...)}
}

// keyOrder returns the key_order refusal of a row whose key is at or below
// the key decoded before it.
func keyOrder(part string, group int, row int64, key []byte) *RefusalError {
	return &RefusalError{Code: CodeKeyOrder, Part: part, RowGroup: group, Row: row, Key: bytes.Clone(key),
		Detail: "key is not above the key before it"}
}

// CodeOf returns err's refusal code, or "" when err is not a refusal.
func CodeOf(err error) Code {
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

// Unsupported codes (spec/refusal-codes.json, non_refusals).
const (
	// UnsupportedProjection: a request projects a nested or repeated
	// column (COL-3).
	UnsupportedProjection = "unsupported_projection"
	// UnsupportedEncoding: a file uses an encoding or codec outside what
	// this reader decodes (ENC-1).
	UnsupportedEncoding = "unsupported_encoding"
	// UnsupportedEncryption: a part uses Parquet modular encryption
	// (FILE-2).
	UnsupportedEncryption = "unsupported_encryption"
)

// UnsupportedError reports a valid dataset or request that uses a feature
// this reader does not support. It is not a refusal of the data.
type UnsupportedError struct {
	// Code is one of the Unsupported constants.
	Code   string
	Detail string
}

func (e *UnsupportedError) Error() string {
	return "seekparquet: unsupported (" + e.Code + "): " + e.Detail
}

// ExpectationError reports a part that does not meet a ColumnExpectation
// the caller gave. It is the caller's policy failing, not a refusal of the
// data: the dataset may be perfectly valid.
type ExpectationError struct {
	Part   string
	Column string
	Detail string
}

func (e *ExpectationError) Error() string {
	return fmt.Sprintf("seekparquet: part %s: column %s: %s", e.Part, e.Column, e.Detail)
}

// ErrClosed reports a read of a closed Reader or Dataset.
var ErrClosed = errors.New("seekparquet: closed")

// ErrLimitExceeded reports input beyond one of the Limits (§11): a policy
// of the caller, not a refusal of the data.
var ErrLimitExceeded = errors.New("seekparquet: limit exceeded")
