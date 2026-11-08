// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package manifest parses and validates a SeekParquet manifest.json
// (format 1, §3): strictly, so that a manifest that breaks any rule of the
// specification is refused with the refusal code the specification gives.
package manifest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Format is the manifest's required "format" value.
const Format = "seekparquet"

// Refusal codes this package reports (spec/refusal-codes.json).
const (
	CodeInvalidManifest    = "invalid_manifest"
	CodeUnsupportedVersion = "unsupported_version"
	CodeBoundsMismatch     = "bounds_mismatch"
)

// Version is a (format_version, draft_revision) pair. DraftRevision is 0
// when the manifest has none (format_version 1 onwards).
type Version struct {
	FormatVersion int64
	DraftRevision int64
}

// Supported lists the version pairs this implementation reads.
var Supported = []Version{{FormatVersion: 0, DraftRevision: 1}}

// Error is a manifest refusal: Code is one of the Code constants.
type Error struct {
	Code   string
	Detail string
}

func (e *Error) Error() string { return e.Code + ": " + e.Detail }

func refuse(code, format string, args ...any) error {
	return &Error{Code: code, Detail: fmt.Sprintf(format, args...)}
}

func invalid(format string, args ...any) error {
	return refuse(CodeInvalidManifest, format, args...)
}

// Manifest is a parsed, validated manifest.
type Manifest struct {
	Version  Version
	SchemaID string
	Parts    []Part
	// Extensions is the extensions object exactly as written, nil when the
	// manifest has none.
	Extensions json.RawMessage
}

// Part is one part entry.
type Part struct {
	Path   string
	Rows   int64
	Bytes  int64
	MinKey []byte
	MaxKey []byte
	// SHA256 is the lowercase hex digest, "" when absent.
	SHA256 string
}

// Parse parses and validates raw. It checks, in this order: that raw is
// one UTF-8 JSON object without a byte order mark and without duplicate
// member names in any object (MAN-1, MAN-2); the format name (MAN-4); the
// version pair (MAN-4, VER-1), before any other member, so that a manifest
// of a later revision is refused as unsupported rather than invalid; then
// every member (MAN-3, PART-1 to PART-3); and last the order of the bounds
// (PART-4, bounds_mismatch).
func Parse(raw []byte) (*Manifest, error) {
	if bytes.HasPrefix(raw, []byte("\xef\xbb\xbf")) {
		return nil, invalid("the manifest begins with a byte order mark")
	}
	if !utf8.Valid(raw) {
		return nil, invalid("the manifest is not valid UTF-8")
	}
	if err := checkSyntax(raw); err != nil {
		return nil, invalid("%v", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, invalid("the manifest is not a JSON object")
	}
	if top == nil {
		return nil, invalid("the manifest is not a JSON object")
	}

	var format string
	if err := member(top, "format", &format, true); err != nil {
		return nil, err
	}
	if format != Format {
		return nil, invalid("format is %q, not %q", format, Format)
	}
	version, err := parseVersion(top)
	if err != nil {
		return nil, err
	}

	for name := range top {
		switch name {
		case "format", "format_version", "draft_revision", "schema_id", "parts", "extensions":
		default:
			return nil, invalid("undefined member %q", name)
		}
	}
	m := &Manifest{Version: version}
	if err := member(top, "schema_id", &m.SchemaID, true); err != nil {
		return nil, err
	}
	if m.SchemaID == "" {
		return nil, invalid("schema_id is empty")
	}
	if ext, ok := top["extensions"]; ok {
		if err := checkExtensions(ext); err != nil {
			return nil, err
		}
		m.Extensions = bytes.Clone(ext)
	}
	rawParts, ok := top["parts"]
	if !ok {
		return nil, invalid("parts is missing")
	}
	var parts []json.RawMessage
	if err := json.Unmarshal(rawParts, &parts); err != nil || parts == nil {
		return nil, invalid("parts is not an array")
	}
	m.Parts = make([]Part, 0, len(parts))
	paths := make(map[string]int, len(parts))
	for i, rawPart := range parts {
		part, err := parsePart(rawPart)
		if err != nil {
			return nil, prefix(err, "part %d: ", i)
		}
		if j, dup := paths[part.Path]; dup {
			return nil, invalid("parts %d and %d have the same path %q", j, i, part.Path)
		}
		paths[part.Path] = i
		m.Parts = append(m.Parts, part)
	}
	for i, part := range m.Parts {
		if bytes.Compare(part.MinKey, part.MaxKey) > 0 {
			return nil, refuse(CodeBoundsMismatch, "part %d (%s): min_key is above max_key", i, part.Path)
		}
		if i > 0 && bytes.Compare(m.Parts[i-1].MaxKey, part.MinKey) >= 0 {
			return nil, refuse(CodeBoundsMismatch, "part %d (%s) does not start after part %d ends", i, part.Path, i-1)
		}
	}
	return m, nil
}

func prefix(err error, format string, args ...any) error {
	var e *Error
	if errors.As(err, &e) {
		return &Error{Code: e.Code, Detail: fmt.Sprintf(format, args...) + e.Detail}
	}
	return err
}

// parseVersion reads format_version and draft_revision and requires a
// supported pair. Well-typed but unknown pairs are unsupported_version;
// ill-typed members are invalid_manifest.
func parseVersion(top map[string]json.RawMessage) (Version, error) {
	rawVersion, ok := top["format_version"]
	if !ok {
		return Version{}, invalid("format_version is missing")
	}
	formatVersion, err := integer(rawVersion)
	if err != nil || formatVersion < 0 {
		return Version{}, invalid("format_version is not a non-negative integer")
	}
	v := Version{FormatVersion: formatVersion}
	if rawDraft, ok := top["draft_revision"]; ok {
		draft, err := integer(rawDraft)
		if err != nil || draft < 1 {
			return Version{}, invalid("draft_revision is not an integer of at least 1")
		}
		v.DraftRevision = draft
	}
	for _, s := range Supported {
		if s == v {
			return v, nil
		}
	}
	if v.FormatVersion == 0 && v.DraftRevision == 0 {
		return Version{}, invalid("format_version 0 requires a draft_revision")
	}
	return Version{}, refuse(CodeUnsupportedVersion, "format_version %d, draft_revision %d is not supported", v.FormatVersion, v.DraftRevision)
}

func parsePart(raw json.RawMessage) (Part, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return Part{}, invalid("not an object")
	}
	for name := range fields {
		switch name {
		case "path", "rows", "bytes", "min_key_base64", "max_key_base64", "sha256":
		default:
			return Part{}, invalid("undefined member %q", name)
		}
	}
	var p Part
	if err := member(fields, "path", &p.Path, true); err != nil {
		return Part{}, err
	}
	if err := CheckPath(p.Path); err != nil {
		return Part{}, invalid("path %q: %v", p.Path, err)
	}
	for _, f := range []struct {
		name string
		into *int64
	}{{"rows", &p.Rows}, {"bytes", &p.Bytes}} {
		rawN, ok := fields[f.name]
		if !ok {
			return Part{}, invalid("%s is missing", f.name)
		}
		n, err := integer(rawN)
		if err != nil || n < 1 {
			return Part{}, invalid("%s is not an integer of at least 1", f.name)
		}
		*f.into = n
	}
	for _, f := range []struct {
		name string
		into *[]byte
	}{{"min_key_base64", &p.MinKey}, {"max_key_base64", &p.MaxKey}} {
		var s string
		if err := member(fields, f.name, &s, true); err != nil {
			return Part{}, err
		}
		key, err := DecodeKey(s)
		if err != nil {
			return Part{}, invalid("%s: %v", f.name, err)
		}
		*f.into = key
	}
	if _, ok := fields["sha256"]; ok {
		if err := member(fields, "sha256", &p.SHA256, true); err != nil {
			return Part{}, err
		}
		if !isLowerHex(p.SHA256, 64) {
			return Part{}, invalid("sha256 is not 64 lowercase hexadecimal digits")
		}
	}
	return p, nil
}

// member decodes the string member name of fields into s.
func member(fields map[string]json.RawMessage, name string, s *string, required bool) error {
	raw, ok := fields[name]
	if !ok {
		if required {
			return invalid("%s is missing", name)
		}
		return nil
	}
	if len(raw) == 0 || raw[0] != '"' {
		return invalid("%s is not a string", name)
	}
	if err := json.Unmarshal(raw, s); err != nil {
		return invalid("%s is not a string", name)
	}
	return nil
}

// integer decodes a JSON number that is an integer: JSON Schema's integer
// type, which admits any number whose fractional part is zero (1.0, 1e2).
func integer(raw json.RawMessage) (int64, error) {
	text := strings.TrimSpace(string(raw))
	if text == "" || !(text[0] == '-' || text[0] >= '0' && text[0] <= '9') {
		return 0, errors.New("not a number")
	}
	if i := strings.IndexAny(text, "eE"); i >= 0 {
		// Bound the exponent before big.Rat expands it: no integer that
		// fits in 64 bits needs one beyond a few hundred.
		exp, err := strconv.Atoi(strings.TrimPrefix(text[i+1:], "+"))
		if err != nil || exp > 400 || exp < -400 {
			if strings.Trim(text[:i], "-0.") == "" {
				return 0, nil // zero, whatever its exponent
			}
			return 0, errors.New("not a 64-bit integer")
		}
	}
	r, ok := new(big.Rat).SetString(text)
	if !ok || !r.IsInt() || !r.Num().IsInt64() {
		return 0, errors.New("not an integer")
	}
	return r.Num().Int64(), nil
}

// DecodeKey decodes a non-empty key in standard base64 with padding
// (RFC 4648 §4), refusing any other spelling: unpadded, URL-safe, with
// whitespace or line breaks, or with non-zero trailing bits.
func DecodeKey(s string) ([]byte, error) {
	if s == "" {
		return nil, errors.New("a key is not empty")
	}
	if strings.ContainsAny(s, "\r\n") {
		return nil, errors.New("not standard base64")
	}
	key, err := base64.StdEncoding.Strict().DecodeString(s)
	if err != nil {
		return nil, errors.New("not standard base64")
	}
	if len(key) == 0 {
		return nil, errors.New("a key is not empty")
	}
	return key, nil
}

// CheckPath applies PART-1 to a part path: segments separated by "/", each
// non-empty, neither "." nor "..", without "\" or NUL, and no leading "/".
func CheckPath(path string) error {
	if path == "" {
		return errors.New("empty")
	}
	if strings.HasPrefix(path, "/") {
		return errors.New("begins with /")
	}
	if strings.ContainsAny(path, "\\\x00") {
		return errors.New(`contains \ or NUL`)
	}
	for seg := range strings.SplitSeq(path, "/") {
		switch seg {
		case "":
			return errors.New("has an empty segment")
		case ".", "..":
			return fmt.Errorf("has a %q segment", seg)
		}
	}
	return nil
}

func checkExtensions(raw json.RawMessage) error {
	var ext map[string]json.RawMessage
	if err := json.Unmarshal(raw, &ext); err != nil || ext == nil {
		return invalid("extensions is not an object")
	}
	for name := range ext {
		if name == "" {
			return invalid("extensions has an empty namespace name")
		}
	}
	return nil
}

func isLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !(s[i] >= '0' && s[i] <= '9' || s[i] >= 'a' && s[i] <= 'f') {
			return false
		}
	}
	return true
}

// checkSyntax requires raw to be exactly one JSON value with no object
// holding the same member name twice, at any depth (MAN-2).
func checkSyntax(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := walk(dec); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("data after the manifest object")
	}
	return nil
}

func walk(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("not valid JSON: %w", err)
	}
	switch tok {
	case json.Delim('{'):
		seen := map[string]bool{}
		for dec.More() {
			name, err := dec.Token()
			if err != nil {
				return fmt.Errorf("not valid JSON: %w", err)
			}
			key, ok := name.(string)
			if !ok {
				return errors.New("not valid JSON")
			}
			if seen[key] {
				return fmt.Errorf("duplicate member name %q", key)
			}
			seen[key] = true
			if err := walk(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("not valid JSON: %w", err)
		}
	case json.Delim('['):
		for dec.More() {
			if err := walk(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("not valid JSON: %w", err)
		}
	}
	return nil
}
