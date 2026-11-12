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
)

// Version is a (format_version, draft_revision) pair. DraftRevision is 0
// when the manifest has none (format_version 1 onwards).
type Version struct {
	FormatVersion int64
	DraftRevision int64
}

// Supported lists the version pairs this implementation reads.
var Supported = []Version{{FormatVersion: 0, DraftRevision: 1}}

// ErrTooManyParts reports a manifest listing more parts than its reader
// allows: a limit of the reader, not a fault of the manifest.
var ErrTooManyParts = errors.New("too many parts")

// MaxDepth is the deepest nesting of objects and arrays a manifest may
// have, the manifest object itself at depth 1, as in the Java reader: a
// limit of the reader (CHECK-5), not a fault of the manifest.
const MaxDepth = 512

// ErrTooDeep reports a manifest nested deeper than MaxDepth.
var ErrTooDeep = errors.New("nested too deep")

// splitArray returns the elements of a JSON array, failing with
// ErrTooManyParts as soon as there are more than max.
func splitArray(raw json.RawMessage, max int) ([]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('[') {
		return nil, invalid("parts is not an array")
	}
	out := []json.RawMessage{}
	for dec.More() {
		if len(out) == max {
			return nil, ErrTooManyParts
		}
		var elem json.RawMessage
		if err := dec.Decode(&elem); err != nil {
			return nil, invalid("parts is not an array")
		}
		out = append(out, elem)
	}
	return out, nil
}

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
	Version   Version
	SchemaID  string
	KeyColumn string
	KeyUnique bool
	Parts     []Part
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
	// Extensions is the part's extensions object exactly as written, nil
	// when the part has none (EXT-2).
	Extensions json.RawMessage
}

// Parse parses and validates raw. It checks, in this order: that raw is
// one UTF-8 JSON object without a byte order mark and without duplicate
// member names in any object (MAN-1, MAN-2); the format name (MAN-4); the
// version pair (MAN-4, VER-1), before any other member, so that a manifest
// of a later revision is refused as unsupported rather than invalid; then
// every member (MAN-3, PART-1 to PART-3); and last the order of the bounds
// (PART-4).
func Parse(raw []byte, maxParts int) (*Manifest, error) {
	if bytes.HasPrefix(raw, []byte("\xef\xbb\xbf")) {
		return nil, invalid("the manifest begins with a byte order mark")
	}
	if !utf8.Valid(raw) {
		return nil, invalid("the manifest is not valid UTF-8")
	}
	if err := checkSyntax(raw); err != nil {
		if errors.Is(err, ErrTooDeep) {
			return nil, err
		}
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
		case "format", "format_version", "draft_revision", "schema_id", "key", "parts", "extensions":
		default:
			return nil, invalid("undefined member %q", name)
		}
	}
	m := &Manifest{Version: version}
	if err := member(top, "schema_id", &m.SchemaID, true); err != nil {
		return nil, err
	}
	if m.SchemaID == "" || hasControl(m.SchemaID) {
		return nil, invalid("schema_id is empty or holds a control character")
	}
	var key map[string]json.RawMessage
	if err := json.Unmarshal(top["key"], &key); err != nil || key == nil || len(key) != 2 {
		return nil, invalid("key must contain column and unique")
	}
	if err := member(key, "column", &m.KeyColumn, true); err != nil {
		return nil, err
	}
	if m.KeyColumn == "" || hasControl(m.KeyColumn) {
		return nil, invalid("invalid key column name")
	}
	if string(key["unique"]) != "true" && string(key["unique"]) != "false" {
		return nil, invalid("key.unique must be boolean")
	}
	m.KeyUnique = string(key["unique"]) == "true"
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
	parts, err := splitArray(rawParts, maxParts)
	if err != nil {
		return nil, err
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
			return nil, invalid("part %d (%s): min_key is above max_key", i, part.Path)
		}
		if i > 0 && bytes.Compare(m.Parts[i-1].MaxKey, part.MinKey) >= 0 {
			return nil, invalid("part %d (%s) does not start after part %d ends", i, part.Path, i-1)
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
// supported pair (MAN-7). Well-formed but unknown pairs are
// unsupported_version; ill-typed members, a format_version 0 without a
// draft_revision and a draft_revision on another format_version are
// invalid_manifest.
func parseVersion(top map[string]json.RawMessage) (Version, error) {
	rawVersion, ok := top["format_version"]
	if !ok {
		return Version{}, invalid("format_version is missing")
	}
	formatVersion, err := Integer(rawVersion)
	if err != nil || formatVersion < 0 {
		return Version{}, invalid("format_version is not a non-negative integer")
	}
	v := Version{FormatVersion: formatVersion}
	if rawDraft, ok := top["draft_revision"]; ok {
		draft, err := Integer(rawDraft)
		if err != nil || draft < 1 {
			return Version{}, invalid("draft_revision is not an integer of at least 1")
		}
		v.DraftRevision = draft
	}
	if v.FormatVersion == 0 && v.DraftRevision == 0 {
		return Version{}, invalid("format_version 0 requires a draft_revision")
	}
	if v.FormatVersion != 0 && v.DraftRevision != 0 {
		return Version{}, invalid("draft_revision is present on format_version %d", v.FormatVersion)
	}
	for _, s := range Supported {
		if s == v {
			return v, nil
		}
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
		case "path", "rows", "bytes", "min_key_base64", "max_key_base64", "sha256", "extensions":
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
		n, err := Integer(rawN)
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
	if ext, ok := fields["extensions"]; ok {
		if err := checkExtensions(ext); err != nil {
			return Part{}, err
		}
		p.Extensions = bytes.Clone(ext)
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

// Integer decodes an integer member exactly (MAN-6): a JSON number with no
// sign, fraction, exponent or leading zero, from 0 to 2^63-1. The
// conformance protocol spells cursor offsets the same way.
func Integer(raw json.RawMessage) (int64, error) {
	text := string(raw)
	if text == "" || len(text) > 1 && text[0] == '0' {
		return 0, errors.New("not an integer")
	}
	for i := 0; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return 0, errors.New("not an integer")
		}
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, errors.New("not an integer from 0 to 2^63-1")
	}
	return n, nil
}

// DecodeKey decodes a non-empty key in canonical standard base64 with
// padding (PART-3): decoding and re-encoding must give back the same string,
// so whitespace, missing padding, the URL-safe alphabet and non-zero unused
// bits are all refused.
func DecodeKey(s string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(s)
	if err != nil || base64.StdEncoding.EncodeToString(key) != s {
		return nil, errors.New("not canonical standard base64")
	}
	if len(key) == 0 {
		return nil, errors.New("a key is not empty")
	}
	return key, nil
}

// CheckPath applies PART-1 to a part path: segments separated by "/", each
// non-empty, neither "." nor "..", without "\" or a control character
// (U+0000 to U+001F, U+007F), and no leading "/". Paths are literal: nothing
// is decoded or normalised.
func CheckPath(path string) error {
	if path == "" {
		return errors.New("empty")
	}
	if strings.HasPrefix(path, "/") {
		return errors.New("begins with /")
	}
	if strings.ContainsRune(path, '\\') || hasControl(path) {
		return errors.New(`contains \ or a control character`)
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

// checkExtensions requires an extensions object, top-level or a part's,
// whose namespace names are not empty (EXT-4). Its values are not
// interpreted: no accelerator is defined, and unknown namespaces and
// members are ignored (EXT-3). Duplicate names and unpaired surrogates
// inside it are refused with the rest of the manifest (checkSyntax).
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

// hasControl reports whether s holds U+0000 to U+001F or U+007F.
func hasControl(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return true
		}
	}
	return false
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
	if err := checkSurrogates(raw); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := walk(dec, 0); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("data after the manifest object")
	}
	return nil
}

// walk checks the value dec is at, nested depth containers deep, and
// what it holds; a container beyond MaxDepth is ErrTooDeep, before it is
// descended into, so that nesting never exhausts the stack.
func walk(dec *json.Decoder, depth int) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("not valid JSON: %w", err)
	}
	if (tok == json.Delim('{') || tok == json.Delim('[')) && depth >= MaxDepth {
		return ErrTooDeep
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
			if err := walk(dec, depth+1); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("not valid JSON: %w", err)
		}
	case json.Delim('['):
		for dec.More() {
			if err := walk(dec, depth+1); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("not valid JSON: %w", err)
		}
	}
	return nil
}

// checkSurrogates refuses an escaped unpaired surrogate in any string
// (MAN-1), which encoding/json would silently replace with U+FFFD. It runs
// on any input and assumes nothing about its validity: whatever it does not
// refuse, the JSON parser still checks.
func checkSurrogates(raw []byte) error {
	inString := false
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if !inString {
			if c == '"' {
				inString = true
			}
			continue
		}
		switch c {
		case '"':
			inString = false
		case '\\':
			if i+1 < len(raw) && raw[i+1] == 'u' {
				r, ok := hex4(raw, i+2)
				switch {
				case ok && r >= 0xd800 && r <= 0xdbff:
					low, okLow := rune(0), false
					if i+7 < len(raw) && raw[i+6] == '\\' && raw[i+7] == 'u' {
						low, okLow = hex4(raw, i+8)
					}
					if !okLow || low < 0xdc00 || low > 0xdfff {
						return errors.New("a string holds an unpaired surrogate escape")
					}
					i += 11
					continue
				case ok && r >= 0xdc00 && r <= 0xdfff:
					return errors.New("a string holds an unpaired surrogate escape")
				}
			}
			i++ // skip the escaped character
		}
	}
	return nil
}

// hex4 decodes the four hex digits at raw[i:i+4].
func hex4(raw []byte, i int) (rune, bool) {
	if i+4 > len(raw) {
		return 0, false
	}
	var r rune
	for _, c := range raw[i : i+4] {
		r <<= 4
		switch {
		case c >= '0' && c <= '9':
			r |= rune(c - '0')
		case c >= 'a' && c <= 'f':
			r |= rune(c-'a') + 10
		case c >= 'A' && c <= 'F':
			r |= rune(c-'A') + 10
		default:
			return 0, false
		}
	}
	return r, true
}
