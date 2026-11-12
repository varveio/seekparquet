// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/varveio/seekparquet/go/internal/manifest"
)

// Manifest is a dataset's manifest.json (§3) as values: what ParseManifest
// returns and MarshalManifest writes. Keys are bytes, compared as bytes;
// their base64 spelling is the encoding's business (PART-3).
type Manifest struct {
	Key      Key
	Version  Version
	SchemaID string
	// Parts are the parts in ascending key order (PART-4). Nil and empty
	// both mean an empty dataset.
	Parts []PartInfo
	// Extensions is the extensions object (§3.4), nil when there is none.
	// ParseManifest keeps it exactly as written (EXT-2).
	Extensions json.RawMessage
}

// Extension returns the value of one extensions namespace exactly as
// written, and whether the manifest has it.
func (m Manifest) Extension(namespace string) (json.RawMessage, bool) {
	return extension(m.Extensions, namespace)
}

func extension(raw json.RawMessage, namespace string) (json.RawMessage, bool) {
	if raw == nil {
		return nil, false
	}
	var ext map[string]json.RawMessage
	if err := json.Unmarshal(raw, &ext); err != nil {
		return nil, false
	}
	v, ok := ext[namespace]
	return v, ok
}

// ParseManifest parses and checks a manifest.json as opening a dataset
// does (§3): strictly, refusing duplicate member names (MAN-2), undefined
// members (MAN-3), inexact integers (MAN-6), non-canonical base64 (PART-3),
// invalid part paths (PART-1) and bounds out of order (PART-4) with a
// *RefusalError (invalid_manifest), and a version this package does not
// read with unsupported_version (MAN-7). A manifest larger than
// lim.ManifestBytes, or listing more than lim.Parts parts, is
// ErrLimitExceeded; zero limits take their defaults.
//
// The part files are not read: their existence, sizes and contents are
// checked when the dataset is opened (CHECK-1).
func ParseManifest(raw []byte, lim Limits) (Manifest, error) {
	lim = lim.withDefaults()
	if int64(len(raw)) > lim.ManifestBytes {
		return Manifest{}, exceeded(LimitManifestBytes, int64(len(raw)), lim.ManifestBytes, "%s", ManifestName)
	}
	m, err := parseManifest(raw, &lim)
	if err != nil {
		return Manifest{}, err
	}
	return manifestOf(m), nil
}

// parseManifest parses raw with the internal parser and reports its faults
// as refusals.
func parseManifest(raw []byte, lim *Limits) (*manifest.Manifest, error) {
	m, err := manifest.Parse(raw, lim.Parts)
	if err != nil {
		var e *manifest.Error
		switch {
		case errors.As(err, &e):
			return nil, refuse(RefusalCode(e.Code), "%s: %s", ManifestName, e.Detail)
		case errors.Is(err, manifest.ErrTooManyParts):
			return nil, exceeded(LimitParts, -1, int64(lim.Parts), "%s lists more parts", ManifestName)
		case errors.Is(err, manifest.ErrTooDeep):
			return nil, exceeded(LimitManifestNesting, -1, manifest.MaxDepth, "%s nests objects and arrays deeper", ManifestName)
		}
		return nil, err
	}
	return m, nil
}

func manifestOf(m *manifest.Manifest) Manifest {
	out := Manifest{Key: Key{Column: m.KeyColumn, Unique: m.KeyUnique}, Version: Version(m.Version), SchemaID: m.SchemaID,
		Parts: make([]PartInfo, len(m.Parts)), Extensions: m.Extensions}
	for i, p := range m.Parts {
		out.Parts[i] = PartInfo(p)
	}
	return out
}

// compactExtensions returns an extensions object compacted, or nil when
// raw is empty or the empty object, which MarshalManifest leaves out.
func compactExtensions(raw json.RawMessage, where string) (json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return nil, refuse(CodeInvalidManifest, "%s: %sextensions is not valid JSON: %v", ManifestName, where, err)
	}
	if compact.String() == "{}" {
		return nil, nil
	}
	return compact.Bytes(), nil
}

// MarshalManifest writes m as a manifest.json: UTF-8 JSON indented by two
// spaces, with a final newline, its members in the order of the
// specification's examples (format, format_version, draft_revision,
// schema_id, parts, extensions, key), draft_revision only on format
// version 0, keys in canonical standard base64 (PART-3), a part's sha256
// only when it is set, and extensions, the top-level object and each
// part's (after sha256), only when they are non-empty objects. Extensions
// keep their members, their order and the spelling of every string and
// number; only the whitespace between them is re-indented.
//
// The result is checked as ParseManifest checks a manifest, with no limit
// on its size or its parts: m must be a manifest this package reads, or
// MarshalManifest fails with the *RefusalError ParseManifest would give.
// Agreement with the part files is not checked.
func MarshalManifest(m Manifest) ([]byte, error) {
	type key struct {
		Column string `json:"column"`
		Unique bool   `json:"unique"`
	}
	type part struct {
		Path       string          `json:"path"`
		Rows       int64           `json:"rows"`
		Bytes      int64           `json:"bytes"`
		MinKey     string          `json:"min_key_base64"`
		MaxKey     string          `json:"max_key_base64"`
		SHA256     string          `json:"sha256,omitempty"`
		Extensions json.RawMessage `json:"extensions,omitempty"`
	}
	type document struct {
		Format        string          `json:"format"`
		FormatVersion int64           `json:"format_version"`
		DraftRevision *int64          `json:"draft_revision,omitempty"`
		SchemaID      string          `json:"schema_id"`
		Parts         []part          `json:"parts"`
		Extensions    json.RawMessage `json:"extensions,omitempty"`
		Key           key             `json:"key"`
	}
	// encoding/json would replace invalid UTF-8 rather than refuse it.
	text := []string{m.SchemaID, m.Key.Column}
	for _, p := range m.Parts {
		text = append(text, p.Path)
	}
	for _, s := range text {
		if !utf8.ValidString(s) {
			return nil, refuse(CodeInvalidManifest, "%s: %q is not valid UTF-8 (MAN-1)", ManifestName, s)
		}
	}
	doc := document{Format: Format, FormatVersion: m.Version.FormatVersion, SchemaID: m.SchemaID,
		Parts: make([]part, len(m.Parts)), Key: key{Column: m.Key.Column, Unique: m.Key.Unique}}
	if m.Version.FormatVersion == 0 || m.Version.DraftRevision != 0 {
		doc.DraftRevision = &m.Version.DraftRevision
	}
	for i, p := range m.Parts {
		ext, err := compactExtensions(p.Extensions, fmt.Sprintf("part %d: ", i))
		if err != nil {
			return nil, err
		}
		doc.Parts[i] = part{Path: p.Path, Rows: p.Rows, Bytes: p.Bytes, SHA256: p.SHA256, Extensions: ext,
			MinKey: base64.StdEncoding.EncodeToString(p.MinKey), MaxKey: base64.StdEncoding.EncodeToString(p.MaxKey)}
	}
	ext, err := compactExtensions(m.Extensions, "")
	if err != nil {
		return nil, err
	}
	doc.Extensions = ext
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		return nil, refuse(CodeInvalidManifest, "%s: %v", ManifestName, err)
	}
	if _, err := parseManifest(out.Bytes(), &Limits{Parts: len(m.Parts) + 1}); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
