// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestManifestExamplesRoundTrip: every valid example manifest of the
// specification parses, and marshals back to exactly its own bytes.
func TestManifestExamplesRoundTrip(t *testing.T) {
	files, err := filepath.Glob("../spec/examples/*.json")
	must(t, err)
	if len(files) < 3 {
		t.Fatalf("found %d example manifests", len(files))
	}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		must(t, err)
		m, err := ParseManifest(raw, Limits{})
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		out, err := MarshalManifest(m)
		if err != nil {
			t.Fatalf("%s: marshal: %v", path, err)
		}
		if !bytes.Equal(out, raw) {
			t.Errorf("%s: marshalled\n%s\nwant\n%s", path, out, raw)
		}
		again, err := ParseManifest(out, Limits{})
		must(t, err)
		if !reflect.DeepEqual(again, m) {
			t.Errorf("%s: reparsed %+v, want %+v", path, again, m)
		}
	}
}

// TestManifestRefusals: the invalid examples and malformed manifests are
// refused with the specification's codes; limits are the caller's.
func TestManifestRefusals(t *testing.T) {
	invalid, err := filepath.Glob("../spec/examples/invalid/*.json")
	must(t, err)
	for _, path := range invalid {
		raw, err := os.ReadFile(path)
		must(t, err)
		if _, err := ParseManifest(raw, Limits{}); RefusalCodeOf(err) != CodeInvalidManifest {
			t.Errorf("%s: %v, want invalid_manifest", path, err)
		}
	}
	const head = `{"format":"seekparquet","format_version":0,"draft_revision":1,"schema_id":"s","key":{"column":"key","unique":true},`
	part := `{"path":"data/a.parquet","rows":1,"bytes":10,"min_key_base64":"YQ==","max_key_base64":"YQ=="}`
	for name, c := range map[string]struct {
		raw  string
		code RefusalCode
	}{
		"not JSON":             {`{"format":`, CodeInvalidManifest},
		"trailing data":        {head + `"parts":[]} {}`, CodeInvalidManifest},
		"unknown top member":   {head + `"parts":[],"created_at":"now"}`, CodeInvalidManifest},
		"unknown part member":  {head + `"parts":[` + strings.Replace(part, `"rows"`, `"md5":"x","rows"`, 1) + `]}`, CodeInvalidManifest},
		"unknown key member":   {strings.Replace(head, `"unique":true`, `"unique":true,"order":"asc"`, 1) + `"parts":[]}`, CodeInvalidManifest},
		"duplicate member":     {head + `"parts":[],"schema_id":"t"}`, CodeInvalidManifest},
		"duplicate in ext":     {head + `"parts":[],"extensions":{"a":{"x":1,"x":2}}}`, CodeInvalidManifest},
		"inexact integer":      {head + `"parts":[` + strings.Replace(part, `"rows":1`, `"rows":1.0`, 1) + `]}`, CodeInvalidManifest},
		"url-safe base64":      {head + `"parts":[` + strings.Replace(part, `"YQ==","max`, `"-_8=","max`, 1) + `]}`, CodeInvalidManifest},
		"unpadded base64":      {head + `"parts":[` + strings.Replace(part, `"max_key_base64":"YQ=="`, `"max_key_base64":"YQ"`, 1) + `]}`, CodeInvalidManifest},
		"overlapping parts":    {head + `"parts":[` + part + `,` + strings.Replace(part, "data/a", "data/b", 1) + `]}`, CodeInvalidManifest},
		"later draft revision": {strings.Replace(head, `"draft_revision":1`, `"draft_revision":2`, 1) + `"parts":[]}`, CodeUnsupportedVersion},
		"final format":         {strings.Replace(head, `"format_version":0,"draft_revision":1`, `"format_version":1`, 1) + `"parts":[]}`, CodeUnsupportedVersion},
	} {
		if _, err := ParseManifest([]byte(c.raw), Limits{}); RefusalCodeOf(err) != c.code {
			t.Errorf("%s: %v, want %s", name, err, c.code)
		}
	}
	two := head + `"parts":[` + part + `,` + strings.NewReplacer("data/a", "data/b", "YQ==", "Yg==").Replace(part) + `]}`
	if _, err := ParseManifest([]byte(two), Limits{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseManifest([]byte(two), Limits{Parts: 1}); !errors.Is(err, ErrLimitExceeded) {
		t.Errorf("two parts with Limits.Parts 1: %v", err)
	}
	if _, err := ParseManifest([]byte(two), Limits{ManifestBytes: 64}); !errors.Is(err, ErrLimitExceeded) {
		t.Errorf("Limits.ManifestBytes: %v", err)
	}
}

// TestManifestExtensionsPreserved: extensions keep every member, their
// order and the spelling of strings and numbers through a marshal, and a
// marshalled manifest round-trips byte for byte.
func TestManifestExtensionsPreserved(t *testing.T) {
	ext := `{"zeta":{"b":1e400,"a":12345678901234567890123,"s":"é😀<&>","n":[1.50,-0.0,null]},"alpha":true}`
	m := Manifest{Key: Key{Column: "key", Unique: true}, Version: Version{FormatVersion, DraftRevision}, SchemaID: "s",
		Extensions: json.RawMessage(ext)}
	out, err := MarshalManifest(m)
	must(t, err)
	got, err := ParseManifest(out, Limits{})
	must(t, err)
	var compact bytes.Buffer
	must(t, json.Compact(&compact, got.Extensions))
	if compact.String() != ext {
		t.Errorf("extensions became %s, want %s", compact.String(), ext)
	}
	zeta, ok := got.Extension("zeta")
	if !ok || !strings.Contains(string(zeta), `12345678901234567890123`) || !strings.Contains(string(zeta), `1e400`) {
		t.Errorf("Extension(zeta) = %s, %v", zeta, ok)
	}
	again, err := MarshalManifest(got)
	must(t, err)
	if !bytes.Equal(again, out) {
		t.Errorf("second marshal differs:\n%s\n%s", again, out)
	}
	if !bytes.Equal(got.Extensions, mustExtension(t, again)) {
		t.Errorf("parsed extensions are not the marshalled bytes")
	}
	for _, empty := range []string{"", "{}", " { } "} {
		m.Extensions = json.RawMessage(empty)
		out, err := MarshalManifest(m)
		must(t, err)
		if bytes.Contains(out, []byte("extensions")) {
			t.Errorf("extensions %q written: %s", empty, out)
		}
	}
	m.Extensions = json.RawMessage(`[1]`)
	if _, err := MarshalManifest(m); RefusalCodeOf(err) != CodeInvalidManifest {
		t.Errorf("array extensions: %v", err)
	}
}

func mustExtension(t *testing.T, raw []byte) json.RawMessage {
	t.Helper()
	m, err := ParseManifest(raw, Limits{})
	must(t, err)
	return m.Extensions
}

// TestManifestBinaryBounds: keys that are not UTF-8, hold zero and 0xFF
// bytes or run to thousands of bytes survive a marshal exactly, in
// canonical base64; sha256 is written only when set.
func TestManifestBinaryBounds(t *testing.T) {
	long := bytes.Repeat([]byte{0xff}, 5000)
	m := Manifest{Key: Key{Column: "kéy", Unique: false}, Version: Version{FormatVersion, DraftRevision}, SchemaID: "binary/v1",
		Parts: []PartInfo{
			{Path: "data/part-00000.parquet", Rows: 2, Bytes: 100, MinKey: []byte{0}, MaxKey: []byte{0, 0xff, 0x80}},
			{Path: "data/part-00001.parquet", Rows: 1 << 40, Bytes: 1<<63 - 1, MinKey: append([]byte{0x01}, long...), MaxKey: long,
				SHA256: strings.Repeat("0123456789abcdef", 4)},
		}}
	out, err := MarshalManifest(m)
	must(t, err)
	got, err := ParseManifest(out, Limits{})
	must(t, err)
	if !reflect.DeepEqual(got, m) {
		t.Errorf("round trip changed the manifest")
	}
	if n := bytes.Count(out, []byte(`"sha256"`)); n != 1 {
		t.Errorf("%d sha256 members, want 1", n)
	}
	if !bytes.Contains(out, []byte(`"min_key_base64": "AA=="`)) || !bytes.Contains(out, []byte(`"bytes": 9223372036854775807`)) {
		t.Errorf("unexpected spelling:\n%s", out)
	}
}

// TestMarshalManifestRefuses: MarshalManifest writes only a manifest this
// package would read.
func TestMarshalManifestRefuses(t *testing.T) {
	good := func() Manifest {
		return Manifest{Key: Key{Column: "key", Unique: true}, Version: Version{FormatVersion, DraftRevision}, SchemaID: "s",
			Parts: []PartInfo{{Path: "data/a.parquet", Rows: 1, Bytes: 1, MinKey: []byte("a"), MaxKey: []byte("a")}}}
	}
	if _, err := MarshalManifest(good()); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		edit func(*Manifest)
		code RefusalCode
	}{
		"zero version":      {func(m *Manifest) { m.Version = Version{} }, CodeInvalidManifest},
		"later revision":    {func(m *Manifest) { m.Version.DraftRevision = 2 }, CodeUnsupportedVersion},
		"empty schema id":   {func(m *Manifest) { m.SchemaID = "" }, CodeInvalidManifest},
		"invalid UTF-8":     {func(m *Manifest) { m.SchemaID = "\xff" }, CodeInvalidManifest},
		"empty key column":  {func(m *Manifest) { m.Key.Column = "" }, CodeInvalidManifest},
		"empty key":         {func(m *Manifest) { m.Parts[0].MinKey = nil }, CodeInvalidManifest},
		"inverted bounds":   {func(m *Manifest) { m.Parts[0].MinKey = []byte("b") }, CodeInvalidManifest},
		"zero rows":         {func(m *Manifest) { m.Parts[0].Rows = 0 }, CodeInvalidManifest},
		"escaping path":     {func(m *Manifest) { m.Parts[0].Path = "../a.parquet" }, CodeInvalidManifest},
		"uppercase sha256":  {func(m *Manifest) { m.Parts[0].SHA256 = strings.Repeat("A", 64) }, CodeInvalidManifest},
		"duplicate path":    {func(m *Manifest) { m.Parts = append(m.Parts, m.Parts[0]) }, CodeInvalidManifest},
		"invalid extension": {func(m *Manifest) { m.Extensions = json.RawMessage(`{"a":`) }, CodeInvalidManifest},
	} {
		m := good()
		c.edit(&m)
		if _, err := MarshalManifest(m); RefusalCodeOf(err) != c.code {
			t.Errorf("%s: %v, want %s", name, err, c.code)
		}
	}
}

func TestFormatConstants(t *testing.T) {
	if got := SupportedVersions(); len(got) != 1 || got[0] != (Version{FormatVersion, DraftRevision}) {
		t.Errorf("SupportedVersions() = %v, want the written version only", got)
	}
}
