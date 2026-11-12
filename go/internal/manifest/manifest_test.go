// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package manifest

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

const validManifest = `{
  "format": "seekparquet",
  "format_version": 0,
  "draft_revision": 1,
  "schema_id": "example/objects/v1",
  "key": {"column": "key", "unique": true},
  "parts": [
    {"path": "data/part-00000.parquet", "rows": 3, "bytes": 1843,
     "min_key_base64": "bG9ncy8yMDI2LzAxL2EuZ3o=", "max_key_base64": "bG9ncy8yMDI2LzAxL3ouZ3o=",
     "sha256": "0000000000000000000000000000000000000000000000000000000000000000"},
    {"path": "data/part-00001.parquet", "rows": 2, "bytes": 1790,
     "min_key_base64": "bG9ncy8yMDI2LzAyL2EuZ3o=", "max_key_base64": "bG9ncy8yMDI2LzAyL2IuZ3o=",
     "extensions": {"example-producer": {"batch": [1, {"n": 2.50}]}, "seekparquet": {"unknown": {"version": 99}}}}
  ],
  "extensions": {"example-producer": {"source": "s3://example-bucket/logs/", "complete": true}}
}`

const partExtensions = `{"example-producer": {"batch": [1, {"n": 2.50}]}, "seekparquet": {"unknown": {"version": 99}}}`

func TestParseValid(t *testing.T) {
	m, err := Parse([]byte(validManifest), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != (Version{0, 1}) || m.SchemaID != "example/objects/v1" || len(m.Parts) != 2 {
		t.Fatalf("%+v", m)
	}
	if string(m.Parts[0].MinKey) != "logs/2026/01/a.gz" || m.Parts[1].SHA256 != "" || m.Parts[0].Rows != 3 {
		t.Fatalf("%+v", m.Parts)
	}
	if !bytes.Contains(m.Extensions, []byte(`"complete": true`)) {
		t.Fatalf("extensions %s", m.Extensions)
	}
	if m.Parts[0].Extensions != nil || string(m.Parts[1].Extensions) != partExtensions {
		t.Fatalf("part extensions %q, %q", m.Parts[0].Extensions, m.Parts[1].Extensions)
	}
	empty, err := Parse([]byte(`{"format":"seekparquet","format_version":0,"draft_revision":1,"schema_id":"x","key":{"column":"key","unique":true},"parts":[]}`), 1000)
	if err != nil || len(empty.Parts) != 0 || empty.Extensions != nil {
		t.Fatalf("empty dataset: %+v, %v", empty, err)
	}
}

// edit replaces the first occurrence of old in validManifest.
func edit(old, new string) string {
	if !strings.Contains(validManifest, old) {
		panic("no " + old)
	}
	return strings.Replace(validManifest, old, new, 1)
}

func TestParseRefusals(t *testing.T) {
	cases := []struct {
		name, raw, code string
	}{
		{"not json", `{"format":`, CodeInvalidManifest},
		{"not an object", `[]`, CodeInvalidManifest},
		{"null", `null`, CodeInvalidManifest},
		{"trailing data", validManifest + `{}`, CodeInvalidManifest},
		{"bom", "\xef\xbb\xbf" + validManifest, CodeInvalidManifest},
		{"not utf-8", edit(`example/objects/v1`, "example/\xff"), CodeInvalidManifest},
		{"unpaired surrogate", edit(`example/objects/v1`, `example\ud800x`), CodeInvalidManifest},
		{"lone low surrogate", edit(`example/objects/v1`, `\udc00`), CodeInvalidManifest},
		{"surrogate in extensions", edit(`"complete": true`, `"complete": "\ud83d"`), CodeInvalidManifest},
		{"duplicate member", edit(`"schema_id": "example/objects/v1",`, `"schema_id": "a", "schema_id": "b",`), CodeInvalidManifest},
		{"duplicate escaped member", edit(`"schema_id": "example/objects/v1",`, `"schema_id": "a", "schema_id": "b",`), CodeInvalidManifest},
		{"duplicate in part", edit(`"rows": 3,`, `"rows": 3, "rows": 3,`), CodeInvalidManifest},
		{"duplicate in extensions", edit(`"complete": true`, `"complete": true, "complete": false`), CodeInvalidManifest},
		{"wrong format", edit(`"seekparquet"`, `"parquet"`), CodeInvalidManifest},
		{"no format", edit(`"format": "seekparquet",`, ``), CodeInvalidManifest},
		{"no format_version", edit(`"format_version": 0,`, ``), CodeInvalidManifest},
		{"fractional format_version", edit(`"format_version": 0,`, `"format_version": 0.0,`), CodeInvalidManifest},
		{"string format_version", edit(`"format_version": 0,`, `"format_version": "0",`), CodeInvalidManifest},
		{"draft without revision", edit(`"draft_revision": 1,`, ``), CodeInvalidManifest},
		{"draft revision zero", edit(`"draft_revision": 1,`, `"draft_revision": 0,`), CodeInvalidManifest},
		{"draft revision 2", edit(`"draft_revision": 1,`, `"draft_revision": 2,`), CodeUnsupportedVersion},
		{"format_version 1", edit(`"format_version": 0,
  "draft_revision": 1,`, `"format_version": 1,`), CodeUnsupportedVersion},
		{"format_version 1 with a draft revision", edit(`"format_version": 0,`, `"format_version": 1,`), CodeInvalidManifest},
		{"format_version 2 with new members", edit(`"format_version": 0,
  "draft_revision": 1,`, `"format_version": 2, "new_member": true,`), CodeUnsupportedVersion},
		{"missing key", edit(`"key": {"column": "key", "unique": true},`, ``), CodeInvalidManifest},
		{"null key", edit(`{"column": "key", "unique": true}`, `null`), CodeInvalidManifest},
		{"missing unique", edit(`, "unique": true`, ``), CodeInvalidManifest},
		{"string unique", edit(`"unique": true`, `"unique": "true"`), CodeInvalidManifest},
		{"null unique", edit(`"unique": true`, `"unique": null`), CodeInvalidManifest},
		{"empty key column", edit(`"column": "key"`, `"column": ""`), CodeInvalidManifest},
		{"control key column", edit(`"column": "key"`, `"column": "bad\u007f"`), CodeInvalidManifest},
		{"extra key field", edit(`"unique": true`, `"unique": true, "other": 1`), CodeInvalidManifest},
		{"undefined member", edit(`"parts"`, `"created_at": "now", "parts"`), CodeInvalidManifest},
		{"undefined part member", edit(`"rows": 3,`, `"rows": 3, "checksum": "x",`), CodeInvalidManifest},
		{"no parts", `{"format":"seekparquet","format_version":0,"draft_revision":1,"schema_id":"x"}`, CodeInvalidManifest},
		{"parts not an array", edit(`"parts": [`, `"parts": {"a": [`) + "}", CodeInvalidManifest},
		{"empty schema_id", edit(`"example/objects/v1"`, `""`), CodeInvalidManifest},
		{"control in schema_id", edit(`"example/objects/v1"`, `"a\u0007"`), CodeInvalidManifest},
		{"extensions not an object", edit(`"extensions": {`, `"extensions": [{`) + "]", CodeInvalidManifest},
		{"empty namespace", edit(`"example-producer"`, `""`), CodeInvalidManifest},
		{"part extensions not an object", edit(partExtensions, `"x"`), CodeInvalidManifest},
		{"part extensions null", edit(partExtensions, `null`), CodeInvalidManifest},
		{"part extensions array", edit(partExtensions, `[]`), CodeInvalidManifest},
		{"part empty namespace", edit(`"seekparquet": {"unknown"`, `"": {"unknown"`), CodeInvalidManifest},
		{"duplicate in part extensions", edit(`{"version": 99}`, `{"version": 99, "version": 99}`), CodeInvalidManifest},
		{"duplicate part extensions", edit(`"extensions": {"example-producer": {"batch"`, `"extensions": {}, "extensions": {"example-producer": {"batch"`), CodeInvalidManifest},
		{"surrogate in part extensions", edit(`{"version": 99}`, `{"version": "\udfff"}`), CodeInvalidManifest},
		{"undefined part member beside extensions", edit(`"extensions": {"example-producer": {"batch"`, `"checksum": "x", "extensions": {"example-producer": {"batch"`), CodeInvalidManifest},
		{"rows zero", edit(`"rows": 3`, `"rows": 0`), CodeInvalidManifest},
		{"rows negative", edit(`"rows": 3`, `"rows": -3`), CodeInvalidManifest},
		{"rows exponent", edit(`"rows": 3`, `"rows": 3e0`), CodeInvalidManifest},
		{"rows fraction", edit(`"rows": 3`, `"rows": 3.0`), CodeInvalidManifest},
		{"rows leading zero", edit(`"rows": 3`, `"rows": 03`), CodeInvalidManifest},
		{"rows string", edit(`"rows": 3`, `"rows": "3"`), CodeInvalidManifest},
		{"bytes beyond 2^63-1", edit(`"bytes": 1843`, `"bytes": 9223372036854775808`), CodeInvalidManifest},
		{"no bytes", edit(`"bytes": 1843,`, ``), CodeInvalidManifest},
		{"no min key", edit(`"min_key_base64": "bG9ncy8yMDI2LzAxL2EuZ3o=",`, ``), CodeInvalidManifest},
		{"empty key", edit(`"bG9ncy8yMDI2LzAxL2EuZ3o="`, `""`), CodeInvalidManifest},
		{"unpadded key", edit(`"bG9ncy8yMDI2LzAxL2EuZ3o="`, `"bG9ncy8yMDI2LzAxL2EuZ3o"`), CodeInvalidManifest},
		{"url alphabet key", edit(`"bG9ncy8yMDI2LzAxL2EuZ3o="`, `"_w=="`), CodeInvalidManifest},
		{"non-canonical bits", edit(`"bG9ncy8yMDI2LzAxL2EuZ3o="`, `"YR=="`), CodeInvalidManifest},
		{"key with newline", edit(`"bG9ncy8yMDI2LzAxL2EuZ3o="`, `"bG9ncy8y\nMDI2LzAxL2EuZ3o="`), CodeInvalidManifest},
		{"bad sha256", edit(`"0000000000000000000000000000000000000000000000000000000000000000"`, `"ABCD"`), CodeInvalidManifest},
		{"uppercase sha256", edit(`"0000000000000000000000000000000000000000000000000000000000000000"`, `"`+strings.Repeat("A", 64)+`"`), CodeInvalidManifest},
		{"absolute path", edit(`"data/part-00000.parquet"`, `"/data/part-00000.parquet"`), CodeInvalidManifest},
		{"escaping path", edit(`"data/part-00000.parquet"`, `"../outside.parquet"`), CodeInvalidManifest},
		{"dot segment", edit(`"data/part-00000.parquet"`, `"data/./part.parquet"`), CodeInvalidManifest},
		{"empty segment", edit(`"data/part-00000.parquet"`, `"data//part.parquet"`), CodeInvalidManifest},
		{"trailing slash", edit(`"data/part-00000.parquet"`, `"data/"`), CodeInvalidManifest},
		{"backslash", edit(`"data/part-00000.parquet"`, `"data\\part.parquet"`), CodeInvalidManifest},
		{"control character", edit(`"data/part-00000.parquet"`, `"data/\u007fpart.parquet"`), CodeInvalidManifest},
		{"duplicate path", edit(`"data/part-00001.parquet"`, `"data/part-00000.parquet"`), CodeInvalidManifest},
		{"min above max", edit(`"bG9ncy8yMDI2LzAxL2EuZ3o="`, `"enp6"`), CodeInvalidManifest},
		{"parts overlap", edit(`"bG9ncy8yMDI2LzAyL2EuZ3o="`, `"bG9ncy8yMDI2LzAxL3ouZ3o="`), CodeInvalidManifest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(c.raw), 1000)
			var e *Error
			if !errors.As(err, &e) || e.Code != c.code {
				t.Fatalf("got %v, want %s", err, c.code)
			}
		})
	}
}

func TestPathsAreLiteral(t *testing.T) {
	for _, ok := range []string{"a", "data/part%2F1.parquet", "é/x.parquet", "..a/b", "a./.b", "A/a"} {
		if err := CheckPath(ok); err != nil {
			t.Fatalf("%q: %v", ok, err)
		}
	}
}

// FuzzParse requires Parse never to panic, and every manifest it accepts
// to satisfy the rules it is meant to have checked.
func FuzzParse(f *testing.F) {
	f.Add([]byte(validManifest))
	f.Add([]byte(edit(`{"column": "key", "unique": true}`, `{"column": "object.id", "unique": false}`)))
	f.Add([]byte(`{"format":"seekparquet","format_version":0,"draft_revision":1,"schema_id":"x","key":{"column":"key","unique":true},"parts":[]}`))
	f.Add([]byte(`{"format":"seekparquet","format_version":7,"parts":[]}`))
	f.Add([]byte(`{"a":"𐀀","a":1}`))
	f.Add([]byte(`{"format":"seekparquet","format_version":1e999999999}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		m, err := Parse(raw, 1000)
		if err != nil {
			var e *Error
			if !errors.As(err, &e) {
				t.Fatalf("not a manifest refusal: %v", err)
			}
			return
		}
		if m.Version != (Version{0, 1}) || m.SchemaID == "" || m.KeyColumn == "" || hasControl(m.KeyColumn) {
			t.Fatalf("accepted %+v", m)
		}
		seen := map[string]bool{}
		for i, p := range m.Parts {
			if CheckPath(p.Path) != nil || seen[p.Path] || p.Rows < 1 || p.Bytes < 1 || len(p.MinKey) == 0 || bytes.Compare(p.MinKey, p.MaxKey) > 0 {
				t.Fatalf("accepted part %d %+v", i, p)
			}
			if i > 0 && bytes.Compare(m.Parts[i-1].MaxKey, p.MinKey) >= 0 {
				t.Fatalf("accepted overlapping parts")
			}
			seen[p.Path] = true
		}
	})
}
