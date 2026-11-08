// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestQuickstart(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	// Derive expectations from the checked-in source rows, not reader output.
	source, err := os.ReadFile(filepath.Join(root, "conformance", "generators", "rows", "multipart.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Parts []struct {
			Rows []struct {
				Key string `json:"key_base64"`
			}
		}
	}
	if err := json.Unmarshal(source, &fixture); err != nil {
		t.Fatal(err)
	}
	var preview []string
	var prefixes []string
	seen := make(map[string]bool)
	for _, part := range fixture.Parts {
		for _, row := range part.Rows {
			keyBytes, err := base64.StdEncoding.DecodeString(row.Key)
			if err != nil {
				t.Fatal(err)
			}
			key := string(keyBytes)
			if strings.HasPrefix(key, "logs/2026/01/") && len(preview) < 2 {
				preview = append(preview, key)
			}
			if tail, ok := strings.CutPrefix(key, "logs/2026/"); ok {
				month, _, found := strings.Cut(tail, "/")
				if !found {
					t.Fatalf("expected month directory in source key %q", key)
				}
				prefix := "logs/2026/" + month + "/"
				if !seen[prefix] {
					prefixes = append(prefixes, prefix)
					seen[prefix] = true
				}
			}
		}
	}
	if len(preview) != 2 || len(prefixes) != 3 {
		t.Fatalf("fixture changed: %d preview rows, %d prefixes", len(preview), len(prefixes))
	}
	var want, got bytes.Buffer
	for _, key := range preview {
		fmt.Fprintf(&want, "preview: %s\n", key)
	}
	for i, prefix := range prefixes {
		fmt.Fprintf(&want, "page %d: prefix %s\n", i+1, prefix)
	}
	if err := run(context.Background(), filepath.Join(root, "conformance", "datasets", "multipart"), &got); err != nil {
		t.Fatal(err)
	}
	if got.String() != want.String() {
		t.Fatalf("output:\n%s\nwant:\n%s", &got, &want)
	}
	doc, err := os.ReadFile(filepath.Join(root, "docs", "quickstart.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(doc), "```text\n"+want.String()+"```") {
		t.Fatal("quickstart documentation does not match expected output")
	}
}

func TestMissingDataset(t *testing.T) {
	if err := run(context.Background(), filepath.Join(t.TempDir(), "missing"), &bytes.Buffer{}); err == nil {
		t.Fatal("expected an error for missing dataset")
	}
}
