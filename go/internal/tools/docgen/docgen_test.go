// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDocsUpToDate fails when a generated page, region or snippet differs
// from what the code and docs/data render, or when the data and the code
// disagree. Fix: go -C go run ./internal/tools/docgen -w.
func TestDocsUpToDate(t *testing.T) {
	g, err := newGenerator(filepath.Join("..", "..", "..", ".."), "")
	if err != nil {
		t.Fatal(err)
	}
	changed, err := g.run(false)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range changed {
		t.Errorf("%s is out of date: run go -C go run ./internal/tools/docgen -w", f)
	}
}

func TestRel(t *testing.T) {
	for _, c := range []struct{ page, target, want string }{
		{"README.md", "docs/reference/compatibility.md", "docs/reference/compatibility.md"},
		{"docs/reference/limits.md", "docs/guides/troubleshooting.md", "../guides/troubleshooting.md"},
		{"docs/reference/limits.md", "docs/reference/parity.md", "parity.md"},
		{"docs/guides/troubleshooting.md", "spec/format-1.md", "../../spec/format-1.md"},
		{"spec/IMPLEMENTING.md", "spec/format-1.md", "format-1.md"},
	} {
		if got := rel(c.page, c.target); got != c.want {
			t.Errorf("rel(%q, %q) = %q, want %q", c.page, c.target, got, c.want)
		}
	}
}

func TestFill(t *testing.T) {
	dir := t.TempDir()
	src := "package x\n\nfunc Example() {\n\tfmt.Println(1)\n\t// Output:\n\t// 1\n}\n"
	if err := os.WriteFile(filepath.Join(dir, "x_test.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	java := "class A {\n    void f() {\n        // tag::one[]\n        int a = 1;\n          int b = 2;\n        // end::one[]\n    }\n}\n"
	if err := os.WriteFile(filepath.Join(dir, "A.java"), []byte(java), 0o644); err != nil {
		t.Fatal(err)
	}
	g := &generator{root: dir}
	page := "intro\n<!-- snippet: x_test.go#Example -->\nstale\n<!-- /snippet -->\n" +
		"```md\n<!-- snippet: missing.go#Nothing -->\n```\n" +
		"<!-- snippet: A.java#one -->\n<!-- /snippet -->\nend\n"
	got, err := g.fill("p.md", page)
	if err != nil {
		t.Fatal(err)
	}
	want := "intro\n<!-- snippet: x_test.go#Example -->\n```go\nfmt.Println(1)\n// Output:\n// 1\n```\n<!-- /snippet -->\n" +
		"```md\n<!-- snippet: missing.go#Nothing -->\n```\n" +
		"<!-- snippet: A.java#one -->\n```java\nint a = 1;\n  int b = 2;\n```\n<!-- /snippet -->\nend\n"
	if got != want {
		t.Errorf("fill:\n%s\nwant:\n%s", got, want)
	}
	if _, err := g.fill("p.md", "<!-- snippet: x_test.go#Example -->\nno end\n"); err == nil || !strings.Contains(err.Error(), "no <!-- /snippet -->") {
		t.Errorf("an unterminated snippet: %v", err)
	}
	if _, err := g.fill("p.md", "<!-- snippet: x_test.go#Missing -->\n<!-- /snippet -->\n"); err == nil {
		t.Error("a snippet of a missing function did not fail")
	}
}

func TestFormatAmount(t *testing.T) {
	for _, c := range []struct {
		v    int64
		unit string
		want string
	}{
		{64 << 20, "bytes", "64 MiB"}, {1 << 30, "bytes", "1 GiB"}, {832 << 20, "bytes", "832 MiB"},
		{1_000_000, "count", "1,000,000"}, {1 << 24, "count", "16,777,216"}, {1000, "count", "1,000"}, {512, "count", "512"},
	} {
		if got := formatAmount(c.v, c.unit); got != c.want {
			t.Errorf("formatAmount(%d, %s) = %q, want %q", c.v, c.unit, got, c.want)
		}
	}
}
