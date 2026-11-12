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

// mutableTree copies what the generator reads (docs/data, spec, the Go
// packages' non-test sources and the Java main sources) into a temporary
// root, so that a test can change one thing and see the check fail.
func mutableTree(t *testing.T) string {
	t.Helper()
	src := filepath.Join("..", "..", "..", "..")
	dst := t.TempDir()
	copyDir := func(rel string, keep func(name string) bool) {
		entries, err := os.ReadDir(filepath.Join(src, rel))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(dst, rel), 0o755); err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.IsDir() || !keep(e.Name()) {
				continue
			}
			raw, err := os.ReadFile(filepath.Join(src, rel, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dst, rel, e.Name()), raw, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	all := func(string) bool { return true }
	goSource := func(n string) bool { return strings.HasSuffix(n, ".go") && !strings.HasSuffix(n, "_test.go") }
	copyDir("docs/data", all)
	copyDir("spec", func(n string) bool { return strings.HasSuffix(n, ".json") })
	for _, pkg := range []string{"go", "go/write", "go/local"} {
		copyDir(pkg, goSource)
	}
	for _, pkg := range []string{javaMain, javaMain + "/storage"} {
		copyDir("java/"+pkg, func(n string) bool { return strings.HasSuffix(n, ".java") })
	}
	return dst
}

// edit replaces old with new in root/file, failing when old is absent.
func edit(t *testing.T, root, file, old, new string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(file))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), old) {
		t.Fatalf("%s does not contain %q", file, old)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(raw), old, new, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
}

func checkErr(t *testing.T, root string) string {
	t.Helper()
	g, err := newGenerator(root, "")
	if err != nil {
		return err.Error()
	}
	if err := g.check(); err != nil {
		return err.Error()
	}
	for _, render := range g.generatedFiles() {
		if _, err := render(); err != nil {
			return err.Error()
		}
	}
	return ""
}

// TestCheckCatchesDrift changes one fact at a time and requires the check
// to fail naming it.
func TestCheckCatchesDrift(t *testing.T) {
	const limitsJava = "java/" + javaMain + "/Limits.java"
	const statsJava = "java/" + javaMain + "/ReadStats.java"
	for _, c := range []struct {
		name, file, old, new, want string
	}{
		{"java default changed", limitsJava, "DEFAULT_PAGE_BYTES = 64 * MIB", "DEFAULT_PAGE_BYTES = 128 * MIB", "pageBytes default"},
		{"java default unparsable", limitsJava, "DEFAULT_PAGE_BYTES = 64 * MIB", "DEFAULT_PAGE_BYTES = Math.multiplyExact(64, MIB)", "cannot evaluate Limits.DEFAULT_PAGE_BYTES"},
		{"java default missing", limitsJava, "DEFAULT_PAGE_BYTES = 64 * MIB", "DEFAULT_PAGE_BYTEZ = 64 * MIB", "no public constant DEFAULT_PAGE_BYTES"},
		{"java builder method removed, field kept", limitsJava, "public Builder pageBytes(", "public Builder pageByteCount(", "no public method pageBytes"},
		{"java record component added", statsJava, "long readAheadUnusedBytes)", "long readAheadUnusedBytes, long somethingNew)", "somethingNew is not in read-stats.json"},
		{"go comment states another default", "go/limits.go", "Default\n\t// 64 MiB.", "Default\n\t// 65 MiB.", "does not state its default"},
		{"go default not constant", "go/limits.go", "set(&l.PageBytes, 64<<20)", "set(&l.PageBytes, pageBytesDefault())", "not a constant expression"},
		{"go code in conversion form", "go/errors.go", "CodeDigestMismatch        RefusalCode = \"digest_mismatch\"", "CodeDigestMismatch = RefusalCode(\"digest_mismatch\")\n\tCodeNewOne = RefusalCode(\"new_one\")", "RefusalCode constants"},
		{"go code in an unread form", "go/errors.go", "CodeDigestMismatch        RefusalCode = \"digest_mismatch\"", "CodeDigestMismatch RefusalCode = \"digest_\" + \"mismatch\"", "not a string literal"},
		{"documented limit without a LimitName", "go/limits.go", "LimitGetRows           LimitName = \"GetRows\"", "", "no LimitName LimitGetRows"},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := mutableTree(t)
			edit(t, root, c.file, c.old, c.new)
			if got := checkErr(t, root); !strings.Contains(got, c.want) {
				t.Errorf("want an error containing %q, got:\n%s", c.want, got)
			}
		})
	}
	if got := checkErr(t, mutableTree(t)); got != "" {
		t.Errorf("the unchanged tree fails:\n%s", got)
	}
}

func TestEvalJava(t *testing.T) {
	consts := map[string]string{"Limits.MIB": "1 << 20"}
	for expr, want := range map[string]int64{"64 * MIB": 64 << 20, "(128 * MIB)": 128 << 20, "1L << 30": 1 << 30, "1_000_000": 1_000_000, "256L * MIB": 256 << 20} {
		if got, ok := evalJava(expr, consts); !ok || got != want {
			t.Errorf("evalJava(%q) = %d, %v; want %d", expr, got, ok, want)
		}
	}
	if _, ok := evalJava("compute()", consts); ok {
		t.Error("evalJava accepted a call")
	}
}
