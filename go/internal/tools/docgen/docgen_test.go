// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"os/exec"
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
		{"java parenthesized default changed", limitsJava, "DEFAULT_PAGE_BYTES = 64 * MIB", "DEFAULT_PAGE_BYTES = (128 * MIB)", "pageBytes default"},
		{"java default unparsable", limitsJava, "DEFAULT_PAGE_BYTES = 64 * MIB", "DEFAULT_PAGE_BYTES = Math.multiplyExact(64, MIB)", "cannot evaluate Limits.DEFAULT_PAGE_BYTES"},
		{"java default missing", limitsJava, "DEFAULT_PAGE_BYTES = 64 * MIB", "DEFAULT_PAGE_BYTEZ = 64 * MIB", "no public constant DEFAULT_PAGE_BYTES"},
		{"java builder method removed, field kept", limitsJava, "public Builder pageBytes(", "public Builder pageByteCount(", "no public method pageBytes"},
		{"java record component added", statsJava, "long readAheadUnusedBytes)", "long readAheadUnusedBytes, long somethingNew)", "somethingNew is not in read-stats.json"},
		{"java record component removed", statsJava, ",\n        long readAheadUnusedBytes)", ")", "no component readAheadUnusedBytes"},
		{"java derived default changed", "java/" + javaMain + "/PageDecoder.java", "return 48L * pageValues + pageBytes;", "return 49L * pageValues + pageBytes;", "pageMemory default"},
		{"java derived helper missing", "java/" + javaMain + "/PageDecoder.java", "static long worstPageMemory(", "static long differentMemory(", "cannot extract derived default"},
		{"java derived helper unparsable", "java/" + javaMain + "/PageDecoder.java", "return 48L * pageValues + pageBytes;", "return Math.multiplyExact(48L, pageValues) + pageBytes;", "cannot evaluate derived default"},
		{"java derived getter changed", limitsJava, "PageDecoder.worstPageMemory(pageValues(), pageBytes())", "832L << 20", "cannot extract derived default"},
		{"go comment states another default", "go/limits.go", "Default\n\t// 64 MiB.", "Default\n\t// 65 MiB.", "does not state its default"},
		{"go default not constant", "go/limits.go", "set(&l.PageBytes, 64<<20)", "set(&l.PageBytes, pageBytesDefault())", "not a constant expression"},
		{"go default missing", "go/limits.go", "set(&l.PageBytes, 64<<20)", "", "sets no default for PageBytes"},
		{"go derived call changed", "go/limits.go", "worstPageMemory(l.PageValues, l.PageBytes)", "worstPageMemory(l.PageValues, l.PageBytes) + 1", "cannot extract derived default"},
		{"go derived helper changed", "go/limits.go", "return pageValues*per + pageBytes", "return pageValues*per + pageBytes + 1", "cannot extract derived default"},
		{"go derived parameters reversed", "go/limits.go", "func worstPageMemory(pageValues, pageBytes int64)", "func worstPageMemory(pageBytes, pageValues int64)", "cannot extract derived default"},
		{"go derived default missing", "go/limits.go", "set(&l.PageMemory, worstPageMemory(l.PageValues, l.PageBytes))", "", "sets no default for PageMemory"},
		{"go code in conversion form", "go/errors.go", "CodeDigestMismatch        RefusalCode = \"digest_mismatch\"", "CodeDigestMismatch = RefusalCode(\"digest_mismatch\")\n\tCodeNewOne = RefusalCode(\"new_one\")", "RefusalCode constants"},
		{"go code in an unread form", "go/errors.go", "CodeDigestMismatch        RefusalCode = \"digest_mismatch\"", "CodeDigestMismatch RefusalCode = \"digest_\" + \"mismatch\"", "not a string literal"},
		{"go conversion in an unread form", "go/errors.go", "CodeDigestMismatch        RefusalCode = \"digest_mismatch\"", "CodeDigestMismatch = RefusalCode(\"digest_\" + \"mismatch\")", "not a string literal"},
		{"go inferred typed expression", "go/errors.go", "CodeDigestMismatch        RefusalCode = \"digest_mismatch\"", "CodeDigestMismatch RefusalCode = \"digest_mismatch\"\n CodeNewOne = CodeDigestMismatch + \"_new\"", "CodeNewOne has a code type"},
		{"documented limit without a LimitName", "go/limits.go", "LimitGetRows           LimitName = \"GetRows\"", "", "no LimitName LimitGetRows"},
		{"cache with an incorrect LimitName", "go/limits.go", "LimitOffsetIndexMemory LimitName = \"OffsetIndexMemory\"", "LimitOffsetIndexMemory LimitName = \"Other\"", "no LimitName LimitOffsetIndexMemory"},
		{"Go-only cache marked as a limit", "docs/data/limits.json", "\"PageIndexCacheMemory\", \"java\": null, \"kind\": \"cache\"", "\"PageIndexCacheMemory\", \"java\": null, \"kind\": \"limit\"", "no LimitName LimitPageIndexCacheMemory"},
		{"Go-only cache given a Java counterpart", "docs/data/limits.json", "\"PageIndexCacheMemory\", \"java\": null", "\"PageIndexCacheMemory\", \"java\": \"pageIndexCacheMemory\"", "no public constant DEFAULT_PAGE_INDEX_CACHE_MEMORY"},
		{"Go-only counter given a Java counterpart", "docs/data/read-stats.json", "\"SourceBatches\", \"java\": null", "\"SourceBatches\", \"java\": \"sourceBatches\"", "no component sourceBatches"},
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

func TestGoOnlyCacheReference(t *testing.T) {
	g, err := newGenerator(mutableTree(t), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := g.check(); err != nil {
		t.Fatal(err)
	}
	page, err := g.renderLimits()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"| [`PageIndexCacheMemory`](#pageindexcachememory) | Go only | 128 MiB | cache budget |",
		"only of those raw bytes",
		"no aggregate bound on a Dataset's metadata exists yet",
	} {
		if !strings.Contains(page, want) {
			t.Errorf("cache reference is missing %q", want)
		}
	}
}

func TestReaderIOOperationReferences(t *testing.T) {
	g, err := newGenerator(mutableTree(t), "")
	if err != nil {
		t.Fatal(err)
	}
	stats, err := g.renderReadStats()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Dataset.Verify(ctx, stats)",
		"pass nil to discard statistics",
	} {
		if !strings.Contains(stats, want) {
			t.Errorf("statistics reference is missing %q", want)
		}
	}
	parity, err := g.renderParity()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"| Logical read policy | `ReadPolicy` | — |",
	} {
		if !strings.Contains(parity, want) {
			t.Errorf("parity reference is missing %q", want)
		}
	}
}

func TestJavaReadStatsDifferences(t *testing.T) {
	g, err := newGenerator(mutableTree(t), "")
	if err != nil {
		t.Fatal(err)
	}
	page, err := g.renderReadStats()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"The table's meanings describe Go",
		"`groupsSkipped` is always 0",
		"`sourceReads` counts `PartSource.read` calls; Java does not batch reads",
		"`indexLoads` counts successful key page index reads from storage",
		"no retained raw metadata case",
	} {
		if !strings.Contains(page, want) {
			t.Errorf("Java statistics reference is missing %q", want)
		}
	}
}

func TestNoPublicVerifyConcurrencyOption(t *testing.T) {
	g, err := newGenerator(mutableTree(t), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := g.check(); err != nil {
		t.Fatal(err)
	}
	page, err := g.renderParity()
	if err != nil {
		t.Fatal(err)
	}
	for _, removed := range []string{"Verify concurrency", "verifyConcurrency", "WithVerifyConcurrency"} {
		if strings.Contains(page, removed) {
			t.Errorf("parity reference exposes removed concurrency option %q", removed)
		}
	}
}

func TestLimitsScope(t *testing.T) {
	g, err := newGenerator(mutableTree(t), "")
	if err != nil {
		t.Fatal(err)
	}
	page, err := g.renderLimits()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(page, "They do not bound result sizes, dataset lifetime or concurrency; bound those yourself.") {
		t.Error("limits reference must distinguish resource settings from caller-owned bounds")
	}
}

func TestParenthesizedDefault(t *testing.T) {
	root := mutableTree(t)
	edit(t, root, "java/"+javaMain+"/Limits.java", "DEFAULT_PAGE_BYTES = 64 * MIB", "DEFAULT_PAGE_BYTES = (64 * MIB)")
	if got := checkErr(t, root); got != "" {
		t.Fatal(got)
	}
}

func TestJavaPublicOwners(t *testing.T) {
	j := &javaSource{types: map[string]*javaType{}, constants: map[string]string{}}
	err := j.parse("", `public class Limits {
		private long pageBytes;
		public long pageBytes() { return pageBytes; }
		public static class Builder {
			private long pageBytes;
			private Builder pageBytes(long n) { return this; }
			public Builder other(long n) { return this; }
		}
		public record Stats(long rows, long bytes) {}
		private static class Hidden { public void other() {} }
	}`)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]bool{
		"Limits.pageBytes": true, "Limits.Builder.pageBytes": false,
		"Limits.Builder.other": true, "Limits.other": false,
		"Limits.Stats.rows": true, "Limits.Stats.bytes": true,
		"Limits.Hidden": false, "Limits.Hidden.other": false,
	} {
		if got := j.has(name); got != want {
			t.Errorf("has(%s) = %v, want %v", name, got, want)
		}
	}
}

func TestDeterministicChecksAndAliases(t *testing.T) {
	g, err := newGenerator(mutableTree(t), "")
	if err != nil {
		t.Fatal(err)
	}
	p := g.goPkgs[""]
	p.consts["ZAlias"] = goConst{"ZAlias", "RefusalCode", "no_manifest"}
	p.consts["AAlias"] = goConst{"AAlias", "RefusalCode", "no_manifest"}
	delete(p.consts, "LimitParts")
	delete(p.consts, "LimitGetRows")
	want := g.check().Error()
	for i := 0; i < 20; i++ {
		if got := g.check().Error(); got != want {
			t.Fatalf("error ordering changed:\n%s\nwant:\n%s", got, want)
		}
		if got := g.goConstFor("RefusalCode", "no_manifest"); got != "AAlias" {
			t.Fatalf("alias selection = %s", got)
		}
	}
}

func TestMarkdownFilesTrackedOnly(t *testing.T) {
	root := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}, {"add", "tracked.md"}} {
		if args[0] == "add" {
			for _, name := range []string{"tracked.md", "untracked.md"} {
				if err := os.WriteFile(filepath.Join(root, name), []byte("# Example\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
		}
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %s: %v", args, out, err)
		}
	}
	g := &generator{root: root}
	got, err := g.markdownFiles()
	if err != nil || len(got) != 1 || got[0] != "tracked.md" {
		t.Fatalf("markdownFiles = %v, %v", got, err)
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

// Pending parity names must not suppress checks of Go or unrelated Java
// facts, and an explicit Java checkout must check the pending names too.
func TestPendingJavaParityRows(t *testing.T) {
	root := mutableTree(t)
	if got := checkErr(t, root); got != "" {
		t.Fatal(got)
	}
	g, err := newGenerator(root, filepath.Join(root, "java"))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.check(); err == nil || !strings.Contains(err.Error(), "no public ListEntry.rows") {
		t.Fatalf("explicit Java check did not check pending rows: %v", err)
	}
	edit(t, root, "docs/data/parity.json", `"go": "Plan.ScanValues"`, `"go": "Plan.NoSuchMethod"`)
	if got := checkErr(t, root); !strings.Contains(got, "Go has no Plan.NoSuchMethod") {
		t.Fatalf("pending row bypassed Go check: %s", got)
	}
}

func TestInputRowSpansParity(t *testing.T) {
	g, err := newGenerator(mutableTree(t), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := g.check(); err != nil {
		t.Fatal(err)
	}
	page, err := g.renderParity()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(page, "| Input row intervals before union normalization | `Plan.InputRowSpans` | `Plan.inputRowSpans` | Java pending") {
		t.Error("parity reference must use InputRowSpans and its pending Java counterpart")
	}
}
