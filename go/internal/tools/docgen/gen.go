// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// generator renders the generated documentation of the repository at
// root. Every path it handles is slash-separated and relative to root.
type generator struct {
	root string
	// goPkgs are the parsed Go packages by import suffix: "" for the root
	// package, "write" and "local".
	goPkgs map[string]*goPackage
	// java is the Java source to check names against, nil while
	// parity.json marks the Java names pending and no override was given.
	java *javaSource

	limits  limitsData
	stats   statsData
	actions actionsData
	parity  parityData
	codes   specCodes
	status  specStatus
}

type limitsData struct {
	Description string `json:"description"`
	Structs     []struct {
		Go     string `json:"go"`
		Java   string `json:"java"`
		Fields []struct {
			Go      string `json:"go"`
			Java    string `json:"java"`
			Kind    string `json:"kind"`
			Unit    string `json:"unit"`
			Derived bool   `json:"derived"`
			Action  string `json:"action"`
		} `json:"fields"`
	} `json:"structs"`
	Fixed []struct {
		Go     string `json:"go"`
		Name   string `json:"name"`
		Java   string `json:"java"`
		Value  string `json:"value"`
		Action string `json:"action"`
	} `json:"fixed"`
}

type statsData struct {
	Description string `json:"description"`
	Fields      []struct {
		Go   string  `json:"go"`
		Java *string `json:"java"`
	} `json:"fields"`
}

type action struct {
	Cause  string `json:"cause"`
	Action string `json:"action"`
}

type actionsData struct {
	Description string            `json:"description"`
	Refusals    map[string]action `json:"refusals"`
	Unsupported map[string]action `json:"unsupported"`
}

type parityData struct {
	Description string `json:"description"`
	JavaPending string `json:"java_pending"`
	Sections    []struct {
		Title string `json:"title"`
		Rows  []struct {
			Concept string  `json:"concept"`
			Go      *string `json:"go"`
			Java    *string `json:"java"`
			Note    string  `json:"note"`
		} `json:"rows"`
	} `json:"sections"`
}

type specCode struct {
	Code         string   `json:"code"`
	Meaning      string   `json:"meaning"`
	Requirements []string `json:"requirements"`
	Detect       string   `json:"detect"`
}

type specCodes struct {
	Codes       []specCode `json:"codes"`
	NonRefusals []struct {
		Status       string     `json:"status"`
		Meaning      string     `json:"meaning"`
		Requirements []string   `json:"requirements"`
		Codes        []specCode `json:"codes"`
	} `json:"non_refusals"`
}

type specStatus struct {
	Status              string `json:"status"`
	TargetFormatVersion int    `json:"target_format_version"`
	DraftRevision       int    `json:"draft_revision"`
	OnDisk              struct {
		FormatVersion int `json:"format_version"`
		DraftRevision int `json:"draft_revision"`
	} `json:"on_disk"`
	ConformanceRevision string `json:"conformance_revision"`
}

// newGenerator loads the data files and parses the sources. javaDir, when
// set, is a java/ directory to check Java names against even while they
// are marked pending.
func newGenerator(root, javaDir string) (*generator, error) {
	g := &generator{root: root, goPkgs: map[string]*goPackage{}}
	for file, v := range map[string]any{
		"docs/data/limits.json":          &g.limits,
		"docs/data/read-stats.json":      &g.stats,
		"docs/data/refusal-actions.json": &g.actions,
		"docs/data/parity.json":          &g.parity,
		"spec/refusal-codes.json":        &g.codes,
		"spec/status.json":               &g.status,
	} {
		if err := g.readJSON(file, v); err != nil {
			return nil, err
		}
	}
	for _, pkg := range []string{"", "write", "local"} {
		p, err := parseGoPackage(filepath.Join(root, "go", pkg))
		if err != nil {
			return nil, err
		}
		g.goPkgs[pkg] = p
	}
	switch {
	case javaDir != "":
		j, err := loadJava(javaDir)
		if err != nil {
			return nil, err
		}
		g.java = j
	case g.parity.JavaPending == "":
		j, err := loadJava(filepath.Join(root, "java"))
		if err != nil {
			return nil, err
		}
		g.java = j
	}
	return g, nil
}

func (g *generator) readJSON(file string, v any) error {
	raw, err := os.ReadFile(filepath.Join(g.root, filepath.FromSlash(file)))
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if strings.HasPrefix(file, "docs/data/") {
		dec.DisallowUnknownFields() // a misspelled member must not pass silently
	}
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%s: %w", file, err)
	}
	return nil
}

// generatedFiles are the files rendered whole.
func (g *generator) generatedFiles() map[string]func() (string, error) {
	return map[string]func() (string, error){
		"docs/reference/limits.md":        g.renderLimits,
		"docs/reference/read-stats.md":    g.renderReadStats,
		"docs/reference/refusal-codes.md": g.renderRefusalCodes,
		"docs/reference/parity.md":        g.renderParity,
	}
}

// regions are the generated regions a hand-written page may contain, by
// name; each renders for the page at the given path.
func (g *generator) regions() map[string]func(page string) (string, error) {
	return map[string]func(string) (string, error){
		"status":                      g.renderStatus,
		"troubleshooting-refusals":    g.renderTroubleRefusals,
		"troubleshooting-unsupported": g.renderTroubleUnsupported,
		"troubleshooting-limits":      g.renderTroubleLimits,
	}
}

// run renders everything and compares it with the files on disk. With
// write, it writes the files that differ. It returns the paths that
// differed.
func (g *generator) run(write bool) ([]string, error) {
	if err := g.check(); err != nil {
		return nil, err
	}
	want := map[string]string{}
	for file, render := range g.generatedFiles() {
		text, err := render()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		want[file] = text
	}
	pages, err := g.markdownFiles()
	if err != nil {
		return nil, err
	}
	for _, page := range pages {
		if _, ok := want[page]; ok {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(g.root, filepath.FromSlash(page)))
		if err != nil {
			return nil, err
		}
		text, err := g.fill(page, string(raw))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", page, err)
		}
		want[page] = text
	}
	var changed []string
	for file, text := range want {
		full := filepath.Join(g.root, filepath.FromSlash(file))
		old, err := os.ReadFile(full)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		if string(old) == text {
			continue
		}
		changed = append(changed, file)
		if write {
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				return nil, err
			}
			if err := os.WriteFile(full, []byte(text), 0o644); err != nil {
				return nil, err
			}
		}
	}
	sort.Strings(changed)
	return changed, nil
}

// skipDirs are directories never searched for Markdown.
var skipDirs = map[string]bool{".git": true, "build": true, ".gradle": true, "node_modules": true, ".venv": true, "datasets": true}

// markdownFiles returns every Markdown file of the repository.
func (g *generator) markdownFiles() ([]string, error) {
	var out []string
	err := filepath.WalkDir(g.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != g.root && (skipDirs[d.Name()] || strings.HasPrefix(d.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(p, ".md") {
			rel, err := filepath.Rel(g.root, p)
			if err != nil {
				return err
			}
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

var marker = regexp.MustCompile(`^<!-- (snippet|generated): ([^ ]+) -->[ \t]*$`)

// fill replaces the body of every snippet and generated region of the
// page. Markers inside fenced code blocks are text, not markers.
func (g *generator) fill(page, text string) (string, error) {
	lines := strings.SplitAfter(text, "\n")
	var out strings.Builder
	fenced := false
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		out.WriteString(line)
		trimmed := strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(strings.TrimSpace(trimmed), "```") {
			fenced = !fenced
			continue
		}
		m := marker.FindStringSubmatch(trimmed)
		if fenced || m == nil {
			continue
		}
		kind, arg := m[1], m[2]
		end := "<!-- /" + kind + " -->"
		j := i + 1
		for j < len(lines) && strings.TrimRight(lines[j], "\r\n") != end {
			j++
		}
		if j == len(lines) {
			return "", fmt.Errorf("%s %s: no %s on a line of its own", kind, arg, end)
		}
		var body string
		var err error
		if kind == "snippet" {
			body, err = g.snippet(arg)
		} else {
			render, ok := g.regions()[arg]
			if !ok {
				return "", fmt.Errorf("unknown generated region %q", arg)
			}
			body, err = render(page)
		}
		if err != nil {
			return "", fmt.Errorf("%s %s: %w", kind, arg, err)
		}
		if body != "" && !strings.HasSuffix(body, "\n") {
			body += "\n"
		}
		out.WriteString(body)
		out.WriteString(lines[j])
		i = j
	}
	return out.String(), nil
}

// snippet renders a fenced block of the snippet "file#name": a Go
// function's body, a tagged region (// tag::name[] to // end::name[]) of
// any other file, or a whole file when there is no name.
func (g *generator) snippet(ref string) (string, error) {
	file, name, _ := strings.Cut(ref, "#")
	full := filepath.Join(g.root, filepath.FromSlash(file))
	lang := strings.TrimPrefix(path.Ext(file), ".")
	if lang == "py" {
		lang = "python"
	}
	var code string
	switch {
	case name == "":
		raw, err := os.ReadFile(full)
		if err != nil {
			return "", err
		}
		code = string(raw)
		lang = "text"
	case lang == "go":
		var err error
		code, err = goFuncBody(full, name)
		if err != nil {
			return "", err
		}
	default:
		var err error
		code, err = taggedRegion(full, name)
		if err != nil {
			return "", err
		}
	}
	if !strings.HasSuffix(code, "\n") {
		code += "\n"
	}
	return "```" + lang + "\n" + code + "```\n", nil
}

// taggedRegion returns the lines between "tag::name[]" and "end::name[]"
// comments, without their common indentation.
func taggedRegion(file, name string) (string, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}
	var lines []string
	in, found := false, false
	for _, line := range strings.Split(string(raw), "\n") {
		switch {
		case strings.Contains(line, "tag::"+name+"[]"):
			in, found = true, true
		case strings.Contains(line, "end::"+name+"[]"):
			in = false
		case in:
			lines = append(lines, line)
		}
	}
	if !found {
		return "", fmt.Errorf("%s: no region %s", file, name)
	}
	indent := -1
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		n := len(line) - len(strings.TrimLeft(line, " \t"))
		if indent < 0 || n < indent {
			indent = n
		}
	}
	for i, line := range lines {
		if len(line) >= indent && indent > 0 {
			lines[i] = line[indent:]
		}
	}
	return trimBlankLines(lines), nil
}

// goSymbol reports whether a Go name exists: "Name", "Type.Member",
// "write.Name" or "local.Type.Member".
func (g *generator) goSymbol(name string) bool {
	pkg := ""
	if p, rest, ok := strings.Cut(name, "."); ok && (p == "write" || p == "local") {
		pkg, name = p, rest
	}
	return g.goPkgs[pkg].symbols[name]
}

// check compares the data files with the code, both ways.
func (g *generator) check() error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	root := g.goPkgs[""]

	// Limits: every exported field and every LimitName is described.
	described := map[string]bool{}
	for _, s := range g.limits.Structs {
		fields, ok := root.fields[s.Go]
		if !ok {
			fail("limits.json: Go has no struct %s", s.Go)
			continue
		}
		listed := map[string]bool{}
		for _, f := range s.Fields {
			listed[f.Go] = true
			described[f.Go] = true
		}
		for _, f := range fields {
			if !listed[f.name] {
				fail("limits.json: %s.%s is not described; add it", s.Go, f.name)
			}
			delete(listed, f.name)
		}
		for name := range listed {
			fail("limits.json: Go has no field %s.%s", s.Go, name)
		}
	}
	for _, f := range g.limits.Fixed {
		described[f.Name] = true
		if c, ok := root.consts[f.Go]; !ok || c.typ != "LimitName" || c.value != f.Name {
			fail("limits.json: Go has no LimitName %s = %q", f.Go, f.Name)
		}
	}
	for _, c := range root.consts {
		if c.typ == "LimitName" && !described[c.value] {
			fail("limits.json: LimitName %s (%q) is not described", c.name, c.value)
		}
	}

	// ReadStats.
	stats := map[string]bool{}
	for _, f := range g.stats.Fields {
		stats[f.Go] = true
	}
	for _, f := range root.fields["ReadStats"] {
		if !stats[f.name] {
			fail("read-stats.json: ReadStats.%s is not described; add it", f.name)
		}
		delete(stats, f.name)
	}
	for name := range stats {
		fail("read-stats.json: Go has no field ReadStats.%s", name)
	}

	// Codes: spec, Go constants and actions agree.
	checkCodes := func(kind, goType string, spec []specCode, actions map[string]action) {
		want := map[string]bool{}
		for _, c := range spec {
			want[c.Code] = true
			if _, ok := actions[c.Code]; !ok {
				fail("refusal-actions.json: %s code %s has no action", kind, c.Code)
			}
		}
		for code := range actions {
			if !want[code] {
				fail("refusal-actions.json: %s is not a %s code of spec/refusal-codes.json", code, kind)
			}
		}
		got := map[string]bool{}
		for _, c := range root.consts {
			if c.typ == goType {
				got[c.value] = true
			}
		}
		if !mapsEqual(want, got) {
			fail("Go %s constants %v differ from spec/refusal-codes.json %v", goType, sortedKeys(got), sortedKeys(want))
		}
	}
	checkCodes("refusal", "RefusalCode", g.codes.Codes, g.actions.Refusals)
	checkCodes("unsupported", "UnsupportedCode", g.unsupportedCodes(), g.actions.Unsupported)

	// Parity: every Go name exists.
	for _, s := range g.parity.Sections {
		for _, r := range s.Rows {
			if r.Go != nil && !strings.HasPrefix(*r.Go, "std:") && !g.goSymbol(*r.Go) {
				fail("parity.json: Go has no %s", *r.Go)
			}
		}
	}

	if g.java != nil {
		errs = append(errs, g.checkJava()...)
	}
	return errors.Join(errs...)
}

// checkJava checks the Java names and defaults the docs give.
func (g *generator) checkJava() []error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf("java: "+format, args...)) }
	for _, s := range g.parity.Sections {
		for _, r := range s.Rows {
			if r.Java != nil && !strings.HasPrefix(*r.Java, "std:") && !g.java.has(*r.Java) {
				fail("parity.json: no %s", *r.Java)
			}
		}
	}
	for _, f := range g.stats.Fields {
		if f.Java != nil && !g.java.has("ReadStats."+*f.Java) {
			fail("read-stats.json: no ReadStats.%s", *f.Java)
		}
	}
	defaults, err := g.java.limitDefaults()
	if err != nil {
		return append(errs, err)
	}
	names := map[string]bool{}
	for _, s := range g.limits.Structs {
		for _, f := range s.Fields {
			names[upperSnake(f.Java)] = true
			if !g.java.has(s.Java + "." + f.Java) {
				fail("limits.json: no %s.%s", s.Java, f.Java)
			}
			if f.Derived {
				continue
			}
			goDefault, ok := g.goPkgs[""].defaults[s.Go][f.Go]
			javaDefault, jok := defaults[upperSnake(f.Java)]
			if ok && jok && goDefault != javaDefault {
				fail("%s default %d differs from Go's %d", f.Java, javaDefault, goDefault)
			}
		}
	}
	for _, f := range g.limits.Fixed {
		names[f.Java] = true
	}
	got, err := g.java.enumConstants("LimitName")
	if err != nil {
		return append(errs, err)
	}
	if !mapsEqual(names, keysOf(got)) {
		fail("LimitName constants %v differ from the documented %v", sortedKeys(keysOf(got)), sortedKeys(names))
	}
	for class, spec := range map[string][]specCode{"RefusalCode": g.codes.Codes, "UnsupportedCode": g.unsupportedCodes()} {
		consts, err := g.java.enumConstants(class)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		want, have := map[string]bool{}, map[string]bool{}
		for _, c := range spec {
			want[c.Code] = true
		}
		for _, v := range consts {
			have[v] = true
		}
		if !mapsEqual(want, have) {
			fail("%s codes %v differ from spec/refusal-codes.json %v", class, sortedKeys(have), sortedKeys(want))
		}
	}
	return errs
}

func (g *generator) unsupportedCodes() []specCode {
	for _, n := range g.codes.NonRefusals {
		if n.Status == "unsupported" {
			return n.Codes
		}
	}
	return nil
}

func mapsEqual(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func keysOf(m map[string]string) map[string]bool {
	out := map[string]bool{}
	for k := range m {
		out[k] = true
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
