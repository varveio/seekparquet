// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// javaSource indexes the Java reader's main sources by class name, for
// checking that the names the docs give exist. It reads source text; it
// does not compile anything.
type javaSource struct {
	files map[string]string // "Dataset" or "storage.LocalStorage" → source without comments
}

// javaMain is where the reader's sources are, relative to the java/
// directory.
const javaMain = "seekparquet/src/main/java/io/varve/seekparquet"

func loadJava(javaDir string) (*javaSource, error) {
	base := filepath.Join(javaDir, filepath.FromSlash(javaMain))
	j := &javaSource{files: map[string]string{}}
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".java") {
			return err
		}
		rel, err := filepath.Rel(base, path)
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(filepath.ToSlash(rel), ".java")
		name = strings.ReplaceAll(name, "/", ".")
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		j.files[name] = stripJavaComments(string(src))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(j.files) == 0 {
		return nil, fmt.Errorf("no Java sources under %s", base)
	}
	return j, nil
}

var javaComment = regexp.MustCompile(`(?s)/\*.*?\*/|//[^\n]*`)

func stripJavaComments(src string) string { return javaComment.ReplaceAllString(src, "") }

// has reports whether a Java name exists: "Class", "Class.member" or
// "Class.Nested", optionally under a subpackage ("storage.LocalStorage").
// A member is any identifier the class's source declares or uses, which
// is loose on purpose: the check catches renames and removals, not
// signatures.
func (j *javaSource) has(name string) bool {
	parts := strings.Split(name, ".")
	for i := len(parts); i > 0; i-- {
		class := strings.Join(parts[:i], ".")
		src, ok := j.files[class]
		if !ok {
			continue
		}
		for _, member := range parts[i:] {
			if !regexp.MustCompile(`\b` + regexp.QuoteMeta(member) + `\b`).MatchString(src) {
				return false
			}
		}
		return true
	}
	return false
}

var javaEnumConstant = regexp.MustCompile(`(?m)^\s*([A-Z][A-Z0-9_]*)\s*(?:\(\s*"([^"]*)"\s*\))?\s*[,;]?\s*$`)

// enumConstants returns an enum's constants and, for constants declared
// with a string argument, that argument.
func (j *javaSource) enumConstants(class string) (map[string]string, error) {
	src, ok := j.files[class]
	if !ok {
		return nil, fmt.Errorf("java: no class %s", class)
	}
	open := strings.Index(src, "{")
	if open < 0 {
		return nil, fmt.Errorf("java: %s has no body", class)
	}
	body := src[open+1:]
	if end := strings.Index(body, ";"); end >= 0 {
		body = body[:end+1]
	}
	out := map[string]string{}
	for _, m := range javaEnumConstant.FindAllStringSubmatch(body, -1) {
		out[m[1]] = m[2]
	}
	return out, nil
}

var javaDefault = regexp.MustCompile(`DEFAULT_([A-Z0-9_]+)\s*=\s*([^;]+);`)

// limitDefaults returns Limits.java's DEFAULT_* constants that are integer
// expressions of literals, MIB, * and <<.
func (j *javaSource) limitDefaults() (map[string]int64, error) {
	src, ok := j.files["Limits"]
	if !ok {
		return nil, fmt.Errorf("java: no class Limits")
	}
	out := map[string]int64{}
	for _, m := range javaDefault.FindAllStringSubmatch(src, -1) {
		if v, ok := evalJava(m[2]); ok {
			out[m[1]] = v
		}
	}
	return out, nil
}

func evalJava(expr string) (int64, bool) {
	expr = strings.TrimSpace(expr)
	for _, op := range []string{"<<", "*"} {
		if a, b, ok := strings.Cut(expr, op); ok {
			x, ok1 := evalJava(a)
			y, ok2 := evalJava(b)
			if !ok1 || !ok2 {
				return 0, false
			}
			if op == "<<" {
				return x << y, true
			}
			return x * y, true
		}
	}
	if expr == "MIB" {
		return 1 << 20, true
	}
	expr = strings.TrimSuffix(strings.ReplaceAll(expr, "_", ""), "L")
	v, err := strconv.ParseInt(expr, 10, 64)
	return v, err == nil
}

// upperSnake turns a Java or Go camel-case name into UPPER_SNAKE_CASE.
func upperSnake(name string) string {
	var b strings.Builder
	for i, r := range name {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return strings.ToUpper(b.String())
}
