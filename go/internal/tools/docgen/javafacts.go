// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// javaSource is the public API the Java reader's main sources declare,
// read from source text without compiling: public types (top-level and
// nested, as "Outer.Inner", prefixed "storage." in that subpackage) and
// their public members, by owner.
type javaSource struct {
	types map[string]*javaType
	// constants are each source file's static final fields with their
	// initializer text, for defaults: "Limits.DEFAULT_PAGE_BYTES" → "64 * MIB".
	constants map[string]string
}

type javaType struct {
	name    string // "Limits.Builder"
	kind    string // class, interface, record or enum
	members map[string]bool
	// components are a record's components, in order.
	components []string
	// enum constants with their string argument, if any.
	enumValues map[string]string
}

// javaMain is where the reader's sources are, relative to the java/
// directory.
const javaMain = "seekparquet/src/main/java/io/varve/seekparquet"

func loadJava(javaDir string) (*javaSource, error) {
	base := filepath.Join(javaDir, filepath.FromSlash(javaMain))
	j := &javaSource{types: map[string]*javaType{}, constants: map[string]string{}}
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".java") {
			return err
		}
		rel, err := filepath.Rel(base, path)
		if err != nil {
			return err
		}
		prefix := ""
		if dir := filepath.ToSlash(filepath.Dir(rel)); dir != "." {
			prefix = strings.ReplaceAll(dir, "/", ".") + "."
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return j.parse(prefix, string(src))
	})
	if err != nil {
		return nil, err
	}
	if len(j.types) == 0 {
		return nil, fmt.Errorf("no Java sources under %s", base)
	}
	return j, nil
}

var (
	javaTypeDecl  = regexp.MustCompile(`\b(class|interface|record|enum)\s+([A-Z]\w*)`)
	javaMethod    = regexp.MustCompile(`(\w+)\s*\([^()]*(?:\([^()]*\)[^()]*)*\)\s*(?:throws\s+[\w.,\s]+)?$`)
	javaField     = regexp.MustCompile(`(\w+)\s*(?:=.*)?$`)
	javaModifiers = regexp.MustCompile(`\b(public|private|protected|static|final|abstract|default|sealed|non-sealed|synchronized)\b`)
	javaEnumConst = regexp.MustCompile(`^\s*([A-Z][A-Z0-9_]*)\s*(?:\(\s*"([^"]*)"\s*\))?\s*$`)
)

// blank replaces comments and the contents of string and character
// literals with spaces, so that braces and semicolons in them are not
// structure. String literal contents are kept for enum arguments: only
// their delimiting quotes matter to the parser, so they are kept but any
// brace or semicolon inside them is blanked.
func blank(src string) string {
	b := []byte(src)
	for i := 0; i < len(b); i++ {
		switch {
		case b[i] == '/' && i+1 < len(b) && b[i+1] == '/':
			for ; i < len(b) && b[i] != '\n'; i++ {
				b[i] = ' '
			}
		case b[i] == '/' && i+1 < len(b) && b[i+1] == '*':
			for ; i+1 < len(b) && !(b[i] == '*' && b[i+1] == '/'); i++ {
				if b[i] != '\n' {
					b[i] = ' '
				}
			}
			if i+1 < len(b) {
				b[i], b[i+1] = ' ', ' '
				i++
			}
		case b[i] == '"' && strings.HasPrefix(string(b[i:]), `"""`):
			for i += 3; i+2 < len(b) && string(b[i:i+3]) != `"""`; i++ {
				if b[i] != '\n' {
					b[i] = ' '
				}
			}
			i += 2
		case b[i] == '"' || b[i] == '\'':
			q := b[i]
			for i++; i < len(b) && b[i] != q; i++ {
				if b[i] == '\\' {
					b[i] = ' '
					i++
					b[i] = ' '
					continue
				}
				if strings.IndexByte("{};()", b[i]) >= 0 {
					b[i] = ' '
				}
			}
		}
	}
	return string(b)
}

// parse records the public types and members of one source file.
func (j *javaSource) parse(prefix, src string) error {
	src = blank(src)
	var stack []javaFrame
	var decl strings.Builder
	top := func() *javaFrame {
		if len(stack) == 0 {
			return nil
		}
		return &stack[len(stack)-1]
	}
	addMember := func(f *javaFrame, text string, terminator byte) {
		text = strings.TrimSpace(text)
		if f == nil || f.t == nil || text == "" || strings.HasPrefix(text, "@") && !strings.Contains(text, " ") {
			return
		}
		// Drop annotations.
		text = strings.TrimSpace(regexp.MustCompile(`@\w+(\([^)]*\))?`).ReplaceAllString(text, ""))
		mods := javaModifiers.FindAllString(text, -1)
		has := func(m string) bool {
			for _, x := range mods {
				if x == m {
					return true
				}
			}
			return false
		}
		eq, paren := strings.Index(text, "="), strings.Index(text, "(")
		isField := eq >= 0 && (paren < 0 || eq < paren)
		if isField && has("static") && has("final") {
			head, init, _ := strings.Cut(text, "=")
			if m := javaField.FindStringSubmatch(strings.TrimSpace(head)); m != nil {
				j.constants[f.t.name+"."+m[1]] = strings.TrimSpace(init)
			}
		}
		visible := has("public") || (f.t.kind == "interface" && !has("private"))
		if !f.public || !visible {
			return
		}
		if m := javaMethod.FindStringSubmatch(text); m != nil && !isField {
			f.t.members[m[1]] = true
			return
		}
		if terminator == ';' || terminator == '{' {
			head, _, _ := strings.Cut(text, "=")
			if m := javaField.FindStringSubmatch(strings.TrimSpace(head)); m != nil {
				f.t.members[m[1]] = true
			}
		}
	}
	for i := 0; i < len(src); i++ {
		c := src[i]
		switch c {
		case '{':
			text := decl.String()
			decl.Reset()
			f := top()
			inBody := f == nil || f.t != nil
			if m := javaTypeDecl.FindStringSubmatch(text); inBody && m != nil && !strings.Contains(text, "=") && !strings.Contains(text, "new ") {
				name := m[2]
				if f != nil && f.t != nil {
					name = f.t.name + "." + name
				} else {
					name = prefix + name
				}
				mods := javaModifiers.FindAllString(text[:strings.Index(text, m[0])], -1)
				public := false
				for _, x := range mods {
					if x == "public" {
						public = true
					}
				}
				if f != nil && f.t != nil && f.t.kind == "interface" {
					public = true
				}
				if f != nil && !f.public {
					public = false
				}
				t := &javaType{name: name, kind: m[1], members: map[string]bool{}, enumValues: map[string]string{}}
				if m[1] == "record" {
					if open := strings.Index(text, "("); open >= 0 {
						if end := strings.LastIndex(text, ")"); end > open {
							for _, comp := range splitTop(text[open+1 : end]) {
								fields := strings.Fields(comp)
								if len(fields) > 0 {
									n := fields[len(fields)-1]
									t.components = append(t.components, n)
									t.members[n] = true
								}
							}
						}
					}
				}
				if public {
					j.types[name] = t
				}
				stack = append(stack, javaFrame{t: t, public: public, enum: m[1] == "enum"})
				continue
			}
			if f != nil && f.t != nil {
				if f.enum {
					// "CONSTANT {": a constant with a body; the list goes on after it.
					parseEnumConstants(f.t, text)
				} else {
					addMember(f, text, '{')
				}
			}
			stack = append(stack, javaFrame{})
		case '}':
			if f := top(); f != nil && f.enum {
				parseEnumConstants(f.t, decl.String())
				f.enum = false
			}
			decl.Reset()
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		case ';':
			text := decl.String()
			decl.Reset()
			f := top()
			if f != nil && f.enum {
				parseEnumConstants(f.t, text)
				f.enum = false
				continue
			}
			addMember(f, text, ';')
		default:
			decl.WriteByte(c)
		}
	}
	return nil
}

// javaFrame is one open brace: a type body, or another block (t nil).
type javaFrame struct {
	t      *javaType
	public bool // members of this type body are visible
	enum   bool // still in an enum's constant list
}

// parseEnumConstants records the constants in text, a part of an enum's
// constant list.
func parseEnumConstants(t *javaType, text string) {
	text = regexp.MustCompile(`@\w+(\([^)]*\))?`).ReplaceAllString(text, "")
	for _, item := range splitTop(text) {
		if m := javaEnumConst.FindStringSubmatch(item); m != nil {
			t.enumValues[m[1]] = m[2]
			t.members[m[1]] = true
		}
	}
}

// splitTop splits s at commas outside angle brackets and parentheses.
func splitTop(s string) []string {
	var out []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '<', '(':
			depth++
		case '>', ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}

// has reports whether a public Java type or member exists: "Class",
// "Class.member", "Class.Nested" or "Class.Nested.member", optionally
// under a subpackage ("storage.LocalStorage").
func (j *javaSource) has(name string) bool {
	if _, ok := j.types[name]; ok {
		return true
	}
	i := strings.LastIndex(name, ".")
	if i < 0 {
		return false
	}
	t, ok := j.types[name[:i]]
	return ok && t.members[name[i+1:]]
}

// publicMembers returns a type's public members, sorted.
func (j *javaSource) publicMembers(class string) ([]string, error) {
	t, ok := j.types[class]
	if !ok {
		return nil, fmt.Errorf("java: no public type %s", class)
	}
	var out []string
	for m := range t.members {
		out = append(out, m)
	}
	sort.Strings(out)
	return out, nil
}

// recordComponents returns a public record's components.
func (j *javaSource) recordComponents(class string) ([]string, error) {
	t, ok := j.types[class]
	if !ok || t.kind != "record" {
		return nil, fmt.Errorf("java: no public record %s", class)
	}
	return t.components, nil
}

// enumValues returns a public enum's constants and, for constants declared
// with a string argument, that argument.
func (j *javaSource) enumValues(class string) (map[string]string, error) {
	t, ok := j.types[class]
	if !ok || t.kind != "enum" {
		return nil, fmt.Errorf("java: no public enum %s", class)
	}
	if len(t.enumValues) == 0 {
		return nil, fmt.Errorf("java: enum %s declares no constants the parser recognizes", class)
	}
	return t.enumValues, nil
}

// limitDefault evaluates Limits.DEFAULT_<NAME>. It fails when the constant
// is missing or its initializer is not an expression evalJava supports, so
// that a default the check cannot read never passes silently.
func (j *javaSource) limitDefault(name string) (int64, error) {
	init, ok := j.constants["Limits.DEFAULT_"+name]
	if !ok {
		return 0, fmt.Errorf("java: Limits has no public constant DEFAULT_%s", name)
	}
	v, ok := evalJava(init, j.constants)
	if !ok {
		return 0, fmt.Errorf("java: cannot evaluate Limits.DEFAULT_%s = %s; extend evalJava", name, init)
	}
	return v, nil
}

// evalJava evaluates an integer constant expression of decimal literals
// (with _ and an L suffix), parentheses, *, << and +, and Limits constants
// (MIB).
func evalJava(expr string, consts map[string]string) (int64, bool) {
	expr = strings.TrimSpace(expr)
	for strings.HasPrefix(expr, "(") && matchingParen(expr) == len(expr)-1 {
		expr = strings.TrimSpace(expr[1 : len(expr)-1])
	}
	for _, op := range []string{"+", "<<", "*"} {
		if i := topLevelIndex(expr, op); i >= 0 {
			x, ok1 := evalJava(expr[:i], consts)
			y, ok2 := evalJava(expr[i+len(op):], consts)
			if !ok1 || !ok2 {
				return 0, false
			}
			switch op {
			case "+":
				return x + y, true
			case "<<":
				return x << y, true
			}
			return x * y, true
		}
	}
	if regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`).MatchString(expr) {
		init, ok := consts["Limits."+expr]
		if !ok {
			return 0, false
		}
		return evalJava(init, consts)
	}
	lit := strings.TrimSuffix(strings.ReplaceAll(expr, "_", ""), "L")
	v, err := strconv.ParseInt(lit, 10, 64)
	return v, err == nil
}

func matchingParen(s string) int {
	depth := 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// topLevelIndex returns the last index of op outside parentheses, or -1.
func topLevelIndex(s, op string) int {
	depth := 0
	found := -1
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && strings.HasPrefix(s[i:], op) {
				found = i
			}
		}
	}
	return found
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
