// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// goPackage is what the generator needs from one Go package's non-test
// source: its exported names, typed string constants, struct field
// comments, integer constants and the defaults Limits.withDefaults sets.
type goPackage struct {
	symbols    map[string]bool             // "Name" and "Type.Member"
	consts     map[string]goConst          // typed constants with string values
	ints       map[string]int64            // untyped or int constants with constant values
	fields     map[string][]goField        // struct type → fields in order
	defaults   map[string]map[string]int64 // type → field → constant default
	defaultSet map[string]map[string]bool  // type → field → has a default, constant or not
	// errs are declarations of documented constant types in a form the
	// parser does not read; they fail the check rather than go unseen.
	errs []error
}

// codeTypes are the string types whose constants the docs enumerate.
var codeTypes = map[string]bool{"RefusalCode": true, "UnsupportedCode": true, "LimitName": true}

type goConst struct {
	name, typ, value string
}

type goField struct {
	name string
	doc  string
}

// parseGoPackage parses the non-test .go files of dir.
func parseGoPackage(dir string) (*goPackage, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	p := &goPackage{
		symbols: map[string]bool{}, consts: map[string]goConst{}, ints: map[string]int64{},
		fields: map[string][]goField{}, defaults: map[string]map[string]int64{}, defaultSet: map[string]map[string]bool{},
	}
	fset := token.NewFileSet()
	found := false
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ParseComments)
		if err != nil {
			return nil, err
		}
		found = true
		p.add(f)
	}
	if !found {
		return nil, fmt.Errorf("no Go source in %s", dir)
	}
	return p, nil
}

func (p *goPackage) add(f *ast.File) {
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil {
				p.symbols[d.Name.Name] = true
				continue
			}
			recv := receiverName(d.Recv.List[0].Type)
			p.symbols[recv+"."+d.Name.Name] = true
			if d.Name.Name == "withDefaults" {
				p.addDefaults(recv, d.Body)
			}
		case *ast.GenDecl:
			p.addGen(d)
		}
	}
}

func receiverName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return receiverName(t.X)
	case *ast.IndexExpr:
		return receiverName(t.X)
	case *ast.IndexListExpr:
		return receiverName(t.X)
	case *ast.Ident:
		return t.Name
	}
	return ""
}

func (p *goPackage) addGen(d *ast.GenDecl) {
	lastType := ""
	for _, spec := range d.Specs {
		switch s := spec.(type) {
		case *ast.TypeSpec:
			p.symbols[s.Name.Name] = true
			switch t := s.Type.(type) {
			case *ast.StructType:
				for _, f := range t.Fields.List {
					doc := commentText(f.Doc)
					if len(f.Names) == 0 {
						name := receiverName(f.Type)
						p.symbols[s.Name.Name+"."+name] = true
						continue
					}
					for _, n := range f.Names {
						p.symbols[s.Name.Name+"."+n.Name] = true
						if n.IsExported() {
							p.fields[s.Name.Name] = append(p.fields[s.Name.Name], goField{n.Name, doc})
						}
					}
				}
			case *ast.InterfaceType:
				for _, m := range t.Methods.List {
					for _, n := range m.Names {
						p.symbols[s.Name.Name+"."+n.Name] = true
					}
				}
			}
		case *ast.ValueSpec:
			p.addValueSpec(d.Tok, s, &lastType)
		}
	}
}

// addValueSpec records the names of a var or const spec and, for typed
// string constants (T("x") or name T = "x"), their values. A constant of a
// documented code type in any other form is an error. lastType is the
// type of the previous spec of the group, for implicit repetition.
func (p *goPackage) addValueSpec(tok token.Token, s *ast.ValueSpec, lastType *string) {
	typ := ""
	if id, ok := s.Type.(*ast.Ident); ok {
		typ = id.Name
	}
	if tok == token.CONST && s.Type == nil && len(s.Values) == 0 && codeTypes[*lastType] {
		p.errs = append(p.errs, fmt.Errorf("constant %s repeats a %s implicitly; give it an explicit value", s.Names[0].Name, *lastType))
	}
	for i, n := range s.Names {
		p.symbols[n.Name] = true
		if i >= len(s.Values) {
			continue
		}
		t, value, ok := typ, "", false
		switch v := s.Values[i].(type) {
		case *ast.BasicLit:
			if v.Kind == token.STRING {
				value, ok = unquote(v.Value)
			}
		case *ast.CallExpr:
			if fn, isIdent := v.Fun.(*ast.Ident); isIdent && len(v.Args) == 1 {
				if lit, isLit := v.Args[0].(*ast.BasicLit); isLit && lit.Kind == token.STRING && t == "" {
					t = fn.Name
					value, ok = unquote(lit.Value)
				}
			}
		}
		if tok == token.CONST {
			if ok && t != "" {
				p.consts[n.Name] = goConst{n.Name, t, value}
			} else if codeTypes[t] {
				p.errs = append(p.errs, fmt.Errorf("constant %s of type %s is not a string literal docgen reads", n.Name, t))
			}
			if v, isInt := evalInt(s.Values[i], p.ints); isInt {
				p.ints[n.Name] = v
			}
		}
		if t != "" {
			*lastType = t
		}
	}
}

func unquote(lit string) (string, bool) {
	v, err := strconv.Unquote(lit)
	return v, err == nil
}

// addDefaults records the defaults a withDefaults method sets, in either
// of the forms the reader uses: set(&l.Field, value), and
// if l.Field <= 0 { l.Field = value }.
func (p *goPackage) addDefaults(typ string, body *ast.BlockStmt) {
	if p.defaults[typ] == nil {
		p.defaults[typ] = map[string]int64{}
		p.defaultSet[typ] = map[string]bool{}
	}
	record := func(field string, value ast.Expr) {
		p.defaultSet[typ][field] = true
		if v, ok := evalInt(value, p.ints); ok {
			p.defaults[typ][field] = v
		}
	}
	ast.Inspect(body, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.CallExpr:
			if fn, ok := s.Fun.(*ast.Ident); ok && fn.Name == "set" && len(s.Args) == 2 {
				if u, ok := s.Args[0].(*ast.UnaryExpr); ok && u.Op == token.AND {
					if sel, ok := u.X.(*ast.SelectorExpr); ok {
						record(sel.Sel.Name, s.Args[1])
					}
				}
			}
		case *ast.IfStmt:
			cond, ok := s.Cond.(*ast.BinaryExpr)
			if !ok || cond.Op != token.LEQ {
				return true
			}
			sel, ok := cond.X.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			for _, st := range s.Body.List {
				if as, ok := st.(*ast.AssignStmt); ok && len(as.Lhs) == 1 && len(as.Rhs) == 1 {
					if lhs, ok := as.Lhs[0].(*ast.SelectorExpr); ok && lhs.Sel.Name == sel.Sel.Name {
						record(lhs.Sel.Name, as.Rhs[0])
					}
				}
			}
		}
		return true
	})
}

// evalInt evaluates an integer constant expression of literals, known
// constants, +, -, *, << and parentheses.
func evalInt(e ast.Expr, consts map[string]int64) (int64, bool) {
	switch x := e.(type) {
	case *ast.BasicLit:
		if x.Kind != token.INT {
			return 0, false
		}
		v, err := strconv.ParseInt(x.Value, 0, 64)
		return v, err == nil
	case *ast.Ident:
		v, ok := consts[x.Name]
		return v, ok
	case *ast.ParenExpr:
		return evalInt(x.X, consts)
	case *ast.BinaryExpr:
		a, ok := evalInt(x.X, consts)
		if !ok {
			return 0, false
		}
		b, ok := evalInt(x.Y, consts)
		if !ok {
			return 0, false
		}
		switch x.Op {
		case token.ADD:
			return a + b, true
		case token.SUB:
			return a - b, true
		case token.MUL:
			return a * b, true
		case token.SHL:
			return a << b, true
		}
	}
	return 0, false
}

// commentText returns a comment group's text as one paragraph per blank
// line, each collapsed to single spaces.
func commentText(g *ast.CommentGroup) string {
	if g == nil {
		return ""
	}
	var paras []string
	for _, para := range strings.Split(strings.TrimSpace(g.Text()), "\n\n") {
		paras = append(paras, strings.Join(strings.Fields(para), " "))
	}
	return strings.Join(paras, "\n\n")
}

// goFuncBody returns the body of the top-level function name in the Go
// file at path, without its braces, dedented by one tab.
func goFuncBody(path, name string) (string, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return "", err
	}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != name {
			continue
		}
		body := string(src[fset.Position(fn.Body.Lbrace).Offset+1 : fset.Position(fn.Body.Rbrace).Offset])
		var lines []string
		for _, line := range strings.Split(body, "\n") {
			lines = append(lines, strings.TrimPrefix(line, "\t"))
		}
		return trimBlankLines(lines), nil
	}
	return "", fmt.Errorf("%s: no function %s", path, name)
}

// trimBlankLines joins lines without the blank lines at either end, with
// a final newline.
func trimBlankLines(lines []string) string {
	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
	}
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n") + "\n"
}
