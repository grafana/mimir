// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// Package is the result of parsing a single Go package directory.
type Package struct {
	Name        string
	NodeStructs []*Struct // structs annotated with //node:generate, sorted by name
}

// Struct represents a struct type in the package.
type Struct struct {
	Name   string
	Fields []Field
}

// Field is one declared field of a Struct.
type Field struct {
	Name     string
	Tag      *NodeTag
	Embedded bool
}

// NodeTag is the parsed value of a node:"..." struct tag.
type NodeTag struct {
	IsChild    bool
	IsChildren bool
	Nilable    bool // child only; non-nil by default; set true with the "nilable" tag option for the rare case (e.g. AggregateExpression.Param)
}

// ParsePackage reads all non-test .go files in packagePath (skipping the generated file named genFileName)
// and returns the parsed IR.
func ParsePackage(packagePath, genFileName string) (*Package, error) {
	entries, err := os.ReadDir(packagePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %q: %w", packagePath, err)
	}

	fset := token.NewFileSet()
	files := make([]*ast.File, 0, len(entries))

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".go") || genFileName == entry.Name() || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(packagePath, entry.Name())
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return nil, fmt.Errorf("parsing %q: %w", path, err)
		}
		files = append(files, f)
	}

	return buildPackage(files)
}

// buildPackage assembles a Package IR from AST files. Annotated struct names are returned sorted alphabetically
// so the output is invariant to source-level reorderings.
func buildPackage(files []*ast.File) (*Package, error) {
	pkg := &Package{}
	if len(files) > 0 {
		pkg.Name = files[0].Name.Name
	}
	for _, f := range files {
		structs, err := collectNodeStructs(f)
		if err != nil {
			return nil, err
		}
		pkg.NodeStructs = append(pkg.NodeStructs, structs...)
	}
	slices.SortFunc(pkg.NodeStructs, func(a, b *Struct) int {
		return strings.Compare(a.Name, b.Name)
	})
	return pkg, nil
}

// collectNodeStructs returns the //node:generate-annotated struct type declarations found in file.
// Non-annotated declarations are ignored.
func collectNodeStructs(f *ast.File) ([]*Struct, error) {
	var structs []*Struct
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		if !hasNodeGenerateComment(gd.Doc) {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				continue
			}
			s, err := parseStruct(ts.Name.Name, st)
			if err != nil {
				return nil, fmt.Errorf("struct %s: %w", ts.Name.Name, err)
			}
			structs = append(structs, s)
		}
	}
	return structs, nil
}

func hasNodeGenerateComment(cg *ast.CommentGroup) bool {
	if cg == nil {
		return false
	}
	for _, c := range cg.List {
		if strings.TrimSpace(c.Text) == "//node:generate" {
			return true
		}
	}
	return false
}

func parseStruct(name string, st *ast.StructType) (*Struct, error) {
	s := &Struct{Name: name}
	for _, field := range st.Fields.List {
		fields, err := parseField(field)
		if err != nil {
			return nil, err
		}
		s.Fields = append(s.Fields, fields...)
	}
	return s, nil
}

// parseField expands one ast.Field declaration into one or more Field entries:
// multi-name declarations (e.g. "a, b int") yield one Field per name
func parseField(field *ast.Field) ([]Field, error) {
	var tag *NodeTag
	if field.Tag != nil {
		raw, err := strconv.Unquote(field.Tag.Value)
		if err != nil {
			return nil, fmt.Errorf("unquoting tag %q: %w", field.Tag.Value, err)
		}
		tag, err = parseNodeTag(reflect.StructTag(raw))
		if err != nil {
			if len(field.Names) > 0 {
				return nil, fmt.Errorf("field %q tag: %w", field.Names[0].Name, err)
			}
			return nil, fmt.Errorf("embedded field tag: %w", err)
		}
	}
	if len(field.Names) == 0 {
		return []Field{{Tag: tag, Embedded: true}}, nil
	}
	result := make([]Field, len(field.Names))
	for i, name := range field.Names {
		result[i] = Field{Name: name.Name, Tag: tag}
	}
	return result, nil
}

// parseNodeTag parses the node:"..." struct tag value.
func parseNodeTag(tag reflect.StructTag) (*NodeTag, error) {
	val, ok := tag.Lookup("node")
	if !ok {
		return nil, nil
	}
	parts := strings.Split(val, ",")
	switch parts[0] {
	case "child":
		return parseChildTag(parts[1:])
	case "children":
		return &NodeTag{IsChildren: true}, nil
	default:
		return nil, fmt.Errorf("unknown node tag kind %q", parts[0])
	}
}

// parseChildTag parses the comma-separated options after "child".
func parseChildTag(opts []string) (*NodeTag, error) {
	t := &NodeTag{IsChild: true}
	for _, opt := range opts {
		switch opt {
		case "nilable":
			t.Nilable = true
		default:
			return nil, fmt.Errorf("unknown child tag option %q", opt)
		}
	}
	return t, nil
}
