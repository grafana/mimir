// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
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
	Type     *FieldType
}

// FieldType is a struct field's declared type, resolved for code generation.
type FieldType struct {
	Name       string // Name is the type rendered as source, e.g. "planning.Node", "*core.FunctionCall", "[]planning.Node".
	ImportPath string // import path of the type's package qualifier, or "" when the type has no qualifier (a local or builtin type).
}

// NodeTag is the parsed value of a node:"..." struct tag.
type NodeTag struct {
	IsChild    bool
	IsChildren bool
	Nilable    bool   // child only; non-nil by default; set true with the "nilable" tag option for the rare case (e.g. AggregateExpression.Param)
	Label      string // child only; the child's label, set with the "label=X" tag option
	Min        int    // children only; minimum required number of children, set with the "min=N" tag option (0 means no lower bound)
	LabelFmt   string // children only; fmt format applied with each child's index to build its label (e.g. "labelfmt=node %d" yields "node 0", "node 1", ...)
	NoCollapse bool   // children only; with "nocollapse", a single child keeps its labelfmt label instead of collapsing to ""
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
		filePath := filepath.Join(packagePath, entry.Name())
		f, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
		if err != nil {
			return nil, fmt.Errorf("parsing %q: %w", filePath, err)
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
	imports, err := parseFileImports(f)
	if err != nil {
		return nil, err
	}
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
			s, err := parseStruct(ts.Name.Name, st, imports)
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

func parseStruct(name string, st *ast.StructType, imports map[string]string) (*Struct, error) {
	s := &Struct{Name: name}
	for _, field := range st.Fields.List {
		fields, err := parseField(field, imports)
		if err != nil {
			return nil, err
		}
		s.Fields = append(s.Fields, fields...)
	}
	return s, nil
}

// parseField expands one ast.Field declaration into one or more Field entries:
// multi-name declarations (e.g. "a, b int") yield one Field per name
func parseField(field *ast.Field, imports map[string]string) ([]Field, error) {
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

	fieldType, err := resolveFieldType(field.Type, imports)
	if err != nil {
		if len(field.Names) > 0 {
			return nil, fmt.Errorf("field %q: %w", field.Names[0].Name, err)
		}
		return nil, fmt.Errorf("embedded field: %w", err)
	}

	if len(field.Names) == 0 {
		return []Field{{Tag: tag, Embedded: true, Type: fieldType}}, nil
	}
	result := make([]Field, len(field.Names))
	for i, name := range field.Names {
		result[i] = Field{Name: name.Name, Tag: tag, Type: fieldType}
	}
	return result, nil
}

// parseFileImports maps a file's import local names to their import paths.
func parseFileImports(f *ast.File) (map[string]string, error) {
	imports := make(map[string]string, len(f.Imports))
	for _, imp := range f.Imports {
		importPath, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return nil, fmt.Errorf("unquoting import path %s: %w", imp.Path.Value, err)
		}
		// The local name is the import alias if one is given, otherwise the package name.
		// We assume the latter equals the last path segment.
		name := path.Base(importPath)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		imports[name] = importPath
	}
	return imports, nil
}

// resolveFieldType renders a field's declared type as source and resolves the import path of
// its package qualifier (empty when the type has no qualifier).
func resolveFieldType(expr ast.Expr, imports map[string]string) (*FieldType, error) {
	switch t := expr.(type) {
	case *ast.Ident:
		return &FieldType{Name: t.Name}, nil
	case *ast.StarExpr:
		inner, err := resolveFieldType(t.X, imports)
		if err != nil {
			return nil, err
		}
		return &FieldType{Name: "*" + inner.Name, ImportPath: inner.ImportPath}, nil
	case *ast.ArrayType:
		if t.Len != nil {
			return nil, fmt.Errorf("unsupported fixed-size array field type")
		}
		inner, err := resolveFieldType(t.Elt, imports)
		if err != nil {
			return nil, err
		}
		return &FieldType{Name: "[]" + inner.Name, ImportPath: inner.ImportPath}, nil
	case *ast.SelectorExpr:
		pkg, ok := t.X.(*ast.Ident)
		if !ok {
			return nil, fmt.Errorf("unsupported field type")
		}
		return &FieldType{Name: pkg.Name + "." + t.Sel.Name, ImportPath: imports[pkg.Name]}, nil
	default:
		return nil, fmt.Errorf("unsupported field type %T", expr)
	}
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
		return parseChildrenTag(parts[1:])
	default:
		return nil, fmt.Errorf("unknown node tag kind %q", parts[0])
	}
}

// parseChildTag parses the comma-separated options after "child".
func parseChildTag(opts []string) (*NodeTag, error) {
	t := &NodeTag{IsChild: true}
	for _, opt := range opts {
		switch {
		case opt == "nilable":
			t.Nilable = true
		case strings.HasPrefix(opt, "label="):
			t.Label = strings.TrimPrefix(opt, "label=")
		default:
			return nil, fmt.Errorf("unknown child tag option %q", opt)
		}
	}
	return t, nil
}

// parseChildrenTag parses the comma-separated options after "children".
func parseChildrenTag(opts []string) (*NodeTag, error) {
	t := &NodeTag{IsChildren: true}
	for _, opt := range opts {
		switch {
		case strings.HasPrefix(opt, "min="):
			raw := strings.TrimPrefix(opt, "min=")
			minChildren, err := strconv.Atoi(raw)
			if err != nil {
				return nil, fmt.Errorf("invalid children min value %q", raw)
			}
			t.Min = minChildren
		case strings.HasPrefix(opt, "labelfmt="):
			t.LabelFmt = strings.TrimPrefix(opt, "labelfmt=")
		case opt == "nocollapse":
			t.NoCollapse = true
		default:
			return nil, fmt.Errorf("unknown children tag option %q", opt)
		}
	}
	return t, nil
}
