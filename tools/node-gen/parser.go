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
	NodeStructs []*Struct     // structs annotated with //node:generate, sorted by name
	Registry    *TypeRegistry // structs reachable from the node fields, used to generate equality
}

// Struct represents a struct type in the package.
type Struct struct {
	Name    string
	Fields  []Field
	Package string
}

// Field is one declared field of a Struct.
type Field struct {
	Name     string
	Tag      *NodeTag
	Embedded bool
	Type     *FieldType
}

type TypeKind int

const (
	KindNamed TypeKind = iota
	KindPointer
	KindSlice
)

// FieldType is a struct field's declared type, resolved for code generation.
type FieldType struct {
	Kind       TypeKind
	Elem       *FieldType // set for KindPointer and KindSlice
	Name       string     // KindNamed: bare type name, e.g. "Node", "int64"
	Qualifier  string     // KindNamed: package selector as written, e.g. "core"; "" when unqualified
	ImportPath string     // resolved import path of the named type, propagated onto pointer/slice wrappers; "" when unqualified
}

// Render reproduces the type as Go source, e.g. "*core.FunctionCall", "[]planning.Node".
func (t *FieldType) Render() string {
	switch t.Kind {
	case KindPointer:
		return "*" + t.Elem.Render()
	case KindSlice:
		return "[]" + t.Elem.Render()
	case KindNamed:
		if t.ImportPath != "" {
			return path.Base(t.ImportPath) + "." + t.Name
		}
		if t.Qualifier != "" {
			return t.Qualifier + "." + t.Name
		}
		return t.Name
	default:
		panic(fmt.Sprintf("unexpected FieldType kind: %d", t.Kind))
	}
}

// resolvePackage a type with no import path inherits enclosingPkg, otherwise the package is the base of its ImportPath.
func (t *FieldType) resolvePackage(enclosingPkg string) string {
	if t.ImportPath != "" {
		return path.Base(t.ImportPath)
	}
	return enclosingPkg
}

// typeKey returns the registry key for a named-type reference resolved against enclosingPkg.
func (t *FieldType) typeKey(enclosingPkg string) TypeKey {
	return TypeKey{Package: t.resolvePackage(enclosingPkg), Name: t.Name}
}

// NodeTag is the parsed value of a node:"..." struct tag.
type NodeTag struct {
	IsChild    bool
	IsChildren bool
	Nilable    bool     // child only; non-nil by default; set true with the "nilable" tag option for the rare case (e.g. AggregateExpression.Param)
	Label      *string  // child only; the child's label, set with the "label=X" tag option (nil when unset)
	Min        int      // children only; minimum required number of children, set with the "min=N" tag option (0 means no lower bound)
	LabelFmt   string   // children only; fmt format applied with each child's index to build its label (e.g. "labelfmt=node %d" yields "node 0", "node 1", ...)
	NoCollapse bool     // children only; with "nocollapse", a single child keeps its labelfmt label instead of collapsing to ""
	Hints      []string // embedded *Details only; field names excluded from EquivalentToIgnoringHintsAndChildren, set with "hints=A;B"
}

// dependencyPackages lists the packages, besides the target, whose types the target's nodes may reference and that
// the equality generator must therefore resolve. A referenced type from a package not listed here fails generation.
var dependencyPackages = []string{
	"pkg/mimirpb",
	"pkg/streamingpromql/planning/core",
	"pkg/streamingpromql/operators/functions",
	"vendor/github.com/prometheus/prometheus/model/labels",
	"vendor/github.com/prometheus/prometheus/promql/parser",
}

// ParsePackage reads all non-test .go files in packagePath (skipping the generated file named genFileName)
// and returns the parsed IR, including a registry of the structs.
func ParsePackage(packagePath, genFileName string) (*Package, error) {
	targetFiles, err := parseDir(packagePath, genFileName)
	if err != nil {
		return nil, err
	}

	var dependencyFiles []*ast.File
	for _, dependencyPackage := range dependencyPackages {
		files, err := parseDir(dependencyPackage, genFileName)
		if err != nil {
			return nil, err
		}
		dependencyFiles = append(dependencyFiles, files...)
	}

	return buildPackage(targetFiles, dependencyFiles)
}

// parseDir parses all non-test .go files in dir (skipping the generated file named genFileName).
func parseDir(dir, genFileName string) ([]*ast.File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %q: %w", dir, err)
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
		filePath := filepath.Join(dir, entry.Name())
		f, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
		if err != nil {
			return nil, fmt.Errorf("parsing %q: %w", filePath, err)
		}
		files = append(files, f)
	}
	return files, nil
}

// buildPackage assembles a Package IR from AST files. Annotated struct names are returned sorted alphabetically
// so the output is invariant to source-level reorderings.
func buildPackage(targetFiles, dependencyFiles []*ast.File) (*Package, error) {
	reg := newTypeRegistry()
	pkg := &Package{Registry: reg}
	if len(targetFiles) > 0 {
		pkg.Name = targetFiles[0].Name.Name
	}

	var nodeStructs []*Struct
	for _, f := range targetFiles {
		structs, err := collectStructs(f, reg)
		if err != nil {
			return nil, err
		}
		nodeStructs = append(nodeStructs, structs...)
	}
	slices.SortFunc(nodeStructs, func(a, b *Struct) int {
		return strings.Compare(a.Name, b.Name)
	})
	pkg.NodeStructs = nodeStructs

	for _, f := range dependencyFiles {
		if _, err := collectStructs(f, reg); err != nil {
			return nil, err
		}
	}
	return pkg, nil
}

// collectStructs registers every struct and defined type declaration in the file under its declared package
// name, and returns the //node:generate-annotated structs.
func collectStructs(f *ast.File, reg *TypeRegistry) ([]*Struct, error) {
	pkg := f.Name.Name
	imports, err := parseFileImports(f)
	if err != nil {
		return nil, err
	}
	var nodeStructs []*Struct
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		annotated := hasNodeGenerateComment(gd.Doc)
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok || ts.TypeParams != nil {
				continue
			}
			key := TypeKey{Package: pkg, Name: ts.Name.Name}
			astStruct, isStruct := ts.Type.(*ast.StructType)
			if !isStruct {
				// Defined non-struct type i.e. enum, or an alias to another named type
				if underlyingType := resolveFieldType(ts.Type, imports); underlyingType != nil {
					reg.aliases[key] = underlyingType
				}
				continue
			}
			s, err := parseStruct(ts.Name.Name, astStruct, imports)
			if err != nil {
				return nil, fmt.Errorf("struct %s: %w", ts.Name.Name, err)
			}
			s.Package = pkg
			reg.structs[key] = s
			if annotated {
				nodeStructs = append(nodeStructs, s)
			}
		}
	}
	return nodeStructs, nil
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
	embedded := len(field.Names) == 0
	if field.Tag != nil {
		raw, err := strconv.Unquote(field.Tag.Value)
		if err != nil {
			return nil, fmt.Errorf("unquoting tag %q: %w", field.Tag.Value, err)
		}
		tag, err = parseNodeTag(reflect.StructTag(raw), embedded)
		if err != nil {
			if embedded {
				return nil, fmt.Errorf("embedded field tag: %w", err)
			}
			return nil, fmt.Errorf("field %q tag: %w", field.Names[0].Name, err)
		}
	}

	fieldType := resolveFieldType(field.Type, imports)

	if embedded {
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

// resolveFieldType builds a structured FieldType, resolving the import path of its package qualifier.
// It returns nil for an unsupported type expression (map, func, interface, ...).
func resolveFieldType(expr ast.Expr, imports map[string]string) *FieldType {
	switch t := expr.(type) {
	case *ast.Ident:
		return &FieldType{Kind: KindNamed, Name: t.Name}
	case *ast.StarExpr:
		inner := resolveFieldType(t.X, imports)
		if inner == nil {
			return nil
		}
		return &FieldType{Kind: KindPointer, Elem: inner, ImportPath: inner.ImportPath}
	case *ast.ArrayType:
		if t.Len != nil {
			return nil
		}
		inner := resolveFieldType(t.Elt, imports)
		if inner == nil {
			return nil
		}
		return &FieldType{Kind: KindSlice, Elem: inner, ImportPath: inner.ImportPath}
	case *ast.SelectorExpr:
		pkg, ok := t.X.(*ast.Ident)
		if !ok {
			return nil
		}
		return &FieldType{Kind: KindNamed, Name: t.Sel.Name, Qualifier: pkg.Name, ImportPath: imports[pkg.Name]}
	}
	return nil
}

// parseNodeTag parses the node:"..." struct tag value.
func parseNodeTag(tag reflect.StructTag, embedded bool) (*NodeTag, error) {
	val, ok := tag.Lookup("node")
	if !ok {
		return nil, nil
	}
	parts := strings.Split(val, ",")
	switch {
	case parts[0] == "child":
		return parseChildTag(parts[1:])
	case parts[0] == "children":
		return parseChildrenTag(parts[1:])
	case strings.HasPrefix(parts[0], "hints="):
		if !embedded {
			return nil, fmt.Errorf("hints tag is only supported on embedded fields")
		}
		return parseHintsTag(parts[0], parts[1:])
	default:
		return nil, fmt.Errorf("unknown node tag kind %q", parts[0])
	}
}

// parseHintsTag parses a "hints=A;B" tag carried on a node's embedded *Details field.
func parseHintsTag(kind string, rest []string) (*NodeTag, error) {
	if len(rest) > 0 {
		return nil, fmt.Errorf("unexpected options after hints: %q", strings.Join(rest, ","))
	}
	list := strings.TrimPrefix(kind, "hints=")
	if list == "" {
		return nil, fmt.Errorf("hints tag requires at least one field name")
	}
	return &NodeTag{Hints: strings.Split(list, ";")}, nil
}

// parseChildTag parses the comma-separated options after "child".
func parseChildTag(opts []string) (*NodeTag, error) {
	t := &NodeTag{IsChild: true}
	for _, opt := range opts {
		switch {
		case opt == "nilable":
			t.Nilable = true
		case strings.HasPrefix(opt, "label="):
			label := strings.TrimPrefix(opt, "label=")
			t.Label = &label
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

// TypeKey identifies a parsed struct by its declared package name and type name.
type TypeKey struct {
	Package string
	Name    string
}

// TypeRegistry holds the parsed types keyed by package and name: structs, and aliases (defined
// non-struct types mapped to their underlying type).
type TypeRegistry struct {
	structs map[TypeKey]*Struct
	aliases map[TypeKey]*FieldType
}

func newTypeRegistry() *TypeRegistry {
	return &TypeRegistry{
		structs: make(map[TypeKey]*Struct),
		aliases: make(map[TypeKey]*FieldType),
	}
}

func (r *TypeRegistry) LookupStruct(ft *FieldType, enclosingPkg string) (*Struct, bool) {
	s, ok := r.structs[ft.typeKey(enclosingPkg)]
	return s, ok
}

// LookupAlias returns the underlying type of defined non-struct type (e.g. int32 for `type Op int32`).
func (r *TypeRegistry) LookupAlias(ft *FieldType, enclosingPkg string) (*FieldType, bool) {
	u, ok := r.aliases[ft.typeKey(enclosingPkg)]
	return u, ok
}
