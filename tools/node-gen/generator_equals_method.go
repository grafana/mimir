// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	_ "embed"
	"fmt"
	"slices"
	"strings"
	"text/template"
)

//go:embed equals_method.tmpl
var equalsTmplContent string
var equalsTmpl = template.Must(template.New("equals_method").Parse(equalsTmplContent))

// template names defined in equals_method.tmpl.
const (
	tmplNodeEquivalentMethod = "node_equivalent_method"
	tmplStructEqualsFunc     = "struct_equals_func"
)

// comparisonKind enumerates the possible ways two fields can be compared.
type comparisonKind string

const (
	kindScalar       comparisonKind = "scalar"
	kindFunc         comparisonKind = "func"
	kindMethod       comparisonKind = "method"
	kindPointer      comparisonKind = "pointer"
	kindSliceEqual   comparisonKind = "sliceEqual"   // comparable elements: slices.Equal
	kindSliceFunc    comparisonKind = "sliceFunc"    // struct elements: slices.EqualFunc with the genEqualsXxx func passed directly
	kindSliceClosure comparisonKind = "sliceClosure" // any other element: slices.EqualFunc with a closure comparing the elements
)

// comparison describes how to compare two operands A and B.
type comparison struct {
	Kind     comparisonKind
	A, B     string      // operand expressions
	Func     string      // kindFunc, kindSliceFunc: the equality function name (e.g. genEqualsLabelMatcher)
	Method   string      // kindMethod: the equality method name (e.g. Equal)
	ElemType string      // kindSliceClosure: the slice element type (e.g. "*core.LabelMatcher"), used as the closure's parameter type
	Elem     *comparison // kindPointer, kindSliceClosure: the comparison of the elements
}

// ignoredTypes are types whose fields never participate in equality.
var ignoredTypes = map[string]struct{}{
	"PositionRange": {},
}

// predefinedComparisons maps a type to how its values are compared, instead of recursing into its fields.
// It covers Go's builtin types and a few standard-library types with their own equality.
var predefinedComparisons = map[string]comparison{
	"bool":   {Kind: kindScalar},
	"string": {Kind: kindScalar},
	"int":    {Kind: kindScalar}, "int8": {Kind: kindScalar}, "int16": {Kind: kindScalar}, "int32": {Kind: kindScalar}, "int64": {Kind: kindScalar},
	"uint": {Kind: kindScalar}, "uint8": {Kind: kindScalar}, "uint16": {Kind: kindScalar}, "uint32": {Kind: kindScalar}, "uint64": {Kind: kindScalar}, "uintptr": {Kind: kindScalar},
	"byte": {Kind: kindScalar}, "rune": {Kind: kindScalar},
	"float32": {Kind: kindScalar}, "float64": {Kind: kindScalar},
	"time.Time":     {Kind: kindMethod, Method: "Equal"},
	"time.Duration": {Kind: kindScalar},
}

// equalsFuncData is the input for the "node_equivalent_method" and "struct_equals_func" templates.
type equalsFuncData struct {
	Receiver    string
	FuncName    string
	Type        string
	Comparisons []*comparison
}

// equalsGenerator generates EquivalentToIgnoringHintsAndChildren and its genEqualsXxx funcs for the nodes of one package.
type equalsGenerator struct {
	stagedEqualsFuncs    map[string]*equalsFuncData // equals funcs built for the current node, not yet rendered
	generatedEqualsFuncs map[string]struct{}        // every equal func name already generated, so later nodes neither rebuild nor re-emit them
}

// newEquivalentMethod returns the generator for EquivalentToIgnoringHintsAndChildren. It shares one equalsGenerator across
// all nodes in the package, so each genEqualsXxx func is built and emitted once, alongside the first method that triggers it.
func newEquivalentMethod() MethodGenerator {
	g := &equalsGenerator{
		stagedEqualsFuncs:    make(map[string]*equalsFuncData),
		generatedEqualsFuncs: make(map[string]struct{}),
	}

	return MethodGenerator{
		Name:     "EquivalentToIgnoringHintsAndChildren",
		Generate: g.generate,
	}
}

func (g *equalsGenerator) generate(s *Struct, imports *ImportsCollector, reg *TypeRegistry) (string, error) {
	imports.Add("github.com/grafana/mimir/pkg/streamingpromql/planning")

	comparisons, err := g.buildStructFieldComparisons(s, receiverName(s.Name), "oi", imports, reg)
	if err != nil {
		return "", err
	}

	methodBody, err := renderEqualsTemplate(tmplNodeEquivalentMethod, equalsFuncData{
		Receiver:    receiverName(s.Name),
		Type:        s.Name,
		Comparisons: comparisons,
	})
	if err != nil {
		return "", err
	}

	funcsBody, err := renderEqualsFuncs(g.stagedEqualsFuncs)
	if err != nil {
		return "", err
	}
	// reset for the next node. generatedEqualsFuncs already records these so they aren't rebuilt or re-emitted
	g.stagedEqualsFuncs = make(map[string]*equalsFuncData)

	if funcsBody == "" {
		return methodBody, nil
	}
	return methodBody + "\n\n" + funcsBody, nil
}

// buildStructFieldComparisons builds the field-by-field comparison for given struct st, with operands accessed as "a.<field>" and "b.<field>"
func (g *equalsGenerator) buildStructFieldComparisons(st *Struct, a, b string, imports *ImportsCollector, reg *TypeRegistry) ([]*comparison, error) {
	fields, err := g.collectEqualityFields(st, reg)
	if err != nil {
		return nil, err
	}
	fieldComparisons := make([]*comparison, 0, len(fields))
	for _, f := range fields {
		c, err := g.buildFieldComparison(f.Type, st.Package, a+"."+f.Name, b+"."+f.Name, imports, reg)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", st.Name, f.Name, err)
		}
		fieldComparisons = append(fieldComparisons, c)
	}
	return fieldComparisons, nil
}

// collectEqualityFields collects the fields that take part in equality
func (g *equalsGenerator) collectEqualityFields(s *Struct, reg *TypeRegistry) ([]Field, error) {
	var fields []Field
	for _, f := range s.Fields {
		if f.Type == nil {
			return nil, fmt.Errorf("%s.%s: unsupported field type", s.Name, f.Name)
		}

		if f.Embedded {
			embedFields, err := g.collectEmbedFields(s, f, reg)
			if err != nil {
				return nil, err
			}

			fields = append(fields, embedFields...)
			continue
		}

		if shouldCompareField(f, nil) {
			fields = append(fields, f)
		}
	}
	slices.SortFunc(fields, func(a, b Field) int {
		return strings.Compare(a.Name, b.Name)
	})
	return fields, nil
}

func (g *equalsGenerator) collectEmbedFields(s *Struct, f Field, reg *TypeRegistry) ([]Field, error) {
	var embedType *FieldType
	switch f.Type.Kind {
	case KindNamed:
		embedType = f.Type
	case KindPointer:
		embedType = f.Type.Elem
	default:
		return nil, fmt.Errorf("%s: unsupported embedded type %q", s.Name, f.Type.Render())
	}

	if embedType == nil {
		return nil, fmt.Errorf("%s: unsupported embedded type", s.Name)
	}

	embedStruct, ok := reg.LookupStruct(embedType, s.Package)
	if !ok {
		return nil, fmt.Errorf("%s: embedded type %q not found in registry", s.Name, embedType.Render())
	}

	hints := hintsSet(f.Tag)
	if err := validateHints(hints, embedStruct.Fields); err != nil {
		return nil, fmt.Errorf("%s: %w", s.Name, err)
	}

	fields := make([]Field, 0, len(embedStruct.Fields))
	for _, ef := range embedStruct.Fields {
		if ef.Type == nil {
			return nil, fmt.Errorf("%s.%s: unsupported field type", s.Name, ef.Name)
		}
		if shouldCompareField(ef, hints) {
			fields = append(fields, ef)
		}
	}
	return fields, nil
}

// buildFieldComparison builds the comparison of operands a and b, recursing through pointers and slices down to the named element type.
func (g *equalsGenerator) buildFieldComparison(ft *FieldType, pkg string, a, b string, imports *ImportsCollector, reg *TypeRegistry) (*comparison, error) {
	if ft == nil {
		return nil, fmt.Errorf("unsupported field type")
	}
	switch ft.Kind {
	case KindPointer:
		elem, err := g.buildFieldComparison(ft.Elem, pkg, "*"+a, "*"+b, imports, reg)
		if err != nil {
			return nil, err
		}
		return &comparison{Kind: kindPointer, A: a, B: b, Elem: elem}, nil
	case KindSlice:
		imports.Add("slices")
		elem, err := g.buildFieldComparison(ft.Elem, pkg, "a", "b", imports, reg)
		if err != nil {
			return nil, err
		}
		switch elem.Kind {
		case kindScalar:
			return &comparison{Kind: kindSliceEqual, A: a, B: b}, nil
		case kindFunc:
			return &comparison{Kind: kindSliceFunc, A: a, B: b, Func: elem.Func}, nil
		default:
			imports.Add(ft.Elem.ImportPath)
			return &comparison{Kind: kindSliceClosure, A: a, B: b, ElemType: ft.Elem.Render(), Elem: elem}, nil
		}
	case KindNamed:
		return g.buildNamedComparison(ft, pkg, a, b, imports, reg)
	}

	return nil, fmt.Errorf("unsupported type %s", ft.Render())
}

func (g *equalsGenerator) buildNamedComparison(fieldType *FieldType, enclosingPkg, a, b string, imports *ImportsCollector, reg *TypeRegistry) (*comparison, error) {
	// A builtin or a known standard-library type: compare with a fixed rule
	if comp, ok := predefinedComparisons[fieldType.Render()]; ok {
		comp.A, comp.B = a, b
		return &comp, nil
	}

	// A defined type (e.g. type Op int32): compare its underlying type
	if underlyingType, ok := reg.LookupAlias(fieldType, enclosingPkg); ok {
		return g.buildFieldComparison(underlyingType, fieldType.resolvePackage(enclosingPkg), a, b, imports, reg)
	}

	// A struct: compare it field by field via a generated genEqualsXxx func.
	if st, ok := reg.LookupStruct(fieldType, enclosingPkg); ok {
		name, err := g.ensureEqualsFunc(st, fieldType, imports, reg)
		if err != nil {
			return nil, err
		}
		return &comparison{Kind: kindFunc, A: a, B: b, Func: name}, nil
	}

	return nil, fmt.Errorf("unsupported type %q", fieldType.Render())
}

// ensureEqualsFunc builds the genEqualsXxx func for struct once and returns its name. Already-generated funcs are not rebuilt
func (g *equalsGenerator) ensureEqualsFunc(st *Struct, ref *FieldType, imports *ImportsCollector, reg *TypeRegistry) (string, error) {
	name := "genEquals" + st.Name
	if _, generated := g.generatedEqualsFuncs[name]; generated {
		return name, nil
	}
	// put it right here to break recursion for types like this `type Node struct { next *Node }`
	g.generatedEqualsFuncs[name] = struct{}{}
	imports.Add(ref.ImportPath) // the func signature renders ref's type.

	comparisons, err := g.buildStructFieldComparisons(st, "a", "b", imports, reg)
	if err != nil {
		return "", err
	}
	g.stagedEqualsFuncs[name] = &equalsFuncData{FuncName: name, Type: ref.Render(), Comparisons: comparisons}
	return name, nil
}

func hintsSet(tag *NodeTag) map[string]struct{} {
	if tag == nil || len(tag.Hints) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(tag.Hints))
	for _, h := range tag.Hints {
		set[h] = struct{}{}
	}
	return set
}

// validateHints checks that every hint names a non-empty and existing field.
func validateHints(hints map[string]struct{}, fields []Field) error {
	if len(hints) == 0 {
		return nil
	}
	structFields := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		structFields[f.Name] = struct{}{}
	}
	for h := range hints {
		if h == "" {
			return fmt.Errorf("hints tag contains an empty field name")
		}
		if _, ok := structFields[h]; !ok {
			return fmt.Errorf("hints tag names unknown field %s", h)
		}
	}
	return nil
}

// shouldCompareField reports whether a comparison should be generated for field. It is excluded when:
//   - it is a child/children field
//   - named in the hints set
//   - an always-ignored type
func shouldCompareField(f Field, hints map[string]struct{}) bool {
	if f.Tag != nil && (f.Tag.IsChild || f.Tag.IsChildren) {
		return false
	}
	if _, hinted := hints[f.Name]; hinted {
		return false
	}
	_, ignoredType := ignoredTypes[f.Type.Name]
	return !ignoredType
}

// renderEqualsFuncs renders the equals funcs built for the current node (sorted for deterministic output)
func renderEqualsFuncs(equalsFuncs map[string]*equalsFuncData) (string, error) {
	names := make([]string, 0, len(equalsFuncs))
	for name := range equalsFuncs {
		names = append(names, name)
	}
	slices.Sort(names)

	blocks := make([]string, 0, len(names))
	for _, name := range names {
		body, err := renderEqualsTemplate(tmplStructEqualsFunc, *equalsFuncs[name])
		if err != nil {
			return "", err
		}
		blocks = append(blocks, body)
	}
	return strings.Join(blocks, "\n\n"), nil
}

func renderEqualsTemplate(tmplName string, data equalsFuncData) (string, error) {
	var sb strings.Builder
	if err := equalsTmpl.ExecuteTemplate(&sb, tmplName, data); err != nil {
		return "", fmt.Errorf("rendering %s: %w", tmplName, err)
	}
	return sb.String(), nil
}
