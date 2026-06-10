// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	_ "embed"
	"fmt"
	"strings"
	"text/template"
)

// templateStructData carries the per-struct data templates use to render a generated method.
type templateStructData struct {
	Receiver      string
	Type          string
	ChildFields   []childField
	ChildrenField string
	ChildrenMin   int
}

func (d *templateStructData) LastField() childField {
	return d.ChildFields[len(d.ChildFields)-1]
}

// CountWithoutLast returns len(ChildFields) - 1, i.e. the child count when the last (nilable) field is nil.
// Exists because text/template has no arithmetic.
func (d *templateStructData) CountWithoutLast() int {
	return len(d.ChildFields) - 1
}

// childField represents a struct field tagged with child/children.
type childField struct {
	Name       string
	Nilable    bool
	Type       string // source type (e.g. *core.FunctionCall)
	TypeImport string
}

//go:embed child_method.tmpl
var childTmplContent string
var childTmpl = template.Must(template.New("child_method").Parse(childTmplContent))

// ChildMethod emits the Child(idx int) planning.Node method.
var ChildMethod = MethodGenerator{
	Name:     "Child",
	Generate: childMethodGenerate,
}

//go:embed child_count_method.tmpl
var childCountTmplContent string
var childCountTmpl = template.Must(template.New("child_count_method").Parse(childCountTmplContent))

// ChildCountMethod emits the ChildCount() int method.
var ChildCountMethod = MethodGenerator{
	Name:     "ChildCount",
	Generate: childCountMethodGenerate,
}

//go:embed set_children_method.tmpl
var setChildrenTmplContent string
var setChildrenTmpl = template.Must(template.New("set_children_method").Parse(setChildrenTmplContent))

var SetChildrenMethod = MethodGenerator{
	Name:     "SetChildren",
	Generate: setChildrenMethodGenerate,
}

func childMethodGenerate(s *Struct, imports *ImportsCollector) (string, error) {
	data, err := buildTemplateStructData(s)
	if err != nil {
		return "", err
	}

	imports.Add("fmt")
	imports.Add("github.com/grafana/mimir/pkg/streamingpromql/planning")

	var subtmplName string
	switch {
	case data.ChildrenField != "":
		subtmplName = "children_field"
	case len(data.ChildFields) == 1:
		subtmplName = "single_child_field"
	case len(data.ChildFields) > 1:
		subtmplName = "multi_child_fields"
	default:
		subtmplName = "no_fields"
	}

	return renderTemplate(childTmpl, subtmplName, data)
}

func childCountMethodGenerate(s *Struct, _ *ImportsCollector) (string, error) {
	data, err := buildTemplateStructData(s)
	if err != nil {
		return "", err
	}

	var subtmplName string
	switch {
	case data.ChildrenField != "":
		subtmplName = "children_field"
	case len(data.ChildFields) > 0:
		subtmplName = "child_fields"
	default:
		subtmplName = "no_fields"
	}

	return renderTemplate(childCountTmpl, subtmplName, data)
}

func setChildrenMethodGenerate(s *Struct, imports *ImportsCollector) (string, error) {
	data, err := buildTemplateStructData(s)
	if err != nil {
		return "", err
	}

	imports.Add("fmt")
	imports.Add("github.com/grafana/mimir/pkg/streamingpromql/planning")
	for _, cf := range data.ChildFields {
		imports.Add(cf.TypeImport)
	}

	var subtmplName string
	switch {
	case data.ChildrenField != "":
		subtmplName = "children_field"
	case len(data.ChildFields) == 0:
		subtmplName = "no_fields"
	case data.LastField().Nilable:
		subtmplName = "nilable_last"
	default:
		subtmplName = "child_fields"
	}

	return renderTemplate(setChildrenTmpl, subtmplName, data)
}

// buildTemplateStructData validates the tagged fields of the given structure and returns the template input for it.
// Returns an error if the tags violate the supported shape:
//   - node:"children,min=N" must have a non-negative N,
//   - node:"child" and node:"children" cannot be mixed,
//   - at most one node:"children" field,
//   - at most one node:"child,nilable" field, and it must be the last node:"child" field,
//   - node:"child" and node:"children" tags on embedded fields are not supported.
func buildTemplateStructData(s *Struct) (*templateStructData, error) {
	var (
		childFields       []childField
		nilableFieldCount int
		childrenFieldName string
		childrenMin       int
	)
	for _, f := range s.Fields {
		if f.Tag == nil {
			continue
		}
		if f.Embedded {
			if f.Tag.IsChildren || f.Tag.IsChild {
				return nil, fmt.Errorf(`node:"child" and node:"children" tags on embedded fields are not supported`)
			}
			continue
		}
		switch {
		case f.Tag.IsChild:
			if f.Tag.Nilable {
				nilableFieldCount++
			}
			childFields = append(childFields, childField{
				Name:       f.Name,
				Nilable:    f.Tag.Nilable,
				Type:       f.Type.Name,
				TypeImport: f.Type.ImportPath,
			})
		case f.Tag.IsChildren:
			if childrenFieldName != "" {
				return nil, fmt.Errorf(`multiple node:"children" fields are not supported`)
			}
			if f.Tag.Min < 0 {
				return nil, fmt.Errorf(`node:"children" min must be non-negative, got %d`, f.Tag.Min)
			}
			childrenFieldName = f.Name
			childrenMin = f.Tag.Min
		}
	}
	if len(childFields) > 0 && childrenFieldName != "" {
		return nil, fmt.Errorf(`mixing node:"child" and node:"children" is not supported`)
	}
	if nilableFieldCount > 1 {
		return nil, fmt.Errorf(`only one node:"child,nilable" field is supported`)
	}
	if nilableFieldCount == 1 && !childFields[len(childFields)-1].Nilable {
		return nil, fmt.Errorf(`a node:"child,nilable" field must be the last node:"child" field`)
	}

	return &templateStructData{
		Receiver:      receiverName(s.Name),
		Type:          s.Name,
		ChildFields:   childFields,
		ChildrenField: childrenFieldName,
		ChildrenMin:   childrenMin,
	}, nil
}

func renderTemplate(tmpl *template.Template, subtmplName string, data *templateStructData) (string, error) {
	var sb strings.Builder
	if err := tmpl.ExecuteTemplate(&sb, subtmplName, data); err != nil {
		return "", fmt.Errorf("failed to execute template %q: %w", subtmplName, err)
	}
	return sb.String(), nil
}
