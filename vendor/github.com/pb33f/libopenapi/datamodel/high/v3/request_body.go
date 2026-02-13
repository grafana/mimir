// Copyright 2022-2023 Princess B33f Heavy Industries / Dave Shanley
// SPDX-License-Identifier: MIT

package v3

import (
	"github.com/pb33f/libopenapi/datamodel/high"
	low "github.com/pb33f/libopenapi/datamodel/low/v3"
	"github.com/pb33f/libopenapi/orderedmap"
	"github.com/pb33f/libopenapi/utils"
	"go.yaml.in/yaml/v4"
)

// RequestBody represents a high-level OpenAPI 3+ RequestBody object, backed by a low-level one.
//   - https://spec.openapis.org/oas/v3.1.0#request-body-object
type RequestBody struct {
	Reference   string                              `json:"$ref,omitempty" yaml:"$ref,omitempty"`
	Description string                              `json:"description,omitempty" yaml:"description,omitempty"`
	Content     *orderedmap.Map[string, *MediaType] `json:"content,omitempty" yaml:"content,omitempty"`
	Required    *bool                               `json:"required,omitempty" yaml:"required,renderZero,omitempty"`
	Extensions  *orderedmap.Map[string, *yaml.Node] `json:"-" yaml:"-"`
	low         *low.RequestBody
}

// NewRequestBody will create a new high-level RequestBody instance, from a low-level one.
func NewRequestBody(rb *low.RequestBody) *RequestBody {
	r := new(RequestBody)
	r.low = rb
	r.Description = rb.Description.Value
	if !rb.Required.IsEmpty() {
		r.Required = &rb.Required.Value
	}
	r.Extensions = high.ExtractExtensions(rb.Extensions)
	r.Content = ExtractContent(rb.Content.Value)
	return r
}

// GoLow returns the low-level RequestBody instance used to create the high-level one.
func (r *RequestBody) GoLow() *low.RequestBody {
	return r.low
}

// GoLowUntyped will return the low-level RequestBody instance that was used to create the high-level one, with no type
func (r *RequestBody) GoLowUntyped() any {
	return r.low
}

// IsReference returns true if this RequestBody is a reference to another RequestBody definition.
func (r *RequestBody) IsReference() bool {
	return r.Reference != ""
}

// GetReference returns the reference string if this is a reference RequestBody.
func (r *RequestBody) GetReference() string {
	return r.Reference
}

// Render will return a YAML representation of the RequestBody object as a byte slice.
func (r *RequestBody) Render() ([]byte, error) {
	return yaml.Marshal(r)
}

func (r *RequestBody) RenderInline() ([]byte, error) {
	d, _ := r.MarshalYAMLInline()
	return yaml.Marshal(d)
}

// MarshalYAML will create a ready to render YAML representation of the RequestBody object.
func (r *RequestBody) MarshalYAML() (interface{}, error) {
	// Handle reference-only request body
	if r.Reference != "" {
		return utils.CreateRefNode(r.Reference), nil
	}
	nb := high.NewNodeBuilder(r, r.low)
	return nb.Render(), nil
}

// MarshalYAMLInline will create a ready to render YAML representation of the RequestBody object,
// resolving any references inline where possible.
func (r *RequestBody) MarshalYAMLInline() (interface{}, error) {
	// reference-only objects render as $ref nodes
	if r.Reference != "" {
		return utils.CreateRefNode(r.Reference), nil
	}
	nb := high.NewNodeBuilder(r, r.low)
	nb.Resolve = true
	return nb.Render(), nil
}

// MarshalYAMLInlineWithContext will create a ready to render YAML representation of the RequestBody object,
// resolving any references inline where possible. Uses the provided context for cycle detection.
// The ctx parameter should be *base.InlineRenderContext but is typed as any to satisfy the
// high.RenderableInlineWithContext interface without import cycles.
func (r *RequestBody) MarshalYAMLInlineWithContext(ctx any) (interface{}, error) {
	if r.Reference != "" {
		return utils.CreateRefNode(r.Reference), nil
	}
	nb := high.NewNodeBuilder(r, r.low)
	nb.Resolve = true
	nb.RenderContext = ctx
	return nb.Render(), nil
}

// CreateRequestBodyRef creates a RequestBody that renders as a $ref to another request body definition.
// This is useful when building OpenAPI specs programmatically and you want to reference
// a request body defined in components/requestBodies rather than inlining the full definition.
//
// Example:
//
//	rb := v3.CreateRequestBodyRef("#/components/requestBodies/UserInput")
//
// Renders as:
//
//	$ref: '#/components/requestBodies/UserInput'
func CreateRequestBodyRef(ref string) *RequestBody {
	return &RequestBody{Reference: ref}
}
