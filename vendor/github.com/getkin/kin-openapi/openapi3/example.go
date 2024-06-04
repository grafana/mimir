package openapi3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-openapi/jsonpointer"
)

type Examples map[string]*ExampleRef

var _ jsonpointer.JSONPointable = (*Examples)(nil)

// JSONLookup implements https://pkg.go.dev/github.com/go-openapi/jsonpointer#JSONPointable
func (e Examples) JSONLookup(token string) (interface{}, error) {
	ref, ok := e[token]
	if ref == nil || !ok {
		return nil, fmt.Errorf("object has no field %q", token)
	}

	if ref.Ref != "" {
		return &Ref{Ref: ref.Ref}, nil
	}
	return ref.Value, nil
}

// Example is specified by OpenAPI/Swagger 3.0 standard.
// See https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#example-object
type Example struct {
	Extensions map[string]interface{} `json:"-" yaml:"-"`

	Summary       string      `json:"summary,omitempty" yaml:"summary,omitempty"`
	Description   string      `json:"description,omitempty" yaml:"description,omitempty"`
	Value         interface{} `json:"value,omitempty" yaml:"value,omitempty"`
	ExternalValue string      `json:"externalValue,omitempty" yaml:"externalValue,omitempty"`
}

func NewExample(value interface{}) *Example {
	return &Example{Value: value}
}

// MarshalJSON returns the JSON encoding of Example.
func (example Example) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{}, 4+len(example.Extensions))
	for k, v := range example.Extensions {
		m[k] = v
	}
	if x := example.Summary; x != "" {
		m["summary"] = x
	}
	if x := example.Description; x != "" {
		m["description"] = x
	}
	if x := example.Value; x != nil {
		m["value"] = x
	}
	if x := example.ExternalValue; x != "" {
		m["externalValue"] = x
	}
	return json.Marshal(m)
}

// UnmarshalJSON sets Example to a copy of data.
func (example *Example) UnmarshalJSON(data []byte) error {
	type ExampleBis Example
	var x ExampleBis
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	_ = json.Unmarshal(data, &x.Extensions)
	delete(x.Extensions, "summary")
	delete(x.Extensions, "description")
	delete(x.Extensions, "value")
	delete(x.Extensions, "externalValue")
	*example = Example(x)
	return nil
}

// Validate returns an error if Example does not comply with the OpenAPI spec.
func (example *Example) Validate(ctx context.Context, opts ...ValidationOption) error {
	ctx = WithValidationOptions(ctx, opts...)

	if example.Value != nil && example.ExternalValue != "" {
		return errors.New("value and externalValue are mutually exclusive")
	}
	if example.Value == nil && example.ExternalValue == "" {
		return errors.New("no value or externalValue field")
	}

	return validateExtensions(ctx, example.Extensions)
}
