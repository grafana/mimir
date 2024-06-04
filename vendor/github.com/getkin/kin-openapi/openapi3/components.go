package openapi3

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// Components is specified by OpenAPI/Swagger standard version 3.
// See https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#components-object
type Components struct {
	Extensions map[string]interface{} `json:"-" yaml:"-"`

	Schemas         Schemas         `json:"schemas,omitempty" yaml:"schemas,omitempty"`
	Parameters      ParametersMap   `json:"parameters,omitempty" yaml:"parameters,omitempty"`
	Headers         Headers         `json:"headers,omitempty" yaml:"headers,omitempty"`
	RequestBodies   RequestBodies   `json:"requestBodies,omitempty" yaml:"requestBodies,omitempty"`
	Responses       Responses       `json:"responses,omitempty" yaml:"responses,omitempty"`
	SecuritySchemes SecuritySchemes `json:"securitySchemes,omitempty" yaml:"securitySchemes,omitempty"`
	Examples        Examples        `json:"examples,omitempty" yaml:"examples,omitempty"`
	Links           Links           `json:"links,omitempty" yaml:"links,omitempty"`
	Callbacks       Callbacks       `json:"callbacks,omitempty" yaml:"callbacks,omitempty"`
}

func NewComponents() Components {
	return Components{}
}

// MarshalJSON returns the JSON encoding of Components.
func (components Components) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{}, 9+len(components.Extensions))
	for k, v := range components.Extensions {
		m[k] = v
	}
	if x := components.Schemas; len(x) != 0 {
		m["schemas"] = x
	}
	if x := components.Parameters; len(x) != 0 {
		m["parameters"] = x
	}
	if x := components.Headers; len(x) != 0 {
		m["headers"] = x
	}
	if x := components.RequestBodies; len(x) != 0 {
		m["requestBodies"] = x
	}
	if x := components.Responses; len(x) != 0 {
		m["responses"] = x
	}
	if x := components.SecuritySchemes; len(x) != 0 {
		m["securitySchemes"] = x
	}
	if x := components.Examples; len(x) != 0 {
		m["examples"] = x
	}
	if x := components.Links; len(x) != 0 {
		m["links"] = x
	}
	if x := components.Callbacks; len(x) != 0 {
		m["callbacks"] = x
	}
	return json.Marshal(m)
}

// UnmarshalJSON sets Components to a copy of data.
func (components *Components) UnmarshalJSON(data []byte) error {
	type ComponentsBis Components
	var x ComponentsBis
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	_ = json.Unmarshal(data, &x.Extensions)
	delete(x.Extensions, "schemas")
	delete(x.Extensions, "parameters")
	delete(x.Extensions, "headers")
	delete(x.Extensions, "requestBodies")
	delete(x.Extensions, "responses")
	delete(x.Extensions, "securitySchemes")
	delete(x.Extensions, "examples")
	delete(x.Extensions, "links")
	delete(x.Extensions, "callbacks")
	*components = Components(x)
	return nil
}

// Validate returns an error if Components does not comply with the OpenAPI spec.
func (components *Components) Validate(ctx context.Context, opts ...ValidationOption) (err error) {
	ctx = WithValidationOptions(ctx, opts...)

	schemas := make([]string, 0, len(components.Schemas))
	for name := range components.Schemas {
		schemas = append(schemas, name)
	}
	sort.Strings(schemas)
	for _, k := range schemas {
		v := components.Schemas[k]
		if err = ValidateIdentifier(k); err != nil {
			return fmt.Errorf("schema %q: %w", k, err)
		}
		if err = v.Validate(ctx); err != nil {
			return fmt.Errorf("schema %q: %w", k, err)
		}
	}

	parameters := make([]string, 0, len(components.Parameters))
	for name := range components.Parameters {
		parameters = append(parameters, name)
	}
	sort.Strings(parameters)
	for _, k := range parameters {
		v := components.Parameters[k]
		if err = ValidateIdentifier(k); err != nil {
			return fmt.Errorf("parameter %q: %w", k, err)
		}
		if err = v.Validate(ctx); err != nil {
			return fmt.Errorf("parameter %q: %w", k, err)
		}
	}

	requestBodies := make([]string, 0, len(components.RequestBodies))
	for name := range components.RequestBodies {
		requestBodies = append(requestBodies, name)
	}
	sort.Strings(requestBodies)
	for _, k := range requestBodies {
		v := components.RequestBodies[k]
		if err = ValidateIdentifier(k); err != nil {
			return fmt.Errorf("request body %q: %w", k, err)
		}
		if err = v.Validate(ctx); err != nil {
			return fmt.Errorf("request body %q: %w", k, err)
		}
	}

	responses := make([]string, 0, len(components.Responses))
	for name := range components.Responses {
		responses = append(responses, name)
	}
	sort.Strings(responses)
	for _, k := range responses {
		v := components.Responses[k]
		if err = ValidateIdentifier(k); err != nil {
			return fmt.Errorf("response %q: %w", k, err)
		}
		if err = v.Validate(ctx); err != nil {
			return fmt.Errorf("response %q: %w", k, err)
		}
	}

	headers := make([]string, 0, len(components.Headers))
	for name := range components.Headers {
		headers = append(headers, name)
	}
	sort.Strings(headers)
	for _, k := range headers {
		v := components.Headers[k]
		if err = ValidateIdentifier(k); err != nil {
			return fmt.Errorf("header %q: %w", k, err)
		}
		if err = v.Validate(ctx); err != nil {
			return fmt.Errorf("header %q: %w", k, err)
		}
	}

	securitySchemes := make([]string, 0, len(components.SecuritySchemes))
	for name := range components.SecuritySchemes {
		securitySchemes = append(securitySchemes, name)
	}
	sort.Strings(securitySchemes)
	for _, k := range securitySchemes {
		v := components.SecuritySchemes[k]
		if err = ValidateIdentifier(k); err != nil {
			return fmt.Errorf("security scheme %q: %w", k, err)
		}
		if err = v.Validate(ctx); err != nil {
			return fmt.Errorf("security scheme %q: %w", k, err)
		}
	}

	examples := make([]string, 0, len(components.Examples))
	for name := range components.Examples {
		examples = append(examples, name)
	}
	sort.Strings(examples)
	for _, k := range examples {
		v := components.Examples[k]
		if err = ValidateIdentifier(k); err != nil {
			return fmt.Errorf("example %q: %w", k, err)
		}
		if err = v.Validate(ctx); err != nil {
			return fmt.Errorf("example %q: %w", k, err)
		}
	}

	links := make([]string, 0, len(components.Links))
	for name := range components.Links {
		links = append(links, name)
	}
	sort.Strings(links)
	for _, k := range links {
		v := components.Links[k]
		if err = ValidateIdentifier(k); err != nil {
			return fmt.Errorf("link %q: %w", k, err)
		}
		if err = v.Validate(ctx); err != nil {
			return fmt.Errorf("link %q: %w", k, err)
		}
	}

	callbacks := make([]string, 0, len(components.Callbacks))
	for name := range components.Callbacks {
		callbacks = append(callbacks, name)
	}
	sort.Strings(callbacks)
	for _, k := range callbacks {
		v := components.Callbacks[k]
		if err = ValidateIdentifier(k); err != nil {
			return fmt.Errorf("callback %q: %w", k, err)
		}
		if err = v.Validate(ctx); err != nil {
			return fmt.Errorf("callback %q: %w", k, err)
		}
	}

	return validateExtensions(ctx, components.Extensions)
}
