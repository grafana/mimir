package openapi3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-openapi/jsonpointer"
)

type RequestBodies map[string]*RequestBodyRef

var _ jsonpointer.JSONPointable = (*RequestBodyRef)(nil)

// JSONLookup implements https://pkg.go.dev/github.com/go-openapi/jsonpointer#JSONPointable
func (r RequestBodies) JSONLookup(token string) (interface{}, error) {
	ref, ok := r[token]
	if !ok {
		return nil, fmt.Errorf("object has no field %q", token)
	}

	if ref != nil && ref.Ref != "" {
		return &Ref{Ref: ref.Ref}, nil
	}
	return ref.Value, nil
}

// RequestBody is specified by OpenAPI/Swagger 3.0 standard.
// See https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#request-body-object
type RequestBody struct {
	Extensions map[string]interface{} `json:"-" yaml:"-"`

	Description string  `json:"description,omitempty" yaml:"description,omitempty"`
	Required    bool    `json:"required,omitempty" yaml:"required,omitempty"`
	Content     Content `json:"content" yaml:"content"`
}

func NewRequestBody() *RequestBody {
	return &RequestBody{}
}

func (requestBody *RequestBody) WithDescription(value string) *RequestBody {
	requestBody.Description = value
	return requestBody
}

func (requestBody *RequestBody) WithRequired(value bool) *RequestBody {
	requestBody.Required = value
	return requestBody
}

func (requestBody *RequestBody) WithContent(content Content) *RequestBody {
	requestBody.Content = content
	return requestBody
}

func (requestBody *RequestBody) WithSchemaRef(value *SchemaRef, consumes []string) *RequestBody {
	requestBody.Content = NewContentWithSchemaRef(value, consumes)
	return requestBody
}

func (requestBody *RequestBody) WithSchema(value *Schema, consumes []string) *RequestBody {
	requestBody.Content = NewContentWithSchema(value, consumes)
	return requestBody
}

func (requestBody *RequestBody) WithJSONSchemaRef(value *SchemaRef) *RequestBody {
	requestBody.Content = NewContentWithJSONSchemaRef(value)
	return requestBody
}

func (requestBody *RequestBody) WithJSONSchema(value *Schema) *RequestBody {
	requestBody.Content = NewContentWithJSONSchema(value)
	return requestBody
}

func (requestBody *RequestBody) WithFormDataSchemaRef(value *SchemaRef) *RequestBody {
	requestBody.Content = NewContentWithFormDataSchemaRef(value)
	return requestBody
}

func (requestBody *RequestBody) WithFormDataSchema(value *Schema) *RequestBody {
	requestBody.Content = NewContentWithFormDataSchema(value)
	return requestBody
}

func (requestBody *RequestBody) GetMediaType(mediaType string) *MediaType {
	m := requestBody.Content
	if m == nil {
		return nil
	}
	return m[mediaType]
}

// MarshalJSON returns the JSON encoding of RequestBody.
func (requestBody RequestBody) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{}, 3+len(requestBody.Extensions))
	for k, v := range requestBody.Extensions {
		m[k] = v
	}
	if x := requestBody.Description; x != "" {
		m["description"] = requestBody.Description
	}
	if x := requestBody.Required; x {
		m["required"] = x
	}
	if x := requestBody.Content; true {
		m["content"] = x
	}
	return json.Marshal(m)
}

// UnmarshalJSON sets RequestBody to a copy of data.
func (requestBody *RequestBody) UnmarshalJSON(data []byte) error {
	type RequestBodyBis RequestBody
	var x RequestBodyBis
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	_ = json.Unmarshal(data, &x.Extensions)
	delete(x.Extensions, "description")
	delete(x.Extensions, "required")
	delete(x.Extensions, "content")
	*requestBody = RequestBody(x)
	return nil
}

// Validate returns an error if RequestBody does not comply with the OpenAPI spec.
func (requestBody *RequestBody) Validate(ctx context.Context, opts ...ValidationOption) error {
	ctx = WithValidationOptions(ctx, opts...)

	if requestBody.Content == nil {
		return errors.New("content of the request body is required")
	}

	if vo := getValidationOptions(ctx); !vo.examplesValidationDisabled {
		vo.examplesValidationAsReq, vo.examplesValidationAsRes = true, false
	}

	if err := requestBody.Content.Validate(ctx); err != nil {
		return err
	}

	return validateExtensions(ctx, requestBody.Extensions)
}
