package openapi3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-openapi/jsonpointer"
)

type Links map[string]*LinkRef

// JSONLookup implements https://pkg.go.dev/github.com/go-openapi/jsonpointer#JSONPointable
func (links Links) JSONLookup(token string) (interface{}, error) {
	ref, ok := links[token]
	if !ok {
		return nil, fmt.Errorf("object has no field %q", token)
	}

	if ref != nil && ref.Ref != "" {
		return &Ref{Ref: ref.Ref}, nil
	}
	return ref.Value, nil
}

var _ jsonpointer.JSONPointable = (*Links)(nil)

// Link is specified by OpenAPI/Swagger standard version 3.
// See https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#link-object
type Link struct {
	Extensions map[string]interface{} `json:"-" yaml:"-"`

	OperationRef string                 `json:"operationRef,omitempty" yaml:"operationRef,omitempty"`
	OperationID  string                 `json:"operationId,omitempty" yaml:"operationId,omitempty"`
	Description  string                 `json:"description,omitempty" yaml:"description,omitempty"`
	Parameters   map[string]interface{} `json:"parameters,omitempty" yaml:"parameters,omitempty"`
	Server       *Server                `json:"server,omitempty" yaml:"server,omitempty"`
	RequestBody  interface{}            `json:"requestBody,omitempty" yaml:"requestBody,omitempty"`
}

// MarshalJSON returns the JSON encoding of Link.
func (link Link) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{}, 6+len(link.Extensions))
	for k, v := range link.Extensions {
		m[k] = v
	}

	if x := link.OperationRef; x != "" {
		m["operationRef"] = x
	}
	if x := link.OperationID; x != "" {
		m["operationId"] = x
	}
	if x := link.Description; x != "" {
		m["description"] = x
	}
	if x := link.Parameters; len(x) != 0 {
		m["parameters"] = x
	}
	if x := link.Server; x != nil {
		m["server"] = x
	}
	if x := link.RequestBody; x != nil {
		m["requestBody"] = x
	}

	return json.Marshal(m)
}

// UnmarshalJSON sets Link to a copy of data.
func (link *Link) UnmarshalJSON(data []byte) error {
	type LinkBis Link
	var x LinkBis
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	_ = json.Unmarshal(data, &x.Extensions)
	delete(x.Extensions, "operationRef")
	delete(x.Extensions, "operationId")
	delete(x.Extensions, "description")
	delete(x.Extensions, "parameters")
	delete(x.Extensions, "server")
	delete(x.Extensions, "requestBody")
	*link = Link(x)
	return nil
}

// Validate returns an error if Link does not comply with the OpenAPI spec.
func (link *Link) Validate(ctx context.Context, opts ...ValidationOption) error {
	ctx = WithValidationOptions(ctx, opts...)

	if link.OperationID == "" && link.OperationRef == "" {
		return errors.New("missing operationId or operationRef on link")
	}
	if link.OperationID != "" && link.OperationRef != "" {
		return fmt.Errorf("operationId %q and operationRef %q are mutually exclusive", link.OperationID, link.OperationRef)
	}

	return validateExtensions(ctx, link.Extensions)
}
