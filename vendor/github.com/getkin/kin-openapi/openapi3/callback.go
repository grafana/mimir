package openapi3

import (
	"context"
	"fmt"
	"sort"

	"github.com/go-openapi/jsonpointer"
)

type Callbacks map[string]*CallbackRef

var _ jsonpointer.JSONPointable = (*Callbacks)(nil)

// JSONLookup implements https://pkg.go.dev/github.com/go-openapi/jsonpointer#JSONPointable
func (c Callbacks) JSONLookup(token string) (interface{}, error) {
	ref, ok := c[token]
	if ref == nil || !ok {
		return nil, fmt.Errorf("object has no field %q", token)
	}

	if ref.Ref != "" {
		return &Ref{Ref: ref.Ref}, nil
	}
	return ref.Value, nil
}

// Callback is specified by OpenAPI/Swagger standard version 3.
// See https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#callback-object
type Callback map[string]*PathItem

// Validate returns an error if Callback does not comply with the OpenAPI spec.
func (callback Callback) Validate(ctx context.Context, opts ...ValidationOption) error {
	ctx = WithValidationOptions(ctx, opts...)

	keys := make([]string, 0, len(callback))
	for key := range callback {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		v := callback[key]
		if err := v.Validate(ctx); err != nil {
			return err
		}
	}
	return nil
}
