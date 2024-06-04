package openapi3

import (
	"context"
	"encoding/json"
	"errors"
)

// Info is specified by OpenAPI/Swagger standard version 3.
// See https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#info-object
type Info struct {
	Extensions map[string]interface{} `json:"-" yaml:"-"`

	Title          string   `json:"title" yaml:"title"` // Required
	Description    string   `json:"description,omitempty" yaml:"description,omitempty"`
	TermsOfService string   `json:"termsOfService,omitempty" yaml:"termsOfService,omitempty"`
	Contact        *Contact `json:"contact,omitempty" yaml:"contact,omitempty"`
	License        *License `json:"license,omitempty" yaml:"license,omitempty"`
	Version        string   `json:"version" yaml:"version"` // Required
}

// MarshalJSON returns the JSON encoding of Info.
func (info Info) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{}, 6+len(info.Extensions))
	for k, v := range info.Extensions {
		m[k] = v
	}
	m["title"] = info.Title
	if x := info.Description; x != "" {
		m["description"] = x
	}
	if x := info.TermsOfService; x != "" {
		m["termsOfService"] = x
	}
	if x := info.Contact; x != nil {
		m["contact"] = x
	}
	if x := info.License; x != nil {
		m["license"] = x
	}
	m["version"] = info.Version
	return json.Marshal(m)
}

// UnmarshalJSON sets Info to a copy of data.
func (info *Info) UnmarshalJSON(data []byte) error {
	type InfoBis Info
	var x InfoBis
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	_ = json.Unmarshal(data, &x.Extensions)
	delete(x.Extensions, "title")
	delete(x.Extensions, "description")
	delete(x.Extensions, "termsOfService")
	delete(x.Extensions, "contact")
	delete(x.Extensions, "license")
	delete(x.Extensions, "version")
	*info = Info(x)
	return nil
}

// Validate returns an error if Info does not comply with the OpenAPI spec.
func (info *Info) Validate(ctx context.Context, opts ...ValidationOption) error {
	ctx = WithValidationOptions(ctx, opts...)

	if contact := info.Contact; contact != nil {
		if err := contact.Validate(ctx); err != nil {
			return err
		}
	}

	if license := info.License; license != nil {
		if err := license.Validate(ctx); err != nil {
			return err
		}
	}

	if info.Version == "" {
		return errors.New("value of version must be a non-empty string")
	}

	if info.Title == "" {
		return errors.New("value of title must be a non-empty string")
	}

	return validateExtensions(ctx, info.Extensions)
}
