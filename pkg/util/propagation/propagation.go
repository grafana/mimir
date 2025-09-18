// SPDX-License-Identifier: AGPL-3.0-only

package propagation

import (
	"context"
	"net/http"
	"net/textproto"
)

// Extractor represents something that extracts auxiliary information from a request.
type Extractor interface {
	// ReadFromCarrier extracts auxiliary information from a request (represented by a carrier)
	// and returns a new context derived from ctx with that information included.
	ReadFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error)
}

type NoopExtractor struct{}

func (e *NoopExtractor) ReadFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error) {
	return ctx, nil
}

type MultiExtractor struct {
	Extractors []Extractor
}

func (m *MultiExtractor) ReadFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error) {
	for _, e := range m.Extractors {
		var err error
		ctx, err = e.ReadFromCarrier(ctx, carrier)
		if err != nil {
			return nil, err
		}
	}

	return ctx, nil
}

// Carrier represents a carrier of key-value pairs for a request, such as HTTP headers.
//
// Keys provided will be canonicalized by textproto.CanonicalMIMEHeaderKey.
type Carrier interface {
	// Get returns the value with the given name, or an empty string if it is not present.
	Get(name string) string

	// GetAll returns all values with the given name, or a nil slice if it is not present.
	GetAll(name string) []string
}

type MapCarrier map[string][]string

func (m MapCarrier) Get(name string) string {
	if values := m.GetAll(name); len(values) > 0 {
		return values[0]
	}

	return ""
}

func (m MapCarrier) GetAll(name string) []string {
	return m[textproto.CanonicalMIMEHeaderKey(name)]
}

type HttpHeaderCarrier http.Header

func (h HttpHeaderCarrier) Get(name string) string {
	// We don't need to canonicalize name here, because Get does that for us.
	return http.Header(h).Get(name)
}

func (h HttpHeaderCarrier) GetAll(name string) []string {
	return h[textproto.CanonicalMIMEHeaderKey(name)]
}
