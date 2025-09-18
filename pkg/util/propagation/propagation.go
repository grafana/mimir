// SPDX-License-Identifier: AGPL-3.0-only

package propagation

import (
	"context"
	"net/http"
)

// Propagator represents something that extracts auxiliary information to and from a request.
type Propagator interface {
	// ReadFromCarrier extracts auxiliary information from a request (represented by a carrier)
	// and returns a new context derived from ctx with that information included.
	ReadFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error)
}

type NoopPropagator struct{}

func (p *NoopPropagator) ReadFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error) {
	return ctx, nil
}

type MultiPropagator struct {
	Propagators []Propagator
}

func (m *MultiPropagator) ReadFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error) {
	for _, p := range m.Propagators {
		var err error
		ctx, err = p.ReadFromCarrier(ctx, carrier)
		if err != nil {
			return nil, err
		}
	}

	return ctx, nil
}

// Carrier represents a carrier of key-value pairs for a request, such as HTTP headers.
type Carrier interface {
	// Get returns the value with the given name, or an empty string if it is not present.
	Get(name string) string
}

type MapCarrier map[string]string

func (m MapCarrier) Get(name string) string {
	return m[name]
}

type HttpHeaderCarrier http.Header

func (h HttpHeaderCarrier) Get(name string) string {
	return http.Header(h).Get(name)
}
