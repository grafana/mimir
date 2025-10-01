// SPDX-License-Identifier: AGPL-3.0-only

package propagation

import (
	"context"
	"net/http"
	"net/textproto"
)

// Why does this package exist?
//
// We currently support two paths for sending requests to queriers:
// - httpgrpc from query-frontends and query-schedulers / HTTP direct to queriers (previously this was the only path)
// - Protobuf from query-frontends and query-schedulers (this is the new path, only used by remote execution at the time of writing)
//
// There are a number of pieces of auxiliary information that need to be passed for requests on both of these paths, such as
// strong consistency information and chunk info debugging and GEM features like authentication and LBAC.
// We don't want to have to implement each of these twice.
//
// So this package acts as an abstraction layer for the two paths, allowing for injection and extraction of auxiliary
// information for requests.

// Why can't we use gRPC metadata?
//
// We use long-lived streams between the query-frontend and the query-scheduler, and gRPC metadata can only
// be attached to the stream, rather than individual messages sent over the stream.

// Extractor represents something that extracts auxiliary information from a request.
type Extractor interface {
	// ExtractFromCarrier extracts auxiliary information from a request (represented by carrier)
	// and returns a new context derived from ctx with that information included.
	ExtractFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error)
}

// Injector represents something that adds auxiliary information to a request.
type Injector interface {
	// InjectToCarrier injects auxiliary information into a request (represented by carrier).
	InjectToCarrier(ctx context.Context, carrier Carrier) error
}

type NoopExtractor struct{}

func (e *NoopExtractor) ExtractFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error) {
	return ctx, nil
}

type MultiExtractor struct {
	Extractors []Extractor
}

func (m *MultiExtractor) ExtractFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error) {
	for _, e := range m.Extractors {
		var err error
		ctx, err = e.ExtractFromCarrier(ctx, carrier)
		if err != nil {
			return nil, err
		}
	}

	return ctx, nil
}

type MultiInjector struct {
	Injectors []Injector
}

func (m *MultiInjector) InjectToCarrier(ctx context.Context, carrier Carrier) error {
	for _, i := range m.Injectors {
		if err := i.InjectToCarrier(ctx, carrier); err != nil {
			return err
		}
	}

	return nil
}

type NoopInjector struct{}

func (i *NoopInjector) InjectToCarrier(ctx context.Context, carrier Carrier) error {
	return nil
}

// Carrier represents a carrier of key-value pairs for a request, such as HTTP headers.
//
// Keys provided will be canonicalized by textproto.CanonicalMIMEHeaderKey.
type Carrier interface {
	// Get returns the value with the given name, or an empty string if it is not present.
	Get(name string) string

	// GetAll returns all values with the given name, or a nil slice if it is not present.
	GetAll(name string) []string

	// Add adds value to the values stored for name.
	Add(name, value string)

	// SetAll sets the values stored for name to value.
	SetAll(name string, value []string)
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

func (m MapCarrier) Add(name, value string) {
	key := textproto.CanonicalMIMEHeaderKey(name)
	m[key] = append(m[key], value)
}

func (m MapCarrier) SetAll(name string, value []string) {
	m[textproto.CanonicalMIMEHeaderKey(name)] = value
}

type HttpHeaderCarrier http.Header

func (h HttpHeaderCarrier) Get(name string) string {
	// We don't need to canonicalize name here, because Get does that for us.
	return http.Header(h).Get(name)
}

func (h HttpHeaderCarrier) GetAll(name string) []string {
	return http.Header(h).Values(name)
}

func (h HttpHeaderCarrier) Add(name, value string) {
	http.Header(h).Add(name, value)
}

func (h HttpHeaderCarrier) SetAll(name string, value []string) {
	h[textproto.CanonicalMIMEHeaderKey(name)] = value
}
