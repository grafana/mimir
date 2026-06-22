// SPDX-License-Identifier: AGPL-3.0-only

package httpgrpcpb

import (
	"github.com/grafana/dskit/httpgrpc"
)

// HeadersCarrier implements the OTel TextMapCarrier interface over an HTTPRequest,
// allowing trace context to be injected into or extracted from request headers.
// It mirrors httpgrpcutil.HttpgrpcHeadersCarrier but operates on the local bridge type.
type HeadersCarrier HTTPRequest

func (c *HeadersCarrier) Get(key string) string {
	for _, h := range c.Headers {
		if h != nil && h.Key == key && len(h.Values) > 0 {
			return h.Values[0]
		}
	}
	return ""
}

func (c *HeadersCarrier) Keys() []string {
	keys := make([]string, 0, len(c.Headers))
	for _, h := range c.Headers {
		if h != nil {
			keys = append(keys, h.Key)
		}
	}
	return keys
}

func (c *HeadersCarrier) Set(key, val string) {
	for _, h := range c.Headers {
		if h != nil && h.Key == key {
			h.Values = []string{val}
			return
		}
	}
	c.Headers = append(c.Headers, &Header{Key: key, Values: []string{val}})
}

// FromHTTPRequest converts a dskit httpgrpc request into the local
// wire-identical representation. nil maps to nil.
func FromHTTPRequest(r *httpgrpc.HTTPRequest) *HTTPRequest {
	if r == nil {
		return nil
	}
	return &HTTPRequest{
		Method:  r.Method,
		Url:     r.Url,
		Headers: fromHeaders(r.Headers),
		Body:    r.Body,
	}
}

// ToHTTPRequest converts the local representation back to the dskit type.
func ToHTTPRequest(r *HTTPRequest) *httpgrpc.HTTPRequest {
	if r == nil {
		return nil
	}
	return &httpgrpc.HTTPRequest{
		Method:  r.Method,
		Url:     r.Url,
		Headers: toHeaders(r.Headers),
		Body:    r.Body,
	}
}

// FromHTTPResponse converts a dskit httpgrpc response into the local
// wire-identical representation. nil maps to nil.
func FromHTTPResponse(r *httpgrpc.HTTPResponse) *HTTPResponse {
	if r == nil {
		return nil
	}
	return &HTTPResponse{
		Code:    r.Code,
		Headers: fromHeaders(r.Headers),
		Body:    r.Body,
	}
}

// ToHTTPResponse converts the local representation back to the dskit type.
func ToHTTPResponse(r *HTTPResponse) *httpgrpc.HTTPResponse {
	if r == nil {
		return nil
	}
	return &httpgrpc.HTTPResponse{
		Code:    r.Code,
		Headers: toHeaders(r.Headers),
		Body:    r.Body,
	}
}

// FromHeaders converts a slice of dskit httpgrpc headers to the local bridge type.
func FromHeaders(hs []*httpgrpc.Header) []*Header {
	return fromHeaders(hs)
}

// ToHeaders converts a slice of local bridge headers to the dskit httpgrpc type.
func ToHeaders(hs []*Header) []*httpgrpc.Header {
	return toHeaders(hs)
}

func fromHeaders(hs []*httpgrpc.Header) []*Header {
	if hs == nil {
		return nil
	}
	out := make([]*Header, 0, len(hs))
	for _, h := range hs {
		if h == nil {
			continue
		}
		out = append(out, &Header{Key: h.Key, Values: h.Values})
	}
	return out
}

func toHeaders(hs []*Header) []*httpgrpc.Header {
	if hs == nil {
		return nil
	}
	out := make([]*httpgrpc.Header, 0, len(hs))
	for _, h := range hs {
		if h == nil {
			continue
		}
		out = append(out, &httpgrpc.Header{Key: h.Key, Values: h.Values})
	}
	return out
}
