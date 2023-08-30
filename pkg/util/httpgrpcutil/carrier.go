// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/grpcutil/carrier.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package httpgrpcutil

import (
	"context"

	"github.com/grafana/dskit/httpgrpc"
	"go.opentelemetry.io/otel"
)

// Used to transfer trace information from/to HTTP request.
type HttpgrpcHeadersCarrier httpgrpc.HTTPRequest

func (c *HttpgrpcHeadersCarrier) Set(key, val string) {
	c.Headers = append(c.Headers, &httpgrpc.Header{
		Key:    key,
		Values: []string{val},
	})
}

func (c *HttpgrpcHeadersCarrier) Get(key string) string {
	for _, h := range c.Headers {
		if h.Key == key {
			return h.Values[0]
		}
	}
	return ""
}

func (c *HttpgrpcHeadersCarrier) Keys() []string {
	keys := make([]string, len(c.Headers))
	for i, h := range c.Headers {
		keys[i] = h.Key
	}
	return keys
}

func (c *HttpgrpcHeadersCarrier) ForeachKey(handler func(key, val string) error) error {
	for _, h := range c.Headers {
		for _, v := range h.Values {
			if err := handler(h.Key, v); err != nil {
				return err
			}
		}
	}
	return nil
}

func GetParentSpanForRequest(req *httpgrpc.HTTPRequest) context.Context {
	propagators := otel.GetTextMapPropagator()
	carrier := (*HttpgrpcHeadersCarrier)(req)
	ctx := propagators.Extract(context.Background(), carrier)
	return ctx
}
