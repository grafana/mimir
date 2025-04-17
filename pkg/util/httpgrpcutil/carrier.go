// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/grpcutil/carrier.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package httpgrpcutil

import (
	"context"

	"github.com/grafana/dskit/httpgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Used to transfer trace information from/to HTTP request.
type HttpgrpcHeadersCarrier httpgrpc.HTTPRequest

func (c *HttpgrpcHeadersCarrier) Get(key string) string {
	for _, h := range c.Headers {
		if h.Key == key && len(h.Values) > 0 {
			return h.Values[0]
		}
	}
	return ""
}

func (c *HttpgrpcHeadersCarrier) Keys() []string {
	keys := make([]string, 0, len(c.Headers))
	for _, h := range c.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

func (c *HttpgrpcHeadersCarrier) Set(key, val string) {
	c.Headers = append(c.Headers, &httpgrpc.Header{
		Key:    key,
		Values: []string{val},
	})
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

func ContextWithSpanFromRequest(ctx context.Context, req *httpgrpc.HTTPRequest) (context.Context, bool) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, (*HttpgrpcHeadersCarrier)(req))
	return ctx, trace.SpanFromContext(ctx).SpanContext().IsValid()
}
