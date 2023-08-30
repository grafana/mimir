// SPDX-License-Identifier: AGPL-3.0-only

package instrumentation

import (
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// TracerTransport uses opentracing.GlobalTracer() to inject request trace span (if any) to the request headers,
// then it passes then handling to Next, or to http.DefaultTransport if Next is nil.
type TracerTransport struct {
	Next http.RoundTripper
}

func (t TracerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tracer, span := otel.Tracer("github.com/grafana/mimir"), trace.SpanFromContext(req.Context())
	if tracer != nil && span != nil {
		carrier := opentracing.HTTPHeadersCarrier(req.Header)
		err := tracer.Inject(span.Context(), opentracing.HTTPHeaders, carrier)
		if err != nil {
			return nil, err
		}
	}
	next := t.Next
	if next == nil {
		next = http.DefaultTransport
	}
	return next.RoundTrip(req)
}
