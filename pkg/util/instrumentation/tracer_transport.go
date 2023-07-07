// SPDX-License-Identifier: AGPL-3.0-only

package instrumentation

import (
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// TracerTransport uses opentelemetry.GlobalTracerProvider() to inject request trace span (if any) to the request headers,
// then it passes then handling to Next, or to http.DefaultTransport if Next is nil.
type TracerTransport struct {
	Next http.RoundTripper
}

func (t TracerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	otel.GetTextMapPropagator().Inject(req.Context(), propagation.HeaderCarrier(req.Header))
	next := t.Next
	if next == nil {
		next = http.DefaultTransport
	}
	return next.RoundTrip(req)
}
