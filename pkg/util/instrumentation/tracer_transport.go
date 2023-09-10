// SPDX-License-Identifier: AGPL-3.0-only

package instrumentation

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/otel/trace"
)

// TracerTransport inject context with tracing info to the request headers,
// then it passes then handling to Next, or to http.DefaultTransport if Next is nil.
type TracerTransport struct {
	Next http.RoundTripper
}

func (t TracerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	sp := trace.SpanFromContext(req.Context())
	if sp.SpanContext().IsValid() {
		otelhttptrace.Inject(req.Context(), req)
	}

	next := t.Next
	if next == nil {
		next = http.DefaultTransport
	}
	return next.RoundTrip(req)
}
