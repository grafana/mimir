// SPDX-License-Identifier: AGPL-3.0-only

package instrumentation

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/user"
	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

func TestTracerTransportPropagatesTrace(t *testing.T) {
	for _, tc := range []struct {
		name          string
		nextTransport http.RoundTripper
		handlerAssert func(t *testing.T, req *http.Request)
	}{
		{
			name:          "no next transport",
			handlerAssert: func(*testing.T, *http.Request) {},
		},
		{
			name: "with next transport",
			nextTransport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				r.Header.Set("X-Testing", "True")
				return http.DefaultTransport.RoundTrip(r)
			}),
			handlerAssert: func(t *testing.T, req *http.Request) {
				assert.Equal(t, "True", req.Header.Get("X-Testing"))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			closer, err := config.Configuration{}.InitGlobalTracer("test")
			require.NoError(t, err)
			defer closer.Close()

			observedTraceID := make(chan string, 2)
			handler := middleware.Tracer{}.Wrap(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				sp := opentracing.SpanFromContext(r.Context())
				defer sp.Finish()

				observedTraceID <- spanTraceID(sp)
				tc.handlerAssert(t, r)
			}))
			srv := httptest.NewServer(handler)
			defer srv.Close()

			sp, ctx := opentracing.StartSpanFromContext(context.Background(), "client")
			defer sp.Finish()
			traceID := spanTraceID(sp)

			req, err := http.NewRequestWithContext(ctx, "GET", srv.URL, nil)
			require.NoError(t, err)
			require.NoError(t, user.InjectOrgIDIntoHTTPRequest(user.InjectOrgID(ctx, "1"), req))

			client := http.Client{Transport: &TracerTransport{Next: tc.nextTransport}}
			resp, err := client.Do(req)
			require.NoError(t, err)
			require.Equal(t, 200, resp.StatusCode)
			defer resp.Body.Close()

			// Query should do one call.
			assert.Equal(t, traceID, <-observedTraceID)
		})
	}
}

func spanTraceID(sp opentracing.Span) string {
	traceID := fmt.Sprintf("%v", sp.Context().(jaeger.SpanContext).TraceID())
	return traceID
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
