// SPDX-License-Identifier: AGPL-3.0-only

package instrumentation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tracing"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	jaegerpropagator "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestTracerTransportPropagatesTrace(t *testing.T) {
	for _, tc := range []struct {
		name          string
		nextTransport http.RoundTripper
		handlerAssert func(t *testing.T, req *http.Request)
	}{
		{
			name:          "no next transport",
			handlerAssert: func(t *testing.T, req *http.Request) {},
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
			exp := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(
				sdktrace.WithBatcher(exp),
				sdktrace.WithSampler(sdktrace.AlwaysSample()),
			)

			otel.SetTracerProvider(tp)

			otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator([]propagation.TextMapPropagator{
				// w3c Propagator is the default propagator for opentelemetry
				propagation.TraceContext{}, propagation.Baggage{},
				// jaeger Propagator is for opentracing backwards compatibility
				jaegerpropagator.Jaeger{},
			}...))

			defer tp.Shutdown(context.Background())

			observedTraceID := make(chan string, 2)
			handler := middleware.Tracer{}.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				id, _ := tracing.ExtractOtelSampledTraceID(r.Context())
				observedTraceID <- id
				tc.handlerAssert(t, r)
			}))

			srv := httptest.NewServer(handler)
			defer srv.Close()

			ctx, sp := otel.Tracer("github.com/grafana/mimir").Start(context.Background(), "client")
			defer sp.End()

			traceID, _ := tracing.ExtractOtelSampledTraceID(ctx)
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
