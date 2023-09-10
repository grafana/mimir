// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/middleware/http_tracing.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package middleware

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Tracer is a middleware which traces incoming requests.
type Tracer struct {
	RouteMatcher RouteMatcher
	SourceIPs    *SourceIPExtractor
}

// Wrap implements Interface
func (t Tracer) Wrap(next http.Handler) http.Handler {
	options := []otelhttp.Option{
		otelhttp.WithSpanNameFormatter(makeHTTPOperationNameFunc(t.RouteMatcher)),
	}

	// Apply OpenTelemetry tracing middleware first
	tracingMiddleware := otelhttp.NewHandler(next, "http.tracing", options...)

	// Wrap the 'tracingMiddleware' to capture its execution
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		labeler, _ := otelhttp.LabelerFromContext(r.Context())
		// add a tag with the client's user agent to the span
		userAgent := r.Header.Get("User-Agent")
		if userAgent != "" {
			labeler.Add(attribute.String("http.user_agent", userAgent))
		}

		labeler.Add(attribute.String("http.url", r.URL.Path))
		labeler.Add(attribute.String("http.method", r.Method))

		labeler.Add(attribute.String("headers", fmt.Sprintf("%v", r.Header)))
		// add a tag with the client's sourceIPs to the span, if a
		// SourceIPExtractor is given.
		if t.SourceIPs != nil {
			labeler.Add(attribute.String("sourceIPs", t.SourceIPs.Get(r)))
		}

		tracingMiddleware.ServeHTTP(w, r)
	})

	return handler
}

// HTTPGRPCTracer is a middleware which traces incoming httpgrpc requests.
type HTTPGRPCTracer struct {
	RouteMatcher RouteMatcher
}

// InitHTTPGRPCMiddleware initializes gorilla/mux-compatible HTTP middleware
//
// HTTPGRPCTracer is specific to the server-side handling of HTTP requests which were
// wrapped into gRPC requests and routed through the httpgrpc.HTTP/Handle gRPC.
//
// HTTPGRPCTracer.Wrap must be attached to the same mux.Router assigned to dskit/server.Config.Router
// but it does not need to be attached to dskit/server.Config.HTTPMiddleware.
// dskit/server.Config.HTTPMiddleware is applied to direct HTTP requests not routed through gRPC;
// the server utilizes the default http middleware Tracer.Wrap for those standard http requests.
func InitHTTPGRPCMiddleware(router *mux.Router) *mux.Router {
	middleware := HTTPGRPCTracer{RouteMatcher: router}
	router.Use(middleware.Wrap)
	return router
}

// Wrap creates and decorates server-side tracing spans for httpgrpc requests
//
// The httpgrpc client wraps HTTP requests up into a generic httpgrpc.HTTP/Handle gRPC method.
// The httpgrpc server unwraps httpgrpc.HTTP/Handle gRPC requests into HTTP requests
// and forwards them to its own internal HTTP router.
//
// By default, the server-side tracing spans for the httpgrpc.HTTP/Handle gRPC method
// have no data about the wrapped HTTP request being handled.
//
// HTTPGRPCTracer.Wrap starts a child span with span name and tags and
// attaches the HTTP server span tags to the parent httpgrpc.HTTP/Handle gRPC span, allowing
// tracing tooling to differentiate the HTTP requests represented by the httpgrpc.HTTP/Handle spans.
func (hgt HTTPGRPCTracer) Wrap(next http.Handler) http.Handler {
	httpOperationNameFunc := makeHTTPOperationNameFunc(hgt.RouteMatcher)

	fn := func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		parentSpan := trace.SpanFromContext(ctx)

		// extract relevant span & tag data from request
		method := r.Method
		matchedRoute := getRouteName(hgt.RouteMatcher, r)
		urlPath := r.URL.Path
		userAgent := r.Header.Get("User-Agent")

		// tag parent httpgrpc.HTTP/Handle server span, if it exists
		if parentSpan.SpanContext().IsValid() {
			parentSpan.SetAttributes(attribute.String("http.url", urlPath))
			parentSpan.SetAttributes(attribute.String("http.method", method))
			parentSpan.SetAttributes(attribute.String("http.route", matchedRoute))
			parentSpan.SetAttributes(attribute.String("http.user_agent", userAgent))
		}

		// create and start child HTTP span and set span name and attributes
		childSpanName := httpOperationNameFunc("", r)

		startSpanOpts := []trace.SpanStartOption{
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("component", "net/http"),
				attribute.String("http.method", method),
				attribute.String("http.url", urlPath),
				attribute.String("http.route", matchedRoute),
				attribute.String("http.user_agent", userAgent),
			),
		}
		var childSpan trace.Span
		ctx, childSpan = otel.Tracer("").Start(ctx, childSpanName, startSpanOpts...)
		defer childSpan.End()

		r = r.WithContext(trace.ContextWithSpan(ctx, childSpan))
		next.ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}

func makeHTTPOperationNameFunc(routeMatcher RouteMatcher) func(_ string, r *http.Request) string {
	return func(_ string, r *http.Request) string {
		op := getRouteName(routeMatcher, r)
		if op == "" {
			return "HTTP " + r.Method
		}
		return fmt.Sprintf("HTTP %s - %s", r.Method, op)
	}
}
