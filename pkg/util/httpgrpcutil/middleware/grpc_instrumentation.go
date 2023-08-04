// SPDX-License-Identifier: AGPL-3.0-only

package middleware

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/uber/jaeger-client-go"
)

const httpGRPCHandleMethod = "/httpgrpc.HTTP/Handle"
const httpSpanNameSep = " "

// InitHTTPMiddleware initializes gorilla/mux-compatible HTTP middleware
func InitHTTPMiddleware(router *mux.Router) {
	middleware := OpenTracingHTTPGRPCMiddleware{router: router}
	router.Use(middleware.Wrap)
}

type OpenTracingHTTPGRPCMiddleware struct {
	router *mux.Router
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
// Wrap starts & decorates a child span with OTEL HTTP server span name and tag conventions
// and attaches the HTTP server span tags to the parent httpgrpc.HTTP/Handle gRPC span,
// allowing tracing tooling to differentiate the HTTP requests represented by the httpgrpc.HTTP/Handle spans.
//
// Parent span tagging depends on using a Jaeger tracer for now in order to access OperationName(),
// which is not required to satisfy the generic opentracing Tracer interface.
//
// (Currently-experimental) OTEL HTTP tracing standards are used, as opentracing has limited standards defined.
func (m *OpenTracingHTTPGRPCMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		tracer := opentracing.GlobalTracer()

		matchedRoute, _ := mux.CurrentRoute(r).GetPathTemplate()

		parentSpan := opentracing.SpanFromContext(ctx)
		if parentSpan, ok := parentSpan.(*jaeger.Span); ok {
			if parentSpan.OperationName() == httpGRPCHandleMethod {
				parentSpan.SetTag("http.request.method", r.Method)
				parentSpan.SetTag("http.route", matchedRoute)
				parentSpan.SetTag("url.path", r.RequestURI)
			}
		}

		childSpanName := httpServerSpanName(r.Method, matchedRoute)
		startSpanOpts := []opentracing.StartSpanOption{
			ext.SpanKindRPCServer,
			opentracing.Tag{Key: string(ext.Component), Value: "net/http"},
			opentracing.Tag{Key: "http.request.method", Value: r.Method},
			opentracing.Tag{Key: "http.route", Value: matchedRoute},
			opentracing.Tag{Key: "url.path", Value: r.RequestURI},
		}
		childSpan, _ := opentracing.StartSpanFromContextWithTracer(ctx, tracer, childSpanName, startSpanOpts...)
		defer childSpan.Finish()

		r = r.WithContext(opentracing.ContextWithSpan(r.Context(), childSpan))

		next.ServeHTTP(w, r)
	})
}

// httpServerSpanName constructs a tracing span operation name according to OTEL standards
//
// > HTTP server span names SHOULD be {http.request.method} {http.route}
// > if there is a (low-cardinality) http.route available.
// > HTTP server span names SHOULD be {http.method}
// > if there is no (low-cardinality) http.route available
func httpServerSpanName(method, matchedRoute string) string {
	spanName := method
	if matchedRoute != "" {
		spanName += httpSpanNameSep + matchedRoute
	}
	return spanName
}
