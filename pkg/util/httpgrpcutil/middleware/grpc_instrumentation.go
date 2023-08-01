// SPDX-License-Identifier: AGPL-3.0-only

package middleware

import (
	"context"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc"
)

// InitGRPCMiddleware initializes the stuff
func InitGRPCMiddleware(cfg *server.Config) {
	cfg.GRPCMiddleware = append(cfg.GRPCMiddleware, OpenTracingHTTPGRPCUnaryServerInterceptor(opentracing.GlobalTracer()))
}

const httpGRPCHandleMethod = "/httpgrpc.HTTP/Handle"
const httpSpanNameSep = " "

// InitHTTPMiddleware initializes the stuff
func InitHTTPMiddleware(router *mux.Router) {
	middleware := OpenTracingHTTPGRPCMiddleware{router: router}

	//middlewareFunc := func(handler http.Handler) http.Handler {
	//	return nethttp.Middleware(opentracing.GlobalTracer(), router)
	//}
	//
	//router.Use(middlewareFunc)
	router.Use(middleware.Wrap)
}

type OpenTracingHTTPGRPCMiddleware struct {
	router *mux.Router
}

func (m *OpenTracingHTTPGRPCMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		tracer := opentracing.GlobalTracer()

		matchedRoute, _ := mux.CurrentRoute(r).GetPathTemplate()

		parentSpan := opentracing.SpanFromContext(ctx)
		parentSpan.SetTag("http.request.method", r.Method)
		parentSpan.SetTag("http.route", matchedRoute)
		parentSpan.SetTag("url.path", r.RequestURI)

		childSpanName := r.Method + httpSpanNameSep + matchedRoute
		startSpanOpts := []opentracing.StartSpanOption{
			ext.SpanKindRPCServer,
			opentracing.Tag{Key: string(ext.Component), Value: "net/http"},
			opentracing.Tag{Key: "http.request.method", Value: r.Method},
			opentracing.Tag{Key: "http.route", Value: matchedRoute},
			opentracing.Tag{Key: "url.path", Value: r.RequestURI},
		}
		childSpan, _ := opentracing.StartSpanFromContextWithTracer(ctx, tracer, childSpanName, startSpanOpts...)
		defer childSpan.Finish()

		next.ServeHTTP(w, r)
	})
}

// OpenTracingHTTPGRPCUnaryServerInterceptor returns a grpc.UnaryServerInterceptor suitable
// for use in a grpc.NewServer call.
// TODO not needed if we just use the HTTP middleware approach above?
func OpenTracingHTTPGRPCUnaryServerInterceptor(tracer opentracing.Tracer) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		if info.FullMethod == httpGRPCHandleMethod {
			if httpGRPCReq, ok := req.(*httpgrpc.HTTPRequest); ok {
				// decorate existing parent span with attributes
				fullReqURL := httpGRPCReq.GetUrl()
				reqMethod := httpGRPCReq.GetMethod()
				parentSpan := opentracing.SpanFromContext(ctx)
				parentSpan.SetTag("http.request.method", reqMethod).SetTag("url.path", fullReqURL)

				// start & decorate child span with more specific name, attempting to follow OTEL HTTP conventions:
				//   "HTTP server span names SHOULD be {http.request.method} {http.route}
				//   if there is a (low-cardinality) http.route available"
				// we do not have the lower-cardinality matched route available, only the URL, so we
				// attempt to keep cardinality low by only using the path portion of the URL.
				childSpanName := httpGRPCHandleMethod + httpSpanNameSep + reqMethod // e.g. "/httpgrpc.HTTP/Handle POST"

				// only use the URL path if we can get it
				//   "HTTP server span names SHOULD be {http.method}
				//   if there is no (low-cardinality) http.route available"
				if parsedReqURL, _ := url.Parse(fullReqURL); parsedReqURL != nil {
					childSpanName += httpSpanNameSep + parsedReqURL.Path // e.g. "/httpgrpc.HTTP/Handle POST /api/v1/metrics"

				}

				startSpanOpts := []opentracing.StartSpanOption{
					ext.SpanKindRPCServer,
					opentracing.Tag{Key: string(ext.Component), Value: "gRPC"},
					opentracing.Tag{Key: "http.request.method", Value: reqMethod},
					opentracing.Tag{Key: "url.path", Value: fullReqURL},
				}
				childSpan, _ := opentracing.StartSpanFromContextWithTracer(ctx, tracer, childSpanName, startSpanOpts...)
				defer childSpan.Finish()
			}
		}

		resp, err = handler(ctx, req)
		return resp, err
	}
}
