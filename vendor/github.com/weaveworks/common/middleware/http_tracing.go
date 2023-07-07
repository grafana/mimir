package middleware

import (
	"fmt"
	"net/http"

	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Dummy dependency to enforce that we have a nethttp version newer
// than the one which implements Websockets. (No semver on nethttp)
var _ = nethttp.MWURLTagFunc

// Tracer is a middleware which traces incoming requests.
type Tracer struct {
	RouteMatcher RouteMatcher
	SourceIPs    *SourceIPExtractor
}

// Wrap implements Interface
func (t Tracer) Wrap(next http.Handler) http.Handler {
	options := []otelhttp.Option{
		otelhttp.WithSpanNameFormatter(func(op string, r *http.Request) string {
			if op == "" {
				return "HTTP " + r.Method
			}

			return fmt.Sprintf("HTTP %s - %s", r.Method, op)
		}),
	}

	// options := []nethttp.MWOption{
	// 	nethttp.OperationNameFunc(func(r *http.Request) string {
	// 		op := getRouteName(t.RouteMatcher, r)
	// 		if op == "" {
	// 			return "HTTP " + r.Method
	// 		}

	// 		return fmt.Sprintf("HTTP %s - %s", r.Method, op)
	// 	}),
	// 	nethttp.MWSpanObserver(func(sp opentracing.Span, r *http.Request) {
	// 		// add a tag with the client's user agent to the span
	// 		userAgent := r.Header.Get("User-Agent")
	// 		if userAgent != "" {
	// 			sp.SetTag("http.user_agent", userAgent)
	// 		}

	// 		// add a tag with the client's sourceIPs to the span, if a
	// 		// SourceIPExtractor is given.
	// 		if t.SourceIPs != nil {
	// 			sp.SetTag("sourceIPs", t.SourceIPs.Get(r))
	// 		}
	// 	}),

	return otelhttp.NewHandler(next, "tracer-wrap-handler", options...)
}
