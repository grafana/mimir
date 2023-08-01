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
	return otelhttp.NewHandler(next, "tracer-wrap-handler", options...)
}
