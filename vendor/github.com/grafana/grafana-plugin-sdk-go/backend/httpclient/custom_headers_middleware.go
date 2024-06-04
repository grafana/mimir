package httpclient

import (
	"net/http"
)

// CustomHeadersMiddlewareName is the middleware name used by CustomHeadersMiddleware.
const CustomHeadersMiddlewareName = "CustomHeaders"

// CustomHeadersMiddleware applies custom HTTP headers to the outgoing request.
//
// If opts.Headers is empty, next will be returned.
func CustomHeadersMiddleware() Middleware {
	return NamedMiddlewareFunc(CustomHeadersMiddlewareName, func(opts Options, next http.RoundTripper) http.RoundTripper {
		if len(opts.Headers) == 0 {
			return next
		}

		return RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			for key, value := range opts.Headers {
				// According to https://pkg.go.dev/net/http#Request.Header, Host is a special case
				if http.CanonicalHeaderKey(key) == "Host" {
					req.Host = value
				} else {
					req.Header.Set(key, value)
				}
			}

			return next.RoundTrip(req)
		})
	})
}
