package errorsource

import (
	"errors"
	"net/http"
	"syscall"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
)

// Middleware captures error source metric
func Middleware(plugin string) httpclient.Middleware {
	return httpclient.NamedMiddlewareFunc(plugin, RoundTripper)
}

// RoundTripper returns the error source
func RoundTripper(_ httpclient.Options, next http.RoundTripper) http.RoundTripper {
	return httpclient.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		res, err := next.RoundTrip(req)
		if res != nil && res.StatusCode >= 400 {
			errorSource := backend.ErrorSourceFromHTTPStatus(res.StatusCode)
			if err == nil {
				err = errors.New(res.Status)
			}
			return res, Error{source: errorSource, err: err}
		}
		if errors.Is(err, syscall.ECONNREFUSED) {
			return res, Error{source: backend.ErrorSourceDownstream, err: err}
		}
		return res, err
	})
}
