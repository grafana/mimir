// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"flag"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// HTTPConfig defiens the configuration for a HTTP client.
type HTTPConfig struct {
	ConnectTimeout time.Duration `yaml:"connect_timeout" category:"advanced"`
}

// RegisterFlagsWithPrefix registers flags with the provided prefix.
func (cfg *HTTPConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.ConnectTimeout, prefix+".connect-timeout", 30*time.Second, "Timeout for establishing a connection to the query-frontend.")
}

func newHTTPRoundTripper(cfg *HTTPConfig) http.RoundTripper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   cfg.ConnectTimeout,
		KeepAlive: 30 * time.Second,
	}).DialContext

	return otelhttp.NewTransport(transport)
}

// dialQueryFrontendHTTP creates and initializes a new httpgrpc.HTTPClient taking a QueryFrontendConfig configuration.
func dialQueryFrontendHTTP(cfg QueryFrontendConfig, _ prometheus.Registerer, _ log.Logger) (http.RoundTripper, *url.URL, error) {
	u, err := url.Parse(cfg.Address)
	if err != nil {
		return nil, nil, err
	}
	// this makes .JoinPath("/absolute/path") work as we need
	if u.Path == "" {
		u.Path = "/"
	}

	return newHTTPRoundTripper(&cfg.HTTPClientConfig), u, nil
}
