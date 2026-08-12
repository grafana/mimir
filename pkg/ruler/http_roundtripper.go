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

	"github.com/grafana/mimir/pkg/util"
)

// HTTPConfig defiens the configuration for a HTTP client.
type HTTPConfig struct {
	ConnectTimeout time.Duration  `yaml:"connect_timeout" category:"advanced"`
	BasicAuth      util.BasicAuth `yaml:",inline"`
}

// RegisterFlagsWithPrefix registers flags with the provided prefix.
func (cfg *HTTPConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.ConnectTimeout, prefix+".connect-timeout", 30*time.Second, "Timeout for establishing a connection to the query-frontend.")
	cfg.BasicAuth.RegisterFlagsWithPrefix(prefix+".", f)
}

func newHTTPRoundTripper(cfg *HTTPConfig) http.RoundTripper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   cfg.ConnectTimeout,
		KeepAlive: 30 * time.Second,
	}).DialContext

	var rt http.RoundTripper = otelhttp.NewTransport(transport)
	if cfg.BasicAuth.IsEnabled() {
		rt = &basicAuthRoundTripper{
			username: cfg.BasicAuth.Username,
			password: cfg.BasicAuth.Password.String(),
			next:     rt,
		}
	}
	return rt
}

// basicAuthRoundTripper sets HTTP Basic authentication on outgoing requests.
// The ruler's remote querier calls Transport.RoundTrip directly rather than
// http.Client.Do, so credentials in the address userinfo are not applied;
// this makes the configured basic_auth take effect.
type basicAuthRoundTripper struct {
	username, password string
	next               http.RoundTripper
}

func (rt *basicAuthRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Authorization") == "" {
		// RoundTrippers must not modify the passed request, so clone it.
		req = req.Clone(req.Context())
		req.SetBasicAuth(rt.username, rt.password)
	}
	return rt.next.RoundTrip(req)
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
