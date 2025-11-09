// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/querytee/proxy_backend.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querytee

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/grafana/dskit/clusterutil"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const (
	// DefaultRequestProportion is the default proportion of requests to send to a backend
	DefaultRequestProportion = 1.0
	// DefaultMinDataQueriedAge is the default minimum data queried age for a backend (0s means serve all queries)
	DefaultMinDataQueriedAge = 0 * time.Second
)

type ProxyBackendInterface interface {
	Name() string
	Endpoint() *url.URL
	Preferred() bool
	RequestProportion() float64
	SetRequestProportion(proportion float64)
	HasConfiguredProportion() bool
	MinDataQueriedAge() time.Duration
	ShouldHandleQuery(minQueryTime time.Time) bool
	ForwardRequest(ctx context.Context, orig *http.Request, body io.ReadCloser) (time.Duration, int, []byte, *http.Response, error)
}

// ProxyBackend holds the information of a single backend.
type ProxyBackend struct {
	name     string
	endpoint *url.URL
	client   *http.Client
	timeout  time.Duration

	// Whether this is the preferred backend from which picking up
	// the response and sending it back to the client.
	preferred bool
	cfg       BackendConfig

	// Request proportion for this backend (only applies to secondary backends)
	requestProportion float64

	// Minimum data queried age - backend serves queries with min time >= (now - age)
	minDataQueriedAge time.Duration

	// Cluster validation label to set in outgoing requests.
	clusterLabel string
}

// NewProxyBackend makes a new ProxyBackend
func NewProxyBackend(name string, endpoint *url.URL, timeout time.Duration, preferred bool, skipTLSVerify bool, clusterLabel string, cfg BackendConfig) ProxyBackendInterface {
	innerTransport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: skipTLSVerify,
		},
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100, // see https://github.com/golang/go/issues/13801
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}

	tracingTransport := otelhttp.NewTransport(innerTransport)

	requestProportion := DefaultRequestProportion
	if cfg.RequestProportion != nil {
		requestProportion = *cfg.RequestProportion
	}

	minDataQueriedAge := DefaultMinDataQueriedAge
	if cfg.MinDataQueriedAge != "" {
		if d, err := time.ParseDuration(cfg.MinDataQueriedAge); err == nil {
			minDataQueriedAge = d
		}
	}

	return &ProxyBackend{
		name:              name,
		endpoint:          endpoint,
		timeout:           timeout,
		preferred:         preferred,
		cfg:               cfg,
		requestProportion: requestProportion,
		minDataQueriedAge: minDataQueriedAge,
		clusterLabel:      clusterLabel,
		client: &http.Client{
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return errors.New("the query-tee proxy does not follow redirects")
			},
			Transport: tracingTransport,
		},
	}
}

func (b *ProxyBackend) Name() string {
	return b.name
}

func (b *ProxyBackend) Endpoint() *url.URL {
	return b.endpoint
}

func (b *ProxyBackend) Preferred() bool {
	return b.preferred
}

func (b *ProxyBackend) RequestProportion() float64 {
	return b.requestProportion
}

func (b *ProxyBackend) SetRequestProportion(proportion float64) {
	b.requestProportion = proportion
}

func (b *ProxyBackend) HasConfiguredProportion() bool {
	return b.cfg.RequestProportion != nil
}

func (b *ProxyBackend) MinDataQueriedAge() time.Duration {
	return b.minDataQueriedAge
}

func (b *ProxyBackend) ShouldHandleQuery(minQueryTime time.Time) bool {
	// If the time received is zero we don't know the time so we serve the request
	if minQueryTime.IsZero() {
		return true
	}
	// If age is 0s, backend serves all queries
	if b.minDataQueriedAge == 0 {
		return true
	}
	// Backend serves queries where min_query_time >= (now - age)
	cutOffTs := time.Now().Add(-b.minDataQueriedAge)
	return minQueryTime.Before(cutOffTs)
}

func (b *ProxyBackend) ForwardRequest(ctx context.Context, orig *http.Request, body io.ReadCloser) (time.Duration, int, []byte, *http.Response, error) {
	req, err := b.createBackendRequest(ctx, orig, body)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	start := time.Now()
	status, responseBody, resp, err := b.doBackendRequest(req)
	elapsed := time.Since(start)

	return elapsed, status, responseBody, resp, err
}

func (b *ProxyBackend) createBackendRequest(ctx context.Context, orig *http.Request, body io.ReadCloser) (*http.Request, error) {
	req := orig.Clone(ctx)
	req.Body = body
	// RequestURI can't be set on a cloned request. It's only for handlers.
	req.RequestURI = ""
	// Replace the endpoint with the backend one.
	req.URL.Scheme = b.endpoint.Scheme
	req.URL.Host = b.endpoint.Host

	// Prepend the endpoint path to the request path.
	req.URL.Path = path.Join(b.endpoint.Path, req.URL.Path)

	// Set the correct host header for the backend
	req.Host = b.endpoint.Host

	// Replace the auth:
	// - If the endpoint has user and password, use it.
	// - If the endpoint has user only, keep it and use the request password (if any).
	// - If the endpoint has no user and no password, use the request auth (if any).
	// - If the endpoint has __REQUEST_HEADER_X_SCOPE_ORGID__ as the user, then replace it with the X-Scope-OrgID header value from the request.
	clientUser, clientPass, clientAuth := orig.BasicAuth()
	endpointUser := b.endpoint.User.Username()
	if endpointUser == "__REQUEST_HEADER_X_SCOPE_ORGID__" {
		endpointUser = orig.Header.Get("X-Scope-OrgID")
	}
	endpointPass, _ := b.endpoint.User.Password()

	req.Header.Del("Authorization")
	if endpointUser != "" && endpointPass != "" {
		req.SetBasicAuth(endpointUser, endpointPass)
	} else if endpointUser != "" {
		req.SetBasicAuth(endpointUser, clientPass)
	} else if clientAuth {
		req.SetBasicAuth(clientUser, clientPass)
	}

	// Remove Accept-Encoding header to avoid sending compressed responses
	req.Header.Del("Accept-Encoding")

	// Set cluster validation header if configured
	if b.clusterLabel != "" {
		req.Header.Set(clusterutil.ClusterValidationLabelHeader, b.clusterLabel)
	}

	for headerName, headerValues := range b.cfg.RequestHeaders {
		for _, headerValue := range headerValues {
			req.Header.Add(headerName, headerValue)
		}
	}

	return req, nil
}

func (b *ProxyBackend) doBackendRequest(req *http.Request) (int, []byte, *http.Response, error) {
	// Honor the read timeout.
	ctx, cancel := context.WithTimeout(req.Context(), b.timeout)
	defer cancel()

	// Execute the request.
	res, err := b.client.Do(req.WithContext(ctx))
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "executing backend request")
	}

	// Read the entire response body.
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "reading backend response")
	}

	return res.StatusCode, body, res, nil
}
