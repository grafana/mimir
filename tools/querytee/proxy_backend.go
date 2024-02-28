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

	"github.com/pkg/errors"
)

// ProxyBackend holds the information of a single backend.
type ProxyBackend struct {
	name     string
	endpoint *url.URL
	client   *http.Client
	timeout  time.Duration

	// Whether this is the preferred backend from which picking up
	// the response and sending it back to the client.
	preferred bool
}

// NewProxyBackend makes a new ProxyBackend
func NewProxyBackend(name string, endpoint *url.URL, timeout time.Duration, preferred bool, skipTLSVerify bool) *ProxyBackend {
	return &ProxyBackend{
		name:      name,
		endpoint:  endpoint,
		timeout:   timeout,
		preferred: preferred,
		client: &http.Client{
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return errors.New("the query-tee proxy does not follow redirects")
			},
			Transport: &http.Transport{
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
			},
		},
	}
}

func (b *ProxyBackend) ForwardRequest(orig *http.Request, body io.ReadCloser) (int, []byte, *http.Response, error) {
	req, err := b.createBackendRequest(orig, body)
	if err != nil {
		return 0, nil, nil, err
	}

	return b.doBackendRequest(req)
}

func (b *ProxyBackend) createBackendRequest(orig *http.Request, body io.ReadCloser) (*http.Request, error) {
	req := orig.Clone(context.Background())
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

	return req, nil
}

func (b *ProxyBackend) doBackendRequest(req *http.Request) (int, []byte, *http.Response, error) {
	// Honor the read timeout.
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
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
