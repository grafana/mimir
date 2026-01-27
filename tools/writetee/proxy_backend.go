// SPDX-License-Identifier: AGPL-3.0-only

package writetee

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
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type BackendType int

const (
	BackendTypeMirrored BackendType = iota
	BackendTypeAmplified
)

type ProxyBackend interface {
	Name() string
	Endpoint() *url.URL
	Preferred() bool
	BackendType() BackendType
	ForwardRequest(ctx context.Context, orig *http.Request, body io.ReadCloser) (time.Duration, int, []byte, error)
}

// proxyBackend holds the information of a single backend.
type proxyBackend struct {
	name        string
	endpoint    *url.URL
	client      *http.Client
	timeout     time.Duration
	backendType BackendType

	// Whether this is the preferred backend from which picking up
	// the response and sending it back to the client.
	preferred bool
}

// NewProxyBackend makes a new proxyBackend
func NewProxyBackend(name string, endpoint *url.URL, timeout time.Duration, preferred bool, skipTLSVerify bool, backendType BackendType) ProxyBackend {
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

	return &proxyBackend{
		name:        name,
		endpoint:    endpoint,
		timeout:     timeout,
		preferred:   preferred,
		backendType: backendType,
		client: &http.Client{
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return errors.New("the write-tee proxy does not follow redirects")
			},
			Transport: tracingTransport,
		},
	}
}

func (b *proxyBackend) Name() string {
	return b.name
}

func (b *proxyBackend) Endpoint() *url.URL {
	return b.endpoint
}

func (b *proxyBackend) Preferred() bool {
	return b.preferred
}

func (b *proxyBackend) BackendType() BackendType {
	return b.backendType
}

func (b *proxyBackend) ForwardRequest(ctx context.Context, orig *http.Request, body io.ReadCloser) (time.Duration, int, []byte, error) {
	req, err := b.createBackendRequest(ctx, orig, body)
	if err != nil {
		return 0, 0, nil, err
	}

	start := time.Now()
	status, responseBody, err := b.doBackendRequest(req)
	elapsed := time.Since(start)

	return elapsed, status, responseBody, err
}

func (b *proxyBackend) createBackendRequest(ctx context.Context, orig *http.Request, body io.ReadCloser) (*http.Request, error) {
	req := orig.Clone(ctx)
	req.Body = body
	// RequestURI can't be set on a cloned request. It's only for handlers.
	req.RequestURI = ""

	// Remove Content-Length header so the HTTP client recalculates it from the body.
	// This is important when the body has been amplified and is larger than the original.
	req.ContentLength = -1
	req.Header.Del("Content-Length")

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

func (b *proxyBackend) doBackendRequest(req *http.Request) (int, []byte, error) {
	// Honor the read timeout.
	ctx, cancel := context.WithTimeout(req.Context(), b.timeout)
	defer cancel()

	// Execute the request.
	res, err := b.client.Do(req.WithContext(ctx))
	if err != nil {
		return 0, nil, errors.Wrap(err, "executing backend request")
	}

	// Read the entire response body.
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, nil, errors.Wrap(err, "reading backend response")
	}

	return res.StatusCode, body, nil
}
