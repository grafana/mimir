// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/client/client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/util/version"
)

const (
	rulerAPIPath  = "/prometheus/config/v1/rules"
	legacyAPIPath = "/api/v1/rules"
)

var (
	ErrResourceNotFound = errors.New("requested resource not found")
	errConflict         = errors.New("conflict with current state of target resource")
	errTooManyRequests  = errors.New("too many requests")
)

// UserAgent returns build information in format suitable to be used in HTTP User-Agent header.
func UserAgent() string {
	return fmt.Sprintf("mimirtool/%s %s", version.Version, version.Info())
}

// Config is used to configure a MimirClient.
type Config struct {
	User            string `yaml:"user"`
	Key             string `yaml:"key"`
	Address         string `yaml:"address"`
	ID              string `yaml:"id"`
	TLS             tls.ClientConfig
	UseLegacyRoutes bool              `yaml:"use_legacy_routes"`
	MimirHTTPPrefix string            `yaml:"mimir_http_prefix"`
	AuthToken       string            `yaml:"auth_token"`
	ExtraHeaders    map[string]string `yaml:"extra_headers"`
}

// MimirClient is a client to the Mimir API.
type MimirClient struct {
	user         string
	key          string
	id           string
	endpoint     *url.URL
	Client       http.Client
	apiPath      string
	authToken    string
	extraHeaders map[string]string
	logger       log.Logger
}

// New returns a new MimirClient.
func New(cfg Config, logger log.Logger) (*MimirClient, error) {
	endpoint, err := url.Parse(cfg.Address)
	if err != nil {
		return nil, err
	}

	level.Debug(logger).Log("msg", "New Mimir client created", "address", cfg.Address, "id", cfg.ID)

	client := http.Client{}

	// Setup TLS client
	tlsConfig, err := cfg.TLS.GetTLSConfig()
	if err != nil {
		level.Error(logger).Log("msg", "error loading TLS files", "tls-ca", cfg.TLS.CAPath, "tls-cert", cfg.TLS.CertPath, "tls-key", cfg.TLS.KeyPath, "err", err)
		//nolint:staticcheck
		return nil, fmt.Errorf("Mimir client initialization unsuccessful")
	}

	if tlsConfig != nil {
		transport := &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: tlsConfig,
		}
		client = http.Client{Transport: transport}
	}

	path := rulerAPIPath
	if cfg.UseLegacyRoutes {
		var err error
		if path, err = url.JoinPath(cfg.MimirHTTPPrefix, legacyAPIPath); err != nil {
			return nil, err
		}
	}

	return &MimirClient{
		user:         cfg.User,
		key:          cfg.Key,
		id:           cfg.ID,
		endpoint:     endpoint,
		Client:       client,
		apiPath:      path,
		authToken:    cfg.AuthToken,
		extraHeaders: cfg.ExtraHeaders,
		logger:       logger,
	}, nil
}

// Query executes a PromQL query against the Mimir cluster.
func (c *MimirClient) Query(ctx context.Context, query string) (*http.Response, error) {
	req := fmt.Sprintf("/prometheus/api/v1/query?query=%s&time=%d", url.QueryEscape(query), time.Now().Unix())

	res, err := c.doRequest(ctx, req, "GET", nil, -1)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *MimirClient) doRequest(ctx context.Context, path, method string, payload io.Reader, contentLength int64) (*http.Response, error) {
	req, err := buildRequest(ctx, path, method, *c.endpoint, payload, contentLength)
	if err != nil {
		return nil, err
	}

	switch {
	case (c.user != "" || c.key != "") && c.authToken != "":
		err := errors.New("at most one of basic auth or auth token should be configured")
		level.Error(c.logger).Log("msg", "error during setting up request to mimir api", "url", req.URL.String(), "method", req.Method, "err", err)
		return nil, err

	case c.user != "":
		req.SetBasicAuth(c.user, c.key)

	case c.key != "":
		req.SetBasicAuth(c.id, c.key)

	case c.authToken != "":
		req.Header.Add("Authorization", "Bearer "+c.authToken)
	}

	for k, v := range c.extraHeaders {
		req.Header.Add(k, v)
	}

	req.Header.Add(user.OrgIDHeaderName, c.id)

	level.Debug(c.logger).Log("msg", "sending request to Grafana Mimir API", "url", req.URL.String(), "method", req.Method)

	resp, err := c.Client.Do(req)
	if err != nil {
		level.Error(c.logger).Log("msg", "error during request to Grafana Mimir API", "url", req.URL.String(), "method", req.Method, "err", err.Error())
		return nil, err
	}

	if err := c.checkResponse(resp); err != nil {
		_ = resp.Body.Close()
		return nil, errors.Wrapf(err, "%s request to %s failed", req.Method, req.URL.String())
	}

	return resp, nil
}

// checkResponse checks an API response for errors.
func (c *MimirClient) checkResponse(r *http.Response) error {
	level.Debug(c.logger).Log("msg", "checking response", "status", r.Status)
	if 200 <= r.StatusCode && r.StatusCode <= 299 {
		return nil
	}

	bodyHead, err := io.ReadAll(io.LimitReader(r.Body, 1024))
	if err != nil {
		return errors.Wrapf(err, "reading body")
	}
	bodyStr := string(bodyHead)
	const msg = "response"
	if r.StatusCode == http.StatusNotFound {
		level.Debug(c.logger).Log("msg", msg, "status", r.Status, "body", bodyStr)
		return ErrResourceNotFound
	}
	if r.StatusCode == http.StatusConflict {
		level.Debug(c.logger).Log("msg", msg, "status", r.Status, "body", bodyStr)
		return errConflict
	}
	if r.StatusCode == http.StatusTooManyRequests {
		level.Debug(c.logger).Log("msg", msg, "status", r.Status, "body", bodyStr)
		return errTooManyRequests
	}

	level.Error(c.logger).Log("msg", msg, "status", r.Status, "body", bodyStr)

	var errMsg string
	if bodyStr == "" {
		errMsg = fmt.Sprintf("server returned HTTP status: %s", r.Status)
	} else {
		errMsg = fmt.Sprintf("server returned HTTP status: %s, body: %q", r.Status, bodyStr)
	}

	return errors.New(errMsg)
}

func joinPath(baseURLPath, targetPath string) string {
	// trim exactly one slash at the end of the base URL, this expects target
	// path to always start with a slash
	return strings.TrimSuffix(baseURLPath, "/") + targetPath
}

func buildRequest(ctx context.Context, p, m string, endpoint url.URL, payload io.Reader, contentLength int64) (*http.Request, error) {
	// parse path parameter again (as it already contains escaped path information
	pURL, err := url.Parse(p)
	if err != nil {
		return nil, err
	}

	// if path or endpoint contains escaping that requires RawPath to be populated, also join rawPath
	if pURL.RawPath != "" || endpoint.RawPath != "" {
		endpoint.RawPath = joinPath(endpoint.EscapedPath(), pURL.EscapedPath())
	}
	endpoint.Path = joinPath(endpoint.Path, pURL.Path)
	endpoint.RawQuery = pURL.RawQuery
	r, err := http.NewRequestWithContext(ctx, m, endpoint.String(), payload)
	if err != nil {
		return nil, err
	}
	if contentLength >= 0 {
		r.ContentLength = contentLength
	}
	r.Header.Add("User-Agent", UserAgent())
	return r, nil
}
