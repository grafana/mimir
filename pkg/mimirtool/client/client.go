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

	"github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

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
}

// New returns a new MimirClient.
func New(cfg Config) (*MimirClient, error) {
	endpoint, err := url.Parse(cfg.Address)
	if err != nil {
		return nil, err
	}

	log.WithFields(log.Fields{
		"address": cfg.Address,
		"id":      cfg.ID,
	}).Debugln("New Mimir client created")

	client := http.Client{}

	// Setup TLS client
	tlsConfig, err := cfg.TLS.GetTLSConfig()
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"tls-ca":   cfg.TLS.CAPath,
			"tls-cert": cfg.TLS.CertPath,
			"tls-key":  cfg.TLS.KeyPath,
		}).Errorf("error loading TLS files")
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
	}, nil
}

// Query executes a PromQL query against the Mimir cluster.
func (r *MimirClient) Query(ctx context.Context, query string) (*http.Response, error) {
	req := fmt.Sprintf("/prometheus/api/v1/query?query=%s&time=%d", url.QueryEscape(query), time.Now().Unix())

	res, err := r.doRequest(ctx, req, "GET", nil, -1)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (r *MimirClient) doRequest(ctx context.Context, path, method string, payload io.Reader, contentLength int64) (*http.Response, error) {
	req, err := buildRequest(ctx, path, method, *r.endpoint, payload, contentLength)
	if err != nil {
		return nil, err
	}

	switch {
	case (r.user != "" || r.key != "") && r.authToken != "":
		err := errors.New("at most one of basic auth or auth token should be configured")
		log.WithFields(log.Fields{
			"url":    req.URL.String(),
			"method": req.Method,
			"error":  err,
		}).Errorln("error during setting up request to mimir api")
		return nil, err

	case r.user != "":
		req.SetBasicAuth(r.user, r.key)

	case r.key != "":
		req.SetBasicAuth(r.id, r.key)

	case r.authToken != "":
		req.Header.Add("Authorization", "Bearer "+r.authToken)
	}

	for k, v := range r.extraHeaders {
		req.Header.Add(k, v)
	}

	req.Header.Add(user.OrgIDHeaderName, r.id)

	log.WithFields(log.Fields{
		"url":    req.URL.String(),
		"method": req.Method,
	}).Debugln("sending request to Grafana Mimir API")

	resp, err := r.Client.Do(req)
	if err != nil {
		log.WithFields(log.Fields{
			"url":    req.URL.String(),
			"method": req.Method,
			"error":  err.Error(),
		}).Errorln("error during request to Grafana Mimir API")
		return nil, err
	}

	if err := checkResponse(resp); err != nil {
		_ = resp.Body.Close()
		return nil, errors.Wrapf(err, "%s request to %s failed", req.Method, req.URL.String())
	}

	return resp, nil
}

// checkResponse checks an API response for errors.
func checkResponse(r *http.Response) error {
	log.WithFields(log.Fields{
		"status": r.Status,
	}).Debugln("checking response")
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
		log.WithFields(log.Fields{
			"status": r.Status,
			"body":   bodyStr,
		}).Debugln(msg)
		return ErrResourceNotFound
	}
	if r.StatusCode == http.StatusConflict {
		log.WithFields(log.Fields{
			"status": r.Status,
			"body":   bodyStr,
		}).Debugln(msg)
		return errConflict
	}
	if r.StatusCode == http.StatusTooManyRequests {
		log.WithFields(log.Fields{
			"status": r.Status,
			"body":   bodyStr,
		}).Debugln(msg)
		return errTooManyRequests
	}

	log.WithFields(log.Fields{
		"status": r.Status,
		"body":   bodyStr,
	}).Errorln(msg)

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
