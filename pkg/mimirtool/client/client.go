// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/client/client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/grafana/dskit/crypto/tls"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/weaveworks/common/user"
)

const (
	rulerAPIPath  = "/api/v1/rules"
	legacyAPIPath = "/api/prom/rules"
)

var (
	ErrNoConfig         = errors.New("No config exists for this user")
	ErrResourceNotFound = errors.New("requested resource not found")
)

// Config is used to configure a MimirClient.
type Config struct {
	User            string `yaml:"user"`
	Key             string `yaml:"key"`
	Address         string `yaml:"address"`
	ID              string `yaml:"id"`
	TLS             tls.ClientConfig
	UseLegacyRoutes bool `yaml:"use_legacy_routes"`
}

// MimirClient is a client to the Mimir API.
type MimirClient struct {
	user     string
	key      string
	id       string
	endpoint *url.URL
	Client   http.Client
	apiPath  string
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
		path = legacyAPIPath
	}

	return &MimirClient{
		user:     cfg.User,
		key:      cfg.Key,
		id:       cfg.ID,
		endpoint: endpoint,
		Client:   client,
		apiPath:  path,
	}, nil
}

// Query executes a PromQL query against the Mimir cluster.
func (r *MimirClient) Query(ctx context.Context, query string) (*http.Response, error) {
	query = fmt.Sprintf("query=%s&time=%d", query, time.Now().Unix())
	escapedQuery := url.PathEscape(query)

	res, err := r.doRequest("/prometheus/api/v1/query?"+escapedQuery, "GET", nil, -1)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (r *MimirClient) doRequest(path, method string, payload io.Reader, contentLength int64) (*http.Response, error) {
	req, err := buildRequest(path, method, *r.endpoint, payload, contentLength)
	if err != nil {
		return nil, err
	}

	if r.user != "" {
		req.SetBasicAuth(r.user, r.key)
	} else if r.key != "" {
		req.SetBasicAuth(r.id, r.key)
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

	err = checkResponse(resp)
	if err != nil {
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

	var msg, errMsg string
	scanner := bufio.NewScanner(io.LimitReader(r.Body, 512))
	if scanner.Scan() {
		msg = scanner.Text()
	}

	if msg == "" {
		errMsg = fmt.Sprintf("server returned HTTP status %s", r.Status)
	} else {
		errMsg = fmt.Sprintf("server returned HTTP status %s: %s", r.Status, msg)
	}

	if r.StatusCode == http.StatusNotFound {
		log.WithFields(log.Fields{
			"status": r.Status,
			"msg":    msg,
		}).Debugln(errMsg)
		return ErrResourceNotFound
	}

	log.WithFields(log.Fields{
		"status": r.Status,
		"msg":    msg,
	}).Errorln(errMsg)

	return errors.New(errMsg)
}

func joinPath(baseURLPath, targetPath string) string {
	// trim exactly one slash at the end of the base URL, this expects target
	// path to always start with a slash
	return strings.TrimSuffix(baseURLPath, "/") + targetPath
}

func buildRequest(p, m string, endpoint url.URL, payload io.Reader, contentLength int64) (*http.Request, error) {
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
	r, err := http.NewRequest(m, endpoint.String(), payload)
	if err != nil {
		return nil, err
	}
	if contentLength >= 0 {
		r.ContentLength = contentLength
	}
	return r, nil
}
