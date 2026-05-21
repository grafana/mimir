// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana-tools/sdk/blob/master/rest-request.go
// Provenance-includes-location: https://github.com/grafana-tools/sdk/blob/master/rest-dashboard.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: 2016 Alexander I.Grafov <grafov@gmail.com>.
// Provenance-includes-copyright: 2016-2019 The Grafana SDK authors

package minisdk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
)

// DefaultHTTPClient is the default *http.Client used by NewClient when callers do not provide one.
var DefaultHTTPClient = http.DefaultClient

// Client is a minimal Grafana REST API client. It implements only the
// subset of the API used by mimirtool.
type Client struct {
	baseURL   string
	key       string
	basicAuth bool
	client    *http.Client
}

// NewClient initializes a client for interacting with a Grafana server.
// apiKeyOrBasicAuth accepts either 'username:password' basic authentication
// credentials, or a Grafana API key. If empty, no authentication is used.
func NewClient(apiURL, apiKeyOrBasicAuth string, client *http.Client) (*Client, error) {
	if client == nil {
		client = DefaultHTTPClient
	}
	baseURL, err := url.Parse(apiURL)
	if err != nil {
		return nil, err
	}
	basicAuth := strings.Contains(apiKeyOrBasicAuth, ":")
	key := ""
	if len(apiKeyOrBasicAuth) > 0 {
		if basicAuth {
			parts := strings.SplitN(apiKeyOrBasicAuth, ":", 2)
			baseURL.User = url.UserPassword(parts[0], parts[1])
		} else {
			key = "Bearer " + apiKeyOrBasicAuth
		}
	}
	return &Client{baseURL: baseURL.String(), basicAuth: basicAuth, key: key, client: client}, nil
}

// FoundBoard keeps result of search with metadata of a dashboard.
type FoundBoard struct {
	UID         string `json:"uid"`
	Title       string `json:"title"`
	FolderTitle string `json:"folderTitle"`
}

// SearchParam configures a Search call.
type SearchParam func(url.Values)

// SearchParamType is the value type accepted by SearchType.
type SearchParamType string

// Search entities to be used with SearchType().
const (
	SearchTypeDashboard SearchParamType = "dash-db"
)

// SearchType specifies the entity type to search for.
func SearchType(t SearchParamType) SearchParam {
	return func(v url.Values) {
		v.Set("type", string(t))
	}
}

// SearchPage specifies the page number to be queried for.
// Zero is silently ignored, page numbers start from one.
func SearchPage(page uint) SearchParam {
	return func(v url.Values) {
		if page > 0 {
			v.Set("page", strconv.FormatUint(uint64(page), 10))
		}
	}
}

// Search calls GET /api/search.
func (c *Client) Search(ctx context.Context, params ...SearchParam) ([]FoundBoard, error) {
	q := url.Values{}
	for _, p := range params {
		p(q)
	}
	raw, code, err := c.get(ctx, "api/search", q)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("HTTP error %d: returns %s", code, raw)
	}
	var boards []FoundBoard
	if err := json.Unmarshal(raw, &boards); err != nil {
		return nil, err
	}
	return boards, nil
}

// GetRawDashboardByUID returns the raw JSON of the dashboard with the given UID.
// It strips the API response envelope so that the returned bytes are the dashboard object itself.
func (c *Client) GetRawDashboardByUID(ctx context.Context, uid string) ([]byte, error) {
	raw, code, err := c.get(ctx, "api/dashboards/uid/"+uid, nil)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("HTTP error %d: returns %s", code, raw)
	}
	var envelope struct {
		Board json.RawMessage `json:"dashboard"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, fmt.Errorf("unmarshal board: %w", err)
	}
	return envelope.Board, nil
}

func (c *Client) get(ctx context.Context, query string, params url.Values) ([]byte, int, error) {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, 0, err
	}
	u.Path = path.Join(u.Path, query)
	if params != nil {
		u.RawQuery = params.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, err
	}
	if !c.basicAuth && c.key != "" {
		req.Header.Set("Authorization", c.key)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return data, resp.StatusCode, err
}
