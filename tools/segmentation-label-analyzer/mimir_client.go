// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/mimir/pkg/querier/api"
)

// MimirClient is an HTTP client for Mimir cardinality endpoints.
type MimirClient struct {
	httpClient *http.Client
	address    string
	authType   string // "basic-auth" or "trust"
	tenantID   string
	username   string
	password   string
}

// NewMimirClient creates a new MimirClient.
func NewMimirClient(address, authType, tenantID, username, password string) *MimirClient {
	return &MimirClient{
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		address:  strings.TrimSuffix(address, "/"),
		authType: authType,
		tenantID: tenantID,
		username: username,
		password: password,
	}
}

// labelsResponse is the response from /api/v1/labels.
type labelsResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

// GetLabelNames returns all label names.
func (c *MimirClient) GetLabelNames(ctx context.Context) ([]string, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/prometheus/api/v1/labels", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result labelsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return result.Data, nil
}

// GetLabelValuesCardinality returns series counts for each value of the specified label names.
func (c *MimirClient) GetLabelValuesCardinality(ctx context.Context, labelNames []string, limit int) (*api.LabelValuesCardinalityResponse, error) {
	body := make(url.Values)
	for _, name := range labelNames {
		body.Add("label_names[]", name)
	}
	if limit > 0 {
		body.Set("limit", strconv.Itoa(limit))
	}

	resp, err := c.doRequest(ctx, http.MethodPost, "/prometheus/api/v1/cardinality/label_values", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result api.LabelValuesCardinalityResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &result, nil
}

// doRequest performs an HTTP request with authentication and form-encoded body.
func (c *MimirClient) doRequest(ctx context.Context, method, path string, body url.Values) (*http.Response, error) {
	reqURL := c.address + path

	var reqBody io.Reader
	if len(body) > 0 {
		reqBody = strings.NewReader(body.Encode())
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, reqBody)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	if c.authType == AuthTypeTrust {
		req.Header.Set("X-Scope-OrgID", c.tenantID)
	} else if c.username != "" && c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	if reqBody != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	return c.httpClient.Do(req)
}
