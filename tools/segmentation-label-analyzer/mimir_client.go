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

	"github.com/grafana/dskit/backoff"
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
		// High timeout because some requests are extremely slow (e.g., __name__ cardinality
		// can take 60-90s for tenants with millions of unique metric names).
		httpClient: &http.Client{
			Timeout: 120 * time.Second,
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

// doRequest performs an HTTP request with authentication, form-encoded body, and retry logic.
func (c *MimirClient) doRequest(ctx context.Context, method, path string, body url.Values) (*http.Response, error) {
	reqURL := c.address + path
	bodyEncoded := ""
	if len(body) > 0 {
		bodyEncoded = body.Encode()
	}

	const maxRetries = 3
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 1 * time.Second,
		MaxBackoff: 5 * time.Second,
		MaxRetries: maxRetries,
	})

	var lastErr error
	for boff.Ongoing() {
		var reqBody io.Reader
		if bodyEncoded != "" {
			reqBody = strings.NewReader(bodyEncoded)
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

		resp, err := c.httpClient.Do(req)
		if err == nil {
			return resp, nil
		}

		lastErr = err
		boff.Wait()
	}

	// All retries failed - include request details in error message.
	if bodyEncoded != "" {
		return nil, fmt.Errorf("request failed after %d attempts: %s %s (body: %s): %w", maxRetries, method, reqURL, bodyEncoded, lastErr)
	}
	return nil, fmt.Errorf("request failed after %d attempts: %s %s: %w", maxRetries, method, reqURL, lastErr)
}
