// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-logfmt/logfmt"
)

// LokiClient is an HTTP client for querying Loki logs.
type LokiClient struct {
	httpClient *http.Client
	address    string
	authType   string // "basic-auth" or "trust"
	tenantID   string
	username   string
	password   string
}

// NewLokiClient creates a new LokiClient.
func NewLokiClient(address, authType, tenantID, username, password string) *LokiClient {
	return &LokiClient{
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

// LokiQueryResponse represents the response from Loki's query_range API.
type LokiQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string       `json:"resultType"`
		Result     []LokiStream `json:"result"`
	} `json:"data"`
}

// LokiStream represents a log stream in Loki's response.
type LokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"` // Each value is [timestamp_ns, log_line]
}

// QueryStatsEntry represents a parsed query stats log entry.
type QueryStatsEntry struct {
	Timestamp time.Time
	Query     string
}

// QueryStatsHandler is called for each query stats entry found in the logs.
type QueryStatsHandler func(entry QueryStatsEntry) error

// QueryQueryStats queries Loki for query stats logs and calls the handler for each entry.
// It uses pagination to handle large result sets without loading everything in memory.
// The container parameter specifies which container to query (e.g., "query-frontend" or "ruler-query-frontend").
func (c *LokiClient) QueryQueryStats(ctx context.Context, namespace, tenantID, container string, start, end time.Time, handler QueryStatsHandler) error {
	// Build the LogQL query.
	// Filter for actual PromQL queries (not metadata queries like label values).
	// Actual queries have path containing /query or /query_range.
	query := fmt.Sprintf(
		`{namespace="%s", container="%s"} |= "query stats" | logfmt | msg="query stats" | user="%s" | path=~".*/query.*"`,
		namespace, container, tenantID,
	)

	const batchSize = 5000
	currentStart := start

	// Track seen entries to skip duplicates at batch boundaries.
	seenEntries := make(map[string]struct{})

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Query this batch.
		resp, err := c.queryRange(ctx, query, batchSize, currentStart, end)
		if err != nil {
			return fmt.Errorf("failed to query Loki: %w", err)
		}

		if len(resp.Data.Result) == 0 {
			break
		}

		// Collect all entries from all streams.
		type logEntry struct {
			timestamp time.Time
			line      string
		}
		var allEntries []logEntry

		for _, stream := range resp.Data.Result {
			for _, value := range stream.Values {
				if len(value) < 2 {
					continue
				}
				tsNano, err := strconv.ParseInt(value[0], 10, 64)
				if err != nil {
					continue
				}
				allEntries = append(allEntries, logEntry{
					timestamp: time.Unix(0, tsNano),
					line:      value[1],
				})
			}
		}

		if len(allEntries) == 0 {
			break
		}

		// Process entries.
		var lastTimestamp time.Time

		for _, e := range allEntries {
			// Create a key for deduplication.
			entryKey := fmt.Sprintf("%d:%s", e.timestamp.UnixNano(), e.line)
			if _, seen := seenEntries[entryKey]; seen {
				continue
			}
			seenEntries[entryKey] = struct{}{}

			// Parse the log line to extract the query.
			queryStr, err := extractQueryFromLogLine(e.line)
			if err != nil || queryStr == "" {
				continue
			}

			// Call the handler.
			if err := handler(QueryStatsEntry{
				Timestamp: e.timestamp,
				Query:     queryStr,
			}); err != nil {
				return fmt.Errorf("handler error: %w", err)
			}

			lastTimestamp = e.timestamp
		}

		// If we got fewer entries than batch size, we're done.
		if len(allEntries) < batchSize {
			break
		}

		// Move start time to the last timestamp for the next batch.
		if !lastTimestamp.IsZero() {
			currentStart = lastTimestamp.Add(1 * time.Nanosecond)
		} else {
			break
		}

		// Clear old entries from seen map to avoid memory growth.
		for key := range seenEntries {
			if !strings.HasPrefix(key, fmt.Sprintf("%d:", lastTimestamp.UnixNano())) {
				delete(seenEntries, key)
			}
		}
	}

	return nil
}

// queryRange performs a query_range request to Loki.
func (c *LokiClient) queryRange(ctx context.Context, query string, limit int, start, end time.Time) (*LokiQueryResponse, error) {
	params := url.Values{}
	params.Set("query", query)
	params.Set("limit", strconv.Itoa(limit))
	params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
	params.Set("direction", "forward")

	reqURL := fmt.Sprintf("%s/loki/api/v1/query_range?%s", c.address, params.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	if c.authType == "trust" {
		req.Header.Set("X-Scope-OrgID", c.tenantID)
	} else if c.username != "" && c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result LokiQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &result, nil
}

// extractQueryFromLogLine parses a logfmt log line and extracts the "param_query" field.
func extractQueryFromLogLine(line string) (string, error) {
	decoder := logfmt.NewDecoder(bufio.NewReader(bytes.NewReader([]byte(line))))

	for decoder.ScanRecord() {
		for decoder.ScanKeyval() {
			if string(decoder.Key()) == "param_query" {
				return string(decoder.Value()), nil
			}
		}
	}

	if err := decoder.Err(); err != nil {
		return "", err
	}

	return "", nil
}
