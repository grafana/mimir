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

	querierapi "github.com/grafana/mimir/pkg/querier/api"
)

// userAgent marks both requests, so the query-frontend's query stats log lines of this tool can be told apart.
const userAgent = "mimir-cardinality-estimate-compare"

// client sends requests to one query-frontend as one tenant.
type client struct {
	baseURL string
	tenant  string
	http    *http.Client
}

// httpError is a non-200 answer.
type httpError struct {
	status int
	body   string
}

func (e *httpError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.status, e.body) }

// get sends a GET to path with params and decodes a 200 answer into out.
func (c *client) get(ctx context.Context, path string, params url.Values, out any) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path+"?"+params.Encode(), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("X-Scope-OrgID", c.tenant)
	req.Header.Set("User-Agent", userAgent)
	// Both answers are computed every time: no-store keeps the query-frontend's results cache and its estimate cache out of the pair.
	req.Header.Set("Cache-Control", "no-store")

	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		return time.Since(start), err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	latency := time.Since(start)
	if err != nil {
		return latency, err
	}
	if resp.StatusCode != http.StatusOK {
		msg := strings.TrimSpace(string(body))
		if len(msg) > 512 {
			msg = msg[:512]
		}
		return latency, &httpError{status: resp.StatusCode, body: msg}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return latency, fmt.Errorf("decoding %s response: %w", path, err)
	}
	return latency, nil
}

// estimateGroup and estimateResponse mirror the route's answer (pkg/querier/cardinality_estimate_handler.go).
type estimateGroup struct {
	Value     string  `json:"value"`
	Count     int64   `json:"count"`
	Counts    []int64 `json:"counts,omitempty"`
	Estimated bool    `json:"estimated,omitempty"`
}

type estimateResponse struct {
	Window struct {
		Start   int64 `json:"start"`
		End     int64 `json:"end"`
		Snapped bool  `json:"snapped,omitempty"`
	} `json:"window"`
	StepMS          int64             `json:"step_ms,omitempty"`
	GroupBy         string            `json:"group_by"`
	Read            string            `json:"read"`
	Source          string            `json:"source"`
	HeadWhole       bool              `json:"head_whole,omitempty"`
	HeadFallback    string            `json:"head_fallback,omitempty"`
	HeadMinTime     int64             `json:"head_min_time,omitempty"`
	CoverageGap     bool              `json:"coverage_gap,omitempty"`
	LowerBound      bool              `json:"lower_bound"`
	MissingBlocks   []string          `json:"missing_blocks,omitempty"`
	MissingShards   []json.RawMessage `json:"missing_shards,omitempty"`
	HasMore         bool              `json:"has_more"`
	Dedup           bool              `json:"dedup"`
	GroupCount      int               `json:"group_count"`
	Total           int64             `json:"total"`
	Totals          []int64           `json:"totals,omitempty"`
	EstimatedGroups int               `json:"estimated_groups"`
	Groups          []estimateGroup   `json:"groups"`
	// Error is the streamed table's trailer when the merge failed after the status was sent.
	Error string `json:"error,omitempty"`
}

// incomplete reports whether the answer says it leaves out series.
func (r *estimateResponse) incomplete() bool {
	return r.LowerBound || r.CoverageGap || r.Error != ""
}

func formatTime(ms int64) string { return strconv.FormatFloat(float64(ms)/1000, 'f', 3, 64) }

// estimate asks the route for the series per groupBy value of selector over [startMS, endMS), cut into buckets of step when step > 0.
func (c *client) estimate(ctx context.Context, selector, groupBy string, startMS, endMS int64, step time.Duration, limit int) (*estimateResponse, time.Duration, error) {
	params := url.Values{
		"match[]":  {selector},
		"group_by": {groupBy},
		"start":    {formatTime(startMS)},
		"end":      {formatTime(endMS)},
	}
	if step > 0 {
		params.Set("step", fmt.Sprintf("%ds", int64(step.Seconds())))
	}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	var out estimateResponse
	latency, err := c.get(ctx, "/prometheus/api/v1/cardinality/estimate", params, &out)
	if err != nil {
		return nil, latency, err
	}
	return &out, latency, nil
}

// instantQuery runs a PromQL instant query at atMS and returns each result's value keyed by the value of label in it.
func (c *client) instantQuery(ctx context.Context, query, label string, atMS int64) (map[string]int64, time.Duration, error) {
	var parsed struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	latency, err := c.get(ctx, "/prometheus/api/v1/query", url.Values{"query": {query}, "time": {formatTime(atMS)}}, &parsed)
	if err != nil {
		return nil, latency, err
	}
	if parsed.Status != "success" {
		return nil, latency, fmt.Errorf("query failed: %s", parsed.Error)
	}
	out := make(map[string]int64, len(parsed.Data.Result))
	for _, r := range parsed.Data.Result {
		s, ok := r.Value[1].(string)
		if !ok {
			return nil, latency, fmt.Errorf("query result value %v is not a string", r.Value[1])
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, latency, fmt.Errorf("query result value %q: %w", s, err)
		}
		out[r.Metric[label]] += int64(v)
	}
	return out, latency, nil
}

// labelNames returns the label names of the series matching selector (every series when empty), most distinct values first, from the ingesters' label names cardinality API.
func (c *client) labelNames(ctx context.Context, selector string, limit int) ([]string, error) {
	params := url.Values{"limit": {strconv.Itoa(limit)}}
	if selector != "" {
		params.Set("selector", selector)
	}
	var out querierapi.LabelNamesCardinalityResponse
	if _, err := c.get(ctx, "/prometheus/api/v1/cardinality/label_names", params, &out); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(out.Cardinality))
	for _, item := range out.Cardinality {
		names = append(names, item.LabelName)
	}
	return names, nil
}
