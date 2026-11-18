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

	"github.com/grafana/dskit/clusterutil"
	"github.com/prometheus/common/model"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
)

// userAgent marks both requests, so the query-frontend's query stats log lines of this tool can be told apart.
const userAgent = "mimir-cardinality-estimate-compare"

// client sends requests to one query-frontend as one tenant.
type client struct {
	baseURL string
	tenant  string
	http    *http.Client
	// clusterLabel is sent as the cluster validation header when set, for a query-frontend that validates it.
	clusterLabel string
	// eventualEstimates sends estimates without X-Read-Consistency: strong, as a dashboard does, so their latency has no wait for the ingesters to catch up.
	eventualEstimates bool
}

// httpError is a non-200 answer. body is the error text of a Prometheus API error body, or else the body itself.
type httpError struct {
	status    int
	errorType string
	body      string
}

func (e *httpError) Error() string {
	if e.errorType != "" {
		return fmt.Sprintf("HTTP %d (%s): %s", e.status, e.errorType, e.body)
	}
	return fmt.Sprintf("HTTP %d: %s", e.status, e.body)
}

// newHTTPError reads a non-200 answer, taking the error type and text from a Prometheus API error body ("status", "errorType", "error") when it is one.
func newHTTPError(status int, body []byte) *httpError {
	e := &httpError{status: status, body: strings.TrimSpace(string(body))}
	var apiErr struct {
		ErrorType string `json:"errorType"`
		Error     string `json:"error"`
	}
	if json.Unmarshal(body, &apiErr) == nil && apiErr.Error != "" {
		e.errorType, e.body = apiErr.ErrorType, apiErr.Error
	}
	if len(e.body) > 512 {
		e.body = e.body[:512]
	}
	return e
}

// get sends a GET to path with params and decodes a 200 answer into out. With strong, the request waits for the ingesters to have read everything written before it, so the estimate and its truth see the same head.
func (c *client) get(ctx context.Context, path string, params url.Values, strong bool, out any) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path+"?"+params.Encode(), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("X-Scope-OrgID", c.tenant)
	req.Header.Set("User-Agent", userAgent)
	if c.clusterLabel != "" {
		req.Header.Set(clusterutil.ClusterValidationLabelHeader, c.clusterLabel)
	}
	// Both answers are computed every time: no-store keeps the query-frontend's results cache and its estimate cache out of the pair.
	req.Header.Set("Cache-Control", "no-store")
	if strong {
		req.Header.Set(querierapi.ReadConsistencyHeader, querierapi.ReadConsistencyStrong)
	}

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
		return latency, newHTTPError(resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return latency, fmt.Errorf("decoding %s response: %w", path, err)
	}
	return latency, nil
}

// estimateGroup and estimateResponse mirror the route's answer (pkg/querier/cardinality_estimate_handler.go).
type estimateGroup struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type estimateResponse struct {
	Window struct {
		Start int64 `json:"start"`
		End   int64 `json:"end"`
	} `json:"window"`
	GroupBy     string `json:"group_by"`
	Read        string `json:"read"`
	Source      string `json:"source"`
	HeadMinTime int64  `json:"head_min_time,omitempty"`
	CoverageGap bool   `json:"coverage_gap,omitempty"`
	LowerBound  bool   `json:"lower_bound"`
	// UpperBound is set when head counts of several partitions were added up, so a series may count twice.
	UpperBound    bool            `json:"upper_bound,omitempty"`
	MissingBlocks []string        `json:"missing_blocks,omitempty"`
	MissingShards []missingShard  `json:"missing_shards,omitempty"`
	HasMore       bool            `json:"has_more"`
	Dedup         bool            `json:"dedup"`
	GroupCount    int             `json:"group_count"`
	Total         int64           `json:"total"`
	Groups        []estimateGroup `json:"groups"`
}

// missingShard is a compactor shard no block holds, over its block range [start, end).
type missingShard struct {
	Shard string `json:"shard"`
	Start int64  `json:"start"`
	End   int64  `json:"end"`
}

// incomplete reports whether the answer says it leaves out series.
func (r *estimateResponse) incomplete() bool {
	return r.LowerBound || r.CoverageGap
}

// covered is the longest part of [start, end) the answer says it counted every series of, and false when it doesn't say. The ingesters hold everything from head_min_time on. Without it, a missing shard leaves out its block range, and a missing block or a coverage gap doesn't say which part it leaves out.
func (r *estimateResponse) covered(start, end int64) (int64, int64, bool) {
	if r.HeadMinTime > 0 {
		from := max(start, r.HeadMinTime)
		return from, end, from < end
	}
	if len(r.MissingBlocks) > 0 || r.CoverageGap {
		return 0, 0, false
	}
	parts := [][2]int64{{start, end}}
	for _, s := range r.MissingShards {
		var left [][2]int64
		for _, p := range parts {
			if s.End <= p[0] || s.Start >= p[1] {
				left = append(left, p)
				continue
			}
			if p[0] < s.Start {
				left = append(left, [2]int64{p[0], s.Start})
			}
			if s.End < p[1] {
				left = append(left, [2]int64{s.End, p[1]})
			}
		}
		parts = left
	}
	var best [2]int64
	for _, p := range parts {
		if p[1]-p[0] > best[1]-best[0] {
			best = p
		}
	}
	return best[0], best[1], best[1] > best[0]
}

// flags names the answer's flags that are set, for the flags counter and the log.
func (r *estimateResponse) flags() []string {
	var out []string
	for _, f := range []struct {
		name string
		set  bool
	}{
		{"lower_bound", r.LowerBound},
		{"coverage_gap", r.CoverageGap},
		{"upper_bound", r.UpperBound},
		{"head_min_time", r.HeadMinTime != 0},
		{"missing_blocks", len(r.MissingBlocks) > 0},
		{"missing_shards", len(r.MissingShards) > 0},
		{"has_more", r.HasMore},
		{"dedup", r.Dedup},
	} {
		if f.set {
			out = append(out, f.name)
		}
	}
	return out
}

func formatTime(ms int64) string { return strconv.FormatFloat(float64(ms)/1000, 'f', 3, 64) }

// estimate asks the route for the series per groupBy value of selector over [startMS, endMS).
func (c *client) estimate(ctx context.Context, selector, groupBy string, startMS, endMS int64, limit int) (*estimateResponse, time.Duration, error) {
	params := url.Values{
		"match[]":  {selector},
		"group_by": {groupBy},
		"start":    {formatTime(startMS)},
		"end":      {formatTime(endMS)},
	}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	var out estimateResponse
	latency, err := c.get(ctx, "/prometheus/api/v1/cardinality/estimate", params, !c.eventualEstimates, &out)
	if err != nil {
		return nil, latency, err
	}
	return &out, latency, nil
}

// labelValues returns up to limit values of label in the tenant's index over [startMS, endMS). Values come from the index without a time filter beyond picking the blocks and heads that overlap the range.
func (c *client) labelValues(ctx context.Context, label string, startMS, endMS int64, limit int) ([]string, error) {
	params := url.Values{"start": {formatTime(startMS)}, "end": {formatTime(endMS - 1)}, "limit": {strconv.Itoa(limit)}}
	var out struct {
		Data []string `json:"data"`
	}
	_, err := c.get(ctx, "/prometheus/api/v1/label/"+url.PathEscape(model.EscapeName(label, model.ValueEncodingEscaping))+"/values", params, true, &out)
	return out.Data, err
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
	latency, err := c.get(ctx, "/prometheus/api/v1/query", url.Values{"query": {query}, "time": {formatTime(atMS)}}, true, &parsed)
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
	if _, err := c.get(ctx, "/prometheus/api/v1/cardinality/label_names", params, false, &out); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(out.Cardinality))
	for _, item := range out.Cardinality {
		names = append(names, item.LabelName)
	}
	return names, nil
}
