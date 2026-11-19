// SPDX-License-Identifier: AGPL-3.0-only

package nativemetadataprober

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

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
)

const (
	// The query-frontend's header that asks for the query's stats, which it returns in the Server-Timing header.
	responseQueryStatsHeader = "X-Mimir-Response-Query-Stats"
	serverTimingHeader       = "Server-Timing"
	// How much of an error response's body an error quotes.
	maxErrorBody = 512
)

// HTTPClient queries a cell's Prometheus API as a tenant, through a gateway that takes the tenant from the request's
// header, as the cell's internal one does.
type HTTPClient struct {
	// The API's address, with its prefix, such as http://gateway/prometheus.
	address string
	client  *http.Client
}

func NewHTTPClient(address string, timeout time.Duration) (*HTTPClient, error) {
	u, err := url.Parse(address)
	if err != nil {
		return nil, fmt.Errorf("parsing the address: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("the address %q has no scheme or host", address)
	}
	return &HTTPClient{address: strings.TrimSuffix(address, "/"), client: &http.Client{Timeout: timeout}}, nil
}

func (c *HTTPClient) Query(ctx context.Context, tenant, query string, ts time.Time, mode Mode) (QueryResult, error) {
	params := url.Values{"query": {query}, "time": {formatTime(ts)}}
	req, err := c.request(ctx, tenant, "/api/v1/query", params)
	if err != nil {
		return QueryResult{}, err
	}
	req.Header.Set(querierapi.NativeTargetInfoHeader, string(mode))
	req.Header.Set(responseQueryStatsHeader, "true")
	// Experiment tenants skip the results cache, but a probe answered from it would compare nothing.
	req.Header.Set(requestoptions.CacheControlHeader, requestoptions.NoStoreValue)
	header, body, err := c.do(req)
	if err != nil {
		return QueryResult{}, err
	}

	var resp struct {
		Data struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return QueryResult{}, fmt.Errorf("decoding the answer: %w", err)
	}
	if resp.Data.ResultType != model.ValVector.String() {
		return QueryResult{}, fmt.Errorf("the answer is a %q, not an instant vector", resp.Data.ResultType)
	}
	var vector model.Vector
	if err := json.Unmarshal(resp.Data.Result, &vector); err != nil {
		return QueryResult{}, fmt.Errorf("decoding the answer's vector: %w", err)
	}

	res := QueryResult{Samples: make([]Sample, 0, len(vector))}
	for _, s := range vector {
		builder := labels.NewScratchBuilder(len(s.Metric))
		for name, value := range s.Metric {
			builder.Add(string(name), string(value))
		}
		builder.Sort()
		value := s.Value.String()
		if s.Histogram != nil {
			value = s.Histogram.String()
		}
		res.Samples = append(res.Samples, Sample{Labels: builder.Labels(), Value: value})
	}
	stats := serverTimingValues(header.Get(serverTimingHeader))
	res.LookupResponseBytes = stats["native_lookup_response_bytes"]
	res.TargetInfoFallbacks = stats["native_target_info_fallbacks"]
	return res, nil
}

func (c *HTTPClient) Lookup(ctx context.Context, tenant string, lookup Lookup, matchers []string, start, end time.Time) (LookupResult, error) {
	params := url.Values{"match[]": matchers}
	// Native metric metadata has no time range: its versions are the series' own.
	if lookup == LookupResources {
		params.Set("start", formatTime(start))
		params.Set("end", formatTime(end))
	}
	req, err := c.request(ctx, tenant, "/api/v1/"+string(lookup), params)
	if err != nil {
		return LookupResult{}, err
	}
	_, body, err := c.do(req)
	if err != nil {
		return LookupResult{}, err
	}
	var resp struct {
		Results []json.RawMessage `json:"results"`
		Limited bool              `json:"limited"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return LookupResult{}, fmt.Errorf("decoding the answer: %w", err)
	}
	return LookupResult{Series: len(resp.Results), Limited: resp.Limited}, nil
}

func (c *HTTPClient) request(ctx context.Context, tenant, path string, params url.Values) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.address+path+"?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(user.OrgIDHeaderName, tenant)
	return req, nil
}

// do sends the request, and returns the answer's header and body, or an error for any answer but a 200.
func (c *HTTPClient) do(req *http.Request) (http.Header, []byte, error) {
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("reading the answer: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("status %d: %s", resp.StatusCode, body[:min(len(body), maxErrorBody)])
	}
	return resp.Header, body, nil
}

// serverTimingValues are the values of a Server-Timing header's counts, name;val=N, by name: the query-frontend reports
// the native metadata prototype's lookups and fallbacks so, and only for a query that made them.
func serverTimingValues(header string) map[string]uint64 {
	values := map[string]uint64{}
	for _, metric := range strings.Split(header, ",") {
		name, params, _ := strings.Cut(strings.TrimSpace(metric), ";")
		for _, param := range strings.Split(params, ";") {
			key, value, ok := strings.Cut(strings.TrimSpace(param), "=")
			if !ok || key != "val" {
				continue
			}
			if n, err := strconv.ParseUint(value, 10, 64); err == nil {
				values[name] = n
			}
		}
	}
	return values
}

func formatTime(t time.Time) string {
	return strconv.FormatFloat(float64(t.UnixMilli())/1e3, 'f', -1, 64)
}
