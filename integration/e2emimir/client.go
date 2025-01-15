// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/e2ecortex/client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package e2emimir

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	alertingmodels "github.com/grafana/alerting/models"
	alertingNotify "github.com/grafana/alerting/notify"
	"github.com/klauspost/compress/s2"
	alertConfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/types"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	promConfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/prompb" // OTLP protos are not compatible with gogo
	promRW2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/storage/remote"
	yaml "gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/alertmanager"
	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
)

var ErrNotFound = errors.New("not found")

// Client is a client used to interact with Mimir in integration tests
type Client struct {
	alertmanagerClient  promapi.Client
	querierAddress      string
	alertmanagerAddress string
	rulerAddress        string
	distributorAddress  string
	timeout             time.Duration
	httpClient          *http.Client
	querierClient       promv1.API
	orgID               string
}

type ClientOption func(*clientConfig)

func WithAddHeader(key, value string) ClientOption {
	return WithTripperware(func(tripper http.RoundTripper) http.RoundTripper {
		return querymiddleware.RoundTripFunc(func(req *http.Request) (*http.Response, error) {
			req.Header.Add(key, value)
			return tripper.RoundTrip(req)
		})
	})
}

func WithTripperware(wrap querymiddleware.Tripperware) ClientOption {
	return func(c *clientConfig) {
		c.defaultTransport = wrap(c.defaultTransport)
		c.querierTransport = wrap(c.querierTransport)
		c.alertmanagerTransport = wrap(c.alertmanagerTransport)
	}
}

type clientConfig struct {
	defaultTransport http.RoundTripper

	querierTransport      http.RoundTripper
	alertmanagerTransport http.RoundTripper
}

func defaultConfig() *clientConfig {
	cfg := &clientConfig{
		// Use fresh transport for each Client.
		defaultTransport:      http.DefaultTransport.(*http.Transport).Clone(),
		querierTransport:      http.DefaultTransport.(*http.Transport).Clone(),
		alertmanagerTransport: http.DefaultTransport.(*http.Transport).Clone(),
	}
	// Disable compression in querier client so it's easier to debug issue looking at the HTTP responses
	// logged by the querier.
	cfg.querierTransport.(*http.Transport).DisableCompression = true

	cfg.defaultTransport.(*http.Transport).MaxIdleConns = 0
	cfg.defaultTransport.(*http.Transport).MaxConnsPerHost = 0
	cfg.defaultTransport.(*http.Transport).MaxIdleConnsPerHost = 10000 // 0 would mean DefaultMaxIdleConnsPerHost, ie. 2.

	return cfg
}

// NewClient makes a new Mimir client
func NewClient(
	distributorAddress string,
	querierAddress string,
	alertmanagerAddress string,
	rulerAddress string,
	orgID string,
	opts ...ClientOption,
) (*Client, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	// Create querier API client
	querierAPIClient, err := promapi.NewClient(promapi.Config{
		Address:      "http://" + querierAddress + "/prometheus",
		RoundTripper: &addOrgIDRoundTripper{orgID: orgID, next: cfg.querierTransport},
	})
	if err != nil {
		return nil, err
	}

	c := &Client{
		distributorAddress:  distributorAddress,
		querierAddress:      querierAddress,
		alertmanagerAddress: alertmanagerAddress,
		rulerAddress:        rulerAddress,
		timeout:             5 * time.Second,
		httpClient:          &http.Client{Transport: cfg.defaultTransport},
		querierClient:       promv1.NewAPI(querierAPIClient),
		orgID:               orgID,
	}

	if alertmanagerAddress != "" {
		alertmanagerAPIClient, err := promapi.NewClient(promapi.Config{
			Address:      "http://" + alertmanagerAddress,
			RoundTripper: &addOrgIDRoundTripper{orgID: orgID, next: cfg.alertmanagerTransport},
		})
		if err != nil {
			return nil, err
		}
		c.alertmanagerClient = alertmanagerAPIClient
	}

	return c, nil
}

func (c *Client) SetTimeout(t time.Duration) {
	c.timeout = t
}

// Push the input timeseries to the remote endpoint
func (c *Client) Push(timeseries []prompb.TimeSeries) (*http.Response, error) {
	// Create write request
	data, err := proto.Marshal(&prompb.WriteRequest{Timeseries: timeseries})
	if err != nil {
		return nil, err
	}

	// Create HTTP request
	compressed := snappy.Encode(nil, data)
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api/v1/push", c.distributorAddress), bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	return res, nil
}

func (c *Client) PushRW2(writeRequest *promRW2.Request) (*http.Response, error) {
	// Create write request
	data, err := proto.Marshal(writeRequest)
	if err != nil {
		return nil, err
	}

	// Create HTTP request
	compressed := snappy.Encode(nil, data)
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api/v1/push", c.distributorAddress), bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf;proto=io.prometheus.write.v2.Request")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "2.0.0")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	return res, nil
}

// PushOTLP the input timeseries to the remote endpoint in OTLP format
func (c *Client) PushOTLP(timeseries []prompb.TimeSeries, metadata []mimirpb.MetricMetadata) (*http.Response, error) {
	// Create write request
	otlpRequest := distributor.TimeseriesToOTLPRequest(timeseries, metadata)

	data, err := otlpRequest.MarshalProto()
	if err != nil {
		return nil, err
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/otlp/v1/metrics", c.distributorAddress), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	return res, nil
}

// Query runs an instant query.
func (c *Client) Query(query string, ts time.Time) (model.Value, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	value, _, err := c.querierClient.Query(ctx, query, ts)
	return value, err
}

// Query runs a query range.
func (c *Client) QueryRange(query string, start, end time.Time, step time.Duration) (model.Value, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	value, _, err := c.querierClient.QueryRange(ctx, query, promv1.Range{
		Start: start,
		End:   end,
		Step:  step,
	})
	return value, err
}

// QueryRangeRaw runs a ranged query directly against the querier API.
func (c *Client) QueryRangeRaw(query string, start, end time.Time, step time.Duration) (*http.Response, []byte, error) {
	addr := fmt.Sprintf(
		"http://%s/prometheus/api/v1/query_range?query=%s&start=%s&end=%s&step=%s",
		c.querierAddress,
		url.QueryEscape(query),
		FormatTime(start),
		FormatTime(end),
		strconv.FormatFloat(step.Seconds(), 'f', -1, 64),
	)

	return c.DoGetBody(addr)
}

// RemoteRead runs a remote read query against the querier.
// RemoteRead uses samples streaming. See RemoteReadChunks as well for chunks streaming.
// RemoteRead returns the HTTP response with consumed body, the remote read protobuf response and an error.
// In case the response is not a protobuf, the plaintext body content is returned instead of the protobuf message.
func (c *Client) RemoteRead(query *prompb.Query) (_ *http.Response, _ *prompb.QueryResult, plaintextResponse []byte, _ error) {
	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{query},
	}
	resp, err := c.doRemoteReadReq(req)
	if err != nil {
		return resp, nil, nil, fmt.Errorf("making remote read request: %w", err)
	}
	switch contentType := resp.Header.Get("Content-Type"); contentType {
	case "application/x-protobuf":
		if encoding := resp.Header.Get("Content-Encoding"); encoding != "snappy" {
			return resp, nil, nil, fmt.Errorf("remote read should return snappy-encoded protobuf; got %s %s instead", encoding, contentType)
		}
		queryResult, err := parseRemoteReadSamples(resp)
		if err != nil {
			return resp, queryResult, nil, fmt.Errorf("parsing remote read response: %w", err)
		}
		return resp, queryResult, nil, nil
	case "application/json":
		// The remote read protocol does not have a way to return an error message
		// in the response body, thus the error message is returned as a Prometheus
		// JSON results response.
		fallthrough
	case "text/plain; charset=utf-8":
		respBytes, err := io.ReadAll(resp.Body)
		return resp, nil, respBytes, err
	default:
		return resp, nil, nil, fmt.Errorf("unexpected content type %s", contentType)
	}
}

// RemoteReadChunks runs a remote read query against the querier.
// RemoteReadChunks uses chunks streaming. See RemoteRead as well for samples streaming.
// RemoteReadChunks returns the HTTP response with consumed body, the remote read protobuf response and an error.
// In case the response is not a protobuf, the plaintext body content is returned instead of the protobuf message.
func (c *Client) RemoteReadChunks(query *prompb.Query) (_ *http.Response, _ []prompb.ChunkedReadResponse, plaintextResponse []byte, _ error) {
	req := &prompb.ReadRequest{
		Queries:               []*prompb.Query{query},
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
	}

	resp, err := c.doRemoteReadReq(req)
	if err != nil {
		return resp, nil, nil, fmt.Errorf("making remote read request: %w", err)
	}
	switch contentType := resp.Header.Get("Content-Type"); contentType {
	case api.ContentTypeRemoteReadStreamedChunks:
		if encoding := resp.Header.Get("Content-Encoding"); encoding != "" {
			return resp, nil, nil, fmt.Errorf("remote read should not return Content-Encoding; got %s %s instead", encoding, contentType)
		}
		chunks, err := parseRemoteReadChunks(resp)
		if err != nil {
			return resp, chunks, nil, fmt.Errorf("parsing remote read response: %w", err)
		}
		return resp, chunks, nil, nil
	case "text/plain; charset=utf-8":
		respBytes, err := io.ReadAll(resp.Body)
		return resp, nil, respBytes, err
	default:
		return resp, nil, nil, fmt.Errorf("unexpected content type %s", contentType)
	}
}

func (c *Client) doRemoteReadReq(req *prompb.ReadRequest) (*http.Response, error) {
	addr := fmt.Sprintf(
		"http://%s/prometheus/api/v1/read",
		c.querierAddress,
	)

	reqBytes, err := req.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshalling remote read request: %w", err)
	}

	return c.DoPost(addr, bytes.NewReader(snappy.Encode(nil, reqBytes)))
}

func parseRemoteReadSamples(resp *http.Response) (*prompb.QueryResult, error) {
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading remote read response: %w", err)
	}
	uncompressedBytes, err := snappy.Decode(nil, respBytes)
	if err != nil {
		return nil, fmt.Errorf("decompressing remote read response: %w", err)
	}
	response := &prompb.ReadResponse{}
	err = response.Unmarshal(uncompressedBytes)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling remote read response: %w", err)
	}
	return response.Results[0], nil
}

func parseRemoteReadChunks(resp *http.Response) ([]prompb.ChunkedReadResponse, error) {
	stream := remote.NewChunkedReader(resp.Body, promConfig.DefaultChunkedReadLimit, nil)

	var results []prompb.ChunkedReadResponse
	for {
		var res prompb.ChunkedReadResponse
		err := stream.NextProto(&res)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading remote read response: %w", err)
		}
		results = append(results, res)
	}
	return results, nil
}

// QueryExemplars runs an exemplar query.
func (c *Client) QueryExemplars(query string, start, end time.Time) ([]promv1.ExemplarQueryResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	return c.querierClient.QueryExemplars(ctx, query, start, end)
}

// QuerierAddress returns the address of the querier
func (c *Client) QuerierAddress() string {
	return c.querierAddress
}

// QueryRaw runs a query directly against the querier API.
func (c *Client) QueryRaw(query string) (*http.Response, []byte, error) {
	addr := fmt.Sprintf("http://%s/prometheus/api/v1/query?query=%s", c.querierAddress, url.QueryEscape(query))

	return c.DoGetBody(addr)
}

func (c *Client) QueryRawAt(query string, ts time.Time) (*http.Response, []byte, error) {
	addr := fmt.Sprintf("http://%s/prometheus/api/v1/query?query=%s&time=%s", c.querierAddress, url.QueryEscape(query), FormatTime(ts))

	return c.DoGetBody(addr)
}

// Series finds series by label matchers.
func (c *Client) Series(matches []string, start, end time.Time, opts ...promv1.Option) ([]model.LabelSet, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	result, _, err := c.querierClient.Series(ctx, matches, start, end, opts...)
	return result, err
}

// LabelValues gets label values
func (c *Client) LabelValues(label string, start, end time.Time, matches []string, opts ...promv1.Option) (model.LabelValues, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	result, _, err := c.querierClient.LabelValues(ctx, label, matches, start, end, opts...)
	return result, err
}

// LabelNames gets label names
func (c *Client) LabelNames(start, end time.Time, matches []string, opts ...promv1.Option) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	result, _, err := c.querierClient.LabelNames(ctx, matches, start, end, opts...)
	return result, err
}

// LabelNamesAndValues returns distinct label values per label name.
func (c *Client) LabelNamesAndValues(selector string, limit int) (*api.LabelNamesCardinalityResponse, error) {
	body := make(url.Values)
	if len(selector) > 0 {
		body.Set("selector", selector)
	}
	if limit > 0 {
		body.Set("limit", strconv.Itoa(limit))
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/prometheus/api/v1/cardinality/label_names", c.querierAddress), strings.NewReader(body.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Scope-OrgID", c.orgID)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(body.Encode())))

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}
	var lvalsResp api.LabelNamesCardinalityResponse
	err = json.NewDecoder(resp.Body).Decode(&lvalsResp)
	if err != nil {
		return nil, fmt.Errorf("error decoding label values response: %w", err)
	}
	return &lvalsResp, nil
}

// LabelValuesCardinality returns all values and series total count for each label name.
func (c *Client) LabelValuesCardinality(labelNames []string, selector string, limit int) (*api.LabelValuesCardinalityResponse, error) {
	body := make(url.Values)
	if len(selector) > 0 {
		body.Set("selector", selector)
	}
	if limit > 0 {
		body.Set("limit", strconv.Itoa(limit))
	}
	for _, lbName := range labelNames {
		body.Add("label_names[]", lbName)
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/prometheus/api/v1/cardinality/label_values", c.querierAddress), strings.NewReader(body.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Scope-OrgID", c.orgID)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(body.Encode())))

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}

	var lvalsResp api.LabelValuesCardinalityResponse
	err = json.NewDecoder(resp.Body).Decode(&lvalsResp)
	if err != nil {
		return nil, fmt.Errorf("error decoding label values response: %w", err)
	}
	return &lvalsResp, nil
}

type activeSeriesRequestConfig struct {
	method         string
	useCompression bool
	header         http.Header
}

type ActiveSeriesOption func(*activeSeriesRequestConfig)

func WithEnableCompression() ActiveSeriesOption {
	return func(c *activeSeriesRequestConfig) {
		c.useCompression = true
		c.header.Set("Accept-Encoding", "x-snappy-framed")
	}
}

func WithRequestMethod(m string) ActiveSeriesOption {
	return func(c *activeSeriesRequestConfig) {
		c.method = m
	}
}

func WithQueryShards(n int) ActiveSeriesOption {
	return func(c *activeSeriesRequestConfig) {
		c.header.Set("Sharding-Control", strconv.Itoa(n))
	}
}

func (c *Client) ActiveSeries(selector string, options ...ActiveSeriesOption) (*api.ActiveSeriesResponse, error) {
	cfg := activeSeriesRequestConfig{method: http.MethodGet, header: http.Header{"X-Scope-OrgID": []string{c.orgID}}}
	for _, option := range options {
		option(&cfg)
	}

	req, err := http.NewRequest(cfg.method, fmt.Sprintf("http://%s/prometheus/api/v1/cardinality/active_series", c.querierAddress), nil)
	if err != nil {
		return nil, err
	}
	req.Header = cfg.header

	q := req.URL.Query()
	q.Set("selector", selector)
	switch cfg.method {
	case http.MethodGet:
		req.URL.RawQuery = q.Encode()
	case http.MethodPost:
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		req.Body = io.NopCloser(strings.NewReader(q.Encode()))
	default:
		return nil, fmt.Errorf("invalid method %s", cfg.method)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer func(body io.ReadCloser) {
		_, _ = io.ReadAll(body)
		_ = body.Close()
	}(resp.Body)

	var bodyReader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "x-snappy-framed" {
		bodyReader = s2.NewReader(bodyReader)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(bodyReader)
		return nil, fmt.Errorf("unexpected status code %d, body: %s", resp.StatusCode, body)
	}

	res := &api.ActiveSeriesResponse{}
	err = json.NewDecoder(bodyReader).Decode(res)
	if err != nil {
		return nil, fmt.Errorf("error decoding active series response: %w", err)
	}
	return res, nil
}

func (c *Client) ActiveNativeHistogramMetrics(selector string, options ...ActiveSeriesOption) (*cardinality.ActiveNativeHistogramMetricsResponse, error) {
	cfg := activeSeriesRequestConfig{method: http.MethodGet, header: http.Header{"X-Scope-OrgID": []string{c.orgID}}}
	for _, option := range options {
		option(&cfg)
	}

	req, err := http.NewRequest(cfg.method, fmt.Sprintf("http://%s/prometheus/api/v1/cardinality/active_native_histogram_metrics", c.querierAddress), nil)
	if err != nil {
		return nil, err
	}
	req.Header = cfg.header

	q := req.URL.Query()
	q.Set("selector", selector)
	switch cfg.method {
	case http.MethodGet:
		req.URL.RawQuery = q.Encode()
	case http.MethodPost:
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		req.Body = io.NopCloser(strings.NewReader(q.Encode()))
	default:
		return nil, fmt.Errorf("invalid method %s", cfg.method)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer func(body io.ReadCloser) {
		_, _ = io.ReadAll(body)
		_ = body.Close()
	}(resp.Body)

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d, body: %s", resp.StatusCode, body)
	}

	if resp.Header.Get("Content-Encoding") == "snappy" {
		body, err = snappy.Decode(nil, body)
		if err != nil {
			return nil, fmt.Errorf("error decoding snappy response: %w", err)
		}
	}

	res := &cardinality.ActiveNativeHistogramMetricsResponse{}
	err = json.Unmarshal(body, res)
	if err != nil {
		return nil, fmt.Errorf("error decoding active native histograms response: %w", err)
	}
	return res, nil
}

// GetPrometheusMetadata fetches the metadata from the Prometheus endpoint /api/v1/metadata.
func (c *Client) GetPrometheusMetadata() (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/prometheus/api/v1/metadata", c.querierAddress), nil)

	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	return c.httpClient.Do(req.WithContext(ctx))
}

type addOrgIDRoundTripper struct {
	orgID string
	next  http.RoundTripper
}

func (r *addOrgIDRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("X-Scope-OrgID", r.orgID)

	return r.next.RoundTrip(req)
}

// ServerStatus represents a Alertmanager status response
// TODO: Upgrade to Alertmanager v0.20.0+ and utilize vendored structs
type ServerStatus struct {
	Config struct {
		Original string `json:"original"`
	} `json:"config"`
}

type successResult struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data,omitempty"`
}

// GetPrometheusRules fetches the rules from the Prometheus endpoint /api/v1/rules.
func (c *Client) GetPrometheusRules(maxGroups int, token string) ([]*promv1.RuleGroup, string, error) {
	url, err := url.Parse(fmt.Sprintf("http://%s/prometheus/api/v1/rules", c.rulerAddress))
	if err != nil {
		return nil, "", err
	}
	if token != "" {
		q := url.Query()
		q.Add("group_next_token", token)
		url.RawQuery = q.Encode()
	}

	if maxGroups != 0 {
		q := url.Query()
		q.Add("group_limit", strconv.Itoa(maxGroups))
		url.RawQuery = q.Encode()
	}

	// Create HTTP request
	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, "", err
	}

	// Decode the response.
	type response struct {
		Status string `json:"status"`
		Data   struct {
			RuleGroups []*promv1.RuleGroup `json:"groups"`
			NextToken  string              `json:"groupNextToken,omitempty"`
		} `json:"data"`
	}

	decoded := response{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, "", err
	}

	if decoded.Status != "success" {
		return nil, "", fmt.Errorf("unexpected response status '%s'", decoded.Status)
	}

	return decoded.Data.RuleGroups, decoded.Data.NextToken, nil
}

// GetRuleGroups gets the configured rule groups from the ruler.
func (c *Client) GetRuleGroups() (*http.Response, map[string][]rulefmt.RuleGroup, error) {
	// Create HTTP request
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/prometheus/config/v1/rules", c.rulerAddress), nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	resp, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, nil, err
	}

	defer resp.Body.Close()
	rgs := map[string][]rulefmt.RuleGroup{}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	err = yaml.Unmarshal(data, rgs)
	if err != nil {
		return nil, nil, err
	}

	return resp, rgs, nil
}

// SetRuleGroup configures the provided rulegroup to the ruler.
func (c *Client) SetRuleGroup(rulegroup rulefmt.RuleGroup, namespace string) error {
	// Create write request
	data, err := yaml.Marshal(rulegroup)
	if err != nil {
		return err
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/prometheus/config/v1/rules/%s", c.rulerAddress, url.PathEscape(namespace)), bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.StatusCode != 202 {
		return fmt.Errorf("unexpected status code: %d", res.StatusCode)
	}

	return nil
}

// GetRuleGroup gets a rule group.
func (c *Client) GetRuleGroup(namespace string, groupName string) (*http.Response, error) {
	// Create HTTP request
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/prometheus/config/v1/rules/%s/%s", c.rulerAddress, url.PathEscape(namespace), url.PathEscape(groupName)), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	return c.httpClient.Do(req.WithContext(ctx))
}

// DeleteRuleGroup deletes a rule group.
func (c *Client) DeleteRuleGroup(namespace string, groupName string) error {
	// Create HTTP request
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/prometheus/config/v1/rules/%s/%s", c.rulerAddress, url.PathEscape(namespace), url.PathEscape(groupName)), nil)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	resp, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 202 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// DeleteRuleNamespace deletes all the rule groups (and the namespace itself).
func (c *Client) DeleteRuleNamespace(namespace string) error {
	// Create HTTP request
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/prometheus/config/v1/rules/%s", c.rulerAddress, url.PathEscape(namespace)), nil)
	if err != nil {
		return err
	}

	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	resp, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 202 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)

	}

	return nil
}

// userConfig is used to communicate a users alertmanager configs
type userConfig struct {
	TemplateFiles      map[string]string `yaml:"template_files"`
	AlertmanagerConfig string            `yaml:"alertmanager_config"`
}

// GetAlertmanagerStatusPage gets the status page of alertmanager.
func (c *Client) GetAlertmanagerStatusPage(ctx context.Context) ([]byte, error) {
	return c.getRawPage(ctx, "http://"+c.alertmanagerAddress+"/multitenant_alertmanager/status")
}

func (c *Client) getRawPage(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("fetching page failed with status %d and content %v", resp.StatusCode, string(content))
	}
	return content, nil
}

// GetAlertmanager performs a GET request on a per-tenant alertmanager endpoint.
func (c *Client) GetAlertmanager(ctx context.Context, path string) (string, error) {
	return c.doAlertmanagerRequest(ctx, http.MethodGet, path)
}

// PostAlertmanager performs a POST request on a per-tenant alertmanager endpoint.
func (c *Client) PostAlertmanager(ctx context.Context, path string) error {
	_, err := c.doAlertmanagerRequest(ctx, http.MethodPost, path)
	return err
}

func (c *Client) doAlertmanagerRequest(ctx context.Context, method string, path string) (string, error) {
	u := c.alertmanagerClient.URL(fmt.Sprintf("alertmanager/%s", path), nil)

	req, err := http.NewRequest(method, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return "", err
	}

	if resp.StatusCode == http.StatusNotFound {
		return "", ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("getting %s failed with status %d and error %v", path, resp.StatusCode, string(body))
	}
	return string(body), nil
}

// GetAlertmanagerConfig gets the status of an alertmanager instance
func (c *Client) GetAlertmanagerConfig(ctx context.Context) (*alertConfig.Config, error) {
	u := c.alertmanagerClient.URL("/alertmanager/api/v2/status", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting config failed with status %d and error %v", resp.StatusCode, string(body))
	}

	var ss *ServerStatus
	err = json.Unmarshal(body, &ss)
	if err != nil {
		return nil, err
	}

	cfg := &alertConfig.Config{}
	err = yaml.Unmarshal([]byte(ss.Config.Original), cfg)

	return cfg, err
}

// SetAlertmanagerConfig gets the status of an alertmanager instance
func (c *Client) SetAlertmanagerConfig(ctx context.Context, amConfig string, templates map[string]string) error {
	u := c.alertmanagerClient.URL("/api/v1/alerts", nil)

	data, err := yaml.Marshal(&userConfig{
		AlertmanagerConfig: amConfig,
		TemplateFiles:      templates,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("setting config failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

// DeleteAlertmanagerConfig gets the status of an alertmanager instance
func (c *Client) DeleteAlertmanagerConfig(ctx context.Context) error {
	u := c.alertmanagerClient.URL("/api/v1/alerts", nil)
	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deleting config failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

func (c *Client) GetGrafanaAlertmanagerConfig(ctx context.Context) (*alertmanager.UserGrafanaConfig, error) {
	u := c.alertmanagerClient.URL("/api/v1/grafana/config", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting grafana config failed with status %d and error %v", resp.StatusCode, string(body))
	}

	var sr *successResult
	err = json.Unmarshal(body, &sr)
	if err != nil {
		return nil, err
	}

	var ugc *alertmanager.UserGrafanaConfig
	err = json.Unmarshal(sr.Data, &ugc)
	if err != nil {
		return nil, err
	}

	return ugc, err
}

func (c *Client) SetGrafanaAlertmanagerConfig(ctx context.Context, createdAtTimestamp int64, cfg, hash, externalURL string, isDefault, isPromoted bool, staticHeaders map[string]string) error {
	var grafanaConfig alertmanager.GrafanaAlertmanagerConfig
	if err := json.Unmarshal([]byte(cfg), &grafanaConfig); err != nil {
		return err
	}

	u := c.alertmanagerClient.URL("/api/v1/grafana/config", nil)
	data, err := json.Marshal(&alertmanager.UserGrafanaConfig{
		GrafanaAlertmanagerConfig: grafanaConfig,
		Hash:                      hash,
		CreatedAt:                 createdAtTimestamp,
		Default:                   isDefault,
		Promoted:                  isPromoted,
		ExternalURL:               externalURL,
		StaticHeaders:             staticHeaders,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("setting grafana config failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

func (c *Client) DeleteGrafanaAlertmanagerConfig(ctx context.Context) error {
	u := c.alertmanagerClient.URL("/api/v1/grafana/config", nil)
	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deleting grafana config failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

func (c *Client) GetGrafanaAlertmanagerState(ctx context.Context) (*alertmanager.UserGrafanaState, error) {
	u := c.alertmanagerClient.URL("/api/v1/grafana/state", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting grafana state failed with status %d and error %v", resp.StatusCode, string(body))
	}

	var sr *successResult
	err = json.Unmarshal(body, &sr)
	if err != nil {
		return nil, err
	}

	var ugs *alertmanager.UserGrafanaState
	err = json.Unmarshal(sr.Data, &ugs)
	if err != nil {
		return nil, err
	}

	return ugs, err
}

func (c *Client) SetGrafanaAlertmanagerState(ctx context.Context, state string) error {
	u := c.alertmanagerClient.URL("/api/v1/grafana/state", nil)

	data, err := json.Marshal(&alertmanager.UserGrafanaState{
		State: state,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("setting grafana state failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

func (c *Client) DeleteGrafanaAlertmanagerState(ctx context.Context) error {
	u := c.alertmanagerClient.URL("/api/v1/grafana/state", nil)
	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deleting grafana state failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

// SendAlertToAlermanager sends alerts to the Alertmanager API
func (c *Client) SendAlertToAlermanager(ctx context.Context, alert *model.Alert) error {
	u := c.alertmanagerClient.URL("/alertmanager/api/v2/alerts", nil)

	data, err := json.Marshal([]types.Alert{{Alert: *alert}})
	if err != nil {
		return fmt.Errorf("error marshaling the alert: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sending alert failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

func (c *Client) GetAlerts(ctx context.Context) ([]model.Alert, error) {
	u := c.alertmanagerClient.URL("alertmanager/api/v2/alerts", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting alerts failed with status %d and error %v", resp.StatusCode, string(body))
	}

	decoded := []model.Alert{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}

	return decoded, nil
}

type AlertGroup struct {
	Labels model.LabelSet `json:"labels"`
	Alerts []model.Alert  `json:"alerts"`
}

func (c *Client) GetAlertGroups(ctx context.Context) ([]AlertGroup, error) {
	u := c.alertmanagerClient.URL("alertmanager/api/v2/alerts/groups", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting alert groups failed with status %d and error %v", resp.StatusCode, string(body))
	}

	decoded := []AlertGroup{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

// CreateSilence creates a new silence and returns the unique identifier of the silence.
func (c *Client) CreateSilence(ctx context.Context, silence types.Silence) (string, error) {
	u := c.alertmanagerClient.URL("alertmanager/api/v2/silences", nil)

	data, err := json.Marshal(silence)
	if err != nil {
		return "", fmt.Errorf("error marshaling the silence: %s", err)
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("creating the silence failed with status %d and error %v", resp.StatusCode, string(body))
	}

	type response struct {
		SilenceID string `json:"silenceID"`
	}

	decoded := response{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", err
	}

	return decoded.SilenceID, nil
}

func (c *Client) GetSilences(ctx context.Context) ([]types.Silence, error) {
	u := c.alertmanagerClient.URL("alertmanager/api/v2/silences", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting silences failed with status %d and error %v", resp.StatusCode, string(body))
	}

	decoded := []types.Silence{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}

	return decoded, nil
}

func (c *Client) GetSilence(ctx context.Context, id string) (types.Silence, error) {
	u := c.alertmanagerClient.URL(fmt.Sprintf("alertmanager/api/v2/silence/%s", url.PathEscape(id)), nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return types.Silence{}, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return types.Silence{}, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return types.Silence{}, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return types.Silence{}, fmt.Errorf("getting silence failed with status %d and error %v", resp.StatusCode, string(body))
	}

	decoded := types.Silence{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return types.Silence{}, err
	}

	return decoded, nil
}

func (c *Client) DeleteSilence(ctx context.Context, id string) error {
	u := c.alertmanagerClient.URL(fmt.Sprintf("alertmanager/api/v2/silence/%s", url.PathEscape(id)), nil)

	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deleting silence failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

func (c *Client) GetReceivers(ctx context.Context) ([]string, error) {
	u := c.alertmanagerClient.URL("alertmanager/api/v2/receivers", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting receivers failed with status %d and error %v", resp.StatusCode, string(body))
	}

	type response []struct {
		Name string `json:"name"`
	}

	decoded := response{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}

	var receivers []string
	for _, v := range decoded {
		receivers = append(receivers, v.Name)
	}
	return receivers, nil
}

func (c *Client) GetReceiversExperimental(ctx context.Context) ([]alertingmodels.Receiver, error) {
	u := c.alertmanagerClient.URL("api/v1/grafana/receivers", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting receivers failed with status %d and error %v", resp.StatusCode, string(body))
	}

	decoded := []alertingmodels.Receiver{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}

	return decoded, nil
}

func (c *Client) TestTemplatesExperimental(ctx context.Context, ttConf alertingNotify.TestTemplatesConfigBodyParams) (*alertingNotify.TestTemplatesResults, error) {
	u := c.alertmanagerClient.URL("api/v1/grafana/templates/test", nil)

	data, err := json.Marshal(ttConf)
	if err != nil {
		return nil, fmt.Errorf("error marshalling test templates config: %s", err)
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("testing templates failed with status %d and error %s", resp.StatusCode, string(body))
	}

	decoded := alertingNotify.TestTemplatesResults{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}

	return &decoded, nil
}

func (c *Client) TestReceiversExperimental(ctx context.Context, trConf alertingNotify.TestReceiversConfigBodyParams) (*alertingNotify.TestReceiversResult, error) {
	u := c.alertmanagerClient.URL("api/v1/grafana/receivers/test", nil)

	data, err := json.Marshal(trConf)
	if err != nil {
		return nil, fmt.Errorf("error marshalling test receivers config: %s", err)
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("testing receivers failed with status %d and error %s", resp.StatusCode, string(body))
	}

	decoded := alertingNotify.TestReceiversResult{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}

	return &decoded, nil
}

// DoGet performs a HTTP GET request towards the supplied URL. The request
// contains the X-Scope-OrgID header and the timeout configured by the client
// object.
func (c *Client) DoGet(url string) (*http.Response, error) {
	return c.doRequest("GET", url, nil)
}

// DoGetBody performs a HTTP GET request towards the supplied URL and returns
// the full response body. The request contains the X-Scope-OrgID header and
// the timeout configured by the client object.
func (c *Client) DoGetBody(url string) (*http.Response, []byte, error) {
	resp, err := c.DoGet(url)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	return resp, body, nil
}

// DoPost performs a HTTP POST request towards the supplied URL and using the
// given request body. The request contains the X-Scope-OrgID header and the
// timeout configured by the client object.
func (c *Client) DoPost(url string, body io.Reader) (*http.Response, error) {
	return c.doRequest("POST", url, body)
}

// DoPostBody performs a HTTP POST request towards the supplied URL and returns
// the full response body. The request contains the X-Scope-OrgID header and
// the timeout configured by the client object.
func (c *Client) DoPostBody(url string, body io.Reader) (*http.Response, []byte, error) {
	resp, err := c.DoPost(url, body)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	return resp, respBytes, nil
}

func (c *Client) doRequest(method, url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Scope-OrgID", c.orgID)

	client := *c.httpClient
	client.Timeout = c.timeout

	return client.Do(req)
}

// FormatTime converts a time to a string acceptable by the Prometheus API.
func FormatTime(t time.Time) string {
	return strconv.FormatFloat(float64(t.Unix())+float64(t.Nanosecond())/1e9, 'f', -1, 64)
}

func (c *Client) CloseIdleConnections() {
	c.httpClient.CloseIdleConnections()
}
