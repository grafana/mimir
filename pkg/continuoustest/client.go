// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util/instrumentation"
	util_math "github.com/grafana/mimir/pkg/util/math"
)

const (
	maxErrMsgLen = 256
)

// MimirClient is the interface implemented by a client used to interact with Mimir.
type MimirClient interface {
	// WriteSeries writes input series to Mimir. Returns the response status code and optionally
	// an error. The error is always returned if request was not successful (eg. received a 4xx or 5xx error).
	WriteSeries(ctx context.Context, series []prompb.TimeSeries) (statusCode int, err error)

	// QueryRange performs a range query.
	QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration, options ...RequestOption) (model.Matrix, error)

	// Query performs an instant query.
	Query(ctx context.Context, query string, ts time.Time, options ...RequestOption) (model.Vector, error)
}

type ClientConfig struct {
	TenantID          string
	BasicAuthUser     string
	BasicAuthPassword string
	BearerToken       string

	WriteBaseEndpoint flagext.URLValue
	WriteBatchSize    int
	WriteTimeout      time.Duration
	WriteProtocol     string

	ReadBaseEndpoint flagext.URLValue
	ReadTimeout      time.Duration
}

func (cfg *ClientConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.TenantID, "tests.tenant-id", "anonymous", "The tenant ID to use to write and read metrics in tests. (mutually exclusive with basic-auth or bearer-token flags)")
	f.StringVar(&cfg.BasicAuthUser, "tests.basic-auth-user", "", "The username to use for HTTP bearer authentication. (mutually exclusive with tenant-id or bearer-token flags)")
	f.StringVar(&cfg.BasicAuthPassword, "tests.basic-auth-password", "", "The password to use for HTTP bearer authentication. (mutually exclusive with tenant-id or bearer-token flags)")
	f.StringVar(&cfg.BearerToken, "tests.bearer-token", "", "The bearer token to use for HTTP bearer authentication. (mutually exclusive with tenant-id flag or basic-auth flags)")

	f.Var(&cfg.WriteBaseEndpoint, "tests.write-endpoint", "The base endpoint on the write path. The URL should have no trailing slash. The specific API path is appended by the tool to the URL, for example /api/v1/push for the remote write API endpoint, so the configured URL must not include it.")
	f.IntVar(&cfg.WriteBatchSize, "tests.write-batch-size", 1000, "The maximum number of series to write in a single request.")
	f.DurationVar(&cfg.WriteTimeout, "tests.write-timeout", 5*time.Second, "The timeout for a single write request.")
	f.StringVar(&cfg.WriteProtocol, "tests.write-protocol", "prometheus", "The protocol to use to write series data. Supported values are: prometheus, otlp-http")

	f.Var(&cfg.ReadBaseEndpoint, "tests.read-endpoint", "The base endpoint on the read path. The URL should have no trailing slash. The specific API path is appended by the tool to the URL, for example /api/v1/query_range for range query API, so the configured URL must not include it.")
	f.DurationVar(&cfg.ReadTimeout, "tests.read-timeout", 60*time.Second, "The timeout for a single read request.")

}

type Client struct {
	writeClient clientWriter
	readClient  v1.API
	cfg         ClientConfig
	logger      log.Logger
}

type clientWriter interface {
	sendWriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error)
}

func NewClient(cfg ClientConfig, logger log.Logger) (*Client, error) {
	rt := &clientRoundTripper{
		tenantID:          cfg.TenantID,
		basicAuthUser:     cfg.BasicAuthUser,
		basicAuthPassword: cfg.BasicAuthPassword,
		bearerToken:       cfg.BearerToken,
		rt:                instrumentation.TracerTransport{},
	}

	// Ensure the required config has been set.
	if cfg.WriteBaseEndpoint.URL == nil {
		return nil, errors.New("the write endpoint has not been set")
	}
	if cfg.ReadBaseEndpoint.URL == nil {
		return nil, errors.New("the read endpoint has not been set")
	}
	if cfg.WriteProtocol != "prometheus" && cfg.WriteProtocol != "otlp-http" {
		return nil, fmt.Errorf("the only supported write protocols are \"prometheus\" or \"otlp-http\"")
	}
	// Ensure not both tenant-id and basic-auth are used at the same time
	// anonymous is the default value for TenantID.
	if (cfg.TenantID != "anonymous" && cfg.BasicAuthUser != "" && cfg.BasicAuthPassword != "" && cfg.BearerToken != "") || // all authentication at once
		(cfg.TenantID != "anonymous" && cfg.BasicAuthUser != "" && cfg.BasicAuthPassword != "") || // tenant-id and basic auth
		(cfg.TenantID != "anonymous" && cfg.BearerToken != "") || // tenant-id and bearer token
		(cfg.BasicAuthUser != "" && cfg.BasicAuthPassword != "" && cfg.BearerToken != "") { // basic auth and bearer token
		return nil, errors.New("either set tests.tenant-id or tests.basic-auth-user/tests.basic-auth-password or tests.bearer-token")
	}

	apiCfg := api.Config{
		Address:      cfg.ReadBaseEndpoint.String(),
		RoundTripper: rt,
	}

	readClient, err := api.NewClient(apiCfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create read client")
	}

	var writeClient clientWriter

	switch cfg.WriteProtocol {

	case "prometheus":
		writeClient = &prometheusWriter{
			httpClient:        &http.Client{Transport: rt},
			writeBaseEndpoint: cfg.WriteBaseEndpoint,
			writeBatchSize:    cfg.WriteBatchSize,
			writeTimeout:      cfg.WriteTimeout,
		}

	case "otlp-http":
		writeClient = &otlpHTTPWriter{
			httpClient:        &http.Client{Transport: rt},
			writeBaseEndpoint: cfg.WriteBaseEndpoint,
			writeBatchSize:    cfg.WriteBatchSize,
			writeTimeout:      cfg.WriteTimeout,
		}
	}

	return &Client{
		writeClient: writeClient,
		readClient:  v1.NewAPI(readClient),
		cfg:         cfg,
		logger:      logger,
	}, nil
}

// QueryRange implements MimirClient.
func (c *Client) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration, options ...RequestOption) (model.Matrix, error) {
	ctx = contextWithRequestOptions(ctx, options...)
	ctx, cancel := context.WithTimeout(ctx, c.cfg.ReadTimeout)
	defer cancel()

	ctx = querierapi.ContextWithReadConsistency(ctx, querierapi.ReadConsistencyStrong)

	value, _, err := c.readClient.QueryRange(ctx, query, v1.Range{
		Start: start,
		End:   end,
		Step:  step,
	})
	if err != nil {
		return nil, err
	}

	if value.Type() != model.ValMatrix {
		return nil, fmt.Errorf("was expecting to get a Matrix, but got %s", value.Type().String())
	}

	matrix, ok := value.(model.Matrix)
	if !ok {
		return nil, fmt.Errorf("failed to cast type to Matrix, type was %T", value)
	}

	return matrix, nil
}

// Query implements MimirClient.
func (c *Client) Query(ctx context.Context, query string, ts time.Time, options ...RequestOption) (model.Vector, error) {
	ctx = contextWithRequestOptions(ctx, options...)
	ctx, cancel := context.WithTimeout(ctx, c.cfg.ReadTimeout)
	defer cancel()

	ctx = querierapi.ContextWithReadConsistency(ctx, querierapi.ReadConsistencyStrong)

	value, _, err := c.readClient.Query(ctx, query, ts)
	if err != nil {
		return nil, err
	}

	if value.Type() != model.ValVector {
		return nil, fmt.Errorf("was expecting to get a Vector, but got %s", value.Type().String())
	}

	vector, ok := value.(model.Vector)
	if !ok {
		return nil, fmt.Errorf("failed to cast type to Vector, type was %T", value)
	}

	return vector, nil
}

// WriteSeries implements MimirClient.
func (c *Client) WriteSeries(ctx context.Context, series []prompb.TimeSeries) (int, error) {
	lastStatusCode := 0

	// Honor the batch size.
	for len(series) > 0 {
		end := util_math.Min(len(series), c.cfg.WriteBatchSize)
		batch := series[0:end]
		series = series[end:]

		var err error
		lastStatusCode, err = c.writeClient.sendWriteRequest(ctx, &prompb.WriteRequest{Timeseries: batch})
		if err != nil {
			return lastStatusCode, err
		}
	}

	return lastStatusCode, nil
}

// RequestOption defines a functional-style request option.
type RequestOption func(options *requestOptions)

// WithResultsCacheEnabled controls whether the query-frontend results cache should be enabled or disabled for the request.
// This function assumes query-frontend results cache is enabled by default.
func WithResultsCacheEnabled(enabled bool) RequestOption {
	return func(options *requestOptions) {
		options.resultsCacheDisabled = !enabled
	}
}

// contextWithRequestOptions returns a context.Context with the request options applied.
func contextWithRequestOptions(ctx context.Context, options ...RequestOption) context.Context {
	actual := &requestOptions{}
	for _, option := range options {
		option(actual)
	}

	return context.WithValue(ctx, requestOptionsKey, actual)
}

type requestOptions struct {
	resultsCacheDisabled bool
}

type key int

var requestOptionsKey key

type clientRoundTripper struct {
	tenantID          string
	basicAuthUser     string
	basicAuthPassword string
	bearerToken       string
	rt                http.RoundTripper
}

// RoundTrip add the tenant ID header required by Mimir.
func (rt *clientRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	options, _ := req.Context().Value(requestOptionsKey).(*requestOptions)
	if options != nil && options.resultsCacheDisabled {
		// Despite the name, the "no-store" directive also disables results cache lookup in Mimir.
		req.Header.Set("Cache-Control", "no-store")
	}

	if rt.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+rt.bearerToken)
	} else if rt.basicAuthUser != "" && rt.basicAuthPassword != "" {
		req.SetBasicAuth(rt.basicAuthUser, rt.basicAuthPassword)
	} else {
		req.Header.Set("X-Scope-OrgID", rt.tenantID)
	}

	req.Header.Set("User-Agent", "mimir-continuous-test")

	if lvl, ok := querierapi.ReadConsistencyFromContext(req.Context()); ok {
		req.Header.Add(querierapi.ReadConsistencyHeader, lvl)
	}

	return rt.rt.RoundTrip(req)
}
