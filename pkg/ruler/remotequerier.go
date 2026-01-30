// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util/grpcencoding/s2"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	"github.com/grafana/mimir/pkg/util/propagation"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/version"
)

const (
	serviceConfig = `{"loadBalancingPolicy": "round_robin"}`

	readEndpointPath  = "/api/v1/read"
	queryEndpointPath = "/api/v1/query"

	mimeTypeFormPost = "application/x-www-form-urlencoded"

	statusError = "error"

	maxRequestRetries = 3

	formatJSON     = "json"
	formatProtobuf = "protobuf"
)

var allFormats = []string{formatJSON, formatProtobuf}

// QueryFrontendConfig defines query-frontend transport configuration.
type QueryFrontendConfig struct {
	// Address is the address of the query-frontend to connect to.
	Address string `yaml:"address"`

	// GRPCClientConfig contains gRPC specific config options.
	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config" doc:"description=Configures the gRPC client used to communicate between the rulers and query-frontends."`

	// HTTPClientConfig contains HTTP specific config options.
	HTTPClientConfig HTTPConfig `yaml:"http_client_config" doc:"description=Configures the HTTP client used to communicate between the rulers and query-frontends."`

	QueryResultResponseFormat string `yaml:"query_result_response_format"`

	MaxRetriesRate float64 `yaml:"max_retries_rate"`
}

func (c *QueryFrontendConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(
		&c.Address,
		"ruler.query-frontend.address",
		"",
		"Can be either the GRPC listen address of the query-frontend(s) or the HTTP/HTTPS address of a Prometheus-compatible server. Must be a DNS address (prefixed with dns:///) "+
			"to enable GRPC client side load balancing.",
	)
	c.GRPCClientConfig.CustomCompressors = []string{s2.Name}
	c.GRPCClientConfig.RegisterFlagsWithPrefix("ruler.query-frontend.grpc-client-config", f)

	c.HTTPClientConfig.RegisterFlagsWithPrefix("ruler.query-frontend.http-client-config", f)

	f.StringVar(&c.QueryResultResponseFormat, "ruler.query-frontend.query-result-response-format", formatProtobuf, fmt.Sprintf("Format to use when retrieving query results from query-frontends. Supported values: %s", strings.Join(allFormats, ", ")))
	f.Float64Var(&c.MaxRetriesRate, "ruler.query-frontend.max-retries-rate", 170, "Maximum number of retries for failed queries per second.")
}

func (c *QueryFrontendConfig) Validate() error {
	if !slices.Contains(allFormats, c.QueryResultResponseFormat) {
		return fmt.Errorf("unknown query result response format '%s'. Supported values: %s", c.QueryResultResponseFormat, strings.Join(allFormats, ", "))
	}

	// Make sure the DNS prefix is correct (three slashes) if it is being used.
	// This is a GRPC specific requirement/format when using service discovery.
	if strings.HasPrefix(c.Address, "dns://") && !strings.HasPrefix(c.Address, "dns:///") {
		return fmt.Errorf(`address must have "dns:///" prefix when using GRPC service discovery, got: %q`, c.Address)
	}

	return nil
}

// DialQueryFrontend creates and initializes a new httpgrpc.HTTPClient taking a QueryFrontendConfig configuration.
func DialQueryFrontend(cfg QueryFrontendConfig, prometheusHTTPPrefix string, reg prometheus.Registerer, logger log.Logger) (http.RoundTripper, *url.URL, error) {
	if strings.HasPrefix(cfg.Address, "http://") || strings.HasPrefix(cfg.Address, "https://") {
		return dialQueryFrontendHTTP(cfg, reg, logger)
	}

	return dialQueryFrontendGRPC(cfg, prometheusHTTPPrefix, reg, logger)
}

// Middleware provides a mechanism to inspect outgoing remote querier requests.
type Middleware func(ctx context.Context, req *http.Request) error

// RemoteQuerier executes read operations against a httpgrpc.HTTPClient.
type RemoteQuerier struct {
	client                             http.RoundTripper
	retryLimiter                       *rate.Limiter
	timeout                            time.Duration
	middlewares                        []Middleware
	promHTTPURL                        *url.URL
	logger                             log.Logger
	preferredQueryResultResponseFormat string
	decoders                           map[string]decoder
}

var jsonDecoderInstance = jsonDecoder{}
var protobufDecoderInstance = protobufDecoder{}

// NewRemoteQuerier creates and initializes a new RemoteQuerier instance.
func NewRemoteQuerier(
	client http.RoundTripper,
	timeout time.Duration,
	maxRetryRate float64, // maxRetryRate is the maximum number of retries for failed queries per second.
	preferredQueryResultResponseFormat string,
	prometheusHTTPURL *url.URL,
	logger log.Logger,
	middlewares ...Middleware,
) *RemoteQuerier {
	return &RemoteQuerier{
		client:                             client,
		timeout:                            timeout,
		retryLimiter:                       rate.NewLimiter(rate.Limit(maxRetryRate), 1),
		middlewares:                        middlewares,
		promHTTPURL:                        prometheusHTTPURL,
		logger:                             logger,
		preferredQueryResultResponseFormat: preferredQueryResultResponseFormat,
		decoders: map[string]decoder{
			jsonDecoderInstance.ContentType():     jsonDecoderInstance,
			protobufDecoderInstance.ContentType(): protobufDecoderInstance,
		},
	}
}

// Read satisfies Prometheus remote.ReadClient.
// See: https://github.com/prometheus/prometheus/blob/28a830ed9f331e71549c24c2ac3b441033201e8f/storage/remote/client.go#L342
func (q *RemoteQuerier) Read(ctx context.Context, query *prompb.Query, sortSeries bool) (storage.SeriesSet, error) {
	log, ctx := spanlogger.New(ctx, q.logger, tracer, "ruler.RemoteQuerier.Read")
	defer log.Finish()

	rdReq := &prompb.ReadRequest{
		Queries: []*prompb.Query{
			query,
		},
	}
	data, err := proto.Marshal(rdReq)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to marshal read request")
	}

	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, q.promHTTPURL.JoinPath(readEndpointPath).String(), bytes.NewBuffer(snappy.Encode(nil, data)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("Accept-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("User-Agent", version.UserAgent())
	req.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")
	consistencyInjector := &api.ConsistencyInjector{}
	if err := consistencyInjector.InjectToCarrier(ctx, propagation.HttpHeaderCarrier(req.Header)); err != nil {
		return nil, err
	}

	for _, mdw := range q.middlewares {
		if err := mdw(ctx, req); err != nil {
			return nil, err
		}
	}

	resp, err := q.client.RoundTrip(req)
	if err != nil {
		if code := grpcutil.ErrorToStatusCode(err); code/100 != 4 {
			level.Warn(log).Log("msg", "failed to perform remote read", "err", err, "qs", query)
		}
		return nil, err
	}

	defer resp.Body.Close()
	body, err := httpgrpcutil.ReadAll(resp.Body)
	if err != nil {
		return nil, httpgrpc.Errorf(resp.StatusCode, "error reading response body for status code %d: %s", resp.StatusCode, err)
	}

	if resp.StatusCode/100 != 2 {
		return nil, httpgrpc.Errorf(resp.StatusCode, "unexpected response status code %d: %s", resp.StatusCode, string(body))
	}
	level.Debug(log).Log("msg", "remote read successfully performed", "qs", query)

	contentType := resp.Header.Get("Content-Type")
	if len(contentType) > 0 && contentType != "application/x-protobuf" {
		return nil, errors.Errorf("unexpected response content type %s expected application/x-protobuf", contentType)
	}

	uncompressed, err := snappy.Decode(nil, body)
	if err != nil {
		return nil, errors.Wrap(err, "error reading response")
	}
	var rdResp prompb.ReadResponse

	err = proto.Unmarshal(uncompressed, &rdResp)
	if err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal response body")
	}

	if len(rdResp.Results) != 1 {
		return nil, errors.Errorf("responses: want %d, got %d", 1, len(rdResp.Results))
	}

	res := rdResp.Results[0]
	return remote.FromQueryResult(sortSeries, res), nil
}

// ReadMultiple implements Prometheus remote.ReadClient.
// As of writing this the method is not used by the ruler and the prometheus remote queryable implementation.
func (q *RemoteQuerier) ReadMultiple(context.Context, []*prompb.Query, bool) (storage.SeriesSet, error) {
	return nil, errors.New("ReadMultiple is not supported by ruler.RemoteQuerier; open a bug report if you see this error")
}

// Query performs a query for the given time.
func (q *RemoteQuerier) Query(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
	logger, ctx := spanlogger.New(ctx, q.logger, tracer, "ruler.RemoteQuerier.Query")
	defer logger.Finish()

	return q.query(ctx, qs, t, logger)
}

func (q *RemoteQuerier) query(ctx context.Context, query string, ts time.Time, logger log.Logger) (promql.Vector, error) {
	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()

	resp, err := q.sendRequest(ctx, query, ts, logger)
	if err != nil {
		if code := grpcutil.ErrorToStatusCode(err); code/100 != 4 {
			level.Warn(logger).Log("msg", "failed to remotely evaluate query expression", "err", err, "qs", query, "tm", ts)
		}
		return promql.Vector{}, err
	}
	defer resp.Body.Close()
	body, err := httpgrpcutil.ReadAll(resp.Body)
	if err != nil {
		return promql.Vector{}, httpgrpc.Errorf(resp.StatusCode, "error reading response body for status code %d: %s", resp.StatusCode, err)
	}
	if resp.StatusCode/100 != 2 {
		return promql.Vector{}, httpgrpc.Errorf(resp.StatusCode, "unexpected response status code %d: %s", resp.StatusCode, string(body))
	}
	level.Debug(logger).Log("msg", "query expression successfully evaluated", "qs", query, "tm", ts)

	contentTypeHeader := resp.Header.Get("Content-Type")
	decoder, ok := q.decoders[contentTypeHeader]
	if !ok {
		return promql.Vector{}, fmt.Errorf("unknown response content type '%s'", contentTypeHeader)
	}

	return decoder.Decode(body)
}

func (q *RemoteQuerier) createRequest(ctx context.Context, query string, ts time.Time) (*http.Request, error) {
	args := make(url.Values)
	args.Set("query", query)
	if !ts.IsZero() {
		args.Set("time", ts.Format(time.RFC3339Nano))
	}
	body := []byte(args.Encode())
	acceptHeader := ""

	switch q.preferredQueryResultResponseFormat {
	case formatJSON:
		acceptHeader = jsonDecoderInstance.ContentType()
	case formatProtobuf:
		acceptHeader = protobufDecoderInstance.ContentType() + "," + jsonDecoderInstance.ContentType()
	default:
		return nil, fmt.Errorf("unknown response format '%s'", q.preferredQueryResultResponseFormat)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, q.promHTTPURL.JoinPath(queryEndpointPath).String(), bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", version.UserAgent())
	req.Header.Set("Content-Type", mimeTypeFormPost)
	req.Header.Set("Content-Length", strconv.Itoa(len(body)))
	req.Header.Set("Accept", acceptHeader)
	consistencyInjector := &api.ConsistencyInjector{}
	if err := consistencyInjector.InjectToCarrier(ctx, propagation.HttpHeaderCarrier(req.Header)); err != nil {
		return nil, err
	}

	for _, mdw := range q.middlewares {
		if err := mdw(ctx, req); err != nil {
			return nil, err
		}
	}

	return req, nil
}

func (q *RemoteQuerier) sendRequest(ctx context.Context, query string, ts time.Time, logger log.Logger) (*http.Response, error) {
	// Ongoing request may be cancelled during evaluation due to some transient error or server shutdown,
	// so we'll keep retrying until we get a successful response or backoff is terminated.
	retryConfig := backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: maxRequestRetries,
	}
	retry := backoff.New(ctx, retryConfig)

	for {
		req, err := q.createRequest(ctx, query, ts)
		if err != nil {
			return nil, err
		}

		resp, err := q.client.RoundTrip(req)
		if err == nil {
			// Responses with status codes 4xx should always be considered erroneous.
			// These errors shouldn't be retried because it is expected that
			// running the same query gives rise to the same 4xx error.
			if resp.StatusCode/100 == 4 {
				defer resp.Body.Close()

				body, err := httpgrpcutil.ReadAll(resp.Body)
				if err != nil {
					return nil, httpgrpc.Errorf(resp.StatusCode, "error reading response body for status code %d: %s", resp.StatusCode, err)
				}

				return nil, httpgrpc.Error(resp.StatusCode, string(body))
			}
			return resp, nil
		}

		// Bail out if the error is known to be not retriable.
		switch code := grpcutil.ErrorToStatusCode(err); code {
		case codes.ResourceExhausted:
			// In case the server is configured with "grpc-max-send-msg-size-bytes",
			// and the response exceeds this limit, there is no point retrying the request.
			// This is a special case, refer to grafana/mimir#7216.
			if strings.Contains(err.Error(), "message larger than max") {
				return nil, err
			}
		default:
			// In case the error was a wrapped HTTPResponse, its code represents HTTP status;
			// 4xx errors shouldn't be retried because it is expected that
			// running the same query gives rise to the same 4xx error.
			if code/100 == 4 {
				return nil, err
			}
		}

		if !retry.Ongoing() {
			return nil, err
		}

		retryReservation := q.retryLimiter.Reserve()
		if !retryReservation.OK() {
			// This should only happen if we've misconfigured the limiter.
			return nil, fmt.Errorf("couldn't reserve a retry token")
		}
		// We want to wait at least the time for the backoff, but also don't want to exceed the rate limit.
		// All of this is capped to 1m, so that we are less likely to overrun into the next evaluation.
		// 1m was selected as giving enough time to spread out the retries.
		retryDelay := max(retry.NextDelay(), min(time.Minute, retryReservation.Delay()))
		level.Warn(logger).Log("msg", "failed to remotely evaluate query expression, will retry", "err", err, "retry_delay", retryDelay)
		select {
		case <-time.After(retryDelay):
		case <-ctx.Done():
			retryReservation.Cancel()
			// Avoid masking last known error if context was cancelled while waiting.
			return nil, fmt.Errorf("%s while retrying request, last error was: %w", ctx.Err(), err)
		}
	}
}

// WithOrgIDMiddleware attaches 'X-Scope-OrgID' header value to the outgoing request by inspecting the passed context.
// In case the expression to evaluate corresponds to a federated rule, the ExtractTenantIDs function will take care
// of normalizing and concatenating source tenants by separating them with a '|' character.
func WithOrgIDMiddleware(ctx context.Context, req *http.Request) error {
	orgID, err := ExtractTenantIDs(ctx)
	if err != nil {
		return err
	}
	req.Header.Set(user.OrgIDHeaderName, orgID)
	return nil
}
