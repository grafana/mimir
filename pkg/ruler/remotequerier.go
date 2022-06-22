// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/grpcclient"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	prommodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/version"
)

const (
	serviceConfig = `{"loadBalancingPolicy": "round_robin"}`

	readEndpointPath  = "/api/v1/read"
	queryEndpointPath = "/api/v1/query"

	mimeTypeFormPost = "application/x-www-form-urlencoded"

	statusError = "error"
)

var userAgent = fmt.Sprintf("mimir/%s", version.Version)

// QueryFrontendConfig defines query-frontend transport configuration.
type QueryFrontendConfig struct {
	// Address is the address of the query-frontend to connect to.
	Address string `yaml:"address"`

	// Timeout is the length of time we wait on the query-frontend before giving up.
	Timeout time.Duration `yaml:"timeout"`

	// GRPCClientConfig contains gRPC specific config options.
	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config"`
}

func (c *QueryFrontendConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&c.Address,
		"ruler.query-frontend.address",
		"",
		"GRPC listen address of the query-frontend(s). Must be a DNS address (prefixed with dns:///) "+
			"to enable client side load balancing.")

	f.DurationVar(&c.Timeout, "ruler.query-frontend.timeout", 2*time.Minute, "The timeout for a rule query being evaluated by the query-frontend.")

	c.GRPCClientConfig.RegisterFlagsWithPrefix("ruler.query-frontend.grpc-client-config", f)
}

// DialQueryFrontend creates and initializes a new httpgrpc.HTTPClient taking a QueryFrontendConfig configuration.
func DialQueryFrontend(cfg QueryFrontendConfig) (httpgrpc.HTTPClient, error) {
	opts, err := cfg.GRPCClientConfig.DialOption([]grpc.UnaryClientInterceptor{
		otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
		middleware.ClientUserHeaderInterceptor,
	}, nil)
	if err != nil {
		return nil, err
	}
	opts = append(opts, grpc.WithDefaultServiceConfig(serviceConfig))

	conn, err := grpc.Dial(cfg.Address, opts...)
	if err != nil {
		return nil, err
	}
	return httpgrpc.NewHTTPClient(conn), nil
}

// Middleware provides a mechanism to inspect outgoing remote querier requests.
type Middleware func(ctx context.Context, req *httpgrpc.HTTPRequest) error

// RemoteQuerier executes read operations against a httpgrpc.HTTPClient.
type RemoteQuerier struct {
	client         httpgrpc.HTTPClient
	timeout        time.Duration
	middlewares    []Middleware
	promHTTPPrefix string
	logger         log.Logger
}

// NewRemoteQuerier creates and initializes a new RemoteQuerier instance.
func NewRemoteQuerier(
	client httpgrpc.HTTPClient,
	timeout time.Duration,
	prometheusHTTPPrefix string,
	logger log.Logger,
	middlewares ...Middleware,
) *RemoteQuerier {
	return &RemoteQuerier{
		client:         client,
		timeout:        timeout,
		middlewares:    middlewares,
		promHTTPPrefix: prometheusHTTPPrefix,
		logger:         logger,
	}
}

// Read satisfies Prometheus remote.ReadClient.
// See: https://github.com/prometheus/prometheus/blob/1291ec71851a7383de30b089f456fdb6202d037a/storage/remote/client.go#L264
func (q *RemoteQuerier) Read(ctx context.Context, query *prompb.Query) (*prompb.QueryResult, error) {
	log, ctx := spanlogger.NewWithLogger(ctx, q.logger, "ruler.RemoteQuerier.Read")
	defer log.Span.Finish()

	rdReq := &prompb.ReadRequest{
		Queries: []*prompb.Query{
			query,
		},
	}
	data, err := proto.Marshal(rdReq)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to marshal read request")
	}

	req := httpgrpc.HTTPRequest{
		Method: http.MethodPost,
		Url:    q.promHTTPPrefix + readEndpointPath,
		Body:   snappy.Encode(nil, data),
		Headers: []*httpgrpc.Header{
			{Key: textproto.CanonicalMIMEHeaderKey("Content-Encoding"), Values: []string{"snappy"}},
			{Key: textproto.CanonicalMIMEHeaderKey("Accept-Encoding"), Values: []string{"snappy"}},
			{Key: textproto.CanonicalMIMEHeaderKey("Content-Type"), Values: []string{"application/x-protobuf"}},
			{Key: textproto.CanonicalMIMEHeaderKey("User-Agent"), Values: []string{userAgent}},
			{Key: textproto.CanonicalMIMEHeaderKey("X-Prometheus-Remote-Read-Version"), Values: []string{"0.1.0"}},
		},
	}

	for _, mdw := range q.middlewares {
		if err := mdw(ctx, &req); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()

	resp, err := q.client.Handle(ctx, &req)
	if err != nil {
		level.Warn(log).Log("msg", "failed to perform remote read", "err", err, "qs", query)
		return nil, err
	}
	if resp.Code/100 != 2 {
		return nil, fmt.Errorf("unexpected response status code %d: %s", resp.Code, string(resp.Body))
	}
	level.Debug(log).Log("msg", "remote read successfully performed", "qs", query)

	uncompressed, err := snappy.Decode(nil, resp.Body)
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
	return rdResp.Results[0], nil
}

// Query performs a query for the given time.
func (q *RemoteQuerier) Query(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
	logger, ctx := spanlogger.NewWithLogger(ctx, q.logger, "ruler.RemoteQuerier.Query")
	defer logger.Span.Finish()

	valTyp, res, err := q.query(ctx, qs, t, logger)
	if err != nil {
		return nil, err
	}
	return decodeQueryResponse(valTyp, res)
}

func (q *RemoteQuerier) query(ctx context.Context, query string, ts time.Time, logger log.Logger) (model.ValueType, json.RawMessage, error) {
	args := make(url.Values)
	args.Set("query", query)
	if !ts.IsZero() {
		args.Set("time", ts.Format(time.RFC3339Nano))
	}
	body := []byte(args.Encode())

	req := httpgrpc.HTTPRequest{
		Method: http.MethodPost,
		Url:    q.promHTTPPrefix + queryEndpointPath,
		Body:   body,
		Headers: []*httpgrpc.Header{
			{Key: textproto.CanonicalMIMEHeaderKey("User-Agent"), Values: []string{userAgent}},
			{Key: textproto.CanonicalMIMEHeaderKey("Content-Type"), Values: []string{mimeTypeFormPost}},
			{Key: textproto.CanonicalMIMEHeaderKey("Content-Length"), Values: []string{strconv.Itoa(len(body))}},
		},
	}

	for _, mdw := range q.middlewares {
		if err := mdw(ctx, &req); err != nil {
			return model.ValNone, nil, err
		}
	}

	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()

	resp, err := q.client.Handle(ctx, &req)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to remotely evaluate query expression", "err", err, "qs", query, "tm", ts)
		return model.ValNone, nil, err
	}
	if resp.Code/100 != 2 {
		return model.ValNone, nil, fmt.Errorf("unexpected response status code %d: %s", resp.Code, string(resp.Body))
	}
	level.Debug(logger).Log("msg", "query expression successfully evaluated", "qs", query, "tm", ts)

	var apiResp struct {
		Status    string          `json:"status"`
		Data      json.RawMessage `json:"data"`
		ErrorType string          `json:"errorType"`
		Error     string          `json:"error"`
	}
	if err := json.NewDecoder(bytes.NewReader(resp.Body)).Decode(&apiResp); err != nil {
		return model.ValNone, nil, err
	}
	if apiResp.Status == statusError {
		return model.ValNone, nil, fmt.Errorf("query response error: %s", apiResp.Error)
	}
	v := struct {
		Type   model.ValueType `json:"resultType"`
		Result json.RawMessage `json:"result"`
	}{}

	if err := json.Unmarshal(apiResp.Data, &v); err != nil {
		return model.ValNone, nil, err
	}
	return v.Type, v.Result, nil
}

func decodeQueryResponse(valTyp model.ValueType, result json.RawMessage) (promql.Vector, error) {
	switch valTyp {
	case model.ValScalar:
		var sv model.Scalar
		if err := json.Unmarshal(result, &sv); err != nil {
			return nil, err
		}
		return scalarToPromQLVector(&sv), nil

	case model.ValVector:
		var vv model.Vector
		if err := json.Unmarshal(result, &vv); err != nil {
			return nil, err
		}
		return vectorToPromQLVector(vv), nil

	default:
		return nil, fmt.Errorf("rule result is not a vector or scalar: %q", valTyp)
	}
}

func vectorToPromQLVector(vec prommodel.Vector) promql.Vector {
	retVal := make(promql.Vector, 0, len(vec))
	for _, p := range vec {

		lbl := make(labels.Labels, 0, len(p.Metric))
		for ln, lv := range p.Metric {
			lbl = append(lbl, labels.Label{
				Name:  string(ln),
				Value: string(lv),
			})
		}

		retVal = append(retVal, promql.Sample{
			Metric: lbl,
			Point: promql.Point{
				V: float64(p.Value),
				T: int64(p.Timestamp),
			},
		})
	}
	return retVal
}

func scalarToPromQLVector(sc *prommodel.Scalar) promql.Vector {
	return promql.Vector{promql.Sample{
		Point: promql.Point{
			V: float64(sc.Value),
			T: int64(sc.Timestamp),
		},
		Metric: labels.Labels{},
	}}
}

// WithOrgIDMiddleware attaches 'X-Scope-OrgID' header value to the outgoing request by inspecting the passed context.
// In case the expression to evaluate corresponds to a federated rule, the ExtractTenantIDs function will take care
// of normalizing and concatenating source tenants by separating them with a '|' character.
func WithOrgIDMiddleware(ctx context.Context, req *httpgrpc.HTTPRequest) error {
	orgID, err := ExtractTenantIDs(ctx)
	if err != nil {
		return err
	}
	req.Headers = append(req.Headers, &httpgrpc.Header{
		Key:    textproto.CanonicalMIMEHeaderKey(user.OrgIDHeaderName),
		Values: []string{orgID},
	})
	return nil
}
