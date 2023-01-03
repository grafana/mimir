// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/ruler"
	"github.com/grafana/mimir/pkg/util/version"
)

const (
	serviceConfig = `{"loadBalancingPolicy": "round_robin"}`

	queryEndpointPath = "/api/v1/query"

	mimeTypeFormPost = "application/x-www-form-urlencoded"

	statusError = "error"

	maxRequestRetries = 3
)

var userAgent = fmt.Sprintf("mimir/%s", version.Version)

// Middleware provides a mechanism to inspect outgoing remote querier requests.
type Middleware func(ctx context.Context, req *httpgrpc.HTTPRequest) error

func DialQueryFrontend(cfg ruler.QueryFrontendConfig, orgID string) (httpgrpc.HTTPClient, error) {
	opts, err := cfg.GRPCClientConfig.DialOption([]grpc.UnaryClientInterceptor{OrgIDInterceptor(orgID)}, nil)
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

func OrgIDInterceptor(orgID string) func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.New(map[string]string{})
		}

		md = md.Copy()
		md["x-scope-orgid"] = []string{orgID}
		ctx = metadata.NewOutgoingContext(ctx, md)

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

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

func (q *RemoteQuerier) Query(ctx context.Context, query string, ts time.Time, logger log.Logger) (model.ValueType, json.RawMessage, error) {
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

	resp, err := q.sendRequest(ctx, &req)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to remotely evaluate query expression", "err", err, "qs", query, "tm", ts)
		return model.ValNone, nil, err
	}
	if resp.Code/100 != 2 {
		return model.ValNone, nil, httpgrpc.Errorf(int(resp.Code), "unexpected response status code %d: %s", resp.Code, string(resp.Body))
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

func (q *RemoteQuerier) sendRequest(ctx context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	// Ongoing request may be cancelled during evaluation due to some transient error or server shutdown,
	// so we'll keep retrying until we get a successful response or backoff is terminated.
	retryConfig := backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: maxRequestRetries,
	}
	retry := backoff.New(ctx, retryConfig)

	for {
		resp, err := q.client.Handle(ctx, req)
		if err == nil {
			return resp, nil
		}
		if !retry.Ongoing() {
			return nil, err
		}
		level.Warn(q.logger).Log("msg", "failed to remotely evaluate query expression, will retry", "err", err)
		retry.Wait()

		// Avoid masking last known error if context was cancelled while waiting.
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%s while retrying request, last err was: %w", ctx.Err(), err)
		}
	}
}
