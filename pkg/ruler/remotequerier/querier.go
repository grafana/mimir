// SPDX-License-Identifier: AGPL-3.0-only

package remotequerier

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
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/version"
)

const (
	readEndpointPath  = "/api/v1/read"
	queryEndpointPath = "/api/v1/query"

	mimeTypeFormPost = "application/x-www-form-urlencoded"

	statusError = "error"
)

var userAgent = fmt.Sprintf("mimir/%s", version.Version)

// Querier executes read operations against a transport.RoundTripper.
type Querier struct {
	transport      httpgrpcutil.RoundTripper
	promHTTPPrefix string
	logger         log.Logger
}

// New creates and initializes a new Querier instance.
func New(
	transport httpgrpcutil.RoundTripper,
	prometheusHTTPPrefix string,
	logger log.Logger,
) *Querier {
	return &Querier{
		transport:      transport,
		promHTTPPrefix: prometheusHTTPPrefix,
		logger:         logger,
	}
}

// Read satisfies Prometheus remote.ReadClient.
// See: https://github.com/prometheus/prometheus/blob/1291ec71851a7383de30b089f456fdb6202d037a/storage/remote/client.go#L264
func (r *Querier) Read(ctx context.Context, query *prompb.Query) (*prompb.QueryResult, error) {
	log, ctx := spanlogger.NewWithLogger(ctx, r.logger, "remotequerier.Querier.Read")
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
	// add required HTTP headers
	headers := []*httpgrpc.Header{
		{Key: textproto.CanonicalMIMEHeaderKey("Content-Encoding"), Values: []string{"snappy"}},
		{Key: textproto.CanonicalMIMEHeaderKey("Accept-Encoding"), Values: []string{"snappy"}},
		{Key: textproto.CanonicalMIMEHeaderKey("Content-Type"), Values: []string{"application/x-protobuf"}},
		{Key: textproto.CanonicalMIMEHeaderKey("User-Agent"), Values: []string{userAgent}},
		{Key: textproto.CanonicalMIMEHeaderKey("X-Prometheus-Remote-Read-Version"), Values: []string{"0.1.0"}},
	}

	req := httpgrpc.HTTPRequest{
		Method:  http.MethodPost,
		Url:     r.promHTTPPrefix + readEndpointPath,
		Body:    snappy.Encode(nil, data),
		Headers: headers,
	}

	resp, err := r.transport.RoundTrip(ctx, &req)
	if err != nil {
		level.Warn(log).Log("msg", "failed to perform remote read", "err", err, "qs", query)
		return nil, err
	}
	level.Debug(log).Log("msg", "remote read successfully performed", "qs", query)

	if resp != nil && resp.Code/100 != 2 {
		return nil, fmt.Errorf("unexpected response status code %d: %s", resp.Code, string(resp.Body))
	}

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
func (r *Querier) Query(ctx context.Context, query string, ts time.Time) (model.ValueType, json.RawMessage, error) {
	log, ctx := spanlogger.NewWithLogger(ctx, r.logger, "remotequerier.Querier.Query")
	defer log.Span.Finish()

	args := make(url.Values)
	args.Set("query", query)
	if !ts.IsZero() {
		args.Set("time", formatQueryTime(ts))
	}
	body := []byte(args.Encode())

	req := httpgrpc.HTTPRequest{
		Method: http.MethodPost,
		Url:    r.promHTTPPrefix + queryEndpointPath,
		Body:   body,
		Headers: []*httpgrpc.Header{
			{Key: textproto.CanonicalMIMEHeaderKey("User-Agent"), Values: []string{userAgent}},
			{Key: textproto.CanonicalMIMEHeaderKey("Content-Type"), Values: []string{mimeTypeFormPost}},
			{Key: textproto.CanonicalMIMEHeaderKey("Content-Length"), Values: []string{strconv.Itoa(len(body))}},
		},
	}

	resp, err := r.transport.RoundTrip(ctx, &req)
	if err != nil {
		level.Warn(log).Log("msg", "failed to remotely evaluate query expression", "err", err, "qs", query, "tm", ts)
		return model.ValNone, nil, err
	}
	level.Debug(log).Log("msg", "query expression successfully evaluated", "qs", query, "tm", ts)

	if resp != nil && resp.Code/100 != 2 {
		return model.ValNone, nil, fmt.Errorf("unexpected response status code %d: %s", resp.Code, string(resp.Body))
	}

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

func formatQueryTime(t time.Time) string {
	return strconv.FormatFloat(float64(t.Unix())+float64(t.Nanosecond())/1e9, 'f', -1, 64)
}
