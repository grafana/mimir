// SPDX-License-Identifier: AGPL-3.0-only

package ruler

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
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	prommodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	rulertransport "github.com/grafana/mimir/pkg/ruler/transport"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	queryEndpointPath = "/api/v1/query"

	mimeTypeFormPost = "application/x-www-form-urlencoded"

	statusError = "error"
)

// RemoteQuerier executes an instant query against a transport.RoundTripper.
type RemoteQuerier struct {
	roundTripper   rulertransport.RoundTripper
	promHTTPPrefix string
	logger         log.Logger
}

// NewRemoteQuerier creates and initializes a RemoteQuerier instance.
func NewRemoteQuerier(
	roundTripper rulertransport.RoundTripper,
	prometheusHTTPPrefix string,
	logger log.Logger,
) *RemoteQuerier {
	return &RemoteQuerier{
		roundTripper:   roundTripper,
		promHTTPPrefix: prometheusHTTPPrefix,
		logger:         logger,
	}
}

// Query implements a rules.QueryFunc for rules.Manager.
func (r *RemoteQuerier) Query(ctx context.Context, query string, ts time.Time) (promql.Vector, error) {
	log, ctx := spanlogger.NewWithLogger(ctx, r.logger, "RemoteQuerier.Query")
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
			{
				Key:    textproto.CanonicalMIMEHeaderKey("Content-Type"),
				Values: []string{mimeTypeFormPost},
			},
			{
				Key:    textproto.CanonicalMIMEHeaderKey("Content-Length"),
				Values: []string{strconv.Itoa(len(body))},
			},
		},
	}

	resp, err := r.roundTripper.RoundTrip(ctx, &req)
	if err != nil {
		level.Warn(log).Log("msg", "failed to remotely evaluate rule expression", "err", err, "qs", query, "tm", ts)
		return nil, err
	}
	level.Debug(log).Log("msg", "rule expression successfully evaluated", "qs", query, "tm", ts)

	if resp != nil && resp.Code/100 != 2 {
		return nil, fmt.Errorf("unexpected response status code %d: %s", resp.Code, string(resp.Body))
	}

	var apiResp struct {
		Status    string          `json:"status"`
		Data      json.RawMessage `json:"data"`
		ErrorType string          `json:"errorType"`
		Error     string          `json:"error"`
	}
	if err := json.NewDecoder(bytes.NewReader(resp.Body)).Decode(&apiResp); err != nil {
		return nil, err
	}
	if apiResp.Status == statusError {
		return nil, fmt.Errorf("query response error: %s", apiResp.Error)
	}
	return decodeQueryResponse(apiResp.Data)
}

func decodeQueryResponse(b []byte) (promql.Vector, error) {
	v := struct {
		Type   model.ValueType `json:"resultType"`
		Result json.RawMessage `json:"result"`
	}{}

	err := json.Unmarshal(b, &v)
	if err != nil {
		return nil, err
	}

	switch v.Type {
	case model.ValScalar:
		var sv model.Scalar
		if err = json.Unmarshal(v.Result, &sv); err != nil {
			return nil, err
		}
		return scalarToPromQLVector(&sv), nil

	case model.ValVector:
		var vv model.Vector
		if err = json.Unmarshal(v.Result, &vv); err != nil {
			return nil, err
		}
		return vectorToPromQLVector(vv), nil

	default:
		return nil, fmt.Errorf("rule result is not a vector or scalar: %q", v.Type)
	}
}

func vectorToPromQLVector(vec prommodel.Vector) promql.Vector {
	var retVal promql.Vector
	for _, p := range vec {
		var sm promql.Sample

		sm.V = float64(p.Value)
		sm.T = int64(p.Timestamp)

		var lbl labels.Labels
		for ln, lv := range p.Metric {
			lbl = append(lbl, labels.Label{Name: string(ln), Value: string(lv)})
		}
		sm.Metric = lbl

		retVal = append(retVal, sm)
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

func formatQueryTime(t time.Time) string {
	return strconv.FormatFloat(float64(t.Unix())+float64(t.Nanosecond())/1e9, 'f', -1, 64)
}

type orgRoundTripper struct {
	next rulertransport.RoundTripper
}

// NewOrgRoundTripper returns a new transport.RoundTripper implementation that injects
// orgID HTTP header by inspecting the passed context.
func NewOrgRoundTripper(next rulertransport.RoundTripper) rulertransport.RoundTripper {
	return &orgRoundTripper{next: next}
}

// RoundTrip satisfies transport.RoundTripper interface.
func (r *orgRoundTripper) RoundTrip(ctx context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	var orgID string

	if sourceTenants, _ := ctx.Value(federatedGroupSourceTenants).([]string); len(sourceTenants) > 0 {
		orgID = tenant.JoinTenantIDs(sourceTenants)
	} else {
		var err error
		orgID, err = user.ExtractOrgID(ctx)
		if err != nil {
			return nil, err
		}
	}
	req.Headers = append(req.Headers, &httpgrpc.Header{
		Key:    textproto.CanonicalMIMEHeaderKey(user.OrgIDHeaderName),
		Values: []string{orgID},
	})
	return r.next.RoundTrip(ctx, req)
}
