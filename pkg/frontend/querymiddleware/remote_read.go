// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage/remote"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/util"
)

// To keep logs and error messages in sync, we define the following keys:
const (
	endLogKey      = "end"
	hintsLogKey    = "hints"
	matchersLogKey = "matchers"
	startLogKey    = "start"
)

type remoteReadRoundTripper struct {
	next http.RoundTripper

	middleware MetricsQueryMiddleware
}

func newRemoteReadRoundTripper(next http.RoundTripper, middlewares ...MetricsQueryMiddleware) http.RoundTripper {
	return &remoteReadRoundTripper{
		next:       next,
		middleware: MergeMetricsQueryMiddlewares(middlewares...),
	}
}

func (r *remoteReadRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	remoteReadRequest, err := getRemoteReadRequestWithoutConsumingBody(req)
	if err != nil {
		return nil, err
	}

	if remoteReadRequest == nil {
		return r.next.RoundTrip(req)
	}

	queries := remoteReadRequest.GetQueries()
	for i, query := range queries {
		metricsRequest, err := remoteReadToMetricsQueryRequest(req.URL.Path, query)
		if err != nil {
			return nil, err
		}
		handler := r.middleware.Wrap(HandlerFunc(func(ctx context.Context, req MetricsQueryRequest) (Response, error) {
			// We do not need to do anything here as this middleware is used for
			// validation only and previous middlewares would have already returned errors.
			return nil, nil
		}))
		_, err = handler.Do(req.Context(), metricsRequest)
		if err != nil {
			return nil, apierror.AddDetails(err, fmt.Sprintf("remote read error (%s_%d: %s)", matchersLogKey, i, metricsRequest.GetQuery()))
		}
	}

	return r.next.RoundTrip(req)
}

// ParseRemoteReadRequestWithoutConsumingBody parses a remote read request
// without consuming the body. It does not check the req.Body size, so it is
// the caller's responsibility to ensure that the body is not too large.
func ParseRemoteReadRequestWithoutConsumingBody(req *http.Request) (url.Values, error) {
	remoteReadRequest, err := getRemoteReadRequestWithoutConsumingBody(req)
	if err != nil {
		return nil, err
	}
	return parseRemoteReadRequest(remoteReadRequest)
}

func getRemoteReadRequestWithoutConsumingBody(req *http.Request) (*prompb.ReadRequest, error) {
	if req.Body == nil {
		return nil, nil
	}

	bodyBytes, err := util.ReadRequestBodyWithoutConsuming(req)
	if err != nil {
		return nil, err
	}

	remoteReadRequest := &prompb.ReadRequest{}

	_, err = util.ParseProtoReader(req.Context(), io.NopCloser(bytes.NewReader(bodyBytes)), int(req.ContentLength), querier.MaxRemoteReadQuerySize, nil, remoteReadRequest, util.RawSnappy)
	if err != nil {
		return nil, err
	}

	return remoteReadRequest, nil
}

func parseRemoteReadRequest(remoteReadRequest *prompb.ReadRequest) (url.Values, error) {
	if remoteReadRequest == nil {
		return nil, nil
	}

	params := make(url.Values)
	add := func(i int, name, value string) { params.Add(name+"_"+strconv.Itoa(i), value) }

	queries := remoteReadRequest.GetQueries()

	for i, query := range queries {
		add(i, startLogKey, fmt.Sprintf("%d", query.GetStartTimestampMs()))
		add(i, endLogKey, fmt.Sprintf("%d", query.GetEndTimestampMs()))

		matcher, err := remoteReadQueryMatchersToString(query)
		if err != nil {
			return nil, err
		}
		add(i, matchersLogKey, matcher)

		if query.Hints != nil {
			if hints, err := json.Marshal(query.Hints); err == nil {
				add(i, hintsLogKey, string(hints))
			} else {
				add(i, hintsLogKey, fmt.Sprintf("error marshalling hints: %v", err))
			}
		}
	}

	return params, nil
}

func remoteReadQueryMatchersToString(q *prompb.Query) (string, error) {
	matchers, err := remote.FromLabelMatchers(q.GetMatchers())
	if err != nil {
		return "", err
	}
	return util.LabelMatchersToString(matchers), nil
}

func remoteReadToMetricsQueryRequest(path string, query *prompb.Query) (MetricsQueryRequest, error) {
	metricsQuery := &remoteReadQueryRequest{
		path:  path,
		query: query,
	}
	var err error
	metricsQuery.promQuery, err = remoteReadQueryMatchersToString(query)
	if err != nil {
		return nil, err
	}
	return metricsQuery, nil
}

type remoteReadQueryRequest struct {
	path      string
	query     *prompb.Query
	promQuery string
}

func (r *remoteReadQueryRequest) AddSpanTags(_ opentracing.Span) {
	// No-op.
}

func (r *remoteReadQueryRequest) GetStart() int64 {
	return r.query.GetStartTimestampMs()
}

func (r *remoteReadQueryRequest) GetEnd() int64 {
	return r.query.GetEndTimestampMs()
}

func (r *remoteReadQueryRequest) GetHints() *Hints {
	return nil
}

func (r *remoteReadQueryRequest) GetStep() int64 {
	if r.query.Hints != nil {
		return r.query.Hints.GetStepMs()
	}
	return 0
}

func (r *remoteReadQueryRequest) GetID() int64 {
	return 0
}

func (r *remoteReadQueryRequest) GetMaxT() int64 {
	return r.GetEnd()
}

func (r *remoteReadQueryRequest) GetMinT() int64 {
	return r.GetStart()
}

func (r *remoteReadQueryRequest) GetOptions() Options {
	return Options{}
}

func (r *remoteReadQueryRequest) GetPath() string {
	return r.path
}

func (r *remoteReadQueryRequest) GetQuery() string {
	return r.promQuery
}

func (r *remoteReadQueryRequest) WithID(_ int64) MetricsQueryRequest {
	panic("not implemented")
}

func (r *remoteReadQueryRequest) WithEstimatedSeriesCountHint(_ uint64) MetricsQueryRequest {
	panic("not implemented")
}

func (r *remoteReadQueryRequest) WithExpr(_ parser.Expr) MetricsQueryRequest {
	panic("not implemented")
}

func (r *remoteReadQueryRequest) WithQuery(_ string) (MetricsQueryRequest, error) {
	panic("not implemented")
}

func (r *remoteReadQueryRequest) WithStartEnd(_ int64, _ int64) MetricsQueryRequest {
	panic("not implemented")
}

func (r *remoteReadQueryRequest) WithTotalQueriesHint(_ int32) MetricsQueryRequest {
	panic("not implemented")
}
