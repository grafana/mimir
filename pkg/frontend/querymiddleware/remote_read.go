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
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/util"
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
	for _, query := range queries {
		metricsRequest, err := toMetricsRequest(req.URL.Path, query)
		if err != nil {
			return nil, err
		}
		handler := r.middleware.Wrap(HandlerFunc(func(ctx context.Context, req MetricsQueryRequest) (Response, error) {
			// We do not need to do anything here as this middleware is used for
			// validation only and previous middlewares would have already returned errors.
			return nil, nil
		}))
		_, error := handler.Do(req.Context(), metricsRequest)
		if error != nil {
			return nil, error
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
		add(i, "start", fmt.Sprintf("%d", query.GetStartTimestampMs()))
		add(i, "end", fmt.Sprintf("%d", query.GetEndTimestampMs()))

		matcher, err := remoteReadMatchersToString(query)
		if err != nil {
			return nil, err
		}
		params.Add("matchers_"+strconv.Itoa(i), matcher)

		if query.Hints != nil {
			if hints, err := json.Marshal(query.Hints); err == nil {
				add(i, "hints", string(hints))
			} else {
				add(i, "hints", fmt.Sprintf("error marshalling hints: %v", err))
			}
		}
	}

	return params, nil
}

func remoteReadMatchersToString(q *prompb.Query) (string, error) {
	matchersStrings := make([]string, 0, len(q.GetMatchers()))
	matchers, err := remote.FromLabelMatchers(q.GetMatchers())
	if err != nil {
		return "", err
	}
	for _, m := range matchers {
		matchersStrings = append(matchersStrings, m.String())
	}
	return strings.Join(matchersStrings, ","), nil
}

func toMetricsRequest(path string, query *prompb.Query) (MetricsQueryRequest, error) {
	metricsQuery := &remoteReadQuery{
		path:  path,
		query: query,
	}
	var err error
	metricsQuery.promQuery, err = remoteReadMatchersToString(query)
	if err != nil {
		return nil, err
	}
	metricsQuery.promQuery = fmt.Sprintf("{%s}", metricsQuery.promQuery)
	return metricsQuery, nil
}

type remoteReadQuery struct {
	path      string
	query     *prompb.Query
	promQuery string
}

var _ = MetricsQueryRequest(&remoteReadQuery{})

// AddSpanTags writes the current `PrometheusRangeQueryRequest` parameters to the specified span tags
// ("attributes" in OpenTelemetry parlance).
func (r *remoteReadQuery) AddSpanTags(sp opentracing.Span) {
	sp.SetTag("query", r.promQuery)
	sp.SetTag("start", timestamp.Time(r.query.GetStartTimestampMs()).String())
	sp.SetTag("end", timestamp.Time(r.query.GetEndTimestampMs()).String())
	//	sp.SetTag("step_ms", r.GetStep())
}

func (r *remoteReadQuery) GetStart() int64 {
	return r.query.GetStartTimestampMs()
}

func (r *remoteReadQuery) GetEnd() int64 {
	return r.query.GetEndTimestampMs()
}

func (r *remoteReadQuery) GetHints() *Hints {
	return nil
}

func (r *remoteReadQuery) GetStep() int64 {
	if r.query.Hints != nil {
		return r.query.Hints.GetStepMs()
	}
	return 0
}

func (r *remoteReadQuery) GetID() int64 {
	return 0
}

func (r *remoteReadQuery) GetMaxT() int64 {
	// ?
	return r.GetEnd()
}

func (r *remoteReadQuery) GetMinT() int64 {
	// ?
	return r.GetStart()
}

func (r *remoteReadQuery) GetOptions() Options {
	// ?
	return Options{}
}

func (r *remoteReadQuery) GetPath() string {
	return r.path
}

func (r *remoteReadQuery) GetQuery() string {
	return r.promQuery
}

func (r *remoteReadQuery) WithID(_ int64) MetricsQueryRequest {
	panic("not implemented")
}

func (r *remoteReadQuery) WithEstimatedSeriesCountHint(_ uint64) MetricsQueryRequest {
	panic("not implemented")
}

func (r *remoteReadQuery) WithExpr(_ parser.Expr) MetricsQueryRequest {
	panic("not implemented")
}

func (r *remoteReadQuery) WithQuery(_ string) (MetricsQueryRequest, error) {
	panic("not implemented")
}

func (r *remoteReadQuery) WithStartEnd(_ int64, _ int64) MetricsQueryRequest {
	panic("not implemented")
}

func (r *remoteReadQuery) WithTotalQueriesHint(_ int32) MetricsQueryRequest {
	panic("not implemented")
}
