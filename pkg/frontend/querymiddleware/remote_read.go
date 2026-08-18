// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage/remote"
	"go.opentelemetry.io/otel/trace"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

// To keep logs and error messages in sync, we define the following keys:
const (
	endLogKey      = "end"
	hintsLogKey    = "hints"
	matchersLogKey = "matchers"
	startLogKey    = "start"
)

var errCantGetQueryOptsForRemoteReadRequest = errors.New("cannot get PromQL query options from remote read query request")

type remoteReadRoundTripper struct {
	next http.RoundTripper

	middleware MetricsQueryMiddleware
}

func NewRemoteReadRoundTripper(next http.RoundTripper, middlewares ...MetricsQueryMiddleware) http.RoundTripper {
	return &remoteReadRoundTripper{
		next:       next,
		middleware: MergeMetricsQueryMiddlewares(middlewares...),
	}
}

func (r *remoteReadRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body == nil {
		return r.next.RoundTrip(req)
	}

	// Parse the request body consuming it! From now on we can't call the next http.RoundTrigger without
	// replacing the Body.
	remoteReadReq, err := unmarshalRemoteReadRequest(req.Context(), req.Body, int(req.ContentLength))
	if err != nil {
		return nil, err
	}

	// Run each query through the middlewares.
	queries := remoteReadReq.GetQueries()

	for i, query := range queries {
		// Parse the original query.
		origQueryReq, err := remoteReadToMetricsQueryRequest(req.URL.Path, query)
		if err != nil {
			return nil, err
		}

		// Run the query through the middlewares.
		var updatedQueryReq *remoteReadQueryRequest
		handler := r.middleware.Wrap(HandlerFunc(func(_ context.Context, req MetricsQueryRequest) (Response, error) {
			var ok bool

			// The middlewares are used only for validation, but some middlewares may manipulate
			// the request to enforce some limits (e.g. time range limit). For this reason, we
			// capture the final request in case it was manipulated.
			if updatedQueryReq, ok = req.(*remoteReadQueryRequest); !ok {
				// This should never happen.
				return nil, errors.New("unexpected logic bug: remote read roundtripper received an unexpected data type")
			}

			return nil, nil
		}))

		_, err = handler.Do(req.Context(), origQueryReq)
		if err != nil {
			return nil, apierror.AddDetails(err, fmt.Sprintf("remote read error (%s_%d: %s)", matchersLogKey, i, origQueryReq.GetQuery()))
		}

		// The query may have been manipulated. We always replace it (if it wasn't manipulated, then
		// we're just overwriting it with the same exact ref).
		//
		// NOTE: updatedQueryReq may be nil if a middleware interrupted the middlewares execution without
		//       returning an error. It could happen in middlewares returning an empty response under some
		//       conditions. In such case, since we don't have a way to return an empty response for the
		//       selected query, we simply keep the original one and let it pass-through the downstream.
		if updatedQueryReq != nil {
			queries[i], err = updatedQueryReq.GetRemoteReadQuery()
			if err != nil {
				return nil, err
			}
		}
	}

	// At this point the queries may have been manipulated by the middlewares. We marshal the remote request again
	// in order to inject the manipulated queries. We always do it, even if the queries haven't been manipulated by
	// middlewares, so that we always exercise this code.
	remoteReadReq.Queries = queries

	// Marshal the (maybe modified) remote read request and replace the request body.
	encodedData, err := marshalRemoteReadRequest(remoteReadReq)
	if err != nil {
		return nil, err
	}

	req.Body = io.NopCloser(bytes.NewBuffer(encodedData))
	req.Header.Set("Content-Length", strconv.Itoa(len(encodedData)))
	req.Header.Set("Content-Encoding", "snappy")
	req.ContentLength = int64(len(encodedData))

	return r.next.RoundTrip(req)
}

// ParseRemoteReadRequestValuesWithoutConsumingBody parses a remote read request
// without consuming the body. It does not check the req.Body size, so it is
// the caller's responsibility to ensure that the body is not too large.
func ParseRemoteReadRequestValuesWithoutConsumingBody(req *http.Request) (url.Values, error) {
	remoteReadRequest, err := parseRemoteReadRequestWithoutConsumingBody(req)
	if err != nil {
		return nil, err
	}
	return parseRemoteReadRequestValues(remoteReadRequest)
}

func parseRemoteReadRequestWithoutConsumingBody(req *http.Request) (*prompb.ReadRequest, error) {
	if req.Body == nil {
		return nil, nil
	}

	bodyBytes, err := util.ReadRequestBodyWithoutConsuming(req)
	if err != nil {
		return nil, err
	}

	return unmarshalRemoteReadRequest(req.Context(), io.NopCloser(bytes.NewReader(bodyBytes)), int(req.ContentLength))
}

// unmarshalRemoteReadRequest reads from the input read and unmarshals the content into a prompb.ReadRequest.
// This function either returns prompb.ReadRequest or an error, but never nil to both.
func unmarshalRemoteReadRequest(ctx context.Context, reader io.ReadCloser, contentLength int) (*prompb.ReadRequest, error) {
	remoteReadRequest := &prompb.ReadRequest{}

	_, err := util.ParseProtoReader(ctx, reader, contentLength, querier.MaxRemoteReadQuerySize, nil, remoteReadRequest, util.RawSnappy)
	if err != nil {
		return nil, err
	}

	return remoteReadRequest, nil
}

// marshalRemoteReadRequest marshals the input prompb.ReadRequest protobuf and encode it with snappy.
func marshalRemoteReadRequest(req *prompb.ReadRequest) ([]byte, error) {
	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}

	return snappy.Encode(nil, data), nil
}

func parseRemoteReadRequestValues(remoteReadRequest *prompb.ReadRequest) (url.Values, error) {
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
	promQuery, err := remoteReadQueryMatchersToString(query)
	if err != nil {
		return nil, err
	}

	expr, err := promqlext.NewPromQLParser().ParseExpr(promQuery)
	if err != nil {
		return nil, err
	}

	metricsQuery := &remoteReadQueryRequest{
		path:      path,
		queryExpr: expr,
		start:     query.StartTimestampMs,
		end:       query.EndTimestampMs,
		hints:     query.Hints,
	}

	return metricsQuery, nil
}

type remoteReadQueryRequest struct {
	path      string
	queryExpr parser.Expr
	start     int64
	end       int64
	hints     *prompb.ReadHints

	// ID of the request used to correlate downstream requests and responses.
	id int64
}

func (r *remoteReadQueryRequest) GetRemoteReadQuery() (*prompb.Query, error) {
	vecSel, ok := r.queryExpr.(*parser.VectorSelector)
	if ok != true {
		return nil, fmt.Errorf("Expecting 'VectorSelector', got %T", r.queryExpr)
	}

	convertType := func(matchType labels.MatchType) prompb.LabelMatcher_Type {
		switch matchType {
		case labels.MatchEqual:
			return prompb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			return prompb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			return prompb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			return prompb.LabelMatcher_NRE
		}
		panic("todo: should we panic or error here?")
	}

	matchers := make([]*prompb.LabelMatcher, 0, len(vecSel.LabelMatchers))
	for _, matcher := range vecSel.LabelMatchers {
		matchers = append(matchers, &prompb.LabelMatcher{
			Type:  convertType(matcher.Type),
			Name:  matcher.Name,
			Value: matcher.Value,
		})
	}

	return &prompb.Query{
		StartTimestampMs: r.start,
		EndTimestampMs:   r.end,
		Matchers:         matchers,
		Hints:            r.hints,
	}, nil
}

func (r *remoteReadQueryRequest) GetQueryOpts() (promql.QueryOpts, error) {
	return nil, errCantGetQueryOptsForRemoteReadRequest
}

func (r *remoteReadQueryRequest) AddSpanTags(_ trace.Span) {
	// No-op.
}

func (r *remoteReadQueryRequest) GetStart() int64 {
	return r.start
}

func (r *remoteReadQueryRequest) GetEnd() int64 {
	return r.end
}

func (r *remoteReadQueryRequest) GetHints() *Hints {
	return nil
}

func (r *remoteReadQueryRequest) GetStep() int64 {
	// Step is ignored when the remote read query is executed.
	return 1
}

func (r *remoteReadQueryRequest) GetID() int64 {
	return r.id
}

func (r *remoteReadQueryRequest) GetMaxT() int64 {
	// Mimir honors the start/end timerange defined in the read hints, but protects from the case
	// the passed read hints are zero values (because unintentionally initialised but not set).
	if r.hints != nil && r.hints.EndMs > 0 {
		return r.hints.EndMs
	}

	return r.GetEnd()
}

func (r *remoteReadQueryRequest) GetMinT() int64 {
	// Mimir honors the start/end timerange defined in the read hints, but protects from the case
	// the passed read hints are zero values (because unintentionally initialised but not set).
	if r.hints != nil && r.hints.StartMs > 0 {
		return r.hints.StartMs
	}

	return r.GetStart()
}

func (r *remoteReadQueryRequest) GetOptions() requestoptions.Options {
	return requestoptions.Options{}
}

func (r *remoteReadQueryRequest) GetPath() string {
	return r.path
}

func (r *remoteReadQueryRequest) GetQuery() string {
	if r.queryExpr != nil {
		return r.queryExpr.String()
	}
	return ""
}

func (r *remoteReadQueryRequest) GetParsedQuery() parser.Expr {
	panic("remoteReadQueryRequest.GetParsedQuery() should not be called")
}

func (r *remoteReadQueryRequest) GetClonedParsedQuery() (parser.Expr, error) {
	if r.queryExpr == nil {
		return nil, errRequestNoQuery
	}

	return astmapper.CloneExpr(r.queryExpr)
}

func (r *remoteReadQueryRequest) GetHeaders() []*PrometheusHeader {
	return nil
}

func (r *remoteReadQueryRequest) GetLookbackDelta() time.Duration {
	return 0
}

func (r *remoteReadQueryRequest) GetStats() string {
	return ""
}

func (r *remoteReadQueryRequest) WithID(id int64) (MetricsQueryRequest, error) {
	newRequest := *r
	var err error
	newRequest.hints, err = cloneHints(r.hints)
	if err != nil {
		return nil, err
	}

	newRequest.id = id
	return &newRequest, nil
}

func (r *remoteReadQueryRequest) WithEstimatedSeriesCountHint(_ uint64) (MetricsQueryRequest, error) {
	return nil, apierror.New(apierror.TypeInternal, "remoteReadQueryRequest.WithEstimatedSeriesCountHint not implemented")
}

func (r *remoteReadQueryRequest) WithExpr(queryExpr parser.Expr) (MetricsQueryRequest, error) {
	newRequest := *r
	var err error
	newRequest.hints, err = cloneHints(r.hints)
	if err != nil {
		return nil, err
	}

	newRequest.queryExpr = queryExpr
	return &newRequest, nil
}

func (r *remoteReadQueryRequest) WithQuery(_ string) (MetricsQueryRequest, error) {
	return nil, apierror.New(apierror.TypeInternal, "remoteReadQueryRequest.WithQuery not implemented")
}

func (r *remoteReadQueryRequest) WithHeaders(_ []*PrometheusHeader) (MetricsQueryRequest, error) {
	return nil, apierror.New(apierror.TypeInternal, "remoteReadQueryRequest.WithHeaders not implemented")
}

// WithStartEnd clones the current remoteReadQueryRequest with a new start and end timestamp.
func (r *remoteReadQueryRequest) WithStartEnd(start int64, end int64) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.start = start
	newRequest.end = end

	var err error
	newRequest.hints, err = cloneHints(r.hints)
	if err != nil {
		return nil, err
	}

	// We only clamp the hints time range (and not extend it). If, for any reason, the hints start/end
	// time range is shorter than the query start/end range, then we manipulate only to clamp it to keep
	// it within the requested range.
	if newRequest.hints != nil && newRequest.hints.StartMs < start {
		newRequest.hints.StartMs = start
	}
	if newRequest.hints != nil && newRequest.hints.EndMs > end {
		newRequest.hints.EndMs = end
	}

	return &newRequest, nil
}

func (r *remoteReadQueryRequest) WithTotalQueriesHint(_ int32) (MetricsQueryRequest, error) {
	newRequest := *r
	var err error
	newRequest.hints, err = cloneHints(r.hints)
	if err != nil {
		return nil, err
	}
	return &newRequest, nil
}

func (r *remoteReadQueryRequest) WithStats(stats string) (MetricsQueryRequest, error) {
	return nil, apierror.New(apierror.TypeInternal, "remoteReadQueryRequest.WithStats not implemented")
}

// cloneHints returns a deep copy of the input prompb.ReadHints. To keep this function safe,
// this function does a full marshal and then unmarshal of the prompb.Hints.
func cloneHints(hints *prompb.ReadHints) (*prompb.ReadHints, error) {
	if hints == nil {
		return nil, nil
	}
	data, err := hints.Marshal()
	if err != nil {
		return nil, err
	}

	cloned := &prompb.ReadHints{}
	if err := cloned.Unmarshal(data); err != nil {
		return nil, err
	}

	return cloned, nil
}
