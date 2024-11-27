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
			queries[i] = updatedQueryReq.query
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
	// Step is ignored when the remote read query is executed.
	return 0
}

func (r *remoteReadQueryRequest) GetID() int64 {
	return 0
}

func (r *remoteReadQueryRequest) GetLookbackDelta() time.Duration {
	return 0
}

func (r *remoteReadQueryRequest) GetMaxT() int64 {
	// Mimir honors the start/end timerange defined in the read hints, but protects from the case
	// the passed read hints are zero values (because unintentionally initialised but not set).
	if r.query.Hints != nil && r.query.Hints.EndMs > 0 {
		return r.query.Hints.EndMs
	}

	return r.GetEnd()
}

func (r *remoteReadQueryRequest) GetMinT() int64 {
	// Mimir honors the start/end timerange defined in the read hints, but protects from the case
	// the passed read hints are zero values (because unintentionally initialised but not set).
	if r.query.Hints != nil && r.query.Hints.StartMs > 0 {
		return r.query.Hints.StartMs
	}

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

func (r *remoteReadQueryRequest) GetHeaders() []*PrometheusHeader {
	return nil
}

func (r *remoteReadQueryRequest) WithID(_ int64) (MetricsQueryRequest, error) {
	return nil, apierror.New(apierror.TypeInternal, "remoteReadQueryRequest.WithID not implemented")
}

func (r *remoteReadQueryRequest) WithEstimatedSeriesCountHint(_ uint64) (MetricsQueryRequest, error) {
	return nil, apierror.New(apierror.TypeInternal, "remoteReadQueryRequest.WithEstimatedSeriesCountHint not implemented")
}

func (r *remoteReadQueryRequest) WithExpr(_ parser.Expr) (MetricsQueryRequest, error) {
	return nil, apierror.New(apierror.TypeInternal, "remoteReadQueryRequest.WithExpr not implemented")
}

func (r *remoteReadQueryRequest) WithQuery(_ string) (MetricsQueryRequest, error) {
	return nil, apierror.New(apierror.TypeInternal, "remoteReadQueryRequest.WithQuery not implemented")
}

func (r *remoteReadQueryRequest) WithHeaders(_ []*PrometheusHeader) (MetricsQueryRequest, error) {
	return nil, apierror.New(apierror.TypeInternal, "remoteReadQueryRequest.WithHeaders not implemented")
}

// WithStartEnd clones the current remoteReadQueryRequest with a new start and end timestamp.
func (r *remoteReadQueryRequest) WithStartEnd(start int64, end int64) (MetricsQueryRequest, error) {
	clonedQuery, err := cloneRemoteReadQuery(r.query)
	if err != nil {
		return nil, err
	}

	clonedQuery.StartTimestampMs = start
	clonedQuery.EndTimestampMs = end

	// We only clamp the hints time range (and not extend it). If, for any reason, the hints start/end
	// time range is shorter than the query start/end range, then we manipulate only to clamp it to keep
	// it within the requested range.
	if clonedQuery.Hints != nil && clonedQuery.Hints.StartMs < start {
		clonedQuery.Hints.StartMs = start
	}
	if clonedQuery.Hints != nil && clonedQuery.Hints.EndMs > end {
		clonedQuery.Hints.EndMs = end
	}

	return remoteReadToMetricsQueryRequest(r.path, clonedQuery)
}

func (r *remoteReadQueryRequest) WithTotalQueriesHint(_ int32) (MetricsQueryRequest, error) {
	return nil, apierror.New(apierror.TypeInternal, "remoteReadQueryRequest.WithTotalQueriesHint not implemented")
}

// cloneRemoteReadQuery returns a deep copy of the input prompb.Query. To keep this function safe,
// this function does a full marshal and then unmarshal of the prompb.Query.
func cloneRemoteReadQuery(orig *prompb.Query) (*prompb.Query, error) {
	data, err := orig.Marshal()
	if err != nil {
		return nil, err
	}

	cloned := &prompb.Query{}
	if err := cloned.Unmarshal(data); err != nil {
		return nil, err
	}

	return cloned, nil
}
