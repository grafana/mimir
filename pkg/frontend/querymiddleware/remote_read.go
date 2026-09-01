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
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage/remote"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	apierror "github.com/grafana/mimir/pkg/api/error"
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

	remoteReadReq, err := unmarshalRemoteReadRequest(req.Context(), req.Body, int(req.ContentLength))
	if err != nil {
		return nil, err
	}

	// Since we run all the requests in parallel and hitting different server,
	// we ensure that they will all behave the same. It would be difficult to
	// merge different response type. In practice, this could make it fail if
	// the querier supported the second and not the first response type, but
	// for now that will do it.
	acceptedResponseTypes := []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES}
	if 0 < len(remoteReadReq.AcceptedResponseTypes) {
		acceptedResponseTypes = remoteReadReq.AcceptedResponseTypes[:1]
	}

	queries := remoteReadReq.GetQueries()
	responses := make([]Response, len(queries))

	eg, ctx := errgroup.WithContext(req.Context())
	for i, query := range queries {
		eg.Go(func() error {
			// @Enhancement: Run all the queries in parallel
			handler := r.middleware.Wrap(HandlerFunc(func(_ context.Context, metricsReq MetricsQueryRequest) (Response, error) {
				updatedQueryReq, ok := metricsReq.(*remoteReadQueryRequest)
				if !ok {
					// This should never happen.
					return nil, errors.New("unexpected logic bug: remote read roundtripper received an unexpected data type")
				}

				newReadReq := &prompb.ReadRequest{
					Queries:               []*prompb.Query{updatedQueryReq.query},
					AcceptedResponseTypes: acceptedResponseTypes,
				}

				encodedData, err := marshalRemoteReadRequest(newReadReq)
				if err != nil {
					return nil, err
				}

				newReq := req.Clone(ctx)
				newReq.Body = io.NopCloser(bytes.NewBuffer(encodedData))
				newReq.ContentLength = int64(len(encodedData))
				newReq.Header.Set("Content-Encoding", "snappy")

				resp, err := r.next.RoundTrip(newReq)
				if err != nil {
					return nil, err
				}

				if resp.StatusCode/100 != 2 {
					body, _ := io.ReadAll(resp.Body)
					_ = resp.Body.Close()
					return nil, fmt.Errorf("request failed with status %d and error %v", resp.StatusCode, string(body))
				}

				contentType := resp.Header.Get("Content-Type")
				switch {
				case strings.HasPrefix(contentType, "application/x-protobuf"):
					return r.handleSampledResponse(resp)
				case strings.HasPrefix(contentType, "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"):
					return r.handleStreamedResponse(resp)
				default:
					return nil, fmt.Errorf("unsupported content-type %s", contentType)
				}
			}))

			rrReq, err := remoteReadToMetricsQueryRequest(req.URL.Path, query)
			if err != nil {
				return err
			}

			queryResp, err := handler.Do(req.Context(), rrReq)
			if err != nil {
				return apierror.AddDetails(err, fmt.Sprintf("remote read error (%s_%d: %s)", matchersLogKey, i, rrReq.GetQuery()))
			}

			responses[i] = queryResp
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	if acceptedResponseTypes[0] == prompb.ReadRequest_SAMPLES {
		buffer, err := mergeSampleResponses(responses)
		if err != nil {
			return nil, err
		}

		httpResp := &http.Response{
			StatusCode:    200,
			Status:        http.StatusText(http.StatusOK),
			Body:          io.NopCloser(buffer),
			ContentLength: int64(buffer.Available()),
			Header: http.Header{
				"Content-Type":     []string{"application/x-protobuf"},
				"Content-Encoding": []string{"snappy"},
			},
		}

		return httpResp, nil
	} else if acceptedResponseTypes[0] == prompb.ReadRequest_STREAMED_XOR_CHUNKS {
		reader, err := mergeStreamedResponses(responses)
		if err != nil {
			return nil, err
		}

		resp := &http.Response{
			StatusCode:    200,
			Status:        http.StatusText(http.StatusOK),
			Body:          reader,
			ContentLength: -1,
			Header: http.Header{
				"Content-Type": []string{"application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"},
			},
		}
		return resp, nil
	} else {
		return nil, fmt.Errorf("unknown response type %v", acceptedResponseTypes[0])
	}
}

func (r *remoteReadRoundTripper) handleSampledResponse(resp *http.Response) (Response, error) {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	data, err = snappy.Decode(nil, data)
	if err != nil {
		return nil, err
	}

	var readResp prompb.ReadResponse
	if err = proto.Unmarshal(data, &readResp); err != nil {
		return nil, err
	}

	if len(readResp.Results) == 0 {
		return &remoteReadSampledResponse{Result: &prompb.QueryResult{}}, nil
	} else if len(readResp.Results) != 1 {
		return nil, fmt.Errorf("Expected 1 result, got %d", len(readResp.Results))
	}

	return &remoteReadSampledResponse{Result: readResp.Results[0]}, nil
}

func (r *remoteReadRoundTripper) handleStreamedResponse(resp *http.Response) (Response, error) {
	retResp := &remoteReadStreamedResponse{
		Reader: remote.NewChunkedReader(resp.Body, config.DefaultChunkedReadLimit, nil),
		Closer: resp.Body.(io.Closer),
	}

	return retResp, nil
}

func mergeSampleResponses(responses []Response) (*bytes.Buffer, error) {
	rrResp := prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(responses)),
	}

	for idx, resp := range responses {
		switch value := resp.(type) {
		case *remoteReadSampledResponse:
			rrResp.Results[idx] = value.Result
		case *PrometheusResponse:
			// This is mostly there to handle the current state of the limits middleware
			// that return an "empty prometheus response" when outside the configured
			// limits. We still need to put a "query result" in the results, but we
			// can put whatever we want.
			rrResp.Results[idx] = &prompb.QueryResult{}
		default:
			return nil, fmt.Errorf("unsupported response type %T", resp)
		}
	}

	data, err := rrResp.Marshal()
	if err != nil {
		return nil, err
	}

	return bytes.NewBuffer(snappy.Encode(nil, data)), nil
}

func mergeStreamedResponses(responses []Response) (io.ReadCloser, error) {
	// @Enhancement:
	// The `ChunkedReader` does a lot of unnecessary works for our purpose,
	// like checksuming the data. We could optimize that part, but it's easier
	// to re-use the code for a first version.
	readers := make([]*remote.ChunkedReader, len(responses))
	closers := make([]io.Closer, len(responses))
	for idx, resp := range responses {
		switch value := resp.(type) {
		case *remoteReadStreamedResponse:
			readers[idx] = value.Reader
			closers[idx] = value.Closer
		case *PrometheusResponse:
			emptyReader := &bytes.Buffer{}
			readers[idx] = remote.NewChunkedReader(emptyReader, 1, nil)
			closers[idx] = io.NopCloser(emptyReader)
		default:
			return nil, fmt.Errorf("unsupported response type %T", resp)
		}
	}

	chunkReader := NewMergeChunkedReader(readers...)
	pr, pw := io.Pipe()
	go func() {
		chunkWriter := remote.NewChunkedWriter(pw, nil)
		for {
			chunk, err := chunkReader.Next()
			if err != nil {
				_ = pw.CloseWithError(err)
				break
			}
			if _, err = chunkWriter.Write(chunk); err != nil {
				_ = pw.CloseWithError(err)
				break
			}
		}

		for _, r := range responses {
			r.Close()
		}
	}()

	return pr, nil
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

func (r *remoteReadQueryRequest) GetQueryOpts() (promql.QueryOpts, error) {
	return nil, errCantGetQueryOptsForRemoteReadRequest
}

func (r *remoteReadQueryRequest) AddSpanTags(_ trace.Span) {
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

func (r *remoteReadQueryRequest) GetOptions() requestoptions.Options {
	return requestoptions.Options{}
}

func (r *remoteReadQueryRequest) GetPath() string {
	return r.path
}

func (r *remoteReadQueryRequest) GetQuery() string {
	return r.promQuery
}

func (r *remoteReadQueryRequest) GetParsedQuery() parser.Expr {
	panic("remoteReadQueryRequest.GetParsedQuery() should not be called")
}

func (r *remoteReadQueryRequest) GetClonedParsedQuery() (parser.Expr, error) {
	if r.promQuery == "" {
		return nil, errRequestNoQuery
	}

	return promqlext.NewPromQLParser().ParseExpr(r.promQuery)
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

func (r *remoteReadQueryRequest) WithStats(stats string) (MetricsQueryRequest, error) {
	return nil, apierror.New(apierror.TypeInternal, "remoteReadQueryRequest.WithStats not implemented")
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

type remoteReadStreamedResponse struct {
	Reader *remote.ChunkedReader
	io.Closer
}

func (r *remoteReadStreamedResponse) GetHeaders() []*PrometheusHeader {
	return nil
}

func (r *remoteReadStreamedResponse) GetPrometheusResponse() (*PrometheusResponse, bool) {
	return nil, false
}

func (r *remoteReadStreamedResponse) Close() {
	_ = r.Closer.Close()
}

func (r *remoteReadStreamedResponse) Reset() {
	panic("no implemented")
}

func (r *remoteReadStreamedResponse) String() string {
	panic("no implemented")
}

func (r *remoteReadStreamedResponse) ProtoMessage() {
	panic("no implemented")
}

type remoteReadSampledResponse struct {
	Result *prompb.QueryResult
}

func (r *remoteReadSampledResponse) GetHeaders() []*PrometheusHeader {
	return nil
}

func (r *remoteReadSampledResponse) GetPrometheusResponse() (*PrometheusResponse, bool) {
	return nil, false
}

func (r *remoteReadSampledResponse) Close() {
}

func (r *remoteReadSampledResponse) Reset() {
	panic("no implemented")
}

func (r *remoteReadSampledResponse) String() string {
	panic("no implemented")
}

func (r *remoteReadSampledResponse) ProtoMessage() {
	panic("no implemented")
}
