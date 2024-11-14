// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/grpcutil"
	"github.com/munnerz/goautoneg"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"golang.org/x/exp/slices"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/chunkinfologger"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var (
	errEndBeforeStart = apierror.New(apierror.TypeBadData, `invalid parameter "end": end timestamp must not be before start time`)
	errNegativeStep   = apierror.New(apierror.TypeBadData, `invalid parameter "step": zero or negative query resolution step widths are not accepted. Try a positive integer`)
	errStepTooSmall   = apierror.New(apierror.TypeBadData, "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
	allFormats        = []string{formatJSON, formatProtobuf}

	// List of HTTP headers to propagate when a Prometheus request is encoded into a HTTP request.
	prometheusCodecPropagateHeadersMetrics = []string{compat.ForceFallbackHeaderName, chunkinfologger.ChunkInfoLoggingHeader, api.ReadConsistencyOffsetsHeader}
	prometheusCodecPropagateHeadersLabels  = []string{}
)

const (
	// statusSuccess Prometheus success result.
	statusSuccess = "success"
	// statusSuccess Prometheus error result.
	statusError = "error"

	totalShardsControlHeader = "Sharding-Control"

	// Instant query specific options
	instantSplitControlHeader = "Instant-Split-Control"

	operationEncode = "encode"
	operationDecode = "decode"

	formatJSON     = "json"
	formatProtobuf = "protobuf"
)

// Codec is used to encode/decode query requests and responses so they can be passed down to middlewares.
type Codec interface {
	Merger
	// DecodeMetricsQueryRequest decodes a MetricsQueryRequest from an http request.
	DecodeMetricsQueryRequest(context.Context, *http.Request) (MetricsQueryRequest, error)
	// DecodeLabelsQueryRequest decodes a LabelsQueryRequest from an http request.
	DecodeLabelsQueryRequest(context.Context, *http.Request) (LabelsQueryRequest, error)
	// DecodeMetricsQueryResponse decodes a Response from an http response.
	// The original request is also passed as a parameter this is useful for implementation that needs the request
	// to merge result or build the result correctly.
	DecodeMetricsQueryResponse(context.Context, *http.Response, MetricsQueryRequest, log.Logger) (Response, error)
	// DecodeLabelsQueryResponse decodes a Response from an http response.
	// The original request is also passed as a parameter this is useful for implementation that needs the request
	// to merge result or build the result correctly.
	DecodeLabelsQueryResponse(context.Context, *http.Response, LabelsQueryRequest, log.Logger) (Response, error)
	// EncodeMetricsQueryRequest encodes a MetricsQueryRequest into an http request.
	EncodeMetricsQueryRequest(context.Context, MetricsQueryRequest) (*http.Request, error)
	// EncodeLabelsQueryRequest encodes a LabelsQueryRequest into an http request.
	EncodeLabelsQueryRequest(context.Context, LabelsQueryRequest) (*http.Request, error)
	// EncodeMetricsQueryResponse encodes a Response from a MetricsQueryRequest into an http response.
	EncodeMetricsQueryResponse(context.Context, *http.Request, Response) (*http.Response, error)
	// EncodeLabelsQueryResponse encodes a Response from a LabelsQueryRequest into an http response.
	EncodeLabelsQueryResponse(context.Context, *http.Request, Response, bool) (*http.Response, error)
}

// Merger is used by middlewares making multiple requests to merge back all responses into a single one.
type Merger interface {
	// MergeResponse merges responses from multiple requests into a single Response
	MergeResponse(...Response) (Response, error)
}

// MetricsQueryRequest represents an instant or query range request that can be process by middlewares.
type MetricsQueryRequest interface {
	// GetID returns the ID of the request used to correlate downstream requests and responses.
	GetID() int64
	// GetPath returns the URL Path of the request
	GetPath() string
	// GetHeaders returns the HTTP headers in the request.
	GetHeaders() []*PrometheusHeader
	// GetStart returns the start timestamp of the query time range in milliseconds.
	GetStart() int64
	// GetEnd returns the end timestamp of the query time range in milliseconds.
	// The start and end timestamp are set to the same value in case of an instant query.
	GetEnd() int64
	// GetStep returns the step of the request in milliseconds.
	GetStep() int64
	// GetQuery returns the query of the request.
	GetQuery() string
	// GetQueryExpr returns the parsed query expression of the request.
	GetQueryExpr() parser.Expr
	// GetMinT returns the minimum timestamp in milliseconds of data to be queried,
	// as determined from the start timestamp and any range vector or offset in the query.
	GetMinT() int64
	// GetMaxT returns the maximum timestamp in milliseconds of data to be queried,
	// as determined from the end timestamp and any offset in the query.
	GetMaxT() int64
	// GetOptions returns the options for the given request.
	GetOptions() Options
	// GetHints returns hints that could be optionally attached to the request to pass down the stack.
	// These hints can be used to optimize the query execution.
	GetHints() *Hints
	// WithID clones the current request with the provided ID.
	WithID(id int64) (MetricsQueryRequest, error)
	// WithStartEnd clone the current request with different start and end timestamp.
	// Implementations must ensure minT and maxT are recalculated when the start and end timestamp change.
	WithStartEnd(startTime int64, endTime int64) (MetricsQueryRequest, error)
	// WithQuery clones the current request with a different query; returns error if query parse fails.
	// Implementations must ensure minT and maxT are recalculated when the query changes.
	WithQuery(string) (MetricsQueryRequest, error)
	// WithHeaders clones the current request with different headers.
	WithHeaders([]*PrometheusHeader) (MetricsQueryRequest, error)
	// WithExpr clones the current `PrometheusRangeQueryRequest` with a new query expression.
	// Implementations must ensure minT and maxT are recalculated when the query changes.
	WithExpr(parser.Expr) (MetricsQueryRequest, error)
	// WithTotalQueriesHint adds the number of total queries to this request's Hints.
	WithTotalQueriesHint(int32) (MetricsQueryRequest, error)
	// WithEstimatedSeriesCountHint WithEstimatedCardinalityHint adds a cardinality estimate to this request's Hints.
	WithEstimatedSeriesCountHint(uint64) (MetricsQueryRequest, error)
	// AddSpanTags writes information about this request to an OpenTracing span
	AddSpanTags(opentracing.Span)
}

// LabelsQueryRequest represents a label names or values query request that can be process by middlewares.
type LabelsQueryRequest interface {
	// GetLabelName returns the label name param from a Label Values request `/api/v1/label/<label_name>/values`
	// or an empty string for a Label Names request `/api/v1/labels`
	GetLabelName() string
	// GetStart returns the start timestamp of the request in milliseconds
	GetStart() int64
	// GetStartOrDefault returns the start timestamp of the request in milliseconds,
	// or the Prometheus v1 API MinTime if no start timestamp was provided on the original request.
	GetStartOrDefault() int64
	// GetEnd returns the start timestamp of the request in milliseconds
	GetEnd() int64
	// GetEndOrDefault returns the end timestamp of the request in milliseconds,
	// or the Prometheus v1 API MaxTime if no end timestamp was provided on the original request.
	GetEndOrDefault() int64
	// GetLabelMatcherSets returns the label matchers a.k.a series selectors for Prometheus label query requests,
	// as retained in their original string format. This enables the request to be symmetrically decoded and encoded
	// to and from the http request format without needing to undo the Prometheus parser converting between formats
	// like `up{job="prometheus"}` and `{__name__="up, job="prometheus"}`, or other idiosyncrasies.
	GetLabelMatcherSets() []string
	// GetLimit returns the limit of the number of items in the response.
	GetLimit() uint64
	// GetHeaders returns the HTTP headers in the request.
	GetHeaders() []*PrometheusHeader
	// WithLabelName clones the current request with a different label name param.
	WithLabelName(string) (LabelsQueryRequest, error)
	// WithLabelMatcherSets clones the current request with different label matchers.
	WithLabelMatcherSets([]string) (LabelsQueryRequest, error)
	// WithHeaders clones the current request with different headers.
	WithHeaders([]*PrometheusHeader) (LabelsQueryRequest, error)
	// AddSpanTags writes information about this request to an OpenTracing span
	AddSpanTags(opentracing.Span)
}

// Response represents a query range response.
type Response interface {
	proto.Message
	// GetHeaders returns the HTTP headers in the response.
	GetHeaders() []*PrometheusHeader
}

type prometheusCodecMetrics struct {
	duration *prometheus.HistogramVec
	size     *prometheus.HistogramVec
}

func newPrometheusCodecMetrics(registerer prometheus.Registerer) *prometheusCodecMetrics {
	factory := promauto.With(registerer)
	second := 1.0
	ms := second / 1000
	kb := 1024.0
	mb := 1024 * kb

	return &prometheusCodecMetrics{
		duration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_frontend_query_response_codec_duration_seconds",
			Help:    "Total time spent encoding or decoding query result payloads, in seconds.",
			Buckets: prometheus.ExponentialBucketsRange(1*ms, 2*second, 10),
		}, []string{"operation", "format"}),
		size: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_frontend_query_response_codec_payload_bytes",
			Help:    "Total size of query result payloads, in bytes.",
			Buckets: prometheus.ExponentialBucketsRange(1*kb, 512*mb, 10),
		}, []string{"operation", "format"}),
	}
}

type prometheusCodec struct {
	metrics                                         *prometheusCodecMetrics
	lookbackDelta                                   time.Duration
	preferredQueryResultResponseFormat              string
	propagateHeadersMetrics, propagateHeadersLabels []string
}

type formatter interface {
	EncodeQueryResponse(resp *PrometheusResponse) ([]byte, error)
	EncodeLabelsResponse(resp *PrometheusLabelsResponse) ([]byte, error)
	EncodeSeriesResponse(resp *PrometheusSeriesResponse) ([]byte, error)
	DecodeQueryResponse([]byte) (*PrometheusResponse, error)
	DecodeLabelsResponse([]byte) (*PrometheusLabelsResponse, error)
	DecodeSeriesResponse([]byte) (*PrometheusSeriesResponse, error)
	Name() string
	ContentType() v1.MIMEType
}

var jsonFormatterInstance = jsonFormatter{}

var knownFormats = []formatter{
	jsonFormatterInstance,
	protobufFormatter{},
}

func NewPrometheusCodec(
	registerer prometheus.Registerer,
	lookbackDelta time.Duration,
	queryResultResponseFormat string,
	propagateHeaders []string,
) Codec {
	return prometheusCodec{
		metrics:                            newPrometheusCodecMetrics(registerer),
		lookbackDelta:                      lookbackDelta,
		preferredQueryResultResponseFormat: queryResultResponseFormat,
		propagateHeadersMetrics:            append(prometheusCodecPropagateHeadersMetrics, propagateHeaders...),
		propagateHeadersLabels:             append(prometheusCodecPropagateHeadersLabels, propagateHeaders...),
	}
}

func (prometheusCodec) MergeResponse(responses ...Response) (Response, error) {
	if len(responses) == 0 {
		return newEmptyPrometheusResponse(), nil
	}

	promResponses := make([]*PrometheusResponse, 0, len(responses))
	promWarningsMap := make(map[string]struct{}, 0)
	promInfosMap := make(map[string]struct{}, 0)
	var present struct{}

	for _, res := range responses {
		pr := res.(*PrometheusResponse)
		if pr.Status != statusSuccess {
			return nil, fmt.Errorf("can't merge an unsuccessful response")
		} else if pr.Data == nil {
			return nil, fmt.Errorf("can't merge response with no data")
		} else if pr.Data.ResultType != model.ValMatrix.String() {
			return nil, fmt.Errorf("can't merge result type %q", pr.Data.ResultType)
		}

		promResponses = append(promResponses, pr)
		for _, warning := range pr.Warnings {
			promWarningsMap[warning] = present
		}
		for _, info := range pr.Infos {
			promInfosMap[info] = present
		}
	}

	var promWarnings []string
	for warning := range promWarningsMap {
		promWarnings = append(promWarnings, warning)
	}

	var promInfos []string
	for info := range promInfosMap {
		promInfos = append(promInfos, info)
	}

	// Merge the responses.
	sort.Sort(byFirstTime(promResponses))

	return &PrometheusResponse{
		Status: statusSuccess,
		Data: &PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result:     matrixMerge(promResponses),
		},
		Warnings: promWarnings,
		Infos:    promInfos,
	}, nil
}

func (c prometheusCodec) DecodeMetricsQueryRequest(_ context.Context, r *http.Request) (MetricsQueryRequest, error) {
	switch {
	case IsRangeQuery(r.URL.Path):
		return c.decodeRangeQueryRequest(r)
	case IsInstantQuery(r.URL.Path):
		return c.decodeInstantQueryRequest(r)
	default:
		return nil, fmt.Errorf("unknown metrics query API endpoint %s", r.URL.Path)
	}
}

func (c prometheusCodec) decodeRangeQueryRequest(r *http.Request) (MetricsQueryRequest, error) {
	reqValues, err := util.ParseRequestFormWithoutConsumingBody(r)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	headers := make([]*PrometheusHeader, 0, len(r.Header))
	for h, hv := range r.Header {
		headers = append(headers, &PrometheusHeader{Name: h, Values: slices.Clone(hv)})
	}
	sort.Slice(headers, func(i, j int) bool { return headers[i].Name < headers[j].Name })

	start, end, step, err := DecodeRangeQueryTimeParams(&reqValues)
	if err != nil {
		return nil, err
	}

	query := reqValues.Get("query")
	queryExpr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, DecorateWithParamName(err, "query")
	}

	var options Options
	decodeOptions(r, &options)

	req := NewPrometheusRangeQueryRequest(
		r.URL.Path, headers, start, end, step, c.lookbackDelta, queryExpr, options, nil,
	)
	return req, nil
}

func (c prometheusCodec) decodeInstantQueryRequest(r *http.Request) (MetricsQueryRequest, error) {
	reqValues, err := util.ParseRequestFormWithoutConsumingBody(r)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	headers := make([]*PrometheusHeader, 0, len(r.Header))
	for h, hv := range r.Header {
		headers = append(headers, &PrometheusHeader{Name: h, Values: slices.Clone(hv)})
	}
	sort.Slice(headers, func(i, j int) bool { return headers[i].Name < headers[j].Name })

	time, err := DecodeInstantQueryTimeParams(&reqValues, time.Now)
	if err != nil {
		return nil, DecorateWithParamName(err, "time")
	}

	query := reqValues.Get("query")
	queryExpr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, DecorateWithParamName(err, "query")
	}

	var options Options
	decodeOptions(r, &options)

	req := NewPrometheusInstantQueryRequest(
		r.URL.Path, headers, time, c.lookbackDelta, queryExpr, options, nil,
	)
	return req, nil
}

func (prometheusCodec) DecodeLabelsQueryRequest(_ context.Context, r *http.Request) (LabelsQueryRequest, error) {
	if !IsLabelsQuery(r.URL.Path) && !IsSeriesQuery(r.URL.Path) {
		return nil, fmt.Errorf("unknown labels query API endpoint %s", r.URL.Path)
	}

	reqValues, err := util.ParseRequestFormWithoutConsumingBody(r)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}
	start, end, err := DecodeLabelsQueryTimeParams(&reqValues, false)
	if err != nil {
		return nil, err
	}

	labelMatcherSets := reqValues["match[]"]

	limit := uint64(0) // 0 means unlimited
	if limitStr := reqValues.Get("limit"); limitStr != "" {
		limit, err = strconv.ParseUint(limitStr, 10, 64)
		if err != nil || limit == 0 {
			return nil, apierror.New(apierror.TypeBadData, fmt.Sprintf("limit parameter must be a positive number: %s", limitStr))
		}
	}

	if IsSeriesQuery(r.URL.Path) {
		return &PrometheusSeriesQueryRequest{
			Path:             r.URL.Path,
			Start:            start,
			End:              end,
			LabelMatcherSets: labelMatcherSets,
			Limit:            limit,
		}, nil
	}
	if IsLabelNamesQuery(r.URL.Path) {
		return &PrometheusLabelNamesQueryRequest{
			Path:             r.URL.Path,
			Start:            start,
			End:              end,
			LabelMatcherSets: labelMatcherSets,
			Limit:            limit,
		}, nil
	}
	// else, must be Label Values Request due to IsLabelsQuery check at beginning of func
	return &PrometheusLabelValuesQueryRequest{
		Path:             r.URL.Path,
		LabelName:        labelValuesPathSuffix.FindStringSubmatch(r.URL.Path)[1],
		Start:            start,
		End:              end,
		LabelMatcherSets: labelMatcherSets,
		Limit:            limit,
	}, nil
}

// DecodeRangeQueryTimeParams encapsulates Prometheus instant query time param parsing,
// emulating the logic in prometheus/prometheus/web/api/v1#API.query_range.
func DecodeRangeQueryTimeParams(reqValues *url.Values) (start, end, step int64, err error) {
	start, err = util.ParseTime(reqValues.Get("start"))
	if err != nil {
		return 0, 0, 0, DecorateWithParamName(err, "start")
	}

	end, err = util.ParseTime(reqValues.Get("end"))
	if err != nil {
		return 0, 0, 0, DecorateWithParamName(err, "end")
	}

	if end < start {
		return 0, 0, 0, errEndBeforeStart
	}

	step, err = parseDurationMs(reqValues.Get("step"))
	if err != nil {
		return 0, 0, 0, DecorateWithParamName(err, "step")
	}

	if step <= 0 {
		return 0, 0, 0, errNegativeStep
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if (end-start)/step > 11000 {
		return 0, 0, 0, errStepTooSmall
	}

	return start, end, step, nil
}

// DecodeInstantQueryTimeParams encapsulates Prometheus instant query time param parsing,
// emulating the logic in prometheus/prometheus/web/api/v1#API.query.
func DecodeInstantQueryTimeParams(reqValues *url.Values, defaultNow func() time.Time) (time int64, err error) {
	timeVal := reqValues.Get("time")
	if timeVal == "" {
		time = defaultNow().UnixMilli()
	} else {
		time, err = util.ParseTime(timeVal)
		if err != nil {
			return 0, DecorateWithParamName(err, "time")
		}
	}

	return time, err
}

// DecodeLabelsQueryTimeParams encapsulates Prometheus label names and label values query time param parsing,
// emulating the logic in prometheus/prometheus/web/api/v1#API.labelNames and v1#API.labelValues.
//
// Setting `usePromDefaults` true will set missing timestamp params to the Prometheus default
// min and max query timestamps; false will default to 0 for missing timestamp params.
func DecodeLabelsQueryTimeParams(reqValues *url.Values, usePromDefaults bool) (start, end int64, err error) {
	var defaultStart, defaultEnd int64
	if usePromDefaults {
		defaultStart = v1.MinTime.UnixMilli()
		defaultEnd = v1.MaxTime.UnixMilli()
	}

	startVal := reqValues.Get("start")
	if startVal == "" {
		start = defaultStart
	} else {
		start, err = util.ParseTime(startVal)
		if err != nil {
			return 0, 0, DecorateWithParamName(err, "start")
		}
	}

	endVal := reqValues.Get("end")
	if endVal == "" {
		end = defaultEnd
	} else {
		end, err = util.ParseTime(endVal)
		if err != nil {
			return 0, 0, DecorateWithParamName(err, "end")
		}
	}

	if endVal != "" && end < start {
		return 0, 0, errEndBeforeStart
	}

	return start, end, err
}

func decodeQueryMinMaxTime(queryExpr parser.Expr, start, end, step int64, lookbackDelta time.Duration) (minTime, maxTime int64) {
	evalStmt := &parser.EvalStmt{
		Expr:          queryExpr,
		Start:         util.TimeFromMillis(start),
		End:           util.TimeFromMillis(end),
		Interval:      time.Duration(step) * time.Millisecond,
		LookbackDelta: lookbackDelta,
	}

	minTime, maxTime = promql.FindMinMaxTime(evalStmt)
	return minTime, maxTime
}

func decodeOptions(r *http.Request, opts *Options) {
	opts.CacheDisabled = decodeCacheDisabledOption(r)

	for _, value := range r.Header.Values(totalShardsControlHeader) {
		shards, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			continue
		}
		opts.TotalShards = int32(shards)
		if opts.TotalShards < 1 {
			opts.ShardingDisabled = true
		}
	}

	for _, value := range r.Header.Values(instantSplitControlHeader) {
		splitInterval, err := time.ParseDuration(value)
		if err != nil {
			continue
		}
		// Instant split by time interval unit stored in nanoseconds (time.Duration unit in int64)
		opts.InstantSplitInterval = splitInterval.Nanoseconds()
		if opts.InstantSplitInterval < 1 {
			opts.InstantSplitDisabled = true
		}
	}
}

func decodeCacheDisabledOption(r *http.Request) bool {
	for _, value := range r.Header.Values(cacheControlHeader) {
		if strings.Contains(value, noStoreValue) {
			return true
		}
	}

	return false
}

func (c prometheusCodec) EncodeMetricsQueryRequest(ctx context.Context, r MetricsQueryRequest) (*http.Request, error) {
	var u *url.URL
	switch r := r.(type) {
	case *PrometheusRangeQueryRequest:
		u = &url.URL{
			Path: r.GetPath(),
			RawQuery: url.Values{
				"start": []string{encodeTime(r.GetStart())},
				"end":   []string{encodeTime(r.GetEnd())},
				"step":  []string{encodeDurationMs(r.GetStep())},
				"query": []string{r.GetQuery()},
			}.Encode(),
		}
	case *PrometheusInstantQueryRequest:
		u = &url.URL{
			Path: r.GetPath(),
			RawQuery: url.Values{
				"time":  []string{encodeTime(r.GetTime())},
				"query": []string{r.GetQuery()},
			}.Encode(),
		}

	default:
		return nil, fmt.Errorf("unsupported request type %T", r)
	}

	req := &http.Request{
		Method:     "GET",
		RequestURI: u.String(), // This is what the httpgrpc code looks at.
		URL:        u,
		Body:       http.NoBody,
		Header:     http.Header{},
	}

	encodeOptions(req, r.GetOptions())

	switch c.preferredQueryResultResponseFormat {
	case formatJSON:
		req.Header.Set("Accept", jsonMimeType)
	case formatProtobuf:
		req.Header.Set("Accept", mimirpb.QueryResponseMimeType+","+jsonMimeType)
	default:
		return nil, fmt.Errorf("unknown query result response format '%s'", c.preferredQueryResultResponseFormat)
	}

	if level, ok := api.ReadConsistencyLevelFromContext(ctx); ok {
		req.Header.Add(api.ReadConsistencyHeader, level)
	}

	// Propagate allowed HTTP headers.
	for _, h := range r.GetHeaders() {
		if !slices.Contains(c.propagateHeadersMetrics, h.Name) {
			continue
		}

		for _, v := range h.Values {
			// There should only be one value, but add all of them for completeness.
			req.Header.Add(h.Name, v)
		}
	}

	return req.WithContext(ctx), nil
}

func (c prometheusCodec) EncodeLabelsQueryRequest(ctx context.Context, req LabelsQueryRequest) (*http.Request, error) {
	var u *url.URL
	switch req := req.(type) {
	case *PrometheusLabelNamesQueryRequest:
		urlValues := url.Values{}
		if req.GetStart() != 0 {
			urlValues["start"] = []string{encodeTime(req.Start)}
		}
		if req.GetEnd() != 0 {
			urlValues["end"] = []string{encodeTime(req.End)}
		}
		if len(req.GetLabelMatcherSets()) > 0 {
			urlValues["match[]"] = req.GetLabelMatcherSets()
		}
		if req.GetLimit() > 0 {
			urlValues["limit"] = []string{strconv.FormatUint(req.GetLimit(), 10)}
		}
		u = &url.URL{
			Path:     req.Path,
			RawQuery: urlValues.Encode(),
		}
	case *PrometheusLabelValuesQueryRequest:
		// repeated from PrometheusLabelNamesQueryRequest case; Go type cast switch
		// does not support accessing struct members on a typeA|typeB switch
		urlValues := url.Values{}
		if req.GetStart() != 0 {
			urlValues["start"] = []string{encodeTime(req.Start)}
		}
		if req.GetEnd() != 0 {
			urlValues["end"] = []string{encodeTime(req.End)}
		}
		if len(req.GetLabelMatcherSets()) > 0 {
			urlValues["match[]"] = req.GetLabelMatcherSets()
		}
		if req.GetLimit() > 0 {
			urlValues["limit"] = []string{strconv.FormatUint(req.GetLimit(), 10)}
		}
		u = &url.URL{
			Path:     req.Path, // path still contains label name
			RawQuery: urlValues.Encode(),
		}
	case *PrometheusSeriesQueryRequest:
		urlValues := url.Values{}
		if req.GetStart() != 0 {
			urlValues["start"] = []string{encodeTime(req.Start)}
		}
		if req.GetEnd() != 0 {
			urlValues["end"] = []string{encodeTime(req.End)}
		}
		if len(req.GetLabelMatcherSets()) > 0 {
			urlValues["match[]"] = req.GetLabelMatcherSets()
		}
		if req.GetLimit() > 0 {
			urlValues["limit"] = []string{strconv.FormatUint(req.GetLimit(), 10)}
		}
		u = &url.URL{
			Path:     req.Path,
			RawQuery: urlValues.Encode(),
		}

	default:
		return nil, fmt.Errorf("unsupported request type %T", req)
	}

	r := &http.Request{
		Method:     "GET",
		RequestURI: u.String(), // This is what the httpgrpc code looks at.
		URL:        u,
		Body:       http.NoBody,
		Header:     http.Header{},
	}

	switch c.preferredQueryResultResponseFormat {
	case formatJSON:
		r.Header.Set("Accept", jsonMimeType)
	case formatProtobuf:
		r.Header.Set("Accept", mimirpb.QueryResponseMimeType+","+jsonMimeType)
	default:
		return nil, fmt.Errorf("unknown query result response format '%s'", c.preferredQueryResultResponseFormat)
	}

	if level, ok := api.ReadConsistencyLevelFromContext(ctx); ok {
		r.Header.Add(api.ReadConsistencyHeader, level)
	}

	// Propagate allowed HTTP headers.
	for _, h := range req.GetHeaders() {
		if !slices.Contains(c.propagateHeadersLabels, h.Name) {
			continue
		}

		for _, v := range h.Values {
			// There should only be one value, but add all of them for completeness.
			r.Header.Add(h.Name, v)
		}
	}

	return r.WithContext(ctx), nil
}

func encodeOptions(req *http.Request, o Options) {
	if o.CacheDisabled {
		req.Header.Set(cacheControlHeader, noStoreValue)
	}
	if o.ShardingDisabled {
		req.Header.Set(totalShardsControlHeader, "0")
	}
	if o.TotalShards > 0 {
		req.Header.Set(totalShardsControlHeader, strconv.Itoa(int(o.TotalShards)))
	}
	if o.InstantSplitDisabled {
		req.Header.Set(instantSplitControlHeader, "0")
	}
	if o.InstantSplitInterval > 0 {
		req.Header.Set(instantSplitControlHeader, time.Duration(o.InstantSplitInterval).String())
	}
}

func (c prometheusCodec) DecodeMetricsQueryResponse(ctx context.Context, r *http.Response, _ MetricsQueryRequest, logger log.Logger) (Response, error) {
	spanlog := spanlogger.FromContext(ctx, logger)
	buf, err := readResponseBody(r)
	if err != nil {
		return nil, spanlog.Error(err)
	}

	spanlog.LogFields(otlog.String("message", "ParseQueryRangeResponse"),
		otlog.Int("status_code", r.StatusCode),
		otlog.Int("bytes", len(buf)))

	// Before attempting to decode a response based on the content type, check if the
	// Content-Type header was even set. When the scheduler returns gRPC errors, they
	// are encoded as httpgrpc.HTTPResponse objects with an HTTP status code and the
	// error message as the body of the response with no content type. We need to handle
	// that case here before we decode well-formed success or error responses.
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		switch r.StatusCode {
		case http.StatusServiceUnavailable:
			return nil, apierror.New(apierror.TypeUnavailable, string(buf))
		case http.StatusTooManyRequests:
			return nil, apierror.New(apierror.TypeTooManyRequests, string(buf))
		case http.StatusRequestEntityTooLarge:
			return nil, apierror.New(apierror.TypeTooLargeEntry, string(buf))
		default:
			if r.StatusCode/100 == 5 {
				return nil, apierror.New(apierror.TypeInternal, string(buf))
			}
		}
	}

	formatter := findFormatter(contentType)
	if formatter == nil {
		return nil, apierror.Newf(apierror.TypeInternal, "unknown response content type '%v'", contentType)
	}

	start := time.Now()
	resp, err := formatter.DecodeQueryResponse(buf)
	if err != nil {
		return nil, apierror.Newf(apierror.TypeInternal, "error decoding response: %v", err)
	}

	c.metrics.duration.WithLabelValues(operationDecode, formatter.Name()).Observe(time.Since(start).Seconds())
	c.metrics.size.WithLabelValues(operationDecode, formatter.Name()).Observe(float64(len(buf)))

	if resp.Status == statusError {
		return nil, apierror.New(apierror.Type(resp.ErrorType), resp.Error)
	}

	for h, hv := range r.Header {
		resp.Headers = append(resp.Headers, &PrometheusHeader{Name: h, Values: hv})
	}
	return resp, nil
}

func (c prometheusCodec) DecodeLabelsQueryResponse(ctx context.Context, r *http.Response, lr LabelsQueryRequest, logger log.Logger) (Response, error) {
	spanlog := spanlogger.FromContext(ctx, logger)
	buf, err := readResponseBody(r)
	if err != nil {
		return nil, spanlog.Error(err)
	}

	spanlog.LogFields(otlog.String("message", "ParseQueryRangeResponse"),
		otlog.Int("status_code", r.StatusCode),
		otlog.Int("bytes", len(buf)))

	// Before attempting to decode a response based on the content type, check if the
	// Content-Type header was even set. When the scheduler returns gRPC errors, they
	// are encoded as httpgrpc.HTTPResponse objects with an HTTP status code and the
	// error message as the body of the response with no content type. We need to handle
	// that case here before we decode well-formed success or error responses.
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		switch r.StatusCode {
		case http.StatusServiceUnavailable:
			return nil, apierror.New(apierror.TypeUnavailable, string(buf))
		case http.StatusTooManyRequests:
			return nil, apierror.New(apierror.TypeTooManyRequests, string(buf))
		case http.StatusRequestEntityTooLarge:
			return nil, apierror.New(apierror.TypeTooLargeEntry, string(buf))
		default:
			if r.StatusCode/100 == 5 {
				return nil, apierror.New(apierror.TypeInternal, string(buf))
			}
		}
	}

	formatter := findFormatter(contentType)
	if formatter == nil {
		return nil, apierror.Newf(apierror.TypeInternal, "unknown response content type '%v'", contentType)
	}

	start := time.Now()

	var response Response

	switch lr.(type) {
	case *PrometheusLabelNamesQueryRequest, *PrometheusLabelValuesQueryRequest:
		resp, err := formatter.DecodeLabelsResponse(buf)
		if err != nil {
			return nil, apierror.Newf(apierror.TypeInternal, "error decoding response: %v", err)
		}

		c.metrics.duration.WithLabelValues(operationDecode, formatter.Name()).Observe(time.Since(start).Seconds())
		c.metrics.size.WithLabelValues(operationDecode, formatter.Name()).Observe(float64(len(buf)))

		if resp.Status == statusError {
			return nil, apierror.New(apierror.Type(resp.ErrorType), resp.Error)
		}

		for h, hv := range r.Header {
			resp.Headers = append(resp.Headers, &PrometheusHeader{Name: h, Values: hv})
		}

		response = resp
	case *PrometheusSeriesQueryRequest:
		resp, err := formatter.DecodeSeriesResponse(buf)
		if err != nil {
			return nil, apierror.Newf(apierror.TypeInternal, "error decoding response: %v", err)
		}

		c.metrics.duration.WithLabelValues(operationDecode, formatter.Name()).Observe(time.Since(start).Seconds())
		c.metrics.size.WithLabelValues(operationDecode, formatter.Name()).Observe(float64(len(buf)))

		if resp.Status == statusError {
			return nil, apierror.New(apierror.Type(resp.ErrorType), resp.Error)
		}

		for h, hv := range r.Header {
			resp.Headers = append(resp.Headers, &PrometheusHeader{Name: h, Values: hv})
		}

		response = resp
	default:
		return nil, apierror.Newf(apierror.TypeInternal, "unsupported request type %T", lr)
	}
	return response, nil
}

func findFormatter(contentType string) formatter {
	for _, f := range knownFormats {
		if f.ContentType().String() == contentType {
			return f
		}
	}

	return nil
}

func (c prometheusCodec) EncodeMetricsQueryResponse(ctx context.Context, req *http.Request, res Response) (*http.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "APIResponse.ToHTTPResponse")
	defer sp.Finish()

	a, ok := res.(*PrometheusResponse)
	if !ok {
		return nil, apierror.Newf(apierror.TypeInternal, "invalid response format")
	}
	if a.Data != nil {
		sp.LogFields(otlog.Int("series", len(a.Data.Result)))
	}

	selectedContentType, formatter := c.negotiateContentType(req.Header.Get("Accept"))
	if formatter == nil {
		return nil, apierror.New(apierror.TypeNotAcceptable, "none of the content types in the Accept header are supported")
	}

	start := time.Now()
	b, err := formatter.EncodeQueryResponse(a)
	if err != nil {
		return nil, apierror.Newf(apierror.TypeInternal, "error encoding response: %v", err)
	}

	encodeDuration := time.Since(start)
	c.metrics.duration.WithLabelValues(operationEncode, formatter.Name()).Observe(encodeDuration.Seconds())
	c.metrics.size.WithLabelValues(operationEncode, formatter.Name()).Observe(float64(len(b)))
	sp.LogFields(otlog.Int("bytes", len(b)))

	queryStats := stats.FromContext(ctx)
	queryStats.AddEncodeTime(encodeDuration)

	resp := http.Response{
		Header: http.Header{
			"Content-Type": []string{selectedContentType},
		},
		Body:          io.NopCloser(bytes.NewBuffer(b)),
		StatusCode:    http.StatusOK,
		ContentLength: int64(len(b)),
	}
	return &resp, nil
}

func (c prometheusCodec) EncodeLabelsQueryResponse(ctx context.Context, req *http.Request, res Response, isSeriesResponse bool) (*http.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "APIResponse.ToHTTPResponse")
	defer sp.Finish()

	selectedContentType, formatter := c.negotiateContentType(req.Header.Get("Accept"))
	if formatter == nil {
		return nil, apierror.New(apierror.TypeNotAcceptable, "none of the content types in the Accept header are supported")
	}

	var start time.Time
	var b []byte

	switch isSeriesResponse {
	case false:
		a, ok := res.(*PrometheusLabelsResponse)
		if !ok {
			return nil, apierror.Newf(apierror.TypeInternal, "invalid response format")
		}
		if a.Data != nil {
			sp.LogFields(otlog.Int("labels", len(a.Data)))
		}

		start = time.Now()
		var err error
		b, err = formatter.EncodeLabelsResponse(a)
		if err != nil {
			return nil, apierror.Newf(apierror.TypeInternal, "error encoding response: %v", err)
		}
	case true:
		a, ok := res.(*PrometheusSeriesResponse)
		if !ok {
			return nil, apierror.Newf(apierror.TypeInternal, "invalid response format")
		}
		if a.Data != nil {
			sp.LogFields(otlog.Int("labels", len(a.Data)))
		}

		start = time.Now()
		var err error
		b, err = formatter.EncodeSeriesResponse(a)
		if err != nil {
			return nil, apierror.Newf(apierror.TypeInternal, "error encoding response: %v", err)
		}
	}

	c.metrics.duration.WithLabelValues(operationEncode, formatter.Name()).Observe(time.Since(start).Seconds())
	c.metrics.size.WithLabelValues(operationEncode, formatter.Name()).Observe(float64(len(b)))
	sp.LogFields(otlog.Int("bytes", len(b)))

	resp := http.Response{
		Header: http.Header{
			"Content-Type": []string{selectedContentType},
		},
		Body:          io.NopCloser(bytes.NewBuffer(b)),
		StatusCode:    http.StatusOK,
		ContentLength: int64(len(b)),
	}
	return &resp, nil
}

func (prometheusCodec) negotiateContentType(acceptHeader string) (string, formatter) {
	if acceptHeader == "" {
		return jsonMimeType, jsonFormatterInstance
	}

	for _, clause := range goautoneg.ParseAccept(acceptHeader) {
		for _, formatter := range knownFormats {
			if formatter.ContentType().Satisfies(clause) {
				return formatter.ContentType().String(), formatter
			}
		}
	}

	return "", nil
}

func matrixMerge(resps []*PrometheusResponse) []SampleStream {
	output := map[string]*SampleStream{}
	for _, resp := range resps {
		if resp.Data == nil {
			continue
		}
		for _, stream := range resp.Data.Result {
			metric := mimirpb.FromLabelAdaptersToKeyString(stream.Labels)
			existing, ok := output[metric]
			if !ok {
				existing = &SampleStream{
					Labels: stream.Labels,
				}
			}
			// We need to make sure we don't repeat samples. This causes some visualisations to be broken in Grafana.
			// The prometheus API is inclusive of start and end timestamps.
			if len(existing.Samples) > 0 && len(stream.Samples) > 0 {
				existingEndTs := existing.Samples[len(existing.Samples)-1].TimestampMs
				if existingEndTs == stream.Samples[0].TimestampMs {
					// Typically this the cases where only 1 sample point overlap,
					// so optimize with simple code.
					stream.Samples = stream.Samples[1:]
				} else if existingEndTs > stream.Samples[0].TimestampMs {
					// Overlap might be big, use heavier algorithm to remove overlap.
					stream.Samples = sliceFloatSamples(stream.Samples, existingEndTs)
				} // else there is no overlap, yay!
			}
			existing.Samples = append(existing.Samples, stream.Samples...)

			if len(existing.Histograms) > 0 && len(stream.Histograms) > 0 {
				existingEndTs := existing.Histograms[len(existing.Histograms)-1].TimestampMs
				if existingEndTs == stream.Histograms[0].TimestampMs {
					// Typically this the cases where only 1 sample point overlap,
					// so optimize with simple code.
					stream.Histograms = stream.Histograms[1:]
				} else if existingEndTs > stream.Histograms[0].TimestampMs {
					// Overlap might be big, use heavier algorithm to remove overlap.
					stream.Histograms = sliceHistogramSamples(stream.Histograms, existingEndTs)
				} // else there is no overlap, yay!
			}
			existing.Histograms = append(existing.Histograms, stream.Histograms...)

			output[metric] = existing
		}
	}

	keys := make([]string, 0, len(output))
	for key := range output {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	result := make([]SampleStream, 0, len(output))
	for _, key := range keys {
		result = append(result, *output[key])
	}

	return result
}

// sliceFloatSamples assumes given samples are sorted by timestamp in ascending order and
// return a sub slice whose first element's is the smallest timestamp that is strictly
// bigger than the given minTs. Empty slice is returned if minTs is bigger than all the
// timestamps in samples
func sliceFloatSamples(samples []mimirpb.Sample, minTs int64) []mimirpb.Sample {
	if len(samples) <= 0 || minTs < samples[0].TimestampMs {
		return samples
	}

	if len(samples) > 0 && minTs > samples[len(samples)-1].TimestampMs {
		return samples[len(samples):]
	}

	searchResult := sort.Search(len(samples), func(i int) bool {
		return samples[i].TimestampMs > minTs
	})

	return samples[searchResult:]
}

// sliceHistogramSamples assumes given samples are sorted by timestamp in ascending order and
// return a sub slice whose first element's is the smallest timestamp that is strictly
// bigger than the given minTs. Empty slice is returned if minTs is bigger than all the
// timestamps in samples
func sliceHistogramSamples(samples []mimirpb.FloatHistogramPair, minTs int64) []mimirpb.FloatHistogramPair {
	if len(samples) <= 0 || minTs < samples[0].TimestampMs {
		return samples
	}

	if len(samples) > 0 && minTs > samples[len(samples)-1].TimestampMs {
		return samples[len(samples):]
	}

	searchResult := sort.Search(len(samples), func(i int) bool {
		return samples[i].TimestampMs > minTs
	})

	return samples[searchResult:]
}

func readResponseBody(res *http.Response) ([]byte, error) {
	// Ensure we close the response Body once we've consumed it, as required by http.Response
	// specifications.
	defer res.Body.Close() // nolint:errcheck

	// Attempt to cast the response body to a Buffer and use it if possible.
	// This is because the frontend may have already read the body and buffered it.
	if buffer, ok := res.Body.(interface{ Bytes() []byte }); ok {
		return buffer.Bytes(), nil
	}
	// Preallocate the buffer with the exact size so we don't waste allocations
	// while progressively growing an initial small buffer. The buffer capacity
	// is increased by MinRead to avoid extra allocations due to how ReadFrom()
	// internally works.
	buf := bytes.NewBuffer(make([]byte, 0, res.ContentLength+bytes.MinRead))
	if _, err := buf.ReadFrom(res.Body); err != nil {
		return nil, apierror.Newf(apierror.TypeInternal, "error decoding response with status %d: %v", res.StatusCode, err)
	}
	return buf.Bytes(), nil
}

func parseDurationMs(s string) (int64, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second/time.Millisecond)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, apierror.Newf(apierror.TypeBadData, "cannot parse %q to a valid duration. It overflows int64", s)
		}
		return int64(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return int64(d) / int64(time.Millisecond/time.Nanosecond), nil
	}
	return 0, apierror.Newf(apierror.TypeBadData, "cannot parse %q to a valid duration", s)
}

func encodeTime(t int64) string {
	f := float64(t) / 1.0e3
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func encodeDurationMs(d int64) string {
	return strconv.FormatFloat(float64(d)/float64(time.Second/time.Millisecond), 'f', -1, 64)
}

func DecorateWithParamName(err error, field string) error {
	errTmpl := "invalid parameter %q: %v"
	if status, ok := grpcutil.ErrorToStatus(err); ok {
		return apierror.Newf(apierror.TypeBadData, errTmpl, field, status.Message())
	}
	return apierror.Newf(apierror.TypeBadData, errTmpl, field, err)
}
