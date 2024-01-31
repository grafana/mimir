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
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"golang.org/x/exp/slices"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var (
	errEndBeforeStart = apierror.New(apierror.TypeBadData, `invalid parameter "end": end timestamp must not be before start time`)
	errNegativeStep   = apierror.New(apierror.TypeBadData, `invalid parameter "step": zero or negative query resolution step widths are not accepted. Try a positive integer`)
	errStepTooSmall   = apierror.New(apierror.TypeBadData, "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
	allFormats        = []string{formatJSON, formatProtobuf}
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

// Codec is used to encode/decode query range requests and responses so they can be passed down to middlewares.
type Codec interface {
	Merger
	// DecodeRequest decodes a Request from an http request.
	DecodeRequest(context.Context, *http.Request) (Request, error)
	// DecodeResponse decodes a Response from an http response.
	// The original request is also passed as a parameter this is useful for implementation that needs the request
	// to merge result or build the result correctly.
	DecodeResponse(context.Context, *http.Response, Request, log.Logger) (Response, error)
	// EncodeRequest encodes a Request into an http request.
	EncodeRequest(context.Context, Request) (*http.Request, error)
	// EncodeResponse encodes a Response into an http response.
	EncodeResponse(context.Context, *http.Request, Response) (*http.Response, error)
}

// Merger is used by middlewares making multiple requests to merge back all responses into a single one.
type Merger interface {
	// MergeResponse merges responses from multiple requests into a single Response
	MergeResponse(...Response) (Response, error)
}

// Request represents a query range request that can be process by middlewares.
type Request interface {
	// GetId returns the ID of the request used by splitAndCacheMiddleware to correlate downstream requests and responses.
	GetId() int64
	// GetStart returns the start timestamp of the request in milliseconds.
	GetStart() int64
	// GetEnd returns the end timestamp of the request in milliseconds.
	GetEnd() int64
	// GetStep returns the step of the request in milliseconds.
	GetStep() int64
	// GetQuery returns the query of the request.
	GetQuery() string
	// GetOptions returns the options for the given request.
	GetOptions() Options
	// GetHints returns hints that could be optionally attached to the request to pass down the stack.
	// These hints can be used to optimize the query execution.
	GetHints() *Hints
	// WithID clones the current request with the provided ID.
	WithID(id int64) Request
	// WithStartEnd clone the current request with different start and end timestamp.
	WithStartEnd(startTime int64, endTime int64) Request
	// WithQuery clone the current request with a different query.
	WithQuery(string) Request
	// WithTotalQueriesHint adds the number of total queries to this request's Hints.
	WithTotalQueriesHint(int32) Request
	// WithEstimatedSeriesCountHint WithEstimatedCardinalityHint adds a cardinality estimate to this request's Hints.
	WithEstimatedSeriesCountHint(uint64) Request
	proto.Message
	// AddSpanTags writes information about this request to an OpenTracing span
	AddSpanTags(opentracing.Span)
}

// Response represents a query range response.
type Response interface {
	proto.Message
	// GetHeaders returns the HTTP headers in the response.
	GetHeaders() []*PrometheusResponseHeader
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
	metrics                            *prometheusCodecMetrics
	preferredQueryResultResponseFormat string
}

type formatter interface {
	EncodeResponse(resp *PrometheusResponse) ([]byte, error)
	DecodeResponse([]byte) (*PrometheusResponse, error)
	Name() string
	ContentType() v1.MIMEType
}

var jsonFormatterInstance = jsonFormatter{}

var knownFormats = []formatter{
	jsonFormatterInstance,
	protobufFormatter{},
}

func NewPrometheusCodec(registerer prometheus.Registerer, queryResultResponseFormat string) Codec {
	return prometheusCodec{
		metrics:                            newPrometheusCodecMetrics(registerer),
		preferredQueryResultResponseFormat: queryResultResponseFormat,
	}
}

func (prometheusCodec) MergeResponse(responses ...Response) (Response, error) {
	if len(responses) == 0 {
		return newEmptyPrometheusResponse(), nil
	}

	promResponses := make([]*PrometheusResponse, 0, len(responses))
	promWarningsMap := make(map[string]struct{}, 0)
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
	}

	var promWarnings []string
	for warning := range promWarningsMap {
		promWarnings = append(promWarnings, warning)
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
	}, nil
}

func (c prometheusCodec) DecodeRequest(_ context.Context, r *http.Request) (Request, error) {
	switch {
	case IsRangeQuery(r.URL.Path):
		return c.decodeRangeQueryRequest(r)
	case IsInstantQuery(r.URL.Path):
		return c.decodeInstantQueryRequest(r)
	default:
		return nil, fmt.Errorf("prometheus codec doesn't support requests to %s", r.URL.Path)
	}
}

func (prometheusCodec) decodeRangeQueryRequest(r *http.Request) (Request, error) {
	var result PrometheusRangeQueryRequest
	var err error
	result.Start, result.End, result.Step, err = DecodeRangeQueryTimeParams(r)
	if err != nil {
		return nil, err
	}

	result.Query = r.FormValue("query")
	result.Path = r.URL.Path
	decodeOptions(r, &result.Options)
	return &result, nil
}

func (c prometheusCodec) decodeInstantQueryRequest(r *http.Request) (Request, error) {
	var result PrometheusInstantQueryRequest
	var err error
	result.Time, err = DecodeInstantQueryTimeParams(r, time.Now)
	if err != nil {
		return nil, decorateWithParamName(err, "time")
	}

	result.Query = r.FormValue("query")
	result.Path = r.URL.Path
	decodeOptions(r, &result.Options)
	return &result, nil
}

// DecodeRangeQueryTimeParams encapsulates Prometheus instant query time param parsing,
// emulating the logic in prometheus/prometheus/web/api/v1#API.query_range.
func DecodeRangeQueryTimeParams(r *http.Request) (start, end, step int64, err error) {
	start, err = util.ParseTime(r.FormValue("start"))
	if err != nil {
		return 0, 0, 0, decorateWithParamName(err, "start")
	}

	end, err = util.ParseTime(r.FormValue("end"))
	if err != nil {
		return 0, 0, 0, decorateWithParamName(err, "end")
	}

	if end < start {
		return 0, 0, 0, errEndBeforeStart
	}

	step, err = parseDurationMs(r.FormValue("step"))
	if err != nil {
		return 0, 0, 0, decorateWithParamName(err, "step")
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
func DecodeInstantQueryTimeParams(r *http.Request, now func() time.Time) (int64, error) {
	time, err := util.ParseTimeParam(r, "time", now().UnixMilli())
	if err != nil {
		return 0, decorateWithParamName(err, "time")
	}
	return time, nil
}

// DecodeLabelsQueryTimeParams encapsulates Prometheus label names query time param parsing,
// emulating the logic in prometheus/prometheus/web/api/v1#API.labelNames and v1#API.labelValues.
func DecodeLabelsQueryTimeParams(r *http.Request) (start, end int64, err error) {
	start, err = util.ParseTimeParam(r, "start", v1.MinTime.UnixMilli())
	if err != nil {
		return 0, 0, decorateWithParamName(err, "start")
	}

	end, err = util.ParseTimeParam(r, "end", v1.MaxTime.UnixMilli())
	if err != nil {
		return 0, 0, decorateWithParamName(err, "end")
	}

	if end < start {
		return 0, 0, errEndBeforeStart
	}

	return start, end, nil
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

func (c prometheusCodec) EncodeRequest(ctx context.Context, r Request) (*http.Request, error) {
	var u *url.URL
	switch r := r.(type) {
	case *PrometheusRangeQueryRequest:
		u = &url.URL{
			Path: r.Path,
			RawQuery: url.Values{
				"start": []string{encodeTime(r.Start)},
				"end":   []string{encodeTime(r.End)},
				"step":  []string{encodeDurationMs(r.Step)},
				"query": []string{r.Query},
			}.Encode(),
		}
	case *PrometheusInstantQueryRequest:
		u = &url.URL{
			Path: r.Path,
			RawQuery: url.Values{
				"time":  []string{encodeTime(r.Time)},
				"query": []string{r.Query},
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

	if consistency, ok := api.ReadConsistencyFromContext(ctx); ok {
		req.Header.Add(api.ReadConsistencyHeader, consistency)
	}

	return req.WithContext(ctx), nil
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

func (c prometheusCodec) DecodeResponse(ctx context.Context, r *http.Response, _ Request, logger log.Logger) (Response, error) {
	switch r.StatusCode {
	case http.StatusServiceUnavailable:
		return nil, apierror.New(apierror.TypeUnavailable, string(mustReadResponseBody(r)))
	case http.StatusTooManyRequests:
		return nil, apierror.New(apierror.TypeTooManyRequests, string(mustReadResponseBody(r)))
	case http.StatusRequestEntityTooLarge:
		return nil, apierror.New(apierror.TypeTooLargeEntry, string(mustReadResponseBody(r)))
	default:
		if r.StatusCode/100 == 5 {
			return nil, apierror.New(apierror.TypeInternal, string(mustReadResponseBody(r)))
		}
	}

	log := spanlogger.FromContext(ctx, logger)

	buf, err := readResponseBody(r)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.LogFields(otlog.String("message", "ParseQueryRangeResponse"),
		otlog.Int("status_code", r.StatusCode),
		otlog.Int("bytes", len(buf)))

	contentType := r.Header.Get("Content-Type")
	formatter := findFormatter(contentType)
	if formatter == nil {
		return nil, apierror.Newf(apierror.TypeInternal, "unknown response content type '%v'", contentType)
	}

	start := time.Now()
	resp, err := formatter.DecodeResponse(buf)
	if err != nil {
		return nil, apierror.Newf(apierror.TypeInternal, "error decoding response: %v", err)
	}

	c.metrics.duration.WithLabelValues(operationDecode, formatter.Name()).Observe(time.Since(start).Seconds())
	c.metrics.size.WithLabelValues(operationDecode, formatter.Name()).Observe(float64(len(buf)))

	if resp.Status == statusError {
		return nil, apierror.New(apierror.Type(resp.ErrorType), resp.Error)
	}

	for h, hv := range r.Header {
		resp.Headers = append(resp.Headers, &PrometheusResponseHeader{Name: h, Values: hv})
	}
	return resp, nil
}

func findFormatter(contentType string) formatter {
	for _, f := range knownFormats {
		if f.ContentType().String() == contentType {
			return f
		}
	}

	return nil
}

func (c prometheusCodec) EncodeResponse(ctx context.Context, req *http.Request, res Response) (*http.Response, error) {
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
	b, err := formatter.EncodeResponse(a)
	if err != nil {
		return nil, apierror.Newf(apierror.TypeInternal, "error encoding response: %v", err)
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
			metric := mimirpb.FromLabelAdaptersToLabels(stream.Labels).String()
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

func mustReadResponseBody(r *http.Response) []byte {
	body, _ := readResponseBody(r)
	return body
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

func decorateWithParamName(err error, field string) error {
	errTmpl := "invalid parameter %q: %v"
	if status, ok := grpcutil.ErrorToStatus(err); ok {
		return apierror.Newf(apierror.TypeBadData, errTmpl, field, status.Message())
	}
	return apierror.Newf(apierror.TypeBadData, errTmpl, field, err)
}
