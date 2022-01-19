// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/results_cache.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/uber/jaeger-client-go"

	"github.com/grafana/mimir/pkg/chunk/cache"
	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	// cacheControlHeader is the name of the cache control header.
	cacheControlHeader = "Cache-Control"

	// noStoreValue is the value that cacheControlHeader has if the response indicates that the results should not be cached.
	noStoreValue = "no-store"
)

// ResultsCacheConfig is the config for the results cache.
type ResultsCacheConfig struct {
	CacheConfig cache.Config `yaml:"cache"`
	Compression string       `yaml:"compression"`
}

// RegisterFlags registers flags.
func (cfg *ResultsCacheConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.CacheConfig.RegisterFlagsWithPrefix("frontend.", "", f)

	f.StringVar(&cfg.Compression, "frontend.compression", "", "Use compression in results cache. Supported values are: 'snappy' and '' (disable compression).")
}

func (cfg *ResultsCacheConfig) Validate() error {
	switch cfg.Compression {
	case "snappy", "":
		// valid
	default:
		return errors.Errorf("unsupported compression type: %s", cfg.Compression)
	}

	return cfg.CacheConfig.Validate()
}

// Extractor is used by the cache to extract a subset of a response from a cache entry.
type Extractor interface {
	// Extract extracts a subset of a response from the `start` and `end` timestamps in milliseconds in the `from` response.
	Extract(start, end int64, from Response) Response
	ResponseWithoutHeaders(resp Response) Response
}

// PrometheusResponseExtractor helps extracting specific info from Query Response.
type PrometheusResponseExtractor struct{}

// Extract extracts response for specific a range from a response.
func (PrometheusResponseExtractor) Extract(start, end int64, from Response) Response {
	promRes := from.(*PrometheusResponse)
	var data *PrometheusData
	if promRes.Data != nil {
		data = &PrometheusData{
			ResultType: promRes.Data.ResultType,
			Result:     extractMatrix(start, end, promRes.Data.Result),
		}
	}
	return &PrometheusResponse{
		Status:  promRes.Status,
		Data:    data,
		Headers: promRes.Headers,
	}
}

// ResponseWithoutHeaders is useful in caching data without headers since
// we anyways do not need headers for sending back the response so this saves some space by reducing size of the objects.
func (PrometheusResponseExtractor) ResponseWithoutHeaders(resp Response) Response {
	promRes := resp.(*PrometheusResponse)
	var data *PrometheusData
	if promRes.Data != nil {
		data = &PrometheusData{
			ResultType: promRes.Data.ResultType,
			Result:     promRes.Data.Result,
		}
	}
	return &PrometheusResponse{
		Status: promRes.Status,
		Data:   data,
	}
}

// CacheSplitter generates cache keys. This is a useful interface for downstream
// consumers who wish to implement their own strategies.
type CacheSplitter interface {
	GenerateCacheKey(userID string, r Request) string
}

// constSplitter is a utility for using a constant split interval when determining cache keys
type constSplitter time.Duration

// GenerateCacheKey generates a cache key based on the userID, Request and interval.
func (t constSplitter) GenerateCacheKey(userID string, r Request) string {
	startInterval := r.GetStart() / time.Duration(t).Milliseconds()
	stepOffset := r.GetStart() % r.GetStep()

	// Use original format for step-aligned request, so that we can use existing cached results for such requests.
	if stepOffset == 0 {
		return fmt.Sprintf("%s:%s:%d:%d", userID, r.GetQuery(), r.GetStep(), startInterval)
	}

	return fmt.Sprintf("%s:%s:%d:%d:%d", userID, r.GetQuery(), r.GetStep(), startInterval, stepOffset)
}

// shouldCacheFn checks whether the current request should go to cache
// or not. If not, just send the request to next handler.
type shouldCacheFn func(r Request) bool

// resultsCacheAlwaysEnabled is a shouldCacheFn function always returning true.
var resultsCacheAlwaysEnabled = func(_ Request) bool { return true }

// isRequestCachable says whether the request is eligible for caching.
func isRequestCachable(req Request, maxCacheTime int64, cacheUnalignedRequests bool, logger log.Logger) bool {
	// We can run with step alignment disabled because Grafana does it already. Mimir automatically aligning start and end is not
	// PromQL compatible. But this means we cannot cache queries that do not have their start and end aligned.
	if !cacheUnalignedRequests && !isRequestStepAligned(req) {
		return false
	}

	// Do not cache it at all if the query time range is more recent than the configured max cache freshness.
	if req.GetStart() > maxCacheTime {
		return false
	}

	if !isAtModifierCachable(req, maxCacheTime, logger) {
		return false
	}

	return true
}

// isResponseCachable says whether the response should be cached or not.
func isResponseCachable(r Response, logger log.Logger) bool {
	headerValues := getHeaderValuesWithName(r, cacheControlHeader)
	for _, v := range headerValues {
		if v == noStoreValue {
			level.Debug(logger).Log("msg", fmt.Sprintf("%s header in response is equal to %s, not caching the response", cacheControlHeader, noStoreValue))
			return false
		}
	}

	return true
}

var errAtModifierAfterEnd = errors.New("at modifier after end")

// isAtModifierCachable returns true if the @ modifier result
// is safe to cache.
func isAtModifierCachable(r Request, maxCacheTime int64, logger log.Logger) bool {
	// There are 2 cases when @ modifier is not safe to cache:
	//   1. When @ modifier points to time beyond the maxCacheTime.
	//   2. If the @ modifier time is > the query range end while being
	//      below maxCacheTime. In such cases if any tenant is intentionally
	//      playing with old data, we could cache empty result if we look
	//      beyond query end.
	query := r.GetQuery()
	if !strings.Contains(query, "@") {
		return true
	}
	expr, err := parser.ParseExpr(query)
	if err != nil {
		// We are being pessimistic in such cases.
		level.Warn(logger).Log("msg", "failed to parse query, considering @ modifier as not cachable", "query", query, "err", err)
		return false
	}

	// This resolves the start() and end() used with the @ modifier.
	expr = promql.PreprocessExpr(expr, timestamp.Time(r.GetStart()), timestamp.Time(r.GetEnd()))

	end := r.GetEnd()
	atModCachable := true
	parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
		switch e := n.(type) {
		case *parser.VectorSelector:
			if e.Timestamp != nil && (*e.Timestamp > end || *e.Timestamp > maxCacheTime) {
				atModCachable = false
				return errAtModifierAfterEnd
			}
		case *parser.MatrixSelector:
			ts := e.VectorSelector.(*parser.VectorSelector).Timestamp
			if ts != nil && (*ts > end || *ts > maxCacheTime) {
				atModCachable = false
				return errAtModifierAfterEnd
			}
		case *parser.SubqueryExpr:
			if e.Timestamp != nil && (*e.Timestamp > end || *e.Timestamp > maxCacheTime) {
				atModCachable = false
				return errAtModifierAfterEnd
			}
		}
		return nil
	})

	return atModCachable
}

func getHeaderValuesWithName(r Response, headerName string) (headerValues []string) {
	for _, hv := range r.GetHeaders() {
		if hv.GetName() != headerName {
			continue
		}

		headerValues = append(headerValues, hv.GetValues()...)
	}

	return
}

// mergeCacheExtentsForRequest merges the provided cache extents for the input request and returns merged extents.
// The input extents can be overlapping and are not required to be sorted.
func mergeCacheExtentsForRequest(ctx context.Context, r Request, merger Merger, extents []Extent) ([]Extent, error) {
	// Fast path.
	if len(extents) <= 1 {
		return extents, nil
	}

	sort.Slice(extents, func(i, j int) bool {
		if extents[i].Start == extents[j].Start {
			// as an optimization, for two extents starts at the same time, we
			// put bigger extent at the front of the slice, which helps
			// to reduce the amount of merge we have to do later.
			return extents[i].End > extents[j].End
		}

		return extents[i].Start < extents[j].Start
	})

	// Merge any extents - potentially overlapping
	accumulator, err := newAccumulator(extents[0])
	if err != nil {
		return nil, err
	}
	mergedExtents := make([]Extent, 0, len(extents))

	for i := 1; i < len(extents); i++ {
		if accumulator.End+r.GetStep() < extents[i].Start {
			mergedExtents, err = mergeCacheExtentsWithAccumulator(mergedExtents, accumulator)
			if err != nil {
				return nil, err
			}
			accumulator, err = newAccumulator(extents[i])
			if err != nil {
				return nil, err
			}
			continue
		}

		if accumulator.End >= extents[i].End {
			continue
		}

		accumulator.TraceId = jaegerTraceID(ctx)
		accumulator.End = extents[i].End
		currentRes, err := extents[i].toResponse()
		if err != nil {
			return nil, err
		}
		merged, err := merger.MergeResponse(accumulator.Response, currentRes)
		if err != nil {
			return nil, err
		}
		accumulator.Response = merged
	}

	return mergeCacheExtentsWithAccumulator(mergedExtents, accumulator)
}

type accumulator struct {
	Response
	Extent
}

func mergeCacheExtentsWithAccumulator(extents []Extent, acc *accumulator) ([]Extent, error) {
	any, err := types.MarshalAny(acc.Response)
	if err != nil {
		return nil, err
	}
	return append(extents, Extent{
		Start:    acc.Extent.Start,
		End:      acc.Extent.End,
		Response: any,
		TraceId:  acc.Extent.TraceId,
	}), nil
}

func newAccumulator(base Extent) (*accumulator, error) {
	res, err := base.toResponse()
	if err != nil {
		return nil, err
	}
	return &accumulator{
		Response: res,
		Extent:   base,
	}, nil
}

func toExtent(ctx context.Context, req Request, res Response) (Extent, error) {
	any, err := types.MarshalAny(res)
	if err != nil {
		return Extent{}, err
	}
	return Extent{
		Start:    req.GetStart(),
		End:      req.GetEnd(),
		Response: any,
		TraceId:  jaegerTraceID(ctx),
	}, nil
}

// partitionCacheExtents calculates the required requests to satisfy req given the cached data.
// extents must be in order by start time.
func partitionCacheExtents(req Request, extents []Extent, minCacheExtent int64, extractor Extractor) ([]Request, []Response, error) {
	var requests []Request
	var cachedResponses []Response
	start := req.GetStart()

	for _, extent := range extents {
		// If there is no overlap, ignore this extent.
		if extent.GetEnd() < start || extent.Start > req.GetEnd() {
			continue
		}

		// If this extent is tiny and request is not tiny, discard it: more efficient to do a few larger queries.
		// Hopefully tiny request can make tiny extent into not-so-tiny extent.

		// However if the step is large enough, the split_query_by_interval middleware would generate a query with same start and end.
		// For example, if the step size is more than 12h and the interval is 24h.
		// This means the extent's start and end time would be same, even if the timerange covers several hours.
		if (req.GetStart() != req.GetEnd()) && (req.GetEnd()-req.GetStart() > minCacheExtent) && (extent.End-extent.Start < minCacheExtent) {
			continue
		}

		// If there is a bit missing at the front, make a request for that.
		if start < extent.Start {
			r := req.WithStartEnd(start, extent.Start)
			requests = append(requests, r)
		}
		res, err := extent.toResponse()
		if err != nil {
			return nil, nil, err
		}
		// extract the overlap from the cached extent.
		cachedResponses = append(cachedResponses, extractor.Extract(start, req.GetEnd(), res))

		// We want next request to start where extent ends, but we must make sure that
		// next start also has the same offset into the step as original request had, ie.
		// "start % req.Step" must be the same as "req.GetStart() % req.GetStep()".
		// We do that by computing "adjustment". Go's % operator is a "remainder" operator
		// and not "modulo" operator, which means it returns negative numbers in our case or zero
		// (because request.GetStart <= extent.End), and we need to adjust it by one step forward.
		// We don't do adjustments if extent.End is already on the same step-offset as request.Start,
		// although technically we could. But existing unit tests expect existing behaviour.

		adjust := (req.GetStart() - extent.End) % req.GetStep()
		if adjust < 0 {
			adjust += req.GetStep()
		}
		start = extent.End + adjust
	}

	// Lastly, make a request for any data missing at the end.
	if start < req.GetEnd() {
		r := req.WithStartEnd(start, req.GetEnd())
		requests = append(requests, r)
	}

	// If start and end are the same (valid in promql), start == req.GetEnd() and we won't do the query.
	// But we should only do the request if we don't have a valid cached response for it.
	if req.GetStart() == req.GetEnd() && len(cachedResponses) == 0 {
		requests = append(requests, req)
	}

	return requests, cachedResponses, nil
}

func filterRecentCacheExtents(req Request, maxCacheFreshness time.Duration, extractor Extractor, extents []Extent) ([]Extent, error) {
	maxCacheTime := (int64(model.Now().Add(-maxCacheFreshness)) / req.GetStep()) * req.GetStep()
	for i := range extents {
		// Never cache data for the latest freshness period.
		if extents[i].End > maxCacheTime {
			extents[i].End = maxCacheTime
			res, err := extents[i].toResponse()
			if err != nil {
				return nil, err
			}
			extracted := extractor.Extract(extents[i].Start, maxCacheTime, res)
			any, err := types.MarshalAny(extracted)
			if err != nil {
				return nil, err
			}
			extents[i].Response = any
		}
	}
	return extents, nil
}

func jaegerTraceID(ctx context.Context) string {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return ""
	}

	spanContext, ok := span.Context().(jaeger.SpanContext)
	if !ok {
		return ""
	}

	return spanContext.TraceID().String()
}

func extractMatrix(start, end int64, matrix []SampleStream) []SampleStream {
	result := make([]SampleStream, 0, len(matrix))
	for _, stream := range matrix {
		extracted, ok := extractSampleStream(start, end, stream)
		if ok {
			result = append(result, extracted)
		}
	}
	return result
}

func extractSampleStream(start, end int64, stream SampleStream) (SampleStream, bool) {
	result := SampleStream{
		Labels:  stream.Labels,
		Samples: make([]mimirpb.Sample, 0, len(stream.Samples)),
	}
	for _, sample := range stream.Samples {
		if start <= sample.TimestampMs && sample.TimestampMs <= end {
			result.Samples = append(result.Samples, sample)
		}
	}
	if len(result.Samples) == 0 {
		return SampleStream{}, false
	}
	return result, true
}

func (e *Extent) toResponse() (Response, error) {
	msg, err := types.EmptyAny(e.Response)
	if err != nil {
		return nil, err
	}

	if err := types.UnmarshalAny(e.Response, msg); err != nil {
		return nil, err
	}

	resp, ok := msg.(Response)
	if !ok {
		return nil, fmt.Errorf("bad cached type")
	}
	return resp, nil
}
