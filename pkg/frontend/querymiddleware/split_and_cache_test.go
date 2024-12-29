// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/user"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/mimirpb_custom"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util"
)

const resultsCacheTTL = 24 * time.Hour
const resultsCacheLowerTTL = 10 * time.Minute

func TestSplitAndCacheMiddleware_SplitByInterval(t *testing.T) {
	var (
		dayOneStartTime   = parseTimeRFC3339(t, "2021-10-14T00:00:00Z")
		dayTwoEndTime     = parseTimeRFC3339(t, "2021-10-15T23:59:59Z")
		dayThreeStartTime = parseTimeRFC3339(t, "2021-10-16T00:00:00Z")
		dayFourEndTime    = parseTimeRFC3339(t, "2021-10-17T23:59:59Z")
		queryURL          = mockQueryRangeURL(dayOneStartTime, dayFourEndTime, `{__name__=~".+"}`)
		seriesLabels      = []mimirpb_custom.LabelAdapter{{Name: "__name__", Value: "test_metric"}}

		// Mock the downstream responses.
		firstDayDownstreamResponse = jsonEncodePrometheusResponse(t,
			mockPrometheusResponseSingleSeries(seriesLabels, mimirpb.Sample{TimestampMs: dayOneStartTime.Unix() * 1000, Value: 10}))

		secondDayDownstreamResponse = jsonEncodePrometheusResponse(t,
			mockPrometheusResponseSingleSeries(seriesLabels, mimirpb.Sample{TimestampMs: dayTwoEndTime.Unix() * 1000, Value: 20}))

		thirdDayHistogram = mimirpb.FloatHistogram{
			CounterResetHint: histogram.GaugeType,
			Schema:           3,
			ZeroThreshold:    1.23,
			ZeroCount:        456,
			Count:            9001,
			Sum:              789.1,
			PositiveSpans: []mimirpb.BucketSpan{
				{Offset: 4, Length: 1},
				{Offset: 3, Length: 2},
			},
			NegativeSpans: []mimirpb.BucketSpan{
				{Offset: 7, Length: 3},
				{Offset: 9, Length: 1},
			},
			PositiveBuckets: []float64{100, 200, 300},
			NegativeBuckets: []float64{400, 500, 600, 700},
		}

		thirdDayDownstreamResponse = protobufEncodePrometheusResponse(t,
			mockProtobufResponseWithSamplesAndHistograms(seriesLabels, nil, []mimirpb.FloatHistogramPair{
				{
					TimestampMs: dayThreeStartTime.Unix() * 1000,
					Histogram:   &thirdDayHistogram,
				},
			}))

		fourthDayHistogram = mimirpb.FloatHistogram{
			CounterResetHint: histogram.GaugeType,
			Schema:           3,
			ZeroThreshold:    1.23,
			ZeroCount:        456,
			Count:            9001,
			Sum:              100789.1,
			PositiveSpans: []mimirpb.BucketSpan{
				{Offset: 4, Length: 1},
				{Offset: 3, Length: 2},
			},
			NegativeSpans: []mimirpb.BucketSpan{
				{Offset: 7, Length: 3},
				{Offset: 9, Length: 1},
			},
			PositiveBuckets: []float64{100, 200, 300},
			NegativeBuckets: []float64{400, 500, 600, 700},
		}

		fourthDayDownstreamResponse = protobufEncodePrometheusResponse(t,
			mockProtobufResponseWithSamplesAndHistograms(seriesLabels, nil, []mimirpb.FloatHistogramPair{
				{
					TimestampMs: dayFourEndTime.Unix() * 1000,
					Histogram:   &fourthDayHistogram,
				},
			}))

		// Build the expected response (which is the merge of the two downstream responses).
		expectedResponse = jsonEncodePrometheusResponse(t, mockPrometheusResponseWithSamplesAndHistograms(seriesLabels,
			[]mimirpb.Sample{
				{TimestampMs: dayOneStartTime.Unix() * 1000, Value: 10},
				{TimestampMs: dayTwoEndTime.Unix() * 1000, Value: 20},
			},
			[]mimirpb.FloatHistogramPair{
				{
					TimestampMs: dayThreeStartTime.Unix() * 1000,
					Histogram:   &thirdDayHistogram,
				},
				{
					TimestampMs: dayFourEndTime.Unix() * 1000,
					Histogram:   &fourthDayHistogram,
				},
			},
		))

		codec = newTestPrometheusCodec()
	)

	var actualCount atomic.Int32
	downstreamServer := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				actualCount.Inc()

				req, err := codec.DecodeMetricsQueryRequest(r.Context(), r)
				require.NoError(t, err)

				if req.GetStart() == dayOneStartTime.Unix()*1000 {
					w.Header().Set("Content-Type", jsonMimeType)
					_, _ = w.Write([]byte(firstDayDownstreamResponse))
				} else if req.GetStart() == dayOneStartTime.Add(24*time.Hour).Unix()*1000 {
					w.Header().Set("Content-Type", jsonMimeType)
					_, _ = w.Write([]byte(secondDayDownstreamResponse))
				} else if req.GetStart() == dayThreeStartTime.Unix()*1000 {
					w.Header().Set("Content-Type", mimirpb.QueryResponseMimeType)
					_, _ = w.Write(thirdDayDownstreamResponse)
				} else if req.GetStart() == dayThreeStartTime.Add(24*time.Hour).Unix()*1000 {
					w.Header().Set("Content-Type", mimirpb.QueryResponseMimeType)
					_, _ = w.Write(fourthDayDownstreamResponse)
				} else {
					_, _ = w.Write([]byte("unexpected request"))
				}
			}),
		),
	)
	defer downstreamServer.Close()

	downstreamURL, err := url.Parse(downstreamServer.URL)
	require.NoError(t, err)

	reg := prometheus.NewPedanticRegistry()
	splitCacheMiddleware := newSplitAndCacheMiddleware(
		true,
		false, // Cache disabled.
		24*time.Hour,
		mockLimits{},
		codec,
		nil,
		nil,
		nil,
		nil,
		log.NewNopLogger(),
		reg,
	)

	// Chain middlewares together.
	middlewares := []MetricsQueryMiddleware{
		newLimitsMiddleware(mockLimits{}, log.NewNopLogger()),
		splitCacheMiddleware,
		newAssertHintsMiddleware(t, &Hints{TotalQueries: 4}),
	}

	roundtripper := newRoundTripper(singleHostRoundTripper{
		host: downstreamURL.Host,
		next: http.DefaultTransport,
	}, codec, log.NewNopLogger(), middlewares...)

	// Execute a query range request.
	req, err := http.NewRequest("GET", queryURL, http.NoBody)
	require.NoError(t, err)
	_, ctx := stats.ContextWithEmptyStats(context.Background())
	req = req.WithContext(user.InjectOrgID(ctx, "user-1"))

	resp, err := roundtripper.RoundTrip(req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)

	actualBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, expectedResponse, string(actualBody))
	require.Equal(t, int32(4), actualCount.Load())

	// Assert metrics
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_frontend_query_result_cache_attempted_total Total number of queries that were attempted to be fetched from cache.
		# TYPE cortex_frontend_query_result_cache_attempted_total counter
		cortex_frontend_query_result_cache_attempted_total 0
		# HELP cortex_frontend_query_result_cache_skipped_total Total number of times a query was not cacheable because of a reason. This metric is tracked for each partial query when time-splitting is enabled.
		# TYPE cortex_frontend_query_result_cache_skipped_total counter
		cortex_frontend_query_result_cache_skipped_total{reason="has-modifiers"} 0
		cortex_frontend_query_result_cache_skipped_total{reason="too-new"} 0
		cortex_frontend_query_result_cache_skipped_total{reason="unaligned-time-range"} 0
		# HELP cortex_frontend_split_queries_total Total number of underlying query requests after the split by interval is applied.
		# TYPE cortex_frontend_split_queries_total counter
		cortex_frontend_split_queries_total 4
		# HELP cortex_frontend_query_result_cache_hits_total Total number of requests (or partial requests) fetched from the results cache.
		# TYPE cortex_frontend_query_result_cache_hits_total counter
		cortex_frontend_query_result_cache_hits_total{request_type="query_range"} 0
		# HELP cortex_frontend_query_result_cache_requests_total Total number of requests (or partial requests) looked up in the results cache.
		# TYPE cortex_frontend_query_result_cache_requests_total counter
		cortex_frontend_query_result_cache_requests_total{request_type="query_range"} 0
	`)))

	// Assert query stats from context
	queryStats := stats.FromContext(ctx)
	assert.Equal(t, uint32(4), queryStats.LoadSplitQueries())
}

func TestSplitAndCacheMiddleware_ResultsCache(t *testing.T) {
	cacheBackend := cache.NewInstrumentedMockCache()

	reg := prometheus.NewPedanticRegistry()
	mw := newSplitAndCacheMiddleware(
		true,
		true,
		24*time.Hour,
		mockLimits{maxCacheFreshness: 10 * time.Minute, resultsCacheTTL: resultsCacheTTL, resultsCacheOutOfOrderWindowTTL: resultsCacheLowerTTL},
		newTestPrometheusCodec(),
		cacheBackend,
		DefaultCacheKeyGenerator{interval: day},
		PrometheusResponseExtractor{},
		resultsCacheAlwaysEnabled,
		log.NewNopLogger(),
		reg,
	)

	expectedResponse := &PrometheusResponse{
		Status: "success",
		Data: &PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result: []SampleStream{
				{
					Labels: []mimirpb_custom.LabelAdapter{
						{Name: "foo", Value: "bar"},
					},
					Samples: []mimirpb.Sample{
						{Value: 137, TimestampMs: 1634292000000},
						{Value: 137, TimestampMs: 1634292120000},
					},
					Histograms: []mimirpb.FloatHistogramPair{
						{
							TimestampMs: 1634292000000,
							Histogram: &mimirpb.FloatHistogram{
								CounterResetHint: histogram.GaugeType,
								Schema:           3,
								ZeroThreshold:    1.23,
								ZeroCount:        456,
								Count:            9001,
								Sum:              789.1,
								PositiveSpans: []mimirpb.BucketSpan{
									{Offset: 4, Length: 1},
									{Offset: 3, Length: 2},
								},
								NegativeSpans: []mimirpb.BucketSpan{
									{Offset: 7, Length: 3},
									{Offset: 9, Length: 1},
								},
								PositiveBuckets: []float64{100, 200, 300},
								NegativeBuckets: []float64{400, 500, 600, 700},
							},
						},
					},
				},
			},
		},
	}

	downstreamReqs := 0
	rc := mw.Wrap(HandlerFunc(func(context.Context, MetricsQueryRequest) (Response, error) {
		downstreamReqs++
		return expectedResponse, nil
	}))

	step := int64(120 * 1000)
	req := MetricsQueryRequest(&PrometheusRangeQueryRequest{
		path:      "/api/v1/query_range",
		start:     parseTimeRFC3339(t, "2021-10-15T10:00:00Z").Unix() * 1000,
		end:       parseTimeRFC3339(t, "2021-10-15T12:00:00Z").Unix() * 1000,
		step:      step,
		queryExpr: parseQuery(t, `{__name__=~".+"}`),
	})

	queryDetails, ctx := ContextWithEmptyDetails(context.Background())
	ctx = user.InjectOrgID(ctx, "1")
	resp, err := rc.Do(ctx, req)
	require.NoError(t, err)

	require.Equal(t, 1, downstreamReqs)
	require.Equal(t, expectedResponse, resp)
	assert.Equal(t, 1, cacheBackend.CountStoreCalls())
	// Assert query stats from context
	queryStats := stats.FromContext(ctx)
	assert.Equal(t, uint32(1), queryStats.LoadSplitQueries())

	assert.NotZero(t, queryDetails.ResultsCacheMissBytes)
	assert.Zero(t, queryDetails.ResultsCacheHitBytes)

	// Doing same request again shouldn't change anything.
	resp, err = rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, downstreamReqs)
	require.Equal(t, expectedResponse, resp)
	assert.Equal(t, 1, cacheBackend.CountStoreCalls())
	// Assert query stats from context
	queryStats = stats.FromContext(ctx)
	assert.Equal(t, uint32(1), queryStats.LoadSplitQueries())

	// Doing request with new end time should do one more query.
	req, err = req.WithStartEnd(req.GetStart(), req.GetEnd()+step)
	require.NoError(t, err)

	_, err = rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 2, downstreamReqs)
	assert.Equal(t, 2, cacheBackend.CountStoreCalls())
	// Assert query stats from context
	queryStats = stats.FromContext(ctx)
	assert.Equal(t, uint32(2), queryStats.LoadSplitQueries())

	// Assert metrics
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_frontend_query_result_cache_attempted_total Total number of queries that were attempted to be fetched from cache.
		# TYPE cortex_frontend_query_result_cache_attempted_total counter
		cortex_frontend_query_result_cache_attempted_total 3

		# HELP cortex_frontend_query_result_cache_skipped_total Total number of times a query was not cacheable because of a reason. This metric is tracked for each partial query when time-splitting is enabled.
		# TYPE cortex_frontend_query_result_cache_skipped_total counter
		cortex_frontend_query_result_cache_skipped_total{reason="has-modifiers"} 0
		cortex_frontend_query_result_cache_skipped_total{reason="too-new"} 0
		cortex_frontend_query_result_cache_skipped_total{reason="unaligned-time-range"} 0

		# HELP cortex_frontend_split_queries_total Total number of underlying query requests after the split by interval is applied.
		# TYPE cortex_frontend_split_queries_total counter
		cortex_frontend_split_queries_total 3

		# HELP cortex_frontend_query_result_cache_requests_total Total number of requests (or partial requests) looked up in the results cache.
		# TYPE cortex_frontend_query_result_cache_requests_total counter
		cortex_frontend_query_result_cache_requests_total{request_type="query_range"} 3

		# HELP cortex_frontend_query_result_cache_hits_total Total number of requests (or partial requests) fetched from the results cache.
		# TYPE cortex_frontend_query_result_cache_hits_total counter
		cortex_frontend_query_result_cache_hits_total{request_type="query_range"} 2
	`)))
}

func TestSplitAndCacheMiddleware_ResultsCacheNoStore(t *testing.T) {
	cacheBackend := cache.NewInstrumentedMockCache()

	reg := prometheus.NewPedanticRegistry()
	mw := newSplitAndCacheMiddleware(
		true,
		true,
		24*time.Hour,
		mockLimits{maxCacheFreshness: 10 * time.Minute, resultsCacheTTL: resultsCacheTTL, resultsCacheOutOfOrderWindowTTL: resultsCacheLowerTTL},
		newTestPrometheusCodec(),
		cacheBackend,
		DefaultCacheKeyGenerator{interval: day},
		PrometheusResponseExtractor{},
		resultsCacheAlwaysDisabled,
		log.NewNopLogger(),
		reg,
	)

	expectedResponse := &PrometheusResponse{
		Status: "success",
		Data: &PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result: []SampleStream{
				{
					Labels: []mimirpb_custom.LabelAdapter{
						{Name: "foo", Value: "bar"},
					},
					Samples: []mimirpb.Sample{
						{Value: 137, TimestampMs: 1634292000000},
						{Value: 137, TimestampMs: 1634292120000},
					},
					Histograms: []mimirpb.FloatHistogramPair{
						{
							TimestampMs: 1634292000000,
							Histogram: &mimirpb.FloatHistogram{
								CounterResetHint: histogram.GaugeType,
								Schema:           3,
								ZeroThreshold:    1.23,
								ZeroCount:        456,
								Count:            9001,
								Sum:              789.1,
								PositiveSpans: []mimirpb.BucketSpan{
									{Offset: 4, Length: 1},
									{Offset: 3, Length: 2},
								},
								NegativeSpans: []mimirpb.BucketSpan{
									{Offset: 7, Length: 3},
									{Offset: 9, Length: 1},
								},
								PositiveBuckets: []float64{100, 200, 300},
								NegativeBuckets: []float64{400, 500, 600, 700},
							},
						},
					},
				},
			},
		},
	}

	downstreamReqs := 0
	rc := mw.Wrap(HandlerFunc(func(context.Context, MetricsQueryRequest) (Response, error) {
		downstreamReqs++
		return expectedResponse, nil
	}))

	step := int64(120 * 1000)
	req := MetricsQueryRequest(&PrometheusRangeQueryRequest{
		path:      "/api/v1/query_range",
		start:     parseTimeRFC3339(t, "2021-10-15T10:00:00Z").Unix() * 1000,
		end:       parseTimeRFC3339(t, "2021-10-15T12:00:00Z").Unix() * 1000,
		step:      step,
		queryExpr: parseQuery(t, `{__name__=~".+"}`),
		options:   Options{CacheDisabled: true},
	})

	queryDetails, ctx := ContextWithEmptyDetails(context.Background())
	ctx = user.InjectOrgID(ctx, "1")
	resp, err := rc.Do(ctx, req)
	require.NoError(t, err)

	require.Equal(t, 1, downstreamReqs)
	require.Equal(t, expectedResponse, resp)
	assert.Equal(t, 0, cacheBackend.CountStoreCalls())
	// Assert query stats from context
	queryStats := stats.FromContext(ctx)
	assert.Equal(t, uint32(1), queryStats.LoadSplitQueries())

	assert.NotZero(t, queryDetails.ResultsCacheMissBytes)
	assert.Zero(t, queryDetails.ResultsCacheHitBytes)

	// Doing same request again shouldn't change anything.
	resp, err = rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 2, downstreamReqs)
	require.Equal(t, expectedResponse, resp)
	assert.Equal(t, 0, cacheBackend.CountStoreCalls())
	// Assert query stats from context
	queryStats = stats.FromContext(ctx)
	assert.Equal(t, uint32(2), queryStats.LoadSplitQueries())

	// Assert metrics
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_frontend_query_result_cache_attempted_total Total number of queries that were attempted to be fetched from cache.
		# TYPE cortex_frontend_query_result_cache_attempted_total counter
		cortex_frontend_query_result_cache_attempted_total 0

		# HELP cortex_frontend_query_result_cache_skipped_total Total number of times a query was not cacheable because of a reason. This metric is tracked for each partial query when time-splitting is enabled.
		# TYPE cortex_frontend_query_result_cache_skipped_total counter
		cortex_frontend_query_result_cache_skipped_total{reason="has-modifiers"} 0
		cortex_frontend_query_result_cache_skipped_total{reason="too-new"} 0
		cortex_frontend_query_result_cache_skipped_total{reason="unaligned-time-range"} 0

		# HELP cortex_frontend_split_queries_total Total number of underlying query requests after the split by interval is applied.
		# TYPE cortex_frontend_split_queries_total counter
		cortex_frontend_split_queries_total 2

		# HELP cortex_frontend_query_result_cache_requests_total Total number of requests (or partial requests) looked up in the results cache.
		# TYPE cortex_frontend_query_result_cache_requests_total counter
		cortex_frontend_query_result_cache_requests_total{request_type="query_range"} 0

		# HELP cortex_frontend_query_result_cache_hits_total Total number of requests (or partial requests) fetched from the results cache.
		# TYPE cortex_frontend_query_result_cache_hits_total counter
		cortex_frontend_query_result_cache_hits_total{request_type="query_range"} 0
	`)))
}

func TestSplitAndCacheMiddleware_ResultsCache_ShouldNotLookupCacheIfStepIsNotAligned(t *testing.T) {
	cacheBackend := cache.NewInstrumentedMockCache()
	reg := prometheus.NewPedanticRegistry()

	mw := newSplitAndCacheMiddleware(
		true,
		true,
		24*time.Hour,
		mockLimits{maxCacheFreshness: 10 * time.Minute},
		newTestPrometheusCodec(),
		cacheBackend,
		DefaultCacheKeyGenerator{interval: day},
		PrometheusResponseExtractor{},
		resultsCacheAlwaysEnabled,
		log.NewNopLogger(),
		reg,
	)

	expectedResponse := &PrometheusResponse{
		Status: "success",
		Data: &PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result: []SampleStream{
				{
					Labels: []mimirpb_custom.LabelAdapter{
						{Name: "foo", Value: "bar"},
					},
					Samples: []mimirpb.Sample{
						{Value: 137, TimestampMs: 1634292000000},
						{Value: 137, TimestampMs: 1634292120000},
					},
					Histograms: []mimirpb.FloatHistogramPair{
						{
							TimestampMs: 1634292000000,
							Histogram: &mimirpb.FloatHistogram{
								CounterResetHint: histogram.GaugeType,
								Schema:           3,
								ZeroThreshold:    1.23,
								ZeroCount:        456,
								Count:            9001,
								Sum:              789.1,
								PositiveSpans: []mimirpb.BucketSpan{
									{Offset: 4, Length: 1},
									{Offset: 3, Length: 2},
								},
								NegativeSpans: []mimirpb.BucketSpan{
									{Offset: 7, Length: 3},
									{Offset: 9, Length: 1},
								},
								PositiveBuckets: []float64{100, 200, 300},
								NegativeBuckets: []float64{400, 500, 600, 700},
							},
						},
					},
				},
			},
		},
	}

	downstreamReqs := 0
	rc := mw.Wrap(HandlerFunc(func(context.Context, MetricsQueryRequest) (Response, error) {
		downstreamReqs++
		return expectedResponse, nil
	}))

	req := MetricsQueryRequest(&PrometheusRangeQueryRequest{
		path:      "/api/v1/query_range",
		start:     parseTimeRFC3339(t, "2021-10-15T10:00:00Z").Unix() * 1000,
		end:       parseTimeRFC3339(t, "2021-10-15T12:00:00Z").Unix() * 1000,
		step:      13 * 1000, // Not aligned to start/end.
		queryExpr: parseQuery(t, `{__name__=~".+"}`),
	})

	_, ctx := stats.ContextWithEmptyStats(context.Background())
	ctx = user.InjectOrgID(ctx, "1")
	resp, err := rc.Do(ctx, req)
	require.NoError(t, err)

	require.Equal(t, 1, downstreamReqs)
	require.Equal(t, expectedResponse, resp)

	// Should not touch the cache at all.
	assert.Equal(t, 0, cacheBackend.CountFetchCalls())
	assert.Equal(t, 0, cacheBackend.CountStoreCalls())
	// Assert query stats from context
	queryStats := stats.FromContext(ctx)
	assert.Equal(t, uint32(1), queryStats.LoadSplitQueries())

	// Assert metrics
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_frontend_query_result_cache_attempted_total Total number of queries that were attempted to be fetched from cache.
		# TYPE cortex_frontend_query_result_cache_attempted_total counter
		cortex_frontend_query_result_cache_attempted_total 1
		# HELP cortex_frontend_query_result_cache_skipped_total Total number of times a query was not cacheable because of a reason. This metric is tracked for each partial query when time-splitting is enabled.
		# TYPE cortex_frontend_query_result_cache_skipped_total counter
		cortex_frontend_query_result_cache_skipped_total{reason="has-modifiers"} 0
		cortex_frontend_query_result_cache_skipped_total{reason="too-new"} 0
		cortex_frontend_query_result_cache_skipped_total{reason="unaligned-time-range"} 1
		# HELP cortex_frontend_split_queries_total Total number of underlying query requests after the split by interval is applied.
		# TYPE cortex_frontend_split_queries_total counter
		cortex_frontend_split_queries_total 1
		# HELP cortex_frontend_query_result_cache_hits_total Total number of requests (or partial requests) fetched from the results cache.
		# TYPE cortex_frontend_query_result_cache_hits_total counter
		cortex_frontend_query_result_cache_hits_total{request_type="query_range"} 0
		# HELP cortex_frontend_query_result_cache_requests_total Total number of requests (or partial requests) looked up in the results cache.
		# TYPE cortex_frontend_query_result_cache_requests_total counter
		cortex_frontend_query_result_cache_requests_total{request_type="query_range"} 0
	`)))
}

func TestSplitAndCacheMiddleware_ResultsCache_EnabledCachingOfStepUnalignedRequest(t *testing.T) {
	cacheBackend := cache.NewInstrumentedMockCache()

	limits := mockLimits{
		maxCacheFreshness:                    10 * time.Minute,
		resultsCacheTTL:                      resultsCacheTTL,
		resultsCacheOutOfOrderWindowTTL:      resultsCacheLowerTTL,
		resultsCacheForUnalignedQueryEnabled: true,
	}

	mw := newSplitAndCacheMiddleware(
		true,
		true,
		24*time.Hour,
		limits,
		newTestPrometheusCodec(),
		cacheBackend,
		DefaultCacheKeyGenerator{interval: day},
		PrometheusResponseExtractor{},
		resultsCacheAlwaysEnabled,
		log.NewNopLogger(),
		prometheus.NewPedanticRegistry(),
	)

	expectedResponse := &PrometheusResponse{
		Status: "success",
		Data: &PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result: []SampleStream{
				{
					Labels: []mimirpb_custom.LabelAdapter{
						{Name: "foo", Value: "bar"},
					},
					Samples: []mimirpb.Sample{
						{Value: 137, TimestampMs: 1634292000000},
						{Value: 137, TimestampMs: 1634292120000},
					},
				},
			},
		},
	}

	downstreamReqs := 0
	rc := mw.Wrap(HandlerFunc(func(context.Context, MetricsQueryRequest) (Response, error) {
		downstreamReqs++
		return expectedResponse, nil
	}))

	req := MetricsQueryRequest(&PrometheusRangeQueryRequest{
		path:      "/api/v1/query_range",
		start:     parseTimeRFC3339(t, "2021-10-15T10:00:00Z").Unix() * 1000,
		end:       parseTimeRFC3339(t, "2021-10-15T12:00:00Z").Unix() * 1000,
		step:      13 * 1000, // Not aligned to start/end.
		queryExpr: parseQuery(t, `{__name__=~".+"}`),
	})

	_, ctx := stats.ContextWithEmptyStats(context.Background())
	ctx = user.InjectOrgID(ctx, "1")
	resp, err := rc.Do(ctx, req)
	require.NoError(t, err)

	require.Equal(t, 1, downstreamReqs)
	require.Equal(t, expectedResponse, resp)

	// Since we're caching unaligned requests, we should see that.
	assert.Equal(t, 1, cacheBackend.CountFetchCalls())
	assert.Equal(t, 1, cacheBackend.CountStoreCalls())
	// Assert query stats from context
	queryStats := stats.FromContext(ctx)
	assert.Equal(t, uint32(1), queryStats.LoadSplitQueries())

	// Doing the same request reuses cached result.
	resp, err = rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, downstreamReqs)
	require.Equal(t, expectedResponse, resp)
	assert.Equal(t, 2, cacheBackend.CountFetchCalls())
	assert.Equal(t, 1, cacheBackend.CountStoreCalls())
	// Assert query stats from context
	queryStats = stats.FromContext(ctx)
	assert.Equal(t, uint32(1), queryStats.LoadSplitQueries())

	// New request with slightly different Start time will not reuse the cached result.
	req, err = req.WithStartEnd(parseTimeRFC3339(t, "2021-10-15T10:00:05Z").Unix()*1000, parseTimeRFC3339(t, "2021-10-15T12:00:05Z").Unix()*1000)
	require.NoError(t, err)

	resp, err = rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 2, downstreamReqs)
	require.Equal(t, expectedResponse, resp)

	assert.Equal(t, 3, cacheBackend.CountFetchCalls())
	assert.Equal(t, 2, cacheBackend.CountStoreCalls())
	// Assert query stats from context
	queryStats = stats.FromContext(ctx)
	assert.Equal(t, uint32(2), queryStats.LoadSplitQueries())
}

func TestSplitAndCacheMiddleware_ResultsCache_ShouldNotCacheRequestEarlierThanMaxCacheFreshness(t *testing.T) {
	const (
		maxCacheFreshness = 10 * time.Minute
		userID            = "user-1"
	)

	var (
		now              = time.Now()
		fiveMinutesAgo   = now.Add(-5 * time.Minute)
		twentyMinutesAgo = now.Add(-20 * time.Minute)
	)

	tests := map[string]struct {
		queryStartTime              time.Time
		queryEndTime                time.Time
		downstreamResponse          *PrometheusResponse
		expectedDownstreamStartTime time.Time
		expectedDownstreamEndTime   time.Time
		expectedCachedResponses     []Response
		expectedMetrics             string
	}{
		"should not cache a response if query time range is earlier than max cache freshness": {
			queryStartTime: fiveMinutesAgo,
			queryEndTime:   now,
			downstreamResponse: mockPrometheusResponseSingleSeries(
				[]mimirpb_custom.LabelAdapter{{Name: "__name__", Value: "test_metric"}},
				mimirpb.Sample{TimestampMs: fiveMinutesAgo.Unix() * 1000, Value: 10},
				mimirpb.Sample{TimestampMs: now.Unix() * 1000, Value: 20}),
			expectedDownstreamStartTime: fiveMinutesAgo,
			expectedDownstreamEndTime:   now,
			expectedCachedResponses:     nil,
			expectedMetrics: `
				# HELP cortex_frontend_query_result_cache_attempted_total Total number of queries that were attempted to be fetched from cache.
				# TYPE cortex_frontend_query_result_cache_attempted_total counter
				cortex_frontend_query_result_cache_attempted_total 2
				# HELP cortex_frontend_query_result_cache_skipped_total Total number of times a query was not cacheable because of a reason. This metric is tracked for each partial query when time-splitting is enabled.
				# TYPE cortex_frontend_query_result_cache_skipped_total counter
				cortex_frontend_query_result_cache_skipped_total{reason="has-modifiers"} 0
				cortex_frontend_query_result_cache_skipped_total{reason="too-new"} 2
				cortex_frontend_query_result_cache_skipped_total{reason="unaligned-time-range"} 0
				# HELP cortex_frontend_split_queries_total Total number of underlying query requests after the split by interval is applied.
				# TYPE cortex_frontend_split_queries_total counter
				cortex_frontend_split_queries_total 0
				# HELP cortex_frontend_query_result_cache_hits_total Total number of requests (or partial requests) fetched from the results cache.
				# TYPE cortex_frontend_query_result_cache_hits_total counter
				cortex_frontend_query_result_cache_hits_total{request_type="query_range"} 0
				# HELP cortex_frontend_query_result_cache_requests_total Total number of requests (or partial requests) looked up in the results cache.
				# TYPE cortex_frontend_query_result_cache_requests_total counter
				cortex_frontend_query_result_cache_requests_total{request_type="query_range"} 0
			`,
		},
		"should cache a response up until max cache freshness time ago": {
			queryStartTime: twentyMinutesAgo,
			queryEndTime:   now,
			downstreamResponse: mockPrometheusResponseSingleSeries(
				[]mimirpb_custom.LabelAdapter{{Name: "__name__", Value: "test_metric"}},
				mimirpb.Sample{TimestampMs: twentyMinutesAgo.Unix() * 1000, Value: 10},
				mimirpb.Sample{TimestampMs: now.Unix() * 1000, Value: 20}),
			expectedDownstreamStartTime: twentyMinutesAgo,
			expectedDownstreamEndTime:   now,
			expectedCachedResponses: []Response{
				mockPrometheusResponseSingleSeries(
					[]mimirpb_custom.LabelAdapter{{Name: "__name__", Value: "test_metric"}},
					// Any sample more recent than max cache freshness shouldn't be cached.
					mimirpb.Sample{TimestampMs: twentyMinutesAgo.Unix() * 1000, Value: 10}),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cacheBackend := cache.NewMockCache()
			keyGenerator := DefaultCacheKeyGenerator{interval: day}
			reg := prometheus.NewPedanticRegistry()

			mw := newSplitAndCacheMiddleware(
				false, // No interval splitting.
				true,
				24*time.Hour,
				mockLimits{maxCacheFreshness: maxCacheFreshness, resultsCacheTTL: resultsCacheTTL, resultsCacheOutOfOrderWindowTTL: resultsCacheLowerTTL},
				newTestPrometheusCodec(),
				cacheBackend,
				keyGenerator,
				PrometheusResponseExtractor{},
				resultsCacheAlwaysEnabled,
				log.NewNopLogger(),
				reg,
			)

			calls := 0
			rc := mw.Wrap(HandlerFunc(func(_ context.Context, r MetricsQueryRequest) (Response, error) {
				calls++

				// Check the downstream request. We only check the 1st request because the subsequent
				// requests may be on a smaller time ranges if the response has been cached previously.
				if calls == 1 {
					require.Equal(t, testData.expectedDownstreamStartTime.Unix()*1000, r.GetStart())
					require.Equal(t, testData.expectedDownstreamEndTime.Unix()*1000, r.GetEnd())
				}

				return testData.downstreamResponse, nil
			}))
			ctx := user.InjectOrgID(context.Background(), userID)

			req := MetricsQueryRequest(&PrometheusRangeQueryRequest{
				path:      "/api/v1/query_range",
				start:     testData.queryStartTime.Unix() * 1000,
				end:       testData.queryEndTime.Unix() * 1000,
				step:      1000, // 1s step so it's guaranteed to be aligned.
				queryExpr: parseQuery(t, `{__name__=~".+"}`),
			})

			// MetricsQueryRequest should result in a query.
			resp, err := rc.Do(ctx, req)
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			require.Equal(t, testData.downstreamResponse, resp)

			// Doing same request again should result in another query to fetch most recent data.
			resp, err = rc.Do(ctx, req)
			require.NoError(t, err)
			require.Equal(t, 2, calls)
			require.Equal(t, testData.downstreamResponse, resp)

			// Check if the response was cached.
			cacheKey := cacheHashKey(keyGenerator.QueryRequest(ctx, userID, req))
			found := cacheBackend.GetMulti(ctx, []string{cacheKey})

			if len(testData.expectedCachedResponses) == 0 {
				assert.Empty(t, found)
			} else {
				var actual CachedResponse
				require.Len(t, found, 1)
				require.NoError(t, proto.Unmarshal(found[cacheKey], &actual))

				// Decode all extents.
				actualCachedResponses := make([]Response, 0, len(actual.Extents))
				for _, extent := range actual.Extents {
					res, err := extent.toResponse()
					require.NoError(t, err)
					actualCachedResponses = append(actualCachedResponses, res)
				}

				assert.Equal(t, testData.expectedCachedResponses, actualCachedResponses)
			}

			if testData.expectedMetrics != "" {
				assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(testData.expectedMetrics)))
			}
		})
	}
}

func TestSplitAndCacheMiddleware_ResultsCacheFuzzy(t *testing.T) {
	const (
		numSeries  = 1000
		numQueries = 10
	)

	tests := map[string]struct {
		splitEnabled        bool
		cacheEnabled        bool
		cacheUnaligned      bool
		maxCacheFreshness   time.Duration
		maxQueryParallelism int
	}{
		"default config": {
			splitEnabled:        true,
			cacheEnabled:        true,
			cacheUnaligned:      false,
			maxCacheFreshness:   time.Minute,
			maxQueryParallelism: 14,
		},
		"reduced query parallelism": {
			splitEnabled:        true,
			cacheEnabled:        true,
			cacheUnaligned:      false,
			maxCacheFreshness:   time.Minute,
			maxQueryParallelism: 1,
		},
		"cache unaligned requests": {
			splitEnabled:        true,
			cacheEnabled:        true,
			cacheUnaligned:      true,
			maxCacheFreshness:   time.Minute,
			maxQueryParallelism: 14,
		},
		"increased max cache freshness": {
			splitEnabled:        true,
			cacheEnabled:        true,
			cacheUnaligned:      true,
			maxCacheFreshness:   time.Hour,
			maxQueryParallelism: 14,
		},
		"split by interval disabled": {
			splitEnabled:        false,
			cacheEnabled:        true,
			maxCacheFreshness:   time.Minute,
			maxQueryParallelism: 14,
		},
		"results cache disabled": {
			splitEnabled:        true,
			cacheEnabled:        false,
			maxCacheFreshness:   time.Minute,
			maxQueryParallelism: 14,
		},
		"both split by interval and results cache disabled": {
			splitEnabled:        false,
			cacheEnabled:        false,
			maxCacheFreshness:   time.Minute,
			maxQueryParallelism: 14,
		},
	}

	ctx := user.InjectOrgID(context.Background(), "1")

	// Randomise the seed but log it in case we need to reproduce the test on failure.
	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	t.Log("random generator seed:", seed)

	// The mocked storage contains samples within the following min/max time.
	minTime := parseTimeRFC3339(t, "2021-10-13T00:00:00Z")
	maxTime := parseTimeRFC3339(t, "2021-10-15T23:09:09Z")
	step := 2 * time.Minute

	// Generate series.
	series := make([]storage.Series, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		series = append(series, newSeries(newTestCounterLabels(i), minTime, maxTime, step, factor(float64(i))))
	}

	// Create a queryable on the fixtures.
	queryable := storageSeriesQueryable(series)

	// Create a downstream handler serving range queries based on the provided queryable.
	downstream := &downstreamHandler{
		engine:    newEngine(),
		queryable: queryable,
	}

	// Generate some random requests.
	reqs := make([]MetricsQueryRequest, 0, numQueries)
	for q := 0; q < numQueries; q++ {
		// Generate a random time range within min/max time.
		startTime := minTime.Add(time.Duration(rnd.Int63n(maxTime.Sub(minTime).Milliseconds())) * time.Millisecond)
		endTime := startTime.Add(time.Duration(rnd.Int63n(maxTime.Sub(startTime).Milliseconds())) * time.Millisecond)

		reqs = append(reqs, &PrometheusRangeQueryRequest{
			id:        int64(q),
			path:      "/api/v1/query_range",
			start:     startTime.Unix() * 1000,
			end:       endTime.Unix() * 1000,
			step:      120 * 1000,
			queryExpr: parseQuery(t, fmt.Sprintf(`sum by(group_2) (rate({__name__=~".+"}[%s]))`, (2*step).String())),
		})
	}

	// Run the query without the split and cache middleware and store it as expected result.
	expectedResMx := sync.Mutex{}
	expectedRes := make(map[int64]Response, len(reqs))
	require.NoError(t, concurrency.ForEachJob(ctx, len(reqs), len(reqs), func(ctx context.Context, idx int) error {
		res, err := downstream.Do(ctx, reqs[idx])
		if err != nil {
			return err
		}

		expectedResMx.Lock()
		expectedRes[reqs[idx].GetID()] = res
		expectedResMx.Unlock()

		return nil
	}))

	for testName, testData := range tests {
		for _, maxConcurrency := range []int{1, numQueries} {
			t.Run(fmt.Sprintf("%s (concurrency: %d)", testName, maxConcurrency), func(t *testing.T) {
				t.Parallel()

				mw := newSplitAndCacheMiddleware(
					testData.splitEnabled,
					testData.cacheEnabled,
					24*time.Hour,
					mockLimits{
						maxCacheFreshness:   testData.maxCacheFreshness,
						maxQueryParallelism: testData.maxQueryParallelism,
					},
					newTestPrometheusCodec(),
					cache.NewMockCache(),
					DefaultCacheKeyGenerator{interval: day},
					PrometheusResponseExtractor{},
					resultsCacheAlwaysEnabled,
					log.NewNopLogger(),
					prometheus.NewPedanticRegistry(),
				).Wrap(downstream)

				// Run requests honoring concurrency.
				require.NoError(t, concurrency.ForEachJob(ctx, len(reqs), maxConcurrency, func(ctx context.Context, idx int) error {
					actual, err := mw.Do(ctx, reqs[idx])
					require.NoError(t, err)
					require.Equal(t, expectedRes[reqs[idx].GetID()], actual)

					return nil
				}))
			})
		}
	}
}

func TestSplitAndCacheMiddleware_ResultsCache_ExtentsEdgeCases(t *testing.T) {
	const userID = "user-1"

	now := time.Now().UnixMilli()
	tests := map[string]struct {
		req                    MetricsQueryRequest
		cachedExtents          []Extent
		expectedUpdatedExtents bool
		expectedCachedExtents  []Extent
	}{
		"Should drop tiny extent that overlaps with non-tiny request only": {
			req: &PrometheusRangeQueryRequest{
				start:     100,
				end:       120,
				step:      5,
				queryExpr: parseQuery(t, "foo"),
			},
			cachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(0, 50, 5, now),
				mkExtentWithStepAndQueryTime(60, 65, 5, now),
				mkExtentWithStepAndQueryTime(100, 105, 5, now), // dropped
				mkExtentWithStepAndQueryTime(110, 150, 5, now),
				mkExtentWithStepAndQueryTime(160, 165, 5, now),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(0, 50, 5, now),
				mkExtentWithStepAndQueryTime(60, 65, 5, now),
				mkExtentWithStepAndQueryTime(100, 150, 5, now),
				mkExtentWithStepAndQueryTime(160, 165, 5, now),
			},
		},
		"Should replace tiny extents that are covered by bigger request": {
			req: &PrometheusRangeQueryRequest{
				start:     100,
				end:       200,
				step:      5,
				queryExpr: parseQuery(t, "foo"),
			},
			cachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(0, 50, 5, now-10),
				mkExtentWithStepAndQueryTime(60, 65, 5, now-20),
				mkExtentWithStepAndQueryTime(100, 105, 5, now-30),
				mkExtentWithStepAndQueryTime(110, 115, 5, now-40),
				mkExtentWithStepAndQueryTime(120, 125, 5, now-50),
				mkExtentWithStepAndQueryTime(220, 225, 5, now-60),
				mkExtentWithStepAndQueryTime(240, 250, 5, now-70),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(0, 50, 5, now-10),
				mkExtentWithStepAndQueryTime(60, 65, 5, now-20),
				mkExtentWithStepAndQueryTime(100, 200, 5, now-50),
				mkExtentWithStepAndQueryTime(220, 225, 5, now-60),
				mkExtentWithStepAndQueryTime(240, 250, 5, now-70),
			},
		},
		"Should not drop tiny extent that completely overlaps with tiny request": {
			req: &PrometheusRangeQueryRequest{
				start:     100,
				end:       105,
				step:      5,
				queryExpr: parseQuery(t, "foo"),
			},
			cachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(0, 50, 5, now),
				mkExtentWithStepAndQueryTime(60, 65, 5, now),
				mkExtentWithStepAndQueryTime(100, 105, 5, now),
				mkExtentWithStepAndQueryTime(160, 165, 5, now),
			},
			// No cache update need, request fulfilled using cache
			expectedUpdatedExtents: false,
			// We expect the same extents in the cache (no changes).
			expectedCachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(0, 50, 5, now),
				mkExtentWithStepAndQueryTime(60, 65, 5, now),
				mkExtentWithStepAndQueryTime(100, 105, 5, now),
				mkExtentWithStepAndQueryTime(160, 165, 5, now),
			},
		},
		"Should not drop tiny extent that partially center-overlaps with tiny request": {
			req: &PrometheusRangeQueryRequest{
				start:     106,
				end:       108,
				step:      2,
				queryExpr: parseQuery(t, "foo"),
			},
			cachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 64, 2, now),
				mkExtentWithStepAndQueryTime(104, 110, 2, now),
				mkExtentWithStepAndQueryTime(160, 166, 2, now),
			},
			// No cache update need, request fulfilled using cache
			expectedUpdatedExtents: false,
			// We expect the same extents in the cache (no changes).
			expectedCachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 64, 2, now),
				mkExtentWithStepAndQueryTime(104, 110, 2, now),
				mkExtentWithStepAndQueryTime(160, 166, 2, now),
			},
		},
		"Should not drop tiny extent that partially left-overlaps with tiny request": {
			req: &PrometheusRangeQueryRequest{
				start:     100,
				end:       106,
				step:      2,
				queryExpr: parseQuery(t, "foo"),
			},
			cachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 64, 2, now),
				mkExtentWithStepAndQueryTime(104, 110, 2, now-100),
				mkExtentWithStepAndQueryTime(160, 166, 2, now),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 64, 2, now),
				mkExtentWithStepAndQueryTime(100, 110, 2, now-100),
				mkExtentWithStepAndQueryTime(160, 166, 2, now),
			},
		},
		"Should not drop tiny extent that partially right-overlaps with tiny request": {
			req: &PrometheusRangeQueryRequest{
				start:     100,
				end:       106,
				step:      2,
				queryExpr: parseQuery(t, "foo"),
			},
			cachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 64, 2, now),
				mkExtentWithStepAndQueryTime(98, 102, 2, now-100),
				mkExtentWithStepAndQueryTime(160, 166, 2, now),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 64, 2, now),
				mkExtentWithStepAndQueryTime(98, 106, 2, now-100),
				mkExtentWithStepAndQueryTime(160, 166, 2, now),
			},
		},
		"Should merge fragmented extents if request fills the hole": {
			req: &PrometheusRangeQueryRequest{
				start:     40,
				end:       80,
				step:      20,
				queryExpr: parseQuery(t, "foo"),
			},
			cachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(0, 20, 20, now-100),
				mkExtentWithStepAndQueryTime(80, 100, 20, now-200),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(0, 100, 20, now-200),
			},
		},
		"Should left-extend extent if request starts earlier than extent in cache": {
			req: &PrometheusRangeQueryRequest{
				start:     40,
				end:       80,
				step:      20,
				queryExpr: parseQuery(t, "foo"),
			},
			cachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 160, 20, now-100),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(40, 160, 20, now-100),
			},
		},
		"Should left-extend extent if request starts earlier than extent in cache, but keep oldest time": {
			req: &PrometheusRangeQueryRequest{
				start:     40,
				end:       80,
				step:      20,
				queryExpr: parseQuery(t, "foo"),
			},
			cachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 160, 20, now+100),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				// "now" is also used as query time during the test, and this is lower than "now+100"
				mkExtentWithStepAndQueryTime(40, 160, 20, now),
			},
		},
		"Should left-extend extent with zero query timestamp if request starts earlier than extent in cache and use recent query timestamp": {
			req: &PrometheusRangeQueryRequest{
				start:     40,
				end:       80,
				step:      20,
				queryExpr: parseQuery(t, "foo"),
			},
			cachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 160, 20, 0),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(40, 160, 20, now),
			},
		},
		"Should right-extend extent if request ends later than extent in cache": {
			req: &PrometheusRangeQueryRequest{
				start:     100,
				end:       180,
				step:      20,
				queryExpr: parseQuery(t, "foo"),
			},
			cachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 160, 20, now-100),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 180, 20, now-100),
			},
		},
		"Should right-extend extent with zero query timestamp if request ends later than extent in cache": {
			req: &PrometheusRangeQueryRequest{
				start:     100,
				end:       180,
				step:      20,
				queryExpr: parseQuery(t, "foo"),
			},
			cachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 160, 20, 0),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 180, 20, now),
			},
		},
		"Should not throw error if complete-overlapped smaller Extent is erroneous": {
			req: &PrometheusRangeQueryRequest{
				// This request is carefully crafted such that cachedEntry is not used to fulfill
				// the request.
				start:     160,
				end:       180,
				step:      20,
				queryExpr: parseQuery(t, "foo"),
			},
			cachedExtents: []Extent{
				{
					Start: 60,
					End:   80,

					// if the optimization of "sorting by End when Start of 2 Extents are equal" is not there, this nil
					// response would cause error during Extents merge phase. With the optimization
					// this bad Extent should be dropped. The good Extent below can be used instead.
					Response: nil,
				},
				mkExtentWithStepAndQueryTime(60, 160, 20, now-100),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStepAndQueryTime(60, 180, 20, now-100),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), userID)
			cacheBackend := cache.NewInstrumentedMockCache()
			keyGenerator := DefaultCacheKeyGenerator{interval: day}

			mw := newSplitAndCacheMiddleware(
				false, // No splitting.
				true,
				24*time.Hour,
				mockLimits{resultsCacheTTL: resultsCacheTTL, resultsCacheOutOfOrderWindowTTL: resultsCacheLowerTTL},
				newTestPrometheusCodec(),
				cacheBackend,
				keyGenerator,
				PrometheusResponseExtractor{},
				resultsCacheAlwaysEnabled,
				log.NewNopLogger(),
				prometheus.NewPedanticRegistry(),
			).Wrap(HandlerFunc(func(_ context.Context, req MetricsQueryRequest) (Response, error) {
				return mkAPIResponse(req.GetStart(), req.GetEnd(), req.GetStep()), nil
			})).(*splitAndCacheMiddleware)
			mw.currentTime = func() time.Time { return time.UnixMilli(now) }

			// Store all extents fixtures in the cache.
			cacheKey := keyGenerator.QueryRequest(ctx, userID, testData.req)
			mw.storeCacheExtents(cacheKey, []string{userID}, testData.cachedExtents)

			// Run the request.
			actualRes, err := mw.Do(ctx, testData.req)
			require.NoError(t, err)

			expectedResponse := mkAPIResponse(testData.req.GetStart(), testData.req.GetEnd(), testData.req.GetStep())
			assert.Equal(t, expectedResponse, actualRes)

			// Check the updated cached extents.
			actualExtents := mw.fetchCacheExtents(ctx, time.UnixMilli(now), []string{userID}, []string{cacheKey})
			require.Len(t, actualExtents, 1)
			assert.Equal(t, testData.expectedCachedExtents, actualExtents[0])

			// We always call Store() once to set the fixtures.
			if testData.expectedUpdatedExtents {
				assert.Equal(t, 2, cacheBackend.CountStoreCalls())
			} else {
				assert.Equal(t, 1, cacheBackend.CountStoreCalls())
			}
		})
	}
}

func TestSplitAndCacheMiddleware_StoreAndFetchCacheExtents(t *testing.T) {
	cacheBackend := cache.NewMockCache()
	mw := newSplitAndCacheMiddleware(
		false,
		true,
		24*time.Hour,
		mockLimits{
			resultsCacheTTL:                 1 * time.Hour,
			resultsCacheOutOfOrderWindowTTL: 10 * time.Minute,
			outOfOrderTimeWindow:            30 * time.Minute,
		},
		newTestPrometheusCodec(),
		cacheBackend,
		DefaultCacheKeyGenerator{interval: day},
		PrometheusResponseExtractor{},
		resultsCacheAlwaysEnabled,
		log.NewNopLogger(),
		prometheus.NewPedanticRegistry(),
	).Wrap(nil).(*splitAndCacheMiddleware)

	ctx := context.Background()

	t.Run("fetchCacheExtents() should return a slice with the same number of input keys but empty extents on cache miss", func(t *testing.T) {
		actual := mw.fetchCacheExtents(ctx, time.Now(), []string{"tenant"}, []string{"key-1", "key-2", "key-3"})
		expected := [][]Extent{nil, nil, nil}
		assert.Equal(t, expected, actual)
	})

	t.Run("fetchCacheExtents() should return a slice with the same number of input keys and some extends filled up on partial cache hit", func(t *testing.T) {
		mw.storeCacheExtents("key-1", []string{"tenant"}, []Extent{mkExtent(10, 20)})
		mw.storeCacheExtents("key-3", []string{"tenant"}, []Extent{mkExtent(20, 30), mkExtent(40, 50)})

		actual := mw.fetchCacheExtents(ctx, time.Now(), []string{"tenant"}, []string{"key-1", "key-2", "key-3"})
		expected := [][]Extent{{mkExtent(10, 20)}, nil, {mkExtent(20, 30), mkExtent(40, 50)}}
		assert.Equal(t, expected, actual)
	})

	t.Run("fetchCacheExtents() should not return an extent if its key doesn't match the requested one (hash collision)", func(t *testing.T) {
		// Simulate an hash collision on "key-1".
		buf, err := proto.Marshal(&CachedResponse{Key: "another", Extents: []Extent{mkExtent(10, 20)}})
		require.NoError(t, err)
		cacheBackend.SetMultiAsync(map[string][]byte{cacheHashKey("key-1"): buf}, 0)

		mw.storeCacheExtents("key-3", []string{"tenant"}, []Extent{mkExtent(20, 30), mkExtent(40, 50)})

		actual := mw.fetchCacheExtents(ctx, time.Now(), []string{"tenant"}, []string{"key-1", "key-2", "key-3"})
		expected := [][]Extent{nil, nil, {mkExtent(20, 30), mkExtent(40, 50)}}
		assert.Equal(t, expected, actual)
	})

	t.Run("fetchCacheExtents() should filter out extents that are outside of configured TTL", func(t *testing.T) {
		now := time.Now().UnixMilli()

		// Query time outside of TTL (1h), extent ends outside of OOO window (30m) -- will be filtered out.
		e1 := mkExtentWithStepAndQueryTime(10, 20, 10, now-3*time.Hour.Milliseconds())
		mw.storeCacheExtents("key-1", []string{"tenant"}, []Extent{e1})

		// Query time inside of TTL (1h), extent ends outside of OOO window (30m) -- will be used.
		e2 := mkExtentWithStepAndQueryTime(20, 30, 10, now-45*time.Minute.Milliseconds())
		mw.storeCacheExtents("key-2", []string{"tenant"}, []Extent{e2})

		// Query time outside of (short) TTL (10m), extent ends inside of OOO window (30min)
		extentEnd := now - 25*time.Minute.Milliseconds()
		e3 := mkExtentWithStepAndQueryTime(extentEnd-100, extentEnd, 10, now-15*time.Minute.Milliseconds())
		mw.storeCacheExtents("key-3", []string{"tenant"}, []Extent{e3})

		// Query time inside of (short) TTL (10m), extent ends inside of OOO window (30min)
		e4 := mkExtentWithStepAndQueryTime(extentEnd-100, extentEnd, 10, now-5*time.Minute.Milliseconds())
		mw.storeCacheExtents("key-4", []string{"tenant"}, []Extent{e4})

		// No query time, extent ends inside of OOO window (30min). This will be used.
		e5 := mkExtentWithStepAndQueryTime(extentEnd-100, extentEnd, 10, 0)
		mw.storeCacheExtents("key-5", []string{"tenant"}, []Extent{e5})

		actual := mw.fetchCacheExtents(ctx, time.UnixMilli(now), []string{"tenant"}, []string{"key-1", "key-2", "key-3", "key-4", "key-5"})
		expected := [][]Extent{
			nil,
			{e2},
			nil,
			{e4},
			{e5},
		}
		assert.Equal(t, expected, actual)
	})
}

func TestSplitAndCacheMiddleware_WrapMultipleTimes(t *testing.T) {
	m := newSplitAndCacheMiddleware(
		false,
		true,
		24*time.Hour,
		mockLimits{},
		newTestPrometheusCodec(),
		cache.NewMockCache(),
		DefaultCacheKeyGenerator{interval: day},
		PrometheusResponseExtractor{},
		resultsCacheAlwaysEnabled,
		log.NewNopLogger(),
		prometheus.NewPedanticRegistry(),
	)

	require.NotPanics(t, func() {
		m.Wrap(mockHandlerWith(nil, nil))
		m.Wrap(mockHandlerWith(nil, nil))
	})
}

func TestSplitRequests_prepareDownstreamRequests(t *testing.T) {
	tests := map[string]struct {
		input    splitRequests
		expected []MetricsQueryRequest
	}{
		"should return an empty slice on no downstream requests": {
			input:    nil,
			expected: nil,
		},
		"should inject ID and hints on downstream requests and return them": {
			input: splitRequests{
				{downstreamRequests: []MetricsQueryRequest{&PrometheusRangeQueryRequest{start: 1}, &PrometheusRangeQueryRequest{start: 2}}},
				{downstreamRequests: []MetricsQueryRequest{}},
				{downstreamRequests: []MetricsQueryRequest{&PrometheusRangeQueryRequest{start: 3}}},
			},
			expected: []MetricsQueryRequest{
				mustSucceed(mustSucceed((&PrometheusRangeQueryRequest{start: 1}).WithID(1)).WithTotalQueriesHint(3)),
				mustSucceed(mustSucceed((&PrometheusRangeQueryRequest{start: 2}).WithID(2)).WithTotalQueriesHint(3)),
				mustSucceed(mustSucceed((&PrometheusRangeQueryRequest{start: 3}).WithID(3)).WithTotalQueriesHint(3)),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Pre-condition check: input requests should have responses not initialized.
			for _, req := range testData.input {
				require.Empty(t, req.downstreamResponses)
			}

			assert.Equal(t, testData.expected, mustSucceed(testData.input.prepareDownstreamRequests()))

			// Ensure responses slices have been initialized.
			for _, req := range testData.input {
				assert.Len(t, req.downstreamResponses, len(req.downstreamRequests))
			}
		})
	}
}

func TestSplitRequests_storeDownstreamResponses(t *testing.T) {
	tests := map[string]struct {
		requests    splitRequests
		responses   []requestResponse
		expectedErr string
		expected    splitRequests
	}{
		"should do nothing on no downstream requests": {
			requests: splitRequests{
				{downstreamRequests: []MetricsQueryRequest{}},
				{downstreamRequests: []MetricsQueryRequest{}},
			},
			responses: nil,
			expected: splitRequests{
				{downstreamRequests: []MetricsQueryRequest{}},
				{downstreamRequests: []MetricsQueryRequest{}},
			},
		},
		"should associate downstream responses to requests": {
			requests: splitRequests{{
				downstreamRequests:  []MetricsQueryRequest{&PrometheusRangeQueryRequest{start: 1, id: 1}, &PrometheusRangeQueryRequest{start: 2, id: 2}},
				downstreamResponses: []Response{nil, nil},
			}, {
				downstreamRequests:  []MetricsQueryRequest{},
				downstreamResponses: []Response{},
			}, {
				downstreamRequests:  []MetricsQueryRequest{&PrometheusRangeQueryRequest{start: 3, id: 3}},
				downstreamResponses: []Response{nil},
			}},
			responses: []requestResponse{{
				Request:  &PrometheusRangeQueryRequest{start: 3, id: 3},
				Response: &PrometheusResponse{Status: "response-3"},
			}, {
				Request:  &PrometheusRangeQueryRequest{start: 1, id: 1},
				Response: &PrometheusResponse{Status: "response-1"},
			}, {
				Request:  &PrometheusRangeQueryRequest{start: 2, id: 2},
				Response: &PrometheusResponse{Status: "response-2"},
			}},
			expected: splitRequests{{
				downstreamRequests:  []MetricsQueryRequest{&PrometheusRangeQueryRequest{start: 1, id: 1}, &PrometheusRangeQueryRequest{start: 2, id: 2}},
				downstreamResponses: []Response{&PrometheusResponse{Status: "response-1"}, &PrometheusResponse{Status: "response-2"}},
			}, {
				downstreamRequests:  []MetricsQueryRequest{},
				downstreamResponses: []Response{},
			}, {
				downstreamRequests:  []MetricsQueryRequest{&PrometheusRangeQueryRequest{start: 3, id: 3}},
				downstreamResponses: []Response{&PrometheusResponse{Status: "response-3"}},
			}},
		},
		"should return error if a downstream response is missing": {
			requests: splitRequests{{
				downstreamRequests:  []MetricsQueryRequest{&PrometheusRangeQueryRequest{start: 1, id: 1}, &PrometheusRangeQueryRequest{start: 2, id: 2}},
				downstreamResponses: []Response{nil, nil},
			}, {
				downstreamRequests:  []MetricsQueryRequest{&PrometheusRangeQueryRequest{start: 3, id: 3}},
				downstreamResponses: []Response{nil},
			}},
			responses: []requestResponse{{
				Request:  &PrometheusRangeQueryRequest{start: 3, id: 3},
				Response: &PrometheusResponse{Status: "response-3"},
			}, {
				Request:  &PrometheusRangeQueryRequest{start: 2, id: 2},
				Response: &PrometheusResponse{Status: "response-2"},
			}},
			expectedErr: "consistency check failed: missing downstream response",
		},
		"should return error if multiple downstream responses have the same ID": {
			requests: splitRequests{{
				downstreamRequests:  []MetricsQueryRequest{&PrometheusRangeQueryRequest{start: 1, id: 1}, &PrometheusRangeQueryRequest{start: 2, id: 2}},
				downstreamResponses: []Response{nil, nil},
			}, {
				downstreamRequests:  []MetricsQueryRequest{&PrometheusRangeQueryRequest{start: 3, id: 3}},
				downstreamResponses: []Response{nil},
			}},
			responses: []requestResponse{{
				Request:  &PrometheusRangeQueryRequest{start: 3, id: 3},
				Response: &PrometheusResponse{Status: "response-3"},
			}, {
				Request:  &PrometheusRangeQueryRequest{start: 2, id: 3},
				Response: &PrometheusResponse{Status: "response-2"},
			}},
			expectedErr: "consistency check failed: conflicting downstream request ID",
		},
		"should return error if extra downstream responses are requested to be stored": {
			requests: splitRequests{{
				downstreamRequests:  []MetricsQueryRequest{&PrometheusRangeQueryRequest{start: 1, id: 1}, &PrometheusRangeQueryRequest{start: 2, id: 2}},
				downstreamResponses: []Response{nil, nil},
			}, {
				downstreamRequests:  []MetricsQueryRequest{&PrometheusRangeQueryRequest{start: 3, id: 3}},
				downstreamResponses: []Response{nil},
			}},
			responses: []requestResponse{{
				Request:  &PrometheusRangeQueryRequest{start: 3, id: 3},
				Response: &PrometheusResponse{Status: "response-3"},
			}, {
				Request:  &PrometheusRangeQueryRequest{start: 2, id: 2},
				Response: &PrometheusResponse{Status: "response-2"},
			}, {
				Request:  &PrometheusRangeQueryRequest{start: 1, id: 1},
				Response: &PrometheusResponse{Status: "response-1"},
			}, {
				Request:  &PrometheusRangeQueryRequest{start: 4, id: 4},
				Response: &PrometheusResponse{Status: "response-4"},
			}},
			expectedErr: "consistency check failed: received more responses than expected (expected: 3, got: 4)",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Pre-condition check: input requests should have responses initialized.
			for _, req := range testData.requests {
				require.Len(t, req.downstreamResponses, len(req.downstreamRequests))
			}

			err := testData.requests.storeDownstreamResponses(testData.responses)

			if testData.expectedErr != "" {
				assert.EqualError(t, err, testData.expectedErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, testData.expected, testData.requests)
			}
		})
	}
}

func parseTimeRFC3339(t *testing.T, input string) time.Time {
	parsed, err := time.Parse(time.RFC3339, input)
	require.NoError(t, err)

	return parsed
}

func mockQueryRangeURL(startTime, endTime time.Time, query string) string {
	generated := url.URL{Path: "/api/v1/query_range"}

	qs := generated.Query()
	qs.Set("start", fmt.Sprintf("%d", startTime.Unix()))
	qs.Set("end", fmt.Sprintf("%d", endTime.Unix()))
	qs.Set("query", query)
	qs.Set("step", "60")
	generated.RawQuery = qs.Encode()

	return generated.String()
}

func mockProtobufResponseWithSamplesAndHistograms(labels []mimirpb_custom.LabelAdapter, samples []mimirpb.Sample, histograms []mimirpb.FloatHistogramPair) *mimirpb.QueryResponse {
	return &mimirpb.QueryResponse{
		Status: mimirpb.QueryResponse_SUCCESS,
		Data: &mimirpb.QueryResponse_Matrix{
			Matrix: &mimirpb.MatrixData{
				Series: []mimirpb.MatrixSeries{
					{
						Metric:     stringArrayFromLabels(labels),
						Samples:    samples,
						Histograms: histograms,
					},
				},
			},
		},
	}
}

func protobufEncodePrometheusResponse(t *testing.T, res *mimirpb.QueryResponse) []byte {
	encoded, err := res.Marshal()
	require.NoError(t, err)
	return encoded
}

func jsonEncodePrometheusResponse(t *testing.T, res *PrometheusResponse) string {
	encoded, err := json.Marshal(res)
	require.NoError(t, err)
	return string(encoded)
}

func newAssertHintsMiddleware(t *testing.T, expected *Hints) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &assertHintsMiddleware{
			next:     next,
			t:        t,
			expected: expected,
		}
	})
}

type assertHintsMiddleware struct {
	next     MetricsQueryHandler
	t        *testing.T
	expected *Hints
}

func (m *assertHintsMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	assert.Equal(m.t, m.expected, req.GetHints())
	return m.next.Do(ctx, req)
}

type roundTripper struct {
	handler MetricsQueryHandler
	codec   Codec
}

// newRoundTripper merges a set of middlewares into an handler, then inject it into the `next` roundtripper
// using the codec to translate requests and responses.
func newRoundTripper(next http.RoundTripper, codec Codec, logger log.Logger, middlewares ...MetricsQueryMiddleware) http.RoundTripper {
	return roundTripper{
		handler: MergeMetricsQueryMiddlewares(middlewares...).Wrap(roundTripperHandler{
			logger: logger,
			next:   next,
			codec:  codec,
		}),
		codec: codec,
	}
}

func (q roundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	request, err := q.codec.DecodeMetricsQueryRequest(r.Context(), r)
	if err != nil {
		return nil, err
	}

	if span := opentracing.SpanFromContext(r.Context()); span != nil {
		request.AddSpanTags(span)
	}

	response, err := q.handler.Do(r.Context(), request)
	if err != nil {
		return nil, err
	}

	return q.codec.EncodeMetricsQueryResponse(r.Context(), r, response)
}

const seconds = 1e3 // 1e3 milliseconds per second.

func TestNextIntervalBoundary(t *testing.T) {
	for i, tc := range []struct {
		in, step, out int64
		interval      time.Duration
	}{
		// Smallest possible period is 1 millisecond
		{0, 1, toMs(day) - 1, day},
		{0, 1, toMs(time.Hour) - 1, time.Hour},
		// A more standard example
		{0, 15 * seconds, toMs(day) - 15*seconds, day},
		{0, 15 * seconds, toMs(time.Hour) - 15*seconds, time.Hour},
		// Move start time forward 1 second; end time moves the same
		{1 * seconds, 15 * seconds, toMs(day) - (15-1)*seconds, day},
		{1 * seconds, 15 * seconds, toMs(time.Hour) - (15-1)*seconds, time.Hour},
		// Move start time forward 14 seconds; end time moves the same
		{14 * seconds, 15 * seconds, toMs(day) - (15-14)*seconds, day},
		{14 * seconds, 15 * seconds, toMs(time.Hour) - (15-14)*seconds, time.Hour},
		// Now some examples where the period does not divide evenly into a day:
		// 1 day modulus 35 seconds = 20 seconds
		{0, 35 * seconds, toMs(day) - 20*seconds, day},
		// 1 hour modulus 35 sec = 30  (3600 mod 35 = 30)
		{0, 35 * seconds, toMs(time.Hour) - 30*seconds, time.Hour},
		// Move start time forward 1 second; end time moves the same
		{1 * seconds, 35 * seconds, toMs(day) - (20-1)*seconds, day},
		{1 * seconds, 35 * seconds, toMs(time.Hour) - (30-1)*seconds, time.Hour},
		// If the end time lands exactly on midnight we stop one period before that
		{20 * seconds, 35 * seconds, toMs(day) - 35*seconds, day},
		{30 * seconds, 35 * seconds, toMs(time.Hour) - 35*seconds, time.Hour},
		// This example starts 35 seconds after the 5th one ends
		{toMs(day) + 15*seconds, 35 * seconds, 2*toMs(day) - 5*seconds, day},
		{toMs(time.Hour) + 15*seconds, 35 * seconds, 2*toMs(time.Hour) - 15*seconds, time.Hour},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require.Equal(t, tc.out, nextIntervalBoundary(tc.in, tc.step, tc.interval))
		})
	}
}

func TestSplitQueryByInterval(t *testing.T) {
	queryFoo := "foo"
	queryFooExpr, _ := parser.ParseExpr(queryFoo)
	queryFooAtStart := "foo @ start()"
	queryFooAtStartExpr, _ := parser.ParseExpr(queryFooAtStart)
	queryFooAtZero := "foo @ 0.000"
	queryFooAtZeroExpr, _ := parser.ParseExpr(queryFooAtZero)
	queryFooSubqueryAtStart := "sum_over_time(foo[1d:] @ start())"
	queryFooSubqueryAtStartExpr, _ := parser.ParseExpr(queryFooSubqueryAtStart)
	queryFooSubqueryAtZero := "sum_over_time(foo[1d:] @ 0.000)"
	queryFooSubqueryAtZeroExpr, _ := parser.ParseExpr(queryFooSubqueryAtZero)
	lookbackDelta := 5 * time.Minute

	for i, tc := range []struct {
		input    MetricsQueryRequest
		expected []MetricsQueryRequest
		interval time.Duration
	}{
		{
			input: &PrometheusRangeQueryRequest{start: 0, end: 60 * 60 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: 0, end: 60 * 60 * seconds, minT: -lookbackDelta.Milliseconds() + 1, maxT: 60 * 60 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{start: 0, end: 60 * 60 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: 0, end: 60 * 60 * seconds, minT: -lookbackDelta.Milliseconds() + 1, maxT: 60 * 60 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &PrometheusRangeQueryRequest{start: 0, end: 24 * 3600 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: 0, end: 24 * 3600 * seconds, minT: -lookbackDelta.Milliseconds() + 1, maxT: 24 * 3600 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{start: 0, end: 3 * 3600 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: 0, end: 3 * 3600 * seconds, minT: -lookbackDelta.Milliseconds() + 1, maxT: 3 * 3600 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &PrometheusRangeQueryRequest{start: 0, end: 2 * 24 * 3600 * seconds, step: 15 * seconds, queryExpr: queryFooAtStartExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: 0, end: (24 * 3600 * seconds) - (15 * seconds), minT: -lookbackDelta.Milliseconds() + 1, step: 15 * seconds, queryExpr: queryFooAtZeroExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: 24 * 3600 * seconds, end: 2 * 24 * 3600 * seconds, minT: -lookbackDelta.Milliseconds() + 1, step: 15 * seconds, queryExpr: queryFooAtZeroExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{minT: -(24 * 3600 * seconds), start: 0, end: 2 * 24 * 3600 * seconds, step: 15 * seconds, queryExpr: queryFooSubqueryAtStartExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{minT: -(24 * 3600 * seconds) - lookbackDelta.Milliseconds() + 1, start: 0, end: (24 * 3600 * seconds) - (15 * seconds), step: 15 * seconds, queryExpr: queryFooSubqueryAtZeroExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{minT: -(24 * 3600 * seconds) - lookbackDelta.Milliseconds() + 1, start: 24 * 3600 * seconds, end: 2 * 24 * 3600 * seconds, step: 15 * seconds, queryExpr: queryFooSubqueryAtZeroExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{start: 0, end: 2 * 3 * 3600 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: 0, end: (3 * 3600 * seconds) - (15 * seconds), minT: -lookbackDelta.Milliseconds() + 1, maxT: (3 * 3600 * seconds) - (15 * seconds), step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: 3 * 3600 * seconds, end: 2 * 3 * 3600 * seconds, minT: 3*3600*seconds - lookbackDelta.Milliseconds() + 1, maxT: 2 * 3 * 3600 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &PrometheusRangeQueryRequest{start: 3 * 3600 * seconds, end: 3 * 24 * 3600 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: 3 * 3600 * seconds, minT: 3*3600*seconds - lookbackDelta.Milliseconds() + 1, end: (24 * 3600 * seconds) - (15 * seconds), maxT: (24 * 3600 * seconds) - (15 * seconds), step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: 24 * 3600 * seconds, minT: 24*3600*seconds - lookbackDelta.Milliseconds() + 1, end: (2 * 24 * 3600 * seconds) - (15 * seconds), maxT: (2 * 24 * 3600 * seconds) - (15 * seconds), step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: 2 * 24 * 3600 * seconds, minT: 2*24*3600*seconds - lookbackDelta.Milliseconds() + 1, end: 3 * 24 * 3600 * seconds, maxT: 3 * 24 * 3600 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{start: 2 * 3600 * seconds, end: 3 * 3 * 3600 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: 2 * 3600 * seconds, end: (3 * 3600 * seconds) - (15 * seconds), minT: 2*3600*seconds - lookbackDelta.Milliseconds() + 1, maxT: (3 * 3600 * seconds) - (15 * seconds), step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: 3 * 3600 * seconds, end: (2 * 3 * 3600 * seconds) - (15 * seconds), minT: 3*3600*seconds - lookbackDelta.Milliseconds() + 1, maxT: (2 * 3 * 3600 * seconds) - (15 * seconds), step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: 2 * 3 * 3600 * seconds, end: 3 * 3 * 3600 * seconds, minT: 2*3*3600*seconds - lookbackDelta.Milliseconds() + 1, maxT: 3 * 3 * 3600 * seconds, step: 15 * seconds, queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-14T23:48:00Z"), end: timeToMillis(t, "2021-10-15T00:03:00Z"), step: 5 * time.Minute.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-14T23:48:00Z"), end: timeToMillis(t, "2021-10-15T00:03:00Z"), minT: timeToMillis(t, "2021-10-14T23:48:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-15T00:03:00Z"), step: 5 * time.Minute.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-14T23:48:00Z"), end: timeToMillis(t, "2021-10-15T00:00:00Z"), step: 6 * time.Minute.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-14T23:48:00Z"), end: timeToMillis(t, "2021-10-14T23:54:00Z"), minT: timeToMillis(t, "2021-10-14T23:48:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-14T23:54:00Z"), step: 6 * time.Minute.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T00:00:00Z"), end: timeToMillis(t, "2021-10-15T00:00:00Z"), minT: timeToMillis(t, "2021-10-15T00:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-15T00:00:00Z"), step: 6 * time.Minute.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-14T22:00:00Z"), end: timeToMillis(t, "2021-10-17T22:00:00Z"), step: 24 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-14T22:00:00Z"), end: timeToMillis(t, "2021-10-14T22:00:00Z"), minT: timeToMillis(t, "2021-10-14T22:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-14T22:00:00Z"), step: 24 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T22:00:00Z"), end: timeToMillis(t, "2021-10-15T22:00:00Z"), minT: timeToMillis(t, "2021-10-15T22:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-15T22:00:00Z"), step: 24 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-16T22:00:00Z"), end: timeToMillis(t, "2021-10-16T22:00:00Z"), minT: timeToMillis(t, "2021-10-16T22:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-16T22:00:00Z"), step: 24 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-17T22:00:00Z"), end: timeToMillis(t, "2021-10-17T22:00:00Z"), minT: timeToMillis(t, "2021-10-17T22:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-17T22:00:00Z"), step: 24 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T00:00:00Z"), end: timeToMillis(t, "2021-10-18T00:00:00Z"), step: 24 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T00:00:00Z"), end: timeToMillis(t, "2021-10-15T00:00:00Z"), minT: timeToMillis(t, "2021-10-15T00:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-15T00:00:00Z"), step: 24 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-16T00:00:00Z"), end: timeToMillis(t, "2021-10-16T00:00:00Z"), minT: timeToMillis(t, "2021-10-16T00:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-16T00:00:00Z"), step: 24 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-17T00:00:00Z"), end: timeToMillis(t, "2021-10-17T00:00:00Z"), minT: timeToMillis(t, "2021-10-17T00:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-17T00:00:00Z"), step: 24 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-18T00:00:00Z"), end: timeToMillis(t, "2021-10-18T00:00:00Z"), minT: timeToMillis(t, "2021-10-18T00:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-18T00:00:00Z"), step: 24 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T22:00:00Z"), end: timeToMillis(t, "2021-10-22T04:00:00Z"), step: 30 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T22:00:00Z"), end: timeToMillis(t, "2021-10-15T22:00:00Z"), minT: timeToMillis(t, "2021-10-15T22:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-15T22:00:00Z"), step: 30 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-17T04:00:00Z"), end: timeToMillis(t, "2021-10-17T04:00:00Z"), minT: timeToMillis(t, "2021-10-17T04:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-17T04:00:00Z"), step: 30 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-18T10:00:00Z"), end: timeToMillis(t, "2021-10-18T10:00:00Z"), minT: timeToMillis(t, "2021-10-18T10:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-18T10:00:00Z"), step: 30 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-19T16:00:00Z"), end: timeToMillis(t, "2021-10-19T16:00:00Z"), minT: timeToMillis(t, "2021-10-19T16:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-19T16:00:00Z"), step: 30 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-20T22:00:00Z"), end: timeToMillis(t, "2021-10-20T22:00:00Z"), minT: timeToMillis(t, "2021-10-20T22:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-20T22:00:00Z"), step: 30 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-22T04:00:00Z"), end: timeToMillis(t, "2021-10-22T04:00:00Z"), minT: timeToMillis(t, "2021-10-22T04:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-22T04:00:00Z"), step: 30 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T06:00:00Z"), end: timeToMillis(t, "2021-10-17T14:00:00Z"), step: 12 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T06:00:00Z"), end: timeToMillis(t, "2021-10-15T18:00:00Z"), minT: timeToMillis(t, "2021-10-15T06:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-15T18:00:00Z"), step: 12 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-16T06:00:00Z"), end: timeToMillis(t, "2021-10-16T18:00:00Z"), minT: timeToMillis(t, "2021-10-16T06:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-16T18:00:00Z"), step: 12 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-17T06:00:00Z"), end: timeToMillis(t, "2021-10-17T14:00:00Z"), minT: timeToMillis(t, "2021-10-17T06:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-17T14:00:00Z"), step: 12 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T06:00:00Z"), end: timeToMillis(t, "2021-10-17T18:00:00Z"), step: 12 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T06:00:00Z"), end: timeToMillis(t, "2021-10-15T18:00:00Z"), minT: timeToMillis(t, "2021-10-15T06:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-15T18:00:00Z"), step: 12 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-16T06:00:00Z"), end: timeToMillis(t, "2021-10-16T18:00:00Z"), minT: timeToMillis(t, "2021-10-16T06:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-16T18:00:00Z"), step: 12 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-17T06:00:00Z"), end: timeToMillis(t, "2021-10-17T18:00:00Z"), minT: timeToMillis(t, "2021-10-17T06:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-17T18:00:00Z"), step: 12 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T06:00:00Z"), end: timeToMillis(t, "2021-10-17T18:00:00Z"), step: 10 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T06:00:00Z"), end: timeToMillis(t, "2021-10-15T16:00:00Z"), minT: timeToMillis(t, "2021-10-15T06:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-15T16:00:00Z"), step: 10 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-16T02:00:00Z"), end: timeToMillis(t, "2021-10-16T22:00:00Z"), minT: timeToMillis(t, "2021-10-16T02:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-16T22:00:00Z"), step: 10 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-17T08:00:00Z"), end: timeToMillis(t, "2021-10-17T18:00:00Z"), minT: timeToMillis(t, "2021-10-17T08:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-17T18:00:00Z"), step: 10 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T06:00:00Z"), end: timeToMillis(t, "2021-10-17T08:00:00Z"), step: 10 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			expected: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-15T06:00:00Z"), end: timeToMillis(t, "2021-10-15T16:00:00Z"), minT: timeToMillis(t, "2021-10-15T06:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-15T16:00:00Z"), step: 10 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-16T02:00:00Z"), end: timeToMillis(t, "2021-10-16T22:00:00Z"), minT: timeToMillis(t, "2021-10-16T02:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-16T22:00:00Z"), step: 10 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
				&PrometheusRangeQueryRequest{start: timeToMillis(t, "2021-10-17T08:00:00Z"), end: timeToMillis(t, "2021-10-17T08:00:00Z"), minT: timeToMillis(t, "2021-10-17T08:00:00Z") - lookbackDelta.Milliseconds() + 1, maxT: timeToMillis(t, "2021-10-17T08:00:00Z"), step: 10 * time.Hour.Milliseconds(), queryExpr: queryFooExpr, lookbackDelta: lookbackDelta},
			},
			interval: day,
		},
	} {
		t.Run(fmt.Sprintf("%d: start: %v, end: %v, step: %v: %v", i, tc.input.GetStart(), tc.input.GetEnd(), tc.input.GetStep(), tc.input.GetQuery()), func(t *testing.T) {
			days, err := splitQueryByInterval(tc.input, tc.interval)
			require.NoError(t, err)
			require.Equal(t, tc.expected, days)
		})
	}
}

func timeToMillis(t *testing.T, input string) int64 {
	r, err := time.Parse(time.RFC3339, input)
	require.NoError(t, err)
	return util.TimeToMillis(r)
}

func Test_evaluateAtModifier(t *testing.T) {
	const (
		start, end = int64(1546300800), int64(1646300800)
	)
	for _, tt := range []struct {
		in, expected string
		err          error
	}{
		{"topk(5, rate(http_requests_total[1h] @ start()))", "topk(5, rate(http_requests_total[1h] @ 1546300.800))", nil},
		{"topk(5, rate(http_requests_total[1h] @ 0))", "topk(5, rate(http_requests_total[1h] @ 0.000))", nil},
		{"http_requests_total[1h] @ 10.001", "http_requests_total[1h] @ 10.001", nil},
		{"sum_over_time(http_requests_total[1h:] @ start())", "sum_over_time(http_requests_total[1h:] @ 1546300.800)", nil},
		{"sum_over_time((http_requests_total @ end())[1h:] @ start())", "sum_over_time((http_requests_total @ 1646300.800)[1h:] @ 1546300.800)", nil},
		{
			`min_over_time(
				sum by(cluster) (
					rate(http_requests_total[5m] @ end())
				)[10m:]
			)
			or
			max_over_time(
				stddev_over_time(
					deriv(
						rate(http_requests_total[10m] @ start())
					[5m:1m])
				[2m:])
			[10m:])`,
			`min_over_time(
				sum by(cluster) (
					rate(http_requests_total[5m] @ 1646300.800)
				)[10m:]
			)
			or
			max_over_time(
				stddev_over_time(
					deriv(
						rate(http_requests_total[10m] @ 1546300.800)
					[5m:1m])
				[2m:])
			[10m:])`, nil,
		},
		{"sum by (foo) (bar[buzz])", "foo{}", apierror.New(apierror.TypeBadData, `invalid parameter "query": 1:19: parse error: bad number or duration syntax: ""`)},
	} {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			expectedExpr, err := parser.ParseExpr(tt.expected)
			require.NoError(t, err)
			out, err := evaluateAtModifierFunction(tt.in, start, end)
			if tt.err != nil {
				require.Equal(t, tt.err, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, expectedExpr.String(), out)
		})
	}
}

func TestSplitAndCacheMiddlewareLowerTTL(t *testing.T) {
	mcache := cache.NewMockCache()
	m := splitAndCacheMiddleware{
		limits: mockLimits{
			outOfOrderTimeWindow:            time.Hour,
			resultsCacheTTL:                 resultsCacheTTL,
			resultsCacheOutOfOrderWindowTTL: resultsCacheLowerTTL,
		},
		cache: mcache,
	}

	cases := []struct {
		endTime time.Time
		expTTL  time.Duration
	}{
		{
			endTime: time.Now(),
			expTTL:  resultsCacheLowerTTL,
		},
		{
			endTime: time.Now().Add(-30 * time.Minute),
			expTTL:  resultsCacheLowerTTL,
		},
		{
			endTime: time.Now().Add(-59 * time.Minute),
			expTTL:  resultsCacheLowerTTL,
		},
		{
			endTime: time.Now().Add(-61 * time.Minute),
			expTTL:  resultsCacheTTL,
		},
		{
			endTime: time.Now().Add(-2 * time.Hour),
			expTTL:  resultsCacheTTL,
		},
		{
			endTime: time.Now().Add(-12 * time.Hour),
			expTTL:  resultsCacheTTL,
		},
	}

	for i, c := range cases {
		// Store.
		key := fmt.Sprintf("k%d", i)
		m.storeCacheExtents(key, []string{"ten1"}, []Extent{
			{Start: 0, End: c.endTime.UnixMilli()},
		})

		// Check.
		key = cacheHashKey(key)
		ci := mcache.GetItems()[key]
		actualTTL := time.Until(ci.ExpiresAt)
		// We use a tolerance of 50ms to avoid flaky tests.
		require.Greater(t, actualTTL, c.expTTL-(50*time.Millisecond))
		require.Less(t, actualTTL, c.expTTL+(50*time.Millisecond))
	}
}
