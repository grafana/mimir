// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"io/ioutil"
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
	"github.com/grafana/dskit/concurrency"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/cache"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
)

func TestSplitAndCacheMiddleware_SplitByInterval(t *testing.T) {
	var (
		startTime    = parseTimeRFC3339(t, "2021-10-14T00:00:00Z")
		endTime      = parseTimeRFC3339(t, "2021-10-15T23:59:59Z")
		queryURL     = mockQueryRangeURL(startTime, endTime, `{__name__=~".+"}`)
		seriesLabels = []mimirpb.LabelAdapter{{Name: "__name__", Value: "test_metric"}}

		// Mock the downstream responses.
		firstDayDownstreamResponse = encodePrometheusResponse(t,
			mockPrometheusResponseSingleSeries(seriesLabels, mimirpb.Sample{TimestampMs: startTime.Unix() * 1000, Value: 10}))

		secondDayDownstreamResponse = encodePrometheusResponse(t,
			mockPrometheusResponseSingleSeries(seriesLabels, mimirpb.Sample{TimestampMs: endTime.Unix() * 1000, Value: 20}))

		// Build the expected response (which is the merge of the two downstream responses).
		expectedResponse = encodePrometheusResponse(t, mockPrometheusResponseSingleSeries(seriesLabels,
			mimirpb.Sample{TimestampMs: startTime.Unix() * 1000, Value: 10},
			mimirpb.Sample{TimestampMs: endTime.Unix() * 1000, Value: 20}))
	)

	var actualCount atomic.Int32
	downstreamServer := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				actualCount.Inc()

				req, err := prometheusCodec{}.DecodeRequest(r.Context(), r)
				require.NoError(t, err)

				if req.GetStart() == startTime.Unix()*1000 {
					_, _ = w.Write([]byte(firstDayDownstreamResponse))
				} else if req.GetStart() == startTime.Add(24*time.Hour).Unix()*1000 {
					_, _ = w.Write([]byte(secondDayDownstreamResponse))
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
		false,
		mockLimits{},
		PrometheusCodec,
		nil,
		nil,
		nil,
		nil,
		log.NewNopLogger(),
		reg,
	)

	// Chain middlewares together.
	middlewares := []Middleware{
		newLimitsMiddleware(mockLimits{}, log.NewNopLogger()),
		splitCacheMiddleware,
		newAssertHintsMiddleware(t, &Hints{TotalQueries: 2}),
	}

	roundtripper := newRoundTripper(singleHostRoundTripper{
		host: downstreamURL.Host,
		next: http.DefaultTransport,
	}, PrometheusCodec, log.NewNopLogger(), middlewares...)

	// Execute a query range request.
	req, err := http.NewRequest("GET", queryURL, http.NoBody)
	require.NoError(t, err)
	req = req.WithContext(user.InjectOrgID(context.Background(), "user-1"))

	resp, err := roundtripper.RoundTrip(req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)

	actualBody, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, expectedResponse, string(actualBody))
	require.Equal(t, int32(2), actualCount.Load())

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_frontend_split_queries_total Total number of underlying query requests after the split by interval is applied
		# TYPE cortex_frontend_split_queries_total counter
		cortex_frontend_split_queries_total 2
	`)))
}

func TestSplitAndCacheMiddleware_ResultsCache(t *testing.T) {
	cacheBackend := cache.NewInstrumentedMockCache()

	mw := newSplitAndCacheMiddleware(
		true,
		true,
		24*time.Hour,
		false,
		mockLimits{maxCacheFreshness: 10 * time.Minute},
		PrometheusCodec,
		cacheBackend,
		constSplitter(day),
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
					Labels: []mimirpb.LabelAdapter{
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
	rc := mw.Wrap(HandlerFunc(func(_ context.Context, req Request) (Response, error) {
		downstreamReqs++
		return expectedResponse, nil
	}))

	step := int64(120 * 1000)
	req := Request(&PrometheusRangeQueryRequest{
		Path:  "/api/v1/query_range",
		Start: parseTimeRFC3339(t, "2021-10-15T10:00:00Z").Unix() * 1000,
		End:   parseTimeRFC3339(t, "2021-10-15T12:00:00Z").Unix() * 1000,
		Step:  step,
		Query: `{__name__=~".+"}`,
	})

	ctx := user.InjectOrgID(context.Background(), "1")
	resp, err := rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, downstreamReqs)
	require.Equal(t, expectedResponse, resp)
	assert.Equal(t, 1, cacheBackend.CountStoreCalls())

	// Doing same request again shouldn't change anything.
	resp, err = rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, downstreamReqs)
	require.Equal(t, expectedResponse, resp)
	assert.Equal(t, 1, cacheBackend.CountStoreCalls())

	// Doing request with new end time should do one more query.
	req = req.WithStartEnd(req.GetStart(), req.GetEnd()+step)
	_, err = rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 2, downstreamReqs)
	assert.Equal(t, 2, cacheBackend.CountStoreCalls())
}

func TestSplitAndCacheMiddleware_ResultsCache_ShouldNotLookupCacheIfStepIsNotAligned(t *testing.T) {
	cacheBackend := cache.NewInstrumentedMockCache()

	mw := newSplitAndCacheMiddleware(
		true,
		true,
		24*time.Hour,
		false,
		mockLimits{maxCacheFreshness: 10 * time.Minute},
		PrometheusCodec,
		cacheBackend,
		constSplitter(day),
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
					Labels: []mimirpb.LabelAdapter{
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
	rc := mw.Wrap(HandlerFunc(func(_ context.Context, req Request) (Response, error) {
		downstreamReqs++
		return expectedResponse, nil
	}))

	req := Request(&PrometheusRangeQueryRequest{
		Path:  "/api/v1/query_range",
		Start: parseTimeRFC3339(t, "2021-10-15T10:00:00Z").Unix() * 1000,
		End:   parseTimeRFC3339(t, "2021-10-15T12:00:00Z").Unix() * 1000,
		Step:  13 * 1000, // Not aligned to start/end.
		Query: `{__name__=~".+"}`,
	})

	ctx := user.InjectOrgID(context.Background(), "1")
	resp, err := rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, downstreamReqs)
	require.Equal(t, expectedResponse, resp)

	// Should not touch the cache at all.
	assert.Equal(t, 0, cacheBackend.CountFetchCalls())
	assert.Equal(t, 0, cacheBackend.CountStoreCalls())
}

func TestSplitAndCacheMiddleware_ResultsCache_EnabledCachingOfStepUnalignedRequest(t *testing.T) {
	cacheBackend := cache.NewInstrumentedMockCache()

	mw := newSplitAndCacheMiddleware(
		true,
		true,
		24*time.Hour,
		true, // caching of step-unaligned requests is enabled in this test.
		mockLimits{maxCacheFreshness: 10 * time.Minute},
		PrometheusCodec,
		cacheBackend,
		constSplitter(day),
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
					Labels: []mimirpb.LabelAdapter{
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
	rc := mw.Wrap(HandlerFunc(func(_ context.Context, req Request) (Response, error) {
		downstreamReqs++
		return expectedResponse, nil
	}))

	req := Request(&PrometheusRangeQueryRequest{
		Path:  "/api/v1/query_range",
		Start: parseTimeRFC3339(t, "2021-10-15T10:00:00Z").Unix() * 1000,
		End:   parseTimeRFC3339(t, "2021-10-15T12:00:00Z").Unix() * 1000,
		Step:  13 * 1000, // Not aligned to start/end.
		Query: `{__name__=~".+"}`,
	})

	ctx := user.InjectOrgID(context.Background(), "1")
	resp, err := rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, downstreamReqs)
	require.Equal(t, expectedResponse, resp)

	// Since we're caching unaligned requests, we should see that.
	assert.Equal(t, 1, cacheBackend.CountFetchCalls())
	assert.Equal(t, 1, cacheBackend.CountStoreCalls())

	// Doing the same request reuses cached result.
	resp, err = rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, downstreamReqs)
	require.Equal(t, expectedResponse, resp)
	assert.Equal(t, 2, cacheBackend.CountFetchCalls())
	assert.Equal(t, 1, cacheBackend.CountStoreCalls())

	// New request with slightly different Start time will not reuse the cached result.
	req = req.WithStartEnd(parseTimeRFC3339(t, "2021-10-15T10:00:05Z").Unix()*1000, parseTimeRFC3339(t, "2021-10-15T12:00:05Z").Unix()*1000)
	resp, err = rc.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 2, downstreamReqs)
	require.Equal(t, expectedResponse, resp)

	assert.Equal(t, 3, cacheBackend.CountFetchCalls())
	assert.Equal(t, 2, cacheBackend.CountStoreCalls())
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
	}{
		"should not cache a response if query time range is earlier than max cache freshness": {
			queryStartTime: fiveMinutesAgo,
			queryEndTime:   now,
			downstreamResponse: mockPrometheusResponseSingleSeries(
				[]mimirpb.LabelAdapter{{Name: "__name__", Value: "test_metric"}},
				mimirpb.Sample{TimestampMs: fiveMinutesAgo.Unix() * 1000, Value: 10},
				mimirpb.Sample{TimestampMs: now.Unix() * 1000, Value: 20}),
			expectedDownstreamStartTime: fiveMinutesAgo,
			expectedDownstreamEndTime:   now,
			expectedCachedResponses:     nil,
		},
		"should cache a response up until max cache freshness time ago": {
			queryStartTime: twentyMinutesAgo,
			queryEndTime:   now,
			downstreamResponse: mockPrometheusResponseSingleSeries(
				[]mimirpb.LabelAdapter{{Name: "__name__", Value: "test_metric"}},
				mimirpb.Sample{TimestampMs: twentyMinutesAgo.Unix() * 1000, Value: 10},
				mimirpb.Sample{TimestampMs: now.Unix() * 1000, Value: 20}),
			expectedDownstreamStartTime: twentyMinutesAgo,
			expectedDownstreamEndTime:   now,
			expectedCachedResponses: []Response{
				mockPrometheusResponseSingleSeries(
					[]mimirpb.LabelAdapter{{Name: "__name__", Value: "test_metric"}},
					// Any sample more recent than max cache freshness shouldn't be cached.
					mimirpb.Sample{TimestampMs: twentyMinutesAgo.Unix() * 1000, Value: 10}),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cacheBackend := cache.NewMockCache()
			cacheSplitter := constSplitter(day)

			mw := newSplitAndCacheMiddleware(
				false, // No interval splitting.
				true,
				24*time.Hour,
				false,
				mockLimits{maxCacheFreshness: maxCacheFreshness},
				PrometheusCodec,
				cacheBackend,
				cacheSplitter,
				PrometheusResponseExtractor{},
				resultsCacheAlwaysEnabled,
				log.NewNopLogger(),
				prometheus.NewPedanticRegistry(),
			)

			calls := 0
			rc := mw.Wrap(HandlerFunc(func(_ context.Context, r Request) (Response, error) {
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

			req := Request(&PrometheusRangeQueryRequest{
				Path:  "/api/v1/query_range",
				Start: testData.queryStartTime.Unix() * 1000,
				End:   testData.queryEndTime.Unix() * 1000,
				Step:  1000, // 1s step so it's guaranteed to be aligned.
				Query: `{__name__=~".+"}`,
			})

			// Request should result in a query.
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
			cacheKey := cacheHashKey(cacheSplitter.GenerateCacheKey(userID, req))
			found := cacheBackend.Fetch(ctx, []string{cacheKey})

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
	rand.Seed(seed)
	t.Log("random generator seed:", seed)

	// The mocked storage contains samples within the following min/max time.
	minTime := parseTimeRFC3339(t, "2021-10-13T00:00:00Z")
	maxTime := parseTimeRFC3339(t, "2021-10-15T23:09:09Z")
	step := 2 * time.Minute

	// Generate series.
	series := make([]*promql.StorageSeries, 0, numSeries)
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
	reqs := make([]Request, 0, numQueries)
	for q := 0; q < numQueries; q++ {
		// Generate a random time range within min/max time.
		startTime := minTime.Add(time.Duration(rand.Int63n(maxTime.Sub(minTime).Milliseconds())) * time.Millisecond)
		endTime := startTime.Add(time.Duration(rand.Int63n(maxTime.Sub(startTime).Milliseconds())) * time.Millisecond)

		reqs = append(reqs, &PrometheusRangeQueryRequest{
			Id:    int64(q),
			Path:  "/api/v1/query_range",
			Start: startTime.Unix() * 1000,
			End:   endTime.Unix() * 1000,
			Step:  120 * 1000,
			Query: fmt.Sprintf(`sum by(group_2) (rate({__name__=~".+"}[%s]))`, (2 * step).String()),
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
		expectedRes[reqs[idx].GetId()] = res
		expectedResMx.Unlock()

		return nil
	}))

	for testName, testData := range tests {
		for _, maxConcurrency := range []int{1, numQueries} {
			// Change scope to ensure tests work fine when run concurrently.
			testData := testData
			maxConcurrency := maxConcurrency

			t.Run(fmt.Sprintf("%s (concurrency: %d)", testName, maxConcurrency), func(t *testing.T) {
				t.Parallel()

				mw := newSplitAndCacheMiddleware(
					testData.splitEnabled,
					testData.cacheEnabled,
					24*time.Hour,
					testData.cacheUnaligned,
					mockLimits{
						maxCacheFreshness:   testData.maxCacheFreshness,
						maxQueryParallelism: testData.maxQueryParallelism,
					},
					PrometheusCodec,
					cache.NewMockCache(),
					constSplitter(day),
					PrometheusResponseExtractor{},
					resultsCacheAlwaysEnabled,
					log.NewNopLogger(),
					prometheus.NewPedanticRegistry(),
				).Wrap(downstream)

				// Run requests honoring concurrency.
				require.NoError(t, concurrency.ForEachJob(ctx, len(reqs), maxConcurrency, func(ctx context.Context, idx int) error {
					actual, err := mw.Do(ctx, reqs[idx])
					require.NoError(t, err)
					require.Equal(t, expectedRes[reqs[idx].GetId()], actual)

					return nil
				}))
			})
		}
	}
}

func TestSplitAndCacheMiddleware_ResultsCache_ExtentsEdgeCases(t *testing.T) {
	const userID = "user-1"

	tests := map[string]struct {
		req                    Request
		cachedExtents          []Extent
		expectedUpdatedExtents bool
		expectedCachedExtents  []Extent
	}{
		"Should drop tiny extent that overlaps with non-tiny request only": {
			req: &PrometheusRangeQueryRequest{
				Start: 100,
				End:   120,
				Step:  5,
			},
			cachedExtents: []Extent{
				mkExtentWithStep(0, 50, 5),
				mkExtentWithStep(60, 65, 5),
				mkExtentWithStep(100, 105, 5),
				mkExtentWithStep(110, 150, 5),
				mkExtentWithStep(160, 165, 5),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStep(0, 50, 5),
				mkExtentWithStep(60, 65, 5),
				mkExtentWithStep(100, 150, 5),
				mkExtentWithStep(160, 165, 5),
			},
		},
		"Should replace tiny extents that are cover by bigger request": {
			req: &PrometheusRangeQueryRequest{
				Start: 100,
				End:   200,
				Step:  5,
			},
			cachedExtents: []Extent{
				mkExtentWithStep(0, 50, 5),
				mkExtentWithStep(60, 65, 5),
				mkExtentWithStep(100, 105, 5),
				mkExtentWithStep(110, 115, 5),
				mkExtentWithStep(120, 125, 5),
				mkExtentWithStep(220, 225, 5),
				mkExtentWithStep(240, 250, 5),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStep(0, 50, 5),
				mkExtentWithStep(60, 65, 5),
				mkExtentWithStep(100, 200, 5),
				mkExtentWithStep(220, 225, 5),
				mkExtentWithStep(240, 250, 5),
			},
		},
		"Should not drop tiny extent that completely overlaps with tiny request": {
			req: &PrometheusRangeQueryRequest{
				Start: 100,
				End:   105,
				Step:  5,
			},
			cachedExtents: []Extent{
				mkExtentWithStep(0, 50, 5),
				mkExtentWithStep(60, 65, 5),
				mkExtentWithStep(100, 105, 5),
				mkExtentWithStep(160, 165, 5),
			},
			// No cache update need, request fulfilled using cache
			expectedUpdatedExtents: false,
			// We expect the same extents in the cache (no changes).
			expectedCachedExtents: []Extent{
				mkExtentWithStep(0, 50, 5),
				mkExtentWithStep(60, 65, 5),
				mkExtentWithStep(100, 105, 5),
				mkExtentWithStep(160, 165, 5),
			},
		},
		"Should not drop tiny extent that partially center-overlaps with tiny request": {
			req: &PrometheusRangeQueryRequest{
				Start: 106,
				End:   108,
				Step:  2,
			},
			cachedExtents: []Extent{
				mkExtentWithStep(60, 64, 2),
				mkExtentWithStep(104, 110, 2),
				mkExtentWithStep(160, 166, 2),
			},
			// No cache update need, request fulfilled using cache
			expectedUpdatedExtents: false,
			// We expect the same extents in the cache (no changes).
			expectedCachedExtents: []Extent{
				mkExtentWithStep(60, 64, 2),
				mkExtentWithStep(104, 110, 2),
				mkExtentWithStep(160, 166, 2),
			},
		},
		"Should not drop tiny extent that partially left-overlaps with tiny request": {
			req: &PrometheusRangeQueryRequest{
				Start: 100,
				End:   106,
				Step:  2,
			},
			cachedExtents: []Extent{
				mkExtentWithStep(60, 64, 2),
				mkExtentWithStep(104, 110, 2),
				mkExtentWithStep(160, 166, 2),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStep(60, 64, 2),
				mkExtentWithStep(100, 110, 2),
				mkExtentWithStep(160, 166, 2),
			},
		},
		"Should not drop tiny extent that partially right-overlaps with tiny request": {
			req: &PrometheusRangeQueryRequest{
				Start: 100,
				End:   106,
				Step:  2,
			},
			cachedExtents: []Extent{
				mkExtentWithStep(60, 64, 2),
				mkExtentWithStep(98, 102, 2),
				mkExtentWithStep(160, 166, 2),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStep(60, 64, 2),
				mkExtentWithStep(98, 106, 2),
				mkExtentWithStep(160, 166, 2),
			},
		},
		"Should merge fragmented extents if request fills the hole": {
			req: &PrometheusRangeQueryRequest{
				Start: 40,
				End:   80,
				Step:  20,
			},
			cachedExtents: []Extent{
				mkExtentWithStep(0, 20, 20),
				mkExtentWithStep(80, 100, 20),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStep(0, 100, 20),
			},
		},
		"Should left-extend extent if request starts earlier than extent in cache": {
			req: &PrometheusRangeQueryRequest{
				Start: 40,
				End:   80,
				Step:  20,
			},
			cachedExtents: []Extent{
				mkExtentWithStep(60, 160, 20),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStep(40, 160, 20),
			},
		},
		"Should right-extend extent if request ends later than extent in cache": {
			req: &PrometheusRangeQueryRequest{
				Start: 100,
				End:   180,
				Step:  20,
			},
			cachedExtents: []Extent{
				mkExtentWithStep(60, 160, 20),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStep(60, 180, 20),
			},
		},
		"Should not throw error if complete-overlapped smaller Extent is erroneous": {
			req: &PrometheusRangeQueryRequest{
				// This request is carefully crafted such that cachedEntry is not used to fulfill
				// the request.
				Start: 160,
				End:   180,
				Step:  20,
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
				mkExtentWithStep(60, 160, 20),
			},
			expectedUpdatedExtents: true,
			expectedCachedExtents: []Extent{
				mkExtentWithStep(60, 180, 20),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), userID)
			cacheBackend := cache.NewInstrumentedMockCache()
			cacheSplitter := constSplitter(day)

			mw := newSplitAndCacheMiddleware(
				false, // No splitting.
				true,
				24*time.Hour,
				false,
				mockLimits{},
				PrometheusCodec,
				cacheBackend,
				cacheSplitter,
				PrometheusResponseExtractor{},
				resultsCacheAlwaysEnabled,
				log.NewNopLogger(),
				prometheus.NewPedanticRegistry(),
			).Wrap(HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return mkAPIResponse(req.GetStart(), req.GetEnd(), req.GetStep()), nil
			})).(*splitAndCacheMiddleware)

			// Store all extents fixtures in the cache.
			cacheKey := cacheSplitter.GenerateCacheKey(userID, testData.req)
			mw.storeCacheExtents(ctx, cacheKey, testData.cachedExtents)

			// Run the request.
			actualRes, err := mw.Do(ctx, testData.req)
			require.NoError(t, err)

			expectedResponse := mkAPIResponse(testData.req.GetStart(), testData.req.GetEnd(), testData.req.GetStep())
			assert.Equal(t, expectedResponse, actualRes)

			// Check the updated cached extents.
			actualExtents := mw.fetchCacheExtents(ctx, []string{cacheKey})
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
		false,
		mockLimits{},
		PrometheusCodec,
		cacheBackend,
		constSplitter(day),
		PrometheusResponseExtractor{},
		resultsCacheAlwaysEnabled,
		log.NewNopLogger(),
		prometheus.NewPedanticRegistry(),
	).Wrap(nil).(*splitAndCacheMiddleware)

	ctx := context.Background()

	t.Run("fetchCacheExtents() should return a slice with the same number of input keys but empty extents on cache miss", func(t *testing.T) {
		actual := mw.fetchCacheExtents(ctx, []string{"key-1", "key-2", "key-3"})
		expected := [][]Extent{nil, nil, nil}
		assert.Equal(t, expected, actual)
	})

	t.Run("fetchCacheExtents() should return a slice with the same number of input keys and some extends filled up on partial cache hit", func(t *testing.T) {
		mw.storeCacheExtents(ctx, "key-1", []Extent{mkExtent(10, 20)})
		mw.storeCacheExtents(ctx, "key-3", []Extent{mkExtent(20, 30), mkExtent(40, 50)})

		actual := mw.fetchCacheExtents(ctx, []string{"key-1", "key-2", "key-3"})
		expected := [][]Extent{{mkExtent(10, 20)}, nil, {mkExtent(20, 30), mkExtent(40, 50)}}
		assert.Equal(t, expected, actual)
	})

	t.Run("fetchCacheExtents() should not return an extent if its key doesn't match the requested one (hash collision)", func(t *testing.T) {
		// Simulate an hash collision on "key-1".
		buf, err := proto.Marshal(&CachedResponse{Key: "another", Extents: []Extent{mkExtent(10, 20)}})
		require.NoError(t, err)
		cacheBackend.Store(ctx, map[string][]byte{cacheHashKey("key-1"): buf}, 0)

		mw.storeCacheExtents(ctx, "key-3", []Extent{mkExtent(20, 30), mkExtent(40, 50)})

		actual := mw.fetchCacheExtents(ctx, []string{"key-1", "key-2", "key-3"})
		expected := [][]Extent{nil, nil, {mkExtent(20, 30), mkExtent(40, 50)}}
		assert.Equal(t, expected, actual)
	})
}

func TestSplitAndCacheMiddleware_WrapMultipleTimes(t *testing.T) {
	m := newSplitAndCacheMiddleware(
		false,
		true,
		24*time.Hour,
		false,
		mockLimits{},
		PrometheusCodec,
		cache.NewMockCache(),
		constSplitter(day),
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
		expected []Request
	}{
		"should return an empty slice on no downstream requests": {
			input:    nil,
			expected: nil,
		},
		"should inject ID and hints on downstream requests and return them": {
			input: splitRequests{
				{downstreamRequests: []Request{&PrometheusRangeQueryRequest{Start: 1}, &PrometheusRangeQueryRequest{Start: 2}}},
				{downstreamRequests: []Request{}},
				{downstreamRequests: []Request{&PrometheusRangeQueryRequest{Start: 3}}},
			},
			expected: []Request{
				(&PrometheusRangeQueryRequest{Start: 1}).WithID(1).WithHints(&Hints{TotalQueries: 3}),
				(&PrometheusRangeQueryRequest{Start: 2}).WithID(2).WithHints(&Hints{TotalQueries: 3}),
				(&PrometheusRangeQueryRequest{Start: 3}).WithID(3).WithHints(&Hints{TotalQueries: 3}),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Pre-condition check: input requests should have responses not initialized.
			for _, req := range testData.input {
				require.Empty(t, req.downstreamResponses)
			}

			assert.Equal(t, testData.expected, testData.input.prepareDownstreamRequests())

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
				{downstreamRequests: []Request{}},
				{downstreamRequests: []Request{}},
			},
			responses: nil,
			expected: splitRequests{
				{downstreamRequests: []Request{}},
				{downstreamRequests: []Request{}},
			},
		},
		"should associate downstream responses to requests": {
			requests: splitRequests{{
				downstreamRequests:  []Request{&PrometheusRangeQueryRequest{Start: 1, Id: 1}, &PrometheusRangeQueryRequest{Start: 2, Id: 2}},
				downstreamResponses: []Response{nil, nil},
			}, {
				downstreamRequests:  []Request{},
				downstreamResponses: []Response{},
			}, {
				downstreamRequests:  []Request{&PrometheusRangeQueryRequest{Start: 3, Id: 3}},
				downstreamResponses: []Response{nil},
			}},
			responses: []requestResponse{{
				Request:  &PrometheusRangeQueryRequest{Start: 3, Id: 3},
				Response: &PrometheusResponse{Status: "response-3"},
			}, {
				Request:  &PrometheusRangeQueryRequest{Start: 1, Id: 1},
				Response: &PrometheusResponse{Status: "response-1"},
			}, {
				Request:  &PrometheusRangeQueryRequest{Start: 2, Id: 2},
				Response: &PrometheusResponse{Status: "response-2"},
			}},
			expected: splitRequests{{
				downstreamRequests:  []Request{&PrometheusRangeQueryRequest{Start: 1, Id: 1}, &PrometheusRangeQueryRequest{Start: 2, Id: 2}},
				downstreamResponses: []Response{&PrometheusResponse{Status: "response-1"}, &PrometheusResponse{Status: "response-2"}},
			}, {
				downstreamRequests:  []Request{},
				downstreamResponses: []Response{},
			}, {
				downstreamRequests:  []Request{&PrometheusRangeQueryRequest{Start: 3, Id: 3}},
				downstreamResponses: []Response{&PrometheusResponse{Status: "response-3"}},
			}},
		},
		"should return error if a downstream response is missing": {
			requests: splitRequests{{
				downstreamRequests:  []Request{&PrometheusRangeQueryRequest{Start: 1, Id: 1}, &PrometheusRangeQueryRequest{Start: 2, Id: 2}},
				downstreamResponses: []Response{nil, nil},
			}, {
				downstreamRequests:  []Request{&PrometheusRangeQueryRequest{Start: 3, Id: 3}},
				downstreamResponses: []Response{nil},
			}},
			responses: []requestResponse{{
				Request:  &PrometheusRangeQueryRequest{Start: 3, Id: 3},
				Response: &PrometheusResponse{Status: "response-3"},
			}, {
				Request:  &PrometheusRangeQueryRequest{Start: 2, Id: 2},
				Response: &PrometheusResponse{Status: "response-2"},
			}},
			expectedErr: "consistency check failed: missing downstream response",
		},
		"should return error if multiple downstream responses have the same ID": {
			requests: splitRequests{{
				downstreamRequests:  []Request{&PrometheusRangeQueryRequest{Start: 1, Id: 1}, &PrometheusRangeQueryRequest{Start: 2, Id: 2}},
				downstreamResponses: []Response{nil, nil},
			}, {
				downstreamRequests:  []Request{&PrometheusRangeQueryRequest{Start: 3, Id: 3}},
				downstreamResponses: []Response{nil},
			}},
			responses: []requestResponse{{
				Request:  &PrometheusRangeQueryRequest{Start: 3, Id: 3},
				Response: &PrometheusResponse{Status: "response-3"},
			}, {
				Request:  &PrometheusRangeQueryRequest{Start: 2, Id: 3},
				Response: &PrometheusResponse{Status: "response-2"},
			}},
			expectedErr: "consistency check failed: conflicting downstream request ID",
		},
		"should return error if extra downstream responses are requested to be stored": {
			requests: splitRequests{{
				downstreamRequests:  []Request{&PrometheusRangeQueryRequest{Start: 1, Id: 1}, &PrometheusRangeQueryRequest{Start: 2, Id: 2}},
				downstreamResponses: []Response{nil, nil},
			}, {
				downstreamRequests:  []Request{&PrometheusRangeQueryRequest{Start: 3, Id: 3}},
				downstreamResponses: []Response{nil},
			}},
			responses: []requestResponse{{
				Request:  &PrometheusRangeQueryRequest{Start: 3, Id: 3},
				Response: &PrometheusResponse{Status: "response-3"},
			}, {
				Request:  &PrometheusRangeQueryRequest{Start: 2, Id: 2},
				Response: &PrometheusResponse{Status: "response-2"},
			}, {
				Request:  &PrometheusRangeQueryRequest{Start: 1, Id: 1},
				Response: &PrometheusResponse{Status: "response-1"},
			}, {
				Request:  &PrometheusRangeQueryRequest{Start: 4, Id: 4},
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

func encodePrometheusResponse(t *testing.T, res *PrometheusResponse) string {
	encoded, err := json.Marshal(res)
	require.NoError(t, err)

	return string(encoded)
}

func newAssertHintsMiddleware(t *testing.T, expected *Hints) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return &assertHintsMiddleware{
			next:     next,
			t:        t,
			expected: expected,
		}
	})
}

type assertHintsMiddleware struct {
	next     Handler
	t        *testing.T
	expected *Hints
}

func (m *assertHintsMiddleware) Do(ctx context.Context, req Request) (Response, error) {
	assert.Equal(m.t, m.expected, req.GetHints())
	return m.next.Do(ctx, req)
}

type roundTripper struct {
	handler Handler
	codec   Codec
}

// newRoundTripper merges a set of middlewares into an handler, then inject it into the `next` roundtripper
// using the codec to translate requests and responses.
func newRoundTripper(next http.RoundTripper, codec Codec, logger log.Logger, middlewares ...Middleware) http.RoundTripper {
	return roundTripper{
		handler: MergeMiddlewares(middlewares...).Wrap(roundTripperHandler{
			logger: logger,
			next:   next,
			codec:  codec,
		}),
		codec: codec,
	}
}

func (q roundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	request, err := q.codec.DecodeRequest(r.Context(), r)
	if err != nil {
		return nil, err
	}

	if span := opentracing.SpanFromContext(r.Context()); span != nil {
		request.LogToSpan(span)
	}

	response, err := q.handler.Do(r.Context(), request)
	if err != nil {
		return nil, err
	}

	return q.codec.EncodeResponse(r.Context(), response)
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
	for i, tc := range []struct {
		input    Request
		expected []Request
		interval time.Duration
	}{
		{
			input: &PrometheusRangeQueryRequest{Start: 0, End: 60 * 60 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: 0, End: 60 * 60 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: 0, End: 60 * 60 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: 0, End: 60 * 60 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: 0, End: 24 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: 0, End: 24 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: 0, End: 3 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: 0, End: 3 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: 0, End: 2 * 24 * 3600 * seconds, Step: 15 * seconds, Query: "foo @ start()"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: 0, End: (24 * 3600 * seconds) - (15 * seconds), Step: 15 * seconds, Query: "foo @ 0.000"},
				&PrometheusRangeQueryRequest{Start: 24 * 3600 * seconds, End: 2 * 24 * 3600 * seconds, Step: 15 * seconds, Query: "foo @ 0.000"},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: 0, End: 2 * 3 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: 0, End: (3 * 3600 * seconds) - (15 * seconds), Step: 15 * seconds, Query: "foo"},
				&PrometheusRangeQueryRequest{Start: 3 * 3600 * seconds, End: 2 * 3 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: 3 * 3600 * seconds, End: 3 * 24 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: 3 * 3600 * seconds, End: (24 * 3600 * seconds) - (15 * seconds), Step: 15 * seconds, Query: "foo"},
				&PrometheusRangeQueryRequest{Start: 24 * 3600 * seconds, End: (2 * 24 * 3600 * seconds) - (15 * seconds), Step: 15 * seconds, Query: "foo"},
				&PrometheusRangeQueryRequest{Start: 2 * 24 * 3600 * seconds, End: 3 * 24 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: 2 * 3600 * seconds, End: 3 * 3 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: 2 * 3600 * seconds, End: (3 * 3600 * seconds) - (15 * seconds), Step: 15 * seconds, Query: "foo"},
				&PrometheusRangeQueryRequest{Start: 3 * 3600 * seconds, End: (2 * 3 * 3600 * seconds) - (15 * seconds), Step: 15 * seconds, Query: "foo"},
				&PrometheusRangeQueryRequest{Start: 2 * 3 * 3600 * seconds, End: 3 * 3 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-14T23:48:00Z"), End: timeToMillis(t, "2021-10-15T00:03:00Z"), Step: 5 * time.Minute.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-14T23:48:00Z"), End: timeToMillis(t, "2021-10-15T00:03:00Z"), Step: 5 * time.Minute.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-14T23:48:00Z"), End: timeToMillis(t, "2021-10-15T00:00:00Z"), Step: 6 * time.Minute.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-14T23:48:00Z"), End: timeToMillis(t, "2021-10-14T23:54:00Z"), Step: 6 * time.Minute.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T00:00:00Z"), End: timeToMillis(t, "2021-10-15T00:00:00Z"), Step: 6 * time.Minute.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-14T22:00:00Z"), End: timeToMillis(t, "2021-10-17T22:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-14T22:00:00Z"), End: timeToMillis(t, "2021-10-14T22:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T22:00:00Z"), End: timeToMillis(t, "2021-10-15T22:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-16T22:00:00Z"), End: timeToMillis(t, "2021-10-16T22:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-17T22:00:00Z"), End: timeToMillis(t, "2021-10-17T22:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T00:00:00Z"), End: timeToMillis(t, "2021-10-18T00:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T00:00:00Z"), End: timeToMillis(t, "2021-10-15T00:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-16T00:00:00Z"), End: timeToMillis(t, "2021-10-16T00:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-17T00:00:00Z"), End: timeToMillis(t, "2021-10-17T00:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-18T00:00:00Z"), End: timeToMillis(t, "2021-10-18T00:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T22:00:00Z"), End: timeToMillis(t, "2021-10-22T04:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T22:00:00Z"), End: timeToMillis(t, "2021-10-15T22:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-17T04:00:00Z"), End: timeToMillis(t, "2021-10-17T04:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-18T10:00:00Z"), End: timeToMillis(t, "2021-10-18T10:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-19T16:00:00Z"), End: timeToMillis(t, "2021-10-19T16:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-20T22:00:00Z"), End: timeToMillis(t, "2021-10-20T22:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-22T04:00:00Z"), End: timeToMillis(t, "2021-10-22T04:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-17T14:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-15T18:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-16T06:00:00Z"), End: timeToMillis(t, "2021-10-16T18:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-17T06:00:00Z"), End: timeToMillis(t, "2021-10-17T14:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-17T18:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-15T18:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-16T06:00:00Z"), End: timeToMillis(t, "2021-10-16T18:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-17T06:00:00Z"), End: timeToMillis(t, "2021-10-17T18:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-17T18:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-15T16:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-16T02:00:00Z"), End: timeToMillis(t, "2021-10-16T22:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-17T08:00:00Z"), End: timeToMillis(t, "2021-10-17T18:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-17T08:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-15T16:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-16T02:00:00Z"), End: timeToMillis(t, "2021-10-16T22:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRangeQueryRequest{Start: timeToMillis(t, "2021-10-17T08:00:00Z"), End: timeToMillis(t, "2021-10-17T08:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
	} {
		t.Run(fmt.Sprintf("%d: start: %v, end: %v, step: %v", i, tc.input.GetStart(), tc.input.GetEnd(), tc.input.GetStep()), func(t *testing.T) {
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
		{"sum by (foo) (bar[buzz])", "foo{}", apierror.New(apierror.TypeBadData, `1:19: parse error: bad duration syntax: ""`)},
	} {
		tt := tt
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
