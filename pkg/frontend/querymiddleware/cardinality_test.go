// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util"
)

func Test_cardinalityEstimateBucket_QueryRequest_keyFormat(t *testing.T) {
	requestTime := parseTimeRFC3339(t, "2023-01-09T03:24:12Z")
	hoursSinceEpoch := util.TimeToMillis(requestTime) / time.Hour.Milliseconds()
	daysSinceEpoch := hoursSinceEpoch / 24

	alphabet := "abcdefghijklmnopqrstuvwxyz"
	longUserID := strings.Join(func(it string) []string {
		tenantName := make([]string, len(it))
		for i, v := range it {
			tenantName[i] = fmt.Sprintf("tenant-%c", v)
		}
		return tenantName
	}(alphabet), "|")

	tests := []struct {
		name     string
		userID   string
		r        MetricsQueryRequest
		expected string
	}{
		{
			name:   "instant query",
			userID: "tenant-a",
			r: &PrometheusInstantQueryRequest{
				time:      requestTime.UnixMilli(),
				queryExpr: parseQuery(t, "up"),
			},
			expected: fmt.Sprintf("QS:%s:%s:%d:%d", cacheHashKey("tenant-a"), cacheHashKey("up"), daysSinceEpoch, 0),
		},
		{
			name:   "range query",
			userID: "tenant-b",
			r: &PrometheusRangeQueryRequest{
				start:     requestTime.UnixMilli(),
				end:       requestTime.Add(2 * time.Hour).UnixMilli(),
				queryExpr: parseQuery(t, "up"),
			},
			expected: fmt.Sprintf("QS:%s:%s:%d:%d", cacheHashKey("tenant-b"), cacheHashKey("up"), daysSinceEpoch, 0),
		},
		{
			name:   "range query with large range",
			userID: "tenant-b",
			r: &PrometheusRangeQueryRequest{
				start: requestTime.UnixMilli(),
				// Over 24 hours, range part should be 1
				end:       requestTime.Add(25 * time.Hour).UnixMilli(),
				queryExpr: parseQuery(t, "up"),
			},
			expected: fmt.Sprintf("QS:%s:%s:%d:%d", cacheHashKey("tenant-b"), cacheHashKey("up"), daysSinceEpoch, 1),
		},
		{
			name:   "long userID creates a valid key",
			userID: longUserID,
			r: &PrometheusRangeQueryRequest{
				start: requestTime.UnixMilli(),
				// Over 24 hours, range part should be 1
				end:       requestTime.Add(25 * time.Hour).UnixMilli(),
				queryExpr: parseQuery(t, "up"),
			},
			expected: fmt.Sprintf("QS:%s:%s:%d:%d", cacheHashKey(longUserID), cacheHashKey("up"), daysSinceEpoch, 1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			key := generateCardinalityEstimationCacheKey(tt.userID, tt.r, 24*time.Hour)
			assert.Equal(t, tt.expected, key)
			assert.LessOrEqual(t, len(key), 250, "Cardinality estimation cache key length should not exceed the maximum length")
		})
	}
}

func Test_cardinalityEstimation_lookupCardinalityForKey(t *testing.T) {
	ctx := context.Background()
	c := cache.NewInstrumentedMockCache()

	actualKey := fmt.Sprintf("QS:tenant-a:%s:1234:4321", cacheHashKey("up"))
	userID := "tenant-a"
	actualValue := uint64(25)

	expectedFetchCount := 0
	tests := []struct {
		cache               cache.Cache
		name                string
		key                 string
		userID              string
		expectedCardinality uint64
		expectedPresent     bool
	}{
		{
			cache:               nil,
			name:                "nil cache",
			key:                 actualKey,
			userID:              userID,
			expectedCardinality: 0,
			expectedPresent:     false,
		},
		{
			cache:               c,
			name:                "cache hit",
			key:                 actualKey,
			userID:              userID,
			expectedCardinality: actualValue,
			expectedPresent:     true,
		},
		{
			cache:               c,
			name:                "cache miss",
			key:                 "not present",
			userID:              userID,
			expectedCardinality: 0,
			expectedPresent:     false,
		},
		{
			cache:               c,
			name:                "cache hit but wrong userID",
			key:                 actualKey,
			userID:              "tenant-b",
			expectedCardinality: 0,
			expectedPresent:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ce := cardinalityEstimation{cache: tt.cache}
			ce.storeCardinalityForKey(actualKey, actualValue, userID)
			estimate, ok := ce.lookupCardinalityForKey(ctx, tt.key, tt.userID)
			if tt.cache != nil {
				expectedFetchCount++
			}
			assert.Equal(t, expectedFetchCount, c.CountFetchCalls())
			assert.Equal(t, tt.expectedCardinality, estimate)
			assert.Equal(t, tt.expectedPresent, ok)
		})
	}
}

func Test_cardinalityEstimation_Do(t *testing.T) {
	const numSeries = uint64(25)
	request := &PrometheusRangeQueryRequest{
		start:     parseTimeRFC3339(t, "2023-01-31T09:00:00Z").Unix() * 1000,
		end:       parseTimeRFC3339(t, "2023-01-31T10:00:00Z").Unix() * 1000,
		queryExpr: parseQuery(t, "up"),
	}
	addSeriesHandler := func(estimate, actual uint64) HandlerFunc {
		return func(ctx context.Context, request MetricsQueryRequest) (Response, error) {
			require.NotNil(t, request.GetHints())
			request.GetHints().GetCardinalityEstimate()
			require.Equal(t, request.GetHints().GetEstimatedSeriesCount(), estimate)

			queryStats := stats.FromContext(ctx)
			queryStats.AddFetchedSeries(actual)
			return &PrometheusResponse{}, nil
		}
	}
	marshaledEstimate := func(numSeries uint64, userID string) []byte {
		est, err := proto.Marshal(&QueryStatistics{EstimatedSeriesCount: numSeries, UserID: userID})
		require.Nil(t, err)
		return est
	}

	tests := []struct {
		name              string
		tenantID          string
		downstreamHandler HandlerFunc
		cacheContent      map[string][]byte
		expectedLoads     int
		expectedStores    int
		expectedErr       assert.ErrorAssertionFunc
	}{
		{
			name:     "no tenantID",
			tenantID: "",
			downstreamHandler: func(_ context.Context, _ MetricsQueryRequest) (Response, error) {
				return &PrometheusResponse{}, nil
			},
			expectedLoads:  0,
			expectedStores: 0,
			expectedErr:    assert.NoError,
		},
		{
			name:     "downstream error",
			tenantID: "1",
			downstreamHandler: func(_ context.Context, _ MetricsQueryRequest) (Response, error) {
				return nil, errors.New("test error")
			},
			expectedLoads:  1,
			expectedStores: 0,
			expectedErr:    assert.Error,
		},
		{
			name:              "with populated cache and unchanged cardinality",
			tenantID:          "1",
			downstreamHandler: addSeriesHandler(numSeries, numSeries),
			cacheContent:      map[string][]byte{generateCardinalityEstimationCacheKey("1", request, cardinalityEstimateBucketSize): marshaledEstimate(numSeries, "1")},
			expectedLoads:     1,
			expectedStores:    0,
			expectedErr:       assert.NoError,
		},
		{
			name:              "with populated cache and marginally changed cardinality",
			tenantID:          "1",
			downstreamHandler: addSeriesHandler(numSeries, numSeries+1),
			cacheContent:      map[string][]byte{generateCardinalityEstimationCacheKey("1", request, cardinalityEstimateBucketSize): marshaledEstimate(numSeries, "1")},
			expectedLoads:     1,
			expectedStores:    0,
			expectedErr:       assert.NoError,
		},
		{
			name:     "with populated cache and different userID",
			tenantID: "1",
			downstreamHandler: func(_ context.Context, _ MetricsQueryRequest) (Response, error) {
				return &PrometheusResponse{}, nil
			},
			cacheContent:   map[string][]byte{generateCardinalityEstimationCacheKey("1", request, cardinalityEstimateBucketSize): marshaledEstimate(numSeries, "2")},
			expectedLoads:  1,
			expectedStores: 1,
			expectedErr:    assert.NoError,
		},
		{
			name:              "with populated cache and significantly changed cardinality",
			tenantID:          "1",
			downstreamHandler: addSeriesHandler(numSeries, numSeries*2),
			cacheContent:      map[string][]byte{generateCardinalityEstimationCacheKey("1", request, cardinalityEstimateBucketSize): marshaledEstimate(numSeries, "1")},
			expectedLoads:     1,
			expectedStores:    1,
			expectedErr:       assert.NoError,
		},
		{
			name:     "with empty cache",
			tenantID: "1",
			downstreamHandler: func(ctx context.Context, _ MetricsQueryRequest) (Response, error) {
				queryStats := stats.FromContext(ctx)
				queryStats.AddFetchedSeries(numSeries)
				return &PrometheusResponse{}, nil
			},
			expectedLoads:  1,
			expectedStores: 1,
			expectedErr:    assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := cache.NewInstrumentedMockCache()
			mw := newCardinalityEstimationMiddleware(c, log.NewNopLogger(), nil)
			handler := mw.Wrap(tt.downstreamHandler)
			_, ctx := stats.ContextWithEmptyStats(context.Background())
			if tt.tenantID != "" {
				ctx = user.InjectOrgID(ctx, tt.tenantID)
			}
			numSetupStoreCalls := 0
			if len(tt.cacheContent) > 0 {
				c.SetMultiAsync(tt.cacheContent, time.Minute)
				numSetupStoreCalls++
			}

			_, err := handler.Do(ctx, request)

			tt.expectedErr(t, err)
			assert.Equal(t, tt.expectedLoads, c.CountFetchCalls())
			assert.Equal(t, numSetupStoreCalls+tt.expectedStores, c.CountStoreCalls())
		})
	}

}

func Test_cardinalityEstimateBucket_QueryRequest_requestEquality(t *testing.T) {
	rangeQuery := &PrometheusRangeQueryRequest{
		start:     util.TimeToMillis(parseTimeRFC3339(t, "2023-01-31T09:00:00Z")),
		end:       util.TimeToMillis(parseTimeRFC3339(t, "2023-01-31T10:00:00Z")),
		queryExpr: parseQuery(t, "up"),
	}
	rangeQuerySum, _ := rangeQuery.WithQuery("sum(up)")

	tests := []struct {
		name          string
		tenantA       string
		tenantB       string
		requestA      MetricsQueryRequest
		requestB      MetricsQueryRequest
		expectedEqual bool
	}{
		{
			name:          "same tenant, same request",
			tenantA:       "1",
			requestA:      rangeQuery,
			tenantB:       "1",
			requestB:      rangeQuery,
			expectedEqual: true,
		},
		{
			name:          "different tenant, same request",
			tenantA:       "1",
			tenantB:       "2",
			requestA:      rangeQuery,
			requestB:      rangeQuery,
			expectedEqual: false,
		},
		{
			name:          "same tenant, same query with start time in same bucket",
			tenantA:       "1",
			tenantB:       "1",
			requestA:      rangeQuery,
			requestB:      mustSucceed(rangeQuery.WithStartEnd(rangeQuery.GetStart()+5*time.Minute.Milliseconds(), rangeQuery.GetEnd()+5*time.Minute.Milliseconds())),
			expectedEqual: true,
		},
		{
			name:     "same tenant, same query with start time in different bucket",
			tenantA:  "1",
			tenantB:  "1",
			requestA: rangeQuery,
			requestB: mustSucceed(rangeQuery.WithStartEnd(
				rangeQuery.GetStart()+2*cardinalityEstimateBucketSize.Milliseconds(),
				rangeQuery.GetEnd()+2*cardinalityEstimateBucketSize.Milliseconds(),
			)),
			expectedEqual: false,
		},
		{
			name:          "same tenant, same query with start time in same bucket and range size in same bucket",
			tenantA:       "1",
			tenantB:       "1",
			requestA:      rangeQuery,
			requestB:      mustSucceed(rangeQuery.WithStartEnd(rangeQuery.GetStart(), rangeQuery.GetEnd()+time.Second.Milliseconds())),
			expectedEqual: true,
		},
		{
			name:     "same tenant, same query with start time in same bucket and range size in different bucket",
			tenantA:  "1",
			tenantB:  "1",
			requestA: rangeQuery,
			requestB: mustSucceed(rangeQuery.WithStartEnd(
				rangeQuery.GetStart()+5*time.Minute.Milliseconds(),
				rangeQuery.GetEnd()+2*cardinalityEstimateBucketSize.Milliseconds(),
			)),
			expectedEqual: false,
		},
		// The following two test cases test consistent hashing of queries, which is used
		// to avoid expiration of all estimates at the same time (i.e., the bucket boundary).
		{
			name:     "same tenant, same query with start time less than a bucket width apart but in different buckets",
			tenantA:  "1",
			tenantB:  "1",
			requestA: rangeQuerySum,
			requestB: mustSucceed(rangeQuerySum.WithStartEnd(
				rangeQuery.GetStart()+(cardinalityEstimateBucketSize/2).Milliseconds(),
				rangeQuery.GetEnd()+(cardinalityEstimateBucketSize/2).Milliseconds(),
			)),
			expectedEqual: false,
		},
		{
			name:     "same tenant, same query with start time less than a bucket width apart and in the same bucket",
			tenantA:  "1",
			tenantB:  "1",
			requestA: rangeQuery,
			requestB: mustSucceed(rangeQuery.WithStartEnd(
				rangeQuery.GetStart()+(cardinalityEstimateBucketSize/2).Milliseconds(),
				rangeQuery.GetEnd()+(cardinalityEstimateBucketSize/2).Milliseconds(),
			)),
			expectedEqual: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			keyA := generateCardinalityEstimationCacheKey(tt.tenantA, tt.requestA, cardinalityEstimateBucketSize)
			keyB := generateCardinalityEstimationCacheKey(tt.tenantB, tt.requestB, cardinalityEstimateBucketSize)
			if tt.expectedEqual {
				assert.Equal(t, keyA, keyB)
			} else {
				assert.NotEqual(t, keyA, keyB)
			}
		})
	}
}

func Test_newCardinalityEstimationMiddleware_canWrapMoreThanOnce(t *testing.T) {
	req := &PrometheusRangeQueryRequest{}

	mw := newCardinalityEstimationMiddleware(nil, log.NewNopLogger(), prometheus.NewRegistry())

	require.NotPanics(t, func() {
		_, err := mw.Wrap(mockHandlerWith(nil, nil)).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.Nil(t, err)
		_, err = mw.Wrap(mockHandlerWith(nil, nil)).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.Nil(t, err)
	})
}
