// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/limits_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestLimitsMiddleware_MaxQueryLookback(t *testing.T) {
	const (
		thirtyDays = 30 * 24 * time.Hour
	)

	now := time.Now()

	tests := map[string]struct {
		maxQueryLookback      time.Duration
		blocksRetentionPeriod time.Duration
		reqStartTime          time.Time
		reqEndTime            time.Time
		expectedSkipped       bool
		expectedStartTime     time.Time
		expectedEndTime       time.Time
	}{
		"should not manipulate time range if max lookback is disabled": {
			maxQueryLookback:      0,
			blocksRetentionPeriod: 0,
			reqStartTime:          time.Unix(0, 0),
			reqEndTime:            now,
			expectedStartTime:     time.Unix(0, 0),
			expectedEndTime:       now,
		},
		"should not manipulate time range for a query on short time range": {
			maxQueryLookback:      thirtyDays,
			blocksRetentionPeriod: thirtyDays,
			reqStartTime:          now.Add(-time.Hour),
			reqEndTime:            now,
			expectedStartTime:     now.Add(-time.Hour),
			expectedEndTime:       now,
		},
		"should not manipulate a query on large time range close to the limit": {
			maxQueryLookback:      thirtyDays,
			blocksRetentionPeriod: thirtyDays,
			reqStartTime:          now.Add(-thirtyDays).Add(time.Hour),
			reqEndTime:            now,
			expectedStartTime:     now.Add(-thirtyDays).Add(time.Hour),
			expectedEndTime:       now,
		},
		"should manipulate a query on large time range over the limit": {
			maxQueryLookback:      thirtyDays,
			blocksRetentionPeriod: thirtyDays,
			reqStartTime:          now.Add(-thirtyDays).Add(-100 * time.Hour),
			reqEndTime:            now,
			expectedStartTime:     now.Add(-thirtyDays),
			expectedEndTime:       now,
		},
		"should skip executing a query outside the allowed time range": {
			maxQueryLookback:      thirtyDays,
			blocksRetentionPeriod: thirtyDays,
			reqStartTime:          now.Add(-thirtyDays).Add(-100 * time.Hour),
			reqEndTime:            now.Add(-thirtyDays).Add(-90 * time.Hour),
			expectedSkipped:       true,
		},
		"should manipulate a query where maxQueryLookback is past the retention period": {
			maxQueryLookback:      thirtyDays,
			blocksRetentionPeriod: thirtyDays - (24 * time.Hour),
			reqStartTime:          now.Add(-thirtyDays),
			reqEndTime:            now,
			expectedStartTime:     now.Add(-thirtyDays).Add(24 * time.Hour),
			expectedEndTime:       now,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &PrometheusRangeQueryRequest{
				Start: util.TimeToMillis(testData.reqStartTime),
				End:   util.TimeToMillis(testData.reqEndTime),
			}

			limits := mockLimits{maxQueryLookback: testData.maxQueryLookback, compactorBlocksRetentionPeriod: testData.blocksRetentionPeriod}
			middleware := newLimitsMiddleware(limits, log.NewNopLogger())

			innerRes := newEmptyPrometheusResponse()
			inner := &mockHandler{}
			inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

			ctx := user.InjectOrgID(context.Background(), "test")
			outer := middleware.Wrap(inner)
			res, err := outer.Do(ctx, req)
			require.NoError(t, err)

			if testData.expectedSkipped {
				// We expect an empty response, but not the one returned by the inner handler
				// which we expect has been skipped.
				assert.NotSame(t, innerRes, res)
				assert.Len(t, inner.Calls, 0)
			} else {
				// We expect the response returned by the inner handler.
				assert.Same(t, innerRes, res)

				// Assert on the time range of the request passed to the inner handler (5s delta).
				delta := float64(5000)
				require.Len(t, inner.Calls, 1)

				assert.InDelta(t, util.TimeToMillis(testData.expectedStartTime), inner.Calls[0].Arguments.Get(1).(Request).GetStart(), delta)
				assert.InDelta(t, util.TimeToMillis(testData.expectedEndTime), inner.Calls[0].Arguments.Get(1).(Request).GetEnd(), delta)
			}
		})
	}
}

func TestLimitsMiddleware_MaxQueryExpressionSizeBytes(t *testing.T) {
	now := time.Now()

	tests := map[string]struct {
		query       string
		queryLimits map[string]int
		expectError bool
	}{
		"should fail for queries longer than the limit": {
			query:       fmt.Sprintf("up{foo=\"%s\"}", strings.Repeat("a", 1000)),
			queryLimits: map[string]int{"test1": 100, "test2": 100},
			expectError: true,
		},
		"should fail for queries longer than a one tenant limit": {
			query:       fmt.Sprintf("up{foo=\"%s\"}", strings.Repeat("a", 1000)),
			queryLimits: map[string]int{"test1": 100, "test2": 2000},
			expectError: true,
		},
		"should fail for queries longer than a one tenant limit with one limit disabled": {
			query:       fmt.Sprintf("up{foo=\"%s\"}", strings.Repeat("a", 1000)),
			queryLimits: map[string]int{"test1": 100, "test2": 0},
			expectError: true,
		},
		"should work for queries under the limit": {
			query:       fmt.Sprintf("up{foo=\"%s\"}", strings.Repeat("a", 50)),
			queryLimits: map[string]int{"test1": 100, "test2": 100},
			expectError: false,
		},
		"should work for queries when the limit is disabled": {
			query:       fmt.Sprintf("up{foo=\"%s\"}", strings.Repeat("a", 50)),
			queryLimits: map[string]int{"test1": 0, "test2": 0},
			expectError: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &PrometheusRangeQueryRequest{
				Query: testData.query,
				Start: util.TimeToMillis(now.Add(-time.Hour * 2)),
				End:   util.TimeToMillis(now.Add(-time.Hour)),
			}

			limits := multiTenantMockLimits{
				byTenant: map[string]mockLimits{
					"test1": {maxQueryExpressionSizeBytes: testData.queryLimits["test1"]},
					"test2": {maxQueryExpressionSizeBytes: testData.queryLimits["test2"]},
				},
			}
			middleware := newLimitsMiddleware(limits, log.NewNopLogger())

			innerRes := newEmptyPrometheusResponse()
			inner := &mockHandler{}
			inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

			ctx := user.InjectOrgID(context.Background(), "test1|test2")
			outer := middleware.Wrap(inner)
			res, err := outer.Do(ctx, req)

			if testData.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "err-mimir-max-query-expression-size-bytes")
			} else {
				require.NoError(t, err)
				require.Same(t, innerRes, res)
			}
		})
	}
}

func TestLimitsMiddleware_MaxQueryLength(t *testing.T) {
	const (
		thirtyDays = 30 * 24 * time.Hour
	)

	now := time.Now()

	tests := map[string]struct {
		maxQueryLength      time.Duration
		maxTotalQueryLength time.Duration
		reqStartTime        time.Time
		reqEndTime          time.Time
		expectedErr         string
	}{
		"should skip validation if max length is disabled": {
			maxQueryLength: 0,
			reqStartTime:   time.Unix(0, 0),
			reqEndTime:     now,
		},
		"should succeed on a query on short time range, ending now": {
			maxQueryLength: thirtyDays,
			reqStartTime:   now.Add(-time.Hour),
			reqEndTime:     now,
		},
		"should succeed on a query on short time range, ending in the past": {
			maxQueryLength: thirtyDays,
			reqStartTime:   now.Add(-2 * thirtyDays).Add(-time.Hour),
			reqEndTime:     now.Add(-2 * thirtyDays),
		},
		"should succeed on a query on large time range close to the limit, ending now": {
			maxQueryLength: thirtyDays,
			reqStartTime:   now.Add(-thirtyDays).Add(time.Hour),
			reqEndTime:     now,
		},
		"should fail on a query on large time range over the limit, ending now": {
			maxQueryLength: thirtyDays,
			reqStartTime:   now.Add(-thirtyDays).Add(-100 * time.Hour),
			reqEndTime:     now,
			expectedErr:    "the total query time range exceeds the limit",
		},
		"should fail on a query on large time range over the limit, ending in the past": {
			maxQueryLength: thirtyDays,
			reqStartTime:   now.Add(-4 * thirtyDays),
			reqEndTime:     now.Add(-2 * thirtyDays),
			expectedErr:    "the total query time range exceeds the limit",
		},
		"should succeed if total query length is higher than query length limit": {
			maxQueryLength:      thirtyDays,
			maxTotalQueryLength: 8 * thirtyDays,
			reqStartTime:        now.Add(-4 * thirtyDays),
			reqEndTime:          now.Add(-2 * thirtyDays),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &PrometheusRangeQueryRequest{
				Start: util.TimeToMillis(testData.reqStartTime),
				End:   util.TimeToMillis(testData.reqEndTime),
			}

			limits := mockLimits{maxQueryLength: testData.maxQueryLength, maxTotalQueryLength: testData.maxTotalQueryLength}
			middleware := newLimitsMiddleware(limits, log.NewNopLogger())

			innerRes := newEmptyPrometheusResponse()
			inner := &mockHandler{}
			inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

			ctx := user.InjectOrgID(context.Background(), "test")
			outer := middleware.Wrap(inner)
			res, err := outer.Do(ctx, req)

			if testData.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), testData.expectedErr)
				assert.Nil(t, res)
				assert.Len(t, inner.Calls, 0)
			} else {
				// We expect the response returned by the inner handler.
				require.NoError(t, err)
				assert.Same(t, innerRes, res)

				// The time range of the request passed to the inner handler should have not been manipulated.
				require.Len(t, inner.Calls, 1)
				assert.Equal(t, util.TimeToMillis(testData.reqStartTime), inner.Calls[0].Arguments.Get(1).(Request).GetStart())
				assert.Equal(t, util.TimeToMillis(testData.reqEndTime), inner.Calls[0].Arguments.Get(1).(Request).GetEnd())
			}
		})
	}
}

func TestLimitsMiddleware_CreationGracePeriod(t *testing.T) {
	now := time.Now()

	tests := map[string]struct {
		reqStartTime        time.Time
		reqEndTime          time.Time
		creationGracePeriod time.Duration
		expectedEndTime     time.Time
	}{
		"should manipulate time range if creation grace period is set to 0": {
			reqStartTime:        now.Add(-time.Hour),
			reqEndTime:          now.Add(2 * time.Hour),
			creationGracePeriod: 0,
			expectedEndTime:     now,
		},
		"should not manipulate time range for a query in now + creation_grace_period": {
			reqStartTime:        now.Add(-time.Hour),
			reqEndTime:          now.Add(30 * time.Minute),
			creationGracePeriod: time.Hour,
			expectedEndTime:     now.Add(30 * time.Minute),
		},
		"should manipulate time range for a query over now + creation_grace_period": {
			reqStartTime:        now.Add(-time.Hour),
			reqEndTime:          now.Add(2 * time.Hour),
			creationGracePeriod: time.Hour,
			expectedEndTime:     now.Add(time.Hour),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &PrometheusRangeQueryRequest{
				Start: util.TimeToMillis(testData.reqStartTime),
				End:   util.TimeToMillis(testData.reqEndTime),
			}

			limits := mockLimits{creationGracePeriod: testData.creationGracePeriod}
			middleware := newLimitsMiddleware(limits, log.NewNopLogger())

			innerRes := newEmptyPrometheusResponse()
			inner := &mockHandler{}
			inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

			ctx := user.InjectOrgID(context.Background(), "test")
			outer := middleware.Wrap(inner)
			res, err := outer.Do(ctx, req)
			require.NoError(t, err)

			// We expect the response returned by the inner handler.
			assert.Same(t, innerRes, res)

			// Assert on the time range of the request passed to the inner handler (5s delta).
			delta := float64(5000)
			require.Len(t, inner.Calls, 1)

			assert.InDelta(t, util.TimeToMillis(testData.expectedEndTime), inner.Calls[0].Arguments.Get(1).(Request).GetEnd(), delta)
		})
	}
}

type multiTenantMockLimits struct {
	byTenant map[string]mockLimits
}

func (m multiTenantMockLimits) MaxQueryLookback(userID string) time.Duration {
	return m.byTenant[userID].maxQueryLookback
}

func (m multiTenantMockLimits) MaxQueryLength(userID string) time.Duration {
	return m.byTenant[userID].maxQueryLength
}

func (m multiTenantMockLimits) MaxTotalQueryLength(userID string) time.Duration {
	return m.byTenant[userID].maxTotalQueryLength
}

func (m multiTenantMockLimits) MaxQueryExpressionSizeBytes(userID string) int {
	return m.byTenant[userID].maxQueryExpressionSizeBytes
}

func (m multiTenantMockLimits) MaxQueryParallelism(userID string) int {
	return m.byTenant[userID].maxQueryParallelism
}

func (m multiTenantMockLimits) MaxCacheFreshness(userID string) time.Duration {
	return m.byTenant[userID].maxCacheFreshness
}

func (m multiTenantMockLimits) QueryShardingTotalShards(userID string) int {
	return m.byTenant[userID].totalShards
}

func (m multiTenantMockLimits) QueryShardingMaxShardedQueries(userID string) int {
	return m.byTenant[userID].maxShardedQueries
}

func (m multiTenantMockLimits) QueryShardingMaxRegexpSizeBytes(userID string) int {
	return m.byTenant[userID].maxRegexpSizeBytes
}

func (m multiTenantMockLimits) SplitInstantQueriesByInterval(userID string) time.Duration {
	return m.byTenant[userID].splitInstantQueriesInterval
}

func (m multiTenantMockLimits) CompactorSplitAndMergeShards(userID string) int {
	return m.byTenant[userID].compactorShards
}

func (m multiTenantMockLimits) CompactorBlocksRetentionPeriod(userID string) time.Duration {
	return m.byTenant[userID].compactorBlocksRetentionPeriod
}

func (m multiTenantMockLimits) OutOfOrderTimeWindow(userID string) time.Duration {
	return m.byTenant[userID].outOfOrderTimeWindow
}

func (m multiTenantMockLimits) ResultsCacheTTL(userID string) time.Duration {
	return m.byTenant[userID].resultsCacheTTL
}

func (m multiTenantMockLimits) ResultsCacheTTLForOutOfOrderTimeWindow(userID string) time.Duration {
	return m.byTenant[userID].resultsCacheOutOfOrderWindowTTL
}

func (m multiTenantMockLimits) ResultsCacheTTLForCardinalityQuery(userID string) time.Duration {
	return m.byTenant[userID].resultsCacheTTLForCardinalityQuery
}

func (m multiTenantMockLimits) ResultsCacheTTLForLabelsQuery(userID string) time.Duration {
	return m.byTenant[userID].resultsCacheTTLForLabelsQuery
}

func (m multiTenantMockLimits) ResultsCacheForUnalignedQueryEnabled(userID string) bool {
	return m.byTenant[userID].resultsCacheForUnalignedQueryEnabled
}

func (m multiTenantMockLimits) BlockedQueries(userID string) []*validation.BlockedQuery {
	return m.byTenant[userID].blockedQueries
}

func (m multiTenantMockLimits) CreationGracePeriod(userID string) time.Duration {
	return m.byTenant[userID].creationGracePeriod
}

func (m multiTenantMockLimits) NativeHistogramsIngestionEnabled(userID string) bool {
	return m.byTenant[userID].nativeHistogramsIngestionEnabled
}

func (m multiTenantMockLimits) AlignQueriesWithStep(userID string) bool {
	return m.byTenant[userID].alignQueriesWithStep
}

func (m multiTenantMockLimits) QueryIngestersWithin(userID string) time.Duration {
	return m.byTenant[userID].queryIngestersWithin
}

type mockLimits struct {
	maxQueryLookback                     time.Duration
	maxQueryLength                       time.Duration
	maxTotalQueryLength                  time.Duration
	maxQueryExpressionSizeBytes          int
	maxCacheFreshness                    time.Duration
	maxQueryParallelism                  int
	maxShardedQueries                    int
	maxRegexpSizeBytes                   int
	splitInstantQueriesInterval          time.Duration
	totalShards                          int
	compactorShards                      int
	compactorBlocksRetentionPeriod       time.Duration
	outOfOrderTimeWindow                 time.Duration
	creationGracePeriod                  time.Duration
	nativeHistogramsIngestionEnabled     bool
	resultsCacheTTL                      time.Duration
	resultsCacheOutOfOrderWindowTTL      time.Duration
	resultsCacheTTLForCardinalityQuery   time.Duration
	resultsCacheTTLForLabelsQuery        time.Duration
	resultsCacheForUnalignedQueryEnabled bool
	blockedQueries                       []*validation.BlockedQuery
	alignQueriesWithStep                 bool
	queryIngestersWithin                 time.Duration
}

func (m mockLimits) MaxQueryLookback(string) time.Duration {
	return m.maxQueryLookback
}

func (m mockLimits) MaxTotalQueryLength(string) time.Duration {
	if m.maxTotalQueryLength == time.Duration(0) {
		return m.maxQueryLength
	}
	return m.maxTotalQueryLength
}

func (m mockLimits) MaxQueryExpressionSizeBytes(string) int {
	return m.maxQueryExpressionSizeBytes
}

func (m mockLimits) MaxQueryParallelism(string) int {
	if m.maxQueryParallelism == 0 {
		return 14 // Flag default.
	}
	return m.maxQueryParallelism
}

func (m mockLimits) MaxCacheFreshness(string) time.Duration {
	return m.maxCacheFreshness
}

func (m mockLimits) QueryShardingTotalShards(string) int {
	return m.totalShards
}

func (m mockLimits) QueryShardingMaxShardedQueries(string) int {
	return m.maxShardedQueries
}

func (m mockLimits) QueryShardingMaxRegexpSizeBytes(string) int {
	return m.maxRegexpSizeBytes
}

func (m mockLimits) SplitInstantQueriesByInterval(string) time.Duration {
	return m.splitInstantQueriesInterval
}

func (m mockLimits) CompactorSplitAndMergeShards(string) int {
	return m.compactorShards
}

func (m mockLimits) CompactorBlocksRetentionPeriod(string) time.Duration {
	return m.compactorBlocksRetentionPeriod
}

func (m mockLimits) OutOfOrderTimeWindow(string) time.Duration {
	return m.outOfOrderTimeWindow
}

func (m mockLimits) ResultsCacheTTL(string) time.Duration {
	return m.resultsCacheTTL
}

func (m mockLimits) ResultsCacheTTLForOutOfOrderTimeWindow(string) time.Duration {
	return m.resultsCacheOutOfOrderWindowTTL
}

func (m mockLimits) ResultsCacheTTLForCardinalityQuery(string) time.Duration {
	return m.resultsCacheTTLForCardinalityQuery
}

func (m mockLimits) BlockedQueries(string) []*validation.BlockedQuery {
	return m.blockedQueries
}

func (m mockLimits) ResultsCacheTTLForLabelsQuery(string) time.Duration {
	return m.resultsCacheTTLForLabelsQuery
}

func (m mockLimits) ResultsCacheForUnalignedQueryEnabled(string) bool {
	return m.resultsCacheForUnalignedQueryEnabled
}

func (m mockLimits) CreationGracePeriod(string) time.Duration {
	return m.creationGracePeriod
}

func (m mockLimits) NativeHistogramsIngestionEnabled(string) bool {
	return m.nativeHistogramsIngestionEnabled
}

func (m mockLimits) AlignQueriesWithStep(string) bool {
	return m.alignQueriesWithStep
}

func (m mockLimits) QueryIngestersWithin(string) time.Duration {
	return m.queryIngestersWithin
}

type mockHandler struct {
	mock.Mock
}

func (m *mockHandler) Do(ctx context.Context, req Request) (Response, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(Response), args.Error(1)
}

func TestLimitedRoundTripper_MaxQueryParallelism(t *testing.T) {
	var (
		maxQueryParallelism = 2
		count               atomic.Int32
		max                 atomic.Int32
		downstream          = RoundTripFunc(func(_ *http.Request) (*http.Response, error) {
			cur := count.Inc()
			if cur > max.Load() {
				max.Store(cur)
			}
			defer count.Dec()
			// simulate some work
			time.Sleep(20 * time.Millisecond)
			return &http.Response{
				Body: http.NoBody,
			}, nil
		})
		ctx = user.InjectOrgID(context.Background(), "foo")
	)

	codec := newTestPrometheusCodec()
	r, err := codec.EncodeRequest(ctx, &PrometheusRangeQueryRequest{
		Path:  "/api/v1/query_range",
		Start: time.Now().Add(time.Hour).Unix(),
		End:   util.TimeToMillis(time.Now()),
		Step:  int64(1 * time.Second * time.Millisecond),
		Query: `foo`,
	})
	require.Nil(t, err)

	_, err = newLimitedParallelismRoundTripper(downstream, codec, mockLimits{maxQueryParallelism: maxQueryParallelism},
		MiddlewareFunc(func(next Handler) Handler {
			return HandlerFunc(func(c context.Context, _ Request) (Response, error) {
				var wg sync.WaitGroup
				for i := 0; i < maxQueryParallelism+20; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, _ = next.Do(c, &PrometheusRangeQueryRequest{})
					}()
				}
				wg.Wait()
				return newEmptyPrometheusResponse(), nil
			})
		}),
	).RoundTrip(r)
	require.NoError(t, err)
	maxFound := int(max.Load())
	require.LessOrEqual(t, maxFound, maxQueryParallelism, "max query parallelism: ", maxFound, " went over the configured one:", maxQueryParallelism)
}

func TestLimitedRoundTripper_MaxQueryParallelismLateScheduling(t *testing.T) {
	var (
		maxQueryParallelism = 2
		downstream          = RoundTripFunc(func(_ *http.Request) (*http.Response, error) {
			// simulate some work
			time.Sleep(20 * time.Millisecond)
			return &http.Response{
				Body: http.NoBody,
			}, nil
		})
		ctx = user.InjectOrgID(context.Background(), "foo")
	)

	codec := newTestPrometheusCodec()
	r, err := codec.EncodeRequest(ctx, &PrometheusRangeQueryRequest{
		Path:  "/api/v1/query_range",
		Start: time.Now().Add(time.Hour).Unix(),
		End:   util.TimeToMillis(time.Now()),
		Step:  int64(1 * time.Second * time.Millisecond),
		Query: `foo`,
	})
	require.Nil(t, err)

	_, err = newLimitedParallelismRoundTripper(downstream, codec, mockLimits{maxQueryParallelism: maxQueryParallelism},
		MiddlewareFunc(func(next Handler) Handler {
			return HandlerFunc(func(c context.Context, _ Request) (Response, error) {
				// fire up work and we don't wait.
				for i := 0; i < 10; i++ {
					go func() {
						_, _ = next.Do(c, &PrometheusRangeQueryRequest{})
					}()
				}
				return newEmptyPrometheusResponse(), nil
			})
		}),
	).RoundTrip(r)
	require.NoError(t, err)
}

func TestLimitedRoundTripper_OriginalRequestContextCancellation(t *testing.T) {
	var (
		maxQueryParallelism = 2
		downstream          = RoundTripFunc(func(req *http.Request) (*http.Response, error) {
			// Sleep for a long time or until the request context is canceled.
			select {
			case <-time.After(time.Minute):
				return &http.Response{Body: http.NoBody}, nil
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		})
		reqCtx, reqCancel = context.WithCancel(user.InjectOrgID(context.Background(), "foo"))
	)

	codec := newTestPrometheusCodec()
	r, err := codec.EncodeRequest(reqCtx, &PrometheusRangeQueryRequest{
		Path:  "/api/v1/query_range",
		Start: time.Now().Add(time.Hour).Unix(),
		End:   util.TimeToMillis(time.Now()),
		Step:  int64(1 * time.Second * time.Millisecond),
		Query: `foo`,
	})
	require.Nil(t, err)

	_, err = newLimitedParallelismRoundTripper(downstream, codec, mockLimits{maxQueryParallelism: maxQueryParallelism},
		MiddlewareFunc(func(next Handler) Handler {
			return HandlerFunc(func(c context.Context, _ Request) (Response, error) {
				var wg sync.WaitGroup

				// Fire up some work. Each sub-request will either be blocked in the sleep or in the queue
				// waiting to be scheduled.
				for i := 0; i < maxQueryParallelism+20; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, _ = next.Do(c, &PrometheusRangeQueryRequest{})
					}()
				}

				// Give it a bit a time to get the first sub-requests running.
				time.Sleep(100 * time.Millisecond)

				// Cancel the original request context.
				reqCancel()

				// Wait until all sub-requests have done. We expect all of them to cancel asap,
				// so it should take a very short time.
				waitStart := time.Now()
				wg.Wait()
				assert.Less(t, time.Since(waitStart).Milliseconds(), int64(100))

				return newEmptyPrometheusResponse(), nil
			})
		}),
	).RoundTrip(r)
	require.NoError(t, err)
}

func BenchmarkLimitedParallelismRoundTripper(b *testing.B) {
	maxParallelism := 10
	workDuration := 20 * time.Millisecond
	ctx := user.InjectOrgID(context.Background(), "test-org-id")

	downstream := RoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		// Simulate some work
		time.Sleep(workDuration)
		return &http.Response{
			Body: http.NoBody,
		}, nil
	})

	codec := newTestPrometheusCodec()
	r, err := codec.EncodeRequest(ctx, &PrometheusRangeQueryRequest{
		Path:  "/api/v1/query_range",
		Start: time.Now().Add(time.Hour).Unix(),
		End:   util.TimeToMillis(time.Now()),
		Step:  int64(1 * time.Second * time.Millisecond),
		Query: `foo`,
	})
	require.Nil(b, err)

	for _, concurrentRequestCount := range []int{1, 10, 100} {
		for _, subRequestCount := range []int{1, 2, 5, 10, 20, 50, 100} {
			tripper := newLimitedParallelismRoundTripper(downstream, codec, mockLimits{maxQueryParallelism: maxParallelism},
				MiddlewareFunc(func(next Handler) Handler {
					return HandlerFunc(func(c context.Context, _ Request) (Response, error) {
						wg := sync.WaitGroup{}
						for i := 0; i < subRequestCount; i++ {
							wg.Add(1)
							go func() {
								defer wg.Done()
								_, _ = next.Do(c, &PrometheusRangeQueryRequest{})
							}()
						}
						wg.Wait()
						return newEmptyPrometheusResponse(), nil
					})
				}),
			)

			b.Run(fmt.Sprintf("%v concurrent requests with %v sub requests each, max parallelism %v, sub request duration %v", concurrentRequestCount, subRequestCount, maxParallelism, workDuration.String()), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					wg := sync.WaitGroup{}

					for i := 0; i < concurrentRequestCount; i++ {
						wg.Add(1)
						go func() {
							defer wg.Done()
							_, err := tripper.RoundTrip(r)

							if err != nil {
								require.NoError(b, err)
							}
						}()
					}

					wg.Wait()
				}
			})
		}
	}
}
