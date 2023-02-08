// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/limits_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util"
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
		"should not manipulate time range if creation grace period is disabled": {
			reqStartTime:        now.Add(-time.Hour),
			reqEndTime:          now.Add(2 * time.Hour),
			creationGracePeriod: 0,
			expectedEndTime:     now.Add(2 * time.Hour),
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

type mockLimits struct {
	maxQueryLookback               time.Duration
	maxQueryLength                 time.Duration
	maxTotalQueryLength            time.Duration
	maxCacheFreshness              time.Duration
	maxQueryParallelism            int
	maxShardedQueries              int
	splitInstantQueriesInterval    time.Duration
	totalShards                    int
	compactorShards                int
	compactorBlocksRetentionPeriod time.Duration
	outOfOrderTimeWindow           model.Duration
	creationGracePeriod            time.Duration
}

func (m mockLimits) MaxQueryLookback(string) time.Duration {
	return m.maxQueryLookback
}

func (m mockLimits) MaxQueryLength(string) time.Duration {
	return m.maxQueryLength
}

func (m mockLimits) MaxTotalQueryLength(string) time.Duration {
	if m.maxTotalQueryLength == time.Duration(0) {
		return m.maxQueryLength
	}
	return m.maxTotalQueryLength
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

func (m mockLimits) SplitInstantQueriesByInterval(string) time.Duration {
	return m.splitInstantQueriesInterval
}

func (m mockLimits) CompactorSplitAndMergeShards(userID string) int {
	return m.compactorShards
}

func (m mockLimits) CompactorBlocksRetentionPeriod(userID string) time.Duration {
	return m.compactorBlocksRetentionPeriod
}

func (m mockLimits) OutOfOrderTimeWindow(userID string) model.Duration {
	return m.outOfOrderTimeWindow
}

func (m mockLimits) CreationGracePeriod(userID string) time.Duration {
	return m.creationGracePeriod
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
		Path:  "/query_range",
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
		Path:  "/query_range",
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
		Path:  "/query_range",
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
