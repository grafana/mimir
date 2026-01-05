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
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/chunkinfologger"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestLimitsMiddleware_MaxQueryLookback_RangeQueryAndRemoteRead(t *testing.T) {
	const (
		thirtyDays = 30 * 24 * time.Hour
		sixtyDays  = 60 * 24 * time.Hour
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
		"should not manipulate time range if maxQueryLookback and blocksRetentionPeriod are both disabled": {
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
		"should manipulate a query on large time range over the maxQueryLookback limit, and blocksRetentionPeriod is not set": {
			maxQueryLookback:      thirtyDays,
			blocksRetentionPeriod: 0,
			reqStartTime:          now.Add(-thirtyDays).Add(-100 * time.Hour),
			reqEndTime:            now,
			expectedStartTime:     now.Add(-thirtyDays),
			expectedEndTime:       now,
		},
		"should manipulate a query on large time range over the blocksRetentionPeriod, and maxQueryLookback limit is not set": {
			maxQueryLookback:      0,
			blocksRetentionPeriod: thirtyDays,
			reqStartTime:          now.Add(-thirtyDays).Add(-100 * time.Hour),
			reqEndTime:            now,
			expectedStartTime:     now.Add(-thirtyDays),
			expectedEndTime:       now,
		},
		"should manipulate a query on large time range over the maxQueryLookback limit, and blocksRetentionPeriod is set to an higher value": {
			maxQueryLookback:      thirtyDays,
			blocksRetentionPeriod: sixtyDays,
			reqStartTime:          now.Add(-thirtyDays).Add(-100 * time.Hour),
			reqEndTime:            now,
			expectedStartTime:     now.Add(-thirtyDays),
			expectedEndTime:       now,
		},
		"should manipulate a query on large time range over the blocksRetentionPeriod, and maxQueryLookback limit is set to an higher value": {
			maxQueryLookback:      sixtyDays,
			blocksRetentionPeriod: thirtyDays,
			reqStartTime:          now.Add(-thirtyDays).Add(-100 * time.Hour),
			reqEndTime:            now,
			expectedStartTime:     now.Add(-thirtyDays),
			expectedEndTime:       now,
		},
		"should skip executing a query outside the allowed maxQueryLookback limit, and blocksRetentionPeriod is not set": {
			maxQueryLookback:      thirtyDays,
			blocksRetentionPeriod: 0,
			reqStartTime:          now.Add(-thirtyDays).Add(-100 * time.Hour),
			reqEndTime:            now.Add(-thirtyDays).Add(-90 * time.Hour),
			expectedSkipped:       true,
		},
		"should skip executing a query outside the allowed maxQueryLookback limit, and blocksRetentionPeriod is set to an higher value": {
			maxQueryLookback:      thirtyDays,
			blocksRetentionPeriod: sixtyDays,
			reqStartTime:          now.Add(-thirtyDays).Add(-100 * time.Hour),
			reqEndTime:            now.Add(-thirtyDays).Add(-90 * time.Hour),
			expectedSkipped:       true,
		},
		"should skip executing a query outside the blocksRetentionPeriod, and maxQueryLookback limit is set to an higher value": {
			maxQueryLookback:      sixtyDays,
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
			reqs := map[string]MetricsQueryRequest{
				"range query": &PrometheusRangeQueryRequest{
					start: util.TimeToMillis(testData.reqStartTime),
					end:   util.TimeToMillis(testData.reqEndTime),
				},
				"remote read": &remoteReadQueryRequest{
					path: remoteReadPathSuffix,
					query: &prompb.Query{
						StartTimestampMs: util.TimeToMillis(testData.reqStartTime),
						EndTimestampMs:   util.TimeToMillis(testData.reqEndTime),
					},
				},
			}

			for reqType, req := range reqs {
				t.Run(reqType, func(t *testing.T) {
					limits := mockLimits{maxQueryLookback: testData.maxQueryLookback, compactorBlocksRetentionPeriod: testData.blocksRetentionPeriod}
					middleware := newLimitsMiddleware(limits, log.NewNopLogger())

					innerRes := NewEmptyPrometheusResponse()
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

						assert.InDelta(t, util.TimeToMillis(testData.expectedStartTime), inner.Calls[0].Arguments.Get(1).(MetricsQueryRequest).GetStart(), delta)
						assert.InDelta(t, util.TimeToMillis(testData.expectedEndTime), inner.Calls[0].Arguments.Get(1).(MetricsQueryRequest).GetEnd(), delta)
					}
				})
			}
		})
	}
}

func TestLimitsMiddleware_MaxQueryLookback_InstantQuery(t *testing.T) {
	const (
		thirtyDays = 30 * 24 * time.Hour
		sixtyDays  = 60 * 24 * time.Hour
	)

	now := time.Now()

	tests := map[string]struct {
		maxQueryLookback      time.Duration
		blocksRetentionPeriod time.Duration
		reqTime               time.Time
		expectedSkipped       bool
		expectedTime          time.Time
	}{
		"should allow executing a query if maxQueryLookback and blocksRetentionPeriod are both disabled": {
			maxQueryLookback:      0,
			blocksRetentionPeriod: 0,
			reqTime:               time.Unix(0, 0),
			expectedTime:          time.Unix(0, 0),
		},
		"should allow executing a query with time within maxQueryLookback and blocksRetentionPeriod": {
			maxQueryLookback:      thirtyDays,
			blocksRetentionPeriod: thirtyDays,
			reqTime:               now.Add(-time.Hour),
			expectedTime:          now.Add(-time.Hour),
		},
		"should allow executing a query with time close to maxQueryLookback and blocksRetentionPeriod": {
			maxQueryLookback:      thirtyDays,
			blocksRetentionPeriod: thirtyDays,
			reqTime:               now.Add(-thirtyDays).Add(time.Hour),
			expectedTime:          now.Add(-thirtyDays).Add(time.Hour),
		},
		"should skip executing a query with time before the maxQueryLookback limit, and blocksRetentionPeriod is not set": {
			maxQueryLookback:      thirtyDays,
			blocksRetentionPeriod: 0,
			reqTime:               now.Add(-thirtyDays).Add(-100 * time.Hour),
			expectedSkipped:       true,
		},
		"should skip executing a query with time before the blocksRetentionPeriod, and maxQueryLookback limit is not set": {
			maxQueryLookback:      0,
			blocksRetentionPeriod: thirtyDays,
			reqTime:               now.Add(-thirtyDays).Add(-100 * time.Hour),
			expectedSkipped:       true,
		},
		"should skip executing a query with time before the maxQueryLookback limit, and blocksRetentionPeriod is set to an higher value": {
			maxQueryLookback:      thirtyDays,
			blocksRetentionPeriod: sixtyDays,
			reqTime:               now.Add(-thirtyDays).Add(-100 * time.Hour),
			expectedSkipped:       true,
		},
		"should skip executing a query with time before the blocksRetentionPeriod, and maxQueryLookback limit is set to an higher value": {
			maxQueryLookback:      sixtyDays,
			blocksRetentionPeriod: thirtyDays,
			reqTime:               now.Add(-thirtyDays).Add(-100 * time.Hour),
			expectedSkipped:       true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &PrometheusInstantQueryRequest{
				time: testData.reqTime.UnixMilli(),
			}

			limits := mockLimits{maxQueryLookback: testData.maxQueryLookback, compactorBlocksRetentionPeriod: testData.blocksRetentionPeriod}
			middleware := newLimitsMiddleware(limits, log.NewNopLogger())

			innerRes := NewEmptyPrometheusResponse()
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

				assert.InDelta(t, util.TimeToMillis(testData.expectedTime), inner.Calls[0].Arguments.Get(1).(MetricsQueryRequest).GetStart(), delta)
				assert.InDelta(t, util.TimeToMillis(testData.expectedTime), inner.Calls[0].Arguments.Get(1).(MetricsQueryRequest).GetEnd(), delta)
			}
		})
	}
}

func TestLimitsMiddleware_MaxQueryLookback_TenantFederation(t *testing.T) {
	const (
		day        = 24 * time.Hour
		tenDays    = 10 * 24 * time.Hour
		twentyDays = 20 * 24 * time.Hour
		thirtyDays = 30 * 24 * time.Hour
		fortyDays  = 40 * 24 * time.Hour
	)

	defaults := validation.Limits{
		MaxQueryLookback:               0,
		CompactorBlocksRetentionPeriod: 0,
	}

	tenantLimits := map[string]*validation.Limits{
		// tenant-a has no overrides

		// Tenants with no blocks retention period configured.
		"tenant-b": {MaxQueryLookback: model.Duration(tenDays), CompactorBlocksRetentionPeriod: 0},
		"tenant-c": {MaxQueryLookback: model.Duration(twentyDays), CompactorBlocksRetentionPeriod: 0},

		// Tenants with no max query lookback configured.
		"tenant-d": {MaxQueryLookback: 0, CompactorBlocksRetentionPeriod: model.Duration(thirtyDays)},
		"tenant-e": {MaxQueryLookback: 0, CompactorBlocksRetentionPeriod: model.Duration(fortyDays)},

		// Tenants with both max query lookback and blocks retention period configured.
		"tenant-f": {MaxQueryLookback: model.Duration(tenDays), CompactorBlocksRetentionPeriod: model.Duration(thirtyDays)},
		"tenant-g": {MaxQueryLookback: model.Duration(twentyDays), CompactorBlocksRetentionPeriod: model.Duration(fortyDays)},
	}

	now := time.Now()

	tests := map[string]struct {
		reqTenants        []string
		reqStartTime      time.Time
		reqEndTime        time.Time
		expectedStartTime time.Time
		expectedEndTime   time.Time
	}{
		"should enforce the smallest 'max query lookback' to not allow any tenant to query past their configured max lookback": {
			reqTenants:        []string{"tenant-a", "tenant-b", "tenant-c"},
			reqStartTime:      now.Add(-365 * day),
			reqEndTime:        now,
			expectedStartTime: now.Add(-tenDays),
			expectedEndTime:   now,
		},
		"should enforce the largest 'block retention period' to allow any tenant to query up their full retention period": {
			reqTenants:        []string{"tenant-a", "tenant-d", "tenant-e"},
			reqStartTime:      now.Add(-365 * day),
			reqEndTime:        now,
			expectedStartTime: now.Add(-fortyDays),
			expectedEndTime:   now,
		},
		"should enforce the smallest between the 'smallest max query lookback' and 'largest block retention period'": {
			reqTenants:        []string{"tenant-a", "tenant-f", "tenant-g"},
			reqStartTime:      now.Add(-365 * day),
			reqEndTime:        now,
			expectedStartTime: now.Add(-tenDays),
			expectedEndTime:   now,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := validation.NewOverrides(defaults, validation.NewMockTenantLimits(tenantLimits))

			middleware := newLimitsMiddleware(limits, log.NewNopLogger())

			innerRes := NewEmptyPrometheusResponse()
			inner := &mockHandler{}
			inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

			ctx := user.InjectOrgID(context.Background(), tenant.JoinTenantIDs(testData.reqTenants))
			outer := middleware.Wrap(inner)

			req := &PrometheusRangeQueryRequest{
				start: util.TimeToMillis(testData.reqStartTime),
				end:   util.TimeToMillis(testData.reqEndTime),
			}

			_, err := outer.Do(ctx, req)
			require.NoError(t, err)

			// Assert on the time range of the request passed to the inner handler (5s delta).
			delta := float64(5000)
			require.Len(t, inner.Calls, 1)

			assert.InDelta(t, util.TimeToMillis(testData.expectedStartTime), inner.Calls[0].Arguments.Get(1).(MetricsQueryRequest).GetStart(), delta)
			assert.InDelta(t, util.TimeToMillis(testData.expectedEndTime), inner.Calls[0].Arguments.Get(1).(MetricsQueryRequest).GetEnd(), delta)
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
			startMs := util.TimeToMillis(now.Add(-time.Hour * 2))
			endMs := util.TimeToMillis(now.Add(-time.Hour))

			reqs := map[string]MetricsQueryRequest{
				"range query": &PrometheusRangeQueryRequest{
					queryExpr: parseQuery(t, testData.query),
					start:     startMs,
					end:       endMs,
				},
				"instant query": &PrometheusInstantQueryRequest{
					queryExpr: parseQuery(t, testData.query),
				},
				"remote read": &remoteReadQueryRequest{
					path:      remoteReadPathSuffix,
					promQuery: testData.query,
					query: &prompb.Query{
						StartTimestampMs: startMs,
						EndTimestampMs:   endMs,
						Matchers: func() []*prompb.LabelMatcher {
							v := &findVectorSelectorsVisitor{}
							require.NoError(t, parser.Walk(v, parseQuery(t, testData.query), nil))

							// This test requires the query has only 1 vector selector.
							require.Len(t, v.selectors, 1)
							require.NotEmpty(t, v.selectors[0].LabelMatchers)

							matchers, err := remote.ToLabelMatchers(v.selectors[0].LabelMatchers)
							require.NoError(t, err)

							return matchers
						}(),
					},
				},
			}

			for reqType, req := range reqs {
				t.Run(reqType, func(t *testing.T) {
					limits := multiTenantMockLimits{
						byTenant: map[string]mockLimits{
							"test1": {maxQueryExpressionSizeBytes: testData.queryLimits["test1"]},
							"test2": {maxQueryExpressionSizeBytes: testData.queryLimits["test2"]},
						},
					}
					middleware := newLimitsMiddleware(limits, log.NewNopLogger())

					innerRes := NewEmptyPrometheusResponse()
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
			// NOTE: instant queries are not tested because they don't have a time range.
			reqs := map[string]MetricsQueryRequest{
				"range query": &PrometheusRangeQueryRequest{
					start: util.TimeToMillis(testData.reqStartTime),
					end:   util.TimeToMillis(testData.reqEndTime),
				},
				"remote read": &remoteReadQueryRequest{
					path: remoteReadPathSuffix,
					query: &prompb.Query{
						StartTimestampMs: util.TimeToMillis(testData.reqStartTime),
						EndTimestampMs:   util.TimeToMillis(testData.reqEndTime),
					},
				},
			}

			for reqType, req := range reqs {
				t.Run(reqType, func(t *testing.T) {
					limits := mockLimits{maxQueryLength: testData.maxQueryLength, maxTotalQueryLength: testData.maxTotalQueryLength}
					middleware := newLimitsMiddleware(limits, log.NewNopLogger())

					innerRes := NewEmptyPrometheusResponse()
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
						assert.Equal(t, util.TimeToMillis(testData.reqStartTime), inner.Calls[0].Arguments.Get(1).(MetricsQueryRequest).GetStart())
						assert.Equal(t, util.TimeToMillis(testData.reqEndTime), inner.Calls[0].Arguments.Get(1).(MetricsQueryRequest).GetEnd())
					}
				})
			}
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

func (m multiTenantMockLimits) ResultsCacheTTLForErrors(userID string) time.Duration {
	return m.byTenant[userID].resultsCacheTTLForErrors
}

func (m multiTenantMockLimits) ResultsCacheForUnalignedQueryEnabled(userID string) bool {
	return m.byTenant[userID].resultsCacheForUnalignedQueryEnabled
}

func (m multiTenantMockLimits) EnabledPromQLExperimentalFunctions(userID string) []string {
	return m.byTenant[userID].enabledPromQLExperimentalFunctions
}

func (m multiTenantMockLimits) EnabledPromQLExtendedRangeSelectors(userID string) []string {
	return m.byTenant[userID].enabledPromQLExtendedRangeSelectors
}

func (m multiTenantMockLimits) Prom2RangeCompat(userID string) bool {
	return m.byTenant[userID].prom2RangeCompat
}

func (m multiTenantMockLimits) BlockedQueries(userID string) []validation.BlockedQuery {
	return m.byTenant[userID].blockedQueries
}

func (m multiTenantMockLimits) LimitedQueries(userID string) []validation.LimitedQuery {
	return m.byTenant[userID].limitedQueries
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

func (m multiTenantMockLimits) IngestStorageReadConsistency(userID string) string {
	return m.byTenant[userID].ingestStorageReadConsistency
}

func (m multiTenantMockLimits) BlockedRequests(userID string) []validation.BlockedRequest {
	return m.byTenant[userID].blockedRequests
}

func (m multiTenantMockLimits) SubquerySpinOffEnabled(userID string) bool {
	return m.byTenant[userID].subquerySpinOffEnabled
}

func (m multiTenantMockLimits) LabelsQueryOptimizerEnabled(userID string) bool {
	return m.byTenant[userID].labelsQueryOptimizerEnabled
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
	resultsCacheTTLForErrors             time.Duration
	resultsCacheForUnalignedQueryEnabled bool
	enabledPromQLExperimentalFunctions   []string
	enabledPromQLExtendedRangeSelectors  []string
	prom2RangeCompat                     bool
	blockedQueries                       []validation.BlockedQuery
	limitedQueries                       []validation.LimitedQuery
	blockedRequests                      []validation.BlockedRequest
	alignQueriesWithStep                 bool
	queryIngestersWithin                 time.Duration
	ingestStorageReadConsistency         string
	subquerySpinOffEnabled               bool
	labelsQueryOptimizerEnabled          bool
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

func (m mockLimits) ResultsCacheTTLForErrors(string) time.Duration {
	return m.resultsCacheTTLForErrors
}

func (m mockLimits) ResultsCacheTTLForCardinalityQuery(string) time.Duration {
	return m.resultsCacheTTLForCardinalityQuery
}

func (m mockLimits) BlockedQueries(string) []validation.BlockedQuery {
	return m.blockedQueries
}

func (m mockLimits) LimitedQueries(userID string) []validation.LimitedQuery {
	return m.limitedQueries
}

func (m mockLimits) ResultsCacheTTLForLabelsQuery(string) time.Duration {
	return m.resultsCacheTTLForLabelsQuery
}

func (m mockLimits) ResultsCacheForUnalignedQueryEnabled(string) bool {
	return m.resultsCacheForUnalignedQueryEnabled
}

func (m mockLimits) EnabledPromQLExperimentalFunctions(string) []string {
	return m.enabledPromQLExperimentalFunctions
}

func (m mockLimits) EnabledPromQLExtendedRangeSelectors(string) []string {
	return m.enabledPromQLExtendedRangeSelectors
}

func (m mockLimits) Prom2RangeCompat(string) bool {
	return m.prom2RangeCompat
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

func (m mockLimits) IngestStorageReadConsistency(string) string {
	return m.ingestStorageReadConsistency
}

func (m mockLimits) BlockedRequests(string) []validation.BlockedRequest {
	return m.blockedRequests
}

func (m mockLimits) SubquerySpinOffEnabled(string) bool {
	return m.subquerySpinOffEnabled
}

func (m mockLimits) LabelsQueryOptimizerEnabled(string) bool {
	return m.labelsQueryOptimizerEnabled
}

type mockHandler struct {
	mock.Mock
}

func (m *mockHandler) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(Response), args.Error(1)
}

func TestLimitedRoundTripper_MaxQueryParallelism(t *testing.T) {
	// This test only applies to requests sent to queriers over HTTPGRPC.
	// For the equivalent test for remote execution, see TestMaxQueryParallelismWithRemoteExecution in pkg/frontend/v2/remoteexec_test.go

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

	codec := newTestCodec()
	r, err := codec.EncodeMetricsQueryRequest(ctx, &PrometheusRangeQueryRequest{
		path:      "/api/v1/query_range",
		start:     time.Now().Add(-time.Hour).Unix(),
		end:       time.Now().Unix(),
		step:      int64(1 * time.Second * time.Millisecond),
		queryExpr: parseQuery(t, `foo`),
	})
	require.NoError(t, err)

	handler := NewHTTPQueryRequestRoundTripperHandler(downstream, codec, log.NewNopLogger())
	_, err = NewLimitedParallelismRoundTripper(handler, codec, mockLimits{maxQueryParallelism: maxQueryParallelism}, false,
		MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
			return HandlerFunc(func(c context.Context, _ MetricsQueryRequest) (Response, error) {
				var wg sync.WaitGroup
				for i := 0; i < maxQueryParallelism+20; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, _ = next.Do(c, &PrometheusRangeQueryRequest{})
					}()
				}
				wg.Wait()
				return NewEmptyPrometheusResponse(), nil
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

	codec := newTestCodec()
	r, err := codec.EncodeMetricsQueryRequest(ctx, &PrometheusRangeQueryRequest{
		path:      "/api/v1/query_range",
		start:     time.Now().Add(time.Hour).Unix(),
		end:       util.TimeToMillis(time.Now()),
		step:      int64(1 * time.Second * time.Millisecond),
		queryExpr: parseQuery(t, `foo`),
	})
	require.Nil(t, err)

	handler := NewHTTPQueryRequestRoundTripperHandler(downstream, codec, log.NewNopLogger())
	_, err = NewLimitedParallelismRoundTripper(handler, codec, mockLimits{maxQueryParallelism: maxQueryParallelism}, false,
		MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
			return HandlerFunc(func(c context.Context, _ MetricsQueryRequest) (Response, error) {
				// fire up work and we don't wait.
				for i := 0; i < 10; i++ {
					go func() {
						_, _ = next.Do(c, &PrometheusRangeQueryRequest{})
					}()
				}
				return NewEmptyPrometheusResponse(), nil
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

	codec := newTestCodec()
	r, err := codec.EncodeMetricsQueryRequest(reqCtx, &PrometheusRangeQueryRequest{
		path:      "/api/v1/query_range",
		start:     time.Now().Add(time.Hour).Unix(),
		end:       util.TimeToMillis(time.Now()),
		step:      int64(1 * time.Second * time.Millisecond),
		queryExpr: parseQuery(t, `foo`),
	})
	require.Nil(t, err)

	handler := NewHTTPQueryRequestRoundTripperHandler(downstream, codec, log.NewNopLogger())
	_, err = NewLimitedParallelismRoundTripper(handler, codec, mockLimits{maxQueryParallelism: maxQueryParallelism}, false,
		MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
			return HandlerFunc(func(c context.Context, _ MetricsQueryRequest) (Response, error) {
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

				return NewEmptyPrometheusResponse(), nil
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

	codec := newTestCodec()
	r, err := codec.EncodeMetricsQueryRequest(ctx, &PrometheusRangeQueryRequest{
		path:      "/api/v1/query_range",
		start:     time.Now().Add(time.Hour).Unix(),
		end:       util.TimeToMillis(time.Now()),
		step:      int64(1 * time.Second * time.Millisecond),
		queryExpr: parseQuery(b, `foo`),
	})
	require.Nil(b, err)

	for _, concurrentRequestCount := range []int{1, 10, 100} {
		for _, subRequestCount := range []int{1, 2, 5, 10, 20, 50, 100} {
			handler := NewHTTPQueryRequestRoundTripperHandler(downstream, codec, log.NewNopLogger())
			tripper := NewLimitedParallelismRoundTripper(handler, codec, mockLimits{maxQueryParallelism: maxParallelism}, false,
				MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
					return HandlerFunc(func(c context.Context, _ MetricsQueryRequest) (Response, error) {
						wg := sync.WaitGroup{}
						for i := 0; i < subRequestCount; i++ {
							wg.Add(1)
							go func() {
								defer wg.Done()
								_, _ = next.Do(c, &PrometheusRangeQueryRequest{})
							}()
						}
						wg.Wait()
						return NewEmptyPrometheusResponse(), nil
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

func TestSmallestPositiveNonZeroDuration(t *testing.T) {
	assert.Equal(t, time.Duration(0), smallestPositiveNonZeroDuration())
	assert.Equal(t, time.Duration(0), smallestPositiveNonZeroDuration(0))
	assert.Equal(t, time.Duration(0), smallestPositiveNonZeroDuration(-1))

	assert.Equal(t, time.Duration(1), smallestPositiveNonZeroDuration(0, 1, -1))
	assert.Equal(t, time.Duration(1), smallestPositiveNonZeroDuration(0, 2, 1))
}

type findVectorSelectorsVisitor struct {
	selectors []*parser.VectorSelector
}

func (v *findVectorSelectorsVisitor) Visit(node parser.Node, _ []parser.Node) (parser.Visitor, error) {
	selector, ok := node.(*parser.VectorSelector)
	if !ok {
		return v, nil
	}

	v.selectors = append(v.selectors, selector)
	return v, nil
}

func TestEngineQueryRequestRoundTripperHandler(t *testing.T) {
	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	logger := log.NewNopLogger()
	engine, err := streamingpromql.NewEngine(opts, streamingpromql.NewStaticQueryLimitsProvider(0, false), stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)
	codec := newTestCodec()
	handler := NewEngineQueryRequestRoundTripperHandler(engine, codec, logger)

	storage := promqltest.LoadedStorage(t, `
		load 1s
			some_metric{foo="bar"} 0+1x50
			some_other_metric{foo="bar", idx="0"} 0+1x50
			some_other_metric{foo="bar", idx="1"} 0+1x50
	`)
	t.Cleanup(func() { storage.Close() })

	lookbackDelta := 5 * time.Minute

	mustParseExpr := func(s string) parser.Expr {
		expr, err := parser.ParseExpr(s)
		require.NoError(t, err)
		return expr
	}

	encodedOffsets := string(api.EncodeOffsets(map[int32]int64{0: 1, 1: 2}))

	requestHeaders := []*PrometheusHeader{
		{Name: compat.ForceFallbackHeaderName, Values: []string{"true"}},
		{Name: chunkinfologger.ChunkInfoLoggingHeader, Values: []string{"chunk-info-logging-enabled"}},
		{Name: api.ReadConsistencyOffsetsHeader, Values: []string{encodedOffsets}},
		{Name: "Some-Other-Ignored-Header", Values: []string{"some-value"}},
	}

	expectedHeaders := map[string][]string{
		// From request headers:
		compat.ForceFallbackHeaderName:         {"true"},
		chunkinfologger.ChunkInfoLoggingHeader: {"chunk-info-logging-enabled"},
		api.ReadConsistencyOffsetsHeader:       {encodedOffsets},

		// From read consistency settings in context:
		api.ReadConsistencyHeader:         {api.ReadConsistencyStrong},
		api.ReadConsistencyMaxDelayHeader: {time.Minute.String()},
	}

	requestOptions := Options{
		TotalShards: 123,
	}

	requestHints := &Hints{
		TotalQueries: 456,
		CardinalityEstimate: &EstimatedSeriesCount{
			EstimatedSeriesCount: 789,
		},
	}

	testCases := map[string]struct {
		req                      MetricsQueryRequest
		expectedResponse         Response
		expectedErr              error
		expectedSamplesProcessed uint64
	}{
		"range query": {
			req:                      NewPrometheusRangeQueryRequest("/", requestHeaders, 1000, 7000, 2000, lookbackDelta, mustParseExpr(`5*some_metric`), requestOptions, requestHints, ""),
			expectedSamplesProcessed: 4,
			expectedResponse: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{
								{Name: "foo", Value: "bar"},
							},
							Samples: []mimirpb.Sample{
								{TimestampMs: 1000, Value: 5},
								{TimestampMs: 3000, Value: 15},
								{TimestampMs: 5000, Value: 25},
								{TimestampMs: 7000, Value: 35},
							},
						},
					},
				},
				Warnings: []string{},
				Infos:    []string{},
			},
		},

		"instant query": {
			req:                      NewPrometheusInstantQueryRequest("/", requestHeaders, 3000, lookbackDelta, mustParseExpr(`5*some_metric`), requestOptions, requestHints, ""),
			expectedSamplesProcessed: 1,
			expectedResponse: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValVector.String(),
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{
								{Name: "foo", Value: "bar"},
							},
							Samples: []mimirpb.Sample{
								{TimestampMs: 3000, Value: 15},
							},
						},
					},
				},
				Warnings: []string{},
				Infos:    []string{},
			},
		},

		"scalar result": {
			req: NewPrometheusInstantQueryRequest("/", requestHeaders, 3000, lookbackDelta, mustParseExpr(`scalar(some_metric)`), requestOptions, requestHints, ""),
			expectedResponse: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValScalar.String(),
					Result: []SampleStream{
						{
							Samples: []mimirpb.Sample{
								{TimestampMs: 3000, Value: 3},
							},
						},
					},
				},
				Warnings: []string{},
				Infos:    []string{},
			},
			expectedSamplesProcessed: 1,
		},

		"string result": {
			req: NewPrometheusInstantQueryRequest("/", requestHeaders, 3000, lookbackDelta, mustParseExpr(`"foo"`), requestOptions, requestHints, ""),
			expectedResponse: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValString.String(),
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{
								{Name: "value", Value: "foo"},
							},
							Samples: []mimirpb.Sample{
								{TimestampMs: 3000, Value: 0},
							},
						},
					},
				},
				Warnings: []string{},
				Infos:    []string{},
			},
		},

		"execution error": {
			req:         NewPrometheusInstantQueryRequest("/", requestHeaders, 3000, lookbackDelta, mustParseExpr(`some_metric * on(foo) some_other_metric`), requestOptions, requestHints, ""),
			expectedErr: apierror.New(apierror.TypeExec, `found duplicate series for the match group {foo="bar"} on the right side of the operation at timestamp 1970-01-01T00:00:03Z: {__name__="some_other_metric", foo="bar", idx="0"} and {__name__="some_other_metric", foo="bar", idx="1"}`),
		},

		"annotations": {
			req:                      NewPrometheusInstantQueryRequest("/", requestHeaders, 3000, lookbackDelta, mustParseExpr(`histogram_quantile(0.1, rate(some_metric[2s]))`), requestOptions, requestHints, ""),
			expectedSamplesProcessed: 2,
			expectedResponse: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: model.ValVector.String(),
					Result:     []SampleStream{},
				},
				Warnings: []string{
					`PromQL warning: bucket label "le" is missing or has a malformed value of "" (1:25)`,
				},
				Infos: []string{
					`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "some_metric" (1:30)`,
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			// Replace the handler's storage with our test storage so we can test specific scenarios that depend on data,
			// and check that the expected headers have been captured for propagation to queriers, including the read consistency level.
			contextCapturingStorage := &contextCapturingStorage{inner: storage}
			handler.(*engineQueryRequestRoundTripperHandler).storage = contextCapturingStorage

			ctx := api.ContextWithReadConsistencyLevel(context.Background(), api.ReadConsistencyStrong)
			ctx = api.ContextWithReadConsistencyMaxDelay(ctx, time.Minute)
			stats, ctx := stats.ContextWithEmptyStats(ctx)
			response, err := handler.Do(ctx, testCase.req)
			require.Equal(t, testCase.expectedErr, err)

			if testCase.expectedErr != nil {
				require.Nil(t, response)
				return
			}

			responseWithFinalizer, ok := response.(*PrometheusResponseWithFinalizer)
			require.True(t, ok)
			require.Equal(t, testCase.expectedResponse, responseWithFinalizer.PrometheusResponse)
			require.NotNil(t, responseWithFinalizer.finalizer, "expected response to have a finalizer")

			responseWithFinalizer.Close()

			require.Equal(t, testCase.expectedSamplesProcessed, stats.SamplesProcessed)

			if responseWithFinalizer.Data.ResultType == model.ValString.String() {
				// We can't perform the assertions below for string results because it doesn't select any data,
				// so we have no way to capture the context used to evaluate the query.
				return
			}

			propagatedHeaders := HeadersToPropagateFromContext(contextCapturingStorage.ctx)
			require.Equal(t, expectedHeaders, propagatedHeaders)

			hints := RequestHintsFromContext(contextCapturingStorage.ctx)
			require.Equal(t, testCase.req.GetHints(), hints)

			options := RequestOptionsFromContext(contextCapturingStorage.ctx)
			require.Equal(t, testCase.req.GetOptions(), options)
		})
	}
}

type contextCapturingStorage struct {
	inner storage.Storage
	ctx   context.Context
}

func (s *contextCapturingStorage) Querier(mint, maxt int64) (storage.Querier, error) {
	q, err := s.inner.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &contextCapturingQuerier{q, s}, nil
}

func (s *contextCapturingStorage) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	panic("not supported")
}

func (s *contextCapturingStorage) Appender(ctx context.Context) storage.Appender {
	panic("not supported")
}

func (s *contextCapturingStorage) StartTime() (int64, error) {
	panic("not supported")
}

func (s *contextCapturingStorage) Close() error {
	return s.inner.Close()
}

type contextCapturingQuerier struct {
	inner   storage.Querier
	storage *contextCapturingStorage
}

func (c *contextCapturingQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	c.storage.ctx = ctx
	return c.inner.Select(ctx, sortSeries, hints, matchers...)
}

func (c *contextCapturingQuerier) Close() error {
	return c.inner.Close()
}

func (c *contextCapturingQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	panic("not supported")
}

func (c *contextCapturingQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	panic("not supported")
}
