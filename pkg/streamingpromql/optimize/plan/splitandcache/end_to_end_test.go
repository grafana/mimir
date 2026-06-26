// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
)

func TestEndToEnd(t *testing.T) {
	timeZero := timestamp.Time(0)
	millisecondsInHour := time.Hour.Milliseconds()

	data := `
		load 1h
			metric 1+1x48
	`
	storage := promqltest.LoadedStorage(t, data)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	testCases := map[string]struct {
		expr  string
		start time.Time
		end   time.Time
		step  time.Duration

		expectedResult       promql.Result
		expectedCacheEntries int
	}{
		"simple range query": {
			expr:  "metric",
			start: timeZero,
			end:   timeZero.Add(48 * time.Hour),
			step:  4 * time.Hour,

			expectedResult: promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.FromStrings(model.MetricNameLabel, "metric"),
						Floats: []promql.FPoint{
							{T: 0, F: 1},
							{T: 4 * millisecondsInHour, F: 5},
							{T: 8 * millisecondsInHour, F: 9},
							{T: 12 * millisecondsInHour, F: 13},
							{T: 16 * millisecondsInHour, F: 17},
							{T: 20 * millisecondsInHour, F: 21},
							{T: 24 * millisecondsInHour, F: 25},
							{T: 28 * millisecondsInHour, F: 29},
							{T: 32 * millisecondsInHour, F: 33},
							{T: 36 * millisecondsInHour, F: 37},
							{T: 40 * millisecondsInHour, F: 41},
							{T: 44 * millisecondsInHour, F: 45},
							{T: 48 * millisecondsInHour, F: 49},
						},
					},
				},
			},
			expectedCacheEntries: 3, // 0-20h, 24-44h, 48h (single step)
		},
		"query with step-invariant expression": {
			// This case tests two things:
			// - that "@ end()" is correctly handled across splits
			// - that step invariant expressions are correctly handled (a single materialized node cannot be reused,
			//   so if the step-invariant node was reused, this query would fail or return incorrect results)

			expr:  "metric @ end()",
			start: timeZero,
			end:   timeZero.Add(48 * time.Hour),
			step:  4 * time.Hour,

			expectedResult: promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.FromStrings(model.MetricNameLabel, "metric"),
						Floats: []promql.FPoint{
							{T: 0, F: 49},
							{T: 4 * millisecondsInHour, F: 49},
							{T: 8 * millisecondsInHour, F: 49},
							{T: 12 * millisecondsInHour, F: 49},
							{T: 16 * millisecondsInHour, F: 49},
							{T: 20 * millisecondsInHour, F: 49},
							{T: 24 * millisecondsInHour, F: 49},
							{T: 28 * millisecondsInHour, F: 49},
							{T: 32 * millisecondsInHour, F: 49},
							{T: 36 * millisecondsInHour, F: 49},
							{T: 40 * millisecondsInHour, F: 49},
							{T: 44 * millisecondsInHour, F: 49},
							{T: 48 * millisecondsInHour, F: 49},
						},
					},
				},
			},
			expectedCacheEntries: 3, // 0-20h, 24-44h, 48h (single step)
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			opts := streamingpromql.NewTestEngineOpts()
			opts.RangeQuerySplittingAndCaching.SplitEnabled = true
			opts.RangeQuerySplittingAndCaching.SplitInterval = 24 * time.Hour
			opts.RangeQuerySplittingAndCaching.CacheEnabled = true
			cache := newRequestCountingCache()
			opts.RangeQuerySplittingAndCaching.CacheClient = cache

			planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)
			metrics := stats.NewQueryMetrics(opts.CommonOpts.Reg)
			require.NoError(t, err)
			engine, err := streamingpromql.NewEngine(opts, metrics, planner)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "some-user")

			runQuery := func(msg string) {
				q, err := engine.NewRangeQuery(ctx, storage, nil, testCase.expr, testCase.start, testCase.end, testCase.step)
				require.NoError(t, err)
				t.Cleanup(q.Close)

				result := q.Exec(ctx)
				require.NoError(t, result.Err)
				require.Equal(t, testCase.expectedResult, *result, "expected result "+msg+" to match expected")
			}

			// Run query once with an empty cache, confirm we get the expected result.
			runQuery("not from cache")
			require.NotEmpty(t, cache.GetItems(), "expected cache to be populated")
			require.Lenf(t, cache.GetItems(), testCase.expectedCacheEntries, "expected %d cache entries, but got %d", testCase.expectedCacheEntries, len(cache.GetItems()))
			require.Equal(t, 1, cache.getCount, "expected cache to be queried exactly once overall, not once per cache entry")
			cache.ResetCounter()

			// Run it again with a populated cache, confirm we still get the expected result.
			runQuery("from cache")
			require.Equal(t, 1, cache.getCount, "expected cache to be queried exactly once overall, not once per cache entry")
		})
	}
}

type requestCountingCache struct {
	*cache.MockCache
	getCount int
}

func newRequestCountingCache() *requestCountingCache {
	return &requestCountingCache{MockCache: cache.NewMockCache()}
}

func (r *requestCountingCache) GetMulti(ctx context.Context, keys []string, opts ...cache.Option) map[string][]byte {
	r.getCount++
	return r.MockCache.GetMulti(ctx, keys, opts...)
}

func (r *requestCountingCache) GetMultiWithError(ctx context.Context, keys []string, opts ...cache.Option) (map[string][]byte, error) {
	r.getCount++
	return r.MockCache.GetMultiWithError(ctx, keys, opts...)
}

func (r *requestCountingCache) ResetCounter() {
	r.getCount = 0
}
