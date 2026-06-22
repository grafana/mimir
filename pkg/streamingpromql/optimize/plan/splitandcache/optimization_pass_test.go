// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/splitandcache"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestOptimizationPass(t *testing.T) {
	limits := streamingpromql.NewStaticQueryLimitsProvider()
	limits.MaxCacheFreshness = time.Hour

	timeZero := timestamp.Time(0)
	timeNow := time.Date(2024, 1, 2, 3, 0, 0, 0, time.UTC)
	rangeQueryTimeRange := types.NewRangeQueryTimeRange(timeZero.Add(2*time.Hour), timeZero.Add(50*time.Hour), time.Hour)
	unalignedRangeQueryTimeRange := types.NewRangeQueryTimeRange(timeZero.Add(2*time.Hour).Add(time.Minute), timeZero.Add(50*time.Hour).Add(time.Minute), time.Hour)
	entirelyInFreshnessWindow := types.NewRangeQueryTimeRange(timeNow.Add(-time.Minute), timeNow.Add(-time.Second), time.Second)
	straddlesFreshnessWindow := types.NewRangeQueryTimeRange(timeNow.Add(-limits.MaxCacheFreshness).Add(-time.Hour), timeNow.Add(-time.Second), time.Second)

	testCases := map[string]struct {
		expr                           string
		timeRange                      types.QueryTimeRange
		disableSplitting               bool
		disableCaching                 bool
		cachingDisabledByRequestOption bool
		allowCachingUnalignedRequests  bool

		expectUnchanged           bool
		expectedPlan              string
		expectedNotCachableReason string
	}{
		"instant query": {
			expr:            "foo{}",
			timeRange:       types.NewInstantQueryTimeRange(time.Now()),
			expectUnchanged: true,
		},

		"simple range query": {
			expr:      "foo{}",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- VectorSelector: {__name__="foo"}
			`,
		},
		"simple range query, caching disabled": {
			expr:           "foo{}",
			timeRange:      rangeQueryTimeRange,
			disableCaching: true,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- VectorSelector: {__name__="foo"}
			`,
		},
		"simple range query, splitting disabled": {
			expr:             "foo{}",
			timeRange:        rangeQueryTimeRange,
			disableSplitting: true,
			expectedPlan: `
				- Cache: split interval 24h0m0s
					- VectorSelector: {__name__="foo"}
			`,
		},
		"simple range query, splitting and caching disabled": {
			expr:             "foo{}",
			timeRange:        rangeQueryTimeRange,
			disableSplitting: true,
			disableCaching:   true,
			expectUnchanged:  true,
		},
		"simple range query, caching disabled by request option": {
			expr:                           "foo{}",
			timeRange:                      rangeQueryTimeRange,
			cachingDisabledByRequestOption: true,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- VectorSelector: {__name__="foo"}
			`,
		},

		"instant vector selector with positive offset": {
			expr:      "foo{} offset 1h",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- VectorSelector: {__name__="foo"} offset 1h0m0s
			`,
		},
		"instant vector selector with negative offset": {
			expr:      "foo{} offset -1h",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- VectorSelector: {__name__="foo"} offset -1h0m0s
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonModifiersNotCachable,
		},
		"range vector selector with positive offset": {
			expr:      "max_over_time(foo{}[5m] offset 1h)",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- DeduplicateAndMerge
							- FunctionCall: max_over_time(...)
								- MatrixSelector: {__name__="foo"}[5m0s] offset 1h0m0s
			`,
		},
		"range vector selector with negative offset": {
			expr:      "max_over_time(foo{}[5m] offset -1h)",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- DeduplicateAndMerge
						- FunctionCall: max_over_time(...)
							- MatrixSelector: {__name__="foo"}[5m0s] offset -1h0m0s
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonModifiersNotCachable,
		},
		"subquery with positive offset": {
			expr:      "max_over_time((foo{})[5m:] offset 1h)",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- DeduplicateAndMerge
							- FunctionCall: max_over_time(...)
								- Subquery: [5m0s:1m0s] offset 1h0m0s
									- VectorSelector: {__name__="foo"}
			`,
		},
		"subquery with negative offset": {
			expr:      "max_over_time((foo{})[5m:] offset -1h)",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- DeduplicateAndMerge
						- FunctionCall: max_over_time(...)
							- Subquery: [5m0s:1m0s] offset -1h0m0s
								- VectorSelector: {__name__="foo"}
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonModifiersNotCachable,
		},
		"subquery with inner selector with negative offset": {
			expr:      "max_over_time((foo{} offset -1h)[5m:])",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- DeduplicateAndMerge
						- FunctionCall: max_over_time(...)
							- Subquery: [5m0s:1m0s]
								- VectorSelector: {__name__="foo"} offset -1h0m0s
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonModifiersNotCachable,
		},

		"request entirely in freshness window": {
			expr:      "foo{}",
			timeRange: entirelyInFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- VectorSelector: {__name__="foo"}
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonTooNew,
		},
		"request straddles freshness window": {
			expr:      "foo{}",
			timeRange: straddlesFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- VectorSelector: {__name__="foo"}
			`,
		},

		"instant vector selector with @ modifier before end time of query range": {
			expr:      "foo{} @ 100",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- StepInvariantExpression
							- VectorSelector: {__name__="foo"} @ 100000 (1970-01-01T00:01:40Z)
			`,
		},
		"range vector selector with @ modifier before end time of query range": {
			expr:      "max_over_time(foo[5m] @ 100)",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- StepInvariantExpression
							- DeduplicateAndMerge
								- FunctionCall: max_over_time(...)
									- MatrixSelector: {__name__="foo"}[5m0s] @ 100000 (1970-01-01T00:01:40Z)
			`,
		},
		"subquery with @ modifier before end time of query range": {
			expr:      "max_over_time((foo)[5m:] @ 100)",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- StepInvariantExpression
							- DeduplicateAndMerge
								- FunctionCall: max_over_time(...)
									- Subquery: [5m0s:1m0s] @ 100000 (1970-01-01T00:01:40Z)
										- VectorSelector: {__name__="foo"}
			`,
		},
		"subquery with nested selector with @ modifier before end time of query range": {
			expr:      "max_over_time((foo @ 100)[5m:])",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- DeduplicateAndMerge
							- FunctionCall: max_over_time(...)
								- Subquery: [5m0s:1m0s]
									- StepInvariantExpression
										- VectorSelector: {__name__="foo"} @ 100000 (1970-01-01T00:01:40Z)
			`,
		},
		"instant vector selector with @ modifier after end time of query range": {
			expr:      "foo{} @ 200000",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- StepInvariantExpression
						- VectorSelector: {__name__="foo"} @ 200000000 (1970-01-03T07:33:20Z)
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonModifiersNotCachable,
		},
		"range vector selector with @ modifier after end time of query range": {
			expr:      "max_over_time(foo[5m] @ 200000)",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- StepInvariantExpression
						- DeduplicateAndMerge
							- FunctionCall: max_over_time(...)
								- MatrixSelector: {__name__="foo"}[5m0s] @ 200000000 (1970-01-03T07:33:20Z)
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonModifiersNotCachable,
		},
		"subquery with @ modifier after end time of query range": {
			expr:      "max_over_time((foo)[5m:] @ 200000)",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- StepInvariantExpression
						- DeduplicateAndMerge
							- FunctionCall: max_over_time(...)
								- Subquery: [5m0s:1m0s] @ 200000000 (1970-01-03T07:33:20Z)
									- VectorSelector: {__name__="foo"}
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonModifiersNotCachable,
		},
		"subquery with nested selector with @ modifier after end time of query range": {
			expr:      "max_over_time((foo @ 200000)[5m:])",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- DeduplicateAndMerge
						- FunctionCall: max_over_time(...)
							- Subquery: [5m0s:1m0s]
								- StepInvariantExpression
									- VectorSelector: {__name__="foo"} @ 200000000 (1970-01-03T07:33:20Z)
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonModifiersNotCachable,
		},
		"instant vector selector with @ modifier before max freshness threshold, query straddles max freshness threshold": {
			expr:      "foo{} @ 100",
			timeRange: straddlesFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- StepInvariantExpression
							- VectorSelector: {__name__="foo"} @ 100000 (1970-01-01T00:01:40Z)
			`,
		},
		"range vector selector with @ modifier before max freshness threshold, query straddles max freshness threshold": {
			expr:      "max_over_time(foo[5m] @ 100)",
			timeRange: straddlesFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- StepInvariantExpression
							- DeduplicateAndMerge
								- FunctionCall: max_over_time(...)
									- MatrixSelector: {__name__="foo"}[5m0s] @ 100000 (1970-01-01T00:01:40Z)
			`,
		},
		"subquery with @ modifier before max freshness threshold, query straddles max freshness threshold": {
			expr:      "max_over_time((foo)[5m:] @ 100)",
			timeRange: straddlesFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- StepInvariantExpression
							- DeduplicateAndMerge
								- FunctionCall: max_over_time(...)
									- Subquery: [5m0s:1m0s] @ 100000 (1970-01-01T00:01:40Z)
										- VectorSelector: {__name__="foo"}
			`,
		},
		"subquery with nested selector with @ modifier before max freshness threshold, query straddles max freshness threshold": {
			expr:      "max_over_time((foo @ 100)[5m:])",
			timeRange: straddlesFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- DeduplicateAndMerge
							- FunctionCall: max_over_time(...)
								- Subquery: [5m0s:1m0s]
									- StepInvariantExpression
										- VectorSelector: {__name__="foo"} @ 100000 (1970-01-01T00:01:40Z)
			`,
		},
		"instant vector selector with @ modifier after max freshness threshold, query straddles max freshness threshold": {
			expr:      "foo{} @ 1704160800",
			timeRange: straddlesFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- StepInvariantExpression
						- VectorSelector: {__name__="foo"} @ 1704160800000 (2024-01-02T02:00:00Z)
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonModifiersNotCachable,
		},
		"range vector selector with @ modifier after max freshness threshold, query straddles max freshness threshold": {
			expr:      "max_over_time(foo[5m] @ 1704160800)",
			timeRange: straddlesFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- StepInvariantExpression
						- DeduplicateAndMerge
							- FunctionCall: max_over_time(...)
								- MatrixSelector: {__name__="foo"}[5m0s] @ 1704160800000 (2024-01-02T02:00:00Z)
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonModifiersNotCachable,
		},
		"subquery with @ modifier after max freshness threshold, query straddles max freshness threshold": {
			expr:      "max_over_time((foo)[5m:] @ 1704160800)",
			timeRange: straddlesFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- StepInvariantExpression
						- DeduplicateAndMerge
							- FunctionCall: max_over_time(...)
								- Subquery: [5m0s:1m0s] @ 1704160800000 (2024-01-02T02:00:00Z)
									- VectorSelector: {__name__="foo"}
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonModifiersNotCachable,
		},
		"subquery with nested selector with @ modifier after max freshness threshold, query straddles max freshness threshold": {
			expr:      "max_over_time((foo @ 1704160800)[5m:])",
			timeRange: straddlesFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- DeduplicateAndMerge
						- FunctionCall: max_over_time(...)
							- Subquery: [5m0s:1m0s]
								- StepInvariantExpression
									- VectorSelector: {__name__="foo"} @ 1704160800000 (2024-01-02T02:00:00Z)
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonModifiersNotCachable,
		},

		"time range not step aligned and tenant does not have unaligned request caching enabled": {
			expr:                          "foo{}",
			timeRange:                     unalignedRangeQueryTimeRange,
			allowCachingUnalignedRequests: false,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- VectorSelector: {__name__="foo"}
			`,
			expectedNotCachableReason: splitandcache.NotCachableReasonUnalignedTimeRange,
		},
		"time range not step aligned and tenant has unaligned request caching enabled": {
			expr:                          "foo{}",
			timeRange:                     unalignedRangeQueryTimeRange,
			allowCachingUnalignedRequests: true,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- VectorSelector: {__name__="foo"}
			`,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			opts := requestoptions.Options{CacheDisabled: testCase.cachingDisabledByRequestOption}
			ctx := requestoptions.ContextWithOptions(context.Background(), opts)

			limits.CacheUnalignedQueries = testCase.allowCachingUnalignedRequests

			if testCase.expectUnchanged {
				testCase.expectedPlan = runOptimizationPass(t, ctx, testCase.expr, testCase.timeRange, false, false, false, limits, prometheus.NewPedanticRegistry(), timeNow)
			}

			reg := prometheus.NewPedanticRegistry()
			actual := runOptimizationPass(t, ctx, testCase.expr, testCase.timeRange, true, !testCase.disableSplitting, !testCase.disableCaching, limits, reg, timeNow)
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)

			expectedMetrics := fmt.Sprintf(`
				# HELP cortex_frontend_query_result_cache_skipped_total Total number of times a query was not cacheable because of a reason. This metric is tracked for each request when splitting is running inside MQE, and for each partial query otherwise.
				# TYPE cortex_frontend_query_result_cache_skipped_total counter
				cortex_frontend_query_result_cache_skipped_total{reason="unaligned-time-range"} %d
				cortex_frontend_query_result_cache_skipped_total{reason="too-new"} %d
				cortex_frontend_query_result_cache_skipped_total{reason="has-modifiers"} %d
			`,
				countForSkipReason(splitandcache.NotCachableReasonUnalignedTimeRange, testCase.expectedNotCachableReason),
				countForSkipReason(splitandcache.NotCachableReasonTooNew, testCase.expectedNotCachableReason),
				countForSkipReason(splitandcache.NotCachableReasonModifiersNotCachable, testCase.expectedNotCachableReason),
			)

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), "cortex_frontend_query_result_cache_skipped_total"))
		})
	}
}

func countForSkipReason(desiredSkipReason string, expectedSkipReason string) int {
	if desiredSkipReason == expectedSkipReason {
		return 1
	}
	return 0
}

func runOptimizationPass(
	t *testing.T,
	ctx context.Context,
	expr string,
	timeRange types.QueryTimeRange,
	enableOptimizationPass bool,
	enableSplitting bool,
	enableCaching bool,
	limits splitandcache.LimitsProvider,
	reg prometheus.Registerer,
	timeNow time.Time,
) string {
	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	if enableOptimizationPass {
		optimizationPass := splitandcache.NewOptimizationPass(enableSplitting, 24*time.Hour, enableCaching, limits, reg)
		optimizationPass.OverrideTimeNow(func() time.Time {
			return timeNow
		})

		planner.RegisterQueryPlanOptimizationPass(optimizationPass)
	}

	p, err := planner.NewQueryPlan(ctx, expr, timeRange, streamingpromql.DefaultLookbackDelta, false, streamingpromql.NoopPlanningObserver{})
	require.NoError(t, err)

	return p.String()
}
