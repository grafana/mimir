// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
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
		expectCachingNotAttempted bool
	}{
		"instant query with instant vector result": {
			expr:                      "foo{}",
			timeRange:                 types.NewInstantQueryTimeRange(time.Now()),
			expectUnchanged:           true,
			expectCachingNotAttempted: true,
		},
		"instant query with range vector result": {
			expr:                      "foo[5m]",
			timeRange:                 types.NewInstantQueryTimeRange(time.Now()),
			expectUnchanged:           true,
			expectCachingNotAttempted: true,
		},
		"scalar range query": {
			expr:                      "scalar(foo)",
			timeRange:                 rangeQueryTimeRange,
			expectUnchanged:           true,
			expectCachingNotAttempted: true,
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

		// We want to only evaluate step-invariant expressions once per query request, rather than once per split.
		// Each child of a step-invariant expression will be evaluated at T=0, and the StepInvariantExpression will
		// be evaluated over the split time range. So we need to duplicate the step-invariant expression for each split,
		// so the result can be used for each split time range.
		"single step-invariant expression": {
			expr:      "foo @ 20",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- StepInvariantExpression
							- Duplicate
								- VectorSelector: {__name__="foo"} @ 20000 (1970-01-01T00:00:20Z)
			`,
		},
		"duplicated step-invariant expression": {
			expr:      "(foo @ 20 - bar) + foo @ 20",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- BinaryExpression: LHS + RHS
							- LHS: BinaryExpression: LHS - RHS
								- LHS: ref#1 Duplicate
									- StepInvariantExpression
										- Duplicate
											- VectorSelector: {__name__="foo"} @ 20000 (1970-01-01T00:00:20Z)
								- RHS: VectorSelector: {__name__="bar"}
							- RHS: ref#1 Duplicate ...
			`,
		},
		"multiple step-invariant expressions": {
			expr:      "(foo @ 20 - bar) + bar @ 30",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- BinaryExpression: LHS + RHS
							- LHS: BinaryExpression: LHS - RHS
								- LHS: StepInvariantExpression
									- Duplicate
										- VectorSelector: {__name__="foo"} @ 20000 (1970-01-01T00:00:20Z)
								- RHS: VectorSelector: {__name__="bar"}
							- RHS: StepInvariantExpression
								- Duplicate
									- VectorSelector: {__name__="bar"} @ 30000 (1970-01-01T00:00:30Z)
			`,
		},

		"instant vector selector with @ modifier before end time of query range": {
			expr:      "foo{} @ 100",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache: split interval 24h0m0s
						- StepInvariantExpression
							- Duplicate
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
							- Duplicate
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
							- Duplicate
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
										- Duplicate
											- VectorSelector: {__name__="foo"} @ 100000 (1970-01-01T00:01:40Z)
			`,
		},
		"instant vector selector with @ modifier after end time of query range": {
			expr:      "foo{} @ 200000",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- StepInvariantExpression
						- Duplicate
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
						- Duplicate
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
						- Duplicate
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
									- Duplicate
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
							- Duplicate
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
							- Duplicate
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
							- Duplicate
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
										- Duplicate
											- VectorSelector: {__name__="foo"} @ 100000 (1970-01-01T00:01:40Z)
			`,
		},
		"instant vector selector with @ modifier after max freshness threshold, query straddles max freshness threshold": {
			expr:      "foo{} @ 1704160800",
			timeRange: straddlesFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- StepInvariantExpression
						- Duplicate
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
						- Duplicate
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
						- Duplicate
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
									- Duplicate
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

			cachingAttempted := !testCase.disableCaching && !testCase.cachingDisabledByRequestOption && !testCase.expectCachingNotAttempted
			expectedMetrics := fmt.Sprintf(`
				# HELP cortex_frontend_query_result_cache_skipped_total Total number of times a query was not cacheable. This metric is tracked for each request when time-splitting is running inside MQE, and for each partial query otherwise.
				# TYPE cortex_frontend_query_result_cache_skipped_total counter
				cortex_frontend_query_result_cache_skipped_total{reason="unaligned-time-range"} %d
				cortex_frontend_query_result_cache_skipped_total{reason="too-new"} %d
				cortex_frontend_query_result_cache_skipped_total{reason="has-modifiers"} %d
				# HELP cortex_frontend_query_result_cache_attempted_total Total number of queries that were attempted to be fetched from cache.
				# TYPE cortex_frontend_query_result_cache_attempted_total counter
				cortex_frontend_query_result_cache_attempted_total %d
			`,
				countForSkipReason(splitandcache.NotCachableReasonUnalignedTimeRange, testCase.expectedNotCachableReason),
				countForSkipReason(splitandcache.NotCachableReasonTooNew, testCase.expectedNotCachableReason),
				countForSkipReason(splitandcache.NotCachableReasonModifiersNotCachable, testCase.expectedNotCachableReason),
				countForBool(cachingAttempted),
			)

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), "cortex_frontend_query_result_cache_skipped_total", "cortex_frontend_query_result_cache_attempted_total"))
		})
	}
}

func countForSkipReason(desiredSkipReason string, expectedSkipReason string) int {
	if desiredSkipReason == expectedSkipReason {
		return 1
	}
	return 0
}

func countForBool(b bool) int {
	if b {
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

	logger := log.NewNopLogger()
	planner.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(false, false, reg, logger))

	if enableOptimizationPass {
		optimizationPass := splitandcache.NewOptimizationPass(enableSplitting, 24*time.Hour, enableCaching, limits, reg, logger)
		optimizationPass.OverrideTimeNow(func() time.Time {
			return timeNow
		})

		planner.RegisterQueryPlanOptimizationPass(optimizationPass)
	}

	p, err := planner.NewQueryPlan(ctx, expr, timeRange, streamingpromql.DefaultLookbackDelta, false, streamingpromql.NoopPlanningObserver{})
	require.NoError(t, err)

	return p.String()
}
