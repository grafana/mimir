// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache_test

import (
	"context"
	"testing"
	"time"

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

		expectUnchanged bool
		expectedPlan    string
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
					- Cache
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
				- Cache
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
					- Cache
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
			// TODO: expectedNotCachableReason: NotCachableReasonModifiersNotCachable
		},
		"range vector selector with positive offset": {
			expr:      "max_over_time(foo{}[5m] offset 1h)",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache
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
			// TODO: expectedNotCachableReason: NotCachableReasonModifiersNotCachable
		},
		"subquery with positive offset": {
			expr:      "max_over_time((foo{})[5m:] offset 1h)",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache
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
			// TODO: expectedNotCachableReason: NotCachableReasonModifiersNotCachable
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
			// TODO: expectedNotCachableReason: NotCachableReasonModifiersNotCachable
		},

		"request entirely in freshness window": {
			expr:      "foo{}",
			timeRange: entirelyInFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- VectorSelector: {__name__="foo"}
			`,
			// TODO: expectedNotCachableReason: NotCachableReasonTooNew
		},
		"request straddles freshness window": {
			expr:      "foo{}",
			timeRange: straddlesFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache
						- VectorSelector: {__name__="foo"}
			`,
		},

		"instant vector selector with @ modifier before end time of query range": {
			expr:      "foo{} @ 100",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache
						- StepInvariantExpression
							- VectorSelector: {__name__="foo"} @ 100000 (1970-01-01T00:01:40Z)
			`,
		},
		"range vector selector with @ modifier before end time of query range": {
			expr:      "max_over_time(foo[5m] @ 100)",
			timeRange: rangeQueryTimeRange,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache
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
					- Cache
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
					- Cache
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
			// TODO: expectedNotCachableReason: NotCachableReasonModifiersNotCachable
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
			// TODO: expectedNotCachableReason: NotCachableReasonModifiersNotCachable
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
			// TODO: expectedNotCachableReason: NotCachableReasonModifiersNotCachable
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
			// TODO: expectedNotCachableReason: NotCachableReasonModifiersNotCachable
		},
		"instant vector selector with @ modifier before max freshness threshold, query straddles max freshness threshold": {
			expr:      "foo{} @ 100",
			timeRange: straddlesFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache
						- StepInvariantExpression
							- VectorSelector: {__name__="foo"} @ 100000 (1970-01-01T00:01:40Z)
			`,
		},
		"range vector selector with @ modifier before max freshness threshold, query straddles max freshness threshold": {
			expr:      "max_over_time(foo[5m] @ 100)",
			timeRange: straddlesFreshnessWindow,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache
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
					- Cache
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
					- Cache
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
			// TODO: expectedNotCachableReason: NotCachableReasonModifiersNotCachable
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
			// TODO: expectedNotCachableReason: NotCachableReasonModifiersNotCachable
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
			// TODO: expectedNotCachableReason: NotCachableReasonModifiersNotCachable
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
			// TODO: expectedNotCachableReason: NotCachableReasonModifiersNotCachable
		},

		"time range not step aligned and tenant does not have unaligned request caching enabled": {
			expr:                          "foo{}",
			timeRange:                     unalignedRangeQueryTimeRange,
			allowCachingUnalignedRequests: false,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- VectorSelector: {__name__="foo"}
			`,
			// TODO: expectedNotCachableReason: // TODO: NotCachableReasonTooNew
		},
		"time range not step aligned and tenant has unaligned request caching enabled": {
			expr:                          "foo{}",
			timeRange:                     unalignedRangeQueryTimeRange,
			allowCachingUnalignedRequests: true,
			expectedPlan: `
				- TimeRangeSplit: interval 24h0m0s
					- Cache
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
				testCase.expectedPlan = runOptimizationPass(t, ctx, testCase.expr, testCase.timeRange, false, false, false, limits, timeNow)
			}

			actual := runOptimizationPass(t, ctx, testCase.expr, testCase.timeRange, true, !testCase.disableSplitting, !testCase.disableCaching, limits, timeNow)
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)
		})
	}
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
	timeNow time.Time,
) string {
	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	if enableOptimizationPass {
		optimizationPass := splitandcache.NewOptimizationPass(enableSplitting, 24*time.Hour, enableCaching, limits)
		optimizationPass.OverrideTimeNow(func() time.Time {
			return timeNow
		})

		planner.RegisterQueryPlanOptimizationPass(optimizationPass)
	}

	p, err := planner.NewQueryPlan(ctx, expr, timeRange, streamingpromql.DefaultLookbackDelta, false, streamingpromql.NoopPlanningObserver{})
	require.NoError(t, err)

	return p.String()
}
