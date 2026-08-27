// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestBuildMatchers(t *testing.T) {
	t.Run("single series single hint", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 1)
		hints := &Hints{Include: []string{"container"}}
		expected := types.Matchers([]types.Matcher{{
			Type:  labels.MatchRegexp,
			Name:  "container",
			Value: "querier",
		}})

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("multiple series single hint", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 3)
		hints := &Hints{Include: []string{"container"}}
		expected := types.Matchers([]types.Matcher{{
			Type:  labels.MatchRegexp,
			Name:  "container",
			Value: "querier|query-frontend|store-gateway",
		}})

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("multiple series multiple hints", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 3)
		hints := &Hints{Include: []string{"container", "region"}}
		expected := types.Matchers([]types.Matcher{
			{
				Type:  labels.MatchRegexp,
				Name:  "container",
				Value: "querier|query-frontend|store-gateway",
			},
			{
				Type:  labels.MatchRegexp,
				Name:  "region",
				Value: "prod-test-1|prod-test-2|prod-test-3",
			},
		})

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("too many values single hint", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 128)
		hints := &Hints{Include: []string{"pod"}}

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Empty(t, res)
	})

	t.Run("too many values multiple hints", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 128)
		hints := &Hints{Include: []string{"pod", "container"}}
		expected := types.Matchers([]types.Matcher{{
			Type:  labels.MatchRegexp,
			Name:  "container",
			Value: "querier|query-frontend|store-gateway",
		}})

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("nil hints: returns nil without generating matchers", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 3)
		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, nil)
		require.Nil(t, res)
	})

	t.Run("without matching: generates matchers for all non-excluded labels present on all series", func(t *testing.T) {
		// generateSeriesMetadata produces series with __name__, container, pod, region.
		// __name__ is always skipped; container, pod and region each appear on all 3 series.
		series := generateSeriesMetadata("http_requests_total", 3)
		hints := &Hints{}
		expected := types.Matchers([]types.Matcher{
			{Type: labels.MatchRegexp, Name: "container", Value: "querier|query-frontend|store-gateway"},
			{Type: labels.MatchRegexp, Name: "pod", Value: "querier-0|query-frontend-1|store-gateway-2"},
			{Type: labels.MatchRegexp, Name: "region", Value: "prod-test-1|prod-test-2|prod-test-3"},
		})

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("without matching with excluded label: excluded label does not appear in matchers", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 3)
		hints := &Hints{Exclude: []string{"pod", "region"}}
		expected := types.Matchers([]types.Matcher{
			{Type: labels.MatchRegexp, Name: "container", Value: "querier|query-frontend|store-gateway"},
		})

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("without matching with empty series: returns nil", func(t *testing.T) {
		hints := &Hints{}
		res := BuildMatchers(context.Background(), log.NewNopLogger(), nil, hints)
		require.Nil(t, res)
	})

	t.Run("without matching with too many values: label with too many unique values is skipped", func(t *testing.T) {
		// generateSeriesMetadata with 128 series produces 128 unique pod values (one per series),
		// exceeding maxHintMatcherValues (64). Pod should be skipped but container (3 unique) and
		// region (3 unique) should still produce matchers.
		series := generateSeriesMetadata("http_requests_total", 128)
		hints := &Hints{} // exclude-matching mode with no exclusions
		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)

		// pod has 128 unique values → skipped; container and region have ≤64 values → included.
		require.Len(t, res, 2)
		require.Equal(t, "container", res[0].Name)
		require.Equal(t, "region", res[1].Name)
	})

	t.Run("without matching with heterogeneous labels: absent label matched with empty string", func(t *testing.T) {
		series := []types.SeriesMetadata{
			{Labels: labels.FromStrings("env", "prod", "region", "us-east")},
			{Labels: labels.FromStrings("env", "prod")}, // no region label
		}
		hints := &Hints{} // exclude-matching, no exclusions
		expected := types.Matchers{
			{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			{Type: labels.MatchRegexp, Name: "region", Value: "|us-east"},
		}

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		// region is absent from one series, so the matcher includes the empty string
		// to also match RHS series without a region label.
		require.Equal(t, expected, res)
	})

	t.Run("without matching excludes all labels: returns nil", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 3)
		// Exclude every non-__name__ label that generateSeriesMetadata produces.
		hints := &Hints{Exclude: []string{"container", "pod", "region"}}
		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Nil(t, res)
	})

	t.Run("without matching with heterogeneous labels across series: each series-specific label includes empty alternative", func(t *testing.T) {
		// Some labels are present on only a subset of series: "service" appears on
		// some series but not others, and "node" appears on a different subset.
		// The optimizer must include "" in both matchers so the RHS can match
		// series regardless of which labels they have.
		series := []types.SeriesMetadata{
			{Labels: labels.FromStrings("entity_type", "Service", "env", "prod", "service", "checkout")},
			{Labels: labels.FromStrings("entity_type", "Service", "env", "prod", "service", "payments")},
			{Labels: labels.FromStrings("entity_type", "Node", "env", "prod", "node", "host-1")},
			{Labels: labels.FromStrings("entity_type", "Node", "env", "prod", "node", "host-2")},
		}
		hints := &Hints{} // exclude-matching, no exclusions (default matching)
		expected := types.Matchers{
			{Type: labels.MatchRegexp, Name: "entity_type", Value: "Node|Service"},
			{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			{Type: labels.MatchRegexp, Name: "node", Value: "|host-1|host-2"},
			{Type: labels.MatchRegexp, Name: "service", Value: "|checkout|payments"},
		}

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Equal(t, expected, res)
	})
}

func generateSeriesMetadata(name string, num int) []types.SeriesMetadata {
	var out []types.SeriesMetadata

	for i := range num {
		var container string
		switch i % 3 {
		case 0:
			container = "querier"
		case 1:
			container = "query-frontend"
		case 2:
			container = "store-gateway"
		}

		out = append(out, types.SeriesMetadata{
			Labels: labels.FromMap(map[string]string{
				"__name__":  name,
				"container": container,
				"pod":       fmt.Sprintf("%s-%d", container, i),
				"region":    fmt.Sprintf("prod-test-%d", i%3+1),
			}),
		})
	}

	return out
}

func BenchmarkBuildMatchers(b *testing.B) {
	series := generateSeriesMetadata("http_requests_total", 1024)
	ctx := context.Background()
	logger := log.NewNopLogger()

	b.Run("container", func(b *testing.B) {
		for b.Loop() {
			_ = BuildMatchers(ctx, logger, series, &Hints{Include: []string{"container"}})
		}
	})
	b.Run("container,region", func(b *testing.B) {
		for b.Loop() {
			_ = BuildMatchers(ctx, logger, series, &Hints{Include: []string{"container", "region"}})
		}
	})
	b.Run("container,region,pod", func(b *testing.B) {
		for b.Loop() {
			_ = BuildMatchers(ctx, logger, series, &Hints{Include: []string{"container", "region", "pod"}})
		}
	})
}

// TestTrimOperatorsRespectMutability guards the trim operators' handling of the
// FloatHistogram.TrimBuckets contract: TrimBuckets mutates its receiver in place, so the
// operators must only do so when they own the left histogram (canMutateLeft). When the left
// histogram may be shared (e.g. it is the "one" side of a group_right join, reused across
// multiple output series), mutating it in place corrupts the shared data and produces wrong
// results for the other samples that reference it.
func TestTrimOperatorsRespectMutability(t *testing.T) {
	// Standard-schema histogram spanning buckets (1,2], (2,4], (4,8], so a trim at 2 actually
	// removes/interpolates buckets and would be observable if the receiver were mutated.
	newHistogram := func() *histogram.FloatHistogram {
		return &histogram.FloatHistogram{
			Schema:          0,
			Count:           30,
			Sum:             100,
			PositiveSpans:   []histogram.Span{{Offset: 0, Length: 3}},
			PositiveBuckets: []float64{10, 10, 10},
		}
	}

	const rF = 2.0

	testCases := map[string]struct {
		op          parser.ItemType
		isUpperTrim bool
	}{
		"TRIM_UPPER": {op: parser.TRIM_UPPER, isUpperTrim: true},
		"TRIM_LOWER": {op: parser.TRIM_LOWER, isUpperTrim: false},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			f := arithmeticAndComparisonOperationFuncs[tc.op]
			require.NotNil(t, f)

			// Expected trimmed histogram, computed on a throwaway copy so the reference input is untouched.
			expected := newHistogram().TrimBuckets(rF, tc.isUpperTrim)

			t.Run("canMutateLeft=false does not mutate the shared left histogram", func(t *testing.T) {
				lH := newHistogram()
				original := lH.Copy()

				_, result, keep, valid, err := f(0, rF, lH, nil, false, false, nil)
				require.NoError(t, err)
				require.True(t, keep)
				require.True(t, valid)

				require.Equal(t, expected, result, "returned histogram should be correctly trimmed")
				require.NotSame(t, lH, result, "must return a copy, not the input")
				require.Equal(t, original, lH, "input histogram must not be mutated when canMutateLeft is false")
			})

			t.Run("canMutateLeft=true trims the left histogram in place", func(t *testing.T) {
				lH := newHistogram()

				_, result, keep, valid, err := f(0, rF, lH, nil, true, false, nil)
				require.NoError(t, err)
				require.True(t, keep)
				require.True(t, valid)

				require.Equal(t, expected, result, "returned histogram should be correctly trimmed")
				require.Same(t, lH, result, "may reuse the input in place when canMutateLeft is true")
			})
		})
	}
}

func TestVectorVectorBinaryOperationEvaluator_MissingSideOptions(t *testing.T) {
	timeRange := types.NewRangeQueryTimeRange(time.Unix(0, 0), time.Unix(60, 0), time.Minute)
	firstStep := timeRange.IndexTime(0)
	secondStep := timeRange.IndexTime(1)

	floatPoint := func(timestamp int64) promql.FPoint {
		return promql.FPoint{T: timestamp, F: 3}
	}
	histogramPoint := func(timestamp int64) promql.HPoint {
		return promql.HPoint{
			T: timestamp,
			H: &histogram.FloatHistogram{Count: 2, Sum: 6},
		}
	}

	testCases := map[string]struct {
		op                  parser.ItemType
		left                types.InstantVectorSeriesData
		right               types.InstantVectorSeriesData
		options             computeResultOptions
		expectedFloatTimes  []int64
		expectedHistTimes   []int64
		expectedAnnotations int
		expectSeparate      bool
	}{
		"missing left evaluates floats with nil presence": {
			op:                 parser.MUL,
			right:              types.InstantVectorSeriesData{Floats: []promql.FPoint{floatPoint(firstStep)}},
			expectedFloatTimes: []int64{firstStep},
		},
		"missing left evaluates only absent steps": {
			op:                 parser.MUL,
			right:              types.InstantVectorSeriesData{Floats: []promql.FPoint{floatPoint(firstStep), floatPoint(secondStep)}},
			options:            computeResultOptions{missingLeft: missingSideOptions{groupPresence: []int{4, -1}}},
			expectedFloatTimes: []int64{secondStep},
		},
		"missing left suppresses a present step": {
			op:      parser.MUL,
			right:   types.InstantVectorSeriesData{Floats: []promql.FPoint{floatPoint(firstStep)}},
			options: computeResultOptions{missingLeft: missingSideOptions{groupPresence: []int{4, -1}}},
		},
		"missing left skip mode suppresses the step": {
			op:      parser.MUL,
			right:   types.InstantVectorSeriesData{Floats: []promql.FPoint{floatPoint(firstStep)}},
			options: computeResultOptions{missingLeft: missingSideOptions{mode: missingSkip}},
		},
		"missing left evaluates a histogram": {
			op:                parser.MUL,
			right:             types.InstantVectorSeriesData{Histograms: []promql.HPoint{histogramPoint(firstStep)}},
			expectedHistTimes: []int64{firstStep},
		},
		"missing left emits an invalid type annotation": {
			op:                  parser.ADD,
			right:               types.InstantVectorSeriesData{Histograms: []promql.HPoint{histogramPoint(firstStep)}},
			options:             computeResultOptions{missingLeft: missingSideOptions{groupPresence: []int{-1, -1}}},
			expectedAnnotations: 1,
		},
		"missing left suppresses an invalid type annotation": {
			op:      parser.ADD,
			right:   types.InstantVectorSeriesData{Histograms: []promql.HPoint{histogramPoint(firstStep)}},
			options: computeResultOptions{missingLeft: missingSideOptions{groupPresence: []int{4, -1}}},
		},
		"missing left keeps separate output mode": {
			op:                 parser.MUL,
			right:              types.InstantVectorSeriesData{Floats: []promql.FPoint{floatPoint(firstStep)}},
			options:            computeResultOptions{missingLeft: missingSideOptions{mode: missingLeftSeparate}},
			expectedFloatTimes: []int64{firstStep},
			expectSeparate:     true,
		},
		"missing right evaluates floats with nil presence": {
			op:                 parser.MUL,
			left:               types.InstantVectorSeriesData{Floats: []promql.FPoint{floatPoint(firstStep)}},
			expectedFloatTimes: []int64{firstStep},
		},
		"missing right evaluates only absent steps": {
			op:                 parser.MUL,
			left:               types.InstantVectorSeriesData{Floats: []promql.FPoint{floatPoint(firstStep), floatPoint(secondStep)}},
			options:            computeResultOptions{missingRight: missingSideOptions{groupPresence: []int{8, -1}}},
			expectedFloatTimes: []int64{secondStep},
		},
		"missing right suppresses a present step": {
			op:      parser.MUL,
			left:    types.InstantVectorSeriesData{Floats: []promql.FPoint{floatPoint(firstStep)}},
			options: computeResultOptions{missingRight: missingSideOptions{groupPresence: []int{8, -1}}},
		},
		"missing right skip mode suppresses the step": {
			op:      parser.MUL,
			left:    types.InstantVectorSeriesData{Floats: []promql.FPoint{floatPoint(firstStep)}},
			options: computeResultOptions{missingRight: missingSideOptions{mode: missingSkip}},
		},
		"missing right evaluates a histogram": {
			op:                parser.MUL,
			left:              types.InstantVectorSeriesData{Histograms: []promql.HPoint{histogramPoint(firstStep)}},
			expectedHistTimes: []int64{firstStep},
		},
		"missing right emits an invalid type annotation": {
			op:                  parser.ADD,
			left:                types.InstantVectorSeriesData{Histograms: []promql.HPoint{histogramPoint(firstStep)}},
			options:             computeResultOptions{missingRight: missingSideOptions{groupPresence: []int{-1, -1}}},
			expectedAnnotations: 1,
		},
		"missing right suppresses an invalid type annotation": {
			op:      parser.ADD,
			left:    types.InstantVectorSeriesData{Histograms: []promql.HPoint{histogramPoint(firstStep)}},
			options: computeResultOptions{missingRight: missingSideOptions{groupPresence: []int{8, -1}}},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			leftBefore := cloneBinaryOperationTestData(testCase.left)
			rightBefore := cloneBinaryOperationTestData(testCase.right)
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
			fillValue := 2.0
			var fillLeft, fillRight *float64
			if len(testCase.right.Floats)+len(testCase.right.Histograms) > 0 {
				fillLeft = &fillValue
			}
			if len(testCase.left.Floats)+len(testCase.left.Histograms) > 0 {
				fillRight = &fillValue
			}

			evaluator, err := newVectorVectorBinaryOperationEvaluator(testCase.op, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, fillLeft, fillRight)
			require.NoError(t, err)

			result, separateResult, err := evaluator.computeResult(testCase.left, testCase.right, false, false, testCase.options)
			require.NoError(t, err)
			t.Cleanup(func() {
				types.PutInstantVectorSeriesData(result, memoryConsumptionTracker)
				types.PutInstantVectorSeriesData(separateResult, memoryConsumptionTracker)
				require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
			})

			actualResult := result
			if testCase.expectSeparate {
				require.Empty(t, result.Floats)
				require.Empty(t, result.Histograms)
				actualResult = separateResult
			} else {
				require.Empty(t, separateResult.Floats)
				require.Empty(t, separateResult.Histograms)
			}

			require.Equal(t, testCase.expectedFloatTimes, binaryOperationFloatTimes(actualResult.Floats))
			require.Equal(t, testCase.expectedHistTimes, binaryOperationHistogramTimes(actualResult.Histograms))
			if len(actualResult.Histograms) > 0 {
				inputHistogram := testCase.left.Histograms
				if len(inputHistogram) == 0 {
					inputHistogram = testCase.right.Histograms
				}
				require.Len(t, inputHistogram, 1)
				require.NotSame(t, inputHistogram[0].H, actualResult.Histograms[0].H)
			}
			require.Len(t, evaluator.annotations, testCase.expectedAnnotations)
			require.Equal(t, leftBefore, testCase.left)
			require.Equal(t, rightBefore, testCase.right)
		})
	}
}

func TestVectorVectorBinaryOperationEvaluator_InvalidMissingSideOptions(t *testing.T) {
	timeRange := types.NewRangeQueryTimeRange(time.Unix(0, 0), time.Unix(60, 0), time.Minute)
	fillValue := 2.0

	testCases := map[string]struct {
		left          types.InstantVectorSeriesData
		right         types.InstantVectorSeriesData
		options       computeResultOptions
		expectedError string
		expectWrapped bool
	}{
		"unknown missing-left mode": {
			options:       computeResultOptions{missingLeft: missingSideOptions{mode: missingSideMode(99)}},
			expectedError: "unknown missing-left mode 99",
		},
		"unknown missing-right mode": {
			options:       computeResultOptions{missingRight: missingSideOptions{mode: missingSideMode(99)}},
			expectedError: "unknown missing-right mode 99",
		},
		"separate missing-right output": {
			options:       computeResultOptions{missingRight: missingSideOptions{mode: missingLeftSeparate}},
			expectedError: "cannot produce separate missing-right output",
		},
		"short missing-left presence": {
			options:       computeResultOptions{missingLeft: missingSideOptions{groupPresence: []int{-1}}},
			expectedError: "missing-left presence has length 1, expected 2",
		},
		"short missing-right presence": {
			options:       computeResultOptions{missingRight: missingSideOptions{groupPresence: []int{-1}}},
			expectedError: "missing-right presence has length 1, expected 2",
		},
		"empty missing-left presence": {
			options:       computeResultOptions{missingLeft: missingSideOptions{groupPresence: []int{}}},
			expectedError: "missing-left presence has length 0, expected 2",
		},
		"empty missing-right presence": {
			options:       computeResultOptions{missingRight: missingSideOptions{groupPresence: []int{}}},
			expectedError: "missing-right presence has length 0, expected 2",
		},
		"missing-left timestamp after query range": {
			right: types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: timeRange.IndexTime(2), F: 3}}},
			options: computeResultOptions{
				missingLeft: missingSideOptions{groupPresence: []int{-1, -1}},
			},
			expectedError: "look up group presence at timestamp 120000: step index 2 is outside presence length 2",
			expectWrapped: true,
		},
		"missing-right timestamp before query range": {
			left: types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: -time.Minute.Milliseconds(), F: 3}}},
			options: computeResultOptions{
				missingRight: missingSideOptions{groupPresence: []int{-1, -1}},
			},
			expectedError: "look up group presence at timestamp -60000: step index -1 is outside presence length 2",
			expectWrapped: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
			evaluator, err := newVectorVectorBinaryOperationEvaluator(parser.MUL, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, &fillValue, &fillValue)
			require.NoError(t, err)

			result, separateResult, err := evaluator.computeResult(testCase.left, testCase.right, false, false, testCase.options)
			require.ErrorContains(t, err, testCase.expectedError)
			if testCase.expectWrapped {
				require.Error(t, errors.Unwrap(err))
			}
			require.Empty(t, result.Floats)
			require.Empty(t, result.Histograms)
			require.Empty(t, separateResult.Floats)
			require.Empty(t, separateResult.Histograms)
			require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
		})
	}
}

func TestVectorVectorBinaryOperationEvaluator_PooledHistogramOwnership(t *testing.T) {
	timeRange := types.NewRangeQueryTimeRange(time.Unix(0, 0), time.Unix(60, 0), time.Minute)
	fillValue := 2.0

	testCases := map[string]struct {
		sourceSide      string
		options         computeResultOptions
		expectOutput    bool
		expectSeparate  bool
		expectSourceNil bool
	}{
		"left ownership moves the histogram": {
			sourceSide:      "left",
			expectOutput:    true,
			expectSourceNil: true,
		},
		"right ownership moves the histogram": {
			sourceSide:      "right",
			expectOutput:    true,
			expectSourceNil: true,
		},
		"suppression returns the owned source": {
			sourceSide: "left",
			options: computeResultOptions{
				missingRight: missingSideOptions{groupPresence: []int{7, -1}},
			},
		},
		"separate output moves the right histogram": {
			sourceSide:      "right",
			options:         computeResultOptions{missingLeft: missingSideOptions{mode: missingLeftSeparate}},
			expectOutput:    true,
			expectSeparate:  true,
			expectSourceNil: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
			sourcePoints, err := types.HPointSlicePool.Get(1, memoryConsumptionTracker)
			require.NoError(t, err)
			sourcePoints = sourcePoints[:1]
			originalHistogram := &histogram.FloatHistogram{Count: 2, Sum: 6}
			sourcePoints[0] = promql.HPoint{T: timeRange.IndexTime(0), H: originalHistogram}

			var left, right types.InstantVectorSeriesData
			var fillLeft, fillRight *float64
			var takeOwnershipOfLeft, takeOwnershipOfRight bool
			switch testCase.sourceSide {
			case "left":
				left.Histograms = sourcePoints
				fillRight = &fillValue
				takeOwnershipOfLeft = true
			case "right":
				right.Histograms = sourcePoints
				fillLeft = &fillValue
				takeOwnershipOfRight = true
			default:
				require.FailNow(t, "unknown source side")
			}

			evaluator, err := newVectorVectorBinaryOperationEvaluator(parser.MUL, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, fillLeft, fillRight)
			require.NoError(t, err)
			result, separateResult, err := evaluator.computeResult(left, right, takeOwnershipOfLeft, takeOwnershipOfRight, testCase.options)
			require.NoError(t, err)

			if !testCase.expectOutput {
				require.Empty(t, result.Histograms)
				require.Empty(t, separateResult.Histograms)
				require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
				return
			}

			output := result
			otherOutput := separateResult
			if testCase.expectSeparate {
				output = separateResult
				otherOutput = result
			}
			require.Empty(t, otherOutput.Histograms)
			require.Len(t, output.Histograms, 1)
			require.Same(t, originalHistogram, output.Histograms[0].H)
			if testCase.expectSourceNil {
				require.Nil(t, sourcePoints[0].H)
			}

			types.PutInstantVectorSeriesData(result, memoryConsumptionTracker)
			types.PutInstantVectorSeriesData(separateResult, memoryConsumptionTracker)
			require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
		})
	}
}

func cloneBinaryOperationTestData(data types.InstantVectorSeriesData) types.InstantVectorSeriesData {
	result := types.InstantVectorSeriesData{Floats: append([]promql.FPoint(nil), data.Floats...)}
	if data.Histograms == nil {
		return result
	}

	result.Histograms = make([]promql.HPoint, len(data.Histograms))
	for i, point := range data.Histograms {
		result.Histograms[i] = promql.HPoint{T: point.T, H: point.H.Copy()}
	}
	return result
}

func binaryOperationFloatTimes(points []promql.FPoint) []int64 {
	if len(points) == 0 {
		return nil
	}

	timestamps := make([]int64, len(points))
	for i, point := range points {
		timestamps[i] = point.T
	}
	return timestamps
}

func binaryOperationHistogramTimes(points []promql.HPoint) []int64 {
	if len(points) == 0 {
		return nil
	}

	timestamps := make([]int64, len(points))
	for i, point := range points {
		timestamps[i] = point.T
	}
	return timestamps
}
