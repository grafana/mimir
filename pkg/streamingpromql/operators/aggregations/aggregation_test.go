// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/scalars"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// Most of the functionality of the aggregation operator is tested through the test scripts in
// pkg/streamingpromql/testdata.
//
// The output sorting behaviour is impossible to test through these scripts, so we instead test it here.
func TestAggregator_ReturnsGroupsFinishedFirstEarliest(t *testing.T) {
	testCases := map[string]struct {
		inputSeries               []labels.Labels
		grouping                  []string
		expectedOutputSeriesOrder []labels.Labels
	}{
		"empty input": {
			inputSeries:               []labels.Labels{},
			grouping:                  []string{},
			expectedOutputSeriesOrder: []labels.Labels{},
		},
		"all series grouped into single group": {
			inputSeries: []labels.Labels{
				labels.FromStrings("pod", "1"),
				labels.FromStrings("pod", "2"),
			},
			grouping: []string{},
			expectedOutputSeriesOrder: []labels.Labels{
				labels.EmptyLabels(),
			},
		},
		"input series already sorted by group": {
			inputSeries: []labels.Labels{
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "1", "group", "A"),
				labels.FromStrings("pod", "2", "group", "A"),
				labels.FromStrings("pod", "1", "group", "C"),
				labels.FromStrings("pod", "2", "group", "C"),
			},
			grouping: []string{"group"},
			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("group", "B"),
				labels.FromStrings("group", "A"),
				labels.FromStrings("group", "C"),
			},
		},
		"input series not sorted by group": {
			inputSeries: []labels.Labels{
				labels.FromStrings("pod", "1", "group", "A"),
				labels.FromStrings("pod", "1", "group", "B"),
				labels.FromStrings("pod", "2", "group", "B"),
				labels.FromStrings("pod", "1", "group", "C"),
				labels.FromStrings("pod", "2", "group", "A"),
				labels.FromStrings("pod", "2", "group", "C"),
			},
			grouping: []string{"group"},
			expectedOutputSeriesOrder: []labels.Labels{
				// Should sort so that the group that is completed first is returned first.
				labels.FromStrings("group", "B"),
				labels.FromStrings("group", "A"),
				labels.FromStrings("group", "C"),
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
			aggregator, err := NewAggregator(parser.SUM, testCase.grouping, false, memoryConsumptionTracker, annotations.New(), types.NewInstantQueryTimeRange(time.Now()), posrange.PositionRange{})
			require.NoError(t, err)

			outputSeries, err := aggregator.ComputeGroups(testutils.LabelsToSeriesMetadata(testCase.inputSeries))
			require.NoError(t, err)
			require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeriesOrder), outputSeries)
		})
	}
}

func TestAggregator_GroupLabelling(t *testing.T) {
	testCases := map[string]struct {
		grouping                    []string
		without                     bool
		inputSeries                 labels.Labels
		expectedOutputSeries        labels.Labels
		overrideExpectedOutputBytes []byte
	}{
		"grouping to a single series": {
			grouping:                    []string{},
			inputSeries:                 labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod"),
			expectedOutputSeries:        labels.EmptyLabels(),
			overrideExpectedOutputBytes: []byte{}, // Special case for grouping to a single series.
		},

		// Grouping with 'by'
		"grouping with 'by', single grouping label, input has only metric name": {
			grouping:             []string{"env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'by', single grouping label, input does not have grouping label": {
			grouping:             []string{"env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "foo", "bar"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'by', single grouping label, input does have grouping label": {
			grouping:             []string{"env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("env", "prod"),
		},
		"grouping with 'by', multiple grouping labels, input has only metric name": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'by', multiple grouping labels, input does not have any grouping labels": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "foo", "bar"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'by', multiple grouping labels, input has some grouping labels": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("env", "prod"),
		},
		"grouping with 'by', multiple grouping labels, input has superset of grouping labels": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1"),
		},
		"grouping with 'by', multiple grouping labels, input has all grouping labels and no others": {
			grouping:             []string{"cluster", "env"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1"),
		},
		"grouping with 'by', unsorted grouping labels": {
			grouping:             []string{"env", "cluster"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings("env", "prod", "cluster", "cluster-1"),
		},
		"grouping with 'by', grouping labels include __name__": {
			grouping:             []string{"cluster", "env", "__name__"},
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "foo", "bar"),
			expectedOutputSeries: labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1"),
		},

		// Grouping with 'without'
		"grouping with 'without', single grouping label, input has only metric name": {
			grouping:             []string{"env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'without', single grouping label, input does not have grouping label": {
			grouping:             []string{"env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "a-label", "a-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "f-label", "f-value"),
		},
		"grouping with 'without', single grouping label, input does have grouping label": {
			grouping:             []string{"env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "a-label", "a-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "f-label", "f-value"),
		},
		"grouping with 'without', multiple grouping labels, input has only metric name": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'without', multiple grouping labels, input does not have any grouping labels": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
		"grouping with 'without', multiple grouping labels, input has some grouping labels": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
		"grouping with 'without', multiple grouping labels, input has superset of grouping labels": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
		"grouping with 'without', multiple grouping labels, input has all grouping labels and no others": {
			grouping:             []string{"cluster", "env"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1"),
			expectedOutputSeries: labels.EmptyLabels(),
		},
		"grouping with 'without', unsorted grouping labels": {
			grouping:             []string{"env", "cluster"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "env", "prod", "cluster", "cluster-1", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
		"grouping with 'without', grouping labels include __name__": {
			grouping:             []string{"cluster", "env", "__name__"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
		"grouping with 'without', grouping labels include duplicates": {
			grouping:             []string{"cluster", "env", "cluster"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
		"grouping with 'without', grouping labels include label that sorts before __name__": {
			grouping:             []string{"cluster", "env", "__aaa__"},
			without:              true,
			inputSeries:          labels.FromStrings(model.MetricNameLabel, "my_metric", "__aaa__", "foo", "a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
			expectedOutputSeries: labels.FromStrings("a-label", "a-value", "d-label", "d-value", "f-label", "f-value"),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			aggregator, err := NewAggregator(parser.SUM, testCase.grouping, testCase.without, nil, nil, types.NewInstantQueryTimeRange(timestamp.Time(0)), posrange.PositionRange{})
			require.NoError(t, err)
			bytesFunc := aggregator.groupLabelsBytesFunc()
			labelsFunc := aggregator.groupLabelsFunc()

			actualLabels := labelsFunc(testCase.inputSeries)
			require.Equal(t, testCase.expectedOutputSeries, actualLabels)

			expectedBytes := string(testCase.overrideExpectedOutputBytes)
			if testCase.overrideExpectedOutputBytes == nil {
				expectedBytes = string(testCase.expectedOutputSeries.Bytes(nil))
			}

			actualBytes := string(bytesFunc(testCase.inputSeries))
			require.Equal(t, expectedBytes, actualBytes)
		})
	}
}

func TestAggregations_ReturnIncompleteGroupsOnEarlyClose(t *testing.T) {
	inputSeries := []labels.Labels{
		// Order series so group 1 will be the first complete group, but part of group 2 will have been loaded first.
		labels.FromStrings("group", "2", "idx", "1"),
		labels.FromStrings("group", "2", "idx", "2"),
		labels.FromStrings("group", "1", "idx", "3"),
		labels.FromStrings("group", "2", "idx", "4"),
		labels.FromStrings("group", "3", "idx", "5"),
	}

	expectedSimpleAggregationOutputSeries := []labels.Labels{
		labels.FromStrings("group", "1"),
		labels.FromStrings("group", "2"),
		labels.FromStrings("group", "3"),
	}

	rangeQueryTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(0), timestamp.Time(0).Add(5*time.Minute), time.Minute)
	instantQueryTimeRange := types.NewInstantQueryTimeRange(timestamp.Time(0))

	createSimpleAggregation := func(op parser.ItemType) func(types.InstantVectorOperator, types.QueryTimeRange, *limiter.MemoryConsumptionTracker) (types.InstantVectorOperator, error) {
		return func(inner types.InstantVectorOperator, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (types.InstantVectorOperator, error) {
			return NewAggregation(inner, timeRange, []string{"group"}, false, op, memoryConsumptionTracker, annotations.New(), posrange.PositionRange{})
		}
	}

	testCases := map[string]struct {
		createOperator                func(types.InstantVectorOperator, types.QueryTimeRange, *limiter.MemoryConsumptionTracker) (types.InstantVectorOperator, error)
		instant                       bool
		expectedSeries                []labels.Labels
		allowExpectedSeriesInAnyOrder bool
	}{
		"avg": {
			createOperator: createSimpleAggregation(parser.AVG),
			expectedSeries: expectedSimpleAggregationOutputSeries,
		},
		"count": {
			createOperator: createSimpleAggregation(parser.COUNT),
			expectedSeries: expectedSimpleAggregationOutputSeries,
		},
		"group": {
			createOperator: createSimpleAggregation(parser.GROUP),
			expectedSeries: expectedSimpleAggregationOutputSeries,
		},
		"max": {
			createOperator: createSimpleAggregation(parser.MAX),
			expectedSeries: expectedSimpleAggregationOutputSeries,
		},
		"min": {
			createOperator: createSimpleAggregation(parser.MIN),
			expectedSeries: expectedSimpleAggregationOutputSeries,
		},
		"quantile": {
			createOperator: func(inner types.InstantVectorOperator, queryTimeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (types.InstantVectorOperator, error) {
				param := scalars.NewScalarConstant(0.5, queryTimeRange, memoryConsumptionTracker, posrange.PositionRange{})
				return NewQuantileAggregation(inner, param, queryTimeRange, []string{"group"}, false, memoryConsumptionTracker, annotations.New(), posrange.PositionRange{})
			},
			expectedSeries: expectedSimpleAggregationOutputSeries,
		},
		"stddev": {
			createOperator: createSimpleAggregation(parser.STDDEV),
			expectedSeries: expectedSimpleAggregationOutputSeries,
		},
		"stdvar": {
			createOperator: createSimpleAggregation(parser.STDVAR),
			expectedSeries: expectedSimpleAggregationOutputSeries,
		},
		"sum": {
			createOperator: createSimpleAggregation(parser.SUM),
			expectedSeries: expectedSimpleAggregationOutputSeries,
		},
		"count_values": {
			createOperator: func(inner types.InstantVectorOperator, queryTimeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (types.InstantVectorOperator, error) {
				labelName := operators.NewStringLiteral("value", posrange.PositionRange{})
				return NewCountValues(inner, labelName, queryTimeRange, []string{"group"}, false, memoryConsumptionTracker, posrange.PositionRange{}), nil
			},
			instant:                       true,
			allowExpectedSeriesInAnyOrder: true,
			expectedSeries: []labels.Labels{
				labels.FromStrings("group", "1", "value", "0"),
				labels.FromStrings("group", "2", "value", "0"),
				labels.FromStrings("group", "2", "value", "{count:0, sum:0}"),
			},
		},
		// topk and bottomk are tested in the topkbottomk package.
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			timeRange := rangeQueryTimeRange

			if testCase.instant {
				timeRange = instantQueryTimeRange
			}

			inner := &operators.TestOperator{
				Series: inputSeries,
				Data: []types.InstantVectorSeriesData{
					createDummyData(t, false, timeRange, memoryConsumptionTracker),
					createDummyData(t, true, timeRange, memoryConsumptionTracker),
					createDummyData(t, false, timeRange, memoryConsumptionTracker),
					{}, // The last two series won't be read, so don't bother creating series for them.
					{},
				},
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}

			o, err := testCase.createOperator(inner, timeRange, memoryConsumptionTracker)
			require.NoError(t, err)

			series, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			if testCase.allowExpectedSeriesInAnyOrder {
				require.ElementsMatch(t, testutils.LabelsToSeriesMetadata(testCase.expectedSeries), series)
			} else {
				require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedSeries), series)
			}
			types.SeriesMetadataSlicePool.Put(&series, memoryConsumptionTracker)

			// Read the first output series to force the creation of incomplete groups.
			seriesData, err := o.NextSeries(ctx)
			require.NoError(t, err)

			types.PutInstantVectorSeriesData(seriesData, memoryConsumptionTracker)

			// Close the operator and confirm all memory has been released.
			o.Close()
			require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
		})
	}
}

func TestAggregation_RuntimeMatchersAdjusted(t *testing.T) {
	rangeQueryTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(0), timestamp.Time(0).Add(5*time.Minute), time.Minute)

	testCases := map[string]struct {
		grouping             []string
		without              bool
		matchers             types.Matchers
		expectedOutputSeries []types.SeriesMetadata
		expectedSeriesData   []types.InstantVectorSeriesData
	}{
		"matcher for grouping all series": {
			grouping: []string{"group"},
			without:  false,
			matchers: []types.Matcher{{Type: labels.MatchRegexp, Name: "group", Value: "1|2|3"}},
			expectedOutputSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("group", "1")},
				{Labels: labels.FromStrings("group", "2")},
				{Labels: labels.FromStrings("group", "3")},
			},
			expectedSeriesData: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 0, F: 0.0},
						{T: 60_000, F: 1.0},
						{T: 120_000, F: 2.0},
						{T: 180_000, F: 3.0},
						{T: 240_000, F: 4.0},
						{T: 300_000, F: 5.0},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 0, F: 0.0},
						{T: 60_000, F: 3.0},
						{T: 120_000, F: 6.0},
						{T: 180_000, F: 9.0},
						{T: 240_000, F: 12.0},
						{T: 300_000, F: 15.0},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 0, F: 0.0},
						{T: 60_000, F: 1.0},
						{T: 120_000, F: 2.0},
						{T: 180_000, F: 3.0},
						{T: 240_000, F: 4.0},
						{T: 300_000, F: 5.0},
					},
				},
			},
		},

		"matcher for grouping subset of series": {
			grouping: []string{"group"},
			without:  false,
			matchers: []types.Matcher{{Type: labels.MatchRegexp, Name: "group", Value: "2"}},
			expectedOutputSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("group", "2")},
			},
			expectedSeriesData: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 0, F: 0.0},
						{T: 60_000, F: 3.0},
						{T: 120_000, F: 6.0},
						{T: 180_000, F: 9.0},
						{T: 240_000, F: 12.0},
						{T: 300_000, F: 15.0},
					},
				},
			},
		},

		"matcher for without grouping should be ignored": {
			grouping: []string{"idx"},
			without:  true,
			matchers: []types.Matcher{{Type: labels.MatchRegexp, Name: "group", Value: "2"}},
			expectedOutputSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("group", "1")},
				{Labels: labels.FromStrings("group", "2")},
				{Labels: labels.FromStrings("group", "3")},
			},
			expectedSeriesData: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 0, F: 0.0},
						{T: 60_000, F: 1.0},
						{T: 120_000, F: 2.0},
						{T: 180_000, F: 3.0},
						{T: 240_000, F: 4.0},
						{T: 300_000, F: 5.0},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 0, F: 0.0},
						{T: 60_000, F: 3.0},
						{T: 120_000, F: 6.0},
						{T: 180_000, F: 9.0},
						{T: 240_000, F: 12.0},
						{T: 300_000, F: 15.0},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 0, F: 0.0},
						{T: 60_000, F: 1.0},
						{T: 120_000, F: 2.0},
						{T: 180_000, F: 3.0},
						{T: 240_000, F: 4.0},
						{T: 300_000, F: 5.0},
					},
				},
			},
		},

		"matcher for label not grouped should be ignored": {
			grouping: []string{"group"},
			without:  false,
			matchers: []types.Matcher{{Type: labels.MatchRegexp, Name: "idx", Value: "1|2"}},
			expectedOutputSeries: []types.SeriesMetadata{
				{Labels: labels.FromStrings("group", "1")},
				{Labels: labels.FromStrings("group", "2")},
				{Labels: labels.FromStrings("group", "3")},
			},
			expectedSeriesData: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 0, F: 0.0},
						{T: 60_000, F: 1.0},
						{T: 120_000, F: 2.0},
						{T: 180_000, F: 3.0},
						{T: 240_000, F: 4.0},
						{T: 300_000, F: 5.0},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 0, F: 0.0},
						{T: 60_000, F: 3.0},
						{T: 120_000, F: 6.0},
						{T: 180_000, F: 9.0},
						{T: 240_000, F: 12.0},
						{T: 300_000, F: 15.0},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 0, F: 0.0},
						{T: 60_000, F: 1.0},
						{T: 120_000, F: 2.0},
						{T: 180_000, F: 3.0},
						{T: 240_000, F: 4.0},
						{T: 300_000, F: 5.0},
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			timeRange := rangeQueryTimeRange
			inputSeries := []labels.Labels{
				labels.FromStrings("group", "2", "idx", "1"),
				labels.FromStrings("group", "2", "idx", "2"),
				labels.FromStrings("group", "1", "idx", "3"),
				labels.FromStrings("group", "2", "idx", "4"),
				labels.FromStrings("group", "3", "idx", "5"),
			}

			inner := &operators.TestOperator{
				Series: inputSeries,
				Data: []types.InstantVectorSeriesData{
					createDummyData(t, false, timeRange, memoryConsumptionTracker),
					createDummyData(t, false, timeRange, memoryConsumptionTracker),
					createDummyData(t, false, timeRange, memoryConsumptionTracker),
					createDummyData(t, false, timeRange, memoryConsumptionTracker),
					createDummyData(t, false, timeRange, memoryConsumptionTracker),
				},
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}

			op, err := NewAggregation(inner, timeRange, testCase.grouping, testCase.without, parser.SUM, memoryConsumptionTracker, annotations.New(), posrange.PositionRange{})
			require.NoError(t, err)

			series, err := op.SeriesMetadata(ctx, testCase.matchers)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedOutputSeries, series)

			for i := range testCase.expectedSeriesData {
				expected := testCase.expectedSeriesData[i]
				seriesData, err := op.NextSeries(ctx)
				require.NoError(t, err)
				require.Equal(t, expected, seriesData)
			}
		})
	}
}

func createDummyData(t *testing.T, histograms bool, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) types.InstantVectorSeriesData {
	d := types.InstantVectorSeriesData{}

	if histograms {
		var err error
		d.Histograms, err = types.HPointSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		require.NoError(t, err)

		for i := range timeRange.StepCount {
			d.Histograms = append(d.Histograms, promql.HPoint{T: timeRange.IndexTime(int64(i)), H: &histogram.FloatHistogram{Count: float64(i)}})
		}

	} else {
		var err error
		d.Floats, err = types.FPointSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		require.NoError(t, err)

		for i := range timeRange.StepCount {
			d.Floats = append(d.Floats, promql.FPoint{T: timeRange.IndexTime(int64(i)), F: float64(i)})
		}
	}

	return d
}
