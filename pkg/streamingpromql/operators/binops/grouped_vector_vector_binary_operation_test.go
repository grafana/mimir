// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestGroupedVectorVectorBinaryOperation_OutputSeriesSorting(t *testing.T) {
	testCases := map[string]struct {
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		matching   parser.VectorMatching
		op         parser.ItemType
		returnBool bool

		expectedOutputSeries []labels.Labels
	}{
		"no series on either side": {
			leftSeries:  []labels.Labels{},
			rightSeries: []labels.Labels{},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne},

			expectedOutputSeries: []labels.Labels{},
		},

		"no series on left side": {
			leftSeries: []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings("series", "a"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne},

			expectedOutputSeries: []labels.Labels{},
		},

		"no series on right side": {
			leftSeries: []labels.Labels{
				labels.FromStrings("series", "a"),
			},
			rightSeries: []labels.Labels{},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne},

			expectedOutputSeries: []labels.Labels{},
		},

		"single series on each side matched and both sides' series are in the same order": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne, MatchingLabels: []string{"group"}, On: true},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "a"),
				labels.FromStrings("group", "b"),
			},
		},

		"single series on each side matched and both sides' series are in different order with group_left": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "a"),
				labels.FromStrings("group", "b"),
			},
		},

		"single series on each side matched and both sides' series are in different order with group_right": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardOneToMany, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "b"),
				labels.FromStrings("group", "a"),
			},
		},

		"multiple series on left side match to a single series on right side with group_left": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a", "idx", "1"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a", "idx", "2"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a", "idx", "3"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b", "idx", "3"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b", "idx", "1"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b", "idx", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "a", "idx", "1"),
				labels.FromStrings("group", "a", "idx", "2"),
				labels.FromStrings("group", "a", "idx", "3"),
				labels.FromStrings("group", "b", "idx", "3"),
				labels.FromStrings("group", "b", "idx", "1"),
				labels.FromStrings("group", "b", "idx", "2"),
			},
		},

		"multiple series on left side match to a single series on right side with group_right": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a", "idx", "1"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a", "idx", "2"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a", "idx", "3"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b", "idx", "3"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b", "idx", "1"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b", "idx", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardOneToMany, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "b"),
				labels.FromStrings("group", "a"),
			},
		},

		"single series on left side match to multiple series on right side with group_left": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b", "idx", "1"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b", "idx", "2"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b", "idx", "3"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a", "idx", "3"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a", "idx", "1"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a", "idx", "2"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "a"),
				labels.FromStrings("group", "b"),
			},
		},

		"single series on left side match to multiple series on right side with group_right": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b", "idx", "1"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b", "idx", "2"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b", "idx", "3"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a", "idx", "3"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a", "idx", "1"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a", "idx", "2"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardOneToMany, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "b", "idx", "1"),
				labels.FromStrings("group", "b", "idx", "2"),
				labels.FromStrings("group", "b", "idx", "3"),
				labels.FromStrings("group", "a", "idx", "3"),
				labels.FromStrings("group", "a", "idx", "1"),
				labels.FromStrings("group", "a", "idx", "2"),
			},
		},

		"multiple series on left side match to multiple series on right side with group_left": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a", "idx_left", "1"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b", "idx_left", "3"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a", "idx_left", "2"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a", "idx_left", "3"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b", "idx_left", "1"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b", "idx_left", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b", "idx_right", "4"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b", "idx_right", "5"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b", "idx_right", "6"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a", "idx_right", "5"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a", "idx_right", "4"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a", "idx_right", "6"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardManyToOne, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "a", "idx_left", "1"),
				labels.FromStrings("group", "b", "idx_left", "3"),
				labels.FromStrings("group", "a", "idx_left", "2"),
				labels.FromStrings("group", "a", "idx_left", "3"),
				labels.FromStrings("group", "b", "idx_left", "1"),
				labels.FromStrings("group", "b", "idx_left", "2"),
			},
		},

		"multiple series on left side match to multiple series on right side with group_right": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a", "idx_left", "1"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b", "idx_left", "3"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a", "idx_left", "2"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "a", "idx_left", "3"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b", "idx_left", "1"),
				labels.FromStrings(model.MetricNameLabel, "left", "group", "b", "idx_left", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b", "idx_right", "4"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b", "idx_right", "5"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "b", "idx_right", "6"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a", "idx_right", "5"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a", "idx_right", "4"),
				labels.FromStrings(model.MetricNameLabel, "right", "group", "a", "idx_right", "6"),
			},

			op:       parser.ADD,
			matching: parser.VectorMatching{Card: parser.CardOneToMany, MatchingLabels: []string{"group"}, On: true},

			// Should be sorted to avoid buffering "many" side.
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "b", "idx_right", "4"),
				labels.FromStrings("group", "b", "idx_right", "5"),
				labels.FromStrings("group", "b", "idx_right", "6"),
				labels.FromStrings("group", "a", "idx_right", "5"),
				labels.FromStrings("group", "a", "idx_right", "4"),
				labels.FromStrings("group", "a", "idx_right", "6"),
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			left := &operators.TestOperator{Series: testCase.leftSeries, MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, MemoryConsumptionTracker: memoryConsumptionTracker}

			o, err := NewGroupedVectorVectorBinaryOperation(
				left,
				right,
				testCase.matching,
				testCase.op,
				testCase.returnBool,
				memoryConsumptionTracker,
				nil,
				posrange.PositionRange{},
				types.QueryTimeRange{},
			)

			require.NoError(t, err)

			outputSeries, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)
		})
	}
}

func TestGroupedVectorVectorBinaryOperation_FinalizesInnerOperatorsAsSoonAsPossible(t *testing.T) {
	testCases := map[string]struct {
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedOutputSeries                           []labels.Labels
		expectLeftSideFinalizedAfterOutputSeriesIndex  int
		expectRightSideFinalizedAfterOutputSeriesIndex int
	}{
		"no series on left": {
			leftSeries: []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
				labels.FromStrings("group", "3"),
			},

			expectedOutputSeries:                           []labels.Labels{},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  -1,
			expectRightSideFinalizedAfterOutputSeriesIndex: -1,
		},
		"no series on right": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
				labels.FromStrings("group", "3"),
			},
			rightSeries: []labels.Labels{},

			expectedOutputSeries:                           []labels.Labels{},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  -1,
			expectRightSideFinalizedAfterOutputSeriesIndex: -1,
		},
		"reach end of both sides at the same time": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  1,
			expectRightSideFinalizedAfterOutputSeriesIndex: 1,
		},
		"no more matches with unmatched series still to read on both sides": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "3"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  0,
			expectRightSideFinalizedAfterOutputSeriesIndex: 0,
		},
		"no more matches with unmatched series still to read on left side": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  0,
			expectRightSideFinalizedAfterOutputSeriesIndex: 0,
		},
		"no more matches with unmatched series still to read on right side": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "3"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  0,
			expectRightSideFinalizedAfterOutputSeriesIndex: 0,
		},
		"no matches": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "3"),
				labels.FromStrings("group", "4"),
				labels.FromStrings("group", "5"),
			},

			expectedOutputSeries:                           []labels.Labels{},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  -1,
			expectRightSideFinalizedAfterOutputSeriesIndex: -1,
		},
		"left side exhausted before right": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "3"),
				labels.FromStrings("group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
				labels.FromStrings("group", "3"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
				labels.FromStrings("group", "3"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  1,
			expectRightSideFinalizedAfterOutputSeriesIndex: 2,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			if testCase.expectLeftSideFinalizedAfterOutputSeriesIndex >= len(testCase.expectedOutputSeries) {
				require.Failf(t, "invalid test case", "expectLeftSideFinalizedAfterOutputSeriesIndex %v is beyond end of expected output series %v", testCase.expectLeftSideFinalizedAfterOutputSeriesIndex, testCase.expectedOutputSeries)
			}

			if testCase.expectRightSideFinalizedAfterOutputSeriesIndex >= len(testCase.expectedOutputSeries) {
				require.Failf(t, "invalid test case", "expectRightSideFinalizedAfterOutputSeriesIndex %v is beyond end of expected output series %v", testCase.expectRightSideFinalizedAfterOutputSeriesIndex, testCase.expectedOutputSeries)
			}

			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			vectorMatching := parser.VectorMatching{On: true, MatchingLabels: []string{"group"}, Card: parser.CardOneToMany}
			o, err := NewGroupedVectorVectorBinaryOperation(left, right, vectorMatching, parser.ADD, false, memoryConsumptionTracker, annotations.New(), posrange.PositionRange{}, timeRange)
			require.NoError(t, err)

			outputSeries, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			if len(testCase.expectedOutputSeries) == 0 {
				require.Empty(t, outputSeries)
			} else {
				require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)
			}

			if testCase.expectLeftSideFinalizedAfterOutputSeriesIndex == -1 {
				require.True(t, left.Finalized, "left side should be finalized after SeriesMetadata, but it is not")
			} else {
				require.False(t, left.Finalized, "left side should not be finalized after SeriesMetadata, but it is")
			}

			if testCase.expectRightSideFinalizedAfterOutputSeriesIndex == -1 {
				require.True(t, right.Finalized, "right side should be finalized after SeriesMetadata, but it is not")
			} else {
				require.False(t, right.Finalized, "right side should not be finalized after SeriesMetadata, but it is")
			}

			require.False(t, left.Closed, "left side should not be closed after SeriesMetadata, but it is")
			require.False(t, right.Closed, "right side should not be closed after SeriesMetadata, but it is")

			for outputSeriesIdx := range outputSeries {
				_, err := o.NextSeries(ctx)
				require.NoErrorf(t, err, "got error while reading series at index %v", outputSeriesIdx)

				if outputSeriesIdx >= testCase.expectLeftSideFinalizedAfterOutputSeriesIndex {
					require.Truef(t, left.Finalized, "left side should be finalized after output series at index %v, but it is not", outputSeriesIdx)
				} else {
					require.Falsef(t, left.Finalized, "left side should not be finalized after output series at index %v, but it is", outputSeriesIdx)
				}

				if outputSeriesIdx >= testCase.expectRightSideFinalizedAfterOutputSeriesIndex {
					require.Truef(t, right.Finalized, "right side should be finalized after output series at index %v, but it is not", outputSeriesIdx)
				} else {
					require.Falsef(t, right.Finalized, "right side should not be finalized after output series at index %v, but it is", outputSeriesIdx)
				}
			}

			require.False(t, left.Closed, "left side should not be closed after reading all output series, but it is")
			require.False(t, right.Closed, "right side should not be closed after reading all output series, but it is")

			types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)

			_, err = o.NextSeries(ctx)
			require.Equal(t, types.EOS, err)

			require.NoError(t, o.Finalize(ctx))
			require.True(t, left.Finalized, "left side should be finalized after calling Finalize, but it is not")
			require.True(t, right.Finalized, "right side should be finalized after calling Finalize, but it is not")
			// Make sure we've returned everything to their pools.
			require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

			o.Close()
			require.True(t, left.Closed, "left side should be closed after closing operator, but it isn't")
			require.True(t, right.Closed, "right side should be closed after closing operator, but it isn't")
		})
	}
}

func TestGroupedVectorVectorBinaryOperation_ReleasesIntermediateStateIfClosedEarly(t *testing.T) {
	testCases := map[string]struct {
		leftSeries       []labels.Labels
		rightSeries      []labels.Labels
		seriesToRead     int
		emptyInputSeries bool

		expectedOutputSeries []labels.Labels
	}{
		"closed after reading no series: multiple series from 'many' side match to a single 'one' series": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1"),
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "env", "prod"),
			},
			seriesToRead: 0,

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1", "env", "prod"),
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_2", "env", "prod"),
			},
		},
		"closed after reading no series: multiple series from 'one' side match to a single 'many' series": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "env", "prod"),
				labels.FromStrings("group", "1", "env", "test"),
			},
			seriesToRead: 0,

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1", "env", "prod"),
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1", "env", "test"),
			},
		},
		"closed after reading first series: multiple series from 'many' side match to a single 'one' series": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1"),
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "env", "prod"),
			},
			seriesToRead: 1,

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1", "env", "prod"),
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_2", "env", "prod"),
			},
		},
		"closed after reading first series: multiple series from 'one' side match to a single 'many' series": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "env", "prod"),
				labels.FromStrings("group", "1", "env", "test"),
			},
			seriesToRead: 1,

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1", "env", "prod"),
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1", "env", "test"),
			},
		},
		"closed after reading all 'one' side input series in a match group, but not all output series for that match group": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1"),
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "env", "prod"),
				labels.FromStrings("group", "1", "env", "test"),
			},
			seriesToRead:     2,
			emptyInputSeries: true, // Don't bother populating the input series with data: we run this test as an instant query, so if both 'one' side series have samples, they conflict with each other.

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1", "env", "prod"),
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1", "env", "test"),
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_2", "env", "prod"),
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_2", "env", "test"),
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			ts := int64(0)
			timeRange := types.NewInstantQueryTimeRange(timestamp.Time(ts))

			createTestData := func(val float64) types.InstantVectorSeriesData {
				if testCase.emptyInputSeries {
					return types.InstantVectorSeriesData{}
				}

				floats, err := types.FPointSlicePool.Get(1, memoryConsumptionTracker)
				require.NoError(t, err)
				floats = append(floats, promql.FPoint{T: ts, F: val})
				return types.InstantVectorSeriesData{Floats: floats}
			}

			leftData := make([]types.InstantVectorSeriesData, len(testCase.leftSeries))
			for i := range testCase.leftSeries {
				leftData[i] = createTestData(float64(i))
			}

			rightData := make([]types.InstantVectorSeriesData, len(testCase.rightSeries))
			for i := range testCase.rightSeries {
				rightData[i] = createTestData(float64(i))
			}

			left := &operators.TestOperator{Series: testCase.leftSeries, Data: leftData, MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, Data: rightData, MemoryConsumptionTracker: memoryConsumptionTracker}
			vectorMatching := parser.VectorMatching{On: true, MatchingLabels: []string{"group"}, Include: []string{"env"}, Card: parser.CardManyToOne}
			o, err := NewGroupedVectorVectorBinaryOperation(left, right, vectorMatching, parser.LTE, false, memoryConsumptionTracker, annotations.New(), posrange.PositionRange{}, timeRange)
			require.NoError(t, err)

			outputSeries, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)
			types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)

			for range testCase.seriesToRead {
				d, err := o.NextSeries(ctx)
				require.NoError(t, err)
				types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)
			}

			// Return any unread data to the pool and update the current memory consumption estimate to match.
			left.ReleaseUnreadData(memoryConsumptionTracker)
			right.ReleaseUnreadData(memoryConsumptionTracker)

			// Finalize the operator and verify that the intermediate state is released.
			require.NoError(t, o.Finalize(ctx))
			require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
			o.Close()
		})
	}
}
