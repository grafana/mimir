// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
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
				nil,
				log.NewNopLogger(),
			)

			require.NoError(t, err)

			outputSeries, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)
		})
	}
}

func TestGroupedVectorVectorBinaryOperation_CallsFinishedReadingOnInnerOperatorsAsSoonAsPossible(t *testing.T) {
	testCases := map[string]struct {
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedOutputSeries                                       []labels.Labels
		expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex  int
		expectRightSideFinishedReadingCalledAfterOutputSeriesIndex int
	}{
		"no series on left": {
			leftSeries: []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
				labels.FromStrings("group", "3"),
			},

			expectedOutputSeries: []labels.Labels{},
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  -1,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: -1,
		},
		"no series on right": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
				labels.FromStrings("group", "3"),
			},
			rightSeries: []labels.Labels{},

			expectedOutputSeries: []labels.Labels{},
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  -1,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: -1,
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
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  1,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: 1,
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
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  0,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: 0,
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
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  0,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: 0,
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
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  0,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: 0,
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

			expectedOutputSeries: []labels.Labels{},
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  -1,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: -1,
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
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  1,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: 2,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			if testCase.expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex >= len(testCase.expectedOutputSeries) {
				require.Failf(t, "invalid test case", "expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex %v is beyond end of expected output series %v", testCase.expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex, testCase.expectedOutputSeries)
			}

			if testCase.expectRightSideFinishedReadingCalledAfterOutputSeriesIndex >= len(testCase.expectedOutputSeries) {
				require.Failf(t, "invalid test case", "expectRightSideFinishedReadingCalledAfterOutputSeriesIndex %v is beyond end of expected output series %v", testCase.expectRightSideFinishedReadingCalledAfterOutputSeriesIndex, testCase.expectedOutputSeries)
			}

			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			vectorMatching := parser.VectorMatching{On: true, MatchingLabels: []string{"group"}, Card: parser.CardOneToMany}
			o, err := NewGroupedVectorVectorBinaryOperation(left, right, vectorMatching, parser.ADD, false, memoryConsumptionTracker, annotations.New(), posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
			require.NoError(t, err)

			outputSeries, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			if len(testCase.expectedOutputSeries) == 0 {
				require.Empty(t, outputSeries)
			} else {
				require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)
			}

			if testCase.expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex == -1 {
				require.True(t, left.FinishedReadingCalled, "left side should have FinishedReading called after SeriesMetadata, but it is not")
			} else {
				require.False(t, left.FinishedReadingCalled, "left side should not have FinishedReading called after SeriesMetadata, but it is")
			}

			if testCase.expectRightSideFinishedReadingCalledAfterOutputSeriesIndex == -1 {
				require.True(t, right.FinishedReadingCalled, "right side should have FinishedReading called after SeriesMetadata, but it is not")
			} else {
				require.False(t, right.FinishedReadingCalled, "right side should not have FinishedReading called after SeriesMetadata, but it is")
			}

			require.False(t, left.Closed, "left side should not be closed after SeriesMetadata, but it is")
			require.False(t, right.Closed, "right side should not be closed after SeriesMetadata, but it is")

			for outputSeriesIdx := range outputSeries {
				_, err := o.NextSeries(ctx)
				require.NoErrorf(t, err, "got error while reading series at index %v", outputSeriesIdx)

				if outputSeriesIdx >= testCase.expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex {
					require.Truef(t, left.FinishedReadingCalled, "left side should have FinishedReading called after output series at index %v, but it is not", outputSeriesIdx)
				} else {
					require.Falsef(t, left.FinishedReadingCalled, "left side should not have FinishedReading called after output series at index %v, but it is", outputSeriesIdx)
				}

				if outputSeriesIdx >= testCase.expectRightSideFinishedReadingCalledAfterOutputSeriesIndex {
					require.Truef(t, right.FinishedReadingCalled, "right side should have FinishedReading called after output series at index %v, but it is not", outputSeriesIdx)
				} else {
					require.Falsef(t, right.FinishedReadingCalled, "right side should not have FinishedReading called after output series at index %v, but it is", outputSeriesIdx)
				}
			}

			require.False(t, left.Closed, "left side should not be closed after reading all output series, but it is")
			require.False(t, right.Closed, "right side should not be closed after reading all output series, but it is")

			types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)

			_, err = o.NextSeries(ctx)
			require.Equal(t, types.EOS, err)

			require.NoError(t, o.FinishedReading(ctx))
			require.True(t, left.FinishedReadingCalled, "left side should have FinishedReading called after calling FinishedReading, but it is not")
			require.True(t, right.FinishedReadingCalled, "right side should have FinishedReading called after calling FinishedReading, but it is not")
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
			o, err := NewGroupedVectorVectorBinaryOperation(left, right, vectorMatching, parser.LTE, false, memoryConsumptionTracker, annotations.New(), posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
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

			// Call FinishedReading on the operator and verify that the intermediate state is released.
			require.NoError(t, o.FinishedReading(ctx))
			require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
			o.Close()
		})
	}
}

func TestGroupedVectorVectorBinaryOperation_HintsPassedToManySide(t *testing.T) {
	testCases := map[string]struct {
		card          parser.VectorMatchCardinality
		includeLabels []string // VectorMatching.Include: extra labels sourced from the many side
		leftSeries    []labels.Labels
		rightSeries   []labels.Labels
		hints         *Hints
		outerMatchers types.Matchers

		expectedLeftMatchers  types.Matchers
		expectedRightMatchers types.Matchers
	}{
		"group_left with hints: left (many) side receives hint-built matchers": {
			card: parser.CardManyToOne,
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "pod", "1"),
				labels.FromStrings("env", "staging", "pod", "1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
				labels.FromStrings("env", "staging"),
			},
			hints:         &Hints{Include: []string{"env"}},
			outerMatchers: nil,
			// one side (right) gets outer matchers
			expectedRightMatchers: nil,
			// many side (left) gets hint-built matchers derived from right (one) series
			expectedLeftMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod|staging"},
			},
		},
		"group_right with hints: right (many) side receives hint-built matchers": {
			card: parser.CardOneToMany,
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "pod", "1"),
				labels.FromStrings("env", "prod", "pod", "2"),
			},
			hints:         &Hints{Include: []string{"env"}},
			outerMatchers: nil,
			// one side (left) gets outer matchers
			expectedLeftMatchers: nil,
			// many side (right) gets hint-built matchers derived from left (one) series
			expectedRightMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
		},
		"group_left without hints: left (many) side receives the same outer matchers as one side": {
			card: parser.CardManyToOne,
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "pod", "1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
			hints:         nil,
			outerMatchers: nil,
			// both sides get outer matchers (nil)
			expectedLeftMatchers:  nil,
			expectedRightMatchers: nil,
		},
		"group_right without hints: right (many) side receives the same outer matchers as one side": {
			card: parser.CardOneToMany,
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "pod", "1"),
			},
			hints:         nil,
			outerMatchers: nil,
			// both sides get outer matchers (nil)
			expectedLeftMatchers:  nil,
			expectedRightMatchers: nil,
		},

		// The following cases cover a bug where outer matchers for VectorMatching.Include
		// labels (which come from the many side) were incorrectly forwarded to the one side,
		// and were discarded instead of being passed to the many side when hints were set.

		"group_left with hints and include-label outer matchers: include-label matchers stripped from one side and merged onto many side": {
			card:          parser.CardManyToOne,
			includeLabels: []string{"region"}, // region comes from the many (left) side
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us"),
				labels.FromStrings("env", "prod", "region", "eu"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
			hints: &Hints{Include: []string{"env"}},
			outerMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
			// one side (right) must not receive the region matcher: region comes from many side
			expectedRightMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
			// many side (left) gets hint-built env matcher merged with the region matcher
			expectedLeftMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
		},
		"group_right with hints and include-label outer matchers: include-label matchers stripped from one side and merged onto many side": {
			card:          parser.CardOneToMany,
			includeLabels: []string{"region"}, // region comes from the many (right) side
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us"),
				labels.FromStrings("env", "prod", "region", "eu"),
			},
			hints: &Hints{Include: []string{"env"}},
			outerMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
			// one side (left) must not receive the region matcher: region comes from many side
			expectedLeftMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
			// many side (right) gets hint-built env matcher merged with the region matcher
			expectedRightMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
		},
		"group_left without hints and include-label outer matchers: include-label matchers still stripped from one side": {
			card:          parser.CardManyToOne,
			includeLabels: []string{"region"},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
			hints: nil,
			outerMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
			// one side (right) must not receive the region matcher
			expectedRightMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
			// many side (left) gets all outer matchers unchanged
			expectedLeftMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}

			o, err := NewGroupedVectorVectorBinaryOperation(
				left,
				right,
				parser.VectorMatching{Card: testCase.card, MatchingLabels: []string{"env"}, On: true, Include: testCase.includeLabels},
				parser.ADD,
				false,
				memoryConsumptionTracker,
				nil,
				posrange.PositionRange{},
				types.QueryTimeRange{},
				testCase.hints,
				log.NewNopLogger(),
			)
			require.NoError(t, err)

			_, err = o.SeriesMetadata(ctx, testCase.outerMatchers)
			require.NoError(t, err)

			require.Equal(t, testCase.expectedLeftMatchers, left.MatchersProvided, "left side received unexpected matchers")
			require.Equal(t, testCase.expectedRightMatchers, right.MatchersProvided, "right side received unexpected matchers")
		})
	}
}

func TestGroupedVectorVectorBinaryOperation_PassesWithoutDerivedMatchersToManySide(t *testing.T) {
	// Verifies that exclude-style matchers are forwarded to the many side via explicit
	// exclude hints (set by an up-to-date query-frontend). When hints are nil (old
	// query-frontend plans), no matchers are generated to avoid incorrect filtering
	// of labels synthesized by label_replace/label_join.
	testCases := map[string]struct {
		card           parser.VectorMatchCardinality
		vectorMatching parser.VectorMatching
		hints          *Hints
		leftSeries     []labels.Labels
		rightSeries    []labels.Labels

		expectedLeftMatchers  types.Matchers
		expectedRightMatchers types.Matchers
	}{
		"group_left with exclude hints: left (many) side receives exclude-derived matchers from right (one) side": {
			card:           parser.CardManyToOne,
			vectorMatching: parser.VectorMatching{Card: parser.CardManyToOne, On: false, MatchingLabels: []string{}},
			hints:          &Hints{Exclude: []string{}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "pod", "1"),
				labels.FromStrings("env", "staging", "pod", "2"), // should be filtered by env hint
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
			// one side (right) gets nil outer matchers
			expectedRightMatchers: nil,
			// many side (left) gets exclude-derived matchers built from right (one) metadata
			expectedLeftMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
		},
		"group_right with exclude hints: right (many) side receives exclude-derived matchers from left (one) side": {
			card:           parser.CardOneToMany,
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToMany, On: false, MatchingLabels: []string{}},
			hints:          &Hints{Exclude: []string{}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "pod", "1"),
				labels.FromStrings("env", "staging", "pod", "2"), // should be filtered by env hint
			},
			// one side (left) gets nil outer matchers
			expectedLeftMatchers: nil,
			// many side (right) gets exclude-derived matchers built from left (one) metadata
			expectedRightMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
		},
		"group_left with exclude hints and ignoring label: excluded label does not appear in matchers": {
			card:           parser.CardManyToOne,
			vectorMatching: parser.VectorMatching{Card: parser.CardManyToOne, On: false, MatchingLabels: []string{"pod"}},
			hints:          &Hints{Exclude: []string{"pod"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "pod", "1"),
				labels.FromStrings("env", "prod", "pod", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
			expectedRightMatchers: nil,
			// pod is excluded; only env matcher is generated
			expectedLeftMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
		},
		"group_left with exclude hints and non-empty Include: Include label from many side does not appear in exclude-derived matchers": {
			// region is a VectorMatching.Include label: it comes from the many (left) side, not the one (right) side.
			// buildMatchersForWithout runs on the one-side metadata, which does not carry "region",
			// so "region" must not appear in the generated matchers even though the many side has it.
			card:           parser.CardManyToOne,
			vectorMatching: parser.VectorMatching{Card: parser.CardManyToOne, On: false, MatchingLabels: []string{}, Include: []string{"region"}},
			hints:          &Hints{Exclude: []string{}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us"),
				labels.FromStrings("env", "prod", "region", "eu"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod"), // one side: does NOT carry "region"
			},
			expectedRightMatchers: nil,
			// many side gets only env matcher (derived from one-side metadata); no region matcher
			// since region is absent from the one side
			expectedLeftMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
		},
		"group_left with exclude hints excluding multiple labels: only non-excluded labels produce matchers": {
			card:           parser.CardManyToOne,
			vectorMatching: parser.VectorMatching{Card: parser.CardManyToOne, On: false, MatchingLabels: []string{"pod", "container"}},
			hints:          &Hints{Exclude: []string{"container", "pod"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "pod", "1", "container", "web"),
				labels.FromStrings("env", "prod", "pod", "2", "container", "api"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
			expectedRightMatchers: nil,
			// pod and container are excluded; only env matcher is generated
			expectedLeftMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
		},
		"group_right with exclude hints and heterogeneous one-side labels: absent label matched with empty string": {
			card:           parser.CardOneToMany,
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToMany, On: false, MatchingLabels: []string{}},
			hints:          &Hints{Exclude: []string{}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
				labels.FromStrings("env", "prod"), // no region label
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "pod", "1"),
				labels.FromStrings("env", "staging", "pod", "2"), // should be filtered by env matcher
			},
			// region is absent from one LHS series, so the matcher includes the empty
			// string to also match RHS series without a region label.
			expectedLeftMatchers: nil,
			expectedRightMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchRegexp, Name: "region", Value: "|us-east"},
			},
		},
		"group_left with exclude hints and include-label outer matchers: include-label matchers merged onto many side with exclude-derived matchers": {
			card:           parser.CardManyToOne,
			vectorMatching: parser.VectorMatching{Card: parser.CardManyToOne, On: false, MatchingLabels: []string{}, Include: []string{"region"}},
			hints:          &Hints{Exclude: []string{}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us"),
				labels.FromStrings("env", "prod", "region", "eu"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod"), // one side: does NOT carry "region"
			},
			expectedRightMatchers: nil,
			// many side gets exclude-derived env matcher from one-side metadata merged with include-label outer matchers
			expectedLeftMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
		},
		"group_left nil hints (!On): left (many) side receives nil matchers (no fallback)": {
			card:           parser.CardManyToOne,
			vectorMatching: parser.VectorMatching{Card: parser.CardManyToOne, On: false, MatchingLabels: []string{}},
			hints:          nil,
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "pod", "1"),
				labels.FromStrings("env", "staging", "pod", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
			expectedRightMatchers: nil,
			expectedLeftMatchers:  nil,
		},
		"group_right nil hints (!On): right (many) side receives nil matchers (no fallback)": {
			card:           parser.CardOneToMany,
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToMany, On: false, MatchingLabels: []string{}},
			hints:          nil,
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "pod", "1"),
				labels.FromStrings("env", "staging", "pod", "2"),
			},
			expectedLeftMatchers:  nil,
			expectedRightMatchers: nil,
		},
		"group_left on matching with include hints: many side receives include-derived matchers": {
			card:           parser.CardManyToOne,
			vectorMatching: parser.VectorMatching{Card: parser.CardManyToOne, On: true, MatchingLabels: []string{"env"}, Include: []string{"region"}},
			hints:          &Hints{Include: []string{"env"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us"),
				labels.FromStrings("env", "prod", "region", "eu"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
				labels.FromStrings("env", "staging"), // filtered by env hint
			},
			// one side (right) gets nil outer matchers
			expectedRightMatchers: nil,
			// many side (left) gets include-derived matcher for env from one-side (right) metadata
			expectedLeftMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod|staging"},
			},
		},
		"group_right on matching with include hints: many side receives include-derived matchers": {
			card:           parser.CardOneToMany,
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToMany, On: true, MatchingLabels: []string{"env"}, Include: []string{"region"}},
			hints:          &Hints{Include: []string{"env"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
				labels.FromStrings("env", "staging"), // filtered by env hint
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us"),
				labels.FromStrings("env", "prod", "region", "eu"),
			},
			// one side (left) gets nil matchers
			expectedLeftMatchers: nil,
			// many side (right) gets include-derived matcher for env from one-side (left) metadata
			expectedRightMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod|staging"},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}

			o, err := NewGroupedVectorVectorBinaryOperation(
				left,
				right,
				testCase.vectorMatching,
				parser.ADD,
				false,
				memoryConsumptionTracker,
				nil,
				posrange.PositionRange{},
				types.QueryTimeRange{},
				testCase.hints,
				log.NewNopLogger(),
			)
			require.NoError(t, err)

			_, err = o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			require.Equal(t, testCase.expectedLeftMatchers, left.MatchersProvided, "left side received unexpected matchers")
			require.Equal(t, testCase.expectedRightMatchers, right.MatchersProvided, "right side received unexpected matchers")
		})
	}
}

func TestGroupedVectorVectorBinaryOperation_ManySideMatchersWhenHintsProduceNoMatchers(t *testing.T) {
	ctx := context.Background()

	t.Run("non-include-label parent matchers are dropped", func(t *testing.T) {
		// When hints are non-nil but BuildMatchers returns nil (e.g., all labels are excluded),
		// parent matchers for non-include labels must still be dropped from the many side.
		// Parent matchers may refer to labels that don't exist on the many side of this
		// binary operation.
		memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

		// group_left: right is "one" side, left is "many" side.
		// The one side (right) has "cluster" so parent matchers don't filter it out.
		rightSeries := []labels.Labels{
			labels.FromStrings("env", "prod", "cluster", "us-east"),
		}
		leftSeries := []labels.Labels{
			labels.FromStrings("env", "prod", "pod", "1"),
		}

		left := &operators.TestOperator{Series: leftSeries, Data: make([]types.InstantVectorSeriesData, len(leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
		right := &operators.TestOperator{Series: rightSeries, Data: make([]types.InstantVectorSeriesData, len(rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}

		// Exclude hints that exclude all one-side labels: BuildMatchers will return nil
		// because all label names present on the one side are excluded.
		hints := &Hints{Exclude: []string{"cluster", "env"}}
		vectorMatching := parser.VectorMatching{Card: parser.CardManyToOne, On: false, MatchingLabels: []string{"cluster", "env"}}

		o, err := NewGroupedVectorVectorBinaryOperation(
			left,
			right,
			vectorMatching,
			parser.ADD,
			false,
			memoryConsumptionTracker,
			nil,
			posrange.PositionRange{},
			types.QueryTimeRange{},
			hints,
			log.NewNopLogger(),
		)
		require.NoError(t, err)

		// Pass non-nil parent matchers that refer to a label ("cluster") not present on the many side.
		parentMatchers := types.Matchers{
			{Type: labels.MatchRegexp, Name: "cluster", Value: "us-east"},
		}
		_, err = o.SeriesMetadata(ctx, parentMatchers)
		require.NoError(t, err)

		// Parent matchers must be dropped from the many (left) side when hints are set but produce no matchers.
		require.Nil(t, left.MatchersProvided, "parent matchers should be dropped from many side when hints are set but produce no matchers")
	})

	t.Run("include-label parent matchers are still forwarded to many side", func(t *testing.T) {
		// When hints are non-nil but BuildMatchers returns nil, parent matchers for
		// included labels (from group_left/group_right) should still be forwarded to
		// the many side, since those labels belong to the many side.
		memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

		// group_left(region): right is "one" side, left is "many" side.
		// "region" is an include label that comes from the many (left) side.
		rightSeries := []labels.Labels{
			labels.FromStrings("env", "prod"),
		}
		leftSeries := []labels.Labels{
			labels.FromStrings("env", "prod", "region", "us-east"),
		}

		left := &operators.TestOperator{Series: leftSeries, Data: make([]types.InstantVectorSeriesData, len(leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
		right := &operators.TestOperator{Series: rightSeries, Data: make([]types.InstantVectorSeriesData, len(rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}

		// Exclude hints that exclude all one-side labels: BuildMatchers will return nil.
		hints := &Hints{Exclude: []string{"env"}}
		vectorMatching := parser.VectorMatching{Card: parser.CardManyToOne, On: false, MatchingLabels: []string{"env"}, Include: []string{"region"}}

		o, err := NewGroupedVectorVectorBinaryOperation(
			left,
			right,
			vectorMatching,
			parser.ADD,
			false,
			memoryConsumptionTracker,
			nil,
			posrange.PositionRange{},
			types.QueryTimeRange{},
			hints,
			log.NewNopLogger(),
		)
		require.NoError(t, err)

		// Pass parent matchers that include one for the "region" include label and one
		// for "cluster" which is unrelated.
		parentMatchers := types.Matchers{
			{Type: labels.MatchEqual, Name: "cluster", Value: "us-east"},
			{Type: labels.MatchEqual, Name: "region", Value: "us-east"},
		}
		_, err = o.SeriesMetadata(ctx, parentMatchers)
		require.NoError(t, err)

		// The many (left) side should only receive the include-label matcher ("region"),
		// not the non-include-label matcher ("cluster").
		expectedManySideMatchers := types.Matchers{
			{Type: labels.MatchEqual, Name: "region", Value: "us-east"},
		}
		require.Equal(t, expectedManySideMatchers, left.MatchersProvided, "many side should receive only include-label matchers when hints produce no matchers")

		// The one (right) side should receive only the non-include-label matcher ("cluster"),
		// since "region" belongs to the many side.
		expectedOneSideMatchers := types.Matchers{
			{Type: labels.MatchEqual, Name: "cluster", Value: "us-east"},
		}
		require.Equal(t, expectedOneSideMatchers, right.MatchersProvided, "one side should not receive include-label matchers")
	})
}

// BenchmarkGroupedVectorVectorBinaryOperation_HintsSideFiltering measures the benefit of
// the hints-based optimization introduced for GroupedVectorVectorBinaryOperation.
//
// The scenario has:
//   - a small "one" side covering oneSideEnvs distinct env values
//   - a large "many" side covering manySideEnvsTotal distinct env values (most having no match)
//
// Without hints the many-side operator returns all series and computeOutputSeries discards the
// non-matching ones. With hints the one side's env values are used to build a matcher that is
// passed to the many-side operator before it returns any series, so the many side only
// materialises the fraction of series that can actually contribute to the output.
//
// Both group_left (many-to-one) and group_right (one-to-many) are benchmarked.
//
// Custom metrics reported:
//   - one-series/op: series fetched from the one side per operation
//   - many-series/op: series fetched from the many side per operation
//   - total-series/op: sum of both sides per operation
//
// Run with:
//
//	go test ./pkg/streamingpromql/operators/binops/ -run=^$ -bench=BenchmarkGroupedVectorVectorBinaryOperation_HintsSideFiltering -benchmem
func BenchmarkGroupedVectorVectorBinaryOperation_HintsSideFiltering(b *testing.B) {
	const (
		oneSideEnvs          = 10
		manySideEnvsTotal    = 100 // 90 % of the many-side envs have no one-side match
		manySideSeriesPerEnv = 10
	)

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	hints := &Hints{Include: []string{"env"}}

	// One side: env-0 … env-9 (the smaller, "one" side).
	oneSeries := make([]labels.Labels, oneSideEnvs)
	for i := range oneSideEnvs {
		oneSeries[i] = labels.FromStrings("env", fmt.Sprintf("env-%d", i))
	}

	// Many side: env-0 … env-99, each with manySideSeriesPerEnv distinct pods.
	// Only env-0 … env-9 will match the one side.
	allManySeries := make([]labels.Labels, 0, manySideEnvsTotal*manySideSeriesPerEnv)
	for e := range manySideEnvsTotal {
		for p := range manySideSeriesPerEnv {
			allManySeries = append(allManySeries, labels.FromStrings(
				"env", fmt.Sprintf("env-%d", e),
				"pod", fmt.Sprintf("pod-%d", p),
			))
		}
	}

	run := func(b *testing.B, card parser.VectorMatchCardinality, h *Hints) {
		b.Helper()
		b.ReportAllocs()

		var totalOneSeries, totalManySeries int

		for b.Loop() {
			// Fresh operators are required each iteration because TestOperator mutates its
			// Series slice in-place when hint-based matchers are applied to it.
			memTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

			// For CardManyToOne (group_left): left=many, right=one.
			// For CardOneToMany (group_right): left=one, right=many.
			var leftSeries, rightSeries []labels.Labels
			if card == parser.CardManyToOne {
				leftSeries = slices.Clone(allManySeries)
				rightSeries = slices.Clone(oneSeries)
			} else {
				leftSeries = slices.Clone(oneSeries)
				rightSeries = slices.Clone(allManySeries)
			}

			left := &operators.TestOperator{
				Series:                   leftSeries,
				Data:                     make([]types.InstantVectorSeriesData, len(leftSeries)),
				MemoryConsumptionTracker: memTracker,
			}
			right := &operators.TestOperator{
				Series:                   rightSeries,
				Data:                     make([]types.InstantVectorSeriesData, len(rightSeries)),
				MemoryConsumptionTracker: memTracker,
			}

			vectorMatching := parser.VectorMatching{Card: card, MatchingLabels: []string{"env"}, On: true}
			op, err := NewGroupedVectorVectorBinaryOperation(
				left, right, vectorMatching, parser.MUL, false,
				memTracker, annotations.New(), posrange.PositionRange{}, timeRange, h, log.NewNopLogger(),
			)
			if err != nil {
				b.Fatal(err)
			}

			if _, err = op.SeriesMetadata(ctx, nil); err != nil {
				b.Fatal(err)
			}

			// Capture series counts after SeriesMetadata has applied any hint-based filtering.
			// TestOperator retains only the series that passed the matcher filter in t.Series.
			if card == parser.CardManyToOne {
				totalManySeries += len(left.Series)
				totalOneSeries += len(right.Series)
			} else {
				totalOneSeries += len(left.Series)
				totalManySeries += len(right.Series)
			}

			if err = op.FinishedReading(ctx); err != nil {
				b.Fatal(err)
			}
			op.Close()
		}

		b.ReportMetric(float64(totalOneSeries)/float64(b.N), "one-series/op")
		b.ReportMetric(float64(totalManySeries)/float64(b.N), "many-series/op")
		b.ReportMetric(float64(totalOneSeries+totalManySeries)/float64(b.N), "total-series/op")
	}

	b.Run("group_left/with_hints", func(b *testing.B) { run(b, parser.CardManyToOne, hints) })
	b.Run("group_left/without_hints", func(b *testing.B) { run(b, parser.CardManyToOne, nil) })
	b.Run("group_right/with_hints", func(b *testing.B) { run(b, parser.CardOneToMany, hints) })
	b.Run("group_right/without_hints", func(b *testing.B) { run(b, parser.CardOneToMany, nil) })
}
