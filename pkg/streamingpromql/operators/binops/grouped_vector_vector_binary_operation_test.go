// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type finishedReadingCountingOperator struct {
	*operators.TestOperator
	finishedReadingCalls int
}

func (o *finishedReadingCountingOperator) FinishedReading(ctx context.Context) error {
	o.finishedReadingCalls++
	return o.TestOperator.FinishedReading(ctx)
}

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
			o, err := NewGroupedVectorVectorBinaryOperation(left, right, vectorMatching, parser.ADD, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
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
			o, err := NewGroupedVectorVectorBinaryOperation(left, right, vectorMatching, parser.LTE, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
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
		card           parser.VectorMatchCardinality
		matchingLabels []string // VectorMatching.MatchingLabels: labels the sides join on (with On).
		on             bool     // VectorMatching.On: "on(...)" (true) vs "ignoring(...)"/default (false).
		includeLabels  []string // VectorMatching.Include: extra labels sourced from the many side.
		leftSeries     []labels.Labels
		rightSeries    []labels.Labels
		hints          *Hints
		outerMatchers  types.Matchers

		expectedLeftMatchers  types.Matchers
		expectedRightMatchers types.Matchers
	}{
		"group_left with hints: left (many) side receives hint-built matchers": {
			card:           parser.CardManyToOne,
			matchingLabels: []string{"env"},
			on:             true,
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
			card:           parser.CardOneToMany,
			matchingLabels: []string{"env"},
			on:             true,
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
			card:           parser.CardManyToOne,
			matchingLabels: []string{"env"},
			on:             true,
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
			card:           parser.CardOneToMany,
			matchingLabels: []string{"env"},
			on:             true,
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
			card:           parser.CardManyToOne,
			matchingLabels: []string{"env"},
			on:             true,
			includeLabels:  []string{"region"}, // region comes from the many (left) side
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
			card:           parser.CardOneToMany,
			matchingLabels: []string{"env"},
			on:             true,
			includeLabels:  []string{"region"}, // region comes from the many (right) side
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
			card:           parser.CardManyToOne,
			matchingLabels: []string{"env"},
			on:             true,
			includeLabels:  []string{"region"},
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

		// Regression: outer matchers for labels the one side does not join on (on() with no
		// matching labels, or on(...) matchers outside the matching set) must not reach the
		// one-side selector, else it is filtered to empty.
		// Shape: "prediction + prediction * on() group_left threshold_margin".

		"group_left with on() (empty matching labels): non-matching outer matchers routed to many side, one side receives none": {
			card:           parser.CardManyToOne,
			matchingLabels: []string{}, // on(): one side joins on nothing.
			on:             true,
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "service", "checkout"),
				labels.FromStrings("env", "prod", "service", "cart"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod"), // one side: no "service" label.
			},
			hints: nil,
			outerMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "service", Value: "cart|checkout"},
				{Type: labels.MatchRegexp, Name: "job", Value: "asserts/latency"},
			},
			// one side (right) joins on nothing, so it must receive no outer matchers.
			expectedRightMatchers: nil,
			// many side (left) still gets the outer matchers to narrow it.
			expectedLeftMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "service", Value: "cart|checkout"},
				{Type: labels.MatchRegexp, Name: "job", Value: "asserts/latency"},
			},
		},
		"group_right with on() (empty matching labels): non-matching outer matchers routed to many side, one side receives none": {
			card:           parser.CardOneToMany,
			matchingLabels: []string{}, // on(): one side joins on nothing.
			on:             true,
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod"), // one side: no "service" label.
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "service", "checkout"),
				labels.FromStrings("env", "prod", "service", "cart"),
			},
			hints: nil,
			outerMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "service", Value: "cart|checkout"},
				{Type: labels.MatchRegexp, Name: "job", Value: "asserts/latency"},
			},
			// one side (left) joins on nothing, so it must receive no outer matchers.
			expectedLeftMatchers: nil,
			// many side (right) still gets the outer matchers to narrow it.
			expectedRightMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "service", Value: "cart|checkout"},
				{Type: labels.MatchRegexp, Name: "job", Value: "asserts/latency"},
			},
		},
		"group_left with on(env) and mixed outer matchers: matching-label matcher kept for one side, non-matching routed to many side": {
			card:           parser.CardManyToOne,
			matchingLabels: []string{"env"}, // join on "env"; "service" is not a matching label.
			on:             true,
			hints:          &Hints{Include: []string{"env"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "service", "checkout"),
				labels.FromStrings("env", "prod", "service", "cart"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod"), // one side: has "env", no "service".
			},
			outerMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "env", Value: "prod"},
				{Type: labels.MatchRegexp, Name: "service", Value: "cart|checkout"},
			},
			// one side (right) gets only the matching-label ("env") matcher; "service" is routed away.
			expectedRightMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "env", Value: "prod"},
			},
			// many side (left) gets the hint-built env matcher merged with the routed "service" matcher.
			expectedLeftMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchRegexp, Name: "service", Value: "cart|checkout"},
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
				parser.VectorMatching{Card: testCase.card, MatchingLabels: testCase.matchingLabels, On: testCase.on, Include: testCase.includeLabels},
				parser.ADD,
				false,
				memoryConsumptionTracker,
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

func TestGroupedVectorVectorBinaryOperation_IgnoresMatchersWithFill(t *testing.T) {
	testCases := map[string]struct {
		card     parser.VectorMatchCardinality
		fillLeft bool
	}{
		"group_left fill_left": {
			card:     parser.CardManyToOne,
			fillLeft: true,
		},
		"group_left fill_right": {
			card: parser.CardManyToOne,
		},
		"group_right fill_left": {
			card:     parser.CardOneToMany,
			fillLeft: true,
		},
		"group_right fill_right": {
			card: parser.CardOneToMany,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			left := &operators.TestOperator{
				Series:                   []labels.Labels{labels.FromStrings("env", "prod")},
				Data:                     make([]types.InstantVectorSeriesData, 1),
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}
			right := &operators.TestOperator{
				Series:                   []labels.Labels{labels.FromStrings("env", "prod")},
				Data:                     make([]types.InstantVectorSeriesData, 1),
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}
			fillValue := 0.0
			fillValues := parser.VectorMatchFillValues{RHS: &fillValue}
			if testCase.fillLeft {
				fillValues = parser.VectorMatchFillValues{LHS: &fillValue}
			}

			o, err := NewGroupedVectorVectorBinaryOperation(
				left,
				right,
				parser.VectorMatching{
					Card:           testCase.card,
					On:             true,
					MatchingLabels: []string{"env"},
					Include:        []string{"region"},
					FillValues:     fillValues,
				},
				parser.ADD,
				false,
				memoryConsumptionTracker,
				posrange.PositionRange{},
				types.QueryTimeRange{},
				&Hints{Include: []string{"env"}},
				log.NewNopLogger(),
			)
			require.NoError(t, err)

			outerMatchers := types.Matchers{
				{Type: labels.MatchEqual, Name: "env", Value: "prod"},
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
				{Type: labels.MatchEqual, Name: "service", Value: "api"},
			}
			_, err = o.SeriesMetadata(ctx, outerMatchers)
			require.NoError(t, err)
			require.Nil(t, left.MatchersProvided)
			require.Nil(t, right.MatchersProvided)
		})
	}
}

func TestGroupedVectorVectorBinaryOperation_NormalizesSidesAndEvaluatorFillValues(t *testing.T) {
	lhsFill := 11.0
	rhsFill := 22.0
	testCases := map[string]struct {
		card parser.VectorMatchCardinality
		fill parser.VectorMatchFillValues

		leftIsMany     bool
		normalizedMany *float64
		normalizedOne  *float64
		evaluatorLeft  *float64
		evaluatorRight *float64
	}{
		"many-to-one without fill": {
			card:       parser.CardManyToOne,
			leftIsMany: true,
		},
		"many-to-one with LHS fill": {
			card:           parser.CardManyToOne,
			fill:           parser.VectorMatchFillValues{LHS: &lhsFill},
			leftIsMany:     true,
			normalizedMany: &lhsFill,
			evaluatorLeft:  &lhsFill,
		},
		"many-to-one with RHS fill": {
			card:           parser.CardManyToOne,
			fill:           parser.VectorMatchFillValues{RHS: &rhsFill},
			leftIsMany:     true,
			normalizedOne:  &rhsFill,
			evaluatorRight: &rhsFill,
		},
		"many-to-one with both fills": {
			card:           parser.CardManyToOne,
			fill:           parser.VectorMatchFillValues{LHS: &lhsFill, RHS: &rhsFill},
			leftIsMany:     true,
			normalizedMany: &lhsFill,
			normalizedOne:  &rhsFill,
			evaluatorLeft:  &lhsFill,
			evaluatorRight: &rhsFill,
		},
		"one-to-many without fill": {
			card: parser.CardOneToMany,
		},
		"one-to-many with LHS fill": {
			card:           parser.CardOneToMany,
			fill:           parser.VectorMatchFillValues{LHS: &lhsFill},
			normalizedMany: &lhsFill,
			evaluatorRight: &lhsFill,
		},
		"one-to-many with RHS fill": {
			card:          parser.CardOneToMany,
			fill:          parser.VectorMatchFillValues{RHS: &rhsFill},
			normalizedOne: &rhsFill,
			evaluatorLeft: &rhsFill,
		},
		"one-to-many with both fills": {
			card:           parser.CardOneToMany,
			fill:           parser.VectorMatchFillValues{LHS: &lhsFill, RHS: &rhsFill},
			normalizedMany: &lhsFill,
			normalizedOne:  &rhsFill,
			evaluatorLeft:  &rhsFill,
			evaluatorRight: &lhsFill,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
			left := &operators.TestOperator{MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{MemoryConsumptionTracker: memoryConsumptionTracker}

			o, err := NewGroupedVectorVectorBinaryOperation(
				left,
				right,
				parser.VectorMatching{Card: testCase.card, FillValues: testCase.fill},
				parser.ADD,
				false,
				memoryConsumptionTracker,
				posrange.PositionRange{},
				types.QueryTimeRange{},
				nil,
				log.NewNopLogger(),
			)
			require.NoError(t, err)

			if testCase.leftIsMany {
				require.Same(t, left, o.manySide)
				require.Same(t, right, o.oneSide)
			} else {
				require.Same(t, right, o.manySide)
				require.Same(t, left, o.oneSide)
			}
			require.Same(t, testCase.normalizedMany, o.fillValues.LHS)
			require.Same(t, testCase.normalizedOne, o.fillValues.RHS)
			require.Same(t, testCase.evaluatorLeft, o.evaluator.fillLeft)
			require.Same(t, testCase.evaluatorRight, o.evaluator.fillRight)
		})
	}
}

func TestGroupedVectorVectorBinaryOperation_EmptySideMetadataWithFill(t *testing.T) {
	fillValue := 1.0
	fillCases := map[string]struct {
		card      parser.VectorMatchCardinality
		fill      parser.VectorMatchFillValues
		fillsMany bool
		fillsOne  bool
	}{
		"many-to-one without fill": {
			card: parser.CardManyToOne,
		},
		"many-to-one with LHS fill": {
			card:      parser.CardManyToOne,
			fill:      parser.VectorMatchFillValues{LHS: &fillValue},
			fillsMany: true,
		},
		"many-to-one with RHS fill": {
			card:     parser.CardManyToOne,
			fill:     parser.VectorMatchFillValues{RHS: &fillValue},
			fillsOne: true,
		},
		"many-to-one with both fills": {
			card:      parser.CardManyToOne,
			fill:      parser.VectorMatchFillValues{LHS: &fillValue, RHS: &fillValue},
			fillsMany: true,
			fillsOne:  true,
		},
		"one-to-many without fill": {
			card: parser.CardOneToMany,
		},
		"one-to-many with LHS fill": {
			card:      parser.CardOneToMany,
			fill:      parser.VectorMatchFillValues{LHS: &fillValue},
			fillsMany: true,
		},
		"one-to-many with RHS fill": {
			card:     parser.CardOneToMany,
			fill:     parser.VectorMatchFillValues{RHS: &fillValue},
			fillsOne: true,
		},
		"one-to-many with both fills": {
			card:      parser.CardOneToMany,
			fill:      parser.VectorMatchFillValues{LHS: &fillValue, RHS: &fillValue},
			fillsMany: true,
			fillsOne:  true,
		},
	}
	emptyCases := map[string]struct {
		many bool
		one  bool
	}{
		"neither side empty": {},
		"many side empty":    {many: true},
		"one side empty":     {one: true},
		"both sides empty":   {many: true, one: true},
	}

	for fillName, fillCase := range fillCases {
		for emptyName, emptyCase := range emptyCases {
			t.Run(fillName+"/"+emptyName, func(t *testing.T) {
				memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
				manySeries := []labels.Labels{labels.FromStrings("group", "a")}
				if emptyCase.many {
					manySeries = nil
				}
				oneSeries := []labels.Labels{labels.FromStrings("group", "a")}
				if emptyCase.one {
					oneSeries = nil
				}

				many := &finishedReadingCountingOperator{TestOperator: &operators.TestOperator{Series: manySeries, Data: make([]types.InstantVectorSeriesData, len(manySeries)), MemoryConsumptionTracker: memoryConsumptionTracker}}
				one := &finishedReadingCountingOperator{TestOperator: &operators.TestOperator{Series: oneSeries, Data: make([]types.InstantVectorSeriesData, len(oneSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}}
				left, right := many, one
				if fillCase.card == parser.CardOneToMany {
					left, right = one, many
				}

				o, err := NewGroupedVectorVectorBinaryOperation(
					left,
					right,
					parser.VectorMatching{Card: fillCase.card, On: true, MatchingLabels: []string{"group"}, FillValues: fillCase.fill},
					parser.ADD,
					false,
					memoryConsumptionTracker,
					posrange.PositionRange{},
					types.QueryTimeRange{},
					nil,
					log.NewNopLogger(),
				)
				require.NoError(t, err)

				canProduceAnySeries, err := o.loadSeriesMetadata(t.Context(), nil)
				require.NoError(t, err)

				expectedManyCall := !emptyCase.one || fillCase.fillsOne
				expectedContinuation := !emptyCase.many && !emptyCase.one
				if emptyCase.many != emptyCase.one {
					expectedContinuation = (emptyCase.many && fillCase.fillsMany) || (emptyCase.one && fillCase.fillsOne)
				}
				require.True(t, one.SeriesMetadataCalled)
				require.Equal(t, expectedManyCall, many.SeriesMetadataCalled)
				require.Equal(t, expectedContinuation, canProduceAnySeries)
				expectedManyFinishedCalls := 0
				expectedOneFinishedCalls := 0
				if canProduceAnySeries && emptyCase.many {
					expectedManyFinishedCalls = 1
				}
				if canProduceAnySeries && emptyCase.one {
					expectedOneFinishedCalls = 1
				}
				require.Equal(t, expectedManyFinishedCalls, many.finishedReadingCalls)
				require.Equal(t, expectedOneFinishedCalls, one.finishedReadingCalls)
				if canProduceAnySeries && (emptyCase.many || emptyCase.one) {
					metadata, _, oneUsed, _, manyUsed, _, err := o.computeOutputSeries()
					require.NoError(t, err)
					expectedMetadataCount := 0
					if emptyCase.one && !emptyCase.many && fillCase.fillsOne {
						expectedMetadataCount = 1
					}
					require.Len(t, metadata, expectedMetadataCount)
					types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)
					types.BoolSlicePool.Put(&oneUsed, memoryConsumptionTracker)
					types.BoolSlicePool.Put(&manyUsed, memoryConsumptionTracker)
				}

				require.NoError(t, o.FinishedReading(t.Context()))
				require.Equal(t, 1, many.finishedReadingCalls)
				require.Equal(t, 1, one.finishedReadingCalls)
			})
		}
	}
}

func TestGroupedVectorVectorBinaryOperation_FillsSyntheticOneSide(t *testing.T) {
	fillThree := 3.0
	fillTwo := 2.0
	fillInfinity := math.Inf(1)
	fillNaN := math.NaN()
	ts := int64(0)
	secondTS := time.Minute.Milliseconds()
	thirdTS := 2 * time.Minute.Milliseconds()

	type expectedSeries struct {
		labels    labels.Labels
		floats    []promql.FPoint
		histogram *histogram.FloatHistogram
	}
	testCases := map[string]struct {
		card       parser.VectorMatchCardinality
		op         parser.ItemType
		returnBool bool
		fill       parser.VectorMatchFillValues
		include    []string
		ignoring   []string
		onLabels   []string

		manySeries []labels.Labels
		manyData   []types.InstantVectorSeriesData
		oneSeries  []labels.Labels
		oneData    []types.InstantVectorSeriesData
		timeRange  types.QueryTimeRange

		expected                       []expectedSeries
		expectOneFinishedAfterMetadata bool
		carrierFirst                   bool
		expectCarrierCollision         bool
	}{
		"many-to-one arithmetic preserves all unmatched many series": {
			card: parser.CardManyToOne,
			op:   parser.SUB,
			fill: parser.VectorMatchFillValues{RHS: &fillThree},
			manySeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "series", "1"),
				labels.FromStrings(model.MetricNameLabel, "many", "group", "b", "series", "2"),
			},
			manyData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: ts, F: 10}}},
				{Floats: []promql.FPoint{{T: ts, F: 20}}},
			},
			expected: []expectedSeries{
				{labels: labels.FromStrings("group", "a", "series", "1"), floats: []promql.FPoint{{T: ts, F: 7}}},
				{labels: labels.FromStrings("group", "b", "series", "2"), floats: []promql.FPoint{{T: ts, F: 17}}},
			},
		},
		"one-to-many arithmetic maps the synthetic operand to the left": {
			card: parser.CardOneToMany,
			op:   parser.SUB,
			fill: parser.VectorMatchFillValues{RHS: &fillThree},
			manySeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "series", "1"),
				labels.FromStrings(model.MetricNameLabel, "many", "group", "b", "series", "2"),
			},
			manyData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: ts, F: 10}}},
				{Floats: []promql.FPoint{{T: ts, F: 20}}},
			},
			expected: []expectedSeries{
				{labels: labels.FromStrings("group", "a", "series", "1"), floats: []promql.FPoint{{T: ts, F: -7}}},
				{labels: labels.FromStrings("group", "b", "series", "2"), floats: []promql.FPoint{{T: ts, F: -17}}},
			},
		},
		"many-to-one uses real include labels and deletes synthetic values": {
			card:    parser.CardManyToOne,
			op:      parser.ADD,
			fill:    parser.VectorMatchFillValues{RHS: &fillThree},
			include: []string{"owner"},
			manySeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "owner", "stale"),
				labels.FromStrings(model.MetricNameLabel, "many", "group", "b", "owner", "stale"),
			},
			manyData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: ts, F: 10}}},
				{Floats: []promql.FPoint{{T: ts, F: 20}}},
			},
			oneSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "one", "group", "a", "owner", "team-a"),
			},
			oneData: []types.InstantVectorSeriesData{{}},
			expected: []expectedSeries{
				{labels: labels.FromStrings("group", "a", "owner", "team-a")},
				{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: ts, F: 13}}},
				{labels: labels.FromStrings("group", "b"), floats: []promql.FPoint{{T: ts, F: 23}}},
			},
		},
		"one-to-many uses real include labels and deletes synthetic values": {
			card:    parser.CardOneToMany,
			op:      parser.ADD,
			fill:    parser.VectorMatchFillValues{RHS: &fillThree},
			include: []string{"owner"},
			manySeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "owner", "stale"),
				labels.FromStrings(model.MetricNameLabel, "many", "group", "b", "owner", "stale"),
			},
			manyData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: ts, F: 10}}},
				{Floats: []promql.FPoint{{T: ts, F: 20}}},
			},
			oneSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "one", "group", "a", "owner", "team-a"),
			},
			oneData: []types.InstantVectorSeriesData{{}},
			expected: []expectedSeries{
				{labels: labels.FromStrings("group", "a", "owner", "team-a")},
				{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: ts, F: 13}}},
				{labels: labels.FromStrings("group", "b"), floats: []promql.FPoint{{T: ts, F: 23}}},
			},
		},
		"many-to-one emits one carrier across intermittent include variants": {
			card:         parser.CardManyToOne,
			op:           parser.ADD,
			fill:         parser.VectorMatchFillValues{RHS: &fillThree},
			include:      []string{"owner"},
			timeRange:    types.NewRangeQueryTimeRange(timestamp.Time(ts), timestamp.Time(thirdTS), time.Minute),
			carrierFirst: true,
			manySeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "owner", "stale"),
			},
			manyData: []types.InstantVectorSeriesData{{Floats: []promql.FPoint{
				{T: ts, F: 10},
				{T: secondTS, F: 20},
				{T: thirdTS, F: 30},
			}}},
			oneSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "one", "group", "a", "owner", "team-a"),
				labels.FromStrings(model.MetricNameLabel, "one", "group", "a", "owner", "team-b"),
			},
			oneData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: ts, F: 1}}},
				{Floats: []promql.FPoint{{T: secondTS, F: 2}}},
			},
			expected: []expectedSeries{
				{labels: labels.FromStrings("group", "a", "owner", "team-a"), floats: []promql.FPoint{{T: ts, F: 11}}},
				{labels: labels.FromStrings("group", "a", "owner", "team-b"), floats: []promql.FPoint{{T: secondTS, F: 22}}},
				{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: thirdTS, F: 33}}},
			},
		},
		"one-to-many merges a carrier collision across intermittent include variants": {
			card:      parser.CardOneToMany,
			op:        parser.ADD,
			fill:      parser.VectorMatchFillValues{RHS: &fillThree},
			include:   []string{"owner"},
			timeRange: types.NewRangeQueryTimeRange(timestamp.Time(ts), timestamp.Time(thirdTS), time.Minute),
			manySeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "owner", "stale"),
			},
			manyData: []types.InstantVectorSeriesData{{Floats: []promql.FPoint{
				{T: ts, F: 10},
				{T: secondTS, F: 20},
				{T: thirdTS, F: 30},
			}}},
			oneSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "one", "group", "a"),
				labels.FromStrings(model.MetricNameLabel, "one", "group", "a", "owner", "team-a"),
			},
			oneData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: ts, F: 1}}},
				{Floats: []promql.FPoint{{T: secondTS, F: 2}}},
			},
			expected: []expectedSeries{
				{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: ts, F: 11}, {T: thirdTS, F: 33}}},
				{labels: labels.FromStrings("group", "a", "owner", "team-a"), floats: []promql.FPoint{{T: secondTS, F: 22}}},
			},
		},
		"many-to-one canonicalizes absent and empty include values": {
			card:                   parser.CardManyToOne,
			op:                     parser.ADD,
			fill:                   parser.VectorMatchFillValues{RHS: &fillThree},
			include:                []string{"owner"},
			timeRange:              types.NewRangeQueryTimeRange(timestamp.Time(ts), timestamp.Time(thirdTS), time.Minute),
			manySeries:             []labels.Labels{labels.FromStrings(model.MetricNameLabel, "many", "group", "a")},
			expectCarrierCollision: true,
			manyData: []types.InstantVectorSeriesData{{Floats: []promql.FPoint{
				{T: ts, F: 10},
				{T: secondTS, F: 20},
				{T: thirdTS, F: 30},
			}}},
			oneSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "one", "group", "a"),
				labels.FromStrings(model.MetricNameLabel, "one", "group", "a", "owner", ""),
			},
			oneData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: ts, F: 1}}},
				{Floats: []promql.FPoint{{T: secondTS, F: 2}}},
			},
			expected: []expectedSeries{
				{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: ts, F: 11}, {T: secondTS, F: 22}, {T: thirdTS, F: 33}}},
			},
		},
		"one-to-many canonicalizes absent and empty include values": {
			card:                   parser.CardOneToMany,
			op:                     parser.ADD,
			fill:                   parser.VectorMatchFillValues{RHS: &fillThree},
			include:                []string{"owner"},
			timeRange:              types.NewRangeQueryTimeRange(timestamp.Time(ts), timestamp.Time(thirdTS), time.Minute),
			manySeries:             []labels.Labels{labels.FromStrings(model.MetricNameLabel, "many", "group", "a")},
			expectCarrierCollision: true,
			manyData: []types.InstantVectorSeriesData{{Floats: []promql.FPoint{
				{T: ts, F: 10},
				{T: secondTS, F: 20},
				{T: thirdTS, F: 30},
			}}},
			oneSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "one", "group", "a"),
				labels.FromStrings(model.MetricNameLabel, "one", "group", "a", "owner", ""),
			},
			oneData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: ts, F: 1}}},
				{Floats: []promql.FPoint{{T: secondTS, F: 2}}},
			},
			expected: []expectedSeries{
				{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: ts, F: 11}, {T: secondTS, F: 22}, {T: thirdTS, F: 33}}},
			},
		},
		"many-to-one on retains an included matching label on fill": {
			card:       parser.CardManyToOne,
			op:         parser.ADD,
			fill:       parser.VectorMatchFillValues{RHS: &fillThree},
			include:    []string{"owner"},
			onLabels:   []string{"group", "owner"},
			manySeries: []labels.Labels{labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "owner", "team-a")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			expected:   []expectedSeries{{labels: labels.FromStrings("group", "a", "owner", "team-a"), floats: []promql.FPoint{{T: ts, F: 13}}}},
		},
		"one-to-many on retains an included matching label on fill": {
			card:       parser.CardOneToMany,
			op:         parser.ADD,
			fill:       parser.VectorMatchFillValues{RHS: &fillThree},
			include:    []string{"owner"},
			onLabels:   []string{"group", "owner"},
			manySeries: []labels.Labels{labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "owner", "team-a")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			expected:   []expectedSeries{{labels: labels.FromStrings("group", "a", "owner", "team-a"), floats: []promql.FPoint{{T: ts, F: 13}}}},
		},
		"many-to-one ignoring retains an included matching label on fill": {
			card:       parser.CardManyToOne,
			op:         parser.ADD,
			fill:       parser.VectorMatchFillValues{RHS: &fillThree},
			include:    []string{"zone"},
			ignoring:   []string{"instance"},
			manySeries: []labels.Labels{labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "instance", "many", "zone", "us")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			expected:   []expectedSeries{{labels: labels.FromStrings("group", "a", "instance", "many", "zone", "us"), floats: []promql.FPoint{{T: ts, F: 13}}}},
		},
		"one-to-many ignoring retains an included matching label on fill": {
			card:       parser.CardOneToMany,
			op:         parser.ADD,
			fill:       parser.VectorMatchFillValues{RHS: &fillThree},
			include:    []string{"zone"},
			ignoring:   []string{"instance"},
			manySeries: []labels.Labels{labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "instance", "many", "zone", "us")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			expected:   []expectedSeries{{labels: labels.FromStrings("group", "a", "instance", "many", "zone", "us"), floats: []promql.FPoint{{T: ts, F: 13}}}},
		},
		"many-to-one ignoring removes an ignored include and merges many series": {
			card:      parser.CardManyToOne,
			op:        parser.ADD,
			fill:      parser.VectorMatchFillValues{RHS: &fillThree},
			include:   []string{"owner"},
			ignoring:  []string{"owner"},
			timeRange: types.NewRangeQueryTimeRange(timestamp.Time(ts), timestamp.Time(secondTS), time.Minute),
			manySeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "owner", "team-a"),
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "owner", "team-b"),
			},
			manyData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: ts, F: 10}}},
				{Floats: []promql.FPoint{{T: secondTS, F: 20}}},
			},
			expected: []expectedSeries{{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: ts, F: 13}, {T: secondTS, F: 23}}}},
		},
		"one-to-many ignoring removes an ignored include and merges many series": {
			card:      parser.CardOneToMany,
			op:        parser.ADD,
			fill:      parser.VectorMatchFillValues{RHS: &fillThree},
			include:   []string{"owner"},
			ignoring:  []string{"owner"},
			timeRange: types.NewRangeQueryTimeRange(timestamp.Time(ts), timestamp.Time(secondTS), time.Minute),
			manySeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "owner", "team-a"),
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "owner", "team-b"),
			},
			manyData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: ts, F: 10}}},
				{Floats: []promql.FPoint{{T: secondTS, F: 20}}},
			},
			expected: []expectedSeries{{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: ts, F: 13}, {T: secondTS, F: 23}}}},
		},
		"many-to-one ignoring merges absent and empty included many labels": {
			card:      parser.CardManyToOne,
			op:        parser.ADD,
			fill:      parser.VectorMatchFillValues{RHS: &fillThree},
			include:   []string{"owner"},
			ignoring:  []string{"instance"},
			timeRange: types.NewRangeQueryTimeRange(timestamp.Time(ts), timestamp.Time(secondTS), time.Minute),
			manySeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "instance", "x"),
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "instance", "x", "owner", ""),
			},
			manyData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: ts, F: 10}}},
				{Floats: []promql.FPoint{{T: secondTS, F: 20}}},
			},
			expected: []expectedSeries{{labels: labels.FromStrings("group", "a", "instance", "x"), floats: []promql.FPoint{{T: ts, F: 13}, {T: secondTS, F: 23}}}},
		},
		"one-to-many ignoring merges absent and empty included many labels": {
			card:      parser.CardOneToMany,
			op:        parser.ADD,
			fill:      parser.VectorMatchFillValues{RHS: &fillThree},
			include:   []string{"owner"},
			ignoring:  []string{"instance"},
			timeRange: types.NewRangeQueryTimeRange(timestamp.Time(ts), timestamp.Time(secondTS), time.Minute),
			manySeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "instance", "x"),
				labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "instance", "x", "owner", ""),
			},
			manyData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: ts, F: 10}}},
				{Floats: []promql.FPoint{{T: secondTS, F: 20}}},
			},
			expected: []expectedSeries{{labels: labels.FromStrings("group", "a", "instance", "x"), floats: []promql.FPoint{{T: ts, F: 13}, {T: secondTS, F: 23}}}},
		},
		"many-to-one finishes a wholly unused disjoint one side": {
			card:                           parser.CardManyToOne,
			op:                             parser.ADD,
			fill:                           parser.VectorMatchFillValues{RHS: &fillThree},
			manySeries:                     []labels.Labels{labels.FromStrings("group", "many")},
			manyData:                       []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			oneSeries:                      []labels.Labels{labels.FromStrings("group", "one")},
			oneData:                        []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 20}}}},
			expected:                       []expectedSeries{{labels: labels.FromStrings("group", "many"), floats: []promql.FPoint{{T: ts, F: 13}}}},
			expectOneFinishedAfterMetadata: true,
		},
		"one-to-many finishes a wholly unused disjoint one side": {
			card:                           parser.CardOneToMany,
			op:                             parser.ADD,
			fill:                           parser.VectorMatchFillValues{RHS: &fillThree},
			manySeries:                     []labels.Labels{labels.FromStrings("group", "many")},
			manyData:                       []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			oneSeries:                      []labels.Labels{labels.FromStrings("group", "one")},
			oneData:                        []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 20}}}},
			expected:                       []expectedSeries{{labels: labels.FromStrings("group", "many"), floats: []promql.FPoint{{T: ts, F: 13}}}},
			expectOneFinishedAfterMetadata: true,
		},
		"many-to-one comparison retains the many metric name": {
			card:       parser.CardManyToOne,
			op:         parser.GTR,
			fill:       parser.VectorMatchFillValues{RHS: &fillThree},
			manySeries: []labels.Labels{labels.FromStrings(model.MetricNameLabel, "many", "group", "a")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			expected: []expectedSeries{
				{labels: labels.FromStrings(model.MetricNameLabel, "many", "group", "a"), floats: []promql.FPoint{{T: ts, F: 10}}},
			},
		},
		"one-to-many comparison retains the many metric name": {
			card:       parser.CardOneToMany,
			op:         parser.LSS,
			fill:       parser.VectorMatchFillValues{RHS: &fillThree},
			manySeries: []labels.Labels{labels.FromStrings(model.MetricNameLabel, "many", "group", "a")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			expected: []expectedSeries{
				{labels: labels.FromStrings(model.MetricNameLabel, "many", "group", "a"), floats: []promql.FPoint{{T: ts, F: 3}}},
			},
		},
		"many-to-one bool comparison drops the metric name": {
			card:       parser.CardManyToOne,
			op:         parser.GTR,
			returnBool: true,
			fill:       parser.VectorMatchFillValues{RHS: &fillThree},
			manySeries: []labels.Labels{labels.FromStrings(model.MetricNameLabel, "many", "group", "a")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			expected: []expectedSeries{
				{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: ts, F: 1}}},
			},
		},
		"one-to-many bool comparison drops the metric name": {
			card:       parser.CardOneToMany,
			op:         parser.LSS,
			returnBool: true,
			fill:       parser.VectorMatchFillValues{RHS: &fillThree},
			manySeries: []labels.Labels{labels.FromStrings(model.MetricNameLabel, "many", "group", "a")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			expected: []expectedSeries{
				{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: ts, F: 1}}},
			},
		},
		"many-to-one multiplies a histogram by the synthetic scalar": {
			card:       parser.CardManyToOne,
			op:         parser.MUL,
			fill:       parser.VectorMatchFillValues{RHS: &fillTwo},
			manySeries: []labels.Labels{labels.FromStrings(model.MetricNameLabel, "many", "group", "a")},
			manyData: []types.InstantVectorSeriesData{{Histograms: []promql.HPoint{
				{T: ts, H: &histogram.FloatHistogram{Count: 3, Sum: 6}},
			}}},
			expected: []expectedSeries{
				{labels: labels.FromStrings("group", "a"), histogram: &histogram.FloatHistogram{Count: 6, Sum: 12}},
			},
		},
		"one-to-many multiplies the synthetic scalar by a histogram": {
			card:       parser.CardOneToMany,
			op:         parser.MUL,
			fill:       parser.VectorMatchFillValues{RHS: &fillTwo},
			manySeries: []labels.Labels{labels.FromStrings(model.MetricNameLabel, "many", "group", "a")},
			manyData: []types.InstantVectorSeriesData{{Histograms: []promql.HPoint{
				{T: ts, H: &histogram.FloatHistogram{Count: 3, Sum: 6}},
			}}},
			expected: []expectedSeries{
				{labels: labels.FromStrings("group", "a"), histogram: &histogram.FloatHistogram{Count: 6, Sum: 12}},
			},
		},
		"many-to-one applies an infinite fill value": {
			card:       parser.CardManyToOne,
			op:         parser.SUB,
			fill:       parser.VectorMatchFillValues{RHS: &fillInfinity},
			manySeries: []labels.Labels{labels.FromStrings("group", "a")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			expected:   []expectedSeries{{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: ts, F: math.Inf(-1)}}}},
		},
		"one-to-many applies an infinite fill value": {
			card:       parser.CardOneToMany,
			op:         parser.SUB,
			fill:       parser.VectorMatchFillValues{RHS: &fillInfinity},
			manySeries: []labels.Labels{labels.FromStrings("group", "a")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			expected:   []expectedSeries{{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: ts, F: math.Inf(1)}}}},
		},
		"many-to-one bool comparison accepts a NaN fill value": {
			card:       parser.CardManyToOne,
			op:         parser.GTR,
			returnBool: true,
			fill:       parser.VectorMatchFillValues{RHS: &fillNaN},
			manySeries: []labels.Labels{labels.FromStrings("group", "a")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			expected:   []expectedSeries{{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: ts, F: 0}}}},
		},
		"one-to-many bool comparison accepts a NaN fill value": {
			card:       parser.CardOneToMany,
			op:         parser.GTR,
			returnBool: true,
			fill:       parser.VectorMatchFillValues{RHS: &fillNaN},
			manySeries: []labels.Labels{labels.FromStrings("group", "a")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
			expected:   []expectedSeries{{labels: labels.FromStrings("group", "a"), floats: []promql.FPoint{{T: ts, F: 0}}}},
		},
		"many-to-one does not synthesize without a normalized one fill": {
			card:       parser.CardManyToOne,
			op:         parser.ADD,
			fill:       parser.VectorMatchFillValues{LHS: &fillThree},
			manySeries: []labels.Labels{labels.FromStrings("group", "a")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
		},
		"one-to-many does not synthesize without a normalized one fill": {
			card:       parser.CardOneToMany,
			op:         parser.ADD,
			fill:       parser.VectorMatchFillValues{LHS: &fillThree},
			manySeries: []labels.Labels{labels.FromStrings("group", "a")},
			manyData:   []types.InstantVectorSeriesData{{Floats: []promql.FPoint{{T: ts, F: 10}}}},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
			copyData := func(source []types.InstantVectorSeriesData) []types.InstantVectorSeriesData {
				result := make([]types.InstantVectorSeriesData, len(source))
				for i, data := range source {
					if len(data.Floats) > 0 {
						points, err := types.FPointSlicePool.Get(len(data.Floats), memoryConsumptionTracker)
						require.NoError(t, err)
						result[i].Floats = append(points, data.Floats...)
					}
					if len(data.Histograms) > 0 {
						points, err := types.HPointSlicePool.Get(len(data.Histograms), memoryConsumptionTracker)
						require.NoError(t, err)
						for _, point := range data.Histograms {
							points = append(points, promql.HPoint{T: point.T, H: point.H.Copy()})
						}
						result[i].Histograms = points
					}
				}
				return result
			}

			many := &finishedReadingCountingOperator{TestOperator: &operators.TestOperator{
				Series:                   testCase.manySeries,
				Data:                     copyData(testCase.manyData),
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}}
			one := &finishedReadingCountingOperator{TestOperator: &operators.TestOperator{
				Series:                   testCase.oneSeries,
				Data:                     copyData(testCase.oneData),
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}}
			left, right := many, one
			if testCase.card == parser.CardOneToMany {
				left, right = one, many
			}

			timeRange := testCase.timeRange
			if timeRange.StepCount == 0 {
				timeRange = types.NewInstantQueryTimeRange(timestamp.Time(ts))
			}
			vectorMatching := parser.VectorMatching{
				Card:           testCase.card,
				On:             true,
				MatchingLabels: []string{"group"},
				Include:        testCase.include,
				FillValues:     testCase.fill,
			}
			if testCase.ignoring != nil {
				vectorMatching.On = false
				vectorMatching.MatchingLabels = testCase.ignoring
			} else if testCase.onLabels != nil {
				vectorMatching.MatchingLabels = testCase.onLabels
			}
			operation, err := NewGroupedVectorVectorBinaryOperation(
				left,
				right,
				vectorMatching,
				testCase.op,
				testCase.returnBool,
				memoryConsumptionTracker,
				posrange.PositionRange{},
				timeRange,
				nil,
				log.NewNopLogger(),
			)
			require.NoError(t, err)

			metadata, err := operation.SeriesMetadata(t.Context(), nil)
			require.NoError(t, err)
			if testCase.expectOneFinishedAfterMetadata {
				require.Equal(t, 1, one.finishedReadingCalls)
			}
			var expectedMetadata []types.SeriesMetadata
			if len(testCase.expected) > 0 {
				expectedMetadata = make([]types.SeriesMetadata, 0, len(testCase.expected))
			}
			for _, expected := range testCase.expected {
				expectedMetadata = append(expectedMetadata, types.SeriesMetadata{Labels: expected.labels})
			}
			require.Equal(t, expectedMetadata, metadata)
			types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)
			if testCase.expectCarrierCollision {
				require.Len(t, operation.remainingSeries, 1)
				require.NotNil(t, operation.remainingSeries[0].oneSide)
				require.NotNil(t, operation.remainingSeries[0].fillCarrier)
				require.Len(t, operation.remainingSeries[0].oneSide.seriesIndices, 2)
			}
			if testCase.carrierFirst {
				carrierIndex := slices.IndexFunc(operation.remainingSeries, func(series *groupedBinaryOperationOutputSeries) bool {
					return series.oneSide == nil && series.fillCarrier != nil
				})
				require.GreaterOrEqual(t, carrierIndex, 0)
				carrier := operation.remainingSeries[carrierIndex]
				copy(operation.remainingSeries[1:carrierIndex+1], operation.remainingSeries[:carrierIndex])
				operation.remainingSeries[0] = carrier
				expectedCarrier := testCase.expected[len(testCase.expected)-1]
				copy(testCase.expected[1:], testCase.expected[:len(testCase.expected)-1])
				testCase.expected[0] = expectedCarrier
			}

			for _, expected := range testCase.expected {
				actual, err := operation.NextSeries(t.Context())
				require.NoError(t, err)
				require.Equal(t, expected.floats, actual.Floats)
				if expected.histogram == nil {
					require.Empty(t, actual.Histograms)
				} else {
					require.Len(t, actual.Histograms, 1)
					require.Equal(t, ts, actual.Histograms[0].T)
					require.Equal(t, expected.histogram, actual.Histograms[0].H)
				}
				types.PutInstantVectorSeriesData(actual, memoryConsumptionTracker)
			}

			_, err = operation.NextSeries(t.Context())
			require.ErrorIs(t, err, types.EOS)
			left.ReleaseUnreadData(memoryConsumptionTracker)
			right.ReleaseUnreadData(memoryConsumptionTracker)
			require.NoError(t, operation.FinishedReading(t.Context()))
			require.Equal(t, 1, left.finishedReadingCalls)
			require.Equal(t, 1, right.finishedReadingCalls)
			require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
			operation.Close()
		})
	}
}

func TestGroupedVectorVectorBinaryOperation_FillCarrierPresenceLifecycle(t *testing.T) {
	fill := 3.0
	firstTS := int64(0)
	secondTS := time.Minute.Milliseconds()
	testCases := map[string]struct {
		card          parser.VectorMatchCardinality
		duplicateTime bool
	}{
		"many-to-one early stop": {
			card: parser.CardManyToOne,
		},
		"one-to-many early stop": {
			card: parser.CardOneToMany,
		},
		"many-to-one evaluation error": {
			card:          parser.CardManyToOne,
			duplicateTime: true,
		},
		"one-to-many evaluation error": {
			card:          parser.CardOneToMany,
			duplicateTime: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
			newData := func(points ...promql.FPoint) types.InstantVectorSeriesData {
				pooled, err := types.FPointSlicePool.Get(len(points), memoryConsumptionTracker)
				require.NoError(t, err)
				return types.InstantVectorSeriesData{Floats: append(pooled, points...)}
			}

			secondOneSideTime := secondTS
			if testCase.duplicateTime {
				secondOneSideTime = firstTS
			}
			many := &finishedReadingCountingOperator{TestOperator: &operators.TestOperator{
				Series: []labels.Labels{
					labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "series", "1"),
					labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "series", "2"),
				},
				Data: []types.InstantVectorSeriesData{
					newData(promql.FPoint{T: firstTS, F: 10}, promql.FPoint{T: secondTS, F: 20}),
					newData(promql.FPoint{T: firstTS, F: 30}, promql.FPoint{T: secondTS, F: 40}),
				},
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}}
			one := &finishedReadingCountingOperator{TestOperator: &operators.TestOperator{
				Series: []labels.Labels{
					labels.FromStrings(model.MetricNameLabel, "one", "group", "a"),
					labels.FromStrings(model.MetricNameLabel, "one", "group", "a", "owner", "team-a"),
				},
				Data: []types.InstantVectorSeriesData{
					newData(promql.FPoint{T: firstTS, F: 1}),
					newData(promql.FPoint{T: secondOneSideTime, F: 2}),
				},
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}}
			left, right := many, one
			if testCase.card == parser.CardOneToMany {
				left, right = one, many
			}

			operation, err := NewGroupedVectorVectorBinaryOperation(
				left,
				right,
				parser.VectorMatching{
					Card:           testCase.card,
					On:             true,
					MatchingLabels: []string{"group"},
					Include:        []string{"owner"},
					FillValues:     parser.VectorMatchFillValues{RHS: &fill},
				},
				parser.ADD,
				false,
				memoryConsumptionTracker,
				posrange.PositionRange{},
				types.NewRangeQueryTimeRange(timestamp.Time(firstTS), timestamp.Time(secondTS), time.Minute),
				nil,
				log.NewNopLogger(),
			)
			require.NoError(t, err)

			metadata, err := operation.SeriesMetadata(t.Context(), nil)
			require.NoError(t, err)
			types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)
			result, err := operation.NextSeries(t.Context())
			if testCase.duplicateTime {
				require.ErrorContains(t, err, "duplicate series")
			} else {
				require.NoError(t, err)
				types.PutInstantVectorSeriesData(result, memoryConsumptionTracker)
			}

			left.ReleaseUnreadData(memoryConsumptionTracker)
			right.ReleaseUnreadData(memoryConsumptionTracker)
			require.NoError(t, operation.FinishedReading(t.Context()))
			require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytesBySource(limiter.IntSlices))
			if !testCase.duplicateTime {
				require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
			}
			require.Equal(t, 1, left.finishedReadingCalls)
			require.Equal(t, 1, right.finishedReadingCalls)
			operation.Close()
		})
	}
}

func TestGroupedVectorVectorBinaryOperation_RejectsInvalidPresenceTimestamps(t *testing.T) {
	fill := 3.0
	startTS := int64(0)
	endTS := time.Minute.Milliseconds()
	testCases := map[string]struct {
		card          parser.VectorMatchCardinality
		invalidTime   int64
		expectedError string
	}{
		"many-to-one out of range": {
			card:          parser.CardManyToOne,
			invalidTime:   2 * time.Minute.Milliseconds(),
			expectedError: "record one-side presence at timestamp 120000: timestamp 120000 is outside query range [0, 60000]",
		},
		"one-to-many out of range": {
			card:          parser.CardOneToMany,
			invalidTime:   2 * time.Minute.Milliseconds(),
			expectedError: "record one-side presence at timestamp 120000: timestamp 120000 is outside query range [0, 60000]",
		},
		"many-to-one misaligned": {
			card:          parser.CardManyToOne,
			invalidTime:   30 * time.Second.Milliseconds(),
			expectedError: "record one-side presence at timestamp 30000: timestamp 30000 is not aligned to query interval 60000",
		},
		"one-to-many misaligned": {
			card:          parser.CardOneToMany,
			invalidTime:   30 * time.Second.Milliseconds(),
			expectedError: "record one-side presence at timestamp 30000: timestamp 30000 is not aligned to query interval 60000",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
			newData := func(point promql.FPoint) types.InstantVectorSeriesData {
				points, err := types.FPointSlicePool.Get(1, memoryConsumptionTracker)
				require.NoError(t, err)
				return types.InstantVectorSeriesData{Floats: append(points, point)}
			}
			many := &operators.TestOperator{
				Series:                   []labels.Labels{labels.FromStrings("group", "a")},
				Data:                     []types.InstantVectorSeriesData{newData(promql.FPoint{T: startTS, F: 10})},
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}
			one := &operators.TestOperator{
				Series:                   []labels.Labels{labels.FromStrings("group", "a")},
				Data:                     []types.InstantVectorSeriesData{newData(promql.FPoint{T: testCase.invalidTime, F: 20})},
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}
			left, right := types.InstantVectorOperator(many), types.InstantVectorOperator(one)
			if testCase.card == parser.CardOneToMany {
				left, right = one, many
			}

			operation, err := NewGroupedVectorVectorBinaryOperation(
				left,
				right,
				parser.VectorMatching{
					Card:           testCase.card,
					On:             true,
					MatchingLabels: []string{"group"},
					FillValues:     parser.VectorMatchFillValues{RHS: &fill},
				},
				parser.ADD,
				false,
				memoryConsumptionTracker,
				posrange.PositionRange{},
				types.NewRangeQueryTimeRange(timestamp.Time(startTS), timestamp.Time(endTS), time.Minute),
				nil,
				log.NewNopLogger(),
			)
			require.NoError(t, err)

			metadata, err := operation.SeriesMetadata(t.Context(), nil)
			require.NoError(t, err)
			types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)
			_, err = operation.NextSeries(t.Context())
			require.EqualError(t, err, testCase.expectedError)
			require.Error(t, errors.Unwrap(err))
			many.ReleaseUnreadData(memoryConsumptionTracker)
			one.ReleaseUnreadData(memoryConsumptionTracker)
			require.NoError(t, operation.FinishedReading(t.Context()))
			require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytesBySource(limiter.IntSlices))
			operation.Close()
		})
	}
}

func TestGroupedVectorVectorBinaryOperation_CanonicalManySideIncludeValuesRejectOverlappingPoints(t *testing.T) {
	fill := 3.0
	testCases := map[string]parser.VectorMatchCardinality{
		"many-to-one": parser.CardManyToOne,
		"one-to-many": parser.CardOneToMany,
	}

	for name, cardinality := range testCases {
		t.Run(name, func(t *testing.T) {
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
			newData := func(value float64) types.InstantVectorSeriesData {
				points, err := types.FPointSlicePool.Get(1, memoryConsumptionTracker)
				require.NoError(t, err)
				return types.InstantVectorSeriesData{Floats: append(points, promql.FPoint{T: 0, F: value})}
			}
			many := &operators.TestOperator{
				Series: []labels.Labels{
					labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "instance", "x"),
					labels.FromStrings(model.MetricNameLabel, "many", "group", "a", "instance", "x", "owner", ""),
				},
				Data:                     []types.InstantVectorSeriesData{newData(10), newData(20)},
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}
			one := &operators.TestOperator{MemoryConsumptionTracker: memoryConsumptionTracker}
			left, right := types.InstantVectorOperator(many), types.InstantVectorOperator(one)
			if cardinality == parser.CardOneToMany {
				left, right = one, many
			}

			operation, err := NewGroupedVectorVectorBinaryOperation(
				left,
				right,
				parser.VectorMatching{
					Card:           cardinality,
					MatchingLabels: []string{"instance"},
					Include:        []string{"owner"},
					FillValues:     parser.VectorMatchFillValues{RHS: &fill},
				},
				parser.ADD,
				false,
				memoryConsumptionTracker,
				posrange.PositionRange{},
				types.NewInstantQueryTimeRange(timestamp.Time(0)),
				nil,
				log.NewNopLogger(),
			)
			require.NoError(t, err)

			metadata, err := operation.SeriesMetadata(t.Context(), nil)
			require.NoError(t, err)
			require.Len(t, metadata, 1)
			types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)
			_, err = operation.NextSeries(t.Context())
			require.ErrorIs(t, err, errMultipleMatchesOnManySide)
			require.NoError(t, operation.FinishedReading(t.Context()))
			operation.Close()
		})
	}
}

func TestGroupedVectorVectorBinaryOperation_RejectsMalformedCardinality(t *testing.T) {
	invalidCardinality := parser.VectorMatchCardinality(99)
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
	left := &operators.TestOperator{MemoryConsumptionTracker: memoryConsumptionTracker}
	right := &operators.TestOperator{MemoryConsumptionTracker: memoryConsumptionTracker}

	_, err := NewGroupedVectorVectorBinaryOperation(
		left,
		right,
		parser.VectorMatching{Card: invalidCardinality},
		parser.ADD,
		false,
		memoryConsumptionTracker,
		posrange.PositionRange{},
		types.QueryTimeRange{},
		nil,
		log.NewNopLogger(),
	)
	require.EqualError(t, err, "unsupported cardinality 99")

	_, _, err = (normalizedGroupedSides{}).evaluatorFillValues(invalidCardinality)
	require.EqualError(t, err, "unsupported cardinality 99")
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
		// The right (one) side must carry the "cluster" label so that the
		// parent matcher cluster=us-east does not filter it out (absent
		// labels are treated as "" by TestOperator.matches, matching real
		// TSDB behavior).
		rightSeries := []labels.Labels{
			labels.FromStrings("cluster", "us-east", "env", "prod"),
		}
		leftSeries := []labels.Labels{
			labels.FromStrings("env", "prod", "region", "us-east"),
		}

		left := &operators.TestOperator{Series: leftSeries, Data: make([]types.InstantVectorSeriesData, len(leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
		right := &operators.TestOperator{Series: rightSeries, Data: make([]types.InstantVectorSeriesData, len(rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}

		// Exclude hints that exclude all one-side labels: BuildMatchers will return nil.
		// Both "env" and "cluster" must be excluded so that BuildMatchers produces no
		// matchers from the one-side metadata (the right side carries both labels).
		hints := &Hints{Exclude: []string{"cluster", "env"}}
		vectorMatching := parser.VectorMatching{Card: parser.CardManyToOne, On: false, MatchingLabels: []string{"env"}, Include: []string{"region"}}

		o, err := NewGroupedVectorVectorBinaryOperation(
			left,
			right,
			vectorMatching,
			parser.ADD,
			false,
			memoryConsumptionTracker,
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
				memTracker, posrange.PositionRange{}, timeRange, h, log.NewNopLogger(),
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
