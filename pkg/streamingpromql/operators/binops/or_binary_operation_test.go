// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestOrBinaryOperationSorting(t *testing.T) {
	// The majority of the functionality of OrBinaryOperation is exercised by the tests in the testdata directory.
	// This test exists to exercise the output series sorting functionality, which is complex and may be incorrect
	// but still result in correct query results (eg. if we read and return left side series earlier than strictly
	// necessary).

	testCases := map[string]struct {
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedOutputSeriesOrder []labels.Labels
	}{
		"no series from either side": {
			leftSeries:  []labels.Labels{},
			rightSeries: []labels.Labels{},

			expectedOutputSeriesOrder: []labels.Labels{},
		},
		"only left series": {
			leftSeries: []labels.Labels{
				labels.FromStrings("series", "1"),
				labels.FromStrings("series", "2"),
			},
			rightSeries: []labels.Labels{},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("series", "1"),
				labels.FromStrings("series", "2"),
			},
		},
		"only right series": {
			leftSeries: []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings("series", "1"),
				labels.FromStrings("series", "2"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("series", "1"),
				labels.FromStrings("series", "2"),
			},
		},
		"no matching series on either side": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "3"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "2"),
				labels.FromStrings("right", "4", "group", "4"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("right", "2", "group", "2"),
				labels.FromStrings("right", "4", "group", "4"),
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "3"),
			},
		},
		"single left series returned first": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
				labels.FromStrings("left", "3", "group", "2"),
			},
		},
		"multiple left series returned first": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
			},
		},
		"single right series returned first": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "4", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "1", "group", "1"),
				labels.FromStrings("right", "2", "group", "2"),
				labels.FromStrings("right", "3", "group", "2"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("right", "1", "group", "1"),
				labels.FromStrings("left", "4", "group", "2"),
				labels.FromStrings("right", "2", "group", "2"),
				labels.FromStrings("right", "3", "group", "2"),
			},
		},
		"multiple right series returned first": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "4", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "1", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "3", "group", "2"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("right", "1", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("left", "4", "group", "2"),
				labels.FromStrings("right", "3", "group", "2"),
			},
		},
		"single left series returned last": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
			},
		},
		"multiple left series returned last": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
				labels.FromStrings("left", "6", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
				labels.FromStrings("left", "6", "group", "2"),
			},
		},
		"single right series returned last": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
			},
		},
		"multiple right series returned last": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "1"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "1"),
			},
		},
		"group order on left side different to right side": {
			leftSeries: []labels.Labels{
				labels.FromStrings("left", "1", "group", "3"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("right", "4", "group", "2"),
				labels.FromStrings("right", "6", "group", "3"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("left", "1", "group", "3"),
				labels.FromStrings("left", "3", "group", "1"),
				labels.FromStrings("right", "2", "group", "1"),
				labels.FromStrings("left", "5", "group", "2"),
				labels.FromStrings("right", "4", "group", "2"),
				labels.FromStrings("right", "6", "group", "3"),
			},
		},
		// OrBinaryOperation does not handle the case where both sides contain series with identical labels, and
		// instead relies on DeduplicateAndMerge to handle merging series with identical labels.
		// Given OrBinaryOperation is expected to be wrapped in a DeduplicateAndMerge, we can still test this
		// here.
		"same series on both sides, one series": {
			leftSeries: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
			},
		},
		"same series on both sides, multiple series in same order": {
			leftSeries: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
				labels.FromStrings("series", "2", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
				labels.FromStrings("series", "2", "group", "2"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
				labels.FromStrings("series", "2", "group", "2"),
			},
		},
		"same series on both sides, multiple series in different order": {
			leftSeries: []labels.Labels{
				labels.FromStrings("series", "1", "group", "1"),
				labels.FromStrings("series", "2", "group", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("series", "2", "group", "2"),
				labels.FromStrings("series", "1", "group", "1"),
			},

			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("series", "2", "group", "2"),
				labels.FromStrings("series", "1", "group", "1"),
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			left := &operators.TestOperator{Series: testCase.leftSeries, MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, MemoryConsumptionTracker: memoryConsumptionTracker}

			op := NewOrBinaryOperation(
				left,
				right,
				parser.VectorMatching{MatchingLabels: []string{"group"}, On: true},
				memoryConsumptionTracker,
				types.NewInstantQueryTimeRange(timestamp.Time(0)),
				posrange.PositionRange{},
			)

			// Wrap OrBinaryOperation in a DeduplicateAndMerge as would happen at the planning level
			op = operators.NewDeduplicateAndMerge(op, memoryConsumptionTracker)
			actualSeriesMetadata, err := op.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			expectedSeriesMetadata := testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeriesOrder)
			require.Equal(t, expectedSeriesMetadata, actualSeriesMetadata)
		})
	}
}

func TestOrBinaryOperation_ClosesInnerOperatorsAsSoonAsPossible(t *testing.T) {
	testCases := map[string]struct {
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedOutputSeries                        []labels.Labels
		expectLeftSideClosedAfterOutputSeriesIndex  int
		expectRightSideClosedAfterOutputSeriesIndex int
	}{
		"reach end of both sides at the same time": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "2", "series", "right-3"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
				labels.FromStrings("group", "2", "series", "right-3"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  4,
			expectRightSideClosedAfterOutputSeriesIndex: 5,
		},
		"no more matches with unmatched series still to read on both sides": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  5,
			expectRightSideClosedAfterOutputSeriesIndex: 4,
		},
		"no more matches with unmatched series still to read on left side": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  4,
			expectRightSideClosedAfterOutputSeriesIndex: 3,
		},
		"no more matches with unmatched series still to read on right side": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  1,
			expectRightSideClosedAfterOutputSeriesIndex: 4,
		},
		"some series do not match anything on the right": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
				labels.FromStrings("group", "1", "series", "left-3"),
				labels.FromStrings("group", "3", "series", "left-4"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
				labels.FromStrings("group", "1", "series", "left-3"),
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "left-4"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  5,
			expectRightSideClosedAfterOutputSeriesIndex: 6,
		},
		"some series do not match anything on the left": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "3", "series", "left-3"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "2", "series", "right-2"),
				labels.FromStrings("group", "1", "series", "right-3"),
				labels.FromStrings("group", "3", "series", "right-4"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "2", "series", "right-2"),
				labels.FromStrings("group", "1", "series", "right-3"),
				labels.FromStrings("group", "3", "series", "left-3"),
				labels.FromStrings("group", "3", "series", "right-4"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  5,
			expectRightSideClosedAfterOutputSeriesIndex: 6,
		},
		"no series match": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "3", "series", "right-1"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "3", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  2,
			expectRightSideClosedAfterOutputSeriesIndex: 0,
		},
		"no series on left": {
			leftSeries: []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "2", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "2", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  -1,
			expectRightSideClosedAfterOutputSeriesIndex: 2,
		},
		"no series on right": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
				labels.FromStrings("group", "3", "series", "left-3"),
			},
			rightSeries: []labels.Labels{},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
				labels.FromStrings("group", "3", "series", "left-3"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  2,
			expectRightSideClosedAfterOutputSeriesIndex: -1,
		},
		"no series on either side": {
			leftSeries:  []labels.Labels{},
			rightSeries: []labels.Labels{},

			expectedOutputSeries:                        []labels.Labels{},
			expectLeftSideClosedAfterOutputSeriesIndex:  -1,
			expectRightSideClosedAfterOutputSeriesIndex: -1,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			if testCase.expectLeftSideClosedAfterOutputSeriesIndex >= len(testCase.expectedOutputSeries) {
				require.Failf(t, "invalid test case", "expectLeftSideClosedAfterOutputSeriesIndex %v is beyond end of expected output series %v", testCase.expectLeftSideClosedAfterOutputSeriesIndex, testCase.expectedOutputSeries)
			}

			if testCase.expectRightSideClosedAfterOutputSeriesIndex >= len(testCase.expectedOutputSeries) {
				require.Failf(t, "invalid test case", "expectRightSideClosedAfterOutputSeriesIndex %v is beyond end of expected output series %v", testCase.expectRightSideClosedAfterOutputSeriesIndex, testCase.expectedOutputSeries)
			}

			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			vectorMatching := parser.VectorMatching{On: true, MatchingLabels: []string{"group"}}
			o := NewOrBinaryOperation(left, right, vectorMatching, memoryConsumptionTracker, timeRange, posrange.PositionRange{})

			outputSeries, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			if len(testCase.expectedOutputSeries) == 0 {
				require.Empty(t, outputSeries)
			} else {
				require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)
			}

			if testCase.expectLeftSideClosedAfterOutputSeriesIndex == -1 {
				require.True(t, left.Finalized, "left side should be finalized after SeriesMetadata, but it is not")
				require.True(t, left.Closed, "left side should be closed after SeriesMetadata, but it is not")
			} else {
				require.False(t, left.Finalized, "left side should not be finalized after SeriesMetadata, but it is")
				require.False(t, left.Closed, "left side should not be closed after SeriesMetadata, but it is")
			}

			if testCase.expectRightSideClosedAfterOutputSeriesIndex == -1 {
				require.True(t, right.Finalized, "right side should be finalized after SeriesMetadata, but it is not")
				require.True(t, right.Closed, "right side should be closed after SeriesMetadata, but it is not")
			} else {
				require.False(t, right.Finalized, "right side should not be finalized after SeriesMetadata, but it is")
				require.False(t, right.Closed, "right side should not be closed after SeriesMetadata, but it is")
			}

			for outputSeriesIdx := range outputSeries {
				_, err := o.NextSeries(ctx)
				require.NoErrorf(t, err, "got error while reading series at index %v", outputSeriesIdx)

				if outputSeriesIdx >= testCase.expectLeftSideClosedAfterOutputSeriesIndex {
					require.Truef(t, left.Finalized, "left side should be finalized after output series at index %v, but it is not", outputSeriesIdx)
					require.Truef(t, left.Closed, "left side should be closed after output series at index %v, but it is not", outputSeriesIdx)
				} else {
					require.Falsef(t, left.Finalized, "left side should not be finalized after output series at index %v, but it is", outputSeriesIdx)
					require.Falsef(t, left.Closed, "left side should not be closed after output series at index %v, but it is", outputSeriesIdx)
				}

				if outputSeriesIdx >= testCase.expectRightSideClosedAfterOutputSeriesIndex {
					require.Truef(t, right.Finalized, "right side should be finalized after output series at index %v, but it is not", outputSeriesIdx)
					require.Truef(t, right.Closed, "right side should be closed after output series at index %v, but it is not", outputSeriesIdx)
				} else {
					require.Falsef(t, right.Finalized, "right side should not be finalized after output series at index %v, but it is", outputSeriesIdx)
					require.Falsef(t, right.Closed, "right side should not be closed after output series at index %v, but it is", outputSeriesIdx)
				}
			}

			types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)

			_, err = o.NextSeries(ctx)
			require.Equal(t, types.EOS, err)

			o.Close()
			// Make sure we've returned everything to their pools.
			require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
		})
	}
}

func TestOrBinaryOperation_ReleasesIntermediateStateIfClosedEarly(t *testing.T) {
	testCases := map[string]struct {
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedOutputSeries   []labels.Labels
		closeAfterReadingIndex int
	}{
		"closed after only reading series for current output group": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
				labels.FromStrings("group", "3", "series", "left-4"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "2", "series", "right-3"),
				labels.FromStrings("group", "4", "series", "right-4"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"), // When we read this series, we'll have loaded some presence for group="1", but nothing for group="2".
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
				labels.FromStrings("group", "2", "series", "right-3"),
				labels.FromStrings("group", "4", "series", "right-4"),
				labels.FromStrings("group", "3", "series", "left-4"),
			},
			closeAfterReadingIndex: 0,
		},
		"closed after reading series for multiple output groups": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
				labels.FromStrings("group", "1", "series", "left-3"),
				labels.FromStrings("group", "2", "series", "left-4"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "2", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "1", "series", "right-3"),
				labels.FromStrings("group", "2", "series", "right-4"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"), // When we read this series, we'll have loaded presence for group="1" and group="2".
				labels.FromStrings("group", "1", "series", "left-3"),
				labels.FromStrings("group", "2", "series", "left-4"),
				labels.FromStrings("group", "2", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "1", "series", "right-3"),
				labels.FromStrings("group", "2", "series", "right-4"),
			},
			closeAfterReadingIndex: 1,
		},
		"closed after reading all left series": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "2", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "1", "series", "right-3"),
				labels.FromStrings("group", "2", "series", "right-4"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"), // When we read this series, we'll have loaded presence for group="1" and group="2".
				labels.FromStrings("group", "2", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "1", "series", "right-3"),
				labels.FromStrings("group", "2", "series", "right-4"),
			},
			closeAfterReadingIndex: 2,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			vectorMatching := parser.VectorMatching{On: true, MatchingLabels: []string{"group"}}
			o := NewOrBinaryOperation(left, right, vectorMatching, memoryConsumptionTracker, timeRange, posrange.PositionRange{})

			outputSeries, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)
			types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)
			// Read the output series to trigger the loading of some intermediate state for at least one of the output groups.
			for range testCase.closeAfterReadingIndex + 1 {
				_, err := o.NextSeries(ctx)
				require.NoError(t, err)
			}

			// Close the operator and confirm that we've returned everything to their pools.
			o.Close()
			require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
		})
	}
}
