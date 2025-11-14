// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestAndUnlessBinaryOperation_ClosesInnerOperatorsAsSoonAsPossible(t *testing.T) {
	testCases := map[string]struct {
		isUnless    bool
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedOutputSeries                        []labels.Labels
		expectLeftSideClosedAfterOutputSeriesIndex  int
		expectRightSideClosedAfterOutputSeriesIndex int
	}{
		"and: reach end of both sides at the same time": {
			isUnless: false,
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
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  2,
			expectRightSideClosedAfterOutputSeriesIndex: 2,
		},
		"unless: reach end of both sides at the same time": {
			isUnless: true,
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
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  2,
			expectRightSideClosedAfterOutputSeriesIndex: 2,
		},
		"and: no more matches with unmatched series still to read on both sides": {
			isUnless: false,
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
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  1,
			expectRightSideClosedAfterOutputSeriesIndex: 0,
		},
		"unless: no more matches with unmatched series still to read on both sides": {
			isUnless: true,
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
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  2,
			expectRightSideClosedAfterOutputSeriesIndex: 0,
		},
		"and: no more matches with unmatched series still to read on left side": {
			isUnless: false,
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
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  1,
			expectRightSideClosedAfterOutputSeriesIndex: 0,
		},
		"unless: no more matches with unmatched series still to read on left side": {
			isUnless: true,
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
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  2,
			expectRightSideClosedAfterOutputSeriesIndex: 0,
		},
		"and: no more matches with unmatched series still to read on right side": {
			isUnless: false,
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
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  1,
			expectRightSideClosedAfterOutputSeriesIndex: 0,
		},
		"unless: no more matches with unmatched series still to read on right side": {
			isUnless: true,
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
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  1,
			expectRightSideClosedAfterOutputSeriesIndex: 0,
		},
		"and: some series do not match anything on the right": {
			isUnless: false,
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
				labels.FromStrings("group", "1", "series", "left-3"),
				labels.FromStrings("group", "3", "series", "left-4"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  2,
			expectRightSideClosedAfterOutputSeriesIndex: 2,
		},
		"and: no series match": {
			isUnless: false,
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "3", "series", "right-1"),
			},

			expectedOutputSeries:                        []labels.Labels{},
			expectLeftSideClosedAfterOutputSeriesIndex:  -1,
			expectRightSideClosedAfterOutputSeriesIndex: -1,
		},
		"unless: no series match": {
			isUnless: true,
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "3", "series", "right-1"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  1,
			expectRightSideClosedAfterOutputSeriesIndex: -1,
		},
		"and: no series on left": {
			isUnless:   false,
			leftSeries: []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "2", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},

			expectedOutputSeries:                        []labels.Labels{},
			expectLeftSideClosedAfterOutputSeriesIndex:  -1,
			expectRightSideClosedAfterOutputSeriesIndex: -1,
		},
		"unless: no series on left": {
			isUnless:   true,
			leftSeries: []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "2", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},

			expectedOutputSeries:                        []labels.Labels{},
			expectLeftSideClosedAfterOutputSeriesIndex:  -1,
			expectRightSideClosedAfterOutputSeriesIndex: -1,
		},
		"and: no series on right": {
			isUnless: false,
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
				labels.FromStrings("group", "3", "series", "left-3"),
			},
			rightSeries: []labels.Labels{},

			expectedOutputSeries:                        []labels.Labels{},
			expectLeftSideClosedAfterOutputSeriesIndex:  -1,
			expectRightSideClosedAfterOutputSeriesIndex: -1,
		},
		"unless: no series on right": {
			isUnless: true,
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
			o := NewAndUnlessBinaryOperation(left, right, vectorMatching, memoryConsumptionTracker, testCase.isUnless, timeRange, posrange.PositionRange{})

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

func TestAndUnlessBinaryOperation_ReleasesIntermediateStateIfClosedEarly(t *testing.T) {
	testCases := map[string]struct {
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedAndOutputSeries    []labels.Labels
		expectedUnlessOutputSeries []labels.Labels
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
			},

			expectedAndOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"), // When we read this series, we'll have loaded presence for group="1", but nothing for group="2".
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectedUnlessOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"), // When we read this series, we'll have loaded presence for group="1", but nothing for group="2".
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
				labels.FromStrings("group", "3", "series", "left-4"),
			},
		},
		"closed after reading series for multiple output groups": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
				labels.FromStrings("group", "3", "series", "left-4"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "2", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "1", "series", "right-3"),
				labels.FromStrings("group", "2", "series", "right-4"),
			},

			expectedAndOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"), // When we read this series, we'll have loaded presence for group="1" and part of group="2".
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectedUnlessOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"), // When we read this series, we'll have loaded presence for group="1" and part of group="2".
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
				labels.FromStrings("group", "3", "series", "left-4"),
			},
		},
	}

	for name, isUnless := range map[string]bool{"and": false, "unless": true} {
		t.Run(name, func(t *testing.T) {
			for name, testCase := range testCases {
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					timeRange := types.NewInstantQueryTimeRange(time.Now())
					memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
					left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
					right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
					vectorMatching := parser.VectorMatching{On: true, MatchingLabels: []string{"group"}}
					o := NewAndUnlessBinaryOperation(left, right, vectorMatching, memoryConsumptionTracker, isUnless, timeRange, posrange.PositionRange{})

					outputSeries, err := o.SeriesMetadata(ctx, nil)
					require.NoError(t, err)

					if isUnless {
						require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedUnlessOutputSeries), outputSeries)
					} else {
						require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedAndOutputSeries), outputSeries)
					}
					types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)

					// Read the first output series to trigger the loading of some intermediate state for at least one of the output groups.
					_, err = o.NextSeries(ctx)
					require.NoError(t, err)

					// Close the operator and confirm that we've returned everything to their pools.
					o.Close()
					require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
				})
			}
		})
	}
}
