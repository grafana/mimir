// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache

import (
	"context"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// These test cases mirror the scenarios covered by TestSplitOperator, but exercise the shared mergeSeriesMetadataFromMultipleSources
// helper directly: they verify both the merged series metadata and the mapping from each output series back to the
// corresponding series index in each source.
func TestMergeSeriesMetadata(t *testing.T) {
	testCases := map[string]struct {
		sources              [][]labels.Labels
		expectedMergedLabels []labels.Labels
		expectedOutputSeries []splitOrCacheOutputSeries
	}{
		"all sources return no series": {
			sources: [][]labels.Labels{
				{},
				{},
				{},
			},
			expectedMergedLabels: nil,
			expectedOutputSeries: nil,
		},
		"all sources have the same series": {
			sources: [][]labels.Labels{
				{labelsForIdx(0), labelsForIdx(1), labelsForIdx(2)},
				{labelsForIdx(0), labelsForIdx(1), labelsForIdx(2)},
				{labelsForIdx(0), labelsForIdx(1), labelsForIdx(2)},
			},
			expectedMergedLabels: []labels.Labels{labelsForIdx(0), labelsForIdx(1), labelsForIdx(2)},
			expectedOutputSeries: []splitOrCacheOutputSeries{
				{sourceSeriesIndices: []int{0, 0, 0}},
				{sourceSeriesIndices: []int{1, 1, 1}},
				{sourceSeriesIndices: []int{2, 2, 2}},
			},
		},
		"each source has different series": {
			sources: [][]labels.Labels{
				{labelsForIdx(0), labelsForIdx(1), labelsForIdx(2)},
				{labelsForIdx(3), labelsForIdx(4), labelsForIdx(5)},
				{labelsForIdx(6), labelsForIdx(7), labelsForIdx(8)},
			},
			expectedMergedLabels: []labels.Labels{
				labelsForIdx(0),
				labelsForIdx(1),
				labelsForIdx(2),
				labelsForIdx(3),
				labelsForIdx(4),
				labelsForIdx(5),
				labelsForIdx(6),
				labelsForIdx(7),
				labelsForIdx(8),
			},
			expectedOutputSeries: []splitOrCacheOutputSeries{
				{sourceSeriesIndices: []int{0, -1, -1}},
				{sourceSeriesIndices: []int{1, -1, -1}},
				{sourceSeriesIndices: []int{2, -1, -1}},
				{sourceSeriesIndices: []int{-1, 0, -1}},
				{sourceSeriesIndices: []int{-1, 1, -1}},
				{sourceSeriesIndices: []int{-1, 2, -1}},
				{sourceSeriesIndices: []int{-1, -1, 0}},
				{sourceSeriesIndices: []int{-1, -1, 1}},
				{sourceSeriesIndices: []int{-1, -1, 2}},
			},
		},
		"sources have some same, some different series": {
			sources: [][]labels.Labels{
				{labelsForIdx(0), labelsForIdx(1), labelsForIdx(2)},
				{labelsForIdx(0), labelsForIdx(3)},
				{labelsForIdx(4), labelsForIdx(0), labelsForIdx(1)},
			},
			expectedMergedLabels: []labels.Labels{
				labelsForIdx(0),
				labelsForIdx(1),
				labelsForIdx(2),
				labelsForIdx(3),
				labelsForIdx(4),
			},
			expectedOutputSeries: []splitOrCacheOutputSeries{
				{sourceSeriesIndices: []int{0, 0, 1}},
				{sourceSeriesIndices: []int{1, -1, 2}},
				{sourceSeriesIndices: []int{2, -1, -1}},
				{sourceSeriesIndices: []int{-1, 1, -1}},
				{sourceSeriesIndices: []int{-1, -1, 0}},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equalf(t, len(testCase.expectedMergedLabels), len(testCase.expectedOutputSeries), "invalid test case: should have same number of output series in list of series labels and sources")

			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

			sources := newSplitRangeSources(testCase.sources, memoryConsumptionTracker)

			series, outputSeries, err := mergeSeriesMetadataFromMultipleSources(ctx, sources, nil, memoryConsumptionTracker)
			require.NoError(t, err)
			require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedMergedLabels), series, "expected merged series metadata to match expected")
			require.Equal(t, testCase.expectedOutputSeries, outputSeries, "expected output series to match expected")

			releaseMergeSeriesMetadata(&series, outputSeries, memoryConsumptionTracker)
			require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "expected all instances to be returned to pool, current memory consumption is:\n%v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
		})
	}
}

func TestMergeSeriesMetadata_SingleSourceFastPath(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	sourceLabels := []labels.Labels{labelsForIdx(0), labelsForIdx(1), labelsForIdx(2)}
	sources := newSplitRangeSources([][]labels.Labels{sourceLabels}, memoryConsumptionTracker)

	series, outputSeries, err := mergeSeriesMetadataFromMultipleSources(ctx, sources, nil, memoryConsumptionTracker)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(sourceLabels), series, "expected the sole source's series metadata to be returned unchanged")
	require.Nil(t, outputSeries, "expected the fast path to leave the output series mapping unpopulated")

	releaseMergeSeriesMetadata(&series, outputSeries, memoryConsumptionTracker)
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "expected all instances to be returned to pool, current memory consumption is:\n%v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
}

func TestMergeSeriesMetadata_ConflictingDropNameValuesForSameSeries(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	source1 := &operators.TestOperator{
		Series:                   []labels.Labels{labelsForIdx(0)},
		DropName:                 []bool{true},
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}

	source2 := &operators.TestOperator{
		Series:                   []labels.Labels{labelsForIdx(0)},
		DropName:                 []bool{false},
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}

	sources := []*splitRange{
		newSplitRange(source1, memoryConsumptionTracker),
		newSplitRange(source2, memoryConsumptionTracker),
	}

	_, _, err := mergeSeriesMetadataFromMultipleSources(ctx, sources, nil, memoryConsumptionTracker)
	require.EqualError(t, err, `series with labels {idx="0"} has conflicting drop name values in different ranges / extents`)
}

func TestMergeSeriesMetadata_PassesMatchersToSources(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	innerOperators := []*operators.TestOperator{
		{Series: []labels.Labels{labelsForIdx(0)}, MemoryConsumptionTracker: memoryConsumptionTracker},
		{Series: []labels.Labels{labelsForIdx(1)}, MemoryConsumptionTracker: memoryConsumptionTracker},
	}

	sources := []*splitRange{
		newSplitRange(innerOperators[0], memoryConsumptionTracker),
		newSplitRange(innerOperators[1], memoryConsumptionTracker),
	}

	matchers := types.Matchers{{Type: labels.MatchRegexp, Name: "idx", Value: "0|1"}}

	series, outputSeries, err := mergeSeriesMetadataFromMultipleSources(ctx, sources, matchers, memoryConsumptionTracker)
	require.NoError(t, err)

	for i, o := range innerOperators {
		require.Equalf(t, matchers, o.MatchersProvided, "expected matchers to be passed through to source %d", i)
	}

	releaseMergeSeriesMetadata(&series, outputSeries, memoryConsumptionTracker)
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "expected all instances to be returned to pool, current memory consumption is:\n%v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
}

func newSplitRangeSources(sources [][]labels.Labels, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) []*splitRange {
	ranges := make([]*splitRange, len(sources))
	for i, sourceLabels := range sources {
		o := &operators.TestOperator{
			Series:                   sourceLabels,
			MemoryConsumptionTracker: memoryConsumptionTracker,
		}
		ranges[i] = newSplitRange(o, memoryConsumptionTracker)
	}
	return ranges
}

func releaseMergeSeriesMetadata(series *[]types.SeriesMetadata, outputSeries []splitOrCacheOutputSeries, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	for i := range outputSeries {
		types.IntSlicePool.Put(&outputSeries[i].sourceSeriesIndices, memoryConsumptionTracker)
	}

	types.SeriesMetadataSlicePool.Put(series, memoryConsumptionTracker)
}

func labelsForIdx(idx int) labels.Labels {
	return labels.FromStrings("idx", strconv.Itoa(idx))
}
