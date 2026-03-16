// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// Most of the functionality of the histogram operator is tested through the test scripts in
// pkg/streamingpromql/testdata.
//
// The output sorting behaviour is impossible to test through these scripts, so we instead test it here.

func TestHistogramFunction_ReturnsGroupsFinishedFirstEarliest(t *testing.T) {
	testCases := map[string]struct {
		inputSeries               []labels.Labels
		expectedOutputSeriesOrder []labels.Labels
	}{
		"empty input": {
			inputSeries:               []labels.Labels{},
			expectedOutputSeriesOrder: []labels.Labels{},
		},
		"classic histogram": {
			inputSeries: []labels.Labels{
				labels.FromStrings("__name__", "series", "le", "1"),
				labels.FromStrings("__name__", "series", "le", "2"),
				labels.FromStrings("__name__", "series", "le", "4"),
				labels.FromStrings("__name__", "series", "le", "8"),
				labels.FromStrings("__name__", "series", "le", "16"),
				labels.FromStrings("__name__", "series", "le", "+Inf"),
			},
			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("le", "1"),
				labels.FromStrings("le", "2"),
				labels.FromStrings("le", "4"),
				labels.FromStrings("le", "8"),
				labels.FromStrings("le", "16"),
				labels.EmptyLabels(),
				labels.FromStrings("le", "+Inf"),
			},
		},
		"series without le": {
			inputSeries: []labels.Labels{
				labels.FromStrings("__name__", "series", "abc", "1"),
				labels.FromStrings("__name__", "series", "abc", "2"),
			},
			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("abc", "1"),
				labels.FromStrings("abc", "2"),
			},
		},
		"classic histogram and series with same name": {
			inputSeries: []labels.Labels{
				labels.FromStrings("__name__", "series", "le", "1"),
				labels.FromStrings("__name__", "series", "le", "2"),
				labels.FromStrings("__name__", "series", "le", "4"),
				labels.FromStrings("__name__", "series", "le", "8"),
				labels.FromStrings("__name__", "series", "le", "16"),
				labels.FromStrings("__name__", "series", "le", "+Inf"),
				labels.FromStrings("__name__", "series"),
			},
			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("le", "1"),
				labels.FromStrings("le", "2"),
				labels.FromStrings("le", "4"),
				labels.FromStrings("le", "8"),
				labels.FromStrings("le", "16"),
				labels.FromStrings("le", "+Inf"),
				labels.EmptyLabels(),
			},
		},
		"multiple classic histograms with interleaved series": {
			inputSeries: []labels.Labels{
				labels.FromStrings("label", "A", "le", "1"),
				labels.FromStrings("label", "B", "le", "1"),
				labels.FromStrings("label", "A", "le", "2"),
				labels.FromStrings("label", "B", "le", "2"),
				labels.FromStrings("label", "A", "le", "4"),
				labels.FromStrings("label", "B", "le", "4"),
				labels.FromStrings("label", "A", "le", "8"),
				labels.FromStrings("label", "A", "le", "16"),
				labels.FromStrings("label", "A", "le", "+Inf"),
				labels.FromStrings("label", "B", "le", "8"),
				labels.FromStrings("label", "B", "le", "16"),
				labels.FromStrings("label", "B", "le", "+Inf"),
			},
			expectedOutputSeriesOrder: []labels.Labels{
				labels.FromStrings("label", "A", "le", "1"),
				labels.FromStrings("label", "B", "le", "1"),
				labels.FromStrings("label", "A", "le", "2"),
				labels.FromStrings("label", "B", "le", "2"),
				labels.FromStrings("label", "A", "le", "4"),
				labels.FromStrings("label", "B", "le", "4"),
				labels.FromStrings("label", "A", "le", "8"),
				labels.FromStrings("label", "A", "le", "16"),
				labels.FromStrings("label", "A"),
				labels.FromStrings("label", "A", "le", "+Inf"),
				labels.FromStrings("label", "B", "le", "8"),
				labels.FromStrings("label", "B", "le", "16"),
				labels.FromStrings("label", "B"),
				labels.FromStrings("label", "B", "le", "+Inf"),
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			hOp := &HistogramFunction{
				f: &histogramQuantile{
					phArg: &testScalarOperator{},
				},
				inner:                    &operators.TestOperator{Series: testCase.inputSeries, MemoryConsumptionTracker: memoryConsumptionTracker},
				innerSeriesMetricNames:   &operators.MetricNames{},
				memoryConsumptionTracker: memoryConsumptionTracker,
			}

			outputSeries, err := hOp.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeriesOrder), outputSeries)
		})
	}
}

// TestHistogramFunction_MemoryTracking verifies that seriesGroupPairs and remainingGroups
// are accounted for in the memory consumption tracker.
func TestHistogramFunction_MemoryTracking(t *testing.T) {
	ctx := context.Background()

	// A simple classic histogram: 4 bucket series + 1 implicit classic group = 5 groups total.
	inputSeries := []labels.Labels{
		labels.FromStrings("__name__", "series", "le", "0.1"),
		labels.FromStrings("__name__", "series", "le", "1"),
		labels.FromStrings("__name__", "series", "le", "10"),
		labels.FromStrings("__name__", "series", "le", "+Inf"),
	}

	newHistogramFunction := func(tracker *limiter.MemoryConsumptionTracker) *HistogramFunction {
		return &HistogramFunction{
			f: &histogramQuantile{
				phArg:                    &testScalarOperator{},
				memoryConsumptionTracker: tracker,
			},
			inner:                    &operators.TestOperator{Series: append([]labels.Labels{}, inputSeries...), MemoryConsumptionTracker: tracker},
			innerSeriesMetricNames:   &operators.MetricNames{},
			memoryConsumptionTracker: tracker,
		}
	}

	t.Run("SeriesGroupPairSlices and BucketGroupPointerSlices are tracked after SeriesMetadata", func(t *testing.T) {
		tracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
		hOp := newHistogramFunction(tracker)

		_, err := hOp.SeriesMetadata(ctx, nil)
		require.NoError(t, err)

		// seriesGroupPairs: pool rounds 4 up to 4 (already a power of two).
		expectedSGP := uint64(4) * seriesGroupPairSize
		require.Equal(t, expectedSGP, tracker.CurrentEstimatedMemoryConsumptionBytesBySource(limiter.SeriesGroupPairSlices),
			"seriesGroupPairs memory should be tracked")

		// remainingGroups: 4 native groups + 1 classic group = 5, each a pointer.
		expectedBGP := uint64(5) * bucketGroupPointerSize
		require.Equal(t, expectedBGP, tracker.CurrentEstimatedMemoryConsumptionBytesBySource(limiter.BucketGroupPointerSlices),
			"remainingGroups memory should be tracked")

		hOp.Close()
	})

	t.Run("SeriesGroupPairSlices and BucketGroupPointerSlices are released after Close", func(t *testing.T) {
		tracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
		hOp := newHistogramFunction(tracker)

		_, err := hOp.SeriesMetadata(ctx, nil)
		require.NoError(t, err)

		hOp.Close()

		require.Equal(t, uint64(0), tracker.CurrentEstimatedMemoryConsumptionBytesBySource(limiter.SeriesGroupPairSlices),
			"seriesGroupPairs memory should be released after Close")
		require.Equal(t, uint64(0), tracker.CurrentEstimatedMemoryConsumptionBytesBySource(limiter.BucketGroupPointerSlices),
			"remainingGroups memory should be released after Close")
	})

	t.Run("memory limit is enforced for seriesGroupPairs allocation", func(t *testing.T) {
		// Measure how many bytes the inner TestOperator's SeriesMetadata consumes.
		// We'll use this as the limit so that the very next allocation (seriesGroupPairPool.Get)
		// exceeds it.
		unlimited := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
		probe := &operators.TestOperator{Series: append([]labels.Labels{}, inputSeries...), MemoryConsumptionTracker: unlimited}
		_, err := probe.SeriesMetadata(ctx, nil)
		require.NoError(t, err)
		innerBytes := unlimited.CurrentEstimatedMemoryConsumptionBytes()

		// Set the limit to exactly the inner bytes consumed so the next allocation
		// (seriesGroupPairPool.Get inside HistogramFunction.SeriesMetadata) exceeds it.
		rejectionCounter := promauto.With(prometheus.NewRegistry()).NewCounter(prometheus.CounterOpts{Name: "test_rejections_total"})
		limited := limiter.NewMemoryConsumptionTracker(ctx, innerBytes, rejectionCounter, "test query")
		hOp := newHistogramFunction(limited)

		_, err = hOp.SeriesMetadata(ctx, nil)
		require.Error(t, err, "SeriesMetadata should fail when memory limit is too small for seriesGroupPairs")
		require.Contains(t, err.Error(), "query exceeded")

		// Close should not panic even after a failed SeriesMetadata.
		hOp.Close()
	})
}
