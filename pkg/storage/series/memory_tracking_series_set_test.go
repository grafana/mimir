// SPDX-License-Identifier: AGPL-3.0-only

package series

import (
	"context"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestMemoryTrackingSeriesSet(t *testing.T) {
	series1 := &ConcreteSeries{
		labels:  labels.FromStrings("foo", "bar"),
		samples: []model.SamplePair{{Timestamp: 1, Value: 2}},
	}
	series2 := &ConcreteSeries{
		labels:  labels.FromStrings("foo", "baz"),
		samples: []model.SamplePair{{Timestamp: 3, Value: 4}},
	}
	series3 := &ConcreteSeries{
		labels:  labels.FromStrings("foo", "bay"),
		samples: []model.SamplePair{{Timestamp: 5, Value: 6}},
	}

	ctx := context.Background()
	tracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "test query")

	// Increase memory consumption for all series labels
	for _, s := range []storage.Series{series1, series2, series3} {
		err := tracker.IncreaseMemoryConsumptionForLabels(s.Labels())
		require.NoError(t, err)
	}

	innerSet := NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{series1, series2, series3})
	memTrackingSet := NewMemoryTrackingSeriesSet(innerSet, tracker)

	initialMemory := tracker.CurrentEstimatedMemoryConsumptionBytes()
	require.Greater(t, initialMemory, uint64(0), "memory consumption must be above 0 after initial increase above")

	// Iterate through series and verify memory is decreased
	require.True(t, memTrackingSet.Next())
	series := memTrackingSet.At()
	require.Equal(t, series1, series)
	memoryAfterFirst := tracker.CurrentEstimatedMemoryConsumptionBytes()
	require.Less(t, memoryAfterFirst, initialMemory)

	require.True(t, memTrackingSet.Next())
	series = memTrackingSet.At()
	require.Equal(t, series3, series)
	memoryAfterSecond := tracker.CurrentEstimatedMemoryConsumptionBytes()
	require.Less(t, memoryAfterSecond, memoryAfterFirst)

	require.True(t, memTrackingSet.Next())
	series = memTrackingSet.At()
	require.Equal(t, series2, series)
	memoryAfterThird := tracker.CurrentEstimatedMemoryConsumptionBytes()
	require.Less(t, memoryAfterThird, memoryAfterSecond)

	require.False(t, memTrackingSet.Next())

	require.Equal(t, uint64(0), tracker.CurrentEstimatedMemoryConsumptionBytes(), "memory consumption must be 0 at the end")
}

func TestMemoryTrackingSeriesSet_MultipleAtCalls(t *testing.T) {
	series1 := &ConcreteSeries{
		labels:  labels.FromStrings("foo", "bar"),
		samples: []model.SamplePair{{Timestamp: 1, Value: 2}},
	}
	series2 := &ConcreteSeries{
		labels:  labels.FromStrings("foo", "baz"),
		samples: []model.SamplePair{{Timestamp: 3, Value: 4}},
	}

	ctx := context.Background()
	tracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "test query")

	// Increase memory consumption for all series labels
	for _, s := range []storage.Series{series1, series2} {
		err := tracker.IncreaseMemoryConsumptionForLabels(s.Labels())
		require.NoError(t, err)
	}

	innerSet := NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{series1, series2})
	memTrackingSet := NewMemoryTrackingSeriesSet(innerSet, tracker)

	initialMemory := tracker.CurrentEstimatedMemoryConsumptionBytes()

	// First series
	require.True(t, memTrackingSet.Next())

	// Call At() multiple times - should only decrease memory once
	series := memTrackingSet.At()
	require.Equal(t, series1, series)
	memoryAfterFirstAt := tracker.CurrentEstimatedMemoryConsumptionBytes()
	require.Less(t, memoryAfterFirstAt, initialMemory)

	series = memTrackingSet.At()
	require.Equal(t, series1, series)
	memoryAfterSecondAt := tracker.CurrentEstimatedMemoryConsumptionBytes()
	require.Equal(t, memoryAfterFirstAt, memoryAfterSecondAt, "multiple At() calls should not decrease memory multiple times")

	series = memTrackingSet.At()
	require.Equal(t, series1, series)
	memoryAfterThirdAt := tracker.CurrentEstimatedMemoryConsumptionBytes()
	require.Equal(t, memoryAfterSecondAt, memoryAfterThirdAt, "multiple At() calls should not decrease memory multiple times")

	// Second series
	require.True(t, memTrackingSet.Next())
	series = memTrackingSet.At()
	require.Equal(t, series2, series)
	memoryAfterSecondSeries := tracker.CurrentEstimatedMemoryConsumptionBytes()
	require.Equal(t, uint64(0), memoryAfterSecondSeries)

	require.False(t, memTrackingSet.Next())
	require.Equal(t, uint64(0), tracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestMemoryTrackingSeriesSet_EmptySet(t *testing.T) {
	ctx := context.Background()
	tracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "test query")

	innerSet := NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{})
	memTrackingSet := NewMemoryTrackingSeriesSet(innerSet, tracker)

	require.False(t, memTrackingSet.Next())
	require.Equal(t, uint64(0), tracker.CurrentEstimatedMemoryConsumptionBytes())
}
