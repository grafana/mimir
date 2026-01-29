// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestSeriesDeduplicator_Deduplicate_HashCollision(t *testing.T) {
	// This test uses a custom hash function to force hash collisions and verify the collision handling code.
	const collisionHash = uint64(12345)

	deduplicator := NewSeriesDeduplicator()
	memoryTracker := NewUnlimitedMemoryConsumptionTracker(context.Background())

	seriesA := labels.FromMap(map[string]string{
		model.MetricNameLabel: "metric_a",
		"label":               "a",
	})
	seriesB := labels.FromMap(map[string]string{
		model.MetricNameLabel: "metric_b",
		"label":               "b",
	})
	seriesC := labels.FromMap(map[string]string{
		model.MetricNameLabel: "metric_c",
		"label":               "c",
	})

	// Override hash function to force collisions for seriesA and seriesB (but not seriesC)
	deduplicator.hashFunc = func(l labels.Labels) uint64 {
		if labels.Equal(l, seriesA) || labels.Equal(l, seriesB) {
			return collisionHash
		}
		return l.Hash()
	}

	// Add seriesA - should go into uniqueSeries
	returnedA1, err := deduplicator.Deduplicate(seriesA, memoryTracker)
	require.NoError(t, err)
	requireSameLabels(t, returnedA1, seriesA)
	require.Equal(t, 1, len(deduplicator.uniqueSeries))
	require.Nil(t, deduplicator.conflictSeries, "conflictSeries should not be initialized yet")
	require.Equal(t, 1, deduplicator.SeriesCount())

	// Add seriesB - should collide with seriesA and go into conflictSeries
	returnedB1, err := deduplicator.Deduplicate(seriesB, memoryTracker)
	require.NoError(t, err)
	requireSameLabels(t, returnedB1, seriesB)
	require.Equal(t, 1, len(deduplicator.uniqueSeries), "uniqueSeries should still have only seriesA")
	require.NotNil(t, deduplicator.conflictSeries, "conflictSeries should now be initialized")
	require.Equal(t, 1, len(deduplicator.conflictSeries[collisionHash]), "should have one collision for this hash")
	require.Equal(t, 2, deduplicator.SeriesCount())

	// Add duplicate of seriesA - should deduplicate correctly
	returnedA2, err := deduplicator.Deduplicate(seriesA, memoryTracker)
	require.NoError(t, err)
	requireSameLabels(t, returnedA2, returnedA1)
	require.Equal(t, 2, deduplicator.SeriesCount())

	// Add duplicate of seriesB - should deduplicate from conflictSeries
	returnedB2, err := deduplicator.Deduplicate(seriesB, memoryTracker)
	require.NoError(t, err)
	requireSameLabels(t, returnedB2, returnedB1)
	require.Equal(t, 2, deduplicator.SeriesCount())

	// Add seriesC (no collision) - should go into uniqueSeries normally
	returnedC1, err := deduplicator.Deduplicate(seriesC, memoryTracker)
	require.NoError(t, err)
	requireSameLabels(t, returnedC1, seriesC)
	require.Equal(t, 2, len(deduplicator.uniqueSeries), "uniqueSeries should now have seriesA and seriesC")
	require.Equal(t, 3, deduplicator.SeriesCount())
}

func TestSeriesDeduplicator_Deduplicate_HashCollisionWithThreeCollidingSeries(t *testing.T) {
	// Test that multiple series can collide on the same hash
	const collisionHash = uint64(12345)

	deduplicator := NewSeriesDeduplicator()
	memoryTracker := NewUnlimitedMemoryConsumptionTracker(context.Background())

	seriesA := labels.FromMap(map[string]string{
		model.MetricNameLabel: "metric_a",
		"label":               "a",
	})
	seriesB := labels.FromMap(map[string]string{
		model.MetricNameLabel: "metric_b",
		"label":               "b",
	})
	seriesC := labels.FromMap(map[string]string{
		model.MetricNameLabel: "metric_c",
		"label":               "c",
	})

	// Force all three series to collide
	deduplicator.hashFunc = func(l labels.Labels) uint64 {
		return collisionHash
	}

	// Add all three series
	returnedA, err := deduplicator.Deduplicate(seriesA, memoryTracker)
	require.NoError(t, err)
	requireSameLabels(t, returnedA, seriesA)
	require.Equal(t, 1, len(deduplicator.uniqueSeries))
	require.Nil(t, deduplicator.conflictSeries)

	returnedB, err := deduplicator.Deduplicate(seriesB, memoryTracker)
	require.NoError(t, err)
	requireSameLabels(t, returnedB, seriesB)
	require.Equal(t, 1, len(deduplicator.uniqueSeries))
	require.Equal(t, 1, len(deduplicator.conflictSeries[collisionHash]))

	returnedC, err := deduplicator.Deduplicate(seriesC, memoryTracker)
	require.NoError(t, err)
	requireSameLabels(t, returnedC, seriesC)
	require.Equal(t, 1, len(deduplicator.uniqueSeries))
	require.Equal(t, 2, len(deduplicator.conflictSeries[collisionHash]), "both seriesB and seriesC should be in conflictSeries")
	require.Equal(t, 3, deduplicator.SeriesCount())

	// Verify deduplication works for all three
	returnedA2, err := deduplicator.Deduplicate(seriesA, memoryTracker)
	require.NoError(t, err)
	requireSameLabels(t, returnedA2, returnedA)

	returnedB2, err := deduplicator.Deduplicate(seriesB, memoryTracker)
	require.NoError(t, err)
	requireSameLabels(t, returnedB2, returnedB)

	returnedC2, err := deduplicator.Deduplicate(seriesC, memoryTracker)
	require.NoError(t, err)
	requireSameLabels(t, returnedC2, returnedC)

	require.Equal(t, 3, deduplicator.SeriesCount())
}

func TestSeriesDeduplicator_Deduplicate_MemoryTrackingWithDuplicates(t *testing.T) {
	// Test that memory tracking correctly avoids double-counting for duplicate series
	deduplicator := NewSeriesDeduplicator()

	ctx := context.Background()
	memoryTracker := NewMemoryConsumptionTracker(ctx, 1000000, nil, "test")

	seriesA := labels.FromMap(map[string]string{
		model.MetricNameLabel: "metric_a",
		"label":               "value_a",
	})
	seriesB := labels.FromMap(map[string]string{
		model.MetricNameLabel: "metric_b",
		"label":               "value_b",
	})

	initialMemory := memoryTracker.CurrentEstimatedMemoryConsumptionBytes()

	// Add seriesA - memory should increase
	_, err := deduplicator.Deduplicate(seriesA, memoryTracker)
	require.NoError(t, err)
	afterFirstAdd := memoryTracker.CurrentEstimatedMemoryConsumptionBytes()
	require.Greater(t, afterFirstAdd, initialMemory, "Memory should increase after adding first series")

	// Add duplicate of seriesA - memory should NOT increase
	_, err = deduplicator.Deduplicate(seriesA, memoryTracker)
	require.NoError(t, err)
	afterDuplicateA := memoryTracker.CurrentEstimatedMemoryConsumptionBytes()
	require.Equal(t, afterFirstAdd, afterDuplicateA, "Memory should not increase for duplicate series")

	// Add seriesB - memory should increase
	_, err = deduplicator.Deduplicate(seriesB, memoryTracker)
	require.NoError(t, err)
	afterSecondAdd := memoryTracker.CurrentEstimatedMemoryConsumptionBytes()
	require.Greater(t, afterSecondAdd, afterDuplicateA, "Memory should increase after adding second series")

	// Add duplicate of seriesB - memory should NOT increase
	_, err = deduplicator.Deduplicate(seriesB, memoryTracker)
	require.NoError(t, err)
	afterDuplicateB := memoryTracker.CurrentEstimatedMemoryConsumptionBytes()
	require.Equal(t, afterSecondAdd, afterDuplicateB, "Memory should not increase for duplicate series")
}

func BenchmarkSeriesDeduplicator_Deduplicate_WithCallerDedup_NoDuplicates(b *testing.B) {
	const (
		metricName  = "test_metric"
		totalSeries = 1000
	)

	// Create all unique series
	series := make([]labels.Labels, 0, totalSeries)
	for i := 0; i < totalSeries; i++ {
		series = append(series, labels.FromMap(map[string]string{
			model.MetricNameLabel: metricName,
			"series":              fmt.Sprint(i),
		}))
	}

	b.ResetTimer()
	b.ReportAllocs()

	deduplicator := NewSeriesDeduplicator()
	memoryTracker := NewUnlimitedMemoryConsumptionTracker(context.Background())

	for b.Loop() {
		for _, s := range series {
			_, _ = deduplicator.Deduplicate(s, memoryTracker)
		}
	}
}

func BenchmarkSeriesDeduplicator_Deduplicate_WithCallerDedup_90pct(b *testing.B) {
	const (
		metricName   = "test_metric"
		uniqueSeries = 100
		totalSeries  = 1000 // 90% duplicates
	)

	// Create few unique series
	uniqueSet := make([]labels.Labels, 0, uniqueSeries)
	for i := 0; i < uniqueSeries; i++ {
		uniqueSet = append(uniqueSet, labels.FromMap(map[string]string{
			model.MetricNameLabel: metricName,
			"series":              fmt.Sprint(i),
		}))
	}

	// Build series with many duplicates
	series := make([]labels.Labels, 0, totalSeries)
	for i := 0; i < totalSeries; i++ {
		series = append(series, uniqueSet[i%uniqueSeries])
	}

	b.ResetTimer()
	b.ReportAllocs()

	deduplicator := NewSeriesDeduplicator()
	memoryTracker := NewUnlimitedMemoryConsumptionTracker(context.Background())

	for b.Loop() {
		for _, s := range series {
			_, _ = deduplicator.Deduplicate(s, memoryTracker)
		}
	}
}

func BenchmarkSeriesDeduplicator_Deduplicate_WithCallerDedup_50pct(b *testing.B) {
	const (
		metricName   = "test_metric"
		uniqueSeries = 500
		totalSeries  = 1000 // 50% duplicates
	)

	// Create unique series
	uniqueSet := make([]labels.Labels, 0, uniqueSeries)
	for i := 0; i < uniqueSeries; i++ {
		uniqueSet = append(uniqueSet, labels.FromMap(map[string]string{
			model.MetricNameLabel: metricName,
			"series":              fmt.Sprint(i),
		}))
	}

	// Create series array with duplicates
	series := make([]labels.Labels, 0, totalSeries)
	for i := 0; i < totalSeries; i++ {
		series = append(series, uniqueSet[i%uniqueSeries])
	}

	b.ReportAllocs()

	deduplicator := NewSeriesDeduplicator()
	memoryTracker := NewUnlimitedMemoryConsumptionTracker(context.Background())

	for b.Loop() {
		for _, s := range series {
			_, _ = deduplicator.Deduplicate(s, memoryTracker)
		}
	}
}
