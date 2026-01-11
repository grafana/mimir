// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/limiter/query_limiter_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package limiter

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
)

func TestQueryLimiter_AddSeries_ShouldReturnNoErrorOnLimitNotExceeded(t *testing.T) {
	const (
		metricName = "test_metric"
	)

	var (
		series1 = labels.FromMap(map[string]string{
			model.MetricNameLabel: metricName + "_1",
			"series1":             "1",
		})
		series2 = labels.FromMap(map[string]string{
			model.MetricNameLabel: metricName + "_2",
			"series2":             "1",
		})
		series2NewInstance = labels.FromMap(map[string]string{
			model.MetricNameLabel: metricName + "_2",
			"series2":             "1",
		})
		reg     = prometheus.NewPedanticRegistry()
		limiter = NewQueryLimiter(100, 0, 0, 0, stats.NewQueryMetrics(reg))
	)

	memoryTracker := NewUnlimitedMemoryConsumptionTracker(context.Background())
	returnedSeries1, err := limiter.AddSeries(series1, memoryTracker)
	assert.NoError(t, err)
	requireSameLabels(t, returnedSeries1, series1)
	returnedSeries2, err := limiter.AddSeries(series2, memoryTracker)
	assert.NoError(t, err)
	requireSameLabels(t, returnedSeries2, series2)
	assert.Equal(t, 2, limiter.uniqueSeriesCount())
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)

	// Re-add previous series to make sure it's not double counted
	returnedSeries1Dup, err := limiter.AddSeries(series1, memoryTracker)
	assert.NoError(t, err)
	requireSameLabels(t, returnedSeries1Dup, series1)
	assert.Equal(t, 2, limiter.uniqueSeriesCount())
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)

	// Re-add previous series to make sure it's not double counted
	returnedSeries2Dup, err := limiter.AddSeries(series2, memoryTracker)
	assert.NoError(t, err)
	requireSameLabels(t, returnedSeries2Dup, series2)
	assert.Equal(t, 2, limiter.uniqueSeriesCount())
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)

	// Add different instance of series with same labels
	returnedSeries2NewInstance, err := limiter.AddSeries(series2NewInstance, memoryTracker)
	assert.NoError(t, err)
	requireSameLabels(t, returnedSeries2NewInstance, returnedSeries2Dup)
	requireSameLabels(t, returnedSeries2NewInstance, returnedSeries2)
	assert.Equal(t, 2, limiter.uniqueSeriesCount())
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)
}

func TestQueryLimiter_AddSeries_ShouldReturnErrorOnLimitExceeded(t *testing.T) {
	const (
		metricName = "test_metric"
	)

	var (
		series1 = labels.FromMap(map[string]string{
			model.MetricNameLabel: metricName + "_1",
			"series1":             "1",
		})
		series2 = labels.FromMap(map[string]string{
			model.MetricNameLabel: metricName + "_2",
			"series2":             "1",
		})
		series3 = labels.FromMap(map[string]string{
			model.MetricNameLabel: metricName + "_3",
			"series2":             "1",
		})
		series3NewInstance = labels.FromMap(map[string]string{
			model.MetricNameLabel: metricName + "_3",
			"series2":             "1",
		})
		reg     = prometheus.NewPedanticRegistry()
		limiter = NewQueryLimiter(1, 0, 0, 0, stats.NewQueryMetrics(reg))
	)
	memoryTracker := NewUnlimitedMemoryConsumptionTracker(context.Background())
	returnedSeries1, err := limiter.AddSeries(series1, memoryTracker)
	require.NoError(t, err)
	requireSameLabels(t, returnedSeries1, series1)
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)

	returnedSeries2, err := limiter.AddSeries(series2, memoryTracker)
	require.Error(t, err)
	// When limit is exceeded, empty labels are returned
	require.True(t, returnedSeries2.IsEmpty(), "should return empty labels on error")
	assertRejectedQueriesMetricValue(t, reg, 1, 0, 0, 0)

	// Add the same series again and ensure that we don't increment the failed queries metric again.
	returnedSeries2Again, err := limiter.AddSeries(series2, memoryTracker)
	require.Error(t, err)
	require.True(t, returnedSeries2Again.IsEmpty(), "should return empty labels on error")
	assertRejectedQueriesMetricValue(t, reg, 1, 0, 0, 0)

	// Add another series and ensure that we don't increment the failed queries metric again.
	returnedSeries3, err := limiter.AddSeries(series3, memoryTracker)
	require.Error(t, err)
	require.True(t, returnedSeries3.IsEmpty(), "should return empty labels on error")
	assertRejectedQueriesMetricValue(t, reg, 1, 0, 0, 0)

	// Add another series with duplicate labels and ensure that we don't increment the failed queries metric again.
	returnedSeries3NewInstance, err := limiter.AddSeries(series3NewInstance, memoryTracker)
	require.Error(t, err)
	require.True(t, returnedSeries3NewInstance.IsEmpty(), "should return empty labels on error")
	assertRejectedQueriesMetricValue(t, reg, 1, 0, 0, 0)
}

func TestQueryLimiter_AddChunkBytes(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	limiter := NewQueryLimiter(0, 100, 0, 0, stats.NewQueryMetrics(reg))

	err := limiter.AddChunkBytes(100)
	require.NoError(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)

	err = limiter.AddChunkBytes(1)
	require.Error(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 1, 0, 0)

	// Add more bytes and ensure that we don't increment the failed queries metric again.
	err = limiter.AddChunkBytes(2)
	require.Error(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 1, 0, 0)
}

func TestQueryLimiter_AddChunks_EnabledLimit(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	limiter := NewQueryLimiter(0, 0, 100, 0, stats.NewQueryMetrics(reg))

	err := limiter.AddChunks(100)
	require.NoError(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)

	err = limiter.AddChunks(1)
	require.Error(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 1, 0)

	// Add more chunks and ensure that we don't increment the failed queries metric again.
	err = limiter.AddChunks(0)
	require.Error(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 1, 0)

	err = limiter.AddChunks(2)
	require.Error(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 1, 0)
}

func TestQueryLimiter_AddChunks_IgnoresDisabledLimit(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	limiter := NewQueryLimiter(0, 0, 0, 0, stats.NewQueryMetrics(reg))

	err := limiter.AddChunks(100)
	require.NoError(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)
}

func TestQueryLimiter_AddEstimatedChunks_EnabledLimit(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	limiter := NewQueryLimiter(0, 0, 0, 100, stats.NewQueryMetrics(reg))

	err := limiter.AddEstimatedChunks(100)
	require.NoError(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)

	err = limiter.AddEstimatedChunks(1)
	require.Error(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 1)

	// Add more chunks and ensure that we don't increment the failed queries metric again.
	err = limiter.AddEstimatedChunks(0)
	require.Error(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 1)

	err = limiter.AddEstimatedChunks(2)
	require.Error(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 1)
}

func TestQueryLimiter_AddEstimatedChunks_IgnoresDisabledLimit(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	limiter := NewQueryLimiter(0, 0, 0, 0, stats.NewQueryMetrics(reg))

	err := limiter.AddEstimatedChunks(100)
	require.NoError(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)
}

func BenchmarkQueryLimiter_AddSeries(b *testing.B) {
	const (
		metricName = "test_metric"
	)
	var series []labels.Labels
	for i := 0; i < b.N; i++ {
		series = append(series,
			labels.FromMap(map[string]string{
				model.MetricNameLabel: metricName + "_1",
				"series1":             fmt.Sprint(i),
			}))
	}
	b.ResetTimer()

	reg := prometheus.NewPedanticRegistry()
	limiter := NewQueryLimiter(b.N+1, 0, 0, 0, stats.NewQueryMetrics(reg))
	memoryTracker := NewUnlimitedMemoryConsumptionTracker(context.Background())
	for _, s := range series {
		_, err := limiter.AddSeries(s, memoryTracker)
		assert.NoError(b, err)
	}
}

// BenchmarkQueryLimiter_AddSeries_WithCallerDedup_NoDuplicates benchmarks with all unique series
func BenchmarkQueryLimiter_AddSeries_WithCallerDedup_NoDuplicates(b *testing.B) {
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

	reg := prometheus.NewPedanticRegistry()
	limiter := NewQueryLimiter(totalSeries*2, 0, 0, 0, stats.NewQueryMetrics(reg))
	memoryTracker := NewUnlimitedMemoryConsumptionTracker(context.Background())

	for b.Loop() {
		for _, s := range series {
			_, _ = limiter.AddSeries(s, memoryTracker)
		}
	}
}

// BenchmarkQueryLimiter_AddSeries_WithCallerDedup_90pct benchmarks with 90% duplicates
func BenchmarkQueryLimiter_AddSeries_WithCallerDedup_90pct(b *testing.B) {
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

	reg := prometheus.NewPedanticRegistry()
	limiter := NewQueryLimiter(totalSeries, 0, 0, 0, stats.NewQueryMetrics(reg))
	memoryTracker := NewUnlimitedMemoryConsumptionTracker(context.Background())

	for b.Loop() {
		for _, s := range series {
			_, _ = limiter.AddSeries(s, memoryTracker)
		}
	}
}

// BenchmarkQueryLimiter_AddSeries_WithCallerDedup benchmarks caller-side deduplication with 50% duplicates
func BenchmarkQueryLimiter_AddSeries_WithCallerDedup_50pct(b *testing.B) {
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

	reg := prometheus.NewPedanticRegistry()
	limiter := NewQueryLimiter(totalSeries, 0, 0, 0, stats.NewQueryMetrics(reg))
	memoryTracker := NewUnlimitedMemoryConsumptionTracker(context.Background())

	for b.Loop() {
		for _, s := range series {
			_, _ = limiter.AddSeries(s, memoryTracker)
		}
	}
}

func assertRejectedQueriesMetricValue(t *testing.T, c prometheus.Collector, expectedMaxSeries, expectedMaxChunkBytes, expectedMaxChunks, expectedMaxEstimatedChunks int) {
	expected := fmt.Sprintf(`
		# HELP cortex_querier_queries_rejected_total Number of queries that were rejected, for example because they exceeded a limit.
		# TYPE cortex_querier_queries_rejected_total counter
		cortex_querier_queries_rejected_total{reason="max-fetched-series-per-query"} %v
		cortex_querier_queries_rejected_total{reason="max-fetched-chunk-bytes-per-query"} %v
		cortex_querier_queries_rejected_total{reason="max-fetched-chunks-per-query"} %v
		cortex_querier_queries_rejected_total{reason="max-estimated-fetched-chunks-per-query"} %v
		cortex_querier_queries_rejected_total{reason="max-estimated-memory-consumption-per-query"} 0
		`,
		expectedMaxSeries,
		expectedMaxChunkBytes,
		expectedMaxChunks,
		expectedMaxEstimatedChunks,
	)

	require.NoError(t, testutil.CollectAndCompare(c, bytes.NewBufferString(expected), "cortex_querier_queries_rejected_total"))
}
