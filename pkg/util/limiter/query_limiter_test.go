// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/limiter/query_limiter_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package limiter

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"unsafe"

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

	memoryTracker := NoopMemoryTracker{}
	returnedSeries1, err := limiter.AddSeries(series1, memoryTracker)
	assert.NoError(t, err)
	assertSameLabels(t, returnedSeries1, series1)
	returnedSeries2, err := limiter.AddSeries(series2, memoryTracker)
	assert.NoError(t, err)
	assertSameLabels(t, returnedSeries2, series2)
	assert.Equal(t, 2, limiter.uniqueSeriesCount())
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)

	// Re-add previous series to make sure it's not double counted
	returnedSeries1Dup, err := limiter.AddSeries(series1, memoryTracker)
	assert.NoError(t, err)
	assertSameLabels(t, returnedSeries1Dup, series1)
	assert.Equal(t, 2, limiter.uniqueSeriesCount())
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)

	// Re-add previous series to make sure it's not double counted
	returnedSeries2Dup, err := limiter.AddSeries(series2, memoryTracker)
	assert.NoError(t, err)
	assertSameLabels(t, returnedSeries2Dup, series2)
	assert.Equal(t, 2, limiter.uniqueSeriesCount())
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)

	// Add different instance of series with same labels
	returnedSeries2NewInstance, err := limiter.AddSeries(series2NewInstance, memoryTracker)
	assert.NoError(t, err)
	assertSameLabels(t, returnedSeries2NewInstance, returnedSeries2Dup)
	assertSameLabels(t, returnedSeries2NewInstance, returnedSeries2)
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
	memoryTracker := NoopMemoryTracker{}
	returnedSeries1, err := limiter.AddSeries(series1, memoryTracker)
	require.NoError(t, err)
	assertSameLabels(t, returnedSeries1, series1)
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
	for _, s := range series {
		_, err := limiter.AddSeries(s, NoopMemoryTracker{})
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

	for b.Loop() {
		// Simulate caller behavior: skip duplicates
		result := make([]labels.Labels, 0, totalSeries)
		for _, s := range series {
			uniqueSeriesLabels, _ := limiter.AddSeries(s, NoopMemoryTracker{})
			result = append(result, uniqueSeriesLabels)
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

	for b.Loop() {
		// Simulate caller behavior: skip duplicates
		result := make([]labels.Labels, 0, totalSeries)
		for _, s := range series {
			uniqueSeriesLabels, _ := limiter.AddSeries(s, NoopMemoryTracker{})
			result = append(result, uniqueSeriesLabels)
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

	for b.Loop() {
		// Simulate caller behavior: skip duplicates
		result := make([]labels.Labels, 0, totalSeries)
		for _, s := range series {
			uniqueSeriesLabels, _ := limiter.AddSeries(s, NoopMemoryTracker{})
			result = append(result, uniqueSeriesLabels)
		}
	}
}

// assertSameLabels checks if two labels.Labels share the same internal data.
// This is used to verify that AddSeries returns the same labels object for duplicates.
// This function is implementation-agnostic and works with:
// - stringlabels (default): stores labels as a single string with length prefixes
// - slicelabels (build with -tags slicelabels): stores labels as a slice of Label structs
// - dedupelabels (build with -tags dedupelabels): uses a shared SymbolTable
//
// To test with different implementations:
//
//	go test -tags slicelabels ./pkg/util/limiter/
//	go test -tags dedupelabels ./pkg/util/limiter/
func assertSameLabels(t *testing.T, a, b labels.Labels) {
	t.Helper()

	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)

	// Try stringlabels implementation (default)
	// stringlabels stores data in a "data" field of type string
	if aData := aVal.FieldByName("data"); aData.IsValid() && aData.Kind() == reflect.String {
		bData := bVal.FieldByName("data")
		if !bData.IsValid() || bData.Kind() != reflect.String {
			assert.Fail(t, "labels have different implementations")
			return
		}

		aStr := aData.String()
		bStr := bData.String()

		if len(aStr) == 0 && len(bStr) == 0 {
			// Both empty
			return
		}

		if len(aStr) > 0 && len(bStr) > 0 {
			// Compare string data pointers
			aPtr := unsafe.Pointer(unsafe.StringData(aStr))
			bPtr := unsafe.Pointer(unsafe.StringData(bStr))
			assert.Equal(t, aPtr, bPtr, "labels should share the same internal data pointer (stringlabels)")
			return
		}

		assert.Fail(t, "labels have different lengths")
		return
	}

	// Try slicelabels implementation (build with -tags slicelabels)
	// slicelabels stores data in a "labels" field which is a slice of Label
	if aSlice := aVal.FieldByName("labels"); aSlice.IsValid() && aSlice.Kind() == reflect.Slice {
		bSlice := bVal.FieldByName("labels")
		if !bSlice.IsValid() || bSlice.Kind() != reflect.Slice {
			assert.Fail(t, "labels have different implementations")
			return
		}

		if aSlice.Len() == 0 && bSlice.Len() == 0 {
			// Both empty
			return
		}

		if aSlice.Len() > 0 && bSlice.Len() > 0 {
			// Compare slice backing array pointers
			aPtr := aSlice.Pointer()
			bPtr := bSlice.Pointer()
			assert.Equal(t, aPtr, bPtr, "labels should share the same slice backing array (slicelabels)")
			return
		}

		assert.Fail(t, "labels have different lengths")
		return
	}

	// Try dedupelabels implementation (build with -tags dedupelabels)
	// dedupelabels uses a SymbolTable; check if there's a "symbolTable" field
	if aSymTable := aVal.FieldByName("symbolTable"); aSymTable.IsValid() {
		bSymTable := bVal.FieldByName("symbolTable")
		if !bSymTable.IsValid() {
			assert.Fail(t, "labels have different implementations")
			return
		}

		// For dedupelabels, we need to check if they reference the same entries in the symbol table
		// Get the "data" field which should contain indices into the symbol table
		if aData := aVal.FieldByName("data"); aData.IsValid() && aData.Kind() == reflect.Slice {
			bData := bVal.FieldByName("data")
			if bData.IsValid() && bData.Kind() == reflect.Slice {
				if aData.Len() == 0 && bData.Len() == 0 {
					return
				}
				if aData.Len() > 0 && bData.Len() > 0 {
					// Compare slice pointers
					aPtr := aData.Pointer()
					bPtr := bData.Pointer()
					assert.Equal(t, aPtr, bPtr, "labels should share the same data slice (dedupelabels)")
					return
				}
			}
		}
	}

	// Fallback: Unknown implementation or unable to determine
	// Just verify that the labels have equal content
	aBytes := a.Bytes(nil)
	bBytes := b.Bytes(nil)

	if len(aBytes) == 0 && len(bBytes) == 0 {
		return
	}

	// This fallback only checks content equality, not pointer equality
	// If we reach here, the test may pass even without proper deduplication
	assert.Equal(t, aBytes, bBytes, "labels should have equal content (unable to verify pointer equality for unknown implementation)")
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
