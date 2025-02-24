// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/limiter/query_limiter_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package limiter

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
			labels.MetricName: metricName + "_1",
			"series1":         "1",
		})
		series2 = labels.FromMap(map[string]string{
			labels.MetricName: metricName + "_2",
			"series2":         "1",
		})
		reg     = prometheus.NewPedanticRegistry()
		limiter = NewQueryLimiter(100, 0, 0, 0, stats.NewQueryMetrics(reg))
	)
	err := limiter.AddSeries(series1)
	assert.NoError(t, err)
	err = limiter.AddSeries(series2)
	assert.NoError(t, err)
	assert.Equal(t, 2, limiter.uniqueSeriesCount())
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)

	// Re-add previous series to make sure it's not double counted
	err = limiter.AddSeries(series1)
	assert.NoError(t, err)
	assert.Equal(t, 2, limiter.uniqueSeriesCount())
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)
}

func TestQueryLimiter_AddSeries_ShouldReturnErrorOnLimitExceeded(t *testing.T) {
	const (
		metricName = "test_metric"
	)

	var (
		series1 = labels.FromMap(map[string]string{
			labels.MetricName: metricName + "_1",
			"series1":         "1",
		})
		series2 = labels.FromMap(map[string]string{
			labels.MetricName: metricName + "_2",
			"series2":         "1",
		})
		series3 = labels.FromMap(map[string]string{
			labels.MetricName: metricName + "_3",
			"series2":         "1",
		})
		reg     = prometheus.NewPedanticRegistry()
		limiter = NewQueryLimiter(1, 0, 0, 0, stats.NewQueryMetrics(reg))
	)
	err := limiter.AddSeries(series1)
	require.NoError(t, err)
	assertRejectedQueriesMetricValue(t, reg, 0, 0, 0, 0)

	err = limiter.AddSeries(series2)
	require.Error(t, err)
	assertRejectedQueriesMetricValue(t, reg, 1, 0, 0, 0)

	// Add the same series again and ensure that we don't increment the failed queries metric again.
	err = limiter.AddSeries(series2)
	require.Error(t, err)
	assertRejectedQueriesMetricValue(t, reg, 1, 0, 0, 0)

	// Add another series and ensure that we don't increment the failed queries metric again.
	err = limiter.AddSeries(series3)
	require.Error(t, err)
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
				labels.MetricName: metricName + "_1",
				"series1":         fmt.Sprint(i),
			}))
	}
	b.ResetTimer()

	reg := prometheus.NewPedanticRegistry()
	limiter := NewQueryLimiter(b.N+1, 0, 0, 0, stats.NewQueryMetrics(reg))
	for _, s := range series {
		err := limiter.AddSeries(s)
		assert.NoError(b, err)
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
