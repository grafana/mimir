//go:build requires_docker

// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/query_frontend_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
package integration

import (
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestQueryFrontendStatsFromResultsCacheShouldBeSameWhenWholeQueryIsCached(t *testing.T) {
	now := time.Now().Round(time.Second)
	queryFrontend, writeClient, queryClient := setupQueryFrontendSamplesStatsTest(t, queryFrontendCacheTestConfig{
		maxCacheFreshness:      10 * time.Minute,
		splitQueriesByInterval: 30 * time.Minute,
	})

	testCases := []struct {
		name                   string
		splitQueriesByInterval time.Duration
		query                  string
		queryStart             time.Time
		queryEnd               time.Time
		setupSeries            func(t *testing.T, writeClient *e2emimir.Client)
	}{
		{
			name:       "basic query",
			query:      "test_series{}",
			queryStart: now.Add(-20 * time.Minute),
			queryEnd:   now.Add(-15 * time.Minute),
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-20*time.Minute), now.Add(-15*time.Minute), "test_series")
			},
		},
		{
			name:       "sharded query",
			query:      "sum(test_sharded_series{})",
			queryStart: now.Add(-20 * time.Minute),
			queryEnd:   now.Add(-15 * time.Minute),
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-20*time.Minute), now.Add(-15*time.Minute), "test_sharded_series")
			},
		},
		{
			name:       "split query",
			query:      "test_split_series{}",
			queryStart: now.Add(-60 * time.Minute),
			queryEnd:   now.Add(-20 * time.Minute),
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-60*time.Minute), now.Add(-20*time.Minute), "test_split_series")
			},
		},
		{
			name:       "split and sharded query",
			query:      "sum(test_split_and_sharded_series{})",
			queryStart: now.Add(-60 * time.Minute),
			queryEnd:   now.Add(-20 * time.Minute),
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-60*time.Minute), now.Add(-20*time.Minute), "test_split_and_sharded_series")
			},
		},
		{
			name:       "series with gap before the end of the query range",
			query:      "test_gap_before_end_series",
			queryStart: now.Add(-60 * time.Minute),
			queryEnd:   now.Add(-20 * time.Minute),
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-60*time.Minute), now.Add(-30*time.Minute), "test_gap_in_front_series")
			},
		},
		{
			name:       "series with gap after the start of the query range",
			query:      "test_gap_after_start_series",
			queryStart: now.Add(-60 * time.Minute),
			queryEnd:   now.Add(-20 * time.Minute),
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-50*time.Minute), now.Add(-20*time.Minute), "test_gap_after_start_series")
			},
		},
		{
			name:       "series with gap inside of a query range",
			query:      "test_gap_inside_series",
			queryStart: now.Add(-60 * time.Minute),
			queryEnd:   now.Add(-20 * time.Minute),
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-60*time.Minute), now.Add(-50*time.Minute), "test_gap_inside_series")
				pushSeries(t, writeClient, now.Add(-40*time.Minute), now.Add(-20*time.Minute), "test_gap_inside_series")
			},
		},
		{
			name: "series with gap after the start, before the end and inside of a query range",

			query:      "test_gap_before_end_after_start_and_inside_series",
			queryStart: now.Add(-60 * time.Minute),
			queryEnd:   now.Add(-10 * time.Minute),
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-50*time.Minute), now.Add(-40*time.Minute), "test_gap_before_end_after_start_and_inside_series")
				pushSeries(t, writeClient, now.Add(-30*time.Minute), now.Add(-20*time.Minute), "test_gap_before_end_after_start_and_inside_series")
			},
		},
	}

	// Track baseline metrics to calculate deltas for each test case
	var samplesProcessedCacheAdjustedTotal, samplesProcessedExcludingCacheTotal float64

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup series for this test case.
			tc.setupSeries(t, writeClient)

			// Execute the first query (should hit datasource).
			_, err := queryClient.QueryRange(tc.query, tc.queryStart, tc.queryEnd, 1*time.Minute)
			require.NoError(t, err)
			values, err := queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_cache_adjusted_total"})
			require.NoError(t, err)
			samplesProcessedCacheAdjustedTotalAfterFirst := e2e.SumValues(values)
			values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_total"})
			require.NoError(t, err)
			samplesProcessedExcludingCacheTotalAfterFirst := e2e.SumValues(values)

			// Calculate deltas for the first query
			samplesProcessedCacheAdjustedDeltaFirst := samplesProcessedCacheAdjustedTotalAfterFirst - samplesProcessedCacheAdjustedTotal
			samplesProcessedExcludingCacheDeltaFirst := samplesProcessedExcludingCacheTotalAfterFirst - samplesProcessedExcludingCacheTotal
			require.Equal(t, samplesProcessedCacheAdjustedDeltaFirst, samplesProcessedExcludingCacheDeltaFirst, "first query should hit datasource only")

			// Execute the same query again (should hit cache).
			_, err = queryClient.QueryRange(tc.query, tc.queryStart, tc.queryEnd, 1*time.Minute)
			require.NoError(t, err)
			values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_cache_adjusted_total"})
			require.NoError(t, err)
			samplesProcessedCacheAdjustedTotalAfterSecond := e2e.SumValues(values)
			values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_total"})
			require.NoError(t, err)
			samplesProcessedExcludingCacheTotalAfterSecond := e2e.SumValues(values)

			// Calculate deltas for the second query
			samplesProcessedCacheAdjustedDeltaSecond := samplesProcessedCacheAdjustedTotalAfterSecond - samplesProcessedCacheAdjustedTotal
			samplesProcessedExcludingCacheDeltaSecond := samplesProcessedExcludingCacheTotalAfterSecond - samplesProcessedExcludingCacheTotal
			// Expect the same number of samples_processed when running an identical query a second time.
			require.Equal(t, samplesProcessedCacheAdjustedDeltaFirst*2, samplesProcessedCacheAdjustedDeltaSecond, "second query should process same amount of samples as first query")
			// The entire second query should be served from the cache, so cortex_query_samples_processed_total should remain unchanged.
			require.Equal(t, samplesProcessedExcludingCacheDeltaFirst, samplesProcessedExcludingCacheDeltaSecond, "second query should hit cache only")

			// Execute the same query third time.
			_, err = queryClient.QueryRange(tc.query, tc.queryStart, tc.queryEnd, 1*time.Minute)
			require.NoError(t, err)
			values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_cache_adjusted_total"})
			require.NoError(t, err)
			samplesProcessedCacheAdjustedTotalAfterThird := e2e.SumValues(values)
			values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_total"})
			require.NoError(t, err)
			samplesProcessedExcludingCacheTotalAfterThird := e2e.SumValues(values)

			// Calculate deltas for the third query
			samplesProcessedCacheAdjustedDeltaThird := samplesProcessedCacheAdjustedTotalAfterThird - samplesProcessedCacheAdjustedTotal
			samplesProcessedExcludingCacheDeltaThird := samplesProcessedExcludingCacheTotalAfterThird - samplesProcessedExcludingCacheTotal
			// Expect the same number of samples_processed when running an identical query a third time.
			require.Equal(t, samplesProcessedCacheAdjustedDeltaFirst*3, samplesProcessedCacheAdjustedDeltaThird, "third query should process same amount of samples as first query")
			// The entire third query should be served from the cache, so cortex_query_samples_processed_total should remain unchanged.
			require.Equal(t, samplesProcessedExcludingCacheDeltaFirst, samplesProcessedExcludingCacheDeltaThird, "third query should hit cache only")

			// Update baseline for next test case
			samplesProcessedCacheAdjustedTotal = samplesProcessedCacheAdjustedTotalAfterThird
			samplesProcessedExcludingCacheTotal = samplesProcessedExcludingCacheTotalAfterThird
		})
	}
}

func TestQueryFrontendStatsFromResultsCacheShouldBeSameWhenQueryHitMaxCacheFreshness(t *testing.T) {
	now := time.Now().Round(time.Second)

	queryFrontend, writeClient, queryClient := setupQueryFrontendSamplesStatsTest(t, queryFrontendCacheTestConfig{
		maxCacheFreshness:      10 * time.Minute,
		splitQueriesByInterval: 30 * time.Minute,
	})

	testCases := []struct {
		name                   string
		splitQueriesByInterval time.Duration
		query                  string
		setupSeries            func(t *testing.T, writeClient *e2emimir.Client)
		queryStart             time.Time
		queryEnd               time.Time
	}{
		{
			name:       "basic query",
			query:      "test_series{}",
			queryStart: now.Add(-20 * time.Minute),
			queryEnd:   now,
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-20*time.Minute), now, "test_series")
			},
		},
		{
			name:       "sharded query",
			query:      "sum(test_sharded_series{})",
			queryStart: now.Add(-20 * time.Minute),
			queryEnd:   now,
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-20*time.Minute), now, "test_sharded_series")
			},
		},
		{
			name:       "split query",
			query:      "test_split_series{}",
			queryStart: now.Add(-60 * time.Minute),
			queryEnd:   now,
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-60*time.Minute), now, "test_split_series")
			},
		},
		{
			name:       "split and sharded query",
			query:      "sum(test_split_and_sharded_series{})",
			queryStart: now.Add(-60 * time.Minute),
			queryEnd:   now,
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-60*time.Minute), now, "test_split_and_sharded_series")
			},
		},
		{
			name:       "series with gap before the end of the query range",
			query:      "test_gap_before_end_series",
			queryStart: now.Add(-60 * time.Minute),
			queryEnd:   now,
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-60*time.Minute), now.Add(-5*time.Minute), "test_gap_before_end_series")
			},
		},
		{
			name:       "series with gap after the start of the query range",
			query:      "test_gap_after_start_series",
			queryStart: now.Add(-60 * time.Minute),
			queryEnd:   now,
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-15*time.Minute), now, "test_gap_after_start_series")
			},
		},
		{
			name:       "series with gap inside of a query range",
			query:      "test_gap_inside_series",
			queryStart: now.Add(-60 * time.Minute),
			queryEnd:   now,
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-60*time.Minute), now.Add(-30*time.Minute), "test_gap_inside_series")
				pushSeries(t, writeClient, now.Add(-20*time.Minute), now, "test_gap_inside_series")
			},
		},
		{
			name:       "series with gap after the start, before the end and inside of a query range",
			query:      "test_gap_before_end_after_start_and_inside_series",
			queryStart: now.Add(-60 * time.Minute),
			queryEnd:   now.Add(-5 * time.Minute),
			setupSeries: func(t *testing.T, writeClient *e2emimir.Client) {
				pushSeries(t, writeClient, now.Add(-50*time.Minute), now.Add(-40*time.Minute), "test_gap_in_front_behind_and_inside_series")
				pushSeries(t, writeClient, now.Add(-30*time.Minute), now.Add(-10*time.Minute), "test_gap_in_front_behind_and_inside_series")
			},
		},
	}

	// Track baseline metrics to calculate deltas for each test case
	var samplesProcessedCacheAdjustedTotal, samplesProcessedExcludingCacheTotal float64

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup series for this test case.
			tc.setupSeries(t, writeClient)

			// Execute the first query (should hit datasource).
			_, err := queryClient.QueryRange(tc.query, tc.queryStart, tc.queryEnd, 1*time.Minute)
			require.NoError(t, err)
			values, err := queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_cache_adjusted_total"})
			require.NoError(t, err)
			samplesProcessedCacheAdjustedTotalAfterFirst := e2e.SumValues(values)
			values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_total"})
			require.NoError(t, err)
			samplesProcessedExcludingCacheTotalAfterFirst := e2e.SumValues(values)

			// Calculate deltas for the first query
			samplesProcessedCacheAdjustedDeltaFirst := samplesProcessedCacheAdjustedTotalAfterFirst - samplesProcessedCacheAdjustedTotal
			samplesProcessedExcludingCacheDeltaFirst := samplesProcessedExcludingCacheTotalAfterFirst - samplesProcessedExcludingCacheTotal
			require.Equal(t, samplesProcessedCacheAdjustedDeltaFirst, samplesProcessedExcludingCacheDeltaFirst, "first query should hit datasource only, but reported samples processed from cache")

			// Execute the same query again – only part of the query should be cached because of maxCacheFreshness.
			_, err = queryClient.QueryRange(tc.query, tc.queryStart, tc.queryEnd, 1*time.Minute)
			require.NoError(t, err)
			values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_cache_adjusted_total"})
			require.NoError(t, err)
			samplesProcessedCacheAdjustedTotalAfterSecond := e2e.SumValues(values)
			values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_total"})
			require.NoError(t, err)
			samplesProcessedExcludingCacheTotalAfterSecond := e2e.SumValues(values)

			// Calculate deltas for the second query
			samplesProcessedCacheAdjustedDeltaSecond := samplesProcessedCacheAdjustedTotalAfterSecond - samplesProcessedCacheAdjustedTotal
			samplesProcessedExcludingCacheDeltaSecond := samplesProcessedExcludingCacheTotalAfterSecond - samplesProcessedExcludingCacheTotal
			require.Equal(t, samplesProcessedCacheAdjustedDeltaFirst*2, samplesProcessedCacheAdjustedDeltaSecond, "second query should process same amount of samples as first query")
			require.Greater(t, samplesProcessedExcludingCacheDeltaSecond, samplesProcessedExcludingCacheDeltaFirst, "second query should hit datasource only for last maxCacheFreshness interval, but samples processed not from cache stayed the same")

			// Execute the same query third time – still only part of the query should be cached because of maxCacheFreshness.
			_, err = queryClient.QueryRange(tc.query, tc.queryStart, tc.queryEnd, 1*time.Minute)
			require.NoError(t, err)
			values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_cache_adjusted_total"})
			require.NoError(t, err)
			samplesProcessedCacheAdjustedTotalAfterThird := e2e.SumValues(values)
			values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_total"})
			require.NoError(t, err)
			samplesProcessedExcludingCacheTotalAfterThird := e2e.SumValues(values)

			// Calculate deltas for the third query
			samplesProcessedCacheAdjustedDeltaThird := samplesProcessedCacheAdjustedTotalAfterThird - samplesProcessedCacheAdjustedTotal
			samplesProcessedExcludingCacheDeltaThird := samplesProcessedExcludingCacheTotalAfterThird - samplesProcessedExcludingCacheTotal
			require.Equal(t, samplesProcessedCacheAdjustedDeltaFirst*3, samplesProcessedCacheAdjustedDeltaThird, "third query should process same amount of samples as first query")
			require.Greater(t, samplesProcessedExcludingCacheDeltaThird, samplesProcessedExcludingCacheDeltaSecond, "third query should hit datasource only for last maxCacheFreshness interval,  but samples processed not from cache stayed the same")

			// Update baseline for next test case
			samplesProcessedCacheAdjustedTotal = samplesProcessedCacheAdjustedTotalAfterThird
			samplesProcessedExcludingCacheTotal = samplesProcessedExcludingCacheTotalAfterThird
		})
	}
}

func TestQueryFrontendStatsFromResultsCacheShouldBeSameWhenZoomInQueryRange(t *testing.T) {
	now := time.Now().Round(time.Second)

	queryFrontend, writeClient, queryClient := setupQueryFrontendSamplesStatsTest(t, queryFrontendCacheTestConfig{
		maxCacheFreshness:      10 * time.Minute,
		splitQueriesByInterval: 24 * time.Hour,
	})

	// Setup series for this test case.
	// It's end is not within maxCacheFreshness interval to simplify test logic.
	pushSeries(t, writeClient, now.Add(-60*time.Minute), now.Add(-20*time.Minute), "test_series")

	query := "test_series{}"
	start := now.Add(-60 * time.Minute)
	end := now.Add(-20 * time.Minute)
	// Execute the first query (should hit datasource).
	_, err := queryClient.QueryRange(query, start, end, 1*time.Minute)
	require.NoError(t, err)
	values, err := queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_cache_adjusted_total"})
	require.NoError(t, err)
	sampleProcessedCacheAdjustedFirst := e2e.SumValues(values)
	values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_total"})
	require.NoError(t, err)
	sampleProcessedExcludingCacheFirst := e2e.SumValues(values)
	require.Equal(t, sampleProcessedCacheAdjustedFirst, sampleProcessedExcludingCacheFirst, "first query should hit datasource only")

	// "Zoom in" the query range
	_, err = queryClient.QueryRange(query, now.Add(-50*time.Minute), now.Add(-40*time.Minute), 1*time.Minute)
	require.NoError(t, err)
	values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_cache_adjusted_total"})
	require.NoError(t, err)
	sampleProcessedCacheAdjustedSecond := e2e.SumValues(values)
	values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_total"})
	require.NoError(t, err)
	sampleProcessedExcludingCacheSecond := e2e.SumValues(values)
	require.Equal(t, sampleProcessedExcludingCacheFirst, sampleProcessedExcludingCacheSecond, "query in the subrange of a first query should hit only cache")

	// "Zoom out" back to the original query range
	_, err = queryClient.QueryRange(query, start, end, 1*time.Minute)
	require.NoError(t, err)
	values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_cache_adjusted_total"})
	require.NoError(t, err)
	sampleProcessedCacheAdjustedThird := e2e.SumValues(values)
	values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_total"})
	require.NoError(t, err)
	sampleProcessedExcludingCacheThird := e2e.SumValues(values)
	// subtract second metric value from third to get number only for the third query
	expectedSamplesProcesedCacheAdjustedThird := sampleProcessedCacheAdjustedThird - sampleProcessedCacheAdjustedSecond
	require.Equal(t, sampleProcessedCacheAdjustedFirst, expectedSamplesProcesedCacheAdjustedThird, "second query in same range should report same amount of samples as first query")
	require.Equal(t, sampleProcessedExcludingCacheFirst, sampleProcessedExcludingCacheThird, "second query in same range should hit only cache")
}

func TestQueryFrontendStatsFromResultsCacheShouldBeSameWhenZoomOutQueryRange(t *testing.T) {
	now := time.Now().Round(time.Second)

	queryFrontend, writeClient, queryClient := setupQueryFrontendSamplesStatsTest(t, queryFrontendCacheTestConfig{
		maxCacheFreshness:      10 * time.Minute,
		splitQueriesByInterval: 24 * time.Hour,
	})

	// Setup series for this test case.
	// It's end is not within maxCacheFreshness to simplify test logic.
	pushSeries(t, writeClient, now.Add(-60*time.Minute), now.Add(-20*time.Minute), "test_series")

	query := "test_series{}"
	smallStart := now.Add(-50 * time.Minute)
	smallEnd := now.Add(-40 * time.Minute)
	largeStart := now.Add(-60 * time.Minute)
	largeEnd := now.Add(-20 * time.Minute)

	// Execute the first query with smaller range (should hit datasource).
	_, err := queryClient.QueryRange(query, smallStart, smallEnd, 1*time.Minute)
	require.NoError(t, err)
	values, err := queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_cache_adjusted_total"})
	require.NoError(t, err)
	sampleProcessedCacheAdjustedFirst := e2e.SumValues(values)
	values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_total"})
	require.NoError(t, err)
	sampleProcessedExcludingCacheFirst := e2e.SumValues(values)
	require.Equal(t, sampleProcessedCacheAdjustedFirst, sampleProcessedExcludingCacheFirst, "first query should hit datasource only")

	// "Zoom out" to a larger query range that encompasses the first range
	_, err = queryClient.QueryRange(query, largeStart, largeEnd, 1*time.Minute)
	require.NoError(t, err)
	values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_cache_adjusted_total"})
	require.NoError(t, err)
	sampleProcessedCacheAdjustedSecond := e2e.SumValues(values)
	values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_total"})
	require.NoError(t, err)
	sampleProcessedExcludingCacheSecond := e2e.SumValues(values)
	// The cached portion should be served from cache, but additional data outside the cached range should hit datasource
	require.Greater(t, sampleProcessedExcludingCacheSecond, sampleProcessedExcludingCacheFirst, "zoom out query should hit datasource for uncached portions")

	// "Zoom back in" to the original smaller query range
	_, err = queryClient.QueryRange(query, smallStart, smallEnd, 1*time.Minute)
	require.NoError(t, err)
	values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_cache_adjusted_total"})
	require.NoError(t, err)
	sampleProcessedCacheAdjustedThird := e2e.SumValues(values)
	values, err = queryFrontend.SumMetrics([]string{"cortex_query_samples_processed_total"})
	require.NoError(t, err)
	sampleProcessedExcludingCacheThird := e2e.SumValues(values)
	// subtract second metric value from third to get number only for the third query
	expectedSamplesProcesedCacheAdjustedThird := sampleProcessedCacheAdjustedThird - sampleProcessedCacheAdjustedSecond
	require.Equal(t, sampleProcessedCacheAdjustedFirst, expectedSamplesProcesedCacheAdjustedThird, "third query in original small range should report same amount of samples as first query")
	require.Equal(t, sampleProcessedExcludingCacheSecond, sampleProcessedExcludingCacheThird, "third query should hit only cache")
}

func generateSeriesWithManySamples(name string, start time.Time, step time.Duration, end time.Time) []prompb.TimeSeries {
	var samples []prompb.Sample

	current := start
	i := 0
	for current.Before(end) || current.Equal(end) {
		samples = append(samples, prompb.Sample{
			Timestamp: current.UnixMilli(),
			Value:     float64(i),
		})
		current = current.Add(step)
		i++
	}

	return []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{
					Name:  "__name__",
					Value: name,
				},
			},
			Samples: samples,
		},
	}
}

// pushSeries creates and pushes test series data
func pushSeries(t *testing.T, writeClient *e2emimir.Client, seriesTime, seriesEnd time.Time, name string) {
	series := generateSeriesWithManySamples(name, seriesTime, 1*time.Minute, seriesEnd)
	res, err := writeClient.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
}

type queryFrontendCacheTestConfig struct {
	maxCacheFreshness      time.Duration
	splitQueriesByInterval time.Duration
}

func setupQueryFrontendSamplesStatsTest(t *testing.T, config queryFrontendCacheTestConfig) (*e2emimir.MimirService, *e2emimir.Client, *e2emimir.Client) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)

	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	const configFile = ""
	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
	)

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	flags = mergeFlags(flags, map[string]string{
		"-log.level":                                        "info",
		"-query-frontend.cache-results":                     "true",
		"-query-frontend.results-cache.backend":             "memcached",
		"-query-frontend.results-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-query-frontend.parallelize-shardable-queries":     "true",
		"-query-frontend.split-queries-by-interval":         config.splitQueriesByInterval.String(),
		"-query-frontend.max-cache-freshness":               config.maxCacheFreshness.String(),
		"-query-frontend.align-queries-with-step":           "true", // to make sure we hit the cache.
		"-query-frontend.cache-samples-processed-stats":     "true", // to collect and cache per-step stats.
		// Default block-ranges-period for integration tests is 1m.
		// Set it to 2h to avoid getting err-mimir-sample-timestamp-too-old, so all series will be in the head block.
		"-blocks-storage.tsdb.block-ranges-period": "2h",
	})

	// Start the query-scheduler
	queryScheduler := e2emimir.NewQueryScheduler("query-scheduler", flags)
	require.NoError(t, s.StartAndWaitReady(queryScheduler))
	flags["-query-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()

	// Start the query-frontend.
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	require.NoError(t, s.Start(queryFrontend))

	// Start all other services.
	ingester := e2emimir.NewIngester("ingester-0", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))

	require.NoError(t, s.StartAndWaitReady(querier, ingester, distributor))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Check if we're discovering memcached or not.
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "thanos_cache_dns_provider_results"))
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Greater(0), "thanos_cache_dns_lookups_total"))

	// Wait until distributor and querier have updated the ingesters ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Create clients.
	writeClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	queryClient, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	return queryFrontend, writeClient, queryClient
}
