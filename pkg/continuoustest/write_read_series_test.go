// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestWriteReadSeriesTest_Run(t *testing.T) {
	logger := log.NewNopLogger()
	cfg := WriteReadSeriesTestConfig{}
	flagext.DefaultValues(&cfg)
	cfg.NumSeries = 2

	t.Run("should write series with current timestamp if it's already aligned to write interval", func(t *testing.T) {
		client := &ClientMock{}
		client.On("WriteSeries", mock.Anything, mock.Anything).Return(200, nil)
		client.On("QueryRange", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{}, nil)
		client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(model.Vector{}, nil)

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadSeriesTest(cfg, client, logger, reg)

		now := time.Unix(1000, 0)
		test.Run(context.Background(), now)

		client.AssertNumberOfCalls(t, "WriteSeries", 1)
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, now, 2))
		assert.Equal(t, int64(1000), test.lastWrittenTimestamp.Unix())

		client.AssertNumberOfCalls(t, "QueryRange", 2)
		client.AssertCalled(t, "QueryRange", mock.Anything, "sum(mimir_continuous_test_sine_wave)", time.Unix(1000, 0), time.Unix(1000, 0), writeInterval)

		client.AssertNumberOfCalls(t, "Query", 2)
		client.AssertCalled(t, "Query", mock.Anything, "sum(mimir_continuous_test_sine_wave)", time.Unix(1000, 0))

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP mimir_continuous_test_writes_total Total number of attempted write requests.
			# TYPE mimir_continuous_test_writes_total counter
			mimir_continuous_test_writes_total{test="write-read-series"} 1

			# HELP mimir_continuous_test_queries_total Total number of attempted query requests.
			# TYPE mimir_continuous_test_queries_total counter
			mimir_continuous_test_queries_total{test="write-read-series"} 4

			# HELP mimir_continuous_test_queries_failed_total Total number of failed query requests.
			# TYPE mimir_continuous_test_queries_failed_total counter
			mimir_continuous_test_queries_failed_total{test="write-read-series"} 0
		`),
			"mimir_continuous_test_writes_total", "mimir_continuous_test_writes_failed_total",
			"mimir_continuous_test_queries_total", "mimir_continuous_test_queries_failed_total"))
	})

	t.Run("should write series with timestamp aligned to write interval", func(t *testing.T) {
		client := &ClientMock{}
		client.On("WriteSeries", mock.Anything, mock.Anything).Return(200, nil)
		client.On("QueryRange", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{}, nil)
		client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(model.Vector{}, nil)

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadSeriesTest(cfg, client, logger, reg)

		now := time.Unix(999, 0)
		test.Run(context.Background(), now)

		client.AssertNumberOfCalls(t, "WriteSeries", 1)
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, time.Unix(980, 0), 2))
		assert.Equal(t, int64(980), test.lastWrittenTimestamp.Unix())

		client.AssertNumberOfCalls(t, "QueryRange", 2)
		client.AssertCalled(t, "QueryRange", mock.Anything, "sum(mimir_continuous_test_sine_wave)", time.Unix(980, 0), time.Unix(980, 0), writeInterval)

		client.AssertNumberOfCalls(t, "Query", 2)
		client.AssertCalled(t, "Query", mock.Anything, "sum(mimir_continuous_test_sine_wave)", time.Unix(980, 0))

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP mimir_continuous_test_writes_total Total number of attempted write requests.
			# TYPE mimir_continuous_test_writes_total counter
			mimir_continuous_test_writes_total{test="write-read-series"} 1

			# HELP mimir_continuous_test_queries_total Total number of attempted query requests.
			# TYPE mimir_continuous_test_queries_total counter
			mimir_continuous_test_queries_total{test="write-read-series"} 4

			# HELP mimir_continuous_test_queries_failed_total Total number of failed query requests.
			# TYPE mimir_continuous_test_queries_failed_total counter
			mimir_continuous_test_queries_failed_total{test="write-read-series"} 0
		`),
			"mimir_continuous_test_writes_total", "mimir_continuous_test_writes_failed_total",
			"mimir_continuous_test_queries_total", "mimir_continuous_test_queries_failed_total"))
	})

	t.Run("should write series from last written timestamp until now", func(t *testing.T) {
		client := &ClientMock{}
		client.On("WriteSeries", mock.Anything, mock.Anything).Return(200, nil)
		client.On("QueryRange", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{}, nil)
		client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(model.Vector{}, nil)

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadSeriesTest(cfg, client, logger, reg)

		test.lastWrittenTimestamp = time.Unix(940, 0)
		now := time.Unix(1000, 0)
		test.Run(context.Background(), now)

		client.AssertNumberOfCalls(t, "WriteSeries", 3)
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, time.Unix(960, 0), 2))
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, time.Unix(980, 0), 2))
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, time.Unix(1000, 0), 2))
		assert.Equal(t, int64(1000), test.lastWrittenTimestamp.Unix())

		client.AssertNumberOfCalls(t, "QueryRange", 2)
		client.AssertCalled(t, "QueryRange", mock.Anything, "sum(mimir_continuous_test_sine_wave)", time.Unix(960, 0), time.Unix(1000, 0), writeInterval)

		client.AssertNumberOfCalls(t, "Query", 2)
		client.AssertCalled(t, "Query", mock.Anything, "sum(mimir_continuous_test_sine_wave)", time.Unix(1000, 0))

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP mimir_continuous_test_writes_total Total number of attempted write requests.
			# TYPE mimir_continuous_test_writes_total counter
			mimir_continuous_test_writes_total{test="write-read-series"} 3

			# HELP mimir_continuous_test_queries_total Total number of attempted query requests.
			# TYPE mimir_continuous_test_queries_total counter
			mimir_continuous_test_queries_total{test="write-read-series"} 4

			# HELP mimir_continuous_test_queries_failed_total Total number of failed query requests.
			# TYPE mimir_continuous_test_queries_failed_total counter
			mimir_continuous_test_queries_failed_total{test="write-read-series"} 0
		`),
			"mimir_continuous_test_writes_total", "mimir_continuous_test_writes_failed_total",
			"mimir_continuous_test_queries_total", "mimir_continuous_test_queries_failed_total"))
	})

	t.Run("should stop remote writing on network error", func(t *testing.T) {
		client := &ClientMock{}
		client.On("WriteSeries", mock.Anything, mock.Anything).Return(0, errors.New("network error"))

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadSeriesTest(cfg, client, logger, reg)

		test.lastWrittenTimestamp = time.Unix(940, 0)
		now := time.Unix(1000, 0)
		test.Run(context.Background(), now)

		client.AssertNumberOfCalls(t, "WriteSeries", 1)
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, time.Unix(960, 0), 2))
		assert.Equal(t, int64(940), test.lastWrittenTimestamp.Unix())

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP mimir_continuous_test_writes_total Total number of attempted write requests.
			# TYPE mimir_continuous_test_writes_total counter
			mimir_continuous_test_writes_total{test="write-read-series"} 1

			# HELP mimir_continuous_test_writes_failed_total Total number of failed write requests.
			# TYPE mimir_continuous_test_writes_failed_total counter
			mimir_continuous_test_writes_failed_total{status_code="0",test="write-read-series"} 1

			# HELP mimir_continuous_test_queries_total Total number of attempted query requests.
			# TYPE mimir_continuous_test_queries_total counter
			mimir_continuous_test_queries_total{test="write-read-series"} 0
		`), "mimir_continuous_test_writes_total", "mimir_continuous_test_writes_failed_total", "mimir_continuous_test_queries_total"))
	})

	t.Run("should stop remote writing on 5xx error", func(t *testing.T) {
		client := &ClientMock{}
		client.On("WriteSeries", mock.Anything, mock.Anything).Return(500, errors.New("500 error"))

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadSeriesTest(cfg, client, logger, reg)

		test.lastWrittenTimestamp = time.Unix(940, 0)
		now := time.Unix(1000, 0)
		test.Run(context.Background(), now)

		client.AssertNumberOfCalls(t, "WriteSeries", 1)
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, time.Unix(960, 0), 2))
		assert.Equal(t, int64(940), test.lastWrittenTimestamp.Unix())

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP mimir_continuous_test_writes_total Total number of attempted write requests.
			# TYPE mimir_continuous_test_writes_total counter
			mimir_continuous_test_writes_total{test="write-read-series"} 1

			# HELP mimir_continuous_test_writes_failed_total Total number of failed write requests.
			# TYPE mimir_continuous_test_writes_failed_total counter
			mimir_continuous_test_writes_failed_total{status_code="500",test="write-read-series"} 1

			# HELP mimir_continuous_test_queries_total Total number of attempted query requests.
			# TYPE mimir_continuous_test_queries_total counter
			mimir_continuous_test_queries_total{test="write-read-series"} 0
		`), "mimir_continuous_test_writes_total", "mimir_continuous_test_writes_failed_total", "mimir_continuous_test_queries_total"))
	})

	t.Run("should keep remote writing next intervals on 4xx error", func(t *testing.T) {
		client := &ClientMock{}
		client.On("WriteSeries", mock.Anything, mock.Anything).Return(400, errors.New("400 error"))

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadSeriesTest(cfg, client, logger, reg)

		test.lastWrittenTimestamp = time.Unix(940, 0)
		now := time.Unix(1000, 0)
		test.Run(context.Background(), now)

		client.AssertNumberOfCalls(t, "WriteSeries", 3)
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, time.Unix(960, 0), 2))
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, time.Unix(980, 0), 2))
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, time.Unix(1000, 0), 2))
		assert.Equal(t, int64(1000), test.lastWrittenTimestamp.Unix())

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP mimir_continuous_test_writes_total Total number of attempted write requests.
			# TYPE mimir_continuous_test_writes_total counter
			mimir_continuous_test_writes_total{test="write-read-series"} 3

			# HELP mimir_continuous_test_writes_failed_total Total number of failed write requests.
			# TYPE mimir_continuous_test_writes_failed_total counter
			mimir_continuous_test_writes_failed_total{status_code="400",test="write-read-series"} 3

			# HELP mimir_continuous_test_queries_total Total number of attempted query requests.
			# TYPE mimir_continuous_test_queries_total counter
			mimir_continuous_test_queries_total{test="write-read-series"} 0
		`), "mimir_continuous_test_writes_total", "mimir_continuous_test_writes_failed_total", "mimir_continuous_test_queries_total"))
	})

	t.Run("should query written series, compare results and track no failure if results match", func(t *testing.T) {
		now := time.Unix(1000, 0)

		client := &ClientMock{}
		client.On("WriteSeries", mock.Anything, mock.Anything).Return(200, nil)
		client.On("QueryRange", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{
			{Values: []model.SamplePair{newSamplePair(now, generateSineWaveValue(now)*float64(cfg.NumSeries))}},
		}, nil)
		client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(model.Vector{
			{Timestamp: model.Time(now.UnixMilli()), Value: model.SampleValue(generateSineWaveValue(now) * float64(cfg.NumSeries))},
		}, nil)

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadSeriesTest(cfg, client, logger, reg)

		test.Run(context.Background(), now)

		client.AssertNumberOfCalls(t, "WriteSeries", 1)
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, now, 2))
		assert.Equal(t, int64(1000), test.lastWrittenTimestamp.Unix())

		client.AssertNumberOfCalls(t, "QueryRange", 2)
		client.AssertCalled(t, "QueryRange", mock.Anything, "sum(mimir_continuous_test_sine_wave)", time.Unix(1000, 0), time.Unix(1000, 0), writeInterval)

		client.AssertNumberOfCalls(t, "Query", 2)
		client.AssertCalled(t, "Query", mock.Anything, "sum(mimir_continuous_test_sine_wave)", time.Unix(1000, 0))

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP mimir_continuous_test_writes_total Total number of attempted write requests.
			# TYPE mimir_continuous_test_writes_total counter
			mimir_continuous_test_writes_total{test="write-read-series"} 1

			# HELP mimir_continuous_test_queries_total Total number of attempted query requests.
			# TYPE mimir_continuous_test_queries_total counter
			mimir_continuous_test_queries_total{test="write-read-series"} 4

			# HELP mimir_continuous_test_queries_failed_total Total number of failed query requests.
			# TYPE mimir_continuous_test_queries_failed_total counter
			mimir_continuous_test_queries_failed_total{test="write-read-series"} 0

			# HELP mimir_continuous_test_query_result_checks_total Total number of query results checked for correctness.
			# TYPE mimir_continuous_test_query_result_checks_total counter
			mimir_continuous_test_query_result_checks_total{test="write-read-series"} 4

			# HELP mimir_continuous_test_query_result_checks_failed_total Total number of query results failed when checking for correctness.
			# TYPE mimir_continuous_test_query_result_checks_failed_total counter
			mimir_continuous_test_query_result_checks_failed_total{test="write-read-series"} 0
		`),
			"mimir_continuous_test_writes_total", "mimir_continuous_test_writes_failed_total",
			"mimir_continuous_test_queries_total", "mimir_continuous_test_queries_failed_total",
			"mimir_continuous_test_query_result_checks_total", "mimir_continuous_test_query_result_checks_failed_total"))
	})

	t.Run("should query written series, compare results and track failure if results don't match", func(t *testing.T) {
		now := time.Unix(1000, 0)

		client := &ClientMock{}
		client.On("WriteSeries", mock.Anything, mock.Anything).Return(200, nil)
		client.On("QueryRange", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{
			{Values: []model.SamplePair{{Timestamp: model.Time(now.UnixMilli()), Value: 12345}}},
		}, nil)

		client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(model.Vector{
			{Timestamp: model.Time(now.UnixMilli()), Value: 12345},
		}, nil)

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadSeriesTest(cfg, client, logger, reg)

		test.Run(context.Background(), now)

		client.AssertNumberOfCalls(t, "WriteSeries", 1)
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, now, 2))
		assert.Equal(t, int64(1000), test.lastWrittenTimestamp.Unix())

		client.AssertNumberOfCalls(t, "QueryRange", 2)
		client.AssertCalled(t, "QueryRange", mock.Anything, "sum(mimir_continuous_test_sine_wave)", time.Unix(1000, 0), time.Unix(1000, 0), writeInterval)

		client.AssertNumberOfCalls(t, "Query", 2)
		client.AssertCalled(t, "Query", mock.Anything, "sum(mimir_continuous_test_sine_wave)", time.Unix(1000, 0))

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP mimir_continuous_test_writes_total Total number of attempted write requests.
			# TYPE mimir_continuous_test_writes_total counter
			mimir_continuous_test_writes_total{test="write-read-series"} 1

			# HELP mimir_continuous_test_queries_total Total number of attempted query requests.
			# TYPE mimir_continuous_test_queries_total counter
			mimir_continuous_test_queries_total{test="write-read-series"} 4

			# HELP mimir_continuous_test_queries_failed_total Total number of failed query requests.
			# TYPE mimir_continuous_test_queries_failed_total counter
			mimir_continuous_test_queries_failed_total{test="write-read-series"} 0

			# HELP mimir_continuous_test_query_result_checks_total Total number of query results checked for correctness.
			# TYPE mimir_continuous_test_query_result_checks_total counter
			mimir_continuous_test_query_result_checks_total{test="write-read-series"} 4

			# HELP mimir_continuous_test_query_result_checks_failed_total Total number of query results failed when checking for correctness.
			# TYPE mimir_continuous_test_query_result_checks_failed_total counter
			mimir_continuous_test_query_result_checks_failed_total{test="write-read-series"} 4
		`),
			"mimir_continuous_test_writes_total", "mimir_continuous_test_writes_failed_total",
			"mimir_continuous_test_queries_total", "mimir_continuous_test_queries_failed_total",
			"mimir_continuous_test_query_result_checks_total", "mimir_continuous_test_query_result_checks_failed_total"))
	})
}

func TestWriteReadSeriesTest_getRangeQueryTimeRanges(t *testing.T) {
	cfg := WriteReadSeriesTestConfig{}
	flagext.DefaultValues(&cfg)
	cfg.MaxQueryAge = 2 * 24 * time.Hour

	now := time.Unix(int64((10*24*time.Hour)+(2*time.Second)), 0)

	t.Run("min/max query time has not been set yet", func(t *testing.T) {
		test := NewWriteReadSeriesTest(cfg, &ClientMock{}, log.NewNopLogger(), nil)

		actualRanges, actualInstants := test.getQueryTimeRanges(now)
		assert.Empty(t, actualRanges)
		assert.Empty(t, actualInstants)
	})

	t.Run("min/max query time is older than max age", func(t *testing.T) {
		test := NewWriteReadSeriesTest(cfg, &ClientMock{}, log.NewNopLogger(), nil)
		test.queryMinTime = now.Add(-cfg.MaxQueryAge).Add(-time.Minute)
		test.queryMaxTime = now.Add(-cfg.MaxQueryAge).Add(-time.Minute)

		actualRanges, actualInstants := test.getQueryTimeRanges(now)
		assert.Empty(t, actualRanges)
		assert.Empty(t, actualInstants)
	})

	t.Run("min query time = max query time", func(t *testing.T) {
		test := NewWriteReadSeriesTest(cfg, &ClientMock{}, log.NewNopLogger(), nil)
		test.queryMinTime = now.Add(-time.Minute)
		test.queryMaxTime = now.Add(-time.Minute)

		actualRanges, actualInstants := test.getQueryTimeRanges(now)
		require.Len(t, actualRanges, 2)
		require.Equal(t, [2]time.Time{now.Add(-time.Minute), now.Add(-time.Minute)}, actualRanges[0]) // Last 1h.
		require.Equal(t, [2]time.Time{now.Add(-time.Minute), now.Add(-time.Minute)}, actualRanges[1]) // Random time range.

		require.Len(t, actualInstants, 2)
		require.Equal(t, now.Add(-time.Minute), actualInstants[0]) // Last 1h.
		require.Equal(t, now.Add(-time.Minute), actualInstants[1]) // Random time range.
	})

	t.Run("min and max query time are within the last 1h", func(t *testing.T) {
		test := NewWriteReadSeriesTest(cfg, &ClientMock{}, log.NewNopLogger(), nil)
		test.queryMinTime = now.Add(-30 * time.Minute)
		test.queryMaxTime = now.Add(-time.Minute)

		actualRanges, actualInstants := test.getQueryTimeRanges(now)
		require.Len(t, actualRanges, 2)
		require.Equal(t, [2]time.Time{now.Add(-30 * time.Minute), now.Add(-time.Minute)}, actualRanges[0]) // Last 1h.

		require.Len(t, actualInstants, 2)
		require.Equal(t, now.Add(-time.Minute), actualInstants[0]) // Last 1h.

		// Random time range.
		require.GreaterOrEqual(t, actualRanges[len(actualRanges)-1][0].Unix(), test.queryMinTime.Unix())
		require.LessOrEqual(t, actualRanges[len(actualRanges)-1][1].Unix(), test.queryMaxTime.Unix())

		require.GreaterOrEqual(t, actualInstants[len(actualInstants)-1].Unix(), test.queryMinTime.Unix())
		require.LessOrEqual(t, actualInstants[len(actualInstants)-1].Unix(), test.queryMaxTime.Unix())
	})

	t.Run("min and max query time are within the last 2h", func(t *testing.T) {
		test := NewWriteReadSeriesTest(cfg, &ClientMock{}, log.NewNopLogger(), nil)
		test.queryMinTime = now.Add(-90 * time.Minute)
		test.queryMaxTime = now.Add(-80 * time.Minute)

		actualRanges, actualInstants := test.getQueryTimeRanges(now)
		require.Len(t, actualRanges, 2)
		require.Equal(t, [2]time.Time{now.Add(-90 * time.Minute), now.Add(-80 * time.Minute)}, actualRanges[0]) // Last 24h.

		require.Len(t, actualInstants, 2)
		require.Equal(t, now.Add(-90*time.Minute), actualInstants[0]) // Last 24h.

		// Random time range.
		require.GreaterOrEqual(t, actualRanges[len(actualRanges)-1][0].Unix(), test.queryMinTime.Unix())
		require.LessOrEqual(t, actualRanges[len(actualRanges)-1][1].Unix(), test.queryMaxTime.Unix())

		require.GreaterOrEqual(t, actualInstants[len(actualInstants)-1].Unix(), test.queryMinTime.Unix())
		require.LessOrEqual(t, actualInstants[len(actualInstants)-1].Unix(), test.queryMaxTime.Unix())
	})

	t.Run("min query time is older than 24h", func(t *testing.T) {
		test := NewWriteReadSeriesTest(cfg, &ClientMock{}, log.NewNopLogger(), nil)
		test.queryMinTime = now.Add(-30 * time.Hour)
		test.queryMaxTime = now.Add(-time.Minute)

		actualRanges, actualInstants := test.getQueryTimeRanges(now)
		require.Len(t, actualRanges, 4)
		require.Equal(t, [2]time.Time{now.Add(-time.Hour), now.Add(-time.Minute)}, actualRanges[0])         // Last 1h.
		require.Equal(t, [2]time.Time{now.Add(-24 * time.Hour), now.Add(-time.Minute)}, actualRanges[1])    // Last 24h.
		require.Equal(t, [2]time.Time{now.Add(-24 * time.Hour), now.Add(-23 * time.Hour)}, actualRanges[2]) // From last 23h to last 24h.

		require.Len(t, actualInstants, 3)
		require.Equal(t, now.Add(-time.Minute), actualInstants[0])  // Last 1h.
		require.Equal(t, now.Add(-24*time.Hour), actualInstants[1]) // Last 24h.

		// Random time range.
		require.GreaterOrEqual(t, actualRanges[len(actualRanges)-1][0].Unix(), test.queryMinTime.Unix())
		require.LessOrEqual(t, actualRanges[len(actualRanges)-1][1].Unix(), test.queryMaxTime.Unix())

		require.GreaterOrEqual(t, actualInstants[len(actualInstants)-1].Unix(), test.queryMinTime.Unix())
		require.LessOrEqual(t, actualInstants[len(actualInstants)-1].Unix(), test.queryMaxTime.Unix())
	})

	t.Run("max query time is older than 24h but more recent than max query age", func(t *testing.T) {
		test := NewWriteReadSeriesTest(cfg, &ClientMock{}, log.NewNopLogger(), nil)
		test.queryMinTime = now.Add(-30 * time.Hour)
		test.queryMaxTime = now.Add(-25 * time.Hour)

		actualRanges, actualInstants := test.getQueryTimeRanges(now)
		require.Len(t, actualRanges, 1)
		require.Len(t, actualInstants, 1)

		// Random time range.
		require.GreaterOrEqual(t, actualRanges[len(actualRanges)-1][0].Unix(), test.queryMinTime.Unix())
		require.LessOrEqual(t, actualRanges[len(actualRanges)-1][1].Unix(), test.queryMaxTime.Unix())

		require.GreaterOrEqual(t, actualInstants[len(actualInstants)-1].Unix(), test.queryMinTime.Unix())
		require.LessOrEqual(t, actualInstants[len(actualInstants)-1].Unix(), test.queryMaxTime.Unix())
	})

	t.Run("min query time is older than 24h but max query age is only 10m", func(t *testing.T) {
		cfg := cfg
		cfg.MaxQueryAge = 10 * time.Minute

		test := NewWriteReadSeriesTest(cfg, &ClientMock{}, log.NewNopLogger(), nil)
		test.queryMinTime = now.Add(-30 * time.Hour)
		test.queryMaxTime = now.Add(-time.Minute)

		actualRanges, actualInstants := test.getQueryTimeRanges(now)
		require.Len(t, actualRanges, 2)
		require.Equal(t, [2]time.Time{now.Add(-10 * time.Minute), now.Add(-time.Minute)}, actualRanges[0]) // Last 1h.

		require.Len(t, actualInstants, 2)
		require.Equal(t, now.Add(-time.Minute), actualInstants[0]) // Last 1h.

		// Random time range.
		require.GreaterOrEqual(t, actualRanges[len(actualRanges)-1][0].Unix(), test.queryMinTime.Unix())
		require.LessOrEqual(t, actualRanges[len(actualRanges)-1][1].Unix(), test.queryMaxTime.Unix())

		require.GreaterOrEqual(t, actualInstants[len(actualInstants)-1].Unix(), test.queryMinTime.Unix())
		require.LessOrEqual(t, actualInstants[len(actualInstants)-1].Unix(), test.queryMaxTime.Unix())
	})
}
