// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestWriteReadSeriesTest_Run(t *testing.T) {
	logger := log.NewNopLogger()
	cfg := WriteReadSeriesTestConfig{
		NumSeries: 2,
	}

	t.Run("should write series with current timestamp if it's already aligned to write interval", func(t *testing.T) {
		client := &ClientMock{}
		client.On("WriteSeries", mock.Anything, mock.Anything).Return(200, nil)

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadSeriesTest(cfg, client, logger, reg)

		now := time.Unix(1000, 0)
		test.Run(context.Background(), now)

		client.AssertNumberOfCalls(t, "WriteSeries", 1)
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, now, 2))
		assert.Equal(t, int64(1000), test.lastWrittenTimestamp.Unix())

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP mimir_continuous_test_writes_total Total number of attempted write requests.
			# TYPE mimir_continuous_test_writes_total counter
			mimir_continuous_test_writes_total{test="write-read-series"} 1
		`)))
	})

	t.Run("should write series with timestamp aligned to write interval", func(t *testing.T) {
		client := &ClientMock{}
		client.On("WriteSeries", mock.Anything, mock.Anything).Return(200, nil)

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadSeriesTest(cfg, client, logger, reg)

		now := time.Unix(999, 0)
		test.Run(context.Background(), now)

		client.AssertNumberOfCalls(t, "WriteSeries", 1)
		client.AssertCalled(t, "WriteSeries", mock.Anything, generateSineWaveSeries(metricName, time.Unix(980, 0), 2))
		assert.Equal(t, int64(980), test.lastWrittenTimestamp.Unix())

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP mimir_continuous_test_writes_total Total number of attempted write requests.
			# TYPE mimir_continuous_test_writes_total counter
			mimir_continuous_test_writes_total{test="write-read-series"} 1
		`)))
	})

	t.Run("should write series from last written timestamp until now", func(t *testing.T) {
		client := &ClientMock{}
		client.On("WriteSeries", mock.Anything, mock.Anything).Return(200, nil)

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
		`)))
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
		`)))
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
		`)))
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
		`)))
	})
}
