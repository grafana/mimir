// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	cfgOOO WriteReadOOOTestConfig
)

func init() {
	cfgOOO = WriteReadOOOTestConfig{}
	flagext.DefaultValues(&cfgOOO)
	cfgOOO.Enabled = true
	cfgOOO.NumSeries = 2
	cfgOOO.MaxOOOLag = 1 * time.Hour
	cfgOOO.MaxQueryAge = 3 * 24 * time.Hour
}

func TestWriteReadOOO_Init(t *testing.T) {
	logger := log.NewNopLogger()
	now := time.Unix(10*86400, 0)
	expQuery := "sum(max_over_time(mimir_continuous_test_sine_wave_ooo_v2[1s]))"

	t.Run("no previously written samples found", func(t *testing.T) {
		client := newMockClient()
		expFrom := now.Add(-24 * time.Hour).Add(inorderWriteInterval)
		expTo := now
		expStep := time.Minute
		client.On("QueryRange", mock.Anything, expQuery, expFrom, expTo, expStep, mock.Anything).
			Return(model.Matrix{}, nil)

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadOOOTest(cfgOOO, client, logger, reg)
		err := test.Init(context.Background(), now)

		require.NoError(t, err)
		client.AssertNumberOfCalls(t, "QueryRange", 1)
		assertHistoryEmpty(t, test.inOrderSamples)
		assertHistoryEmpty(t, test.outOfOrderSamples)
	})

	t.Run("previously written in-order samples found but no OOO samples", func(t *testing.T) {
		client := newMockClient()
		existingData := model.Matrix{{
			Values: generateFloatSamplesSum(now.Add(-2*time.Hour), now.Add(-1*time.Minute), cfgOOO.NumSeries, inorderWriteInterval, generateSineWaveValue),
		}}

		// Our data is minute-aligned, so the query matching the 1m step returns all samples.
		inOrderFrom := now.Add(-24 * time.Hour).Add(inorderWriteInterval)
		inOrderTo := now
		client.On("QueryRange", mock.Anything, expQuery, inOrderFrom, inOrderTo, inorderWriteInterval, mock.Anything).
			Return(existingData, nil)

		// Our second query overlaps the first at a higher resolution.
		// It should return the same samples as the first query, because every 1m it lines up, but nothing more in the gaps.
		oooFrom := now.Add(-24 * time.Hour).Add(outOfOrderWriteInterval)
		oooTo := now
		client.On("QueryRange", mock.Anything, expQuery, oooFrom, oooTo, outOfOrderWriteInterval, mock.Anything).
			Return(existingData, nil)

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadOOOTest(cfgOOO, client, logger, reg)
		err := test.Init(context.Background(), now)

		require.NoError(t, err)
		client.AssertNumberOfCalls(t, "QueryRange", 2)
		require.Equal(t, now.Add(-1*time.Minute), test.inOrderSamples.lastWrittenTimestamp)
		require.Equal(t, now.Add(-2*time.Hour), test.inOrderSamples.queryMinTime)
		require.Equal(t, now.Add(-1*time.Minute), test.inOrderSamples.queryMaxTime)
		assertHistoryEmpty(t, test.outOfOrderSamples)
	})

	t.Run("in-order samples from [-3h, -30m] and OOO samples from [-3h, -1h30m]", func(t *testing.T) {
		client := newMockClient()
		inOrderData := model.Matrix{{
			Values: generateFloatSamplesSum(now.Add(-3*time.Hour), now.Add(-30*time.Minute), cfgOOO.NumSeries, inorderWriteInterval, generateSineWaveValue),
		}}

		// Higher resolution query: we see 20s-aligned samples from [-3h, -1h30m] (dense period with OOO),
		// followed by only minute-aligned samples from (-1h30m, -30m] (sparse period, no OOO).
		// At 20s step, every 3rd tick aligns with a minute (:00, :20, :40, :00...).
		oooDataDense := generateFloatSamplesSum(now.Add(-3*time.Hour), now.Add(-90*time.Minute), cfgOOO.NumSeries, outOfOrderWriteInterval, generateSineWaveValue)
		oooDataSparse := generateFloatSamplesSum(now.Add(-90*time.Minute).Add(inorderWriteInterval), now.Add(-30*time.Minute), cfgOOO.NumSeries, inorderWriteInterval, generateSineWaveValue)
		oooData := model.Matrix{{
			Values: append(oooDataDense, oooDataSparse...),
		}}

		// First query: in-order samples at 1-minute step.
		inOrderFrom := now.Add(-24 * time.Hour).Add(inorderWriteInterval)
		inOrderTo := now
		client.On("QueryRange", mock.Anything, expQuery, inOrderFrom, inOrderTo, inorderWriteInterval, mock.Anything).
			Return(inOrderData, nil)

		// Second query: OOO samples at 20-second step.
		oooFrom := now.Add(-24 * time.Hour).Add(outOfOrderWriteInterval)
		oooTo := now
		client.On("QueryRange", mock.Anything, expQuery, oooFrom, oooTo, outOfOrderWriteInterval, mock.Anything).
			Return(oooData, nil)

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadOOOTest(cfgOOO, client, logger, reg)
		err := test.Init(context.Background(), now)

		require.NoError(t, err)
		client.AssertNumberOfCalls(t, "QueryRange", 2)
		require.Equal(t, now.Add(-30*time.Minute), test.inOrderSamples.lastWrittenTimestamp)
		require.Equal(t, now.Add(-3*time.Hour), test.inOrderSamples.queryMinTime)
		require.Equal(t, now.Add(-30*time.Minute), test.inOrderSamples.queryMaxTime)
		require.Equal(t, now.Add(-30*time.Minute), test.outOfOrderSamples.lastWrittenTimestamp)
		require.Equal(t, now.Add(-3*time.Hour).Add(20*time.Second), test.outOfOrderSamples.queryMinTime)
		require.Equal(t, now.Add(-30*time.Minute), test.outOfOrderSamples.queryMaxTime)
	})

	t.Run("samples exist, but nothing within maxWriteAge, causes us to start fresh", func(t *testing.T) {
		client := newMockClient()
		existingData := model.Matrix{{
			Values: generateFloatSamplesSum(now.Add(-4*time.Hour), now.Add(-3*time.Hour), cfgOOO.NumSeries, inorderWriteInterval, generateSineWaveValue),
		}}

		inOrderFrom := now.Add(-24 * time.Hour).Add(inorderWriteInterval)
		inOrderTo := now
		client.On("QueryRange", mock.Anything, expQuery, inOrderFrom, inOrderTo, inorderWriteInterval, mock.Anything).
			Return(existingData, nil)

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadOOOTest(cfgOOO, client, logger, reg)
		err := test.Init(context.Background(), now)

		require.NoError(t, err)
		client.AssertNumberOfCalls(t, "QueryRange", 1)
		assertHistoryEmpty(t, test.inOrderSamples)
		assertHistoryEmpty(t, test.outOfOrderSamples)
	})

	t.Run("if the first query for in-order samples fails, we start up as if there is no history", func(t *testing.T) {
		client := newMockClient()
		inOrderFrom := now.Add(-24 * time.Hour).Add(inorderWriteInterval)
		inOrderTo := now
		client.On("QueryRange", mock.Anything, expQuery, inOrderFrom, inOrderTo, inorderWriteInterval, mock.Anything).
			Return(model.Matrix{}, errors.New("failed"))

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadOOOTest(cfgOOO, client, logger, reg)
		err := test.Init(context.Background(), now)

		require.NoError(t, err)
		client.AssertNumberOfCalls(t, "QueryRange", 1)
		assertHistoryEmpty(t, test.inOrderSamples)
		assertHistoryEmpty(t, test.outOfOrderSamples)
	})

	t.Run("the query for OOO samples fails after in-order samples are recovered, we start up with no OOO history", func(t *testing.T) {
		client := newMockClient()
		existingData := model.Matrix{{
			Values: generateFloatSamplesSum(now.Add(-2*time.Hour), now.Add(-30*time.Minute), cfgOOO.NumSeries, inorderWriteInterval, generateSineWaveValue),
		}}
		inOrderFrom := now.Add(-24 * time.Hour).Add(inorderWriteInterval)
		inOrderTo := now
		client.On("QueryRange", mock.Anything, expQuery, inOrderFrom, inOrderTo, inorderWriteInterval, mock.Anything).
			Return(existingData, nil)
		oooFrom := now.Add(-24 * time.Hour).Add(outOfOrderWriteInterval)
		oooTo := now
		client.On("QueryRange", mock.Anything, expQuery, oooFrom, oooTo, outOfOrderWriteInterval, mock.Anything).
			Return(model.Matrix{}, errors.New("failed"))

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadOOOTest(cfgOOO, client, logger, reg)
		err := test.Init(context.Background(), now)

		require.NoError(t, err)
		client.AssertNumberOfCalls(t, "QueryRange", 2)
		require.Equal(t, now.Add(-30*time.Minute), test.inOrderSamples.lastWrittenTimestamp)
		require.Equal(t, now.Add(-2*time.Hour), test.inOrderSamples.queryMinTime)
		require.Equal(t, now.Add(-30*time.Minute), test.inOrderSamples.queryMaxTime)
		assertHistoryEmpty(t, test.outOfOrderSamples)
	})

	t.Run("default config passes validation", func(t *testing.T) {
		var defaultCfg WriteReadOOOTestConfig
		flagext.DefaultValues(&defaultCfg)
		require.NoError(t, defaultCfg.ValidateConfig())
	})

	t.Run("fails if NumSeries is zero", func(t *testing.T) {
		client := newMockClient()
		badCfg := cfgOOO
		badCfg.NumSeries = 0

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadOOOTest(badCfg, client, logger, reg)
		err := test.Init(context.Background(), now)

		require.Error(t, err)
		require.Contains(t, err.Error(), "number of series")
	})

	t.Run("fails if we try to write samples beyond the OOO window", func(t *testing.T) {
		client := newMockClient()
		badCfg := cfgOOO
		badCfg.MaxOOOLag = 3 * time.Hour // oooTestWriteMaxAge is 110 minutes

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadOOOTest(badCfg, client, logger, reg)
		err := test.Init(context.Background(), now)

		require.Error(t, err)
		require.Contains(t, err.Error(), "max OOO lag")
	})
}

func assertHistoryEmpty(t *testing.T, h MetricHistory) {
	t.Helper()
	require.Zero(t, h.lastWrittenTimestamp)
	require.Zero(t, h.queryMinTime)
	require.Zero(t, h.queryMaxTime)
}

func TestWriteReadOOOTest_getInorderQueryTimeRanges(t *testing.T) {
	cfg := WriteReadOOOTestConfig{}
	flagext.DefaultValues(&cfg)
	cfg.MaxQueryAge = 2 * 24 * time.Hour

	now := time.Unix(int64((10*24*time.Hour)+(2*time.Second)), 0)

	t.Run("returns error when min/max query time has not been set", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)

		ranges, instants, err := test.getInorderQueryTimeRanges(now)
		require.Error(t, err)
		require.Empty(t, ranges)
		require.Empty(t, instants)
	})

	t.Run("returns error when query time range is older than max query age", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)
		test.inOrderSamples.queryMinTime = now.Add(-cfg.MaxQueryAge).Add(-time.Hour)
		test.inOrderSamples.queryMaxTime = now.Add(-cfg.MaxQueryAge).Add(-time.Minute)

		ranges, instants, err := test.getInorderQueryTimeRanges(now)
		require.Error(t, err)
		require.Empty(t, ranges)
		require.Empty(t, instants)
	})

	t.Run("last 24h - range query", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)
		// assume we have data for the last 30 hours
		test.inOrderSamples.queryMinTime = now.Add(-30 * time.Hour)
		test.inOrderSamples.queryMaxTime = now.Add(-time.Minute)

		ranges, _, err := test.getInorderQueryTimeRanges(now)
		require.NoError(t, err)

		require.Len(t, ranges, 1)
		require.Equal(t, now.Add(-24*time.Hour), ranges[0][0])
		require.Equal(t, now.Add(-time.Minute), ranges[0][1])
	})

	t.Run("clamps query range to max query age if configured", func(t *testing.T) {
		shortMaxAgeCfg := cfg
		shortMaxAgeCfg.MaxQueryAge = 12 * time.Hour
		test := NewWriteReadOOOTest(shortMaxAgeCfg, newMockClient(), log.NewNopLogger(), nil)
		test.inOrderSamples.queryMinTime = now.Add(-30 * time.Hour)
		test.inOrderSamples.queryMaxTime = now.Add(-time.Minute)

		ranges, _, err := test.getInorderQueryTimeRanges(now)
		require.NoError(t, err)

		require.Len(t, ranges, 1)
		require.Equal(t, now.Add(-12*time.Hour), ranges[0][0])
		require.Equal(t, now.Add(-time.Minute), ranges[0][1])
	})

	t.Run("most recent point - instant query", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)
		test.inOrderSamples.queryMinTime = now.Add(-30 * time.Minute)
		test.inOrderSamples.queryMaxTime = now.Add(-time.Minute)

		_, instants, err := test.getInorderQueryTimeRanges(now)
		require.NoError(t, err)

		require.GreaterOrEqual(t, len(instants), 1)
		require.Equal(t, now.Add(-time.Minute), instants[0])
	})

	t.Run("24h ago - instant query", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)
		test.inOrderSamples.queryMinTime = now.Add(-30 * time.Hour)
		test.inOrderSamples.queryMaxTime = now.Add(-time.Minute)

		_, instants, err := test.getInorderQueryTimeRanges(now)
		require.NoError(t, err)

		require.Len(t, instants, 3)
		require.Equal(t, now.Add(-24*time.Hour), instants[1])
	})

	t.Run("clamps 24h-ago instant to the oldest known point when data doesn't go back 24h", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)
		test.inOrderSamples.queryMinTime = now.Add(-30 * time.Minute)
		test.inOrderSamples.queryMaxTime = now.Add(-time.Minute)

		_, instants, err := test.getInorderQueryTimeRanges(now)
		require.NoError(t, err)

		require.Len(t, instants, 3)
		require.Equal(t, now.Add(-time.Minute), instants[0])
		require.Equal(t, now.Add(-30*time.Minute), instants[1])
	})

	t.Run("skips 24h-ago instant when queryMinTime equals queryMaxTime", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)
		test.inOrderSamples.queryMinTime = now.Add(-time.Minute)
		test.inOrderSamples.queryMaxTime = now.Add(-time.Minute)

		_, instants, err := test.getInorderQueryTimeRanges(now)
		require.NoError(t, err)

		require.Len(t, instants, 2)
		require.Equal(t, now.Add(-time.Minute), instants[0])
	})
}

func TestWriteReadOOOTest_getOutOfOrderQueryTimeRanges(t *testing.T) {
	cfg := WriteReadOOOTestConfig{}
	flagext.DefaultValues(&cfg)
	cfg.MaxQueryAge = 2 * 24 * time.Hour

	now := time.Unix(int64((10*24*time.Hour)+(2*time.Second)), 0)

	t.Run("returns error when min/max query time has not been set", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)

		ranges, instants, err := test.getOutOfOrderQueryTimeRanges(now)
		require.Error(t, err)
		require.Empty(t, ranges)
		require.Empty(t, instants)
	})

	t.Run("returns error when query time range is older than max query age", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)
		test.outOfOrderSamples.queryMinTime = now.Add(-cfg.MaxQueryAge).Add(-time.Hour)
		test.outOfOrderSamples.queryMaxTime = now.Add(-cfg.MaxQueryAge).Add(-time.Minute)

		ranges, instants, err := test.getOutOfOrderQueryTimeRanges(now)
		require.Error(t, err)
		require.Empty(t, ranges)
		require.Empty(t, instants)
	})

	t.Run("ooo border to 24h before that - range query", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)
		test.outOfOrderSamples.queryMinTime = now.Add(-30 * time.Hour)
		test.outOfOrderSamples.queryMaxTime = now.Add(-30 * time.Minute)

		ranges, _, err := test.getOutOfOrderQueryTimeRanges(now)
		require.NoError(t, err)

		require.Len(t, ranges, 1)
		require.Equal(t, now.Add(-25*time.Hour), ranges[0][0]) // Clamped to border - 24h.
		require.Equal(t, now.Add(-1*time.Hour), ranges[0][1])
	})

	t.Run("clamps 24h-border range query when data doesn't go back 24h", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)
		test.outOfOrderSamples.queryMinTime = now.Add(-3 * time.Hour)
		test.outOfOrderSamples.queryMaxTime = now.Add(-30 * time.Minute)

		ranges, _, err := test.getOutOfOrderQueryTimeRanges(now)
		require.NoError(t, err)

		require.Len(t, ranges, 1)
		require.Equal(t, now.Add(-3*time.Hour), ranges[0][0])
		require.Equal(t, now.Add(-1*time.Hour), ranges[0][1])
	})

	t.Run("data doesn't reach back to ooo border - no ooo query issued", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)
		test.outOfOrderSamples.queryMinTime = now.Add(-30 * time.Minute)
		test.outOfOrderSamples.queryMaxTime = now.Add(-10 * time.Minute)

		ranges, _, err := test.getOutOfOrderQueryTimeRanges(now)
		require.NoError(t, err)

		require.Empty(t, ranges)
	})

	t.Run("random instant query in dense region", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)
		test.outOfOrderSamples.queryMinTime = now.Add(-3 * time.Hour)
		test.outOfOrderSamples.queryMaxTime = now.Add(-30 * time.Minute)

		_, instants, err := test.getOutOfOrderQueryTimeRanges(now)
		require.NoError(t, err)

		require.GreaterOrEqual(t, len(instants), 1)
		denseInstant := instants[0]
		require.False(t, denseInstant.Before(now.Add(-3*time.Hour)))
		require.False(t, denseInstant.After(now.Add(-1*time.Hour)))
	})

	t.Run("ooo lag border - instant query", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)
		test.outOfOrderSamples.queryMinTime = now.Add(-3 * time.Hour)
		test.outOfOrderSamples.queryMaxTime = now.Add(-30 * time.Minute)

		_, instants, err := test.getOutOfOrderQueryTimeRanges(now)
		require.NoError(t, err)

		require.Len(t, instants, 2)
		oooLagBorder := now.Add(-1 * time.Hour)
		expectedBorderInstant := oooLagBorder.Truncate(outOfOrderWriteInterval)
		require.Equal(t, expectedBorderInstant, instants[1])
	})

	t.Run("data doesn't reach back to ooo border - no instant query issued", func(t *testing.T) {
		test := NewWriteReadOOOTest(cfg, newMockClient(), log.NewNopLogger(), nil)
		test.outOfOrderSamples.queryMinTime = now.Add(-30 * time.Minute)
		test.outOfOrderSamples.queryMaxTime = now.Add(-10 * time.Minute)

		_, instants, err := test.getOutOfOrderQueryTimeRanges(now)
		require.NoError(t, err)

		require.Empty(t, instants)
	})
}

func TestWriteReadOOOTest_Run(t *testing.T) {
	logger := log.NewNopLogger()

	t.Run("writes initial samples and issues basic queries when no history is present", func(t *testing.T) {
		client := newMockClient()
		client.On("WriteSeries", mock.Anything, mock.Anything, mock.Anything).Return(200, nil)
		client.On("QueryRange", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{}, nil)
		client.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Vector{}, nil)

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadOOOTest(cfgOOO, client, logger, reg)

		now := time.Unix(1000*60, 0)
		_ = test.Run(context.Background(), now)

		expectedInOrderSeries := generateSineWaveSeries(oooFloatMetricName, now, cfgOOO.NumSeries, prompb.Label{Name: "protocol", Value: "prometheus"})
		client.AssertCalled(t, "WriteSeries", mock.Anything, expectedInOrderSeries, mock.Anything)
		require.Equal(t, now, test.inOrderSamples.lastWrittenTimestamp)

		// After one write, queryMinTime == queryMaxTime == now, so range is [now, now] and instants are at now.
		expectedQuery := querySumFloat(oooFloatMetricName)
		client.AssertCalled(t, "QueryRange", mock.Anything, expectedQuery, now, now, mock.Anything, mock.Anything)
		client.AssertCalled(t, "Query", mock.Anything, expectedQuery, now, mock.Anything)
	})

	t.Run("partial history exists", func(t *testing.T) {
		client := newMockClient()
		client.On("WriteSeries", mock.Anything, mock.Anything, mock.Anything).Return(200, nil)
		client.On("QueryRange", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{}, nil)
		client.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Vector{}, nil)

		reg := prometheus.NewPedanticRegistry()
		test := NewWriteReadOOOTest(cfgOOO, client, logger, reg)

		now := time.Unix(1000*60+40, 0)
		test.inOrderSamples.lastWrittenTimestamp = alignTimestampToInterval(now, inorderWriteInterval)
		test.inOrderSamples.queryMinTime = now.Add(-30 * time.Minute)
		test.inOrderSamples.queryMaxTime = test.inOrderSamples.lastWrittenTimestamp

		_ = test.Run(context.Background(), now)

		oooNow := now.Add(-cfgOOO.MaxOOOLag)
		expectedOOOTimestamp := alignTimestampToInterval(oooNow, outOfOrderWriteInterval)
		expectedOOOSeries := generateSineWaveSeries(oooFloatMetricName, expectedOOOTimestamp, cfgOOO.NumSeries, prompb.Label{Name: "protocol", Value: "prometheus"})
		client.AssertCalled(t, "WriteSeries", mock.Anything, expectedOOOSeries, mock.Anything)

		expectedQuery := querySumFloat(oooFloatMetricName)
		inOrderQueryMax := alignTimestampToInterval(now, inorderWriteInterval)
		client.AssertCalled(t, "QueryRange", mock.Anything, expectedQuery, now.Add(-30*time.Minute), inOrderQueryMax, mock.Anything, mock.Anything)
		client.AssertCalled(t, "Query", mock.Anything, expectedQuery, inOrderQueryMax, mock.Anything)
		client.AssertCalled(t, "Query", mock.Anything, expectedQuery, now.Add(-30*time.Minute), mock.Anything)
	})
}
