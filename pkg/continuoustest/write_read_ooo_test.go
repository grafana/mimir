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
