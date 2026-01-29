// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
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

func TestWriteReadOOOTest_Init(t *testing.T) {
	logger := log.NewNopLogger()
	now := time.Unix(10*86400, 0)

	t.Run("no previously written samples found", func(t *testing.T) {
		client := newMockClient()
		expQuery := "sum(max_over_time(mimir_continuous_sine_wave_ooo_v2[1s]))"
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
		inOrderHistory := test.inOrderSamples
		require.Zero(t, inOrderHistory.lastWrittenTimestamp)
		require.Zero(t, inOrderHistory.queryMinTime)
		require.Zero(t, inOrderHistory.queryMaxTime)
		oooHistory := test.outOfOrderSamples
		require.Zero(t, oooHistory.lastWrittenTimestamp)
		require.Zero(t, oooHistory.queryMinTime)
		require.Zero(t, oooHistory.queryMaxTime)
	})

	t.Run("previously written in-order samples found but no OOO samples", func(t *testing.T) {
		client := newMockClient()
		expQuery := "sum(max_over_time(mimir_continuous_sine_wave_ooo_v2[1s]))"
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
		inOrderHistory := test.inOrderSamples
		require.Equal(t, now.Add(-1*time.Minute), inOrderHistory.lastWrittenTimestamp)
		require.Equal(t, now.Add(-2*time.Hour), inOrderHistory.queryMinTime)
		require.Equal(t, now.Add(-1*time.Minute), inOrderHistory.queryMaxTime)
		oooHistory := test.outOfOrderSamples
		require.Zero(t, oooHistory.lastWrittenTimestamp)
		require.Zero(t, oooHistory.queryMinTime)
		require.Zero(t, oooHistory.queryMaxTime)
	})

	t.Run("in-order samples from [-3h, -30m] and OOO samples from [-3h, -1h30m]", func(t *testing.T) {
		client := newMockClient()
		expQuery := "sum(max_over_time(mimir_continuous_sine_wave_ooo_v2[1s]))"

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

		// In-order samples should be recovered.
		inOrderHistory := test.inOrderSamples
		require.Equal(t, now.Add(-30*time.Minute), inOrderHistory.lastWrittenTimestamp)
		require.Equal(t, now.Add(-3*time.Hour), inOrderHistory.queryMinTime)
		require.Equal(t, now.Add(-30*time.Minute), inOrderHistory.queryMaxTime)
		oooHistory := test.outOfOrderSamples
		require.Equal(t, now.Add(-30*time.Minute), oooHistory.lastWrittenTimestamp)
		require.Equal(t, now.Add(-3*time.Hour).Add(20*time.Second), oooHistory.queryMinTime)
		require.Equal(t, now.Add(-30*time.Minute), oooHistory.queryMaxTime)
	})

	t.Run("previously written in-order data points are in the range [-2h, -1m]", func(t *testing.T) {
		// client := newMockClient()
		// TODO: Set up mocks.
		//
		// reg := prometheus.NewPedanticRegistry()
		// test := NewWriteReadOOOTest(cfgOOO, client, logger, reg)
		//
		// require.NoError(t, test.Init(context.Background(), now))
		//
		// TODO: Implement assertions.
	})

	t.Run("the most recent previously written in-order sample is older than 1h ago", func(t *testing.T) {
		// client := newMockClient()
		// TODO: Set up mocks.
		//
		// reg := prometheus.NewPedanticRegistry()
		// test := NewWriteReadOOOTest(cfgOOO, client, logger, reg)
		//
		// require.NoError(t, test.Init(context.Background(), now))
		//
		// TODO: Implement assertions.
	})

	t.Run("the most recent previously written OOO sample is older than 1h ago", func(t *testing.T) {
		// client := newMockClient()
		// TODO: Set up mocks.
		//
		// reg := prometheus.NewPedanticRegistry()
		// test := NewWriteReadOOOTest(cfgOOO, client, logger, reg)
		//
		// require.NoError(t, test.Init(context.Background(), now))
		//
		// TODO: Implement assertions.
	})

	t.Run("the first query for in-order samples fails", func(t *testing.T) {
		// client := newMockClient()
		// TODO: Set up mocks.
		//
		// reg := prometheus.NewPedanticRegistry()
		// test := NewWriteReadOOOTest(cfgOOO, client, logger, reg)
		//
		// require.NoError(t, test.Init(context.Background(), now))
		//
		// TODO: Implement assertions.
	})

	t.Run("the query for OOO samples fails after in-order samples are recovered", func(t *testing.T) {
		// client := newMockClient()
		// TODO: Set up mocks.
		//
		// reg := prometheus.NewPedanticRegistry()
		// test := NewWriteReadOOOTest(cfgOOO, client, logger, reg)
		//
		// require.NoError(t, test.Init(context.Background(), now))
		//
		// TODO: Implement assertions.
	})

	t.Run("the configured query max age is < 24h", func(t *testing.T) {
		// client := newMockClient()
		// TODO: Set up mocks.
		//
		// testCfg := cfgOOO
		// testCfg.MaxQueryAge = 2 * time.Hour
		// reg := prometheus.NewPedanticRegistry()
		// test := NewWriteReadOOOTest(testCfg, client, logger, reg)
		//
		// require.NoError(t, test.Init(context.Background(), now))
		//
		// TODO: Implement assertions.
	})
}
