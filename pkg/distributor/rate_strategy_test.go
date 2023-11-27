// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/ingestion_rate_strategy_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestIngestionRateStrategy(t *testing.T) {
	t.Run("rate limiter should share the limit across the number of distributors", func(t *testing.T) {
		// Init limits overrides
		overrides, err := validation.NewOverrides(validation.Limits{
			IngestionRate:      float64(1000),
			IngestionBurstSize: 10000,
		}, nil)
		require.NoError(t, err)

		mockRing := newReadLifecyclerMock()
		mockRing.On("HealthyInstancesCount").Return(2)

		strategy := newGlobalRateStrategyWithBurstFactor(overrides, mockRing)
		assert.Equal(t, strategy.Limit("test"), float64(500))
		assert.Equal(t, strategy.Burst("test"), 10000)
	})

	t.Run("infinite rate limiter should return unlimited settings", func(t *testing.T) {
		strategy := newInfiniteRateStrategy()

		assert.Equal(t, strategy.Limit("test"), float64(rate.Inf))
		assert.Equal(t, strategy.Burst("test"), 0)
	})
	t.Run("Burst factor should be 3x the per distributor limit", func(t *testing.T) {
		// Init limits overrides
		overrides, err := validation.NewOverrides(validation.Limits{
			IngestionRate:        float64(1000),
			IngestionBurstFactor: 3,
		}, nil)
		require.NoError(t, err)

		mockRing := newReadLifecyclerMock()
		mockRing.On("HealthyInstancesCount").Return(2)

		strategy := newGlobalRateStrategyWithBurstFactor(overrides, mockRing)
		assert.Equal(t, strategy.Limit("test"), float64(500))
		assert.Equal(t, strategy.Burst("test"), 1500)
	})
	t.Run("Burst factor should be set to to max int if limit is too large", func(t *testing.T) {
		// Init limits overrides
		overrides, err := validation.NewOverrides(validation.Limits{
			IngestionRate:        float64(math.MaxInt),
			IngestionBurstFactor: 3,
		}, nil)
		require.NoError(t, err)

		mockRing := newReadLifecyclerMock()
		mockRing.On("HealthyInstancesCount").Return(2)

		strategy := newGlobalRateStrategyWithBurstFactor(overrides, mockRing)
		assert.Equal(t, strategy.Limit("test"), float64(math.MaxInt)/2)
		assert.Equal(t, strategy.Burst("test"), math.MaxInt)
	})
	t.Run("Burst factor should be set to to max int if limit is rate.inf", func(t *testing.T) {
		// Init limits overrides
		overrides, err := validation.NewOverrides(validation.Limits{
			IngestionRate:        math.MaxFloat64,
			IngestionBurstFactor: 3,
		}, nil)
		require.NoError(t, err)

		mockRing := newReadLifecyclerMock()
		mockRing.On("HealthyInstancesCount").Return(2)

		strategy := newGlobalRateStrategyWithBurstFactor(overrides, mockRing)
		assert.Equal(t, strategy.Limit("test"), math.MaxFloat64)
		assert.Equal(t, strategy.Burst("test"), math.MaxInt)
	})
}

type readLifecyclerMock struct {
	mock.Mock
}

func newReadLifecyclerMock() *readLifecyclerMock {
	return &readLifecyclerMock{}
}

func (m *readLifecyclerMock) HealthyInstancesCount() int {
	args := m.Called()
	return args.Int(0)
}
