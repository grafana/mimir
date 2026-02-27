// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/ingestion_rate_strategy_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestIngestionRateStrategy(t *testing.T) {
	t.Run("rate limiter should share the limit across the number of distributors", func(t *testing.T) {
		overrides := validation.NewOverrides(validation.Limits{
			IngestionRate:      float64(1000),
			IngestionBurstSize: 10000,
		}, nil)

		mockRing := newReadLifecyclerMock(2, -1, 1)
		strategy := newGlobalRateStrategyWithBurstFactor(overrides, mockRing, false)
		assert.Equal(t, float64(500), strategy.Limit("test"))
		assert.Equal(t, 10000, strategy.Burst("test"))
	})

	t.Run("infinite rate limiter should return unlimited settings", func(t *testing.T) {
		strategy := newInfiniteRateStrategy()

		assert.Equal(t, float64(rate.Inf), strategy.Limit("test"))
		assert.Equal(t, 0, strategy.Burst("test"))
	})
	t.Run("Burst factor should be 3x the per distributor limit", func(t *testing.T) {
		overrides := validation.NewOverrides(validation.Limits{
			IngestionRate:        float64(1000),
			IngestionBurstFactor: 3,
		}, nil)

		mockRing := newReadLifecyclerMock(2, -1, 1)
		strategy := newGlobalRateStrategyWithBurstFactor(overrides, mockRing, false)
		assert.Equal(t, float64(500), strategy.Limit("test"))
		assert.Equal(t, 1500, strategy.Burst("test"))
	})
	t.Run("Burst factor should be set to max int if limit is too large", func(t *testing.T) {
		overrides := validation.NewOverrides(validation.Limits{
			IngestionRate:        float64(math.MaxInt),
			IngestionBurstFactor: 3,
		}, nil)

		mockRing := newReadLifecyclerMock(2, -1, 1)
		strategy := newGlobalRateStrategyWithBurstFactor(overrides, mockRing, false)
		assert.Equal(t, float64(math.MaxInt)/2, strategy.Limit("test"))
		assert.Equal(t, math.MaxInt, strategy.Burst("test"))
	})
	t.Run("Burst factor should be set to max int if limit is rate.inf", func(t *testing.T) {
		overrides := validation.NewOverrides(validation.Limits{
			IngestionRate:        math.MaxFloat64,
			IngestionBurstFactor: 3,
		}, nil)

		mockRing := newReadLifecyclerMock(2, -1, 1)
		strategy := newGlobalRateStrategyWithBurstFactor(overrides, mockRing, false)
		assert.Equal(t, math.MaxFloat64, strategy.Limit("test"))
		assert.Equal(t, math.MaxInt, strategy.Burst("test"))
	})

	t.Run("zone-aware: limit is divided by zone count and distributors in zone", func(t *testing.T) {
		overrides := validation.NewOverrides(validation.Limits{
			IngestionRate:      float64(100000),
			IngestionBurstSize: 200000,
		}, nil)

		mockRing := newReadLifecyclerMock(-1, 35, 2)
		strategy := newGlobalRateStrategyWithBurstFactor(overrides, mockRing, true)
		// 100000 / 35 distributors in zone / 2 zones = 1428.57
		assert.InDelta(t, float64(100000)/35/2, strategy.Limit("test"), 0.01)
	})

	t.Run("zone-aware: smaller zone gets higher per-distributor limit", func(t *testing.T) {
		overrides := validation.NewOverrides(validation.Limits{
			IngestionRate:      float64(100000),
			IngestionBurstSize: 200000,
		}, nil)

		mockRing := newReadLifecyclerMock(-1, 5, 2)
		strategy := newGlobalRateStrategyWithBurstFactor(overrides, mockRing, true)
		// 100000 / 5 distributors in zone / 2 zones = 10000
		assert.Equal(t, float64(10000), strategy.Limit("test"))
	})
	t.Run("zone-aware: 3 zones with burst factor", func(t *testing.T) {
		overrides := validation.NewOverrides(validation.Limits{
			IngestionRate:        float64(9000),
			IngestionBurstFactor: 2,
		}, nil)

		mockRing := newReadLifecyclerMock(-1, 10, 3)
		strategy := newGlobalRateStrategyWithBurstFactor(overrides, mockRing, true)
		// 9000 / 10 / 3 = 300
		assert.Equal(t, float64(300), strategy.Limit("test"))
		// burst = 2 * 300 = 600
		assert.Equal(t, 600, strategy.Burst("test"))
	})
	t.Run("zone-aware: no healthy distributors returns global limit", func(t *testing.T) {
		overrides := validation.NewOverrides(validation.Limits{
			IngestionRate:      float64(1000),
			IngestionBurstSize: 10000,
		}, nil)

		mockRing := newReadLifecyclerMock(-1, 0, 2)
		strategy := newGlobalRateStrategyWithBurstFactor(overrides, mockRing, true)
		assert.Equal(t, float64(1000), strategy.Limit("test"))
	})
}

type readLifecyclerMock struct {
	healthyInstances int
	healthyInZone    int
	zonesCount       int
}

func newReadLifecyclerMock(healthyInstances, healthyInZone, zonesCount int) *readLifecyclerMock {
	return &readLifecyclerMock{
		healthyInstances: healthyInstances,
		healthyInZone:    healthyInZone,
		zonesCount:       zonesCount,
	}
}

func (m *readLifecyclerMock) HealthyInstancesCount() int {
	return m.healthyInstances
}

func (m *readLifecyclerMock) HealthyInstancesInZoneCount() int {
	return m.healthyInZone
}

func (m *readLifecyclerMock) ZonesCount() int {
	return m.zonesCount
}
