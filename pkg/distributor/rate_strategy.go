// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/ingestion_rate_strategy.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"math"

	"github.com/grafana/dskit/limiter"
	"golang.org/x/time/rate"

	"github.com/grafana/mimir/pkg/util/validation"
)

// ReadLifecycler represents the read interface to the lifecycler.
type ReadLifecycler interface {
	HealthyInstancesCount() int
}

type globalIngestionStrategyWithBurstFactor struct {
	limits *validation.Overrides
	ring   ReadLifecycler
}

// We want a rate limiter that can use the burst factor if it's set for ingest rate limiting.
func newGlobalRateStrategyWithBurstFactor(limits *validation.Overrides, ring ReadLifecycler) limiter.RateLimiterStrategy {
	return &globalIngestionStrategyWithBurstFactor{
		limits: limits,
		ring:   ring,
	}
}

func (s *globalIngestionStrategyWithBurstFactor) Limit(tenantID string) float64 {
	numDistributors := s.ring.HealthyInstancesCount()

	limit := s.limits.IngestionRate(tenantID)

	if numDistributors == 0 || limit == float64(rate.Inf) {
		return limit
	}
	return limit / float64(numDistributors)
}

func (s *globalIngestionStrategyWithBurstFactor) Burst(tenantID string) int {
	burstFactor := s.limits.IngestionBurstFactor(tenantID)
	if burstFactor > 0 {
		limit := s.Limit(tenantID)
		burstByFactor := burstFactor * limit
		// If the ingestion rate * burst factor is too large we want to set it to the max possible burst value
		if burstByFactor >= math.MaxInt {
			return math.MaxInt
		}
		return int(math.Ceil(burstByFactor))
	}
	return s.limits.IngestionBurstSize(tenantID)
}

type globalStrategy struct {
	baseStrategy limiter.RateLimiterStrategy
	ring         ReadLifecycler
}

func newGlobalRateStrategy(baseStrategy limiter.RateLimiterStrategy, ring ReadLifecycler) limiter.RateLimiterStrategy {
	return &globalStrategy{
		baseStrategy: baseStrategy,
		ring:         ring,
	}
}

func (s *globalStrategy) Limit(tenantID string) float64 {
	numDistributors := s.ring.HealthyInstancesCount()

	limit := s.baseStrategy.Limit(tenantID)

	if numDistributors == 0 || limit == float64(rate.Inf) {
		return limit
	}
	return limit / float64(numDistributors)
}

func (s *globalStrategy) Burst(tenantID string) int {
	// The meaning of burst doesn't change for the global strategy, in order
	// to keep it easier to understand for users / operators.
	return s.baseStrategy.Burst(tenantID)
}

type requestRateStrategy struct {
	limits *validation.Overrides
}

func newRequestRateStrategy(limits *validation.Overrides) limiter.RateLimiterStrategy {
	return &requestRateStrategy{
		limits: limits,
	}
}

func (s *requestRateStrategy) Limit(tenantID string) float64 {
	if lm := s.limits.RequestRate(tenantID); lm > 0 {
		return lm
	}
	return float64(rate.Inf)
}

func (s *requestRateStrategy) Burst(tenantID string) int {
	if s.limits.RequestRate(tenantID) <= 0 {
		// Burst is ignored when limit = rate.Inf
		return 0
	}
	if lm := s.limits.RequestBurstSize(tenantID); lm > 0 {
		return lm
	}
	return math.MaxInt
}

type infiniteStrategy struct{}

func newInfiniteRateStrategy() limiter.RateLimiterStrategy {
	return &infiniteStrategy{}
}

func (s *infiniteStrategy) Limit(_ string) float64 {
	return float64(rate.Inf)
}

func (s *infiniteStrategy) Burst(_ string) int {
	// Burst is ignored when limit = rate.Inf
	return 0
}
