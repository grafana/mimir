// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/ingestion_rate_strategy.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"github.com/grafana/dskit/limiter"
	"golang.org/x/time/rate"

	"github.com/grafana/mimir/pkg/util/validation"
)

// ReadLifecycler represents the read interface to the lifecycler.
type ReadLifecycler interface {
	HealthyInstancesCount() int
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

	if numDistributors == 0 {
		return s.baseStrategy.Limit(tenantID)
	}

	return s.baseStrategy.Limit(tenantID) / float64(numDistributors)
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
	return s.limits.RequestRate(tenantID)
}

func (s *requestRateStrategy) Burst(tenantID string) int {
	return s.limits.RequestBurstSize(tenantID)
}

type ingestionRateStrategy struct {
	limits *validation.Overrides
}

func newIngestionRateStrategy(limits *validation.Overrides) limiter.RateLimiterStrategy {
	return &ingestionRateStrategy{
		limits: limits,
	}
}

func (s *ingestionRateStrategy) Limit(tenantID string) float64 {
	return s.limits.IngestionRate(tenantID)
}

func (s *ingestionRateStrategy) Burst(tenantID string) int {
	return s.limits.IngestionBurstSize(tenantID)
}

type infiniteStrategy struct{}

func newInfiniteRateStrategy() limiter.RateLimiterStrategy {
	return &infiniteStrategy{}
}

func (s *infiniteStrategy) Limit(tenantID string) float64 {
	return float64(rate.Inf)
}

func (s *infiniteStrategy) Burst(tenantID string) int {
	// Burst is ignored when limit = rate.Inf
	return 0
}
