package strategy

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/platinummonkey/go-concurrency-limits/core"
)

// SimpleStrategy is the simplest strategy for enforcing a concurrency limit that has a single counter for tracking
// total usage.
type SimpleStrategy struct {
	inFlight       *int32
	limit          *int32
	metricListener core.MetricSampleListener
}

// NewSimpleStrategy will create a new SimpleStrategy
func NewSimpleStrategy(limit int) *SimpleStrategy {
	return NewSimpleStrategyWithMetricRegistry(limit, core.EmptyMetricRegistryInstance)
}

// NewSimpleStrategyWithMetricRegistry will create a new SimpleStrategy
func NewSimpleStrategyWithMetricRegistry(limit int, registry core.MetricRegistry, tags ...string) *SimpleStrategy {
	if limit < 1 {
		limit = 1
	}
	currentLimit := int32(limit)
	inFlight := int32(0)
	listener := registry.RegisterDistribution(core.MetricInFlight, tags...)
	strategy := &SimpleStrategy{
		limit:          &currentLimit,
		inFlight:       &inFlight,
		metricListener: listener,
	}

	registry.RegisterGauge(core.MetricLimit, core.NewIntMetricSupplierWrapper(strategy.GetLimit), tags...)
	return strategy
}

// TryAcquire will try to acquire a token from the limiter.
// context Context of the request for partitioned limits.
// returns not ok if limit is exceeded, or a StrategyToken that must be released when the operation completes.
func (s *SimpleStrategy) TryAcquire(ctx context.Context) (token core.StrategyToken, ok bool) {
	inFlight := atomic.LoadInt32(s.inFlight)
	if inFlight >= atomic.LoadInt32(s.limit) {
		s.metricListener.AddSample(float64(inFlight))
		return core.NewNotAcquiredStrategyToken(int(inFlight)), false
	}
	inFlight = atomic.AddInt32(s.inFlight, 1)
	s.metricListener.AddSample(float64(inFlight))
	f := func(ref *int32) func() {
		return func() {
			atomic.AddInt32(ref, -1)
		}
	}
	return core.NewAcquiredStrategyToken(int(inFlight), f(s.inFlight)), true
}

// SetLimit will update the strategy with a new limit.
func (s *SimpleStrategy) SetLimit(limit int) {
	if limit < 1 {
		limit = 1
	}
	atomic.StoreInt32(s.limit, int32(limit))
}

// GetLimit will get the current limit
func (s *SimpleStrategy) GetLimit() int {
	return int(atomic.LoadInt32(s.limit))
}

// GetBusyCount will get the current busy count
func (s *SimpleStrategy) GetBusyCount() int {
	return int(atomic.LoadInt32(s.inFlight))
}

func (s *SimpleStrategy) String() string {
	return fmt.Sprintf("SimpleStrategy{inFlight=%d, limit=%d}", atomic.LoadInt32(s.inFlight), s.limit)
}
