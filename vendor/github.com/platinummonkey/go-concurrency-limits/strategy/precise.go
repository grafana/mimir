package strategy

import (
	"context"
	"fmt"
	"sync"

	"github.com/platinummonkey/go-concurrency-limits/core"
)

// PreciseStrategy strategy is much more strict than Simple strategy. It uses a sync.Mutex to keep track of
// of the number of inFlight requests instead of atomic accounting. The major difference is this adds additional
// contention for precise limiting, vs SimpleStrategy which trades off exact precision for speed.
type PreciseStrategy struct {
	mu             sync.Mutex
	inFlight       int32
	limit          int32
	metricListener core.MetricSampleListener
}

// NewPreciseStrategy will return a new PreciseStrategy.
func NewPreciseStrategy(limit int) *PreciseStrategy {
	return NewPreciseStrategyWithMetricRegistry(limit, core.EmptyMetricRegistryInstance)
}

// NewPreciseStrategyWithMetricRegistry will create a new PreciseStrategy
func NewPreciseStrategyWithMetricRegistry(limit int, registry core.MetricRegistry, tags ...string) *PreciseStrategy {
	if limit < 1 {
		limit = 1
	}
	listener := registry.RegisterDistribution(core.MetricInFlight, tags...)
	strategy := &PreciseStrategy{
		limit:          int32(limit),
		metricListener: listener,
	}

	registry.RegisterGauge(core.MetricLimit, core.NewIntMetricSupplierWrapper(strategy.GetLimit), tags...)
	return strategy
}

// TryAcquire will try to acquire a token from the limiter.
// context Context of the request for partitioned limits.
// returns not ok if limit is exceeded, or a StrategyToken that must be released when the operation completes.
func (s *PreciseStrategy) TryAcquire(ctx context.Context) (token core.StrategyToken, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight >= s.limit {
		s.metricListener.AddSample(float64(s.inFlight))
		return core.NewNotAcquiredStrategyToken(int(s.inFlight)), false
	}
	s.inFlight++
	s.metricListener.AddSample(float64(s.inFlight))
	return core.NewAcquiredStrategyToken(int(s.inFlight), s.releaseHandler), true
}

func (s *PreciseStrategy) releaseHandler() {
	s.mu.Lock()
	s.inFlight--
	s.mu.Unlock()
}

// SetLimit will update the strategy with a new limit.
func (s *PreciseStrategy) SetLimit(limit int) {
	if limit < 1 {
		limit = 1
	}
	s.mu.Lock()
	s.limit = int32(limit)
	s.mu.Unlock()
}

// GetLimit will get the current limit
func (s *PreciseStrategy) GetLimit() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.limit)
}

// GetBusyCount will get the current busy count
func (s *PreciseStrategy) GetBusyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.inFlight)
}

func (s *PreciseStrategy) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("PreciseStrategy{inFlight=%d, limit=%d}", s.inFlight, s.limit)
}
