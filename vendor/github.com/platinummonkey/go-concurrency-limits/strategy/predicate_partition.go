package strategy

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/platinummonkey/go-concurrency-limits/core"
)

// PredicatePartition defines a partition for the PredicatePartitionStrategy
// Note: generally speaking you shouldn't use this directly, instead use the higher level PredicatePartitionStrategy
type PredicatePartition struct {
	name                 string
	percent              float64
	MetricSampleListener core.MetricSampleListener
	predicate            func(ctx context.Context) bool
	limit                int32
	busy                 int32

	mu sync.RWMutex
}

// NewPredicatePartitionWithMetricRegistry will create a new PredicatePartition
func NewPredicatePartitionWithMetricRegistry(
	name string,
	percent float64,
	predicateFunc func(ctx context.Context) bool,
	registry core.MetricRegistry,
) *PredicatePartition {
	p := PredicatePartition{
		name:      name,
		percent:   percent,
		predicate: predicateFunc,
		limit:     1,
		busy:      0,
	}
	sampleListener := registry.RegisterDistribution(core.MetricInFlight,
		fmt.Sprintf("%s:%s", PartitionTagName, name))
	registry.RegisterGauge(core.MetricPartitionLimit, core.NewIntMetricSupplierWrapper(p.Limit),
		fmt.Sprintf("%s:%s", PartitionTagName, name))
	p.MetricSampleListener = sampleListener
	return &p
}

// BusyCount will return the current limit
func (p *PredicatePartition) BusyCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return int(p.busy)
}

// Limit will return the current limit
func (p *PredicatePartition) Limit() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return int(p.limit)
}

// UpdateLimit will update the current limit
// Calculate this bin's limit while rounding up and ensuring the value
// is at least 1.  With this technique the sum of bin limits may end up being
// higher than the concurrency limit.
func (p *PredicatePartition) UpdateLimit(totalLimit int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	limit := int32(math.Max(1, math.Ceil(float64(totalLimit)*p.percent)))
	p.limit = limit
}

// IsLimitExceeded will return true of the number of requests in flight >= limit
// note: not thread safe.
func (p *PredicatePartition) IsLimitExceeded() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.busy >= p.limit
}

// Acquire from the worker pool
// note: not to be used directly, not thread safe.
func (p *PredicatePartition) Acquire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.busy++
	p.MetricSampleListener.AddSample(float64(p.busy))
}

// Release from the worker pool
// note: not to be used directly, not thread safe.
func (p *PredicatePartition) Release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.busy--
}

// Name will return the partition name, these are immutable.
func (p *PredicatePartition) Name() string {
	return p.name
}

// Percent returns the partition percent, these are immutable
func (p *PredicatePartition) Percent() float64 {
	return p.percent
}

func (p *PredicatePartition) String() string {
	return fmt.Sprintf("PredicatePartition{name=%s, percent=%f, limit=%d, busy=%d}",
		p.name, p.percent, p.limit, p.busy)
}

// PredicatePartitionStrategy is a concurrency limiter that guarantees a certain percentage of the limit to specific
// callers while allowing callers to borrow from underutilized callers.
//
// Callers are identified by their index into an array of percentages passed in during initialization.
// A percentage of 0.0 means that a caller may only use excess capacity and can be completely
// starved when the limit is reached.  A percentage of 1.0 means that the caller
// is guaranteed to get the entire limit.
//
// grpc.server.call.inflight (group=[a, b, c])
// grpc.server.call.limit (group=[a,b,c])
type PredicatePartitionStrategy struct {
	partitions []*PredicatePartition

	mu    sync.RWMutex
	busy  int32
	limit int32
}

// NewPredicatePartitionStrategyWithMetricRegistry will create a new PredicatePartitionStrategy
func NewPredicatePartitionStrategyWithMetricRegistry(
	partitions []*PredicatePartition,
	limit int32,
	registry core.MetricRegistry,
) (*PredicatePartitionStrategy, error) {
	// preconditions check
	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions specified")
	}
	sum := float64(0)
	for _, v := range partitions {
		sum += v.Percent()
		v.UpdateLimit(limit)
	}
	if sum > 1.0 {
		return nil, fmt.Errorf("sum of percentages must be <= 1.0")
	}

	strategy := &PredicatePartitionStrategy{
		partitions: partitions,
		busy:       0,
		limit:      limit,
	}

	registry.RegisterGauge(core.MetricLimit, core.NewIntMetricSupplierWrapper(strategy.Limit))

	return strategy, nil
}

// AddPartition will dynamically add a partition
// will return false if this partition is already defined, otherwise true if successfully added
func (s *PredicatePartitionStrategy) AddPartition(partition *PredicatePartition) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	exists := false
	for _, p := range s.partitions {
		if p == partition {
			exists = true
			break
		}
	}
	if exists {
		return false
	}
	s.partitions = append(s.partitions, partition)
	return true
}

// RemovePartitionsMatching will remove partitions dynamically
// will return the removed matching partitions, and true if there are at least 1 removed partition
func (s *PredicatePartitionStrategy) RemovePartitionsMatching(matcher context.Context) ([]*PredicatePartition, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := make([]*PredicatePartition, 0)
	removed := make([]*PredicatePartition, 0)
	for _, p := range s.partitions {
		if p.predicate(matcher) {
			removed = append(removed, p)
		} else {
			kept = append(kept, p)
		}
	}
	s.partitions = kept
	return removed, len(removed) > 0
}

// TryAcquire a token from a partition
func (s *PredicatePartitionStrategy) TryAcquire(ctx context.Context) (core.StrategyToken, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.partitions {
		if p.predicate(ctx) {
			if s.busy >= s.limit && p.IsLimitExceeded() {
				// limit exceeded on this partition
				return core.NewNotAcquiredStrategyToken(int(s.busy)), false
			}
			s.busy++
			p.Acquire()
			return core.NewAcquiredStrategyToken(int(s.busy), s.releasePartition(p)), true
		}
	}
	return core.NewNotAcquiredStrategyToken(int(s.busy)), false
}

func (s *PredicatePartitionStrategy) releasePartition(partition *PredicatePartition) func() {
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.busy--
		partition.Release()
	}
}

// SetLimit will set a new limit for the PredicatePartitionStrategy and it's partitions
func (s *PredicatePartitionStrategy) SetLimit(limit int) {
	if limit < 1 {
		limit = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.limit != int32(limit) {
		// only do it if they don't match, otherwise it's just extra churn by O(N)
		for _, p := range s.partitions {
			p.UpdateLimit(int32(limit))
		}
	}
	s.limit = int32(limit)
}

// BusyCount will return the current busy count.
func (s *PredicatePartitionStrategy) BusyCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int(s.busy)
}

// Limit will return the current limit.
func (s *PredicatePartitionStrategy) Limit() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int(s.limit)
}

// BinBusyCount will return the current bin's busy count
func (s *PredicatePartitionStrategy) BinBusyCount(idx int) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if idx < 0 || idx-1 > len(s.partitions) {
		return 0, fmt.Errorf("invalid bin index %d", idx)
	}
	partition := s.partitions[idx]
	return partition.BusyCount(), nil
}

// BinLimit will return the current bin's limit
func (s *PredicatePartitionStrategy) BinLimit(idx int) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if idx < 0 || idx-1 > len(s.partitions) {
		return 0, fmt.Errorf("invalid bin index %d", idx)
	}
	partition := s.partitions[idx]
	return partition.Limit(), nil
}

func (s *PredicatePartitionStrategy) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l := int(math.Min(10, float64(len(s.partitions))))
	return fmt.Sprintf("PredicatePartitionStrategy{partitions=%v, limit=%d, busy=%d}",
		s.partitions[:l], s.limit, s.busy)
}
