package strategy

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/platinummonkey/go-concurrency-limits/strategy/matchers"
)

// PartitionTagName represents the metric tag used for the partition identifier
const PartitionTagName = "partition"

// LookupPartition defines a partition for the LookupPartitionStrategy
// Note: generally speaking you shouldn't use this directly, instead use the higher level LookupPartitionStrategy
type LookupPartition struct {
	name                 string
	percent              float64
	MetricSampleListener core.MetricSampleListener
	limit                int32
	busy                 int32
	mu                   sync.RWMutex
}

// NewLookupPartitionWithMetricRegistry will create a new LookupPartition
func NewLookupPartitionWithMetricRegistry(
	name string,
	percent float64,
	limit int32,
	registry core.MetricRegistry,
) *LookupPartition {
	pLimit := int32(limit)
	if pLimit < 1 {
		pLimit = 1
	}
	p := LookupPartition{
		name:    name,
		percent: percent,
		limit:   pLimit,
		busy:    0,
	}
	sampleListener := registry.RegisterDistribution(core.MetricInFlight,
		fmt.Sprintf("%s:%s", PartitionTagName, name))
	registry.RegisterGauge(core.MetricPartitionLimit, core.NewIntMetricSupplierWrapper(p.Limit),
		fmt.Sprintf("%s:%s", PartitionTagName, name))
	p.MetricSampleListener = sampleListener
	return &p
}

// BusyCount will return the current limit
func (p *LookupPartition) BusyCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return int(p.busy)
}

// Limit will return the current limit
func (p *LookupPartition) Limit() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return int(p.limit)
}

// UpdateLimit will update the current limit
// Calculate this bin's limit while rounding up and ensuring the value
// is at least 1.  With this technique the sum of bin limits may end up being
// higher than the concurrency limit.
func (p *LookupPartition) UpdateLimit(totalLimit int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	limit := int32(math.Max(1, math.Ceil(float64(totalLimit)*p.percent)))
	p.limit = limit
}

// IsLimitExceeded will return true of the number of requests in flight >= limit
// note: not thread safe.
func (p *LookupPartition) IsLimitExceeded() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.busy >= p.limit
}

// Acquire from the worker pool
// note: not to be used directly, not thread safe.
func (p *LookupPartition) Acquire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.busy++
	p.MetricSampleListener.AddSample(float64(p.busy))
}

// Release from the worker pool
// note: not to be used directly, not thread safe.
func (p *LookupPartition) Release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.busy--
}

// Name will return the partition name, these are immutable.
func (p *LookupPartition) Name() string {
	return p.name
}

// Percent returns the partition percent, these are immutable
func (p *LookupPartition) Percent() float64 {
	return p.percent
}

func (p *LookupPartition) String() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return fmt.Sprintf("LookupPartition{name=%s, percent=%f, limit=%d, busy=%d}",
		p.name, p.percent, p.limit, p.busy)
}

// LookupPartitionStrategy defines the strategy for partitioning the limiter by named groups where the allocation of
// group to percentage is provided up front.
type LookupPartitionStrategy struct {
	partitions       map[string]*LookupPartition
	unknownPartition *LookupPartition
	lookupFunc       func(ctx context.Context) string

	mu    sync.RWMutex
	busy  int32
	limit int32
}

// NewLookupPartitionStrategyWithMetricRegistry will create a new LookupPartitionStrategy
func NewLookupPartitionStrategyWithMetricRegistry(
	partitions map[string]*LookupPartition,
	lookupFunc func(ctx context.Context) string,
	limit int32,
	registry core.MetricRegistry,
) (*LookupPartitionStrategy, error) {
	// preconditions check
	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions specified")
	}
	sum := float64(0)
	for _, v := range partitions {
		sum += v.Percent()
		// update limit
		v.UpdateLimit(limit)
	}
	if sum > 1.0 {
		return nil, fmt.Errorf("sum of percentages must be <= 1.0")
	}

	if lookupFunc == nil {
		lookupFunc = matchers.DefaultStringLookupFunc
	}

	unknownPartition := NewLookupPartitionWithMetricRegistry("<unknown>", 0.0, limit, registry)
	strategy := &LookupPartitionStrategy{
		partitions:       partitions,
		unknownPartition: unknownPartition,
		lookupFunc:       lookupFunc,
		busy:             0,
		limit:            limit,
	}

	registry.RegisterGauge(core.MetricLimit, core.NewIntMetricSupplierWrapper(strategy.Limit))

	return strategy, nil
}

// AddPartition will dynamically add a partition
// will return false if this partition is already defined, otherwise true if successfully added
func (s *LookupPartitionStrategy) AddPartition(name string, partition *LookupPartition) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.partitions[name]
	if ok {
		return false
	}
	s.partitions[name] = partition
	return true
}

// RemovePartition will remove a given partition dynamically
// will return the busy count from that partition, along with true if the partition was found, otherwise false.
func (s *LookupPartitionStrategy) RemovePartition(name string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	partition, ok := s.partitions[name]
	if !ok {
		return 0, false
	}
	delete(s.partitions, name)
	return partition.BusyCount(), true
}

// TryAcquire a token from a partition
func (s *LookupPartitionStrategy) TryAcquire(ctx context.Context) (token core.StrategyToken, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	partitionName := s.lookupFunc(ctx)
	partition, ok := s.partitions[partitionName]
	if !ok {
		partition = s.unknownPartition
	}
	if s.busy >= s.limit && partition.IsLimitExceeded() {
		return core.NewNotAcquiredStrategyToken(int(s.busy)), false
	}
	// otherwise we can acquire
	s.busy++
	partition.Acquire()
	return core.NewAcquiredStrategyToken(int(s.busy), s.releasePartition(partition)), true
}

func (s *LookupPartitionStrategy) releasePartition(partition *LookupPartition) func() {
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.busy--
		partition.Release()
	}
}

// SetLimit will set a new limit for the LookupPartitionStrategy and it's partitions
func (s *LookupPartitionStrategy) SetLimit(limit int) {
	if limit < 1 {
		limit = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.limit != int32(limit) {
		s.limit = int32(limit)
		// only do it if they don't match, otherwise it's just extra churn by O(N)
		for _, v := range s.partitions {
			v.UpdateLimit(int32(limit))
		}
	}
}

// BusyCount will return the current busy count.
func (s *LookupPartitionStrategy) BusyCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int(s.busy)
}

// Limit will return the current limit.
func (s *LookupPartitionStrategy) Limit() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int(s.limit)
}

// BinBusyCount will return the current bin's busy count
func (s *LookupPartitionStrategy) BinBusyCount(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	partition, ok := s.partitions[key]
	if !ok {
		return 0, fmt.Errorf("invalid group %s", key)
	}
	return partition.BusyCount(), nil
}

// BinLimit will return the current bin's limit
func (s *LookupPartitionStrategy) BinLimit(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	partition, ok := s.partitions[key]
	if !ok {
		return 0, fmt.Errorf("invalid group %s", key)
	}
	return partition.Limit(), nil
}

func (s *LookupPartitionStrategy) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("LookupPartitionStrategy{partitions=%v, unknownPartition=%v, limit=%d, busy=%d}",
		s.partitions, s.unknownPartition, s.limit, s.busy)
}
