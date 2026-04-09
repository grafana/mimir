// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// hashRangeRates tracks ingestion rates per owned hash range.
// The rebalancer pushes owned ranges via SetHashRanges. On the hot
// push path, RecordSamples finds the matching range (binary search)
// and atomically increments its counter. Snapshot returns per-range
// samples/sec and resets the counters.
type hashRangeRates struct {
	mu        sync.RWMutex
	ranges    []assignment.HashRange
	counts    []atomic.Uint64
	lastReset atomic.Int64 // unix nanoseconds
}

func newHashRangeRates() *hashRangeRates {
	h := &hashRangeRates{}
	h.lastReset.Store(time.Now().UnixNano())
	return h
}

// SetRanges replaces the current set of owned hash ranges. Any
// accumulated counts for previous ranges are discarded.
func (h *hashRangeRates) SetRanges(ranges []assignment.HashRange) {
	sorted := make([]assignment.HashRange, len(ranges))
	copy(sorted, ranges)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Lo < sorted[j].Lo })

	h.mu.Lock()
	h.ranges = sorted
	h.counts = make([]atomic.Uint64, len(sorted))
	h.lastReset.Store(time.Now().UnixNano())
	h.mu.Unlock()
}

// RecordSamples atomically adds count samples to the range containing
// the given hash. If no range matches (stale routing), the sample is
// silently dropped — the rebalancer tolerates this noise.
func (h *hashRangeRates) RecordSamples(hash uint32, count int) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ranges) == 0 {
		return
	}

	idx := sort.Search(len(h.ranges), func(i int) bool {
		return h.ranges[i].Lo > hash
	}) - 1

	if idx >= 0 && h.ranges[idx].Contains(hash) {
		h.counts[idx].Add(uint64(count))
	}
}

// HashRangeSnapshot holds per-range rates from a snapshot.
type HashRangeSnapshot struct {
	Ranges           []assignment.HashRange
	SamplesPerSecond []float64
}

// Snapshot returns the current per-range rates (samples/sec) and
// resets the counters for the next measurement period.
func (h *hashRangeRates) Snapshot() HashRangeSnapshot {
	now := time.Now().UnixNano()
	lastReset := h.lastReset.Swap(now)
	elapsed := float64(now-lastReset) / float64(time.Second)

	h.mu.RLock()
	defer h.mu.RUnlock()

	snap := HashRangeSnapshot{
		Ranges:           make([]assignment.HashRange, len(h.ranges)),
		SamplesPerSecond: make([]float64, len(h.ranges)),
	}
	copy(snap.Ranges, h.ranges)

	if elapsed <= 0 {
		return snap
	}

	for i := range h.counts {
		count := h.counts[i].Swap(0)
		snap.SamplesPerSecond[i] = float64(count) / elapsed
	}

	return snap
}

// HasRanges returns true if the tracker has any owned ranges configured.
func (h *hashRangeRates) HasRanges() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.ranges) > 0
}
