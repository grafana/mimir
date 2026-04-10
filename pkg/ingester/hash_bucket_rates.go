// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	util_math "github.com/grafana/mimir/pkg/util/math"
)

const (
	hashRangeEWMAAlpha = 0.2
)

// hashRangeRates tracks EWMA ingestion rates per owned hash range.
// The rebalancer pushes owned ranges via SetHashRanges. On the hot
// push path, RecordSamples finds the matching range (binary search)
// and calls Add() on the per-range EwmaRate. The ingester's background
// loop calls Tick() every second to advance the EWMA, and LogSummary()
// every minute to log per-range rates.
type hashRangeRates struct {
	mu     sync.RWMutex
	ranges []assignment.HashRange
	rates  []*util_math.EwmaRate

	tickInterval time.Duration
}

func newHashRangeRates(tickInterval time.Duration) *hashRangeRates {
	return &hashRangeRates{
		tickInterval: tickInterval,
	}
}

// SetRanges replaces the current set of owned hash ranges. EWMA state
// is preserved for ranges that are unchanged; new ranges start with a
// fresh EWMA.
func (h *hashRangeRates) SetRanges(ranges []assignment.HashRange) {
	sorted := make([]assignment.HashRange, len(ranges))
	copy(sorted, ranges)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Lo < sorted[j].Lo })

	h.mu.Lock()
	defer h.mu.Unlock()

	// Build a lookup of old range -> EwmaRate for state preservation.
	old := make(map[assignment.HashRange]*util_math.EwmaRate, len(h.ranges))
	for i, r := range h.ranges {
		old[r] = h.rates[i]
	}

	newRates := make([]*util_math.EwmaRate, len(sorted))
	for i, r := range sorted {
		if existing, ok := old[r]; ok {
			newRates[i] = existing
		} else {
			newRates[i] = util_math.NewEWMARate(hashRangeEWMAAlpha, h.tickInterval)
		}
	}

	h.ranges = sorted
	h.rates = newRates
}

// RecordSamples adds count samples to the EWMA tracker for the range
// containing the given hash. If no range matches (stale routing), the
// sample is silently dropped.
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
		h.rates[idx].Add(int64(count))
	}
}

// Tick advances all per-range EWMAs. Must be called at a fixed
// interval (matching the tickInterval used at construction).
func (h *hashRangeRates) Tick() {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, r := range h.rates {
		r.Tick()
	}
}

// HashRangeSnapshot holds per-range EWMA rates from a snapshot.
type HashRangeSnapshot struct {
	Ranges           []assignment.HashRange
	SamplesPerSecond []float64
}

// Snapshot returns the current per-range EWMA rates (samples/sec).
// Unlike the old counter-based approach, this does not reset state.
func (h *hashRangeRates) Snapshot() HashRangeSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()

	snap := HashRangeSnapshot{
		Ranges:           make([]assignment.HashRange, len(h.ranges)),
		SamplesPerSecond: make([]float64, len(h.ranges)),
	}
	copy(snap.Ranges, h.ranges)

	for i, r := range h.rates {
		snap.SamplesPerSecond[i] = r.Rate()
	}

	return snap
}

// HasRanges returns true if the tracker has any owned ranges configured.
func (h *hashRangeRates) HasRanges() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.ranges) > 0
}

// LogSummary logs a summary of all owned hash ranges and their EWMA
// ingestion rates.
func (h *hashRangeRates) LogSummary(logger log.Logger) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ranges) == 0 {
		return
	}

	var totalRate float64
	lines := make([]string, len(h.ranges))
	for i, r := range h.ranges {
		rate := h.rates[i].Rate()
		totalRate += rate
		lines[i] = fmt.Sprintf("[%08x-%08x]=%.1f/s", r.Lo, r.Hi, rate)
	}

	level.Info(logger).Log(
		"msg", "hash range ingestion rate summary",
		"num_ranges", len(h.ranges),
		"total_samples_per_sec", fmt.Sprintf("%.1f", totalRate),
		"ranges", strings.Join(lines, " "),
	)
}
