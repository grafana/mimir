// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	util_math "github.com/grafana/mimir/pkg/util/math"
)

const (
	hashRangeEWMAAlpha = 0.2

	// seriesHistBucketShift selects the high bits of a 32-bit hash that
	// index into the series histogram. With shift=16, each bucket covers
	// 65536 contiguous hash values, and the histogram has 65536 buckets
	// (512 KB total). This is fine-grained enough that even small ranges
	// span many buckets and the partial-bucket approximation at range
	// edges is sub-percent error in practice.
	seriesHistBucketShift = 16
	seriesHistBuckets     = 1 << (32 - seriesHistBucketShift)
	seriesHistBucketSize  = uint64(1) << seriesHistBucketShift
)

// hashRangeRates tracks two per-hash-range load signals:
//
//  1. Sample ingestion rate (EWMA), updated on every push via RecordSamples.
//  2. Active series count, maintained as a fixed-size histogram bucketed
//     by the high bits of the hash. Updated by IncSeries/DecSeries from
//     the TSDB head's series lifecycle callbacks.
//
// The rebalancer pushes owned ranges via SetHashRanges and polls a
// snapshot via Snapshot, which reports both signals. The histogram
// approach for series counts means inserts/deletions are O(1) lock-free
// atomic ops and require no knowledge of the current range layout, so
// SetHashRanges does not need to recount.
type hashRangeRates struct {
	mu     sync.RWMutex
	ranges []assignment.HashRange
	rates  []*util_math.EwmaRate

	tickInterval time.Duration

	// seriesHist[i] is the count of active series whose hash falls in
	// bucket i. Each bucket covers seriesHistBucketSize hashes. Updated
	// lock-free by IncSeries/DecSeries.
	seriesHist [seriesHistBuckets]atomic.Int64
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

// IncSeries records that a new series with the given hash was created.
// Safe to call concurrently from many goroutines without holding any
// hashRangeRates lock.
func (h *hashRangeRates) IncSeries(hash uint32) {
	h.seriesHist[hash>>seriesHistBucketShift].Add(1)
}

// DecSeries records that a series with the given hash was deleted.
// Safe to call concurrently.
func (h *hashRangeRates) DecSeries(hash uint32) {
	h.seriesHist[hash>>seriesHistBucketShift].Add(-1)
}

// seriesInRange returns the active series count attributed to the given
// hash range. Buckets fully covered by the range contribute their full
// count; buckets only partially covered contribute proportionally to
// the fraction of the bucket that the range spans (this is an O(1)
// approximation that assumes uniform hash distribution within a bucket;
// for typical multi-million-hash ranges the error is sub-percent).
func (h *hashRangeRates) seriesInRange(r assignment.HashRange) int64 {
	startBucket := uint64(r.Lo) >> seriesHistBucketShift
	endBucket := uint64(r.Hi) >> seriesHistBucketShift

	if startBucket == endBucket {
		bucketCount := h.seriesHist[startBucket].Load()
		rangeSize := uint64(r.Hi) - uint64(r.Lo) + 1
		return int64(float64(bucketCount) * float64(rangeSize) / float64(seriesHistBucketSize))
	}

	var sum int64

	startBucketHi := (startBucket+1)<<seriesHistBucketShift - 1
	startCovered := startBucketHi - uint64(r.Lo) + 1
	sum += int64(float64(h.seriesHist[startBucket].Load()) * float64(startCovered) / float64(seriesHistBucketSize))

	for b := startBucket + 1; b < endBucket; b++ {
		sum += h.seriesHist[b].Load()
	}

	endBucketLo := endBucket << seriesHistBucketShift
	endCovered := uint64(r.Hi) - endBucketLo + 1
	sum += int64(float64(h.seriesHist[endBucket].Load()) * float64(endCovered) / float64(seriesHistBucketSize))

	return sum
}

// HashRangeSnapshot holds per-range load signals from a snapshot.
type HashRangeSnapshot struct {
	Ranges           []assignment.HashRange
	SamplesPerSecond []float64
	ActiveSeries     []int64
}

// Snapshot returns the current per-range load signals: EWMA samples
// rate and active series count. Unlike the old counter-based approach,
// this does not reset state.
func (h *hashRangeRates) Snapshot() HashRangeSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()

	snap := HashRangeSnapshot{
		Ranges:           make([]assignment.HashRange, len(h.ranges)),
		SamplesPerSecond: make([]float64, len(h.ranges)),
		ActiveSeries:     make([]int64, len(h.ranges)),
	}
	copy(snap.Ranges, h.ranges)

	for i, r := range h.rates {
		snap.SamplesPerSecond[i] = r.Rate()
		snap.ActiveSeries[i] = h.seriesInRange(h.ranges[i])
	}

	return snap
}

// HasRanges returns true if the tracker has any owned ranges configured.
func (h *hashRangeRates) HasRanges() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.ranges) > 0
}

// Ranges returns a copy of the currently-configured owned ranges, in
// the canonical sorted order established by SetRanges. The returned
// slice is safe for the caller to retain and mutate.
func (h *hashRangeRates) Ranges() []assignment.HashRange {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]assignment.HashRange, len(h.ranges))
	copy(out, h.ranges)
	return out
}

// LogSummary logs a summary of all owned hash ranges with their EWMA
// ingestion rates and active series counts.
func (h *hashRangeRates) LogSummary(logger log.Logger) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ranges) == 0 {
		return
	}

	var totalRate float64
	var totalSeries int64
	lines := make([]string, len(h.ranges))
	for i, r := range h.ranges {
		rate := h.rates[i].Rate()
		series := h.seriesInRange(r)
		totalRate += rate
		totalSeries += series
		lines[i] = fmt.Sprintf("[%08x-%08x]=%.1f/s,%d-series", r.Lo, r.Hi, rate, series)
	}

	level.Info(logger).Log(
		"msg", "hash range load summary",
		"num_ranges", len(h.ranges),
		"total_samples_per_sec", fmt.Sprintf("%.1f", totalRate),
		"total_active_series", totalSeries,
		"ranges", strings.Join(lines, " "),
	)
}
