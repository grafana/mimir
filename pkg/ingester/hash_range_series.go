// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// hashRangeSeries tracks per-hash-range active-series counts for the
// hash ranges this ingester has been told it owns (via SetRanges).
//
// Counts are not maintained incrementally on the write path. Instead
// the ingester periodically walks the TSDB head of each tenant,
// computes mimirpb.ShardByMetricNameLocalityLabels for every series,
// tallies the hits per owned range, and pushes the result via
// SetCountsFor. This avoids the atomic-op hot path on every series
// creation/deletion, avoids the 512 KB fixed histogram, and self-
// corrects any accumulated drift from missed/duplicated lifecycle
// callbacks. The CPU cost is paid on the background tick (see
// updateHashRangeSeriesCounts) rather than on the request path.
//
// The rebalancer fetches counts via Snapshot (through the HashRangeStats
// RPC) and uses them to pick which owned range to move off of a hot
// source partition.
type hashRangeSeries struct {
	mu     sync.RWMutex
	ranges []assignment.HashRange // sorted by Lo, non-overlapping
	counts []int64                // parallel to ranges
}

func newHashRangeSeries() *hashRangeSeries {
	return &hashRangeSeries{}
}

// SetRanges replaces the current set of owned hash ranges. Counts are
// reset to zero — the next walk will repopulate them. Until that walk
// completes, Snapshot reports zeros for every range.
func (h *hashRangeSeries) SetRanges(ranges []assignment.HashRange) {
	sorted := make([]assignment.HashRange, len(ranges))
	copy(sorted, ranges)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Lo < sorted[j].Lo })

	h.mu.Lock()
	defer h.mu.Unlock()

	h.ranges = sorted
	h.counts = make([]int64, len(sorted))
}

// SetCountsFor atomically replaces the per-range counts. The provided
// forRanges slice must equal the currently-configured ranges (same
// length, same Lo/Hi at each position); otherwise the update is
// discarded and false is returned. This guards against a walk whose
// input ranges went stale mid-walk because SetRanges fired; a
// subsequent walk will refresh.
func (h *hashRangeSeries) SetCountsFor(forRanges []assignment.HashRange, counts []int64) bool {
	if len(forRanges) != len(counts) {
		return false
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if len(forRanges) != len(h.ranges) {
		return false
	}
	for i := range forRanges {
		if forRanges[i] != h.ranges[i] {
			return false
		}
	}

	newCounts := make([]int64, len(counts))
	copy(newCounts, counts)
	h.counts = newCounts
	return true
}

// HashRangeSeriesSnapshot holds a consistent view of owned ranges and
// their counts.
type HashRangeSeriesSnapshot struct {
	Ranges []assignment.HashRange
	Counts []int64
}

// Snapshot returns the current owned ranges and their per-range active-
// series counts. Returned slices are copies safe for the caller to
// retain and mutate.
func (h *hashRangeSeries) Snapshot() HashRangeSeriesSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()

	snap := HashRangeSeriesSnapshot{
		Ranges: make([]assignment.HashRange, len(h.ranges)),
		Counts: make([]int64, len(h.counts)),
	}
	copy(snap.Ranges, h.ranges)
	copy(snap.Counts, h.counts)
	return snap
}

// HasRanges returns true if any owned ranges are configured.
func (h *hashRangeSeries) HasRanges() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.ranges) > 0
}

// Ranges returns a copy of the currently-configured owned ranges, in
// the canonical sorted order established by SetRanges. The returned
// slice is safe for the caller to retain and mutate.
func (h *hashRangeSeries) Ranges() []assignment.HashRange {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]assignment.HashRange, len(h.ranges))
	copy(out, h.ranges)
	return out
}

// LogSummary logs a one-line summary of all owned hash ranges with
// their active-series counts.
func (h *hashRangeSeries) LogSummary(logger log.Logger) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ranges) == 0 {
		return
	}

	var total int64
	lines := make([]string, len(h.ranges))
	for i, r := range h.ranges {
		total += h.counts[i]
		lines[i] = fmt.Sprintf("[%08x-%08x]=%d-series", r.Lo, r.Hi, h.counts[i])
	}

	level.Info(logger).Log(
		"msg", "hash range series summary",
		"num_ranges", len(h.ranges),
		"total_active_series", total,
		"ranges", strings.Join(lines, " "),
	)
}
