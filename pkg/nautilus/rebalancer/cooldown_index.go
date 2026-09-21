// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"sort"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// cooldownIndex is an immutable, query-optimised snapshot of
// moveCooldowns taken at a single instant in time.
//
// Rationale: runPhase3 calls overlaps() up to N×K times per round,
// where N is the count of (range, partition) candidate entries and K
// is the count of still-active cooldowns. At dev-15 sizes (N≈22k,
// K≈1.3k) the naive O(K) scan in (*Rebalancer).isInMoveCooldown pegs
// a single CPU core for tens of seconds per rebalance round, which
// delays SetHashRanges / WatchAssignments pushes long enough that
// distributor leases expire and writes get rejected with
// "key_not_covered". A pprof on a fresh deploy showed isInMoveCooldown
// at 80% of CPU with runtime.mapIterNext underneath.
//
// The index pre-filters expired entries, sorts active intervals by
// Lo, and merges overlapping/adjacent ones. Each overlaps() query
// then collapses to a single binary search (O(log K)), so the whole
// phase drops from O(N·K) to O(K log K + N log K) per round.
//
// The index is intentionally value-typed and held on the stack:
// runPhase3 builds it once, queries it many times within a single
// call, and discards it on return. It must NOT outlive a single
// rebalance round because moveCooldowns may have entries appended
// (by recordMoveCooldowns) and deleted (by pruneExpiredCooldowns)
// between rounds.
type cooldownIndex struct {
	// intervals are grouped by tenant, sorted by Lo, and non-overlapping
	// (touching or overlapping inputs were merged at build time).
	intervals map[string][]assignment.HashRange
}

// newCooldownIndex builds an index from the supplied cooldown map.
// Entries whose deadline is not strictly after `now` are dropped
// (matches the !now.Before(deadline) check in isInMoveCooldown:
// deadline==now is treated as expired). Returns a zero-value index
// when cooldowns is empty or every entry has expired; the zero
// value is safe to query and always returns false.
func newCooldownIndex(now time.Time, cooldowns map[tenantRangeKey]time.Time) cooldownIndex {
	if len(cooldowns) == 0 {
		return cooldownIndex{}
	}
	active := make(map[string][]assignment.HashRange)
	for key, deadline := range cooldowns {
		if now.Before(deadline) {
			active[key.tenantID] = append(active[key.tenantID], key.hr)
		}
	}
	if len(active) == 0 {
		return cooldownIndex{}
	}
	for tenantID, intervals := range active {
		sort.Slice(intervals, func(i, j int) bool {
			if intervals[i].Lo != intervals[j].Lo {
				return intervals[i].Lo < intervals[j].Lo
			}
			return intervals[i].Hi < intervals[j].Hi
		})
		// Merge overlapping or touching intervals within this tenant only.
		merged := intervals[:1]
		for _, hr := range intervals[1:] {
			last := &merged[len(merged)-1]
			if last.Hi < ^uint32(0) && hr.Lo > last.Hi+1 {
				merged = append(merged, hr)
				continue
			}
			if hr.Hi > last.Hi {
				last.Hi = hr.Hi
			}
		}
		active[tenantID] = merged
	}
	return cooldownIndex{intervals: active}
}

// overlaps reports whether the supplied range shares at least one
// hash value with any active cooldown interval. Matches the semantics
// of (*Rebalancer).isInMoveCooldown.
func (c cooldownIndex) overlaps(tenantID string, hr assignment.HashRange) bool {
	intervals := c.intervals[tenantID]
	if len(intervals) == 0 {
		return false
	}
	// Find the first interval whose Hi is >= hr.Lo. That interval
	// is the only candidate that can possibly overlap hr — any
	// earlier interval ends before hr begins, any later interval
	// starts after that candidate (which has Lo > prev.Hi >= hr.Lo
	// only if non-overlapping, but our index is non-overlapping by
	// construction so the candidate is unique).
	i := sort.Search(len(intervals), func(i int) bool {
		return intervals[i].Hi >= hr.Lo
	})
	if i >= len(intervals) {
		return false
	}
	return intervals[i].Lo <= hr.Hi
}

// len returns the number of merged active intervals in the index.
// Exposed for tests and for cheap "is the index empty" checks; the
// production hot path uses overlaps() directly.
func (c cooldownIndex) len() int {
	var total int
	for _, intervals := range c.intervals {
		total += len(intervals)
	}
	return total
}
