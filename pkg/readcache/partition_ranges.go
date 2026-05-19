// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"sort"
	"sync"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// partitionRanges is the per-Kafka-partition hash-range bookkeeping
// used to feed the rebalancer's load signal.
//
// Two range sets are tracked per partition:
//
//   - currentRanges: ranges that the rebalancer last told us this
//     partition owns (via SetHashRanges). Distributors are routing
//     hashes in these ranges to this partition right now.
//
//   - historicalRanges: hash space that USED to be in currentRanges
//     and may still have TSDB head residue from before the move.
//     Disjoint from currentRanges. Each round of head compaction
//     drops some residue; the walker re-tallies on its next tick and
//     drops historical ranges whose count has fallen to zero.
//
// rangeCounts is the walker's view of head series in this partition,
// keyed by ranges drawn from currentRanges ∪ historicalRanges. The
// invariant is Σ rangeCounts == NumSeries(head) over the
// (current ∪ historical) cover, modulo races between the walker and
// concurrent appends/compactions.
//
// This per-partition shape is what makes residue "stay" on the right
// partition after a range moves: residue series in partition P's head
// are bucketed into P's rangeCounts under a P-owned historicalRange,
// and the HashRangeStats RPC emits them as (P, range, count) so the
// rebalancer attributes the residue to P rather than to whichever
// partition is now the current owner of that range.
type partitionRanges struct {
	mu sync.RWMutex

	currentRanges    []assignment.HashRange
	historicalRanges []assignment.HashRange
	rangeCounts      map[assignment.HashRange]int64
}

func newPartitionRanges() *partitionRanges {
	return &partitionRanges{
		rangeCounts: make(map[assignment.HashRange]int64),
	}
}

// setRanges applies a new set of currently-owned ranges. Hash space
// that drops out of currentRanges moves to historicalRanges so the
// walker keeps counting residue there until compaction clears the
// head. Hash space that enters currentRanges is dropped from
// historicalRanges (it's "ours" again, accounted for by the current
// side).
//
// Range boundaries from the rebalancer are preserved: if a single
// range splits across rounds, both halves appear separately so the
// rebalancer can score each independently. Counts for ranges still in
// the working set (current ∪ historical) are retained; counts for
// ranges that fell out of both sets are discarded.
func (pr *partitionRanges) setRanges(newRanges []assignment.HashRange) {
	sortedNew := sortedNonOverlappingCopy(newRanges)

	pr.mu.Lock()
	defer pr.mu.Unlock()

	// Hash space that this partition just lost.
	removed := rangeDiff(pr.currentRanges, sortedNew)
	// Existing historical ranges that the new current re-claims must
	// drop out of historical (they will be tracked on the current side
	// from now on).
	keptHistorical := rangeDiff(pr.historicalRanges, sortedNew)
	// New historical = (kept historical) ∪ (just-removed current).
	pr.historicalRanges = rangeUnionPreservingBoundaries(keptHistorical, removed)

	pr.currentRanges = sortedNew

	// Drop counts for ranges that are now in neither set.
	if len(pr.rangeCounts) > 0 {
		live := make(map[assignment.HashRange]struct{}, len(pr.currentRanges)+len(pr.historicalRanges))
		for _, r := range pr.currentRanges {
			live[r] = struct{}{}
		}
		for _, r := range pr.historicalRanges {
			live[r] = struct{}{}
		}
		for r := range pr.rangeCounts {
			if _, ok := live[r]; !ok {
				delete(pr.rangeCounts, r)
			}
		}
	}
}

// rangesSnapshot returns the current working set of ranges (current ∪
// historical) sorted by Lo. The returned slice is a copy safe to
// retain. The walker uses this as the bucket boundary list for one
// head walk.
func (pr *partitionRanges) rangesSnapshot() []assignment.HashRange {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	if len(pr.currentRanges) == 0 && len(pr.historicalRanges) == 0 {
		return nil
	}
	out := make([]assignment.HashRange, 0, len(pr.currentRanges)+len(pr.historicalRanges))
	out = append(out, pr.currentRanges...)
	out = append(out, pr.historicalRanges...)
	sort.Slice(out, func(i, j int) bool { return out[i].Lo < out[j].Lo })
	return out
}

// applyWalkResult merges per-range counts produced by one walk pass
// (which used the snapshot ranges as bucket keys) into the partition's
// rangeCounts. Ranges that walked to zero AND are not in currentRanges
// are dropped from historicalRanges and rangeCounts — residue has
// compacted out of the head.
//
// forRanges must equal what rangesSnapshot returned at the start of
// the walk (same length, same Lo/Hi at each position). Otherwise the
// update is discarded — currentRanges shifted mid-walk and a fresh
// walk on the new snapshot will reconcile.
func (pr *partitionRanges) applyWalkResult(forRanges []assignment.HashRange, counts []int64) bool {
	if len(forRanges) != len(counts) {
		return false
	}

	pr.mu.Lock()
	defer pr.mu.Unlock()

	// Validate the snapshot still matches.
	snap := make([]assignment.HashRange, 0, len(pr.currentRanges)+len(pr.historicalRanges))
	snap = append(snap, pr.currentRanges...)
	snap = append(snap, pr.historicalRanges...)
	sort.Slice(snap, func(i, j int) bool { return snap[i].Lo < snap[j].Lo })
	if len(snap) != len(forRanges) {
		return false
	}
	for i := range snap {
		if snap[i] != forRanges[i] {
			return false
		}
	}

	currentSet := make(map[assignment.HashRange]struct{}, len(pr.currentRanges))
	for _, r := range pr.currentRanges {
		currentSet[r] = struct{}{}
	}

	// Replace counts wholesale. Historical ranges that walked to zero
	// can be GC'd because their residue has compacted out.
	nextCounts := make(map[assignment.HashRange]int64, len(forRanges))
	var newHistorical []assignment.HashRange
	for i, r := range forRanges {
		c := counts[i]
		if _, isCurrent := currentSet[r]; isCurrent {
			// Always retain current ranges, even at zero — the
			// rebalancer needs to see them as part of this
			// partition's footprint.
			nextCounts[r] = c
			continue
		}
		// Historical range. Drop if its count has gone to zero.
		if c == 0 {
			continue
		}
		nextCounts[r] = c
		newHistorical = append(newHistorical, r)
	}
	sort.Slice(newHistorical, func(i, j int) bool { return newHistorical[i].Lo < newHistorical[j].Lo })

	pr.historicalRanges = newHistorical
	pr.rangeCounts = nextCounts
	return true
}

// snapshotCounts returns a deep copy of the per-range counts. Used by
// the HashRangeStats RPC.
func (pr *partitionRanges) snapshotCounts() []hashRangeCount {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	if len(pr.rangeCounts) == 0 && len(pr.currentRanges) == 0 {
		return nil
	}
	// Always emit currentRanges (even if zero) so the rebalancer sees
	// the partition's claimed footprint. Historical entries are only
	// emitted if they have residue (count > 0).
	out := make([]hashRangeCount, 0, len(pr.currentRanges)+len(pr.historicalRanges))
	seen := make(map[assignment.HashRange]struct{}, len(pr.currentRanges))
	for _, r := range pr.currentRanges {
		out = append(out, hashRangeCount{Range: r, Count: pr.rangeCounts[r]})
		seen[r] = struct{}{}
	}
	for _, r := range pr.historicalRanges {
		if _, dup := seen[r]; dup {
			continue
		}
		if c := pr.rangeCounts[r]; c > 0 {
			out = append(out, hashRangeCount{Range: r, Count: c})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Range.Lo < out[j].Range.Lo })
	return out
}

// currentRangesCopy returns a copy of the currently-owned ranges for
// the GetHashRanges RPC.
func (pr *partitionRanges) currentRangesCopy() []assignment.HashRange {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	out := make([]assignment.HashRange, len(pr.currentRanges))
	copy(out, pr.currentRanges)
	return out
}

// adminSnapshot returns the current and historical ranges with their
// per-range counts, split into two slices so the admin page can show
// growth (current) and residue (historical) separately. Unlike
// snapshotCounts, this preserves the current vs historical
// distinction even when residue is non-zero.
func (pr *partitionRanges) adminSnapshot() (current, historical []hashRangeCount) {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	current = make([]hashRangeCount, 0, len(pr.currentRanges))
	for _, r := range pr.currentRanges {
		current = append(current, hashRangeCount{Range: r, Count: pr.rangeCounts[r]})
	}
	historical = make([]hashRangeCount, 0, len(pr.historicalRanges))
	for _, r := range pr.historicalRanges {
		historical = append(historical, hashRangeCount{Range: r, Count: pr.rangeCounts[r]})
	}
	return current, historical
}

// hashRangeCount is the in-process shape of a per-(partition, range)
// count emitted by HashRangeStats. Partition association is implicit
// (it's the partition this came from); the wire format adds it back.
type hashRangeCount struct {
	Range assignment.HashRange
	Count int64
}

// sortedNonOverlappingCopy returns a fresh sorted-by-Lo copy of the
// input ranges. It does not validate non-overlap; callers are expected
// to pass a non-overlapping set (the rebalancer's assignment is
// non-overlapping by construction).
func sortedNonOverlappingCopy(rs []assignment.HashRange) []assignment.HashRange {
	out := make([]assignment.HashRange, len(rs))
	copy(out, rs)
	sort.Slice(out, func(i, j int) bool { return out[i].Lo < out[j].Lo })
	return out
}

// rangeDiff computes the geometric set difference a − b: the portions
// of any range in a that are not covered by any range in b. Both
// inputs must be sorted by Lo and non-overlapping. The output is
// sorted, non-overlapping, and preserves the original a boundaries
// where possible (a single range in a may be split into multiple
// output entries by boundaries from b).
func rangeDiff(a, b []assignment.HashRange) []assignment.HashRange {
	if len(a) == 0 {
		return nil
	}
	if len(b) == 0 {
		out := make([]assignment.HashRange, len(a))
		copy(out, a)
		return out
	}

	var out []assignment.HashRange
	bi := 0
	for _, r := range a {
		// Skip b entries strictly before r.
		for bi < len(b) && b[bi].Hi < r.Lo {
			bi++
		}
		// Walk b entries that overlap r without consuming our pointer
		// (later a entries may also overlap them).
		lo := int64(r.Lo)
		consumed := false
		for bj := bi; bj < len(b) && b[bj].Lo <= r.Hi; bj++ {
			bLo := int64(b[bj].Lo)
			bHi := int64(b[bj].Hi)
			if bLo > lo {
				out = append(out, assignment.HashRange{Lo: uint32(lo), Hi: uint32(bLo - 1)})
			}
			if bHi >= int64(r.Hi) {
				consumed = true
				break
			}
			lo = bHi + 1
		}
		if !consumed && lo <= int64(r.Hi) {
			out = append(out, assignment.HashRange{Lo: uint32(lo), Hi: r.Hi})
		}
	}
	return out
}

// rangeUnionPreservingBoundaries merges two sorted non-overlapping
// range lists. Adjacent or overlapping pairs are NOT coalesced — we
// keep range boundaries the slicer once chose, so the walker still
// reports them as separate buckets (helps the rebalancer correlate
// residue to its previous slicing decisions). The two inputs are
// assumed to be disjoint from each other at the hash-space level; we
// don't try to detect overlaps across the inputs.
func rangeUnionPreservingBoundaries(a, b []assignment.HashRange) []assignment.HashRange {
	if len(a) == 0 {
		out := make([]assignment.HashRange, len(b))
		copy(out, b)
		return out
	}
	if len(b) == 0 {
		out := make([]assignment.HashRange, len(a))
		copy(out, a)
		return out
	}
	merged := make([]assignment.HashRange, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].Lo <= b[j].Lo {
			merged = append(merged, a[i])
			i++
		} else {
			merged = append(merged, b[j])
			j++
		}
	}
	merged = append(merged, a[i:]...)
	merged = append(merged, b[j:]...)
	return merged
}
