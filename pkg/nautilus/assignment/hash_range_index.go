// SPDX-License-Identifier: AGPL-3.0-only

package assignment

import "time"

// hashRangeIndex is an immutable interval index over Log.entries.
// Leaves retain the entries' Range.Lo ordering, while each internal
// node stores the maximum Range.Hi in its subtree. A query can skip a
// subtree when every range ends before lo or the subtree's first range
// starts after hi.
type hashRangeIndex struct {
	entryCount int
	leafBase   int
	maxHi      []uint32
}

func newHashRangeIndex(entries []LogEntry) *hashRangeIndex {
	if len(entries) == 0 {
		return nil
	}

	leafBase := 1
	for leafBase < len(entries) {
		leafBase *= 2
	}
	maxHi := make([]uint32, 2*leafBase)
	for i, e := range entries {
		maxHi[leafBase+i] = e.Range.Hi
	}
	for i := leafBase - 1; i > 0; i-- {
		maxHi[i] = max(maxHi[2*i], maxHi[2*i+1])
	}

	return &hashRangeIndex{
		entryCount: len(entries),
		leafBase:   leafBase,
		maxHi:      maxHi,
	}
}

func (idx *hashRangeIndex) addPartitionsOverlappingInterval(entries []LogEntry, w0, w1 time.Time, lo, hi uint32, seen map[int32]struct{}) {
	if idx == nil || idx.entryCount != len(entries) {
		return
	}
	idx.addPartitionsOverlappingIntervalNode(entries, 1, 0, idx.leafBase, w0, w1, lo, hi, seen)
}

func (idx *hashRangeIndex) addPartitionsOverlappingIntervalNode(entries []LogEntry, node, left, right int, w0, w1 time.Time, lo, hi uint32, seen map[int32]struct{}) {
	// entries are sorted by Range.Lo, so entries[left] is the minimum
	// Lo in this subtree. maxHi[node] is its maximum Hi.
	if left >= idx.entryCount || entries[left].Range.Lo > hi || idx.maxHi[node] < lo {
		return
	}
	if right-left == 1 {
		e := &entries[left]
		if e.From.Before(w1) && e.To.After(w0) {
			seen[e.PartitionID] = struct{}{}
		}
		return
	}

	mid := left + (right-left)/2
	idx.addPartitionsOverlappingIntervalNode(entries, 2*node, left, mid, w0, w1, lo, hi, seen)
	idx.addPartitionsOverlappingIntervalNode(entries, 2*node+1, mid, right, w0, w1, lo, hi, seen)
}
