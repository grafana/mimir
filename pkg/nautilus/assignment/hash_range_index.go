// SPDX-License-Identifier: AGPL-3.0-only

package assignment

import "time"

// hashRangeIndex is an immutable interval index over Log.entries.
// Leaves retain the entries' Range.Lo ordering, while each internal
// node stores the maximum Range.Hi in its subtree. A query can skip a
// subtree when every range ends before lo or the subtree's first range
// starts after hi.
type hashRangeIndex struct {
	entryStart int
	entryCount int
	leafBase   int
	maxHi      []uint32
}

func newHashRangeIndex(entries []LogEntry, start, end int) *hashRangeIndex {
	if start == end {
		return nil
	}

	count := end - start
	leafBase := 1
	for leafBase < count {
		leafBase *= 2
	}
	maxHi := make([]uint32, 2*leafBase)
	for i, e := range entries[start:end] {
		maxHi[leafBase+i] = e.Range.Hi
	}
	for i := leafBase - 1; i > 0; i-- {
		maxHi[i] = max(maxHi[2*i], maxHi[2*i+1])
	}

	return &hashRangeIndex{
		entryStart: start,
		entryCount: count,
		leafBase:   leafBase,
		maxHi:      maxHi,
	}
}

func (idx *hashRangeIndex) addPartitionsOverlappingInterval(entries []LogEntry, w0, w1 time.Time, lo, hi uint32, seen map[int32]struct{}) {
	if idx == nil || idx.entryStart+idx.entryCount > len(entries) {
		return
	}
	idx.addPartitionsOverlappingIntervalNode(entries, 1, 0, idx.leafBase, w0, w1, lo, hi, seen)
}

func (idx *hashRangeIndex) addPartitionsOverlappingIntervalNode(entries []LogEntry, node, left, right int, w0, w1 time.Time, lo, hi uint32, seen map[int32]struct{}) {
	// entries are sorted by Range.Lo, so entries[left] is the minimum
	// Lo in this subtree. maxHi[node] is its maximum Hi.
	entry := idx.entryStart + left
	if left >= idx.entryCount || entries[entry].Range.Lo > hi || idx.maxHi[node] < lo {
		return
	}
	if right-left == 1 {
		e := &entries[entry]
		if e.From.Before(w1) && e.endsAfter(w0) {
			seen[e.PartitionID] = struct{}{}
		}
		return
	}

	mid := left + (right-left)/2
	idx.addPartitionsOverlappingIntervalNode(entries, 2*node, left, mid, w0, w1, lo, hi, seen)
	idx.addPartitionsOverlappingIntervalNode(entries, 2*node+1, mid, right, w0, w1, lo, hi, seen)
}
