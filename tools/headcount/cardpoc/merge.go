// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import "github.com/oklog/ulid/v2"

// SumAll adds up every block's own series count, with no deduplication
// across blocks. This is what a naive "ask every block, add the answers"
// query would compute, and it overcounts whenever a series appears in
// more than one visible block.
func SumAll(blocks []Block) int {
	n := 0
	for _, b := range blocks {
		n += b.TotalSeries()
	}
	return n
}

// SumSkipSources sums every block except those a real compaction has
// already superseded, but whose deletion hasn't happened (or hasn't been
// observed) yet.
//
// A block is superseded when some other visible block's Compaction.Sources
// is a superset of its own: Sources records the transitive set of
// original level-1 ancestor IDs, not each intermediate block's own ID, so
// an intermediate block's ID is never literally listed anywhere -- only
// its lineage is, once absorbed into something bigger. Checking simple ID
// membership (is this block's own ID in anyone's Sources list) therefore
// only ever catches original level-1 blocks; every intermediate block
// would wrongly look unsuperseded forever, which is exactly what a first,
// naive version of this function got wrong against real compactor output.
func SumSkipSources(blocks []Block) int {
	superseded := supersededByLineage(blocks)
	n := 0
	for _, b := range blocks {
		if superseded[b.ID] {
			continue
		}
		n += b.TotalSeries()
	}
	return n
}

// SumMinLevel sums only blocks at or above minLevel. Excluding low-level
// blocks trades freshness (recent, not-yet-compacted data) for exactness
// on whatever is left: it's cheap and correct on the blocks it keeps
// precisely because it drops any that might still overlap.
func SumMinLevel(blocks []Block, minLevel int) int {
	n := 0
	for _, b := range blocks {
		if b.Level >= minLevel {
			n += b.TotalSeries()
		}
	}
	return n
}

// HashUnion returns the exact number of distinct series across blocks,
// deduplicated by label-set hash. Unlike the sum-based modes, this is
// exact regardless of how the visible blocks overlap.
func HashUnion(blocks []Block) int {
	seen := map[uint64]struct{}{}
	for _, b := range blocks {
		for _, hashes := range b.SeriesByName {
			for _, h := range hashes {
				seen[h] = struct{}{}
			}
		}
	}
	return len(seen)
}

// hashesForName returns the set of every distinct series hash for name
// across blocks, unioned (deduplicated) the same way HashUnion is for a
// whole tenant. E6 and E9 both need this: a name's series can be split
// across more than one block in a compacted snapshot (one per shard, or
// per day), so summing per-block counts for a single name would
// overcount exactly as SumAll does for the whole tenant.
func hashesForName(blocks []Block, name string) map[uint64]struct{} {
	seen := map[uint64]struct{}{}
	for _, b := range blocks {
		for _, h := range b.SeriesByName[name] {
			seen[h] = struct{}{}
		}
	}
	return seen
}

// supersededByLineage returns the set of block IDs whose source-set
// (their transitive level-1 ancestors) is contained in some other visible
// block's source-set: A is superseded by B when Sources(A) is a subset
// of Sources(B) and B isn't A. Two blocks can have identical source-sets
// (a compactor retry, or an overlapping split/merge, can leave more than
// one output covering the same ancestry): in that case the tie is broken
// by level, then by ID, so exactly one of them survives rather than both
// superseding each other into nothing.
func supersededByLineage(blocks []Block) map[ulid.ULID]bool {
	sourceSets := make([]map[ulid.ULID]bool, len(blocks))
	for i, b := range blocks {
		set := make(map[ulid.ULID]bool, len(b.Sources))
		for _, src := range b.Sources {
			set[src] = true
		}
		sourceSets[i] = set
	}

	superseded := map[ulid.ULID]bool{}
	for i, a := range blocks {
		for j, b := range blocks {
			if i == j {
				continue
			}
			if isSubset(sourceSets[i], sourceSets[j]) && supersededByTiebreak(a, b, sourceSets[i], sourceSets[j]) {
				superseded[a.ID] = true
				break
			}
		}
	}
	return superseded
}

// supersededByTiebreak decides, once a's source-set is contained in b's,
// whether that actually makes a superseded by b. A strict superset always
// does; an equal set (a tie) is broken by level, then by ID, so between
// two blocks with identical lineage exactly one survives.
func supersededByTiebreak(a, b Block, aSet, bSet map[ulid.ULID]bool) bool {
	if len(aSet) != len(bSet) {
		return true // aSet ⊊ bSet: a strict subset, so b strictly dominates.
	}
	if a.Level != b.Level {
		return a.Level < b.Level
	}
	return a.ID.String() < b.ID.String()
}

func isSubset(a, b map[ulid.ULID]bool) bool {
	if len(a) == 0 {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}
