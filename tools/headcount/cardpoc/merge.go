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

// SumSkipSources sums every block except those listed in some other
// visible block's Compaction.Sources: a block that has already been
// superseded by a compaction, but whose deletion hasn't happened (or
// hasn't been observed) yet. A level-1 block is its own sole source, so
// this never skips original, uncompacted data.
func SumSkipSources(blocks []Block) int {
	superseded := supersededIDs(blocks)
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

// supersededIDs returns the set of block IDs that appear in some other
// block's Compaction.Sources, excluding a block's own self-reference (a
// level-1 block lists itself as its sole source, and that must not count
// as superseding itself).
func supersededIDs(blocks []Block) map[ulid.ULID]bool {
	superseded := map[ulid.ULID]bool{}
	for _, b := range blocks {
		for _, src := range b.Sources {
			if src != b.ID {
				superseded[src] = true
			}
		}
	}
	return superseded
}
