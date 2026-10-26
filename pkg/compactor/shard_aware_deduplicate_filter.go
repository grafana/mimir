// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/2be2db775cbc9e1854aada8dfa7ba66307d90001/pkg/block/fetcher.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package compactor

import (
	"context"
	"slices"

	"github.com/cespare/xxhash/v2"
	"github.com/oklog/ulid/v2"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// ShardAwareDeduplicateFilter is a MetaFetcher filter that filters out older blocks that have exactly the same data.
// Not go-routine safe.
type ShardAwareDeduplicateFilter struct {
	// List of duplicate IDs after last Filter call.
	duplicateIDs []ulid.ULID
}

// NewShardAwareDeduplicateFilter creates a ShardAwareDeduplicateFilter.
func NewShardAwareDeduplicateFilter() *ShardAwareDeduplicateFilter {
	return &ShardAwareDeduplicateFilter{}
}

// Filter filters out from metas, the initial map of blocks, all the blocks that are contained in other, compacted, blocks.
// The removed blocks are source blocks of the blocks that remain in metas after the filtering is executed.
func (f *ShardAwareDeduplicateFilter) Filter(ctx context.Context, metas map[ulid.ULID]*block.Meta, synced block.GaugeVec) error {
	f.duplicateIDs = f.duplicateIDs[:0]

	metasByResolution := make(map[int64][]*block.Meta)
	for _, meta := range metas {
		res := meta.Thanos.Downsample.Resolution
		metasByResolution[res] = append(metasByResolution[res], meta)
	}

	for res := range metasByResolution {
		duplicateULIDs, err := f.findDuplicates(ctx, metasByResolution[res])
		if err != nil {
			return err
		}

		for id := range duplicateULIDs {
			if metas[id] != nil {
				f.duplicateIDs = append(f.duplicateIDs, id)
			}
			synced.WithLabelValues(block.DuplicateMeta).Inc()
			delete(metas, id)
		}
	}

	return nil
}

// findDuplicates finds all the blocks from the input slice of blocks that are fully included in other blocks within the
// same slice. The found blocks are returned as a map which keys are blocks' ULIDs.
//
// Blocks are sorted with fewer sources first, and the successors of a block are the blocks after it in that order
// that it is included in. Every successor contains the block's minimum source, so successors are only looked for
// among the blocks containing that source, rather than among all blocks.
//
// For example consider the following blocks ("four base blocks merged and split into 2 separate shards, plus another level" test):
//
//	ULID(1): {sources: []ulid.ULID{ULID(1)}},
//	ULID(2): {sources: []ulid.ULID{ULID(2)}},
//	ULID(3): {sources: []ulid.ULID{ULID(3)}},
//	ULID(4): {sources: []ulid.ULID{ULID(4)}},
//
//	ULID(5): {sources: []ulid.ULID{ULID(1), ULID(2)}, shardID: "1_of_2"},
//	ULID(6): {sources: []ulid.ULID{ULID(1), ULID(2)}, shardID: "2_of_2"},
//
//	ULID(7): {sources: []ulid.ULID{ULID(3), ULID(4)}, shardID: "1_of_2"},
//	ULID(8): {sources: []ulid.ULID{ULID(3), ULID(4)}, shardID: "2_of_2"},
//
//	ULID(9):  {sources: []ulid.ULID{ULID(1), ULID(2), ULID(3), ULID(4)}, shardID: "1_of_2"},
//	ULID(10): {sources: []ulid.ULID{ULID(1), ULID(2), ULID(3), ULID(4)}, shardID: "2_of_2"},
//
// The blocks containing each minimum source are:
//
//	ULID(1): ULID(1), ULID(5), ULID(6), ULID(9), ULID(10)
//	ULID(2): ULID(2), ULID(5), ULID(6), ULID(9), ULID(10)
//	ULID(3): ULID(3), ULID(7), ULID(8), ULID(9), ULID(10)
//	ULID(4): ULID(4), ULID(7), ULID(8), ULID(9), ULID(10)
//
// ULID(3) has successors ULID(7), ULID(8), ULID(9) and ULID(10), which cover both shards, so it is a duplicate.
// ULID(7) has the successor ULID(9) with the same shard, so it is a duplicate. ULID(9) has no successors.
func (f *ShardAwareDeduplicateFilter) findDuplicates(ctx context.Context, input []*block.Meta) (map[ulid.ULID]struct{}, error) {
	slices.SortFunc(input, compareBySourcesCount)

	blocks := make([]*blockWithSources, len(input))
	minSources := make([]ulid.ULID, len(input))
	candidatesBySource := make(map[ulid.ULID][]int, len(input))
	for i, meta := range input {
		blocks[i] = newBlockWithSources(meta)
		if len(blocks[i].sources) == 0 {
			continue
		}
		minSources[i] = minSource(blocks[i].sources)
		candidatesBySource[minSources[i]] = nil
	}

	// Only index sources that are the minimum source of some block.
	for i, b := range blocks {
		for s := range b.sources {
			if candidates, ok := candidatesBySource[s]; ok {
				candidatesBySource[s] = append(candidates, i)
			}
		}
	}

	duplicateULIDs := make(map[ulid.ULID]struct{})
	var successorShardIDs []string
	for i, b := range blocks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		successorShardIDs = successorShardIDs[:0]
		addIfSuccessor := func(j int) {
			if b.isIncludedIn(blocks[j]) {
				successorShardIDs = append(successorShardIDs, blocks[j].shardID)
			}
		}

		if len(b.sources) == 0 {
			// A block without sources is included in every block.
			for j := i + 1; j < len(blocks); j++ {
				addIfSuccessor(j)
			}
		} else {
			// Candidates are in sort order, so skip those that can't come after this block.
			candidates := candidatesBySource[minSources[i]]
			start, _ := slices.BinarySearch(candidates, i+1)
			for _, j := range candidates[start:] {
				addIfSuccessor(j)
			}
		}

		if isFullyIncludedInSuccessors(b.shardID, successorShardIDs) {
			duplicateULIDs[b.meta.ULID] = struct{}{}
		}
	}
	return duplicateULIDs, nil
}

func compareBySourcesCount(a, b *block.Meta) int {
	alen := len(a.Compaction.Sources)
	blen := len(b.Compaction.Sources)

	if alen == blen {
		return a.ULID.Compare(b.ULID)
	}

	return alen - blen
}

func minSource(sources map[ulid.ULID]struct{}) ulid.ULID {
	first := true
	var minID ulid.ULID
	for s := range sources {
		if first || s.Compare(minID) < 0 {
			minID = s
			first = false
		}
	}
	return minID
}

// DuplicateIDs returns slice of block ids that are filtered out by ShardAwareDeduplicateFilter.
func (f *ShardAwareDeduplicateFilter) DuplicateIDs() []ulid.ULID {
	return f.duplicateIDs
}

// blockWithSources describes block (Meta) with its sources, for finding other blocks that contain the same sources as
// this block. We call such blocks "successors" here. For example, if there are blocks
//
// - A with sources 1
//
// - B with sources 1, 2, 3
//
// - C with sources 4, 5
//
// - D with sources 1, 2, 3, 4, 5
//
// Then B is a successor of A (A sources are subset of B sources, but not vice versa), and D is a successor of A, B and C.
type blockWithSources struct {
	meta    *block.Meta
	shardID string // Shard ID label value extracted from meta. If not empty, all successors must have the same shardID.

	// Sources extracted from meta for easier comparison.
	sources            map[ulid.ULID]struct{}
	sourcesBloomFilter bloomFilter
}

func newBlockWithSources(m *block.Meta) *blockWithSources {
	b := &blockWithSources{
		meta:    m,
		shardID: m.Thanos.Labels[block.CompactorShardIDExternalLabel],
		sources: make(map[ulid.ULID]struct{}, len(m.Compaction.Sources)),
	}

	for _, bid := range m.Compaction.Sources {
		if _, ok := b.sources[bid]; !ok {
			b.sources[bid] = struct{}{}
			b.sourcesBloomFilter.add(bid)
		}
	}
	return b
}

// isIncludedIn returns true, if *this* block is included in other block.
// If this block already has shard ID, then supplied metadata must use the same shard ID,
// in order to be considered "included" in other block.
func (b *blockWithSources) isIncludedIn(other *blockWithSources) bool {
	if b.shardID != "" && b.shardID != other.shardID {
		return false
	}

	//
	// All sources of this block must be in other block.
	//

	// Optimization: if the other block has less sources than this block, then it's not possible
	// that all sources of this block are in the other block.
	if len(other.sources) < len(b.sources) {
		return false
	}

	// Optimization: If the bloom filter of this block is not included in the other block's bloom filter,
	// then it's not possible that all sources of this block are in the other block.
	if !b.sourcesBloomFilter.isIncludedIn(&other.sourcesBloomFilter) {
		return false
	}

	for bid := range b.sources {
		if _, ok := other.sources[bid]; !ok {
			return false
		}
	}

	return true
}

// isFullyIncludedInSuccessors returns true if a block with the given shardID is *fully* included in its successor blocks,
// given their shard IDs. This is true under the following conditions:
//
// - if this block has a non-empty shardID, and it has *any* successors, then it is fully included in its successors.
//
// - if this block doesn't have a shardID, and it has a successor without a shardID, then it is fully included in that successor.
//
// - if this block doesn't have shardID, but *all* its successors do, and together they cover all shards, then it is fully included in its successors.
func isFullyIncludedInSuccessors(shardID string, successorShardIDs []string) bool {
	if len(successorShardIDs) == 0 {
		return false
	}

	// If there are any successors with same shard ID (all successors must have same shard ID),
	// then this block must be included in them, since we don't do splitting into more shards at later levels anymore.
	if shardID != "" {
		return true
	}

	shardCount := uint64(0)
	shards := map[uint64]bool{}

	for _, successorShardID := range successorShardIDs {
		if successorShardID == "" {
			return true
		}

		index, count, err := sharding.ParseShardIDLabelValue(successorShardID)
		// If we fail to parse shardID, we better not consider this block fully included in successors.
		if err != nil {
			return false
		}

		if shardCount == 0 {
			shardCount = count
		}
		shards[index] = true
	}

	// If this condition is true, and all above checks passed, then this block is fully included in successors.
	return uint64(len(shards)) == shardCount
}

const bloomFilterSizeBits = 6
const bloomFilterSizeMask = (1 << bloomFilterSizeBits) - 1
const bloomFilterBits = 6 // log2(64)
const bloomFilterMask = (1 << bloomFilterBits) - 1

type bloomFilter [1 << bloomFilterSizeBits]uint64

func (b *bloomFilter) add(bid ulid.ULID) {
	h := xxhash.Sum64(bid[:])
	bucket := (h >> bloomFilterBits) & bloomFilterSizeMask
	b[bucket] |= 1 << (h & bloomFilterMask)
}

func (b *bloomFilter) isIncludedIn(other *bloomFilter) bool {
	// Check that all bits of b are set in other.
	for i := range b {
		// If we have some of the bits that are missing in other, then we're not included in other.
		if (b[i] & ^other[i]) > 0 {
			return false
		}
	}
	return true
}
