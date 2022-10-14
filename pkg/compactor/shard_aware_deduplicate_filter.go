// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/2be2db775cbc9e1854aada8dfa7ba66307d90001/pkg/block/fetcher.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package compactor

import (
	"context"
	"sort"

	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb"
)

const duplicateMeta = "duplicate"

// ShardAwareDeduplicateFilter is a BaseFetcher filter that filters out older blocks that have exactly the same data.
// Not go-routine safe.
type ShardAwareDeduplicateFilter struct {
	// List of duplicate IDs after last Filter call.
	duplicateIDs []ulid.ULID
}

// NewShardAwareDeduplicateFilter creates ShardAwareDeduplicateFilter.
func NewShardAwareDeduplicateFilter() *ShardAwareDeduplicateFilter {
	return &ShardAwareDeduplicateFilter{}
}

// Filter filters out duplicate blocks that can be formed
// from two or more overlapping blocks that fully submatches the source blocks of the older blocks.
func (f *ShardAwareDeduplicateFilter) Filter(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, synced block.GaugeVec, modified block.GaugeVec) error {
	f.duplicateIDs = f.duplicateIDs[:0]

	metasByResolution := make(map[int64][]*metadata.Meta)
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
			synced.WithLabelValues(duplicateMeta).Inc()
			delete(metas, id)
		}
	}

	return nil
}

// findDuplicates finds blocks that are fully included in other blocks.
// It works by 1) sorting blocks by number of sources, and 2) iterating through each block, and adding it to the
// tree of blocks, where children are successors of given block.
//
// For example for input ("four base blocks merged and split into 2 separate shards, plus another level" test):
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
// Resulting tree will look like this:
//
//	Root
//	`--- ULID(1)
//	|    `--- ULID(5)
//	|    |    `--- ULID(9)
//	|    |    `--- ULID(10)
//	|    `--- ULID(6)
//	|         `--- ULID(9)
//	|         `--- ULID(10)
//	`--- ULID(2)
//	|    `--- ULID(5)
//	|    |    `--- ULID(9)
//	|    |    `--- ULID(10)
//	|    `--- ULID(6)
//	|         `--- ULID(9)
//	|         `--- ULID(10)
//	`--- ULID(3)
//	|    `--- ULID(7)
//	|    |    `--- ULID(9)
//	|    |    `--- ULID(10)
//	|    `--- ULID(8)
//	|         `--- ULID(9)
//	|         `--- ULID(10)
//	`--- ULID(4)
//	     `--- ULID(7)
//	     |    `--- ULID(9)
//	     |    `--- ULID(10)
//	     `--- ULID(8)
//	          `--- ULID(9)
//	          `--- ULID(10)
//
// There is a lot of repetition in this tree, but individual block nodes are shared (it would be difficult to draw that though).
// So for example there is only one ULID(9) node, referenced from nodes 5, 6, 7, 8 (each of them also exists only once). See
// blockWithSuccessors structure -- it uses maps to pointers to handle all this cross-referencing correctly.
func (f *ShardAwareDeduplicateFilter) findDuplicates(ctx context.Context, input []*metadata.Meta) (map[ulid.ULID]struct{}, error) {
	// Sort blocks with fewer sources first.
	sort.Slice(input, func(i, j int) bool {
		ilen := len(input[i].Compaction.Sources)
		jlen := len(input[j].Compaction.Sources)

		if ilen == jlen {
			return input[i].ULID.Compare(input[j].ULID) < 0
		}

		return ilen < jlen
	})

	root := newBlockWithSuccessors(nil)
	for _, meta := range input {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		root.addSuccessorIfPossible(newBlockWithSuccessors(meta))
	}

	duplicateULIDs := make(map[ulid.ULID]struct{})
	root.getDuplicateBlocks(duplicateULIDs)
	return duplicateULIDs, nil
}

// DuplicateIDs returns slice of block ids that are filtered out by ShardAwareDeduplicateFilter.
func (f *ShardAwareDeduplicateFilter) DuplicateIDs() []ulid.ULID {
	return f.duplicateIDs
}

// blockWithSuccessors describes block (Meta) with other blocks, that contain the same sources as
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
// Then B is a successor of A (A sources are subset of B sources, but not vice versa), and D is successor of both A, B and C.
type blockWithSuccessors struct {
	meta    *metadata.Meta         // If meta is nil, then this is root node of the tree.
	shardID string                 // Shard ID label value extracted from meta. If not empty, that all successors must have same shardID.
	sources map[ulid.ULID]struct{} // Sources extracted from meta for easier comparison.

	successors map[ulid.ULID]*blockWithSuccessors
}

func newBlockWithSuccessors(m *metadata.Meta) *blockWithSuccessors {
	b := &blockWithSuccessors{meta: m}
	if m != nil {
		b.shardID = m.Thanos.Labels[tsdb.CompactorShardIDExternalLabel]
		b.sources = make(map[ulid.ULID]struct{}, len(m.Compaction.Sources))
		for _, bid := range m.Compaction.Sources {
			b.sources[bid] = struct{}{}
		}
	}
	return b
}

// isIncludedIn returns true, if *this* block is included in other block.
// If this block already has shard ID, then supplied metadata must use the same shard ID,
// in order to be considered "included" in other block.
func (b *blockWithSuccessors) isIncludedIn(other *blockWithSuccessors) bool {
	if b.meta == nil {
		return true
	}

	if b.shardID != "" && b.shardID != other.shardID {
		return false
	}

	// All sources of this block must be in other block.
	for bid := range b.sources {
		if _, ok := other.sources[bid]; !ok {
			return false
		}
	}
	return true
}

// addSuccessorIfPossible adds successor to this block, but only if this block is included in the other block.
// Successor is not added directly to this block, but correct place in the tree is found for it first.
// Returns true, if other block was added as successor (somewhere in the tree), false otherwise.
//
// Since different subtrees overlap, we need to handle the case when this block and other block are the same pointer.
func (b *blockWithSuccessors) addSuccessorIfPossible(other *blockWithSuccessors) bool {
	if b == other || !b.isIncludedIn(other) {
		return false
	}

	// Try to find ALL successors where we can include the block
	added := false
	for _, s := range b.successors {
		if s.addSuccessorIfPossible(other) {
			added = true
		}
	}

	if !added {
		if b.successors == nil {
			b.successors = map[ulid.ULID]*blockWithSuccessors{}
		}
		b.successors[other.meta.ULID] = other
	}
	return true
}

// isFullyIncludedInSuccessors returns true if this block is *fully* included in successor blocks.
// This is true under following conditions:
//
// - if this block has non-empty shardID and has *any* successors, then it is fully included already.
//
// - if this block doesn't have shardID, and there is any successor without shardID, then it if fully included in that successor.
//
// - if this block doesn't have shardID, but ALL successors do, and together they cover all shards, then this block is fully included in successors.
func (b *blockWithSuccessors) isFullyIncludedInSuccessors() bool {
	if len(b.successors) == 0 {
		return false
	}

	// If there are any successors with same shard ID (all successors must have same shard ID),
	// then this block must be included in them, since we don't do splitting into more shards at later levels anymore.
	if b.shardID != "" {
		// Double check that our invariant holds.
		for _, s := range b.successors {
			if s.shardID != b.shardID {
				panic("successor has different shardID!")
			}
		}
		return true
	}

	shardCount := uint64(0)
	shards := map[uint64]bool{}

	for _, s := range b.successors {
		if s.shardID == "" {
			return true
		}

		index, count, err := sharding.ParseShardIDLabelValue(s.shardID)
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

func (b *blockWithSuccessors) getDuplicateBlocks(output map[ulid.ULID]struct{}) {
	if b.meta != nil && b.isFullyIncludedInSuccessors() {
		output[b.meta.ULID] = struct{}{}
	}

	for _, s := range b.successors {
		s.getDuplicateBlocks(output)
	}
}
