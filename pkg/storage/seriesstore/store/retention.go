// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"math"
	"slices"
)

// retained is what retention keeps: data that ends at or after cutoff and, with outOfOrderFrom, the out-of-order
// chunks of each shard from that chunk reference on. A shard's references only grow, so those are the
// out-of-order head's and the ones of the out-of-order compactions written less than the retention period ago,
// whatever their samples' times: Mimir's ingesters without shipping keep a block that long, and an out-of-order
// window may reach back further than the retention. Without outOfOrderFrom, out-of-order chunks go by time only.
type retained struct {
	cutoff         int64
	outOfOrderFrom []uint64
}

var retainedEverything = &retained{cutoff: math.MinInt64}

func (r *retained) keeps(meta ChunkMeta, shard int) bool {
	return meta.MaxTime >= r.cutoff || meta.OutOfOrder && r.outOfOrderFrom != nil && uint64(meta.Ref) >= r.outOfOrderFrom[shard]
}

// outOfOrderCompaction is an out-of-order compaction that wrote blocks: the chunks it compacted are its shards' from
// the watermarks it started from.
type outOfOrderCompaction struct {
	createdMs  int64
	watermarks []uint64
}

// outOfOrderRetained is, for each shard, the reference from which retention keeps out-of-order chunks: the
// watermark the oldest compaction written since deadline started from, or the current one, which the chunks the
// out-of-order head still has are from. Compactions are in the order they ran, so their watermarks only grow.
func outOfOrderRetained(watermarks []uint64, compactions []outOfOrderCompaction, deadline int64) []uint64 {
	from := slices.Clone(watermarks)
	for _, compaction := range compactions {
		if compaction.createdMs < deadline {
			continue
		}
		for shard := range from {
			// A compaction an older engine state didn't record its watermarks of keeps everything.
			watermark := uint64(0)
			if shard < len(compaction.watermarks) {
				watermark = compaction.watermarks[shard]
			}
			from[shard] = min(from[shard], watermark)
		}
		break
	}
	return from
}

// retainedCompactions drops the compactions written before deadline.
func retainedCompactions(compactions []outOfOrderCompaction, deadline int64) []outOfOrderCompaction {
	kept := compactions[:0:0]
	for _, compaction := range compactions {
		if compaction.createdMs >= deadline {
			kept = append(kept, compaction)
		}
	}
	return kept
}
