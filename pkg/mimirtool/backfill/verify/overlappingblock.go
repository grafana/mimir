// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/go-kit/log"
)

// OverlappingBlockVerifier rejects a batch that contains two or more blocks
// in the same compactor shard whose [MinTime, MaxTime) ranges overlap. This is
// a more expensive test than DuplicateDayVerifier, but may be needed for cases
// where created blocks are less than 24hr in size.
//
// Overlap is only checked between blocks that share a compactor shard ID
// (__compactor_shard_id__), because blocks in different shards legitimately
// cover the same time range. Blocks without a shard ID form their own group and
// are never compared with sharded blocks, matching DuplicateDayVerifier.
//
// OverlappingBlockVerifier assumes MinTime < MaxTime for every block, a constraint
// that is ideally enforced by SingleUTCDayVerifier.
//
// yoloString safety: the ULIDs recorded in failures come from
// meta.ULID.String(), where ulid.ULID is a fixed-size [16]byte array.
// String() allocates a fresh 26-byte string that is not backed by any pooled
// request buffer, so retaining these strings in the Report (which outlives
// the verifier call) is safe without strings.Clone. Shard IDs come from
// meta.json parsed off local disk, not from a pooled request buffer, so they
// are safe to retain for the same reason.
type OverlappingBlockVerifier struct {
	logger log.Logger
}

// NewOverlappingBlockVerifier constructs an OverlappingBlockVerifier.
func NewOverlappingBlockVerifier(logger log.Logger) *OverlappingBlockVerifier {
	return &OverlappingBlockVerifier{logger: logger}
}

// Name returns the stable check name used in log lines and Report entries.
func (v *OverlappingBlockVerifier) Name() string { return "overlapping-block" }

// Verify records one failure per block whose [MinTime, MaxTime) range overlaps
// another block's in the same compactor shard, and returns a bounded summary
// error naming how many blocks and regions overlapped. Shards are reported in
// ascending order, with unsharded blocks first.
func (v *OverlappingBlockVerifier) Verify(_ context.Context, blocks []BlockRef, report *Report) error {
	if len(blocks) < 2 {
		return nil
	}

	byShard := make(map[string][]BlockRef)
	for _, blk := range blocks {
		shard := getShardID(&blk.Meta)
		byShard[shard] = append(byShard[shard], blk)
	}

	overlapRegions, overlapBlocks := 0, 0
	for _, shard := range slices.Sorted(maps.Keys(byShard)) {
		group := byShard[shard]
		if len(group) < 2 {
			continue
		}
		r, o := v.overlapsForShard(report, shard, group)
		overlapRegions += r
		overlapBlocks += o
	}

	if overlapRegions == 0 {
		return nil
	}
	return fmt.Errorf("%d block(s) overlap another block, across %d region(s)", overlapBlocks, overlapRegions)
}

// overlapsForShard sorts the blocks of a single shard in place and records a
// failure for every block that overlaps another. It returns the number of
// overlapping regions and blocks.
func (v *OverlappingBlockVerifier) overlapsForShard(report *Report, shard string, sortedBlocks []BlockRef) (overlapRegions, overlapBlocks int) {
	slices.SortFunc(sortedBlocks, func(a, b BlockRef) int {
		if c := cmp.Compare(a.Meta.MinTime, b.Meta.MinTime); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Meta.MaxTime, b.Meta.MaxTime); c != 0 {
			return c
		}
		return strings.Compare(a.Meta.ULID.String(), b.Meta.ULID.String())
	})

	// Sweep once, holding the current run of overlapping blocks as a
	// [runStart, end) window into sorted. Tracking indices rather than
	// accumulating a slice keeps the common case, where nothing overlaps and
	// every run has length one, free of allocations.
	runStart := 0
	curMax := sortedBlocks[0].Meta.MaxTime

	flush := func(end int) {
		if end-runStart > 1 {
			v.recordRun(report, shard, sortedBlocks[runStart:end])
			overlapRegions++
			overlapBlocks += end - runStart
		}
	}

	for i := 1; i < len(sortedBlocks); i++ {
		if sortedBlocks[i].Meta.MinTime < curMax {
			if sortedBlocks[i].Meta.MaxTime > curMax {
				curMax = sortedBlocks[i].Meta.MaxTime
			}
			continue
		}
		flush(i)
		runStart = i
		curMax = sortedBlocks[i].Meta.MaxTime
	}
	flush(len(sortedBlocks))
	return overlapRegions, overlapBlocks
}

// recordRun records one failure per block in a run of mutually overlapping
// blocks that all belong to shard ("" for unsharded blocks). run must be sorted
// by MinTime and hold at least two blocks.
//
// The message states how many peers a block overlaps and the bounds of the
// region they jointly cover, but deliberately does not name the peers:
// enumerating them in every message would make the total output quadratic in
// the size of the run. The region bounds identify the run instead.
func (v *OverlappingBlockVerifier) recordRun(report *Report, shard string, run []BlockRef) {
	regionMin := run[0].Meta.MinTime
	regionMax := run[0].Meta.MaxTime
	for _, blk := range run[1:] {
		if blk.Meta.MaxTime > regionMax {
			regionMax = blk.Meta.MaxTime
		}
	}

	for _, blk := range run {
		label := "unsharded block"
		if shard != "" {
			label = "block in shard " + shard
		}
		report.Add(blk.Meta.ULID.String(), v.Name(), blk.Dir, shard, fmt.Errorf(
			"%s [minTime=%d, maxTime=%d) overlaps %d other block(s) in region [%d, %d)",
			label, blk.Meta.MinTime, blk.Meta.MaxTime, len(run)-1, regionMin, regionMax))
	}
}
