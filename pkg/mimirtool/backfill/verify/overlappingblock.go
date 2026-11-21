// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/go-kit/log"
)

// OverlappingBlockVerifier rejects a batch in which blocks whose [MinTime,
// MaxTime) ranges overlap do not follow the sharding rule.
//
// All blocks are swept into regions of transitively overlapping ranges. Blocks
// in a region are flagged if the region mixes unsharded and sharded blocks or
// have blocks with different shard counts. Blocks in the same shard must not
// overlap each other. Empty or malformed shard labels count as unsharded. The
// check compares labels only and does not verify that the series in a block
// hash into the shard its label names.
//
// OverlappingBlockVerifier assumes MinTime < MaxTime for every block, a
// constraint that is ideally enforced by SingleUTCDayVerifier.
//
// yoloString safety: the ULIDs recorded in failures come from
// meta.ULID.String(), where ulid.ULID is a fixed-size [16]byte array. String()
// allocates a fresh 26-byte string that is not backed by any pooled request
// buffer, so retaining these strings in the Report (which outlives the verifier
// call) is safe without strings.Clone. Shard IDs come from meta.json parsed off
// local disk, not from a pooled request buffer, so they are safe to retain for
// the same reason.
type OverlappingBlockVerifier struct {
	logger log.Logger
}

// NewOverlappingBlockVerifier constructs an OverlappingBlockVerifier.
func NewOverlappingBlockVerifier(logger log.Logger) *OverlappingBlockVerifier {
	return &OverlappingBlockVerifier{logger: logger}
}

// Name returns the stable check name used in log lines and Report entries.
func (v *OverlappingBlockVerifier) Name() string { return "overlapping-block" }

// Verify records one failure per block in regions that fail the overlap test.
// It returns an error naming how many blocks and regions were flagged. The
// ordering of the returned reports is consistent.
func (v *OverlappingBlockVerifier) Verify(_ context.Context, blocks []BlockRef, report *Report) error {
	if len(blocks) < 2 {
		return nil
	}

	refs := newShardedRefs(blocks)
	slices.SortFunc(refs, compareByTime)

	overlapRegions, overlapBlocks := 0, 0
	sweepOverlaps(refs, func(region []shardedRef) {
		r, o := v.checkRegion(report, region)
		overlapRegions += r
		overlapBlocks += o
	})

	if overlapRegions == 0 {
		return nil
	}
	return fmt.Errorf("%d block(s) overlap another block, across %d region(s)", overlapBlocks, overlapRegions)
}

// compareByTime orders blocks by MinTime, MaxTime and then ULID.
func compareByTime(a, b shardedRef) int {
	return cmp.Or(
		cmp.Compare(a.ref.Meta.MinTime, b.ref.Meta.MinTime),
		cmp.Compare(a.ref.Meta.MaxTime, b.ref.Meta.MaxTime),
		a.ref.Meta.ULID.Compare(b.ref.Meta.ULID),
	)
}

// sweepOverlaps calls fn once for each maximal run of two or more transitively
// overlapping blocks in refs, which must be sorted by MinTime. Blocks where
// one's MaxTime equals the other's MinTime do not overlap.
//
// fn may reorder its run in place: the sweep only calls fn after it has found
// the end of the run, and only reads later indices afterwards.
func sweepOverlaps(refs []shardedRef, fn func(run []shardedRef)) {
	runStart := 0
	curMax := refs[0].ref.Meta.MaxTime

	flush := func(end int) {
		if end-runStart > 1 {
			fn(refs[runStart:end])
		}
	}

	for i := 1; i < len(refs); i++ {
		if refs[i].ref.Meta.MinTime < curMax {
			if refs[i].ref.Meta.MaxTime > curMax {
				curMax = refs[i].ref.Meta.MaxTime
			}
			continue
		}
		flush(i)
		runStart = i
		curMax = refs[i].ref.Meta.MaxTime
	}
	flush(len(refs))
}

// checkRegion records failures for one overlap region of two or more blocks,
// sorted by MinTime, and returns the number of regions and blocks flagged.
func (v *OverlappingBlockVerifier) checkRegion(report *Report, region []shardedRef) (overlapRegions, overlapBlocks int) {
	regionMin := region[0].ref.Meta.MinTime
	regionMax := region[0].ref.Meta.MaxTime
	for _, r := range region[1:] {
		regionMax = max(regionMax, r.ref.Meta.MaxTime)
	}

	slices.SortFunc(region, func(a, b shardedRef) int {
		return cmp.Or(compareBlockShards(a.shard, b.shard), compareByTime(a, b))
	})

	if mix := classifyShardMix(region); mix.mixed() {
		for _, r := range region {
			report.Add(r.ref.Meta.ULID.String(), v.Name(), r.ref.Dir, r.shard.id, fmt.Errorf(
				"%s [minTime=%d, maxTime=%d) is in overlap region [%d, %d), which %s",
				shardLabel(r.shard), r.ref.Meta.MinTime, r.ref.Meta.MaxTime, regionMin, regionMax, mix))
		}
		return 1, len(region)
	}

	for start := 0; start < len(region); {
		end := start + 1
		for end < len(region) && compareBlockShards(region[start].shard, region[end].shard) == 0 {
			end++
		}
		if end-start > 1 {
			sweepOverlaps(region[start:end], func(run []shardedRef) {
				v.recordRun(report, run)
				overlapRegions++
				overlapBlocks += len(run)
			})
		}
		start = end
	}
	return overlapRegions, overlapBlocks
}

// recordRun records one failure per block in a run of overlapping blocks that
// all belong to the same shard. run must be sorted by MinTime and hold at least
// two blocks.
func (v *OverlappingBlockVerifier) recordRun(report *Report, run []shardedRef) {
	regionMin := run[0].ref.Meta.MinTime
	regionMax := run[0].ref.Meta.MaxTime
	for _, r := range run[1:] {
		regionMax = max(regionMax, r.ref.Meta.MaxTime)
	}

	for _, r := range run {
		report.Add(r.ref.Meta.ULID.String(), v.Name(), r.ref.Dir, r.shard.id, fmt.Errorf(
			"%s [minTime=%d, maxTime=%d) overlaps %d other block(s) in region [%d, %d)",
			shardLabel(r.shard), r.ref.Meta.MinTime, r.ref.Meta.MaxTime, len(run)-1, regionMin, regionMax))
	}
}
