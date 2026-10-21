// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/go-kit/log"
)

// OverlappingBlockVerifier rejects a batch that contains two or more blocks
// whose [MinTime, MaxTime) ranges overlap. This is a more expensive test than
// DuplicateDayVerifier, but may be needed for cases where created blocks are
// less than 24hr in size.
//
// OverlappingBlockVerifier assumes MinTime < MaxTime for every block, a constraint
// that is ideally enforced by SingleUTCDayVerifier.
//
// yoloString safety: the ULIDs recorded in failures come from
// meta.ULID.String(), where ulid.ULID is a fixed-size [16]byte array.
// String() allocates a fresh 26-byte string that is not backed by any pooled
// request buffer, so retaining these strings in the Report (which outlives
// the verifier call) is safe without strings.Clone.
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
// another block's, and returns a bounded summary error naming how many blocks
// and regions overlapped.
func (v *OverlappingBlockVerifier) Verify(_ context.Context, blocks []BlockRef, report *Report) error {
	if len(blocks) < 2 {
		return nil
	}

	sortedBlocks := make([]BlockRef, len(blocks))
	copy(sortedBlocks, blocks)
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

	// total number of overlapping regions, and total number of overlapping blocks.
	regions, overlapping := 0, 0
	flush := func(end int) {
		if end-runStart > 1 {
			v.recordRun(report, sortedBlocks[runStart:end])
			regions++
			overlapping += end - runStart
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

	if regions == 0 {
		return nil
	}
	return fmt.Errorf("%d block(s) overlap another block, across %d region(s)", overlapping, regions)
}

// recordRun records one failure per block in a run of mutually overlapping
// blocks. run must be sorted by MinTime and hold at least two blocks.
//
// The message states how many peers a block overlaps and the bounds of the
// region they jointly cover, but deliberately does not name the peers:
// enumerating them in every message would make the total output quadratic in
// the size of the run. The region bounds identify the run instead.
func (v *OverlappingBlockVerifier) recordRun(report *Report, run []BlockRef) {
	regionMin := run[0].Meta.MinTime
	regionMax := run[0].Meta.MaxTime
	for _, blk := range run[1:] {
		if blk.Meta.MaxTime > regionMax {
			regionMax = blk.Meta.MaxTime
		}
	}

	for _, blk := range run {
		report.Add(blk.Meta.ULID.String(), v.Name(), blk.Dir, fmt.Errorf(
			"block [minTime=%d, maxTime=%d) overlaps %d other block(s) in region [%d, %d)",
			blk.Meta.MinTime, blk.Meta.MaxTime, len(run)-1, regionMin, regionMax))
	}
}
