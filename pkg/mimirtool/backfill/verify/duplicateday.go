// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/go-kit/log"
)

// DuplicateDayVerifier rejects a batch that contains two or more blocks whose
// [MinTime, MaxTime) ranges share the same UTC-day key. Such blocks should be
// pre-compacted before pushing.
//
// DuplicateDayVerifier only checks min-times, so it should also be run
// alongside the SingleUTCDayVerifier.
//
// yoloString safety: the ULIDs recorded in failures come from
// meta.ULID.String(), where ulid.ULID is a fixed-size [16]byte array. String()
// allocates a fresh 26-byte string that is not backed by any pooled request
// buffer, so retaining these strings in the Report (which outlives the
// verifier call) is safe without strings.Clone.
type DuplicateDayVerifier struct {
	logger log.Logger
}

// NewDuplicateDayVerifier constructs a DuplicateDayVerifier.
func NewDuplicateDayVerifier(logger log.Logger) *DuplicateDayVerifier {
	return &DuplicateDayVerifier{logger: logger}
}

// Name returns the stable check name used in log lines and Report entries.
func (v *DuplicateDayVerifier) Name() string { return "duplicate-day" }

// Verify records one failure per block that shares its UTC day with another
// block, and returns a bounded summary error naming how many blocks and days
// collided.
func (v *DuplicateDayVerifier) Verify(_ context.Context, blocks []BlockRef, report *Report) error {
	if len(blocks) < 2 {
		return nil
	}
	dayToBlocks := make(map[int64][]BlockRef, len(blocks))
	for _, blk := range blocks {
		day := blk.Meta.MinTime / msPerDay
		dayToBlocks[day] = append(dayToBlocks[day], blk)
	}
	var collidingDays []int64
	for day, blks := range dayToBlocks {
		if len(blks) > 1 {
			collidingDays = append(collidingDays, day)
		}
	}
	if len(collidingDays) == 0 {
		return nil
	}

	// Record days in ascending order, and blocks within a day by ULID, so the
	// Report reads the same regardless of input order.
	slices.Sort(collidingDays)
	collided := 0
	for _, day := range collidingDays {
		blks := dayToBlocks[day]
		slices.SortFunc(blks, func(a, b BlockRef) int {
			return strings.Compare(a.Meta.ULID.String(), b.Meta.ULID.String())
		})
		for _, blk := range blks {
			report.Add(blk.Meta.ULID.String(), v.Name(), blk.Dir, fmt.Errorf(
				"block covers UTC day %d, which is also covered by %d other block(s)",
				day, len(blks)-1))
		}
		collided += len(blks)
	}
	return fmt.Errorf("%d block(s) share a UTC day with another block, across %d day(s)",
		collided, len(collidingDays))
}
