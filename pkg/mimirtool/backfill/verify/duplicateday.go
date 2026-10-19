// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
)

// DuplicateDayVerifier rejects a batch that contains two or more blocks whose
// [MinTime, MaxTime) ranges share the same UTC-day key. Such blocks should be
// pre-compacted before pushing.
//
// Collisions are only counted between blocks with the same compactor shard ID
// (__compactor_shard_id__), because blocks in different shards legitimately
// cover the same day. Blocks without a shard ID form their own group and are
// never compared with sharded blocks.
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
// block in the same compactor shard, and returns a bounded summary error naming
// how many blocks and distinct days collided.
func (v *DuplicateDayVerifier) Verify(_ context.Context, blocks []BlockRef, report *Report) error {
	if len(blocks) < 2 {
		return nil
	}
	dayToBlocks := make(map[string]map[time.Time][]BlockRef)
	for _, blk := range blocks {
		day := time.UnixMilli(blk.Meta.MinTime).UTC().Truncate(24 * time.Hour)
		shard := getShardID(&blk.Meta)
		if dayToBlocks[shard] == nil {
			dayToBlocks[shard] = make(map[time.Time][]BlockRef)
		}
		dayToBlocks[shard][day] = append(dayToBlocks[shard][day], blk)
	}
	collidingDays := make(map[string][]time.Time)
	for shard, days := range dayToBlocks {
		for day, blks := range days {
			if len(blks) > 1 {
				collidingDays[shard] = append(collidingDays[shard], day)
			}
		}
	}
	if len(collidingDays) == 0 {
		return nil
	}

	// Record shards in ascending order (unsharded first), days in ascending
	// order, and blocks within a day by ULID, so the Report reads the same
	// regardless of input order.
	collided := 0
	distinctDays := make(map[time.Time]struct{})
	for _, shard := range slices.Sorted(maps.Keys(collidingDays)) {
		days := collidingDays[shard]
		slices.SortFunc(days, time.Time.Compare)
		for _, day := range days {
			distinctDays[day] = struct{}{}
			blks := dayToBlocks[shard][day]
			slices.SortFunc(blks, func(a, b BlockRef) int {
				return strings.Compare(a.Meta.ULID.String(), b.Meta.ULID.String())
			})
			for _, blk := range blks {
				if shard == "" {
					report.Add(blk.Meta.ULID.String(), v.Name(), blk.Dir, shard, fmt.Errorf(
						"unsharded block covers UTC day %s, which is also covered by %d other block(s)",
						day.Format("2006-01-02"), len(blks)-1))
				} else {
					report.Add(blk.Meta.ULID.String(), v.Name(), blk.Dir, shard, fmt.Errorf(
						"block in shard %s covers UTC day %s, which is also covered by %d other block(s)",
						shard, day.Format("2006-01-02"), len(blks)-1))
				}
			}
			collided += len(blks)
		}
	}
	return fmt.Errorf("%d block(s) share a UTC day with another block, across %d day(s)",
		collided, len(distinctDays))
}
