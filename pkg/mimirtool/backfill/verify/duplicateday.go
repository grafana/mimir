// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/go-kit/log"
)

// DuplicateDayVerifier rejects a batch in which a UTC day holds more than one
// block unless every block on that day is sharded with the same shard count and
// no shard repeats. Such blocks should be pre-compacted before pushing.
//
// The check compares shard labels only and does not verify that the series in a
// block hash into the shard its label names.
//
// DuplicateDayVerifier only checks min-times, so it should also be run
// alongside the SingleUTCDayVerifier.
//
// yoloString safety: the ULIDs recorded in failures come from
// meta.ULID.String(), where ulid.ULID is a fixed-size [16]byte array. String()
// allocates a fresh 26-byte string that is not backed by any pooled request
// buffer, so retaining these strings in the Report (which outlives the verifier
// call) is safe without strings.Clone. Shard IDs come from meta.json read off
// local disk, not from a pooled request buffer, so they are safe to retain for
// the same reason.
type DuplicateDayVerifier struct {
	logger log.Logger
}

// NewDuplicateDayVerifier constructs a DuplicateDayVerifier.
func NewDuplicateDayVerifier(logger log.Logger) *DuplicateDayVerifier {
	return &DuplicateDayVerifier{logger: logger}
}

// Name returns the stable check name used in log lines and Report entries.
func (v *DuplicateDayVerifier) Name() string { return "duplicate-day" }

// utcDayOf returns the UTC day, as midnight, that contains the block's MinTime.
func utcDayOf(b *BlockRef) time.Time {
	return time.UnixMilli(b.Meta.MinTime).UTC().Truncate(24 * time.Hour)
}

// Verify records one failure per block on a UTC day that mixes unsharded and
// sharded blocks or shard counts, and one failure per block whose shard repeats
// on its day. It returns a bounded summary error naming how many blocks and
// distinct days were flagged.
func (v *DuplicateDayVerifier) Verify(_ context.Context, blocks []BlockRef, report *Report) error {
	if len(blocks) < 2 {
		return nil
	}

	refs := newShardedRefs(blocks)
	slices.SortFunc(refs, func(a, b shardedRef) int {
		return cmp.Or(
			utcDayOf(a.ref).Compare(utcDayOf(b.ref)),
			compareBlockShards(a.shard, b.shard),
			a.ref.Meta.ULID.Compare(b.ref.Meta.ULID),
		)
	})

	flagged, flaggedDays := 0, 0
	for start := 0; start < len(refs); {
		day := utcDayOf(refs[start].ref)
		end := start + 1
		for end < len(refs) && utcDayOf(refs[end].ref).Equal(day) {
			end++
		}
		group := refs[start:end]
		start = end
		if len(group) < 2 {
			continue
		}

		n := 0
		switch mix := classifyShardMix(group); mix.kind {
		case shardMixUnsharded, shardMixCounts:
			for _, r := range group {
				report.Add(r.ref.Meta.ULID.String(), v.Name(), r.ref.Dir, r.shard.id, fmt.Errorf(
					"%s covers UTC day %s, which %s", shardLabel(r.shard), day.Format("2006-01-02"), mix))
			}
			n = len(group)
		case shardMixNone:
			// Every block shares one shard count, so only repeated shards collide.
			for s := 0; s < len(group); {
				e := s + 1
				for e < len(group) && compareBlockShards(group[s].shard, group[e].shard) == 0 {
					e++
				}
				if e-s > 1 {
					for _, r := range group[s:e] {
						report.Add(r.ref.Meta.ULID.String(), v.Name(), r.ref.Dir, r.shard.id, fmt.Errorf(
							"%s covers UTC day %s, which is also covered by %d other block(s)",
							shardLabel(r.shard), day.Format("2006-01-02"), e-s-1))
					}
					n += e - s
				}
				s = e
			}
		}
		if n > 0 {
			flagged += n
			flaggedDays++
		}
	}

	if flagged == 0 {
		return nil
	}
	return fmt.Errorf("%d block(s) share a UTC day with another block, across %d day(s)",
		flagged, flaggedDays)
}
