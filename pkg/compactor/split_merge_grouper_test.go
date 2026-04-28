// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestPlanCompaction(t *testing.T) {
	const userID = "user-1"

	block1 := ulid.MustNew(1, nil)   // Hash: 283204220
	block2 := ulid.MustNew(2, nil)   // Hash: 444110359
	block3 := ulid.MustNew(3, nil)   // Hash: 3253786510
	block4 := ulid.MustNew(4, nil)   // Hash: 122298081
	block5 := ulid.MustNew(5, nil)   // Hash: 2931974232
	block6 := ulid.MustNew(6, nil)   // Hash: 3092880371
	block7 := ulid.MustNew(7, nil)   // Hash: 1607589226
	block8 := ulid.MustNew(8, nil)   // Hash: 2771068093
	block9 := ulid.MustNew(9, nil)   // Hash: 1285776948
	block10 := ulid.MustNew(10, nil) // Hash: 1446683087
	nowRounded := time.Now().Truncate(10 * time.Minute)
	minsAdd := func(t time.Duration) int64 {
		return nowRounded.Add(t * time.Minute).UnixMilli()
	}

	tests := map[string]struct {
		ranges        []int64
		shardCount    uint32
		oooShardCount uint32
		splitGroups   uint32
		blocks        []*block.Meta
		expected      []*job
	}{
		"no input blocks": {
			ranges:   []int64{20},
			blocks:   nil,
			expected: nil,
		},
		"should split a single block if == smallest compaction range": {
			ranges:     []int64{20, 40},
			shardCount: 1,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}},
			},
			expected: []*job{
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}},
					},
				}},
			},
		},
		"should split a single block if < smallest compaction range": {
			ranges:     []int64{20, 40},
			shardCount: 1,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
			},
			expected: []*job{
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
					},
				}},
			},
		},
		"should NOT split a single block if == smallest compaction range but configured shards = 0": {
			ranges:     []int64{20, 40},
			shardCount: 0,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}},
			},
			expected: []*job{},
		},
		"should merge and split multiple 1st level blocks within the same time range": {
			ranges:     []int64{10, 20},
			shardCount: 1,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}},
			},
			expected: []*job{
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}},
					},
				}},
			},
		},
		"should merge and split multiple 1st level blocks in different time ranges": {
			ranges:     []int64{10, 20},
			shardCount: 1,
			blocks: []*block.Meta{
				// 1st level range [0, 10]
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 10}},
				// 1st level range [10, 20]
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}},
			},
			expected: []*job{
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   10,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 10}},
					},
				}},
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}},
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}},
					},
				}},
			},
		},
		"should merge and split multiple 1st level blocks in different time ranges, single split group": {
			ranges:      []int64{10, 20},
			shardCount:  2,
			splitGroups: 1,
			blocks: []*block.Meta{
				// 1st level range [0, 10]
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 10}},
				// 1st level range [10, 20]
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}},
			},
			expected: []*job{
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   10,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 10}},
					},
				}},
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}},
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}},
					},
				}},
			},
		},
		"should merge and split multiple 1st level blocks in different time ranges, two split groups": {
			ranges:      []int64{10, 20},
			shardCount:  2,
			splitGroups: 2,
			blocks: []*block.Meta{
				// 1st level range [0, 10]
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 10}},
				// 1st level range [10, 20]
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}},
			},
			expected: []*job{
				{userID: userID, stage: stageSplit, shardID: "1_of_2", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   10,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
					},
				}},
				{userID: userID, stage: stageSplit, shardID: "2_of_2", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   10,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 10}},
					},
				}},
				{userID: userID, stage: stageSplit, shardID: "1_of_2", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}},
					},
				}},
				{userID: userID, stage: stageSplit, shardID: "2_of_2", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}},
					},
				}},
			},
		},
		"should merge but NOT split multiple 1st level blocks in different time ranges if configured shards = 0": {
			ranges:     []int64{10, 20},
			shardCount: 0,
			blocks: []*block.Meta{
				// 1st level range [0, 10]
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 10}},
				// 1st level range [10, 20]
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}},
			},
			expected: []*job{
				{userID: userID, stage: stageMerge, blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   10,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 10}},
					},
				}},
				{userID: userID, stage: stageMerge, blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}},
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}},
					},
				}},
			},
		},
		"should merge split blocks that can be compacted on the 2nd range only": {
			ranges:     []int64{10, 20},
			shardCount: 2,
			blocks: []*block.Meta{
				// 2nd level range [0, 20]
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0, MaxTime: 10}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "2_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "2_of_2"}}},
				// 2nd level range [20, 40]
				{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 20, MaxTime: 30}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 30, MaxTime: 40}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
			},
			expected: []*job{
				{userID: userID, stage: stageMerge, shardID: "1_of_2", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
					},
				}},
				{userID: userID, stage: stageMerge, shardID: "2_of_2", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0, MaxTime: 10}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "2_of_2"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "2_of_2"}}},
					},
				}},
				{userID: userID, stage: stageMerge, shardID: "1_of_2", blocksGroup: blocksGroup{
					rangeStart: 20,
					rangeEnd:   40,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 20, MaxTime: 30}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 30, MaxTime: 40}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
					},
				}},
			},
		},
		"should not split non-split blocks if they're > smallest compaction range (do not split historical blocks after enabling splitting)": {
			ranges:     []int64{10, 20},
			shardCount: 2,
			blocks: []*block.Meta{
				// 2nd level range [0, 20]
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0, MaxTime: 10}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "2_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "2_of_2"}}},
				// 2nd level range [20, 40]
				{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 20, MaxTime: 40}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
			},
			expected: []*job{
				{userID: userID, stage: stageMerge, shardID: "1_of_2", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
					},
				}},
				{userID: userID, stage: stageMerge, shardID: "2_of_2", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0, MaxTime: 10}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "2_of_2"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "2_of_2"}}},
					},
				}},
			},
		},
		"input blocks can be compacted on a mix of 1st and 2nd ranges, guaranteeing no overlaps and giving preference to smaller ranges": {
			ranges:     []int64{10, 20},
			shardCount: 1,
			blocks: []*block.Meta{
				// To be split on 1st level range [0, 10]
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 7, MaxTime: 10}},
				// Not compacted because on 2nd level because the range [0, 20]
				// has other 1st level range groups to be split first
				{BlockMeta: tsdb.BlockMeta{ULID: block10, MinTime: 0, MaxTime: 10}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				// To be compacted on 2nd level range [20, 40]
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 20, MaxTime: 30}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 30, MaxTime: 40}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				// Already compacted on 2nd level range [40, 60]
				{BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 40, MaxTime: 60}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				// Not compacted on 2nd level because the range [60, 80]
				// has other 1st level range groups to be compacted first
				{BlockMeta: tsdb.BlockMeta{ULID: block7, MinTime: 60, MaxTime: 70}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				// To be compacted on 1st level range [70, 80]
				{BlockMeta: tsdb.BlockMeta{ULID: block8, MinTime: 70, MaxTime: 80}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block9, MinTime: 75, MaxTime: 80}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
			},
			expected: []*job{
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   10,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 7, MaxTime: 10}},
					},
				}},
				{userID: userID, stage: stageMerge, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 70,
					rangeEnd:   80,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block8, MinTime: 70, MaxTime: 80}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block9, MinTime: 75, MaxTime: 80}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
					},
				}},
				{userID: userID, stage: stageMerge, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 20,
					rangeEnd:   40,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 20, MaxTime: 30}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 30, MaxTime: 40}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
					},
				}},
			},
		},
		"input blocks have already been compacted with the largest range": {
			ranges:     []int64{10, 20, 40},
			shardCount: 1,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 40}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 40, MaxTime: 70}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 80, MaxTime: 120}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
			},
			expected: nil,
		},
		"input blocks match the largest range but can be compacted because overlapping": {
			ranges:     []int64{10, 20, 40},
			shardCount: 1,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 40}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 40, MaxTime: 70}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 80, MaxTime: 120}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 80, MaxTime: 120}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
			},
			expected: []*job{
				{userID: userID, stage: stageMerge, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 80,
					rangeEnd:   120,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 80, MaxTime: 120}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 80, MaxTime: 120}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
					},
				}},
			},
		},
		"a block with time range crossing two 1st level ranges should be NOT considered for 1st level splitting": {
			ranges:     []int64{20, 40},
			shardCount: 1,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 30}}, // This block spans across two 1st level ranges.
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 20, MaxTime: 30}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 30, MaxTime: 40}},
			},
			expected: []*job{
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
					},
				}},
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 20,
					rangeEnd:   40,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 20, MaxTime: 30}},
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 30, MaxTime: 40}},
					},
				}},
			},
		},
		"a block with time range crossing two 1st level ranges should BE considered for 2nd level compaction": {
			ranges:     []int64{20, 40},
			shardCount: 1,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 30}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}}, // This block spans across two 1st level ranges.
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 20, MaxTime: 40}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
			},
			expected: []*job{
				{userID: userID, stage: stageMerge, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   40,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 30}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 20, MaxTime: 40}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
					},
				}},
			},
		},
		"a block with time range larger then the largest compaction range should NOT be considered for compaction": {
			ranges:     []int64{10, 20, 40},
			shardCount: 1,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 40}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 30, MaxTime: 150}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}}, // This block is larger then the largest compaction range.
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 40, MaxTime: 70}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 80, MaxTime: 120}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 80, MaxTime: 120}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
			},
			expected: []*job{
				{userID: userID, stage: stageMerge, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 80,
					rangeEnd:   120,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 80, MaxTime: 120}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 80, MaxTime: 120}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_1"}}},
					},
				}},
			},
		},
		"a range containing the most recent block shouldn't be prematurely compacted if doesn't cover the full range": {
			ranges:     []int64{10 * time.Minute.Milliseconds(), 20 * time.Minute.Milliseconds(), 40 * time.Minute.Milliseconds()},
			shardCount: 1,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(5), MaxTime: minsAdd(8)}},
				{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(7), MaxTime: minsAdd(9)}},
				{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(10), MaxTime: minsAdd(12)}},
				{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(13), MaxTime: minsAdd(15)}},
			},
			expected: []*job{
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: nowRounded.UnixMilli(),
					rangeEnd:   nowRounded.Add(10 * time.Minute).UnixMilli(),
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(5), MaxTime: minsAdd(8)}},
						{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(7), MaxTime: minsAdd(9)}},
					},
				}},
			},
		},
		"should compact a block that's one job range in the past": {
			ranges:     []int64{10 * time.Minute.Milliseconds(), 20 * time.Minute.Milliseconds(), 40 * time.Minute.Milliseconds()},
			shardCount: 1,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(-18), MaxTime: minsAdd(-12)}},
				{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(-17), MaxTime: minsAdd(-11)}},
				{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(-8), MaxTime: minsAdd(-2)}},
				{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(-7), MaxTime: minsAdd(-1)}},
			},
			expected: []*job{
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: nowRounded.Add(-20 * time.Minute).UnixMilli(),
					rangeEnd:   nowRounded.Add(-10 * time.Minute).UnixMilli(),
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(-18), MaxTime: minsAdd(-12)}},
						{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(-17), MaxTime: minsAdd(-11)}},
					},
				}},
			},
		},
		"should compact a level 1 block": {
			ranges:     []int64{10 * time.Minute.Milliseconds(), 20 * time.Minute.Milliseconds(), 40 * time.Minute.Milliseconds()},
			shardCount: 1,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(5), MaxTime: minsAdd(8), Compaction: tsdb.BlockMetaCompaction{Level: 1}}},
				{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(7), MaxTime: minsAdd(9), Compaction: tsdb.BlockMetaCompaction{Level: 1}}},
			},
			expected: []*job{
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: nowRounded.UnixMilli(),
					rangeEnd:   nowRounded.Add(10 * time.Minute).UnixMilli(),
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(5), MaxTime: minsAdd(8), Compaction: tsdb.BlockMetaCompaction{Level: 1}}},
						{BlockMeta: tsdb.BlockMeta{MinTime: minsAdd(7), MaxTime: minsAdd(9), Compaction: tsdb.BlockMetaCompaction{Level: 1}}},
					},
				}},
			},
		},
		"should not merge blocks within the same time range but with different external labels": {
			ranges:     []int64{10, 20},
			shardCount: 1,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{"another_group": "a"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{"another_group": "a"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{"another_group": "b"}}},
			},
			expected: []*job{
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
					},
				}},
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{"another_group": "a"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{"another_group": "a"}}},
					},
				}},
				{userID: userID, stage: stageSplit, shardID: "1_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*block.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{"another_group": "b"}}},
					},
				}},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := newMockConfigProvider()
			cfg.splitAndMergeShards[userID] = int(testData.shardCount)
			cfg.oooSplitAndMergeShards[userID] = int(testData.oooShardCount)
			cfg.splitGroups[userID] = int(testData.splitGroups)
			actual := planCompaction(userID, testData.blocks, testData.ranges, cfg)

			// Print the actual jobs (useful for debugging if tests fail).
			t.Logf("got %d jobs:", len(actual))
			for _, job := range actual {
				t.Logf("- %s", job.String())
			}

			assert.ElementsMatch(t, testData.expected, actual)
		})
	}
}

func TestPlanSplitting(t *testing.T) {
	const userID = "user-1"

	block1 := ulid.MustNew(1, nil) // Hash: 283204220
	block2 := ulid.MustNew(2, nil) // Hash: 444110359
	block3 := ulid.MustNew(3, nil) // Hash: 3253786510
	block4 := ulid.MustNew(4, nil) // Hash: 122298081
	block5 := ulid.MustNew(5, nil) // Hash: 2931974232

	tests := map[string]struct {
		blocks      blocksGroup
		splitGroups uint32
		expected    []*job
	}{
		"should return nil if the input group is empty": {
			blocks:      blocksGroup{},
			splitGroups: 2,
			expected:    nil,
		},
		"should return nil if the input group contains no non-sharded blocks": {
			blocks: blocksGroup{
				rangeStart: 10,
				rangeEnd:   20,
				blocks: []*block.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
					{BlockMeta: tsdb.BlockMeta{ULID: block2}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "2_of_2"}}},
				},
			},
			splitGroups: 2,
			expected:    nil,
		},
		"should return a split job if the input group contains 1 non-sharded block": {
			blocks: blocksGroup{
				rangeStart: 10,
				rangeEnd:   20,
				blocks: []*block.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
					{BlockMeta: tsdb.BlockMeta{ULID: block2}},
				},
			},
			splitGroups: 2,
			expected: []*job{
				{
					blocksGroup: blocksGroup{
						rangeStart: 10,
						rangeEnd:   20,
						blocks: []*block.Meta{
							{BlockMeta: tsdb.BlockMeta{ULID: block2}},
						},
					},
					userID:  userID,
					stage:   stageSplit,
					shardID: "2_of_2",
				},
			},
		},
		"should splitGroups split jobs if the input group contains multiple non-sharded blocks": {
			blocks: blocksGroup{
				rangeStart: 10,
				rangeEnd:   20,
				blocks: []*block.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
					{BlockMeta: tsdb.BlockMeta{ULID: block2}},
					{BlockMeta: tsdb.BlockMeta{ULID: block3}},
					{BlockMeta: tsdb.BlockMeta{ULID: block4}},
					{BlockMeta: tsdb.BlockMeta{ULID: block5}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2"}}},
				},
			},
			splitGroups: 2,
			expected: []*job{
				{
					blocksGroup: blocksGroup{
						rangeStart: 10,
						rangeEnd:   20,
						blocks: []*block.Meta{
							{BlockMeta: tsdb.BlockMeta{ULID: block3}},
						},
					},
					userID:  userID,
					stage:   stageSplit,
					shardID: "1_of_2",
				}, {
					blocksGroup: blocksGroup{
						rangeStart: 10,
						rangeEnd:   20,
						blocks: []*block.Meta{
							{BlockMeta: tsdb.BlockMeta{ULID: block2}},
							{BlockMeta: tsdb.BlockMeta{ULID: block4}},
						},
					},
					userID:  userID,
					stage:   stageSplit,
					shardID: "2_of_2",
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.ElementsMatch(t, testData.expected, planSplitting(userID, testData.blocks, testData.splitGroups))
		})
	}
}

func TestGroupBlocksByShardID(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)
	block4 := ulid.MustNew(4, nil)

	tests := map[string]struct {
		blocks   []*block.Meta
		expected map[string][]*block.Meta
	}{
		"no input blocks": {
			blocks:   nil,
			expected: map[string][]*block.Meta{},
		},
		"only 1 block in input with shard ID": {
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1"}}},
			},
			expected: map[string][]*block.Meta{
				"1": {
					{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1"}}},
				},
			},
		},
		"only 1 block in input without shard ID": {
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}},
			},
			expected: map[string][]*block.Meta{
				"": {
					{BlockMeta: tsdb.BlockMeta{ULID: block1}},
				},
			},
		},
		"multiple blocks per shard ID": {
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4}},
			},
			expected: map[string][]*block.Meta{
				"": {
					{BlockMeta: tsdb.BlockMeta{ULID: block4}},
				},
				"1": {
					{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1"}}},
					{BlockMeta: tsdb.BlockMeta{ULID: block3}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "1"}}},
				},
				"2": {
					{BlockMeta: tsdb.BlockMeta{ULID: block2}, Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: "2"}}},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, groupBlocksByShardID(testData.blocks))
		})
	}
}

func TestGroupBlocksByRange(t *testing.T) {
	tests := map[string]struct {
		timeRange int64
		blocks    []*block.Meta
		expected  []blocksGroup
	}{
		"no input blocks": {
			timeRange: 20,
			blocks:    nil,
			expected:  nil,
		},
		"only 1 block in input": {
			timeRange: 20,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*block.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				}},
			},
		},
		"only 1 block per range": {
			timeRange: 20,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*block.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
				}},
				{rangeStart: 40, rangeEnd: 60, blocks: []*block.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
				}},
			},
		},
		"multiple blocks per range": {
			timeRange: 20,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 50, MaxTime: 55}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*block.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				}},
				{rangeStart: 40, rangeEnd: 60, blocks: []*block.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 50, MaxTime: 55}},
				}},
			},
		},
		"a block with time range larger then the range should be excluded": {
			timeRange: 20,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}}, // This block is larger then the range.
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*block.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				}},
				{rangeStart: 20, rangeEnd: 40, blocks: []*block.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
				}},
			},
		},
		"blocks with different time ranges but all fitting within the input range": {
			timeRange: 40,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 40, blocks: []*block.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
				}},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, groupBlocksByRange(testData.blocks, testData.timeRange))
		})
	}
}

func TestEffectiveShardCount(t *testing.T) {
	const userID = "user-1"
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	tests := map[string]struct {
		shardCount    int
		oooShardCount int
		blocks        []*block.Meta
		expected      uint32
	}{
		"empty blocks returns shardCount": {
			shardCount:    4,
			oooShardCount: 2,
			blocks:        []*block.Meta{},
			expected:      4,
		},
		"in-order blocks return shardCount": {
			shardCount:    4,
			oooShardCount: 2,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 20}},
			},
			expected: 4,
		},
		"OOO blocks with oooShardCount>0 return oooShardCount": {
			shardCount:    4,
			oooShardCount: 2,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.OutOfOrderExternalLabel: block.OutOfOrderExternalLabelValue}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.OutOfOrderExternalLabel: block.OutOfOrderExternalLabelValue}}},
			},
			expected: 2,
		},
		"OOO blocks with oooShardCount=0 fall back to shardCount": {
			shardCount:    4,
			oooShardCount: 0,
			blocks: []*block.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.OutOfOrderExternalLabel: block.OutOfOrderExternalLabelValue}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.OutOfOrderExternalLabel: block.OutOfOrderExternalLabelValue}}},
			},
			expected: 4,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := newMockConfigProvider()
			cfg.splitAndMergeShards[userID] = testData.shardCount
			cfg.oooSplitAndMergeShards[userID] = testData.oooShardCount
			assert.Equal(t, testData.expected, effectiveShardCount(testData.blocks, userID, cfg))
		})
	}
}

func TestSplitAndMergeGrouper_Groups_OOOShardCount(t *testing.T) {
	const userID = "user-1"

	t.Run("no splitting when shardCount is 0", func(t *testing.T) {
		block1 := ulid.MustNew(1, nil)
		block2 := ulid.MustNew(2, nil)

		cfg := newMockConfigProvider()
		cfg.splitGroups[userID] = 1
		g := NewSplitAndMergeGrouper(userID, []int64{20, 40}, cfg, log.NewNopLogger())
		jobs, err := g.Groups(map[ulid.ULID]*block.Meta{
			block1: {BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}},
			block2: {BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.OutOfOrderExternalLabel: block.OutOfOrderExternalLabelValue}}},
		})
		assert.NoError(t, err)

		for _, job := range jobs {
			assert.False(t, job.UseSplitting(), "expected no split jobs when shardCount is 0")
		}
	})

	t.Run("OOO jobs use regular shardCount when oooShardCount is 0", func(t *testing.T) {
		block1 := ulid.MustNew(1, nil)
		block2 := ulid.MustNew(2, nil)
		block3 := ulid.MustNew(3, nil)
		block4 := ulid.MustNew(4, nil)

		cfg := newMockConfigProvider()
		cfg.splitAndMergeShards[userID] = 8
		cfg.splitGroups[userID] = 1
		g := NewSplitAndMergeGrouper(userID, []int64{20, 40}, cfg, log.NewNopLogger())
		jobs, err := g.Groups(map[ulid.ULID]*block.Meta{
			block1: {BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}},
			block2: {BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 20}},
			block3: {BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.OutOfOrderExternalLabel: block.OutOfOrderExternalLabelValue}}},
			block4: {BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.OutOfOrderExternalLabel: block.OutOfOrderExternalLabelValue}}},
		})
		assert.NoError(t, err)

		for _, job := range jobs {
			if job.UseSplitting() {
				assert.Equal(t, uint32(8), job.SplittingShards(), "both in-order and OOO jobs should use shardCount=8")
			}
		}
	})

	t.Run("OOO jobs use oooShardCount when configured", func(t *testing.T) {
		block1 := ulid.MustNew(1, nil)
		block2 := ulid.MustNew(2, nil)
		block3 := ulid.MustNew(3, nil)
		block4 := ulid.MustNew(4, nil)

		cfg := newMockConfigProvider()
		cfg.splitAndMergeShards[userID] = 8
		cfg.oooSplitAndMergeShards[userID] = 2
		cfg.splitGroups[userID] = 1
		g := NewSplitAndMergeGrouper(userID, []int64{20, 40}, cfg, log.NewNopLogger())
		jobs, err := g.Groups(map[ulid.ULID]*block.Meta{
			block1: {BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}},
			block2: {BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 20}},
			block3: {BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.OutOfOrderExternalLabel: block.OutOfOrderExternalLabelValue}}},
			block4: {BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0, MaxTime: 20}, Thanos: block.ThanosMeta{Labels: map[string]string{block.OutOfOrderExternalLabel: block.OutOfOrderExternalLabelValue}}},
		})
		assert.NoError(t, err)

		for _, job := range jobs {
			if !job.UseSplitting() {
				continue
			}
			metas := job.Metas()
			if len(metas) > 0 && isOOOBlock(metas[0]) {
				assert.Equal(t, uint32(2), job.SplittingShards(), "OOO job should use oooShardCount=2")
			} else {
				assert.Equal(t, uint32(8), job.SplittingShards(), "in-order job should use shardCount=8")
			}
		}
	})
}
