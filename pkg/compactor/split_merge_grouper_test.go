// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// This is a basic test of Groups(). Planning logic should be tested in TestPlanCompaction.
func TestSplitAndMergeGrouper_Groups(t *testing.T) {
	ranges := []int64{20, 40}

	// Mock some blocks so that each block belongs to a different compactable time range
	// and all of them needs to be splitted.
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)
	block4 := ulid.MustNew(4, nil)

	blocks := map[ulid.ULID]*metadata.Meta{
		block1: {BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}},
		block2: {BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 20, MaxTime: 40}},
		block3: {BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 40, MaxTime: 60}},
		block4: {BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 60, MaxTime: 80}},
	}

	tests := map[string]struct {
		ownJob         ownJobFunc
		expectedGroups int
	}{
		"should return all planned groups if the compactor instance owns all of them": {
			ownJob: func(job *job) (bool, error) {
				return true, nil
			},
			expectedGroups: 4,
		},
		"should return no groups if the compactor instance owns none of them": {
			ownJob: func(job *job) (bool, error) {
				return false, nil
			},
			expectedGroups: 0,
		},
		"should return some groups if the compactor instance owns some of them": {
			ownJob: func() ownJobFunc {
				count := 0
				return func(job *job) (bool, error) {
					count++
					return count%2 == 0, nil
				}
			}(),
			expectedGroups: 2,
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			grouper := NewSplitAndMergeGrouper("test", nil, ranges, 1, testCase.ownJob, log.NewNopLogger(), nil, nil)
			res, err := grouper.Groups(blocks)
			require.NoError(t, err)
			assert.Len(t, res, testCase.expectedGroups)
		})
	}
}

func TestPlanCompaction(t *testing.T) {
	block1 := ulid.MustNew(1, nil) // Hash: 283204220
	block2 := ulid.MustNew(2, nil) // Hash: 444110359
	block3 := ulid.MustNew(3, nil) // Hash: 3253786510
	block4 := ulid.MustNew(4, nil) // Hash: 122298081
	block5 := ulid.MustNew(5, nil) // Hash: 2931974232
	block6 := ulid.MustNew(6, nil) // Hash: 3092880371
	block7 := ulid.MustNew(7, nil) // Hash: 1607589226
	block8 := ulid.MustNew(8, nil) // Hash: 2771068093
	block9 := ulid.MustNew(9, nil) // Hash: 1285776948

	tests := map[string]struct {
		ranges     []int64
		shardCount uint32
		blocks     []*metadata.Meta
		expected   []*job
	}{
		"no input blocks": {
			ranges:   []int64{20},
			blocks:   nil,
			expected: nil,
		},
		"should split a single block if == smallest compaction range": {
			ranges:     []int64{20, 40},
			shardCount: 1,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}},
			},
			expected: []*job{
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}},
					},
				}},
			},
		},
		"should split a single block if < smallest compaction range": {
			ranges:     []int64{20, 40},
			shardCount: 1,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
			},
			expected: []*job{
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
					},
				}},
			},
		},
		"should merge and split multiple 1st level blocks within the same time range": {
			ranges:     []int64{10, 20},
			shardCount: 1,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}},
			},
			expected: []*job{
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}},
					},
				}},
			},
		},
		"should merge and split multiple 1st level blocks in different time ranges": {
			ranges:     []int64{10, 20},
			shardCount: 1,
			blocks: []*metadata.Meta{
				// 1st level range [0, 10]
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 10}},
				// 1st level range [10, 20]
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}},
			},
			expected: []*job{
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   10,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 10}},
					},
				}},
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}},
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}},
					},
				}},
			},
		},
		"should merge and split multiple 1st level blocks in different time ranges, honoring the number of shards": {
			ranges:     []int64{10, 20},
			shardCount: 2,
			blocks: []*metadata.Meta{
				// 1st level range [0, 10]
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 10}},
				// 1st level range [10, 20]
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}},
			},
			expected: []*job{
				{stage: stageSplit, shardID: "0_of_2", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   10,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
					},
				}},
				{stage: stageSplit, shardID: "1_of_2", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   10,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0, MaxTime: 10}},
					},
				}},
				{stage: stageSplit, shardID: "0_of_2", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}},
					},
				}},
				{stage: stageSplit, shardID: "1_of_2", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}},
					},
				}},
			},
		},
		"should merge splitted blocks that can be compacted on the 2nd range only": {
			ranges:     []int64{10, 20},
			shardCount: 2,
			blocks: []*metadata.Meta{
				// 2nd level range [0, 20]
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1_of_2"}}},
				// 2nd level range [20, 40]
				{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 20, MaxTime: 30}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 30, MaxTime: 40}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
			},
			expected: []*job{
				{stage: stageMerge, shardID: "0_of_2", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
					},
				}},
				{stage: stageMerge, shardID: "1_of_2", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1_of_2"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1_of_2"}}},
					},
				}},
				{stage: stageMerge, shardID: "0_of_2", blocksGroup: blocksGroup{
					rangeStart: 20,
					rangeEnd:   40,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 20, MaxTime: 30}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 30, MaxTime: 40}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
					},
				}},
			},
		},
		"should not split non-splitted blocks if they're > smallest compaction range (do not split historical blocks after enabling splitting)": {
			ranges:     []int64{10, 20},
			shardCount: 2,
			blocks: []*metadata.Meta{
				// 2nd level range [0, 20]
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1_of_2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1_of_2"}}},
				// 2nd level range [20, 40]
				{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 20, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 20, MaxTime: 40}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
			},
			expected: []*job{
				{stage: stageMerge, shardID: "0_of_2", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
					},
				}},
				{stage: stageMerge, shardID: "1_of_2", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0, MaxTime: 10}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1_of_2"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1_of_2"}}},
					},
				}},
			},
		},
		"input blocks can be compacted on a mix of 1st and 2nd ranges, guaranteeing no overlaps and giving preference to smaller ranges": {
			ranges:     []int64{10, 20},
			shardCount: 1,
			blocks: []*metadata.Meta{
				// To be splitted on 1st level range [0, 10]
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 7, MaxTime: 10}},
				// Not compacted because on 2nd level because the range [0, 20]
				// has other 1st level range groups to be splitted first
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				// To be compacted on 2nd level range [20, 40]
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 20, MaxTime: 30}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 30, MaxTime: 40}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				// Already compacted on 2nd level range [40, 60]
				{BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 40, MaxTime: 60}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				// Not compacted on 2nd level because the range [60, 80]
				// has other 1st level range groups to be compacted first
				{BlockMeta: tsdb.BlockMeta{ULID: block7, MinTime: 60, MaxTime: 70}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				// To be compacted on 1st level range [70, 80]
				{BlockMeta: tsdb.BlockMeta{ULID: block8, MinTime: 70, MaxTime: 80}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block9, MinTime: 75, MaxTime: 80}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
			},
			expected: []*job{
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   10,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 10}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 7, MaxTime: 10}},
					},
				}},
				{stage: stageMerge, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 70,
					rangeEnd:   80,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block8, MinTime: 70, MaxTime: 80}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block9, MinTime: 75, MaxTime: 80}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
					},
				}},
				{stage: stageMerge, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 20,
					rangeEnd:   40,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 20, MaxTime: 30}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 30, MaxTime: 40}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
					},
				}},
			},
		},
		"input blocks have already been compacted with the largest range": {
			ranges:     []int64{10, 20, 40},
			shardCount: 1,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 40}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 40, MaxTime: 70}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
			},
			expected: nil,
		},
		"input blocks match the largest range but can be compacted because overlapping": {
			ranges:     []int64{10, 20, 40},
			shardCount: 1,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 40}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 40, MaxTime: 70}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
			},
			expected: []*job{
				{stage: stageMerge, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 80,
					rangeEnd:   120,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
					},
				}},
			},
		},
		"a block with time range crossing two 1st level ranges should be NOT considered for 1st level splitting": {
			ranges:     []int64{20, 40},
			shardCount: 1,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 30}}, // This block spans across two 1st level ranges.
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 20, MaxTime: 30}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 30, MaxTime: 40}},
			},
			expected: []*job{
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
					},
				}},
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 20,
					rangeEnd:   40,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 20, MaxTime: 30}},
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 30, MaxTime: 40}},
					},
				}},
			},
		},
		"a block with time range crossing two 1st level ranges should BE considered for 2nd level compaction": {
			ranges:     []int64{20, 40},
			shardCount: 1,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 30}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}}, // This block spans across two 1st level ranges.
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 20, MaxTime: 40}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
			},
			expected: []*job{
				{stage: stageMerge, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   40,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 30}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 20, MaxTime: 40}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
					},
				}},
			},
		},
		"a block with time range larger then the largest compaction range should NOT be considered for compaction": {
			ranges:     []int64{10, 20, 40},
			shardCount: 1,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0, MaxTime: 40}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 30, MaxTime: 150}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}}, // This block is larger then the largest compaction range.
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 40, MaxTime: 70}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
			},
			expected: []*job{
				{stage: stageMerge, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 80,
					rangeEnd:   120,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 80, MaxTime: 120}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_1"}}},
					},
				}},
			},
		},
		"a range containing the most recent block shouldn't be prematurely compacted if doesn't cover the full range": {
			ranges:     []int64{10, 20, 40},
			shardCount: 1,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 5, MaxTime: 8}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 7, MaxTime: 9}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 12}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 13, MaxTime: 15}},
			},
			expected: []*job{
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 0,
					rangeEnd:   10,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{MinTime: 5, MaxTime: 8}},
						{BlockMeta: tsdb.BlockMeta{MinTime: 7, MaxTime: 9}},
					},
				}},
			},
		},
		"should not merge blocks within the same time range but with different external labels": {
			ranges:     []int64{10, 20},
			shardCount: 1,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{"another_group": "a"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{"another_group": "a"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{"another_group": "b"}}},
			},
			expected: []*job{
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
					},
				}},
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{"another_group": "a"}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{"another_group": "a"}}},
					},
				}},
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Labels: map[string]string{"another_group": "b"}}},
					},
				}},
			},
		},
		"should not merge blocks within the same time range and with same external labels but different resolution": {
			ranges:     []int64{10, 20},
			shardCount: 1,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Downsample: metadata.ThanosDownsample{Resolution: downsample.ResLevel0}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Downsample: metadata.ThanosDownsample{Resolution: downsample.ResLevel1}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Downsample: metadata.ThanosDownsample{Resolution: downsample.ResLevel1}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Downsample: metadata.ThanosDownsample{Resolution: downsample.ResLevel2}}},
			},
			expected: []*job{
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10, MaxTime: 20}},
					},
				}},
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Downsample: metadata.ThanosDownsample{Resolution: downsample.ResLevel1}}},
						{BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Downsample: metadata.ThanosDownsample{Resolution: downsample.ResLevel1}}},
					},
				}},
				{stage: stageSplit, shardID: "0_of_1", blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10, MaxTime: 20}, Thanos: metadata.Thanos{Downsample: metadata.ThanosDownsample{Resolution: downsample.ResLevel2}}},
					},
				}},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := planCompaction(testData.blocks, testData.ranges, testData.shardCount)

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
	block1 := ulid.MustNew(1, nil) // Hash: 283204220
	block2 := ulid.MustNew(2, nil) // Hash: 444110359
	block3 := ulid.MustNew(3, nil) // Hash: 3253786510
	block4 := ulid.MustNew(4, nil) // Hash: 122298081
	block5 := ulid.MustNew(5, nil) // Hash: 2931974232

	tests := map[string]struct {
		blocks     blocksGroup
		shardCount uint32
		expected   []*job
	}{
		"should return nil if the input group is empty": {
			blocks:     blocksGroup{},
			shardCount: 2,
			expected:   nil,
		},
		"should return nil if the input group contains no non-sharded blocks": {
			blocks: blocksGroup{
				rangeStart: 10,
				rangeEnd:   20,
				blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
					{BlockMeta: tsdb.BlockMeta{ULID: block2}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1_of_2"}}},
				},
			},
			shardCount: 2,
			expected:   nil,
		},
		"should return a split job if the input group contains 1 non-sharded block": {
			blocks: blocksGroup{
				rangeStart: 10,
				rangeEnd:   20,
				blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
					{BlockMeta: tsdb.BlockMeta{ULID: block2}},
				},
			},
			shardCount: 2,
			expected: []*job{
				{
					blocksGroup: blocksGroup{
						rangeStart: 10,
						rangeEnd:   20,
						blocks: []*metadata.Meta{
							{BlockMeta: tsdb.BlockMeta{ULID: block2}},
						},
					},
					stage:   stageSplit,
					shardID: "1_of_2",
				},
			},
		},
		"should shardCount split jobs if the input group contains multiple non-sharded blocks": {
			blocks: blocksGroup{
				rangeStart: 10,
				rangeEnd:   20,
				blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
					{BlockMeta: tsdb.BlockMeta{ULID: block2}},
					{BlockMeta: tsdb.BlockMeta{ULID: block3}},
					{BlockMeta: tsdb.BlockMeta{ULID: block4}},
					{BlockMeta: tsdb.BlockMeta{ULID: block5}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "0_of_2"}}},
				},
			},
			shardCount: 2,
			expected: []*job{
				{
					blocksGroup: blocksGroup{
						rangeStart: 10,
						rangeEnd:   20,
						blocks: []*metadata.Meta{
							{BlockMeta: tsdb.BlockMeta{ULID: block3}},
						},
					},
					stage:   stageSplit,
					shardID: "0_of_2",
				}, {
					blocksGroup: blocksGroup{
						rangeStart: 10,
						rangeEnd:   20,
						blocks: []*metadata.Meta{
							{BlockMeta: tsdb.BlockMeta{ULID: block2}},
							{BlockMeta: tsdb.BlockMeta{ULID: block4}},
						},
					},
					stage:   stageSplit,
					shardID: "1_of_2",
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.ElementsMatch(t, testData.expected, planSplitting(testData.blocks, testData.shardCount))
		})
	}
}

func TestGroupBlocksByShardID(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)
	block4 := ulid.MustNew(4, nil)

	tests := map[string]struct {
		blocks   []*metadata.Meta
		expected map[string][]*metadata.Meta
	}{
		"no input blocks": {
			blocks:   nil,
			expected: map[string][]*metadata.Meta{},
		},
		"only 1 block in input with shard ID": {
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1"}}},
			},
			expected: map[string][]*metadata.Meta{
				"1": {
					{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1"}}},
				},
			},
		},
		"only 1 block in input without shard ID": {
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}},
			},
			expected: map[string][]*metadata.Meta{
				"": {
					{BlockMeta: tsdb.BlockMeta{ULID: block1}},
				},
			},
		},
		"multiple blocks per shard ID": {
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "2"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block4}},
			},
			expected: map[string][]*metadata.Meta{
				"": {
					{BlockMeta: tsdb.BlockMeta{ULID: block4}},
				},
				"1": {
					{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1"}}},
					{BlockMeta: tsdb.BlockMeta{ULID: block3}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1"}}},
				},
				"2": {
					{BlockMeta: tsdb.BlockMeta{ULID: block2}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "2"}}},
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
		blocks    []*metadata.Meta
		expected  []blocksGroup
	}{
		"no input blocks": {
			timeRange: 20,
			blocks:    nil,
			expected:  nil,
		},
		"only 1 block in input": {
			timeRange: 20,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				}},
			},
		},
		"only 1 block per range": {
			timeRange: 20,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
				}},
				{rangeStart: 40, rangeEnd: 60, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
				}},
			},
		},
		"multiple blocks per range": {
			timeRange: 20,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 50, MaxTime: 55}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				}},
				{rangeStart: 40, rangeEnd: 60, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 50, MaxTime: 55}},
				}},
			},
		},
		"a block with time range larger then the range should be excluded": {
			timeRange: 20,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}}, // This block is larger then the range.
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				}},
				{rangeStart: 20, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
				}},
			},
		},
		"blocks with different time ranges but all fitting within the input range": {
			timeRange: 40,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 40, blocks: []*metadata.Meta{
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
