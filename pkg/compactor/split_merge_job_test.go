// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

func TestJob_conflicts(t *testing.T) {
	block1 := &metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(1, nil)}}
	block2 := &metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(2, nil)}}
	block3 := &metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(3, nil)}}
	block4 := &metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(4, nil)}}

	tests := map[string]struct {
		first    *job
		second   *job
		expected bool
	}{
		"should conflict if jobs compact different blocks but with overlapping time ranges and same shard": {
			first: &job{
				stage:   stageMerge,
				shardID: "1",
				blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks:     []*metadata.Meta{block1, block2},
				},
			},
			second: &job{
				stage:   stageMerge,
				shardID: "1",
				blocksGroup: blocksGroup{
					rangeStart: 15,
					rangeEnd:   25,
					blocks:     []*metadata.Meta{block3, block4},
				},
			},
			expected: true,
		},
		"should NOT conflict if jobs compact different blocks with non-overlapping time ranges and same shard": {
			first: &job{
				stage:   stageMerge,
				shardID: "1",
				blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks:     []*metadata.Meta{block1, block2},
				},
			},
			second: &job{
				stage:   stageMerge,
				shardID: "1",
				blocksGroup: blocksGroup{
					rangeStart: 20,
					rangeEnd:   30,
					blocks:     []*metadata.Meta{block3, block4},
				},
			},
			expected: false,
		},
		"should NOT conflict if jobs compact same blocks with overlapping time ranges but different shard": {
			first: &job{
				stage:   stageMerge,
				shardID: "1",
				blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks:     []*metadata.Meta{block1, block2},
				},
			},
			second: &job{
				stage:   stageMerge,
				shardID: "2",
				blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks:     []*metadata.Meta{block1, block2},
				},
			},
			expected: false,
		},
		"should conflict if jobs compact same blocks with overlapping time ranges and different shard but at a different stage": {
			first: &job{
				stage:   stageSplit,
				shardID: "1",
				blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks:     []*metadata.Meta{block1, block2},
				},
			},
			second: &job{
				stage:   stageMerge,
				shardID: "2",
				blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks:     []*metadata.Meta{block1, block2},
				},
			},
			expected: true,
		},
		"should NOT conflict if jobs compact different blocks with overlapping time ranges but different resolution": {
			first: &job{
				stage:   stageMerge,
				shardID: "1",
				blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(1, nil)}, Thanos: metadata.Thanos{Downsample: metadata.ThanosDownsample{Resolution: downsample.ResLevel0}}},
						{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(2, nil)}, Thanos: metadata.Thanos{Downsample: metadata.ThanosDownsample{Resolution: downsample.ResLevel0}}},
					},
				},
			},
			second: &job{
				stage:   stageMerge,
				shardID: "1",
				blocksGroup: blocksGroup{
					rangeStart: 10,
					rangeEnd:   20,
					blocks: []*metadata.Meta{
						{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(3, nil)}, Thanos: metadata.Thanos{Downsample: metadata.ThanosDownsample{Resolution: downsample.ResLevel1}}},
						{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(4, nil)}, Thanos: metadata.Thanos{Downsample: metadata.ThanosDownsample{Resolution: downsample.ResLevel1}}},
					},
				},
			},
			expected: false,
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testCase.expected, testCase.first.conflicts(testCase.second))
			assert.Equal(t, testCase.expected, testCase.second.conflicts(testCase.first))
		})
	}
}

func TestBlocksGroup_overlaps(t *testing.T) {
	tests := []struct {
		first    blocksGroup
		second   blocksGroup
		expected bool
	}{
		{
			first:    blocksGroup{rangeStart: 10, rangeEnd: 20},
			second:   blocksGroup{rangeStart: 20, rangeEnd: 30},
			expected: false,
		}, {
			first:    blocksGroup{rangeStart: 10, rangeEnd: 20},
			second:   blocksGroup{rangeStart: 19, rangeEnd: 30},
			expected: true,
		}, {
			first:    blocksGroup{rangeStart: 10, rangeEnd: 21},
			second:   blocksGroup{rangeStart: 20, rangeEnd: 30},
			expected: true,
		}, {
			first:    blocksGroup{rangeStart: 10, rangeEnd: 20},
			second:   blocksGroup{rangeStart: 12, rangeEnd: 18},
			expected: true,
		},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.first.overlaps(tc.second))
		assert.Equal(t, tc.expected, tc.second.overlaps(tc.first))
	}
}

func TestBlocksGroup_getNonShardedBlocks(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)

	tests := map[string]struct {
		input    blocksGroup
		expected []*metadata.Meta
	}{
		"should return nil if the group is empty": {
			input:    blocksGroup{},
			expected: nil,
		},
		"should return nil if the group contains only sharded blocks": {
			input: blocksGroup{blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1"}}},
			}},
			expected: nil,
		},
		"should return the list of non-sharded blocks if exist in the group": {
			input: blocksGroup{blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3}, Thanos: metadata.Thanos{Labels: map[string]string{"key": "value"}}},
			}},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3}, Thanos: metadata.Thanos{Labels: map[string]string{"key": "value"}}},
			},
		},
		"should consider non-sharded a block with the shard ID label but empty value": {
			input: blocksGroup{blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: ""}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: "1"}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3}, Thanos: metadata.Thanos{Labels: map[string]string{"key": "value"}}},
			}},
			expected: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}, Thanos: metadata.Thanos{Labels: map[string]string{ShardIDLabelName: ""}}},
				{BlockMeta: tsdb.BlockMeta{ULID: block3}, Thanos: metadata.Thanos{Labels: map[string]string{"key": "value"}}},
			},
		},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.input.getNonShardedBlocks())
	}
}
