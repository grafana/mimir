// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/bucketindex/index_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package bucketindex

import (
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
)

func TestIndex_RemoveBlock(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)
	idx := &Index{
		Blocks:             Blocks{{ID: block1}, {ID: block2}, {ID: block3}},
		BlockDeletionMarks: BlockDeletionMarks{{ID: block2}, {ID: block3}},
	}

	idx.RemoveBlock(block2)
	assert.ElementsMatch(t, []ulid.ULID{block1, block3}, idx.Blocks.GetULIDs())
	assert.ElementsMatch(t, []ulid.ULID{block3}, idx.BlockDeletionMarks.GetULIDs())
}

func TestDetectBlockSegmentsFormat(t *testing.T) {
	tests := map[string]struct {
		meta           metadata.Meta
		expectedFormat string
		expectedNum    int
	}{
		"meta.json without SegmentFiles and Files": {
			meta:           metadata.Meta{},
			expectedFormat: SegmentsFormatUnknown,
			expectedNum:    0,
		},
		"meta.json with SegmentFiles, 0 based 6 digits": {
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					SegmentFiles: []string{
						"000000",
						"000001",
						"000002",
					},
				},
			},
			expectedFormat: SegmentsFormatUnknown,
			expectedNum:    0,
		},
		"meta.json with SegmentFiles, 1 based 6 digits": {
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					SegmentFiles: []string{
						"000001",
						"000002",
						"000003",
					},
				},
			},
			expectedFormat: SegmentsFormat1Based6Digits,
			expectedNum:    3,
		},
		"meta.json with SegmentFiles, 1 based 6 digits but non consecutive": {
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					SegmentFiles: []string{
						"000001",
						"000003",
						"000004",
					},
				},
			},
			expectedFormat: SegmentsFormatUnknown,
			expectedNum:    0,
		},
		"meta.json with Files, 0 based 6 digits": {
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					Files: []metadata.File{
						{RelPath: "index"},
						{RelPath: "chunks/000000"},
						{RelPath: "chunks/000001"},
						{RelPath: "chunks/000002"},
						{RelPath: "tombstone"},
					},
				},
			},
			expectedFormat: SegmentsFormatUnknown,
			expectedNum:    0,
		},
		"meta.json with Files, 1 based 6 digits": {
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					Files: []metadata.File{
						{RelPath: "index"},
						{RelPath: "chunks/000001"},
						{RelPath: "chunks/000002"},
						{RelPath: "chunks/000003"},
						{RelPath: "tombstone"},
					},
				},
			},
			expectedFormat: SegmentsFormat1Based6Digits,
			expectedNum:    3,
		},
		"meta.json with Files, 1 based 6 digits but non consecutive": {
			meta: metadata.Meta{
				Thanos: metadata.Thanos{
					Files: []metadata.File{
						{RelPath: "index"},
						{RelPath: "chunks/000001"},
						{RelPath: "chunks/000003"},
						{RelPath: "chunks/000004"},
						{RelPath: "tombstone"},
					},
				},
			},
			expectedFormat: SegmentsFormatUnknown,
			expectedNum:    0,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualFormat, actualNum := detectBlockSegmentsFormat(testData.meta)
			assert.Equal(t, testData.expectedFormat, actualFormat)
			assert.Equal(t, testData.expectedNum, actualNum)
		})
	}
}

func TestBlockFromThanosMeta(t *testing.T) {
	blockID := ulid.MustNew(1, nil)

	tests := map[string]struct {
		meta     metadata.Meta
		expected Block
	}{
		"meta.json without SegmentFiles and Files": {
			meta: metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    blockID,
					MinTime: 10,
					MaxTime: 20,
				},
				Thanos: metadata.Thanos{},
			},
			expected: Block{
				ID:             blockID,
				MinTime:        10,
				MaxTime:        20,
				SegmentsFormat: SegmentsFormatUnknown,
				SegmentsNum:    0,
			},
		},
		"meta.json with SegmentFiles": {
			meta: metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    blockID,
					MinTime: 10,
					MaxTime: 20,
				},
				Thanos: metadata.Thanos{
					SegmentFiles: []string{
						"000001",
						"000002",
						"000003",
					},
				},
			},
			expected: Block{
				ID:             blockID,
				MinTime:        10,
				MaxTime:        20,
				SegmentsFormat: SegmentsFormat1Based6Digits,
				SegmentsNum:    3,
			},
		},
		"meta.json with Files": {
			meta: metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    blockID,
					MinTime: 10,
					MaxTime: 20,
				},
				Thanos: metadata.Thanos{
					Files: []metadata.File{
						{RelPath: "index"},
						{RelPath: "chunks/000001"},
						{RelPath: "chunks/000002"},
						{RelPath: "chunks/000003"},
						{RelPath: "tombstone"},
					},
				},
			},
			expected: Block{
				ID:             blockID,
				MinTime:        10,
				MaxTime:        20,
				SegmentsFormat: SegmentsFormat1Based6Digits,
				SegmentsNum:    3,
			},
		},
		"meta.json with external labels, no compactor shard ID": {
			meta: metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    blockID,
					MinTime: 10,
					MaxTime: 20,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						"a": "b",
						"c": "d",
					},
				},
			},
			expected: Block{
				ID:      blockID,
				MinTime: 10,
				MaxTime: 20,
			},
		},
		"meta.json with external labels, with compactor shard ID": {
			meta: metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    blockID,
					MinTime: 10,
					MaxTime: 20,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						"a":                                      "b",
						"c":                                      "d",
						mimir_tsdb.CompactorShardIDExternalLabel: "10_of_20",
					},
				},
			},
			expected: Block{
				ID:               blockID,
				MinTime:          10,
				MaxTime:          20,
				CompactorShardID: "10_of_20",
			},
		},
		"meta.json with external labels, with invalid shard ID": {
			meta: metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    blockID,
					MinTime: 10,
					MaxTime: 20,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						"a":                                      "b",
						"c":                                      "d",
						mimir_tsdb.CompactorShardIDExternalLabel: "some weird value",
					},
				},
			},
			expected: Block{
				ID:               blockID,
				MinTime:          10,
				MaxTime:          20,
				CompactorShardID: "some weird value",
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, *BlockFromThanosMeta(testData.meta))
		})
	}
}

func TestBlock_Within(t *testing.T) {
	tests := []struct {
		block    *Block
		minT     int64
		maxT     int64
		expected bool
	}{
		{
			block:    &Block{MinTime: 10, MaxTime: 20},
			minT:     5,
			maxT:     9,
			expected: false,
		}, {
			block:    &Block{MinTime: 10, MaxTime: 20},
			minT:     5,
			maxT:     10,
			expected: true,
		}, {
			block:    &Block{MinTime: 10, MaxTime: 20},
			minT:     5,
			maxT:     10,
			expected: true,
		}, {
			block:    &Block{MinTime: 10, MaxTime: 20},
			minT:     11,
			maxT:     13,
			expected: true,
		}, {
			block:    &Block{MinTime: 10, MaxTime: 20},
			minT:     19,
			maxT:     21,
			expected: true,
		}, {
			block:    &Block{MinTime: 10, MaxTime: 20},
			minT:     20,
			maxT:     21,
			expected: false,
		},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.block.Within(tc.minT, tc.maxT))
	}
}

func TestBlock_ThanosMeta(t *testing.T) {
	blockID := ulid.MustNew(1, nil)

	tests := map[string]struct {
		block    Block
		expected *metadata.Meta
	}{
		"block with segment files format 1 based 6 digits": {
			block: Block{
				ID:             blockID,
				MinTime:        10,
				MaxTime:        20,
				SegmentsFormat: SegmentsFormat1Based6Digits,
				SegmentsNum:    3,
			},
			expected: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    blockID,
					MinTime: 10,
					MaxTime: 20,
					Version: metadata.TSDBVersion1,
				},
				Thanos: metadata.Thanos{
					Version: metadata.ThanosVersion1,
					SegmentFiles: []string{
						"000001",
						"000002",
						"000003",
					},
				},
			},
		},
		"block with unknown segment files format": {
			block: Block{
				ID:             blockID,
				MinTime:        10,
				MaxTime:        20,
				SegmentsFormat: SegmentsFormatUnknown,
				SegmentsNum:    0,
			},
			expected: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    blockID,
					MinTime: 10,
					MaxTime: 20,
					Version: metadata.TSDBVersion1,
				},
				Thanos: metadata.Thanos{
					Version: metadata.ThanosVersion1,
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.block.ThanosMeta())
		})
	}
}

func TestBlockDeletionMark_ThanosDeletionMark(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	mark := &BlockDeletionMark{ID: block1, DeletionTime: 1}

	assert.Equal(t, &metadata.DeletionMark{
		ID:           block1,
		Version:      metadata.DeletionMarkVersion1,
		DeletionTime: 1,
	}, mark.ThanosDeletionMark())
}

func TestBlockDeletionMarks_Clone(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	orig := BlockDeletionMarks{{ID: block1, DeletionTime: 1}, {ID: block2, DeletionTime: 2}}

	// The clone must be identical.
	clone := orig.Clone()
	assert.Equal(t, orig, clone)

	// Changes to the original shouldn't be reflected to the clone.
	orig[0].DeletionTime = -1
	assert.Equal(t, int64(1), clone[0].DeletionTime)
}
