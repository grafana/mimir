// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

func TestMultitenantCompactor_ShouldSupportSplitAndMergeCompactor(t *testing.T) {
	const (
		userID     = "user-1"
		numSeries  = 100
		blockRange = 2 * time.Hour
	)

	var (
		blockRangeMillis = blockRange.Milliseconds()
		compactionRanges = mimir_tsdb.DurationList{blockRange, 2 * blockRange, 4 * blockRange}
	)

	externalLabels := func(shardID string) map[string]string {
		labels := map[string]string{}

		if shardID != "" {
			labels[mimir_tsdb.CompactorShardIDExternalLabel] = shardID
		}
		return labels
	}

	externalLabelsWithTenantID := func(shardID string) map[string]string {
		labels := externalLabels(shardID)
		labels[mimir_tsdb.DeprecatedTenantIDExternalLabel] = userID
		return labels
	}

	tests := map[string]struct {
		numShards int
		setup     func(t *testing.T, bkt objstore.Bucket) []metadata.Meta
	}{
		"overlapping blocks matching the 1st compaction range should be merged and split": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []metadata.Meta {
				block1 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, numSeries, externalLabels(""))
				block2 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, numSeries, externalLabels(""))

				return []metadata.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 1 * blockRangeMillis,
							MaxTime: 2 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "1_of_2",
							},
						},
					}, {
						BlockMeta: tsdb.BlockMeta{
							MinTime: 1 * blockRangeMillis,
							MaxTime: 2 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
				}
			},
		},
		"overlapping blocks matching the 1st compaction range with mixed tenant ID labels should be merged and split": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []metadata.Meta {
				block1 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, numSeries, externalLabels(""))             // Doesn't have __org_id__ label
				block2 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, numSeries, externalLabelsWithTenantID("")) // Has __org_id__ label

				return []metadata.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 1 * blockRangeMillis,
							MaxTime: 2 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "1_of_2",
							},
						},
					}, {
						BlockMeta: tsdb.BlockMeta{
							MinTime: 1 * blockRangeMillis,
							MaxTime: 2 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
				}
			},
		},
		"overlapping blocks matching the beginning of the 1st compaction range should be merged and split": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []metadata.Meta {
				block1 := createTSDBBlock(t, bkt, userID, 0, (5 * time.Minute).Milliseconds(), numSeries, externalLabels(""))
				block2 := createTSDBBlock(t, bkt, userID, time.Minute.Milliseconds(), (7 * time.Minute).Milliseconds(), numSeries, externalLabels(""))

				// Add another block as "most recent one" otherwise the previous blocks are not compacted
				// because the most recent blocks must cover the full range to be compacted.
				block3 := createTSDBBlock(t, bkt, userID, blockRangeMillis, blockRangeMillis+time.Minute.Milliseconds(), numSeries, externalLabels(""))

				return []metadata.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (7 * time.Minute).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "1_of_2",
							},
						},
					}, {
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (7 * time.Minute).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					}, {
						// Not compacted.
						BlockMeta: tsdb.BlockMeta{
							MinTime: blockRangeMillis,
							MaxTime: blockRangeMillis + time.Minute.Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block3},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{},
						},
					},
				}
			},
		},
		"non-overlapping blocks matching the beginning of the 1st compaction range (without gaps) should be merged and split": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []metadata.Meta {
				block1 := createTSDBBlock(t, bkt, userID, 0, (5 * time.Minute).Milliseconds(), numSeries, externalLabels(""))
				block2 := createTSDBBlock(t, bkt, userID, (5 * time.Minute).Milliseconds(), (10 * time.Minute).Milliseconds(), numSeries, externalLabels(""))

				// Add another block as "most recent one" otherwise the previous blocks are not compacted
				// because the most recent blocks must cover the full range to be compacted.
				block3 := createTSDBBlock(t, bkt, userID, blockRangeMillis, blockRangeMillis+time.Minute.Milliseconds(), numSeries, externalLabels(""))

				return []metadata.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (10 * time.Minute).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "1_of_2",
							},
						},
					}, {
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (10 * time.Minute).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					}, {
						// Not compacted.
						BlockMeta: tsdb.BlockMeta{
							MinTime: blockRangeMillis,
							MaxTime: blockRangeMillis + time.Minute.Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block3},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{},
						},
					},
				}
			},
		},
		"non-overlapping blocks matching the beginning of the 1st compaction range (with gaps) should be merged and split": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []metadata.Meta {
				block1 := createTSDBBlock(t, bkt, userID, 0, (5 * time.Minute).Milliseconds(), numSeries, externalLabels(""))
				block2 := createTSDBBlock(t, bkt, userID, (7 * time.Minute).Milliseconds(), (10 * time.Minute).Milliseconds(), numSeries, externalLabels(""))

				// Add another block as "most recent one" otherwise the previous blocks are not compacted
				// because the most recent blocks must cover the full range to be compacted.
				block3 := createTSDBBlock(t, bkt, userID, blockRangeMillis, blockRangeMillis+time.Minute.Milliseconds(), numSeries, externalLabels(""))

				return []metadata.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (10 * time.Minute).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "1_of_2",
							},
						},
					}, {
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (10 * time.Minute).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					}, {
						// Not compacted.
						BlockMeta: tsdb.BlockMeta{
							MinTime: blockRangeMillis,
							MaxTime: blockRangeMillis + time.Minute.Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block3},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{},
						},
					},
				}
			},
		},
		"smaller compaction ranges should take precedence over larger ones, and then re-iterate in subsequent compactions of increasing ranges": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []metadata.Meta {
				// Two split blocks in the 1st compaction range.
				block1a := createTSDBBlock(t, bkt, userID, 1, blockRangeMillis, numSeries, externalLabels("1_of_2"))
				block1b := createTSDBBlock(t, bkt, userID, 1, blockRangeMillis, numSeries, externalLabels("2_of_2"))

				// Two non-split overlapping blocks in the 1st compaction range.
				block2 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, numSeries, externalLabels(""))
				block3 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, numSeries, externalLabels(""))

				// Two split adjacent blocks in the 2nd compaction range.
				block4a := createTSDBBlock(t, bkt, userID, 2*blockRangeMillis, 3*blockRangeMillis, numSeries, externalLabels("1_of_2"))
				block4b := createTSDBBlock(t, bkt, userID, 2*blockRangeMillis, 3*blockRangeMillis, numSeries, externalLabels("2_of_2"))
				block5a := createTSDBBlock(t, bkt, userID, 3*blockRangeMillis, 4*blockRangeMillis, numSeries, externalLabels("1_of_2"))
				block5b := createTSDBBlock(t, bkt, userID, 3*blockRangeMillis, 4*blockRangeMillis, numSeries, externalLabels("2_of_2"))

				// Two non-adjacent non-split blocks in the 1st compaction range.
				block6 := createTSDBBlock(t, bkt, userID, 4*blockRangeMillis, 5*blockRangeMillis, numSeries, externalLabels(""))
				block7 := createTSDBBlock(t, bkt, userID, 7*blockRangeMillis, 8*blockRangeMillis, numSeries, externalLabels(""))

				return []metadata.Meta{
					// The two overlapping blocks (block2, block3) have been merged and split in the 1st range,
					// and then compacted with block1 in 2nd range. Finally, they've been compacted with
					// block4 and block5 in the 3rd range compaction (total levels: 4).
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 1,
							MaxTime: 4 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1a, block2, block3, block4a, block5a},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "1_of_2",
							},
						},
					}, {
						BlockMeta: tsdb.BlockMeta{
							MinTime: 1,
							MaxTime: 4 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1b, block2, block3, block4b, block5b},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
					// The two non-adjacent blocks block6 and block7 are split individually first and then merged
					// together in the 3rd range.
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 4 * blockRangeMillis,
							MaxTime: 8 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block6, block7},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "1_of_2",
							},
						},
					}, {
						BlockMeta: tsdb.BlockMeta{
							MinTime: 4 * blockRangeMillis,
							MaxTime: 8 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block6, block7},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
				}
			},
		},
		"overlapping and non-overlapping blocks within the same range should be split and compacted together": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []metadata.Meta {
				// Overlapping.
				block1 := createTSDBBlock(t, bkt, userID, 0, (5 * time.Minute).Milliseconds(), numSeries, externalLabels(""))
				block2 := createTSDBBlock(t, bkt, userID, time.Minute.Milliseconds(), (7 * time.Minute).Milliseconds(), numSeries, externalLabels(""))

				// Not overlapping.
				block3 := createTSDBBlock(t, bkt, userID, time.Hour.Milliseconds(), (2 * time.Hour).Milliseconds(), numSeries, externalLabels(""))

				return []metadata.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (2 * time.Hour).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2, block3},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "1_of_2",
							},
						},
					}, {
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (2 * time.Hour).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2, block3},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
				}
			},
		},
		"overlapping and non-overlapping blocks within the same range and mixed tenant ID label should be split and compacted together": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []metadata.Meta {
				// Overlapping.
				block1 := createTSDBBlock(t, bkt, userID, 0, (5 * time.Minute).Milliseconds(), numSeries, externalLabels(""))                                      // Without __org_id__ label
				block2 := createTSDBBlock(t, bkt, userID, time.Minute.Milliseconds(), (7 * time.Minute).Milliseconds(), numSeries, externalLabelsWithTenantID("")) // With __org_id__ label

				// Not overlapping.
				block3 := createTSDBBlock(t, bkt, userID, time.Hour.Milliseconds(), (2 * time.Hour).Milliseconds(), numSeries, externalLabelsWithTenantID("")) // With __org_id__ label

				return []metadata.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (2 * time.Hour).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2, block3},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "1_of_2",
							},
						},
					}, {
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (2 * time.Hour).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2, block3},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
				}
			},
		},
		"should correctly handle empty blocks generated in the splitting stage": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []metadata.Meta {
				// Generate a block with only 1 series. This block will be split into 1 split block only,
				// because the source block only has 1 series.
				block1 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, 1, externalLabels(""))

				return []metadata.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: (2 * blockRangeMillis) - 1, // Because there's only 1 sample with timestamp=maxT-1
							MaxTime: 2 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "1_of_2",
							},
						},
					},
				}
			},
		},
		"splitting should be disabled if configured shards = 0": {
			numShards: 0,
			setup: func(t *testing.T, bkt objstore.Bucket) []metadata.Meta {
				block1 := createTSDBBlock(t, bkt, userID, 0, (5 * time.Minute).Milliseconds(), numSeries, externalLabels(""))
				block2 := createTSDBBlock(t, bkt, userID, (5 * time.Minute).Milliseconds(), (10 * time.Minute).Milliseconds(), numSeries, externalLabels(""))

				// Add another block as "most recent one" otherwise the previous blocks are not compacted
				// because the most recent blocks must cover the full range to be compacted.
				block3 := createTSDBBlock(t, bkt, userID, blockRangeMillis, blockRangeMillis+time.Minute.Milliseconds(), numSeries, externalLabels(""))

				return []metadata.Meta{
					// Compacted but not split.
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (10 * time.Minute).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{},
						},
					}, {
						// Not compacted.
						BlockMeta: tsdb.BlockMeta{
							MinTime: blockRangeMillis,
							MaxTime: blockRangeMillis + time.Minute.Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block3},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{},
						},
					},
				}
			},
		},
		"splitting should be disabled but already split blocks should be merged correctly (respecting the shard) if configured shards = 0": {
			numShards: 0,
			setup: func(t *testing.T, bkt objstore.Bucket) []metadata.Meta {
				// Two split blocks in the 1st compaction range.
				block1a := createTSDBBlock(t, bkt, userID, 1, blockRangeMillis, numSeries, externalLabels("1_of_2"))
				block1b := createTSDBBlock(t, bkt, userID, 1, blockRangeMillis, numSeries, externalLabels("2_of_2"))

				// Two non-split overlapping blocks in the 1st compaction range.
				block2 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, numSeries, externalLabels(""))
				block3 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, numSeries, externalLabels(""))

				// Two split adjacent blocks in the 2nd compaction range.
				block4a := createTSDBBlock(t, bkt, userID, 2*blockRangeMillis, 3*blockRangeMillis, numSeries, externalLabels("1_of_2"))
				block4b := createTSDBBlock(t, bkt, userID, 2*blockRangeMillis, 3*blockRangeMillis, numSeries, externalLabels("2_of_2"))
				block5a := createTSDBBlock(t, bkt, userID, 3*blockRangeMillis, 4*blockRangeMillis, numSeries, externalLabels("1_of_2"))
				block5b := createTSDBBlock(t, bkt, userID, 3*blockRangeMillis, 4*blockRangeMillis, numSeries, externalLabels("2_of_2"))

				// Two non-adjacent non-split blocks in the 1st compaction range.
				block6 := createTSDBBlock(t, bkt, userID, 4*blockRangeMillis, 5*blockRangeMillis, numSeries, externalLabels(""))
				block7 := createTSDBBlock(t, bkt, userID, 7*blockRangeMillis, 8*blockRangeMillis, numSeries, externalLabels(""))

				return []metadata.Meta{
					// Block1 have been compacted with block4 and block5 in the 3rd range compaction.
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 1,
							MaxTime: 4 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1a, block4a, block5a},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "1_of_2",
							},
						},
					}, {
						BlockMeta: tsdb.BlockMeta{
							MinTime: 1,
							MaxTime: 4 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1b, block4b, block5b},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{
								mimir_tsdb.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
					// The two overlapping blocks (block2, block3) have been merged in the 1st range.
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: blockRangeMillis,
							MaxTime: 2 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block2, block3},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{},
						},
					},
					// The two non-adjacent blocks block6 and block7 are merged together in the 3rd range.
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 4 * blockRangeMillis,
							MaxTime: 8 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block6, block7},
							},
						},
						Thanos: metadata.Thanos{
							Labels: map[string]string{},
						},
					},
				}
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			workDir := t.TempDir()
			storageDir := t.TempDir()
			fetcherDir := t.TempDir()

			storageCfg := mimir_tsdb.BlocksStorageConfig{}
			flagext.DefaultValues(&storageCfg)
			storageCfg.Bucket.Backend = bucket.Filesystem
			storageCfg.Bucket.Filesystem.Directory = storageDir

			compactorCfg := prepareConfig(t)
			compactorCfg.DataDir = workDir
			compactorCfg.BlockRanges = compactionRanges

			cfgProvider := newMockConfigProvider()
			cfgProvider.splitAndMergeShards[userID] = testData.numShards

			logger := log.NewLogfmtLogger(os.Stdout)
			reg := prometheus.NewPedanticRegistry()
			ctx := context.Background()

			// Create TSDB blocks in the storage and get the expected blocks.
			bucketClient, err := bucket.NewClient(ctx, storageCfg.Bucket, "test", logger, nil)
			require.NoError(t, err)
			expected := testData.setup(t, bucketClient)

			c, err := NewMultitenantCompactor(compactorCfg, storageCfg, cfgProvider, logger, reg)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))
			})

			// Wait until the first compaction run completed.
			test.Poll(t, 15*time.Second, nil, func() interface{} {
				return testutil.GatherAndCompare(reg, strings.NewReader(`
					# HELP cortex_compactor_runs_completed_total Total number of compaction runs successfully completed.
					# TYPE cortex_compactor_runs_completed_total counter
					cortex_compactor_runs_completed_total 1
				`), "cortex_compactor_runs_completed_total")
			})

			// List back any (non deleted) block from the storage.
			userBucket := bucket.NewUserBucketClient(userID, bucketClient, nil)
			fetcher, err := block.NewMetaFetcher(logger,
				1,
				userBucket,
				fetcherDir,
				reg,
				[]block.MetadataFilter{NewExcludeMarkedForDeletionFilter(userBucket)},
			)
			require.NoError(t, err)
			metas, partials, err := fetcher.Fetch(ctx)
			require.NoError(t, err)
			require.Empty(t, partials)

			// Sort blocks by MinTime and labels so that we get a stable comparison.
			actual := sortMetasByMinTime(convertMetasMapToSlice(metas))

			// Compare actual blocks with the expected ones.
			require.Len(t, actual, len(expected))
			for i, e := range expected {
				assert.Equal(t, e.MinTime, actual[i].MinTime)
				assert.Equal(t, e.MaxTime, actual[i].MaxTime)
				assert.Equal(t, e.Compaction.Sources, actual[i].Compaction.Sources)
				assert.Equal(t, e.Thanos.Labels, actual[i].Thanos.Labels)
			}
		})
	}
}

func convertMetasMapToSlice(metas map[ulid.ULID]*metadata.Meta) []*metadata.Meta {
	var out []*metadata.Meta
	for _, m := range metas {
		out = append(out, m)
	}
	return out
}
