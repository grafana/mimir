// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	util_log "github.com/grafana/mimir/pkg/util/log"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
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
			labels[block.CompactorShardIDExternalLabel] = shardID
		}
		return labels
	}

	externalLabelsWithTenantID := func(shardID string) map[string]string {
		labels := externalLabels(shardID)
		labels[block.DeprecatedTenantIDExternalLabel] = userID
		return labels
	}

	tests := map[string]struct {
		numShards int
		setup     func(t *testing.T, bkt objstore.Bucket) []block.Meta
	}{
		"overlapping blocks matching the 1st compaction range should be merged and split": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []block.Meta {
				block1 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, numSeries, externalLabels(""))
				block2 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, numSeries, externalLabels(""))

				return []block.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 1 * blockRangeMillis,
							MaxTime: 2 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "1_of_2",
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
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
				}
			},
		},
		"overlapping blocks matching the 1st compaction range with mixed tenant ID labels should be merged and split": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []block.Meta {
				block1 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, numSeries, externalLabels(""))             // Doesn't have __org_id__ label
				block2 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, numSeries, externalLabelsWithTenantID("")) // Has __org_id__ label

				return []block.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 1 * blockRangeMillis,
							MaxTime: 2 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "1_of_2",
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
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
				}
			},
		},
		"overlapping blocks matching the beginning of the 1st compaction range should be merged and split": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []block.Meta {
				block1 := createTSDBBlock(t, bkt, userID, 0, (5 * time.Minute).Milliseconds(), numSeries, externalLabels(""))
				block2 := createTSDBBlock(t, bkt, userID, time.Minute.Milliseconds(), (7 * time.Minute).Milliseconds(), numSeries, externalLabels(""))

				return []block.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (7 * time.Minute).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "1_of_2",
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
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
				}
			},
		},
		"non-overlapping blocks matching the beginning of the 1st compaction range (without gaps) should be merged and split": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []block.Meta {
				block1 := createTSDBBlock(t, bkt, userID, 0, (5 * time.Minute).Milliseconds(), numSeries, externalLabels(""))
				block2 := createTSDBBlock(t, bkt, userID, (5 * time.Minute).Milliseconds(), (10 * time.Minute).Milliseconds(), numSeries, externalLabels(""))

				return []block.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (10 * time.Minute).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "1_of_2",
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
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
				}
			},
		},
		"non-overlapping blocks matching the beginning of the 1st compaction range (with gaps) should be merged and split": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []block.Meta {
				block1 := createTSDBBlock(t, bkt, userID, 0, (5 * time.Minute).Milliseconds(), numSeries, externalLabels(""))
				block2 := createTSDBBlock(t, bkt, userID, (7 * time.Minute).Milliseconds(), (10 * time.Minute).Milliseconds(), numSeries, externalLabels(""))

				return []block.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (10 * time.Minute).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "1_of_2",
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
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
				}
			},
		},
		"smaller compaction ranges should take precedence over larger ones, and then re-iterate in subsequent compactions of increasing ranges": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []block.Meta {
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

				return []block.Meta{
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
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "1_of_2",
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
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "2_of_2",
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
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "1_of_2",
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
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
				}
			},
		},
		"overlapping and non-overlapping blocks within the same range should be split and compacted together": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []block.Meta {
				// Overlapping.
				block1 := createTSDBBlock(t, bkt, userID, 0, (5 * time.Minute).Milliseconds(), numSeries, externalLabels(""))
				block2 := createTSDBBlock(t, bkt, userID, time.Minute.Milliseconds(), (7 * time.Minute).Milliseconds(), numSeries, externalLabels(""))

				// Not overlapping.
				block3 := createTSDBBlock(t, bkt, userID, time.Hour.Milliseconds(), (2 * time.Hour).Milliseconds(), numSeries, externalLabels(""))

				return []block.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (2 * time.Hour).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2, block3},
							},
						},
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "1_of_2",
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
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
				}
			},
		},
		"overlapping and non-overlapping blocks within the same range and mixed tenant ID label should be split and compacted together": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []block.Meta {
				// Overlapping.
				block1 := createTSDBBlock(t, bkt, userID, 0, (5 * time.Minute).Milliseconds(), numSeries, externalLabels(""))                                      // Without __org_id__ label
				block2 := createTSDBBlock(t, bkt, userID, time.Minute.Milliseconds(), (7 * time.Minute).Milliseconds(), numSeries, externalLabelsWithTenantID("")) // With __org_id__ label

				// Not overlapping.
				block3 := createTSDBBlock(t, bkt, userID, time.Hour.Milliseconds(), (2 * time.Hour).Milliseconds(), numSeries, externalLabelsWithTenantID("")) // With __org_id__ label

				return []block.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (2 * time.Hour).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2, block3},
							},
						},
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "1_of_2",
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
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "2_of_2",
							},
						},
					},
				}
			},
		},
		"should correctly handle empty blocks generated in the splitting stage": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []block.Meta {
				// Generate a block with only 1 series. This block will be split into 1 split block only,
				// because the source block only has 1 series.
				block1 := createTSDBBlock(t, bkt, userID, blockRangeMillis, 2*blockRangeMillis, 1, externalLabels(""))

				return []block.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: (2 * blockRangeMillis) - 1, // Because there's only 1 sample with timestamp=maxT-1
							MaxTime: 2 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1},
							},
						},
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "1_of_2",
							},
						},
					},
				}
			},
		},
		"splitting should be disabled if configured shards = 0": {
			numShards: 0,
			setup: func(t *testing.T, bkt objstore.Bucket) []block.Meta {
				block1 := createTSDBBlock(t, bkt, userID, 0, (5 * time.Minute).Milliseconds(), numSeries, externalLabels(""))
				block2 := createTSDBBlock(t, bkt, userID, (5 * time.Minute).Milliseconds(), (10 * time.Minute).Milliseconds(), numSeries, externalLabels(""))

				return []block.Meta{
					// Compacted but not split.
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 0,
							MaxTime: (10 * time.Minute).Milliseconds(),
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: block.ThanosMeta{
							Labels: map[string]string{},
						},
					},
				}
			},
		},
		"splitting should be disabled but already split blocks should be merged correctly (respecting the shard) if configured shards = 0": {
			numShards: 0,
			setup: func(t *testing.T, bkt objstore.Bucket) []block.Meta {
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

				return []block.Meta{
					// Block1 have been compacted with block4 and block5 in the 3rd range compaction.
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 1,
							MaxTime: 4 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1a, block4a, block5a},
							},
						},
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "1_of_2",
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
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "2_of_2",
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
						Thanos: block.ThanosMeta{
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
						Thanos: block.ThanosMeta{
							Labels: map[string]string{},
						},
					},
				}
			},
		},
		"compaction on blocks containing native histograms": {
			numShards: 2,
			setup: func(t *testing.T, bkt objstore.Bucket) []block.Meta {
				minT := blockRangeMillis
				maxT := 2 * blockRangeMillis

				seriesID := 0

				appendHistograms := func(db *tsdb.DB) {

					appendHistogram := func(seriesID int, ts int64) {
						lbls := labels.FromStrings("series_id", strconv.Itoa(seriesID))

						app := db.Appender(context.Background())
						_, err := app.AppendHistogram(0, lbls, ts, util_test.GenerateTestHistogram(seriesID), nil)
						require.NoError(t, err)

						err = app.Commit()
						require.NoError(t, err)
					}

					for ts := minT; ts < maxT; ts += (maxT - minT) / int64(numSeries-1) {
						appendHistogram(seriesID, ts)
						seriesID++
					}

					appendHistogram(seriesID, maxT-1)
				}

				block1 := createCustomTSDBBlock(t, bkt, userID, externalLabels(""), appendHistograms)
				block2 := createCustomTSDBBlock(t, bkt, userID, externalLabels(""), appendHistograms)

				return []block.Meta{
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 1 * blockRangeMillis,
							MaxTime: 2 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "1_of_2",
							},
						},
					},
					{
						BlockMeta: tsdb.BlockMeta{
							MinTime: 1 * blockRangeMillis,
							MaxTime: 2 * blockRangeMillis,
							Compaction: tsdb.BlockMetaCompaction{
								Sources: []ulid.ULID{block1, block2},
							},
						},
						Thanos: block.ThanosMeta{
							Labels: map[string]string{
								block.CompactorShardIDExternalLabel: "2_of_2",
							},
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
			// Run the whole compactor for a tenant on a non-default encoding, so the lookup in
			// newBucketCompactor() is exercised.
			cfgProvider.floatChunkEncodings[userID] = chunkenc.EncXOR2

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
				nil,
				0,
			)
			require.NoError(t, err)
			metas, partials, err := fetcher.FetchWithoutMarkedForDeletion(ctx)
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

func TestMultitenantCompactor_ShouldGuaranteeSeriesShardingConsistencyOverTheTime(t *testing.T) {
	const (
		userID     = "user-1"
		numSeries  = 100
		blockRange = 2 * time.Hour
		numShards  = 2
	)

	var (
		blockRangeMillis = blockRange.Milliseconds()
		compactionRanges = mimir_tsdb.DurationList{blockRange}

		// You should NEVER CHANGE the expected series here, otherwise it means you're introducing
		// a backward incompatible change.
		expectedSeriesIDByShard = map[string][]int{
			"1_of_2": {0, 1, 3, 4, 5, 6, 7, 11, 12, 15, 16, 17, 18, 19, 20, 21, 24, 25, 27, 31, 36, 37, 38, 40, 42, 45, 47, 50, 51, 52, 53, 54, 55, 57, 59, 60, 61, 63, 68, 70, 71, 72, 74, 77, 79, 80, 81, 82, 83, 84, 85, 86, 88, 89, 90, 91, 92, 94, 98, 100},
			"2_of_2": {2, 8, 9, 10, 13, 14, 22, 23, 26, 28, 29, 30, 32, 33, 34, 35, 39, 41, 43, 44, 46, 48, 49, 56, 58, 62, 64, 65, 66, 67, 69, 73, 75, 76, 78, 87, 93, 95, 96, 97, 99},
		}
	)

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
	cfgProvider.splitAndMergeShards[userID] = numShards

	logger := log.NewLogfmtLogger(os.Stdout)
	reg := prometheus.NewPedanticRegistry()
	ctx := context.Background()

	bucketClient, err := bucket.NewClient(ctx, storageCfg.Bucket, "test", logger, nil)
	require.NoError(t, err)

	// Create a TSDB block in the storage.
	blockID := createTSDBBlock(t, bucketClient, userID, blockRangeMillis, 2*blockRangeMillis, numSeries, nil)

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
		nil,
		0,
	)
	require.NoError(t, err)
	metas, partials, err := fetcher.FetchWithoutMarkedForDeletion(ctx)
	require.NoError(t, err)
	require.Empty(t, partials)

	// Sort blocks by MinTime and labels so that we get a stable comparison.
	actualMetas := sortMetasByMinTime(convertMetasMapToSlice(metas))

	// Ensure the input block has been split.
	require.Len(t, actualMetas, numShards)
	for idx, actualMeta := range actualMetas {
		assert.Equal(t, blockRangeMillis, actualMeta.MinTime)
		assert.Equal(t, 2*blockRangeMillis, actualMeta.MaxTime)
		assert.Equal(t, []ulid.ULID{blockID}, actualMeta.Compaction.Sources)
		assert.Equal(t, sharding.FormatShardIDLabelValue(uint64(idx), numShards), actualMeta.Thanos.Labels[block.CompactorShardIDExternalLabel])
	}

	// Ensure each split block contains the right series, based on a series labels
	// hashing function which doesn't change over time.
	for _, actualMeta := range actualMetas {
		expectedSeriesIDs := expectedSeriesIDByShard[actualMeta.Thanos.Labels[block.CompactorShardIDExternalLabel]]

		b, err := tsdb.OpenBlock(util_log.SlogFromGoKit(logger), filepath.Join(storageDir, userID, actualMeta.ULID.String()), nil, nil)
		require.NoError(t, err)

		indexReader, err := b.Index()
		require.NoError(t, err)

		// Find all series in the block.
		postings, err := indexReader.PostingsForMatchers(ctx, false, labels.MustNewMatcher(labels.MatchRegexp, "series_id", ".+"))
		require.NoError(t, err)

		builder := labels.NewScratchBuilder(1)
		for postings.Next() {
			// Symbolize the series labels.
			require.NoError(t, indexReader.Series(postings.At(), &builder, nil))

			// Ensure the series below to the right shard.
			seriesLabels := builder.Labels()
			seriesID, err := strconv.Atoi(seriesLabels.Get("series_id"))
			require.NoError(t, err)
			assert.Contains(t, expectedSeriesIDs, seriesID, "series:", seriesLabels.String())
		}

		require.NoError(t, postings.Err())
	}
}

func convertMetasMapToSlice(metas map[ulid.ULID]*block.Meta) []*block.Meta {
	var out []*block.Meta
	for _, m := range metas {
		out = append(out, m)
	}
	return out
}

func TestSplitAndMergeCompactorFactory_ShouldRegisterTSDBCompactorMetricsOnce(t *testing.T) {
	cfg := Config{}
	flagext.DefaultValues(&cfg)

	reg := prometheus.NewPedanticRegistry()
	provider, _, err := splitAndMergeCompactorFactory(t.Context(), cfg, newMockConfigProvider(), log.NewNopLogger(), reg)
	require.NoError(t, err)

	require.NotNil(t, provider("user-1"))

	// The compactors share a single metrics instance: building the metrics per compactor would
	// register them twice, which the pedantic registry would reject. Gathering also checks they are
	// registered at all, so the test can't pass with them silently dropped instead.
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP prometheus_tsdb_compactions_total Total number of compactions that were executed for the partition.
		# TYPE prometheus_tsdb_compactions_total counter
		prometheus_tsdb_compactions_total 0
	`), "prometheus_tsdb_compactions_total"))
}

// TestSplitAndMergeCompactorFactory_ShouldFailOnEmptyBlockRanges checks the factory fails at
// startup, rather than per compaction job.
func TestSplitAndMergeCompactorFactory_ShouldFailOnEmptyBlockRanges(t *testing.T) {
	_, _, err := splitAndMergeCompactorFactory(t.Context(), Config{}, newMockConfigProvider(), log.NewNopLogger(), prometheus.NewRegistry())
	require.ErrorContains(t, err, "creating compactor for float chunk encoding")
	require.ErrorContains(t, err, "at least one range must be provided")
}

func TestSplitAndMergeCompactorFactory_VerticalCompactionHonorsFloatChunkEncoding(t *testing.T) {
	tests := map[string]struct {
		encoding    chunkenc.Encoding
		expectedEnc chunkenc.Encoding
	}{
		"unset falls back to the default": {expectedEnc: chunkenc.EncXOR},
		"xor2":                            {encoding: chunkenc.EncXOR2, expectedEnc: chunkenc.EncXOR2},
		// A ConfigProvider is free to return an encoding the limit cannot select.
		"encoding outside the limit falls back to the default": {encoding: chunkenc.EncHistogram, expectedEnc: chunkenc.EncXOR},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			const userID = "user-1"

			cfgProvider := newMockConfigProvider()
			if testData.encoding != chunkenc.EncNone {
				cfgProvider.floatChunkEncodings[userID] = testData.encoding
			}

			cfg := Config{}
			flagext.DefaultValues(&cfg)

			provider, _, err := splitAndMergeCompactorFactory(t.Context(), cfg, cfgProvider, log.NewNopLogger(), prometheus.NewRegistry())
			require.NoError(t, err)

			chunks := verticallyCompactOverlappingBlocks(t, provider(userID), t.TempDir())

			// The two source blocks hold a single float chunk each, and they overlap, so the
			// compactor has to merge them into one re-encoded chunk. Asserting the count keeps the
			// assertion below from passing on a chunk that was copied over verbatim, since only
			// re-encoded chunks get the configured encoding.
			require.Len(t, chunks, 1)
			assert.Equal(t, testData.expectedEnc, chunks[0].encoding)

			// The chunk must span both source blocks, which proves it is the merge of the two
			// overlapping chunks and not one of them passed through.
			assert.Less(t, chunks[0].minTime, verticallyCompactedBlocksOverlapStart)
			assert.Greater(t, chunks[0].maxTime, verticallyCompactedBlocksOverlapStart)
		})
	}
}

// verticallyCompactedBlocksOverlapStart is the timestamp at which the two blocks created by
// verticallyCompactOverlappingBlocks() start overlapping.
const verticallyCompactedBlocksOverlapStart = int64(500)

// floatChunkInfo describes a float chunk stored in a block.
type floatChunkInfo struct {
	encoding chunkenc.Encoding
	minTime  int64
	maxTime  int64
}

// verticallyCompactOverlappingBlocks compacts, in dir, two blocks holding the same series over
// overlapping time ranges, and returns the float chunks of the compacted block. Because the blocks
// overlap, the compactor has to merge and re-encode the float chunks, which is the only case where
// the configured float chunk encoding is applied.
func verticallyCompactOverlappingBlocks(t *testing.T, compactor Compactor, dir string) []floatChunkInfo {
	t.Helper()

	// block.CreateBlock() cycles through value types, so out of these three series only the first
	// one holds floats, while the other two hold histograms and float histograms.
	series := []labels.Labels{
		labels.FromStrings("series", "1"),
		labels.FromStrings("series", "2"),
		labels.FromStrings("series", "3"),
	}

	block1, err := block.CreateBlock(t.Context(), dir, series, 10, 0, 2*verticallyCompactedBlocksOverlapStart, labels.EmptyLabels())
	require.NoError(t, err)
	block2, err := block.CreateBlock(t.Context(), dir, series, 10, verticallyCompactedBlocksOverlapStart, 3*verticallyCompactedBlocksOverlapStart, labels.EmptyLabels())
	require.NoError(t, err)

	compacted, err := compactor.Compact(dir, []string{filepath.Join(dir, block1.String()), filepath.Join(dir, block2.String())}, nil)
	require.NoError(t, err)
	require.Len(t, compacted, 1)

	return blockFloatChunks(t, filepath.Join(dir, compacted[0].String()))
}

// blockFloatChunks returns every float chunk stored in the block at dir.
func blockFloatChunks(t *testing.T, dir string) []floatChunkInfo {
	t.Helper()

	b, err := tsdb.OpenBlock(nil, dir, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	q, err := tsdb.NewBlockChunkQuerier(b, b.MinTime(), b.MaxTime())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, q.Close()) })

	floatEncodings := make([]chunkenc.Encoding, 0, len(validation.FloatChunkEncodingValues))
	for _, value := range validation.FloatChunkEncodingValues {
		floatEncodings = append(floatEncodings, validation.ParseFloatChunkEncoding(value))
	}

	var chunks []floatChunkInfo
	ss := q.Select(t.Context(), true, nil, labels.MustNewMatcher(labels.MatchRegexp, "series", ".*"))
	for ss.Next() {
		it := ss.At().Iterator(nil)
		for it.Next() {
			meta := it.At()
			if slices.Contains(floatEncodings, meta.Chunk.Encoding()) {
				chunks = append(chunks, floatChunkInfo{encoding: meta.Chunk.Encoding(), minTime: meta.MinTime, maxTime: meta.MaxTime})
			}
		}
		require.NoError(t, it.Err())
	}
	require.NoError(t, ss.Err())

	return chunks
}
