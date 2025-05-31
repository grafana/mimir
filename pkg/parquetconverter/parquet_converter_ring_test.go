package parquetconverter

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	mimir_testutil "github.com/grafana/mimir/pkg/storage/tsdb/testutil"
	"github.com/grafana/mimir/pkg/util/test"
)

// TestParquetConverter_InitialSyncWithWaitRing tests the parquet converter start case.
// When several parquet-converters start up at once, we expect each one to only load
// own certain blocks, regardless which parquet-converter joined the ring first or last.
func TestParquetConverter_InitialSyncWithWaitRing(t *testing.T) {
	test.VerifyNoLeak(t)

	bucketClientOnDisk, storageDir := mimir_testutil.PrepareFilesystemBucket(t)

	// This tests uses real TSDB blocks. 24h time range, 2h block range period,
	// 2 users = total (24 / 12) * 2 = 24 blocks.
	numUsers := 2
	numBlocks := numUsers * 12
	now := time.Now()
	mockTSDB(t, path.Join(storageDir, "user-1"), 24, 12, now.Add(-24*time.Hour).Unix()*1000, now.Unix()*1000)
	mockTSDB(t, path.Join(storageDir, "user-2"), 24, 12, now.Add(-24*time.Hour).Unix()*1000, now.Unix()*1000)

	// Uploading the blocks to in memory storage - otherwise we run out of file descriptors during the test
	// The default limit for fsd is 1024 on linux.
	bucketClient := objstore.NewInMemBucket()
	for _, userID := range []string{"user-1", "user-2"} {
		userBucketClient := bucket.NewUserBucketClient(userID, bucketClient, nil)
		require.NoError(t, bucketClientOnDisk.Iter(context.Background(), userID, func(key string) error {
			dir := strings.TrimSuffix(path.Join(storageDir, key), "/")
			err := block.Upload(context.Background(), log.NewNopLogger(), userBucketClient, dir, nil)
			if err != nil {
				return err
			}
			return nil
		}))
	}

	// Write the bucket index.
	// for _, userID := range []string{"user-1", "user-2"} {
	//	createBucketIndex(t, bucketClient, userID)
	// }

	tests := map[string]struct {
		numConverters        int
		expectedBlocksLoaded int
	}{
		"1 converter": {
			numConverters:        1,
			expectedBlocksLoaded: numBlocks,
		},
		"2 converters": {
			numConverters:        2,
			expectedBlocksLoaded: numBlocks, // blocks are sharded across gateways
		},
		"5 converters": {
			numConverters:        5,
			expectedBlocksLoaded: 2 * numBlocks, // blocks are replicated 2 times
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ringStore, closer := consul.NewInMemoryClientWithConfig(ring.GetCodec(), consul.Config{
				MaxCasRetries: 20,
				CasRetryDelay: 500 * time.Millisecond,
			}, log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			// Create the configured number of converters.
			var converters []*ParquetConverter
			registries := dskit_metrics.NewTenantRegistries(log.NewNopLogger())

			for i := 1; i <= testData.numConverters; i++ {
				instanceID := fmt.Sprintf("converter-%d", i)

				converterCfg := prepareConfig(t)
				converterCfg.ShardingRing.Common.KVStore.Mock = ringStore
				converterCfg.ShardingRing.Common.InstanceID = instanceID
				converterCfg.ShardingRing.Common.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)
				converterCfg.ShardingRing.WaitStabilityMinDuration = 4 * time.Second
				converterCfg.ShardingRing.WaitStabilityMaxDuration = 30 * time.Second

				c, reg := prepare(t, converterCfg, objstore.WithNoopInstr(bucketClient))
				t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, c)) })

				converters = append(converters, c)
				registries.AddTenantRegistry(instanceID, reg)
			}

			// Start all converters concurrently.
			for _, g := range converters {
				require.NoError(t, g.StartAsync(ctx))
			}

			// Wait until all converters are running.
			for _, g := range converters {
				require.NoError(t, g.AwaitRunning(ctx))
			}

			// At this point we expect that all converters have done the initial sync
			// and "sync/converted" only their respective blocks. (TODO Review phrasing).
			metrics := registries.BuildMetricFamiliesPerTenant()
			assert.Equal(t, float64(testData.expectedBlocksLoaded), metrics.GetSumOfGauges("cortex_bucket_store_blocks_loaded")) // I think this should be converter total
			assert.Equal(t, float64(2*testData.numConverters), metrics.GetSumOfGauges("parquet_converter_tenants_discovered"))

			assert.Equal(t, float64(testData.numConverters*numBlocks), metrics.GetSumOfGauges("cortex_blocks_meta_synced"))
			assert.Equal(t, float64(testData.numConverters*numUsers), metrics.GetSumOfGauges("cortex_bucket_stores_tenants_synced"))

			// We expect that all converters have only run the initial sync and not the periodic one.
			assert.Equal(t, float64(testData.numConverters), metrics.GetSumOfCounters("cortex_storegateway_bucket_sync_total"))
		})
	}
}

// mockTSDB create 1+ TSDB blocks storing numSeries of series, each series
// with 1 sample and its timestamp evenly distributed between minT and maxT.
// If numBlocks > 0, then it uses numSeries only to find the distribution of
// samples.
func mockTSDB(t *testing.T, dir string, numSeries, numBlocks int, minT, maxT int64) {
	// Create a new TSDB on a temporary directory. The blocks
	// will be then snapshotted to the input dir.
	tempDir := t.TempDir()

	ctx := context.Background()

	db, err := tsdb.Open(tempDir, nil, nil, &tsdb.Options{
		MinBlockDuration:  2 * time.Hour.Milliseconds(),
		MaxBlockDuration:  2 * time.Hour.Milliseconds(),
		RetentionDuration: 15 * 24 * time.Hour.Milliseconds(),
	}, nil)
	require.NoError(t, err)

	db.DisableCompactions()

	step := (maxT - minT) / int64(numSeries)
	addSample := func(i int) {
		lbls := labels.FromStrings("series_id", strconv.Itoa(i))

		app := db.Appender(ctx)
		_, err := app.Append(0, lbls, minT+(step*int64(i)), float64(i))
		require.NoError(t, err)
		require.NoError(t, app.Commit())
		require.NoError(t, db.Compact(ctx))
	}
	if numBlocks > 0 {
		i := 0
		// Snapshot adds another block. Hence numBlocks-1.
		for len(db.Blocks()) < numBlocks-1 {
			addSample(i)
			i++
		}
	} else {
		for i := 0; i < numSeries; i++ {
			addSample(i)
		}
	}

	require.NoError(t, db.Snapshot(dir, true))

	require.NoError(t, db.Close())
}
