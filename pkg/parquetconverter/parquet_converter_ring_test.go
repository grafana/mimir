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
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	mimir_testutil "github.com/grafana/mimir/pkg/storage/tsdb/testutil"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

// mockBlockConverter is a mock implementation of blockConverter that always succeeds.
type mockBlockConverter struct{}

// ConvertBlock implements the blockConverter interface and always returns nil (success).
func (m mockBlockConverter) ConvertBlock(ctx context.Context, meta *block.Meta, localBlockDir string, bkt objstore.Bucket, logger log.Logger) error {
	return nil
}

// prepareWithMockConverter creates a ParquetConverter with a mock block converter for testing.
func prepareWithMockConverter(t *testing.T, cfg Config, bucketClient objstore.Bucket) (*ParquetConverter, *prometheus.Registry) {
	var limits validation.Limits
	flagext.DefaultValues(&limits)
	overrides := validation.NewOverrides(limits, nil)

	cfg.DataDir = t.TempDir()

	logs := &concurrency.SyncBuffer{}
	registry := prometheus.NewRegistry()

	bucketClientFactory := func(ctx context.Context) (objstore.Bucket, error) {
		return bucketClient, nil
	}
	c, err := newParquetConverter(cfg, log.NewLogfmtLogger(logs), registry, bucketClientFactory, overrides, mockBlockConverter{})
	require.NoError(t, err)

	return c, registry
}

// TestParquetConverter_InitialSyncWithWaitRing tests the parquet converter start case.
// When several parquet-converters start up at once, we expect each one to only load
// own certain blocks, regardless which parquet-converter joined the ring first or last.
func TestParquetConverter_InitialSyncWithWaitRing(t *testing.T) {
	test.VerifyNoLeak(t)

	const numUsers = 2
	const numBlocks = 7

	// We run the test with 1, 2, and 3 converters to ensure that the sharding works correctly
	for numConverters := 1; numConverters <= 3; numConverters++ {
		testName := fmt.Sprintf("%d converter%s", numConverters, map[bool]string{true: "s", false: ""}[numConverters > 1])
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			t.Cleanup(cancel)

			// Create isolated bucket and blocks for this test case
			bucketClientOnDisk, storageDir := mimir_testutil.PrepareFilesystemBucket(t)
			now := time.Now()
			mockTSDB(t, path.Join(storageDir, "user-1"), 24, 3, now.Add(-24*time.Hour).Unix()*1000, now.Unix()*1000)
			mockTSDB(t, path.Join(storageDir, "user-2"), 24, 4, now.Add(-24*time.Hour).Unix()*1000, now.Unix()*1000)

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

			ringStore, closer := consul.NewInMemoryClientWithConfig(ring.GetCodec(), consul.Config{
				MaxCasRetries: 20,
				CasRetryDelay: 500 * time.Millisecond,
			}, log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			var converters []*ParquetConverter
			registries := dskit_metrics.NewTenantRegistries(log.NewNopLogger())

			for i := 1; i <= numConverters; i++ {
				instanceID := fmt.Sprintf("converter-%d", i)

				converterCfg := prepareConfig(t)
				converterCfg.ShardingRing.Common.KVStore.Mock = ringStore
				converterCfg.ShardingRing.Common.InstanceID = instanceID
				converterCfg.ShardingRing.Common.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)
				converterCfg.ShardingRing.WaitStabilityMinDuration = 0
				converterCfg.ShardingRing.WaitStabilityMaxDuration = 0
				converterCfg.ShardingRing.WaitActiveInstanceTimeout = 30 * time.Second
				converterCfg.ConversionInterval = 1 * time.Second

				c, reg := prepareWithMockConverter(t, converterCfg, objstore.WithNoopInstr(bucketClient))
				t.Cleanup(func() {
					if c.State() == services.Failed {
						// Don't try to stop a service that already failed
						t.Logf("Converter %s failed with error: %v", instanceID, c.FailureCase())
						return
					}
					err := services.StopAndAwaitTerminated(context.Background(), c)
					if err != nil && c.State() == services.Failed {
						return
					}
					assert.NoError(t, err)
				})

				converters = append(converters, c)
				registries.AddTenantRegistry(instanceID, reg)
			}

			// Start all converters concurrently.
			for _, g := range converters {
				require.NoError(t, g.StartAsync(ctx))
			}

			// Wait until all converters are running (sequentially to avoid deadlock).
			for _, g := range converters {
				err := g.AwaitRunning(ctx)
				require.NoError(t, err)
			}

			time.Sleep(2 * time.Second) // Wait for converters to do the initial pass through the blocks.

			// At this point we expect that all converters have joined the ring, discovered all tenants, and converted
			// blocks for all users.
			metrics := registries.BuildMetricFamiliesPerTenant()
			// All converters should have discovered the same number of tenants.
			assert.Equal(t, float64(numUsers*numConverters), metrics.GetSumOfGauges("cortex_parquet_converter_tenants_discovered"), "number of discovered tenants didnt match")
			// The number of blocks synced should be equal to the number of blocks in the TSDB proving that despite the
			// number of converters the blocks are sharded across them.
			assert.Equal(t, float64(numBlocks), metrics.GetSumOfCounters("cortex_parquet_converter_blocks_converted_total"), "number of blocks converted didnt match")
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
