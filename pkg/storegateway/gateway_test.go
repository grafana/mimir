// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/gateway_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/kv/consul"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	dstest "github.com/grafana/dskit/test"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	mimir_testutil "github.com/grafana/mimir/pkg/storage/tsdb/testutil"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		setup    func(cfg *Config, limits *validation.Limits)
		expected error
	}{
		"should pass by default": {
			setup:    func(*Config, *validation.Limits) {},
			expected: nil,
		},
		"should fail if shard size is negative": {
			setup: func(_ *Config, limits *validation.Limits) {
				limits.StoreGatewayTenantShardSize = -3
			},
			expected: errInvalidTenantShardSize,
		},
		"should pass if shard size has been set": {
			setup: func(_ *Config, limits *validation.Limits) {
				limits.StoreGatewayTenantShardSize = 3
			},
			expected: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := &Config{}
			limits := &validation.Limits{}
			flagext.DefaultValues(cfg, limits)
			testData.setup(cfg, limits)

			assert.Equal(t, testData.expected, cfg.Validate(*limits))
		})
	}
}

func TestStoreGateway_InitialSyncWithDefaultShardingEnabled(t *testing.T) {
	test.VerifyNoLeak(t)

	tests := map[string]struct {
		initialExists bool
		initialState  ring.InstanceState
		initialTokens ring.Tokens
	}{
		"instance not in the ring": {
			initialExists: false,
		},
		"instance already in the ring with PENDING state and has no tokens": {
			initialExists: true,
			initialState:  ring.PENDING,
			initialTokens: ring.Tokens{},
		},
		"instance already in the ring with JOINING state and has some tokens": {
			initialExists: true,
			initialState:  ring.JOINING,
			initialTokens: ring.Tokens{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		"instance already in the ring with ACTIVE state and has all tokens": {
			initialExists: true,
			initialState:  ring.ACTIVE,
			initialTokens: generateSortedTokens(ringNumTokensDefault),
		},
		"instance already in the ring with LEAVING state and has all tokens": {
			initialExists: true,
			initialState:  ring.LEAVING,
			initialTokens: generateSortedTokens(ringNumTokensDefault),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			gatewayCfg := mockGatewayConfig()
			storageCfg := mockStorageConfig(t)
			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			onBucketIndexGet := func() {}

			bucketClient := &bucket.ErrorInjectedBucketClient{Bucket: objstore.NewInMemBucket(), Injector: func(op bucket.Operation, name string) error {
				if op == bucket.OpGet && strings.HasSuffix(name, bucketindex.IndexCompressedFilename) {
					onBucketIndexGet()
				}
				return nil
			}}

			// Setup the initial instance state in the ring.
			if testData.initialExists {
				require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
					ringDesc := ring.GetOrCreateRingDesc(in)
					ringDesc.AddIngester(gatewayCfg.ShardingRing.InstanceID, gatewayCfg.ShardingRing.InstanceAddr, "", testData.initialTokens, testData.initialState, time.Now(), false, time.Time{})
					return ringDesc, true, nil
				}))
			}

			g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), log.NewNopLogger(), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, g)) })
			assert.False(t, g.ringLifecycler.IsRegistered())

			for _, userID := range []string{"user-1", "user-2"} {
				createBucketIndex(t, bucketClient, userID)
			}

			onBucketIndexGet = func() {
				// During the initial sync, we expect the instance to always be in the JOINING
				// state within the ring.
				assert.True(t, g.ringLifecycler.IsRegistered())
				assert.Equal(t, ring.JOINING, g.ringLifecycler.GetState())
				assert.Equal(t, ringNumTokensDefault, len(g.ringLifecycler.GetTokens()))
				assert.Subset(t, g.ringLifecycler.GetTokens(), testData.initialTokens)
			}

			// Once successfully started, the instance should be ACTIVE in the ring.
			require.NoError(t, services.StartAndAwaitRunning(ctx, g))

			assert.True(t, g.ringLifecycler.IsRegistered())
			assert.Equal(t, ring.ACTIVE, g.ringLifecycler.GetState())
			assert.Equal(t, ringNumTokensDefault, len(g.ringLifecycler.GetTokens()))
			assert.Subset(t, g.ringLifecycler.GetTokens(), testData.initialTokens)

			assert.NotNil(t, g.stores.getStore("user-1"))
			assert.NotNil(t, g.stores.getStore("user-2"))
			assert.Nil(t, g.stores.getStore("user-unknown"))
		})
	}
}

func TestStoreGateway_InitialSyncFailure(t *testing.T) {
	test.VerifyNoLeak(t)

	ctx := context.Background()
	gatewayCfg := mockGatewayConfig()
	storageCfg := mockStorageConfig(t)
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	bucketClient := &bucket.ErrorInjectedBucketClient{Injector: func(bucket.Operation, string) error { return assert.AnError }}

	g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), log.NewLogfmtLogger(os.Stdout), nil, nil)
	require.NoError(t, err)

	require.NoError(t, g.StartAsync(ctx))
	err = g.AwaitRunning(ctx)
	assert.Error(t, err)
	assert.Equal(t, services.Failed, g.State())

	// We expect a clean shutdown, including unregistering the instance from the ring.
	assert.False(t, g.ringLifecycler.IsRegistered())
	_ = services.StopAndAwaitTerminated(ctx, g) // There will be an error since the initial sync failed
}

// TestStoreGateway_InitialSyncWithWaitRingTokensStability tests the store-gateway cold start case.
// When several store-gateways start up at once, we expect each store-gateway to only load
// their own blocks, regardless which store-gateway joined the ring first or last (even if starting
// at the same time, they will join the ring at a slightly different time).
func TestStoreGateway_InitialSyncWithWaitRingTokensStability(t *testing.T) {
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
	for _, userID := range []string{"user-1", "user-2"} {
		createBucketIndex(t, bucketClient, userID)
	}

	tests := map[string]struct {
		tenantShardSize      int
		replicationFactor    int
		numGateways          int
		expectedBlocksLoaded int
	}{
		"shard size 0, 1 gateway, RF = 1": {
			tenantShardSize:      0,
			replicationFactor:    1,
			numGateways:          1,
			expectedBlocksLoaded: numBlocks,
		},
		"shard size 0, 2 gateways, RF = 1": {
			tenantShardSize:      0,
			replicationFactor:    1,
			numGateways:          2,
			expectedBlocksLoaded: numBlocks, // blocks are sharded across gateways
		},
		"shard size 0, 3 gateways, RF = 2": {
			tenantShardSize:      0,
			replicationFactor:    2,
			numGateways:          3,
			expectedBlocksLoaded: 2 * numBlocks, // blocks are replicated 2 times
		},
		"shard size 0, 5 gateways, RF = 3": {
			tenantShardSize:      0,
			replicationFactor:    3,
			numGateways:          5,
			expectedBlocksLoaded: 3 * numBlocks, // blocks are replicated 3 times
		},
		"shard size 1, 1 gateway, RF = 1": {
			tenantShardSize:      1,
			replicationFactor:    1,
			numGateways:          1,
			expectedBlocksLoaded: numBlocks,
		},
		"shard size 3, 5 gateways, RF = 2": {
			tenantShardSize:      3,
			replicationFactor:    2,
			numGateways:          5,
			expectedBlocksLoaded: 2 * numBlocks, // blocks are replicated 2 times
		},
		"shard size 3, 20 gateways, RF = 3": {
			tenantShardSize:      3,
			replicationFactor:    3,
			numGateways:          20,
			expectedBlocksLoaded: 3 * numBlocks, // blocks are replicated 3 times
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

			// Create the configured number of gateways.
			var gateways []*StoreGateway
			registries := dskit_metrics.NewTenantRegistries(log.NewNopLogger())

			for i := 1; i <= testData.numGateways; i++ {
				instanceID := fmt.Sprintf("gateway-%d", i)

				storageCfg := mockStorageConfig(t)
				storageCfg.BucketStore.SyncInterval = time.Hour // Do not trigger the periodic sync in this test. We want the initial sync only.

				limits := defaultLimitsConfig()
				gatewayCfg := mockGatewayConfig()
				gatewayCfg.ShardingRing.ReplicationFactor = testData.replicationFactor
				gatewayCfg.ShardingRing.InstanceID = instanceID
				gatewayCfg.ShardingRing.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)
				gatewayCfg.ShardingRing.RingCheckPeriod = time.Hour // Do not check the ring topology changes in this test. We want the initial sync only.
				gatewayCfg.ShardingRing.WaitStabilityMinDuration = 4 * time.Second
				gatewayCfg.ShardingRing.WaitStabilityMaxDuration = 30 * time.Second
				limits.StoreGatewayTenantShardSize = testData.tenantShardSize

				overrides, err := validation.NewOverrides(limits, nil)
				require.NoError(t, err)

				reg := prometheus.NewPedanticRegistry()
				g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, overrides, log.NewNopLogger(), reg, nil)
				require.NoError(t, err)
				t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, g)) })

				gateways = append(gateways, g)
				registries.AddTenantRegistry(instanceID, reg)
			}

			// Start all gateways concurrently.
			for _, g := range gateways {
				require.NoError(t, g.StartAsync(ctx))
			}

			// Wait until all gateways are running.
			for _, g := range gateways {
				require.NoError(t, g.AwaitRunning(ctx))
			}

			// At this point we expect that all gateways have done the initial sync and
			// they have synched only their own blocks, because they waited for a stable
			// ring before starting the initial sync.
			metrics := registries.BuildMetricFamiliesPerTenant()
			assert.Equal(t, float64(testData.expectedBlocksLoaded), metrics.GetSumOfGauges("cortex_bucket_store_blocks_loaded"))
			assert.Equal(t, float64(2*testData.numGateways), metrics.GetSumOfGauges("cortex_bucket_stores_tenants_discovered"))

			if testData.tenantShardSize > 0 {
				assert.Equal(t, float64(testData.tenantShardSize*numBlocks), metrics.GetSumOfGauges("cortex_blocks_meta_synced"))
				assert.Equal(t, float64(testData.tenantShardSize*numUsers), metrics.GetSumOfGauges("cortex_bucket_stores_tenants_synced"))
			} else {
				assert.Equal(t, float64(testData.numGateways*numBlocks), metrics.GetSumOfGauges("cortex_blocks_meta_synced"))
				assert.Equal(t, float64(testData.numGateways*numUsers), metrics.GetSumOfGauges("cortex_bucket_stores_tenants_synced"))
			}

			// We expect that all gateways have only run the initial sync and not the periodic one.
			assert.Equal(t, float64(testData.numGateways), metrics.GetSumOfCounters("cortex_storegateway_bucket_sync_total"))
		})
	}
}

func TestStoreGateway_BlocksSyncWithDefaultSharding_RingTopologyChangedAfterScaleUp(t *testing.T) {
	test.VerifyNoLeak(t)

	const (
		numUsers             = 2
		numBlocks            = numUsers * 12
		replicationFactor    = 3
		numInitialGateways   = 4
		numScaleUpGateways   = 6
		expectedBlocksLoaded = 3 * numBlocks // blocks are replicated 3 times
	)

	bucketClient, storageDir := mimir_testutil.PrepareFilesystemBucket(t)

	// This tests uses real TSDB blocks. 24h time range, 2h block range period,
	// 2 users = total (24 / 12) * 2 = 24 blocks.
	now := time.Now()
	mockTSDB(t, path.Join(storageDir, "user-1"), 24, 12, now.Add(-24*time.Hour).Unix()*1000, now.Unix()*1000)
	mockTSDB(t, path.Join(storageDir, "user-2"), 24, 12, now.Add(-24*time.Hour).Unix()*1000, now.Unix()*1000)

	// Write the bucket index.
	for _, userID := range []string{"user-1", "user-2"} {
		createBucketIndex(t, bucketClient, userID)
	}

	ctx := context.Background()
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Create the configured number of gateways.
	var initialGateways []*StoreGateway
	initialRegistries := dskit_metrics.NewTenantRegistries(log.NewNopLogger())
	allRegistries := dskit_metrics.NewTenantRegistries(log.NewNopLogger())

	createStoreGateway := func(id int, waitStabilityMin time.Duration) (*StoreGateway, string, *prometheus.Registry) {
		instanceID := fmt.Sprintf("gateway-%d", id)

		storageCfg := mockStorageConfig(t)
		storageCfg.BucketStore.SyncInterval = time.Hour // Do not trigger the periodic sync in this test. We want it to be triggered by ring topology changed.

		limits := defaultLimitsConfig()
		gatewayCfg := mockGatewayConfig()
		gatewayCfg.ShardingRing.ReplicationFactor = replicationFactor
		gatewayCfg.ShardingRing.InstanceID = instanceID
		gatewayCfg.ShardingRing.InstanceAddr = fmt.Sprintf("127.0.0.%d", id)
		gatewayCfg.ShardingRing.RingCheckPeriod = 100 * time.Millisecond // Check it continuously. Topology will change on scale up.
		gatewayCfg.ShardingRing.WaitStabilityMinDuration = waitStabilityMin
		gatewayCfg.ShardingRing.WaitStabilityMaxDuration = 30 * time.Second

		overrides, err := validation.NewOverrides(limits, nil)
		require.NoError(t, err)

		reg := prometheus.NewPedanticRegistry()
		g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, overrides, log.NewNopLogger(), reg, nil)
		require.NoError(t, err)

		return g, instanceID, reg
	}

	for i := 1; i <= numInitialGateways; i++ {
		g, instanceID, reg := createStoreGateway(i, 2*time.Second)
		initialGateways = append(initialGateways, g)
		initialRegistries.AddTenantRegistry(instanceID, reg)
		allRegistries.AddTenantRegistry(instanceID, reg)
	}

	// Start all gateways concurrently.
	for _, g := range initialGateways {
		require.NoError(t, g.StartAsync(ctx))
		t.Cleanup(func() {
			assert.NoError(t, services.StopAndAwaitTerminated(ctx, g))
		})
	}

	// Wait until all gateways are running.
	for _, g := range initialGateways {
		require.NoError(t, g.AwaitRunning(ctx))
	}

	// At this point we expect that all gateways have done the initial sync and
	// they have synched only their own blocks.
	metrics := initialRegistries.BuildMetricFamiliesPerTenant()
	assert.Equal(t, float64(expectedBlocksLoaded), metrics.GetSumOfGauges("cortex_bucket_store_blocks_loaded"))
	assert.Equal(t, float64(2*numInitialGateways), metrics.GetSumOfGauges("cortex_bucket_stores_tenants_discovered"))

	assert.Equal(t, float64(numInitialGateways*numBlocks), metrics.GetSumOfGauges("cortex_blocks_meta_synced"))
	assert.Equal(t, float64(numInitialGateways*numUsers), metrics.GetSumOfGauges("cortex_bucket_stores_tenants_synced"))

	// Scale up store-gateways.
	var scaleUpGateways []*StoreGateway
	scaleUpRegistries := dskit_metrics.NewTenantRegistries(log.NewNopLogger())
	numAllGateways := numInitialGateways + numScaleUpGateways

	for i := numInitialGateways + 1; i <= numAllGateways; i++ {
		g, instanceID, reg := createStoreGateway(i, 10*time.Second) // Intentionally high "wait stability min duration".
		scaleUpGateways = append(scaleUpGateways, g)
		scaleUpRegistries.AddTenantRegistry(instanceID, reg)
		allRegistries.AddTenantRegistry(instanceID, reg)
	}

	// Start all new gateways concurrently.
	for _, g := range scaleUpGateways {
		require.NoError(t, g.StartAsync(ctx))
		t.Cleanup(func() {
			assert.NoError(t, services.StopAndAwaitTerminated(ctx, g))
		})
	}

	// Since we configured the new store-gateways with an high "wait stability min duration", we expect
	// them to join the ring at start up (with JOINING state) but then wait at least the min duration
	// before syncing blocks and becoming ACTIVE. This give us enough time to check how the initial
	// store-gateways behaves with regards to blocks syncing while other replicas are JOINING.

	// Wait until all the initial store-gateways sees all new store-gateways too.
	dstest.Poll(t, 11*time.Second, float64(numAllGateways*numInitialGateways), func() interface{} {
		metrics := initialRegistries.BuildMetricFamiliesPerTenant()
		return metrics.GetSumOfGauges("cortex_ring_members")
	})

	// We expect each block to be available for querying on at least 1 initial store-gateway.
	for _, userID := range []string{"user-1", "user-2"} {
		idx, err := bucketindex.ReadIndex(ctx, bucketClient, userID, nil, log.NewNopLogger())
		require.NoError(t, err)

		for _, block := range idx.Blocks {
			queried := false

			for _, g := range initialGateways {
				srv := newStoreGatewayTestServer(t, g)

				req := &storepb.SeriesRequest{MinTime: math.MinInt64, MaxTime: math.MaxInt64}
				_, _, hints, _, err := srv.Series(setUserIDToGRPCContext(ctx, userID), req)
				require.NoError(t, err)

				for _, b := range hints.QueriedBlocks {
					if b.Id == block.ID.String() {
						queried = true
					}
				}
			}

			assert.True(t, queried, "block %s has been successfully queried on initial store-gateways", block.ID.String())
		}
	}

	// Wait until all new gateways are running.
	for _, g := range scaleUpGateways {
		require.NoError(t, g.AwaitRunning(ctx))
	}

	// At this point the new store-gateways are expected to be ACTIVE in the ring and all the initial
	// store-gateways should unload blocks they don't own anymore.
	dstest.Poll(t, 5*time.Second, float64(expectedBlocksLoaded), func() interface{} {
		metrics := allRegistries.BuildMetricFamiliesPerTenant()
		return metrics.GetSumOfGauges("cortex_bucket_store_blocks_loaded")
	})
}

func TestStoreGateway_ShouldSupportLoadRingTokensFromFile(t *testing.T) {
	test.VerifyNoLeak(t)

	tests := map[string]struct {
		storedTokens      ring.Tokens
		expectedNumTokens int
	}{
		"stored tokens are less than the configured ones": {
			storedTokens:      generateSortedTokens(ringNumTokensDefault - 10),
			expectedNumTokens: ringNumTokensDefault,
		},
		"stored tokens are equal to the configured ones": {
			storedTokens:      generateSortedTokens(ringNumTokensDefault),
			expectedNumTokens: ringNumTokensDefault,
		},
		"stored tokens are more then the configured ones": {
			storedTokens:      generateSortedTokens(ringNumTokensDefault + 10),
			expectedNumTokens: ringNumTokensDefault + 10,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			tokensFile, err := os.CreateTemp(os.TempDir(), "tokens-*")
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, os.Remove(tokensFile.Name())) })

			// Store some tokens to the file.
			require.NoError(t, testData.storedTokens.StoreToFile(tokensFile.Name()))

			ctx := context.Background()
			gatewayCfg := mockGatewayConfig()
			gatewayCfg.ShardingRing.TokensFilePath = tokensFile.Name()

			storageCfg := mockStorageConfig(t)
			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			bucketClient := &bucket.ClientMock{}
			bucketClient.MockIter("", []string{}, nil)

			g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), log.NewNopLogger(), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, g)) })
			assert.False(t, g.ringLifecycler.IsRegistered())

			require.NoError(t, services.StartAndAwaitRunning(ctx, g))
			assert.True(t, g.ringLifecycler.IsRegistered())
			assert.Equal(t, ring.ACTIVE, g.ringLifecycler.GetState())
			assert.Len(t, g.ringLifecycler.GetTokens(), testData.expectedNumTokens)
			assert.Subset(t, g.ringLifecycler.GetTokens(), testData.storedTokens)
		})
	}
}

func TestStoreGateway_SyncOnRingTopologyChanged(t *testing.T) {
	test.VerifyNoLeak(t)

	registeredAt := time.Now()

	tests := map[string]struct {
		setupRing    func(desc *ring.Desc)
		updateRing   func(desc *ring.Desc)
		expectedSync bool
	}{
		"should sync when an instance is added to the ring": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{})
			},
			updateRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt, false, time.Time{})
			},
			expectedSync: true,
		},
		"should sync when an instance is removed from the ring": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{})
				desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt, false, time.Time{})
			},
			updateRing: func(desc *ring.Desc) {
				desc.RemoveIngester("instance-1")
			},
			expectedSync: true,
		},
		"should sync when an instance changes state": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{})
				desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.JOINING, registeredAt, false, time.Time{})
			},
			updateRing: func(desc *ring.Desc) {
				instance := desc.Ingesters["instance-2"]
				instance.State = ring.ACTIVE
				desc.Ingesters["instance-2"] = instance
			},
			expectedSync: true,
		},
		"should sync when an healthy instance becomes unhealthy": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{})
				desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt, false, time.Time{})
			},
			updateRing: func(desc *ring.Desc) {
				instance := desc.Ingesters["instance-2"]
				instance.Timestamp = time.Now().Add(-time.Hour).Unix()
				desc.Ingesters["instance-2"] = instance
			},
			expectedSync: true,
		},
		"should sync when an unhealthy instance becomes healthy": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{})

				instance := desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt, false, time.Time{})
				instance.Timestamp = time.Now().Add(-time.Hour).Unix()
				desc.Ingesters["instance-2"] = &instance
			},
			updateRing: func(desc *ring.Desc) {
				instance := desc.Ingesters["instance-2"]
				instance.Timestamp = time.Now().Unix()
				desc.Ingesters["instance-2"] = instance
			},
			expectedSync: true,
		},
		"should NOT sync when an instance updates the heartbeat": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{})
				desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt, false, time.Time{})
			},
			updateRing: func(desc *ring.Desc) {
				instance := desc.Ingesters["instance-2"]
				instance.Timestamp = time.Now().Add(time.Second).Unix()
				desc.Ingesters["instance-2"] = instance
			},
			expectedSync: false,
		},
		"should NOT sync when an instance is auto-forgotten in the ring but was already unhealthy in the previous state": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{})
				desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt, false, time.Time{})

				// Set it already unhealthy.
				instance := desc.Ingesters["instance-2"]
				instance.Timestamp = time.Now().Add(-time.Hour).Unix()
				desc.Ingesters["instance-2"] = instance
			},
			updateRing: func(desc *ring.Desc) {
				// Remove the unhealthy instance from the ring.
				desc.RemoveIngester("instance-2")
			},
			expectedSync: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			gatewayCfg := mockGatewayConfig()
			gatewayCfg.ShardingRing.RingCheckPeriod = 100 * time.Millisecond

			storageCfg := mockStorageConfig(t)
			storageCfg.BucketStore.SyncInterval = time.Hour // Do not trigger the periodic sync in this test.

			reg := prometheus.NewPedanticRegistry()
			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			bucketClient := &bucket.ClientMock{}
			bucketClient.MockIter("", []string{}, nil)

			g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), log.NewNopLogger(), reg, nil)
			require.NoError(t, err)

			// Store the initial ring state before starting the gateway.
			require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
				ringDesc := ring.GetOrCreateRingDesc(in)
				testData.setupRing(ringDesc)
				return ringDesc, true, nil
			}))

			require.NoError(t, services.StartAndAwaitRunning(ctx, g))
			t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, g)) })

			// Assert on the initial state.
			regs := dskit_metrics.NewTenantRegistries(log.NewNopLogger())
			regs.AddTenantRegistry("test", reg)
			metrics := regs.BuildMetricFamiliesPerTenant()
			assert.Equal(t, float64(1), metrics.GetSumOfCounters("cortex_storegateway_bucket_sync_total"))

			// Change the ring topology.
			require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
				ringDesc := ring.GetOrCreateRingDesc(in)
				testData.updateRing(ringDesc)
				return ringDesc, true, nil
			}))

			// Assert whether the sync triggered or not.
			if testData.expectedSync {
				dstest.Poll(t, time.Second, float64(2), func() interface{} {
					metrics := regs.BuildMetricFamiliesPerTenant()
					return metrics.GetSumOfCounters("cortex_storegateway_bucket_sync_total")
				})
			} else {
				// Give some time to the store-gateway to trigger the sync (if any).
				time.Sleep(250 * time.Millisecond)

				metrics := regs.BuildMetricFamiliesPerTenant()
				assert.Equal(t, float64(1), metrics.GetSumOfCounters("cortex_storegateway_bucket_sync_total"))
			}
		})
	}
}

func TestStoreGateway_SyncShouldKeepPreviousBlocksIfInstanceIsUnhealthyInTheRing(t *testing.T) {
	test.VerifyNoLeak(t)

	const (
		instanceID   = "instance-1"
		instanceAddr = "127.0.0.1"
		userID       = "user-1"
		metricName   = "series_1"
	)

	ctx := context.Background()
	gatewayCfg := mockGatewayConfig()
	gatewayCfg.ShardingRing.InstanceID = instanceID
	gatewayCfg.ShardingRing.InstanceAddr = instanceAddr
	gatewayCfg.ShardingRing.RingCheckPeriod = time.Hour // Do not trigger the sync each time the ring changes (we want to control it in this test).

	storageCfg := mockStorageConfig(t)
	storageCfg.BucketStore.SyncInterval = time.Hour // Do not trigger the periodic sync (we want to control it in this test).

	reg := prometheus.NewPedanticRegistry()
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	storageDir := t.TempDir()

	// Generate a real TSDB block in the storage.
	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	generateStorageBlock(t, storageDir, userID, metricName, 10, 100, 15)
	createBucketIndex(t, bucket, userID)

	g, err := newStoreGateway(gatewayCfg, storageCfg, bucket, ringStore, defaultLimitsOverrides(t), log.NewNopLogger(), reg, nil)
	require.NoError(t, err)

	srv := newStoreGatewayTestServer(t, g)

	// No sync retries to speed up tests.
	g.stores.syncBackoffConfig = backoff.Config{MaxRetries: 1}

	// Start the store-gateway.
	require.NoError(t, services.StartAndAwaitRunning(ctx, g))
	t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, g)) })

	t.Run("store-gateway is healthy in the ring", func(t *testing.T) {
		g.syncStores(ctx, syncReasonPeriodic)

		// Run query and ensure the block is queried.
		req := &storepb.SeriesRequest{MinTime: math.MinInt64, MaxTime: math.MaxInt64}
		_, _, hints, _, err := srv.Series(setUserIDToGRPCContext(ctx, userID), req)
		require.NoError(t, err)
		assert.Len(t, hints.QueriedBlocks, 1)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
			# TYPE cortex_bucket_store_blocks_loaded gauge
			cortex_bucket_store_blocks_loaded{component="store-gateway"} 1

			# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
			# TYPE cortex_bucket_store_block_loads_total counter
			cortex_bucket_store_block_loads_total{component="store-gateway"} 1

			# HELP cortex_bucket_store_block_load_failures_total Total number of failed remote block loading attempts.
			# TYPE cortex_bucket_store_block_load_failures_total counter
			cortex_bucket_store_block_load_failures_total{component="store-gateway"} 0
		`),
			"cortex_bucket_store_blocks_loaded",
			"cortex_bucket_store_block_loads_total",
			"cortex_bucket_store_block_load_failures_total",
		))
	})

	t.Run("store-gateway is unhealthy in the ring", func(t *testing.T) {
		// Change heartbeat timestamp in the ring to make it unhealthy.
		require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
			ringDesc := ring.GetOrCreateRingDesc(in)
			instance := ringDesc.Ingesters[instanceID]
			instance.Timestamp = time.Now().Add(-time.Hour).Unix()
			ringDesc.Ingesters[instanceID] = instance
			return ringDesc, true, nil
		}))

		// Wait until the ring client has received the update.
		// We expect the set of healthy instances to be empty.
		dstest.Poll(t, 5*time.Second, true, func() interface{} {
			actual, err := g.ring.GetAllHealthy(BlocksOwnerSync)
			return err == nil && len(actual.Instances) == 0
		})

		g.syncStores(ctx, syncReasonPeriodic)

		// Run query and ensure the block is queried.
		req := &storepb.SeriesRequest{MinTime: math.MinInt64, MaxTime: math.MaxInt64}
		_, _, hints, _, err := srv.Series(setUserIDToGRPCContext(ctx, userID), req)
		require.NoError(t, err)
		assert.Len(t, hints.QueriedBlocks, 1)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
			# TYPE cortex_bucket_store_blocks_loaded gauge
			cortex_bucket_store_blocks_loaded{component="store-gateway"} 1

			# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
			# TYPE cortex_bucket_store_block_loads_total counter
			cortex_bucket_store_block_loads_total{component="store-gateway"} 1
		`),
			"cortex_bucket_store_blocks_loaded",
			"cortex_bucket_store_block_loads_total",
		))
	})

	t.Run("store-gateway is missing in the ring (e.g. removed from another instance because of the auto-forget feature)", func(t *testing.T) {
		// Remove the instance from the ring.
		require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
			ringDesc := ring.GetOrCreateRingDesc(in)
			delete(ringDesc.Ingesters, instanceID)
			return ringDesc, true, nil
		}))

		// Wait until the ring client has received the update.
		// We expect the ring to be empty.
		dstest.Poll(t, 5*time.Second, ring.ErrEmptyRing, func() interface{} {
			_, err := g.ring.GetAllHealthy(BlocksOwnerSync)
			return err
		})

		g.syncStores(ctx, syncReasonPeriodic)

		// Run query and ensure the block is queried.
		req := &storepb.SeriesRequest{MinTime: math.MinInt64, MaxTime: math.MaxInt64}
		_, _, hints, _, err := srv.Series(setUserIDToGRPCContext(ctx, userID), req)
		require.NoError(t, err)
		assert.Len(t, hints.QueriedBlocks, 1)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
			# TYPE cortex_bucket_store_blocks_loaded gauge
			cortex_bucket_store_blocks_loaded{component="store-gateway"} 1

			# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
			# TYPE cortex_bucket_store_block_loads_total counter
			cortex_bucket_store_block_loads_total{component="store-gateway"} 1
		`),
			"cortex_bucket_store_blocks_loaded",
			"cortex_bucket_store_block_loads_total",
		))
	})

	t.Run("store-gateway is re-registered to the ring and it's healthy", func(t *testing.T) {
		// Re-register the instance to the ring.
		require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
			ringDesc := ring.GetOrCreateRingDesc(in)
			ringDesc.AddIngester(instanceID, instanceAddr, "", ring.Tokens{1, 2, 3}, ring.ACTIVE, time.Now(), false, time.Time{})
			return ringDesc, true, nil
		}))

		// Wait until the ring client has received the update.
		dstest.Poll(t, 5*time.Second, true, func() interface{} {
			actual, err := g.ring.GetAllHealthy(BlocksOwnerSync)
			return err == nil && actual.Includes(instanceAddr)
		})

		g.syncStores(ctx, syncReasonPeriodic)

		// Run query and ensure the block is queried.
		req := &storepb.SeriesRequest{MinTime: math.MinInt64, MaxTime: math.MaxInt64}
		_, _, hints, _, err := srv.Series(setUserIDToGRPCContext(ctx, userID), req)
		require.NoError(t, err)
		assert.Len(t, hints.QueriedBlocks, 1)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
			# TYPE cortex_bucket_store_blocks_loaded gauge
			cortex_bucket_store_blocks_loaded{component="store-gateway"} 1

			# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
			# TYPE cortex_bucket_store_block_loads_total counter
			cortex_bucket_store_block_loads_total{component="store-gateway"} 1
		`),
			"cortex_bucket_store_blocks_loaded",
			"cortex_bucket_store_block_loads_total",
		))
	})
}

func TestStoreGateway_RingLifecyclerAutoForgetUnhealthyInstances(t *testing.T) {
	runTest := func(t *testing.T, autoForgetEnabled bool) {
		test.VerifyNoLeak(t)

		const unhealthyInstanceID = "unhealthy-id"
		const heartbeatTimeout = time.Minute

		ctx := context.Background()
		gatewayCfg := mockGatewayConfig()
		gatewayCfg.ShardingRing.HeartbeatPeriod = 100 * time.Millisecond
		gatewayCfg.ShardingRing.HeartbeatTimeout = heartbeatTimeout
		gatewayCfg.ShardingRing.AutoForgetEnabled = autoForgetEnabled

		storageCfg := mockStorageConfig(t)

		ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { assert.NoError(t, closer.Close()) })

		bucketClient := &bucket.ClientMock{}
		bucketClient.MockIter("", []string{}, nil)

		g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), log.NewNopLogger(), nil, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(ctx, g))
		t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, g)) })

		// Add an unhealthy instance to the ring.
		require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
			ringDesc := ring.GetOrCreateRingDesc(in)

			instance := ringDesc.AddIngester(unhealthyInstanceID, "1.1.1.1", "", generateSortedTokens(ringNumTokensDefault), ring.ACTIVE, time.Now(), false, time.Time{})
			instance.Timestamp = time.Now().Add(-(ringAutoForgetUnhealthyPeriods + 1) * heartbeatTimeout).Unix()
			ringDesc.Ingesters[unhealthyInstanceID] = &instance

			return ringDesc, true, nil
		}))

		// Assert whether the unhealthy instance has been removed.
		const maxWaitingTime = time.Second

		if autoForgetEnabled {
			// Ensure the unhealthy instance is removed from the ring.
			dstest.Poll(t, maxWaitingTime, false, func() interface{} {
				d, err := ringStore.Get(ctx, RingKey)
				if err != nil {
					return err
				}

				_, ok := ring.GetOrCreateRingDesc(d).Ingesters[unhealthyInstanceID]
				return ok
			})
		} else {
			// Ensure the unhealthy instance has not been removed from the ring.
			time.Sleep(maxWaitingTime)

			d, err := ringStore.Get(ctx, RingKey)
			require.NoError(t, err)

			_, exists := ring.GetOrCreateRingDesc(d).Ingesters[unhealthyInstanceID]
			require.True(t, exists)
		}
	}

	t.Run("should auto-forget unhealthy instances in the ring when auto-forget is enabled", func(t *testing.T) {
		runTest(t, true)
	})

	t.Run("should not auto-forget unhealthy instances in the ring when auto-forget is disabled", func(t *testing.T) {
		runTest(t, false)
	})
}

func TestStoreGateway_SeriesQueryingShouldRemoveExternalLabels(t *testing.T) {
	test.VerifyNoLeak(t)

	ctx := context.Background()
	logger := log.NewNopLogger()
	userID := "user-1"

	storageDir := t.TempDir()

	// Generate 2 TSDB blocks with the same exact series (and data points).
	numSeries := 2
	now := time.Now()
	minT := now.Add(-1*time.Hour).Unix() * 1000
	maxT := now.Unix() * 1000
	step := (maxT - minT) / int64(numSeries)
	mockTSDB(t, path.Join(storageDir, userID), numSeries, 0, minT, maxT)
	mockTSDB(t, path.Join(storageDir, userID), numSeries, 0, minT, maxT)

	bucketClient, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	createBucketIndex(t, bucketClient, userID)

	// Find the created blocks (we expect 2).
	var blockIDs []string
	require.NoError(t, bucketClient.Iter(ctx, "user-1/", func(key string) error {
		if _, ok := block.IsBlockDir(key); ok {
			blockIDs = append(blockIDs, strings.TrimSuffix(strings.TrimPrefix(key, userID+"/"), "/"))
		}
		return nil
	}))
	require.Len(t, blockIDs, 2)

	// Inject different external labels for each block.
	for idx, blockID := range blockIDs {
		meta := block.ThanosMeta{
			Labels: map[string]string{
				mimir_tsdb.DeprecatedTenantIDExternalLabel:   userID,
				mimir_tsdb.DeprecatedIngesterIDExternalLabel: fmt.Sprintf("ingester-%d", idx),
				mimir_tsdb.CompactorShardIDExternalLabel:     fmt.Sprintf("%d_of_2", (idx%2)+1),
				mimir_tsdb.DeprecatedShardIDExternalLabel:    fmt.Sprintf("shard-%d", idx),
			},
			Source: block.TestSource,
		}

		_, err := block.InjectThanosMeta(logger, filepath.Join(storageDir, userID, blockID), meta, nil)
		require.NoError(t, err)
	}

	// Create a store-gateway used to query back the series from the blocks.
	gatewayCfg := mockGatewayConfig()
	storageCfg := mockStorageConfig(t)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), logger, nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, g))
	t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, g)) })

	srv := newStoreGatewayTestServer(t, g)

	for _, streamingBatchSize := range []int{0, 1, 5} {
		t.Run(fmt.Sprintf("streamingBatchSize=%d", streamingBatchSize), func(t *testing.T) {
			// Query back all series.
			req := &storepb.SeriesRequest{
				MinTime: minT,
				MaxTime: maxT,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".*"},
				},
				StreamingChunksBatchSize: uint64(streamingBatchSize),
			}
			seriesSet, warnings, _, _, err := srv.Series(setUserIDToGRPCContext(ctx, userID), req)
			require.NoError(t, err)
			assert.Empty(t, warnings)
			assert.Len(t, seriesSet, numSeries)

			for seriesID := 0; seriesID < numSeries; seriesID++ {
				actual := seriesSet[seriesID]

				// Ensure Mimir external labels have been removed.
				assert.Equal(t, []mimirpb.LabelAdapter{{Name: "series_id", Value: strconv.Itoa(seriesID)}}, actual.Labels)

				// Ensure samples have been correctly queried. The store-gateway doesn't deduplicate chunks,
				// so the same sample is returned twice because in this test we query two identical blocks.
				samples, err := readSamplesFromChunks(actual.Chunks)
				require.NoError(t, err)
				assert.Equal(t, []sample{
					{t: minT + (step * int64(seriesID)), v: float64(seriesID)},
					{t: minT + (step * int64(seriesID)), v: float64(seriesID)},
				}, samples)
			}
		})
	}
}

func TestStoreGateway_Series_QuerySharding(t *testing.T) {
	test.VerifyNoLeak(t)

	var (
		ctx    = context.Background()
		userID = "user-1"
		series = []labels.Labels{
			labels.FromStrings(labels.MetricName, "series_1"), // Hash: 12248531033489120077
			labels.FromStrings(labels.MetricName, "series_2"), // Hash: 4624373102974193462
			labels.FromStrings(labels.MetricName, "series_3"), // Hash: 11488854180004364397
			labels.FromStrings(labels.MetricName, "series_4"), // Hash: 7076372709108762848
			labels.FromStrings(labels.MetricName, "series_5"), // Hash: 2682489904774096023
		}
	)

	tests := map[string]struct {
		matchers        []storepb.LabelMatcher
		expectedMetrics []string
	}{
		"should touch all series on sharding disabled": {
			matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_RE, Name: labels.MetricName, Value: ".*"},
			},
			expectedMetrics: []string{
				"series_1", "series_2", "series_3", "series_4", "series_5",
			},
		},
		"should touch only series belonging to the specified shard": {
			matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_RE, Name: labels.MetricName, Value: ".*"},
				{Type: storepb.LabelMatcher_EQ, Name: sharding.ShardLabel, Value: sharding.ShardSelector{
					ShardIndex: 2,
					ShardCount: 3,
				}.LabelValue()},
			},
			expectedMetrics: []string{
				"series_2", "series_4",
			},
		},
	}

	for _, streamingBatchSize := range []int{0, 1, 5} {
		t.Run(fmt.Sprintf("streamingBatchSize=%d", streamingBatchSize), func(t *testing.T) {
			// Prepare the storage dir.
			bucketClient, storageDir := mimir_testutil.PrepareFilesystemBucket(t)

			// Generate a TSDB block in the storage dir, containing the fixture series.
			mockTSDBWithGenerator(t, path.Join(storageDir, userID), func() func() (bool, labels.Labels, int64, float64) {
				nextID := 0
				return func() (bool, labels.Labels, int64, float64) {
					if nextID >= len(series) {
						return false, labels.Labels{}, 0, 0
					}

					nextSeries := series[nextID]
					nextID++

					return true, nextSeries, util.TimeToMillis(time.Now().Add(-time.Duration(nextID) * time.Second)), float64(nextID)
				}
			}())

			createBucketIndex(t, bucketClient, userID)

			// Create a store-gateway.
			gatewayCfg := mockGatewayConfig()
			storageCfg := mockStorageConfig(t)

			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), log.NewNopLogger(), nil, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, g))
			t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, g)) })

			srv := newStoreGatewayTestServer(t, g)

			for testName, testData := range tests {
				t.Run(testName, func(t *testing.T) {
					req := &storepb.SeriesRequest{
						MinTime:                  math.MinInt64,
						MaxTime:                  math.MaxInt64,
						Matchers:                 testData.matchers,
						StreamingChunksBatchSize: uint64(streamingBatchSize),
					}

					seriesSet, warnings, _, _, err := srv.Series(setUserIDToGRPCContext(ctx, userID), req)
					require.NoError(t, err)
					assert.Empty(t, warnings)

					actualMetrics := make([]string, 0, len(seriesSet))
					for _, s := range seriesSet {
						actualMetrics = append(actualMetrics, promLabels(s).Get(labels.MetricName))
					}
					assert.ElementsMatch(t, testData.expectedMetrics, actualMetrics)
				})
			}
		})
	}
}

func TestStoreGateway_Series_QueryShardingShouldGuaranteeSeriesShardingConsistencyOverTheTime(t *testing.T) {
	test.VerifyNoLeak(t)

	const (
		numSeries = 100
		numShards = 2
	)

	var (
		ctx    = context.Background()
		userID = "user-1"

		// You should NEVER CHANGE the expected series here, otherwise it means you're introducing
		// a backward incompatible change.
		expectedSeriesIDByShard = map[string][]int{
			"1_of_2": {0, 1, 10, 12, 16, 18, 2, 22, 23, 24, 26, 28, 29, 3, 30, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 47, 53, 54, 57, 58, 60, 61, 63, 66, 67, 68, 69, 7, 71, 75, 77, 80, 81, 83, 84, 86, 87, 89, 9, 90, 91, 92, 94, 96, 98, 99},
			"2_of_2": {11, 13, 14, 15, 17, 19, 20, 21, 25, 27, 31, 32, 37, 38, 4, 45, 46, 48, 49, 5, 50, 51, 52, 55, 56, 59, 6, 62, 64, 65, 70, 72, 73, 74, 76, 78, 79, 8, 82, 85, 88, 93, 95, 97},
		}
	)

	// Prepare the storage dir.
	bucketClient, storageDir := mimir_testutil.PrepareFilesystemBucket(t)

	// Generate a TSDB block in the storage dir, containing the fixture series.
	mockTSDBWithGenerator(t, path.Join(storageDir, userID), func() func() (bool, labels.Labels, int64, float64) {
		nextID := 0
		return func() (bool, labels.Labels, int64, float64) {
			if nextID >= numSeries {
				return false, labels.Labels{}, 0, 0
			}

			nextSeries := labels.FromStrings(labels.MetricName, "test", "series_id", strconv.Itoa(nextID))
			nextID++

			return true, nextSeries, util.TimeToMillis(time.Now().Add(-time.Duration(nextID) * time.Second)), float64(nextID)
		}
	}())

	createBucketIndex(t, bucketClient, userID)

	// Create a store-gateway.
	gatewayCfg := mockGatewayConfig()
	storageCfg := mockStorageConfig(t)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), log.NewNopLogger(), nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, g))
	t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, g)) })

	srv := newStoreGatewayTestServer(t, g)

	// Query all series, 1 shard at a time.
	for shardID := 0; shardID < numShards; shardID++ {
		for _, streamingBatchSize := range []int{0, 1, 5} {
			t.Run(fmt.Sprintf("streamingBatchSize=%d", streamingBatchSize), func(t *testing.T) {
				shardLabel := sharding.FormatShardIDLabelValue(uint64(shardID), numShards)
				expectedSeriesIDs := expectedSeriesIDByShard[shardLabel]

				req := &storepb.SeriesRequest{
					MinTime: math.MinInt64,
					MaxTime: math.MaxInt64,
					Matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "series_id", Value: ".+"},
						{Type: storepb.LabelMatcher_EQ, Name: sharding.ShardLabel, Value: shardLabel},
					},
					StreamingChunksBatchSize: uint64(streamingBatchSize),
				}

				seriesSet, warnings, _, _, err := srv.Series(setUserIDToGRPCContext(ctx, userID), req)
				require.NoError(t, err)
				assert.Empty(t, warnings)
				require.Greater(t, len(seriesSet), 0)

				for _, series := range seriesSet {
					// Ensure the series below to the right shard.
					seriesLabels := mimirpb.FromLabelAdaptersToLabels(series.Labels)
					seriesID, err := strconv.Atoi(seriesLabels.Get("series_id"))
					require.NoError(t, err)

					assert.Contains(t, expectedSeriesIDs, seriesID, "series:", seriesLabels.String())
				}
			})
		}
	}
}

func TestStoreGateway_Series_QueryShardingConcurrency(t *testing.T) {
	test.VerifyNoLeak(t)

	var (
		ctx        = context.Background()
		userID     = "user-1"
		numSeries  = 1000
		numQueries = 100
		shardCount = 16
		now        = time.Now()
	)

	// Prepare the storage dir.
	bucketClient, storageDir := mimir_testutil.PrepareFilesystemBucket(t)

	// Generate a TSDB block in the storage dir, containing the fixture series.
	mockTSDBWithGenerator(t, path.Join(storageDir, userID), func() func() (bool, labels.Labels, int64, float64) {
		nextID := 0
		return func() (bool, labels.Labels, int64, float64) {
			if nextID >= numSeries {
				return false, labels.Labels{}, 0, 0
			}

			series := labels.New(labels.Label{Name: labels.MetricName, Value: fmt.Sprintf("series_%d", nextID)})
			nextID++

			return true, series, util.TimeToMillis(now), float64(nextID)
		}
	}())

	createBucketIndex(t, bucketClient, userID)

	// Create a store-gateway.
	gatewayCfg := mockGatewayConfig()
	storageCfg := mockStorageConfig(t)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), log.NewNopLogger(), nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, g))
	t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, g)) })

	srv := newStoreGatewayTestServer(t, g)

	for _, streamingBatchSize := range []int{0, 1, 5} {
		t.Run(fmt.Sprintf("streamingBatchSize=%d", streamingBatchSize), func(t *testing.T) {
			// Keep track of all responses received (by shard).
			responsesMx := sync.Mutex{}
			responses := make(map[int][][]*storepb.Series)

			wg := sync.WaitGroup{}
			wg.Add(numQueries)

			for i := 0; i < numQueries; i++ {
				go func(shardIndex int) {
					defer wg.Done()

					req := &storepb.SeriesRequest{
						MinTime: math.MinInt64,
						MaxTime: math.MaxInt64,
						Matchers: []storepb.LabelMatcher{
							{Type: storepb.LabelMatcher_RE, Name: labels.MetricName, Value: ".*"},
							{Type: storepb.LabelMatcher_EQ, Name: sharding.ShardLabel, Value: sharding.ShardSelector{
								ShardIndex: uint64(shardIndex),
								ShardCount: uint64(shardCount),
							}.LabelValue()},
						},
						StreamingChunksBatchSize: uint64(streamingBatchSize),
					}

					seriesSet, warnings, _, _, err := srv.Series(setUserIDToGRPCContext(ctx, userID), req)
					require.NoError(t, err)
					assert.Empty(t, warnings)

					responsesMx.Lock()
					responses[shardIndex] = append(responses[shardIndex], seriesSet)
					responsesMx.Unlock()
				}(i % shardCount)
			}

			// Wait until all requests completed.
			wg.Wait()

			// We expect all responses for a given shard contain the same series
			// and all shards merged together contain all the series in the TSDB block.
			totalSeries := 0

			for shardIndex := 0; shardIndex < shardCount; shardIndex++ {
				var expected []*storepb.Series

				for resIdx, res := range responses[shardIndex] {
					// We consider the 1st response for a shard as the expected one
					// (all in all we expect all responses to be the same).
					if resIdx == 0 {
						expected = res
						totalSeries += len(res)
						continue
					}

					assert.Equalf(t, expected, res, "shard: %d", shardIndex)
				}
			}

			assert.Equal(t, numSeries, totalSeries)
		})
	}

}

func TestStoreGateway_SeriesQueryingShouldEnforceMaxChunksPerQueryLimit(t *testing.T) {
	test.VerifyNoLeak(t)

	const chunksQueried = 10

	tests := map[string]struct {
		limit       int
		expectedErr error
	}{
		"no limit enforced if zero": {
			limit:       0,
			expectedErr: nil,
		},
		"should return NO error if the actual number of queried chunks is <= limit": {
			limit:       chunksQueried,
			expectedErr: nil,
		},
		"should return error if the actual number of queried chunks is > limit": {
			limit:       chunksQueried - 1,
			expectedErr: status.Error(http.StatusUnprocessableEntity, fmt.Sprintf("rpc error: code = Code(422) desc = %s", limiter.NewMaxChunksPerQueryLimitError(chunksQueried-1))),
		},
	}

	ctx := context.Background()
	logger := log.NewNopLogger()
	userID := "user-1"

	storageDir := t.TempDir()

	// Generate 1 TSDB block with chunksQueried series. Since each mocked series contains only 1 sample,
	// it will also only have 1 chunk.
	now := time.Now()
	minT := now.Add(-1*time.Hour).Unix() * 1000
	maxT := now.Unix() * 1000
	mockTSDB(t, path.Join(storageDir, userID), chunksQueried, 0, minT, maxT)

	bucketClient, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	createBucketIndex(t, bucketClient, userID)

	// Prepare the request to query back all series (1 chunk per series in this test).
	req := &storepb.SeriesRequest{
		MinTime: minT,
		MaxTime: maxT,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".*"},
		},
	}

	for _, streamingBatchSize := range []int{0, 1, 5} {
		t.Run(fmt.Sprintf("streamingBatchSize=%d", streamingBatchSize), func(t *testing.T) {
			for testName, testData := range tests {
				t.Run(testName, func(t *testing.T) {
					// Customise the limits.
					limits := defaultLimitsConfig()
					limits.MaxChunksPerQuery = testData.limit
					overrides, err := validation.NewOverrides(limits, nil)
					require.NoError(t, err)

					// Create a store-gateway used to query back the series from the blocks.
					gatewayCfg := mockGatewayConfig()
					storageCfg := mockStorageConfig(t)

					ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
					t.Cleanup(func() { assert.NoError(t, closer.Close()) })

					g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, overrides, logger, nil, nil)
					require.NoError(t, err)
					require.NoError(t, services.StartAndAwaitRunning(ctx, g))
					t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, g)) })

					srv := newStoreGatewayTestServer(t, g)

					// Query back all the series (1 chunk per series in this test).
					req.StreamingChunksBatchSize = uint64(streamingBatchSize)
					seriesSet, warnings, _, _, err := srv.Series(setUserIDToGRPCContext(ctx, userID), req)

					if testData.expectedErr != nil {
						require.Error(t, err)
						assert.IsType(t, testData.expectedErr, err)
						s1, ok := grpcutil.ErrorToStatus(err)
						assert.True(t, ok)
						s2, ok := grpcutil.ErrorToStatus(testData.expectedErr)
						assert.True(t, ok)
						assert.Contains(t, s1.Message(), s2.Message())
						assert.Equal(t, s1.Code(), s2.Code())
					} else {
						require.NoError(t, err)
						assert.Empty(t, warnings)
						assert.Len(t, seriesSet, chunksQueried)
					}
				})
			}
		})
	}
}

func mockGatewayConfig() Config {
	cfg := Config{}
	flagext.DefaultValues(&cfg)

	cfg.ShardingRing.InstanceID = "test"
	cfg.ShardingRing.InstanceAddr = "127.0.0.1"
	cfg.ShardingRing.WaitStabilityMinDuration = 0
	cfg.ShardingRing.WaitStabilityMaxDuration = 0

	return cfg
}

func mockStorageConfig(t *testing.T) mimir_tsdb.BlocksStorageConfig {
	tmpDir := t.TempDir()

	cfg := mimir_tsdb.BlocksStorageConfig{}
	flagext.DefaultValues(&cfg)

	cfg.BucketStore.IgnoreBlocksWithin = 0
	cfg.BucketStore.SyncDir = tmpDir

	return cfg
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

func mockTSDBWithGenerator(t *testing.T, dir string, next func() (bool, labels.Labels, int64, float64)) {
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

	for {
		hasNext, lbls, timestamp, value := next()
		if !hasNext {
			break
		}

		app := db.Appender(context.Background())
		_, err := app.Append(0, lbls, timestamp, value)
		require.NoError(t, err)
		require.NoError(t, app.Commit())
		require.NoError(t, db.Compact(ctx))
	}

	require.NoError(t, db.Snapshot(dir, true))
	require.NoError(t, db.Close())
}

func generateSortedTokens(numTokens int) ring.Tokens {
	tokens := ring.NewRandomTokenGenerator().GenerateTokens(numTokens, nil)

	// Ensure generated tokens are sorted.
	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i] < tokens[j]
	})

	return tokens
}

func readSamplesFromChunks(rawChunks []storepb.AggrChunk) ([]sample, error) {
	var samples []sample

	for _, rawChunk := range rawChunks {
		c, err := chunkenc.FromData(chunkenc.EncXOR, rawChunk.Raw.Data)
		if err != nil {
			return nil, err
		}

		it := c.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			if it.Err() != nil {
				return nil, it.Err()
			}

			ts, v := it.At()
			samples = append(samples, sample{
				t: ts,
				v: v,
			})
		}

		if it.Err() != nil {
			return nil, it.Err()
		}
	}

	return samples, nil
}

type sample struct {
	t  int64
	v  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func (s sample) T() int64 {
	return s.t
}

func (s sample) F() float64 {
	return s.v
}

func (s sample) H() *histogram.Histogram {
	return s.h
}

func (s sample) FH() *histogram.FloatHistogram {
	return s.fh
}

func (s sample) Type() chunkenc.ValueType {
	switch {
	case s.h != nil:
		return chunkenc.ValHistogram
	case s.fh != nil:
		return chunkenc.ValFloatHistogram
	default:
		return chunkenc.ValFloat
	}
}

func (s sample) Copy() chunks.Sample {
	c := sample{t: s.t, v: s.v}
	if s.h != nil {
		c.h = s.h.Copy()
	}
	if s.fh != nil {
		c.fh = s.fh.Copy()
	}
	return c
}

func defaultLimitsConfig() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	return limits
}

func defaultLimitsOverrides(t *testing.T) *validation.Overrides {
	overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
	require.NoError(t, err)

	return overrides
}

type mockShardingStrategy struct {
	mock.Mock
}

func (m *mockShardingStrategy) FilterUsers(ctx context.Context, userIDs []string) ([]string, error) {
	args := m.Called(ctx, userIDs)
	return args.Get(0).([]string), args.Error(1)
}

func (m *mockShardingStrategy) FilterBlocks(ctx context.Context, userID string, metas map[ulid.ULID]*block.Meta, loaded map[ulid.ULID]struct{}, synced block.GaugeVec) error {
	args := m.Called(ctx, userID, metas, loaded, synced)
	return args.Error(0)
}

func createBucketIndex(t *testing.T, bkt objstore.Bucket, userID string) *bucketindex.Index {
	updater := bucketindex.NewUpdater(bkt, userID, nil, 16, log.NewNopLogger())
	idx, _, err := updater.UpdateIndex(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, bucketindex.WriteIndex(context.Background(), bkt, userID, nil, idx))

	return idx
}
