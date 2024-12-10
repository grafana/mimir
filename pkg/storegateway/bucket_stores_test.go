// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/bucket_stores_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	filesystemstore "github.com/thanos-io/objstore/providers/filesystem"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"
	grpc_metadata "google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestMain(m *testing.M) {
	test.VerifyNoLeakTestMain(m)
}

func TestBucketStores_InitialSync(t *testing.T) {

	userToMetric := map[string]string{
		"user-1": "series_1",
		"user-2": "series_2",
	}

	ctx := context.Background()
	cfg := prepareStorageConfig(t)

	storageDir := t.TempDir()

	for userID, metricName := range userToMetric {
		generateStorageBlock(t, storageDir, userID, metricName, 10, 100, 15)
	}

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	var allowedTenants *util.AllowedTenants
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, newNoShardingStrategy(), bucket, allowedTenants, defaultLimitsOverrides(t), log.NewLogfmtLogger(os.Stdout), reg)
	require.NoError(t, err)

	// Query series before the initial sync.
	for userID, metricName := range userToMetric {
		seriesSet, warnings, err := querySeries(t, stores, userID, metricName, 20, 40)
		require.NoError(t, err)
		assert.Empty(t, warnings)
		assert.Empty(t, seriesSet)
	}
	for userID := range userToMetric {
		createBucketIndex(t, bucket, userID)
	}
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})

	// Query series after the initial sync.
	for userID, metricName := range userToMetric {
		seriesSet, warnings, err := querySeries(t, stores, userID, metricName, 20, 40)
		require.NoError(t, err)
		assert.Empty(t, warnings)
		require.Len(t, seriesSet, 1)
		assert.Equal(t, []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: metricName}}, seriesSet[0].Labels)
	}

	// Query series of another user.
	seriesSet, warnings, err := querySeries(t, stores, "user-1", "series_2", 20, 40)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Empty(t, seriesSet)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
			# TYPE cortex_bucket_store_blocks_loaded gauge
			cortex_bucket_store_blocks_loaded 2

			# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
			# TYPE cortex_bucket_store_block_loads_total counter
			cortex_bucket_store_block_loads_total 2

			# HELP cortex_bucket_store_block_load_failures_total Total number of failed remote block loading attempts.
			# TYPE cortex_bucket_store_block_load_failures_total counter
			cortex_bucket_store_block_load_failures_total 0

			# HELP cortex_bucket_stores_gate_queries_concurrent_max Number of maximum concurrent queries allowed.
			# TYPE cortex_bucket_stores_gate_queries_concurrent_max gauge
			cortex_bucket_stores_gate_queries_concurrent_max{gate="query"} 200
			cortex_bucket_stores_gate_queries_concurrent_max{gate="index_header"} 4

			# HELP cortex_bucket_stores_gate_queries_in_flight Number of queries that are currently in flight.
			# TYPE cortex_bucket_stores_gate_queries_in_flight gauge
			cortex_bucket_stores_gate_queries_in_flight{gate="query"} 0
			cortex_bucket_stores_gate_queries_in_flight{gate="index_header"} 0
	`),
		"cortex_bucket_store_blocks_loaded",
		"cortex_bucket_store_block_loads_total",
		"cortex_bucket_store_block_load_failures_total",
		"cortex_bucket_stores_gate_queries_concurrent_max",
		"cortex_bucket_stores_gate_queries_in_flight",
	))

	assert.Greater(t, testutil.ToFloat64(stores.syncLastSuccess), float64(0))
}

func TestBucketStores_InitialSyncShouldRetryOnFailure(t *testing.T) {
	const tenantID = "user-1"
	ctx := context.Background()
	cfg := prepareStorageConfig(t)

	storageDir := t.TempDir()

	// Generate a block for the user in the storage.
	generateStorageBlock(t, storageDir, tenantID, "series_1", 10, 100, 15)
	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	createBucketIndex(t, bucket, tenantID)

	// Wrap the bucket to fail the 1st Get() request.
	bucket = &failFirstGetBucket{Bucket: bucket}

	var allowedTenants *util.AllowedTenants
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, newNoShardingStrategy(), bucket, allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), reg)
	require.NoError(t, err)

	// Initial sync should succeed even if a transient error occurs.
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})

	// Query series after the initial sync.
	seriesSet, warnings, err := querySeries(t, stores, tenantID, "series_1", 20, 40)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	require.Len(t, seriesSet, 1)
	assert.Equal(t, []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "series_1"}}, seriesSet[0].Labels)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_blocks_meta_syncs_total Total blocks metadata synchronization attempts
			# TYPE cortex_blocks_meta_syncs_total counter
			cortex_blocks_meta_syncs_total 2

			# HELP cortex_blocks_meta_sync_failures_total Total blocks metadata synchronization failures
			# TYPE cortex_blocks_meta_sync_failures_total counter
			cortex_blocks_meta_sync_failures_total 1

			# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
			# TYPE cortex_bucket_store_blocks_loaded gauge
			cortex_bucket_store_blocks_loaded 1

			# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
			# TYPE cortex_bucket_store_block_loads_total counter
			cortex_bucket_store_block_loads_total 1

			# HELP cortex_bucket_store_block_load_failures_total Total number of failed remote block loading attempts.
			# TYPE cortex_bucket_store_block_load_failures_total counter
			cortex_bucket_store_block_load_failures_total 0
	`),
		"cortex_blocks_meta_syncs_total",
		"cortex_blocks_meta_sync_failures_total",
		"cortex_bucket_store_block_loads_total",
		"cortex_bucket_store_block_load_failures_total",
		"cortex_bucket_store_blocks_loaded",
	))

	assert.Greater(t, testutil.ToFloat64(stores.syncLastSuccess), float64(0))
}

func TestBucketStores_SyncBlocks(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "series_1"
	)

	ctx := context.Background()
	cfg := prepareStorageConfig(t)

	storageDir := t.TempDir()

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	var allowedTenants *util.AllowedTenants
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, newNoShardingStrategy(), bucket, allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), reg)
	require.NoError(t, err)

	// Run an initial sync to discover 1 block.
	generateStorageBlock(t, storageDir, userID, metricName, 10, 100, 15)
	createBucketIndex(t, bucket, userID)
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})

	// Query a range for which we have no samples.
	seriesSet, warnings, err := querySeries(t, stores, userID, metricName, 150, 180)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Empty(t, seriesSet)

	// Generate another block and sync blocks again.
	generateStorageBlock(t, storageDir, userID, metricName, 100, 200, 15)
	createBucketIndex(t, bucket, userID)
	require.NoError(t, stores.SyncBlocks(ctx))

	seriesSet, warnings, err = querySeries(t, stores, userID, metricName, 150, 180)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Len(t, seriesSet, 1)
	assert.Equal(t, []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: metricName}}, seriesSet[0].Labels)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
			# TYPE cortex_bucket_store_blocks_loaded gauge
			cortex_bucket_store_blocks_loaded 2

			# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
			# TYPE cortex_bucket_store_block_loads_total counter
			cortex_bucket_store_block_loads_total 2

			# HELP cortex_bucket_store_block_load_failures_total Total number of failed remote block loading attempts.
			# TYPE cortex_bucket_store_block_load_failures_total counter
			cortex_bucket_store_block_load_failures_total 0

			# HELP cortex_bucket_stores_gate_queries_concurrent_max Number of maximum concurrent queries allowed.
			# TYPE cortex_bucket_stores_gate_queries_concurrent_max gauge
			cortex_bucket_stores_gate_queries_concurrent_max{gate="query"} 200
			cortex_bucket_stores_gate_queries_concurrent_max{gate="index_header"} 4

			# HELP cortex_bucket_stores_gate_queries_in_flight Number of queries that are currently in flight.
			# TYPE cortex_bucket_stores_gate_queries_in_flight gauge
			cortex_bucket_stores_gate_queries_in_flight{gate="query"} 0
			cortex_bucket_stores_gate_queries_in_flight{gate="index_header"} 0
	`),
		"cortex_bucket_store_blocks_loaded",
		"cortex_bucket_store_block_loads_total",
		"cortex_bucket_store_block_load_failures_total",
		"cortex_bucket_stores_gate_queries_concurrent_max",
		"cortex_bucket_stores_gate_queries_in_flight",
	))

	assert.Greater(t, testutil.ToFloat64(stores.syncLastSuccess), float64(0))
}

func TestBucketStores_ownedUsers(t *testing.T) {
	allUsers := []string{"user-1", "user-2", "user-3"}

	tests := map[string]struct {
		shardingStrategy ShardingStrategy
		allowedTenants   *util.AllowedTenants
		expectedStores   int
	}{
		"when sharding is disabled all users should be synced": {
			shardingStrategy: newNoShardingStrategy(),
			expectedStores:   3,
		},
		"when sharding is enabled only stores for filtered users should be created": {
			shardingStrategy: func() ShardingStrategy {
				s := &mockShardingStrategy{}
				s.On("FilterUsers", mock.Anything, allUsers).Return([]string{"user-1", "user-2"}, nil)
				return s
			}(),
			expectedStores: 2,
		},
		"when user is disabled, their stores should not be created": {
			shardingStrategy: newNoShardingStrategy(),
			allowedTenants:   util.NewAllowedTenants(nil, []string{"user-2"}),
			expectedStores:   2,
		},

		"when single user is enabled, only their stores should be created": {
			shardingStrategy: newNoShardingStrategy(),
			allowedTenants:   util.NewAllowedTenants([]string{"user-3"}, nil),
			expectedStores:   1,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := prepareStorageConfig(t)

			bucketClient := &bucket.ClientMock{}
			bucketClient.MockIter("", allUsers, nil)

			stores, err := NewBucketStores(cfg, testData.shardingStrategy, bucketClient, testData.allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), nil)
			require.NoError(t, err)

			// Sync user stores and count the number of times the callback is called.
			ownedUsers, err := stores.ownedUsers(context.Background())

			assert.NoError(t, err)
			bucketClient.AssertNumberOfCalls(t, "Iter", 1)
			assert.Len(t, ownedUsers, testData.expectedStores)
		})
	}
}

func TestBucketStores_Series_ShouldCorrectlyQuerySeriesSpanningMultipleChunks(t *testing.T) {
	for _, lazyLoadingEnabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("lazy loading enabled = %v", lazyLoadingEnabled), func(t *testing.T) {
			testBucketStoresSeriesShouldCorrectlyQuerySeriesSpanningMultipleChunks(t, lazyLoadingEnabled)
		})
	}
}

func TestBucketStores_ChunksAndSeriesLimiterFactoriesInitializedByEnforcedLimits(t *testing.T) {
	const (
		userID                = "user-1"
		overriddenChunksLimit = 1000000
		overriddenSeriesLimit = 2000
	)

	defaultLimits := defaultLimitsConfig()

	tests := map[string]struct {
		tenantLimits        map[string]*validation.Limits
		expectedChunkLimit  uint64
		expectedSeriesLimit uint64
	}{
		"when max_fetched_chunks_per_query and max_fetched_series_per_query are not overridden, their default values are used as the limit of the Limiter": {
			expectedChunkLimit:  uint64(defaultLimits.MaxChunksPerQuery),
			expectedSeriesLimit: uint64(defaultLimits.MaxFetchedSeriesPerQuery),
		},
		"when max_fetched_chunks_per_query and max_fetched_series_per_query are overridden, the overridden values are used as the limit of the Limiter": {
			tenantLimits: map[string]*validation.Limits{
				userID: {
					MaxChunksPerQuery:        overriddenChunksLimit,
					MaxFetchedSeriesPerQuery: overriddenSeriesLimit,
				},
			},
			expectedChunkLimit:  uint64(overriddenChunksLimit),
			expectedSeriesLimit: uint64(overriddenSeriesLimit),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := prepareStorageConfig(t)

			storageDir := t.TempDir()

			bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
			require.NoError(t, err)

			overrides, err := validation.NewOverrides(defaultLimits, validation.NewMockTenantLimits(testData.tenantLimits))
			require.NoError(t, err)

			var allowedTenants *util.AllowedTenants
			reg := prometheus.NewPedanticRegistry()
			stores, err := NewBucketStores(cfg, newNoShardingStrategy(), bucket, allowedTenants, overrides, log.NewNopLogger(), reg)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), stores))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
			})

			store, err := stores.getOrCreateStore(context.Background(), userID)
			require.NoError(t, err)

			chunksLimit := overrides.MaxChunksPerQuery(userID)
			if chunksLimit != 0 {
				chunksLimiter := store.chunksLimiterFactory(promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "chunks"}))
				err = chunksLimiter.Reserve(testData.expectedChunkLimit)
				require.NoError(t, err)

				err = chunksLimiter.Reserve(1)
				require.Error(t, err)
			}

			seriesLimit := overrides.MaxFetchedSeriesPerQuery(userID)
			if seriesLimit != 0 {
				seriesLimiter := store.seriesLimiterFactory(promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "series"}))
				err = seriesLimiter.Reserve(testData.expectedSeriesLimit)
				require.NoError(t, err)

				err = seriesLimiter.Reserve(1)
				require.Error(t, err)
			}

		})
	}
}

func testBucketStoresSeriesShouldCorrectlyQuerySeriesSpanningMultipleChunks(t *testing.T, lazyLoadingEnabled bool) {
	const (
		userID     = "user-1"
		metricName = "series_1"
	)

	ctx := context.Background()
	cfg := prepareStorageConfig(t)
	cfg.BucketStore.IndexHeader.LazyLoadingEnabled = lazyLoadingEnabled
	cfg.BucketStore.IndexHeader.LazyLoadingIdleTimeout = time.Minute

	storageDir := t.TempDir()

	// Generate a single block with 1 series and a lot of samples.
	generateStorageBlock(t, storageDir, userID, metricName, 0, 10000, 1)

	promBlock := openPromBlocks(t, filepath.Join(storageDir, userID))[0]

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	var allowedTenants *util.AllowedTenants
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, newNoShardingStrategy(), bucket, allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), reg)
	require.NoError(t, err)

	createBucketIndex(t, bucket, userID)
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})

	tests := map[string]struct {
		reqMinTime int64
		reqMaxTime int64
	}{
		"query the entire block": {
			reqMinTime: math.MinInt64,
			reqMaxTime: math.MaxInt64,
		},
		"query the beginning of the block": {
			reqMinTime: 0,
			reqMaxTime: 100,
		},
		"query the middle of the block": {
			reqMinTime: 4000,
			reqMaxTime: 4050,
		},
		"query the end of the block": {
			reqMinTime: 9800,
			reqMaxTime: 10000,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Query a range for which we have no samples.
			seriesSet, warnings, err := querySeries(t, stores, userID, metricName, testData.reqMinTime, testData.reqMaxTime)
			require.NoError(t, err)
			assert.Empty(t, warnings)
			assert.Len(t, seriesSet, 1)

			compareToPromChunks(t, seriesSet[0].Chunks, mimirpb.FromLabelAdaptersToLabels(seriesSet[0].Labels), testData.reqMinTime, testData.reqMaxTime, promBlock)
		})
	}
}

func TestBucketStore_Series_ShouldQueryBlockWithOutOfOrderChunks(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "test"
	)

	ctx := context.Background()
	cfg := prepareStorageConfig(t)
	fixtureDir := filepath.Join("fixtures", "test-query-block-with-ooo-chunks")
	storageDir := t.TempDir()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	userBkt := bucket.NewUserBucketClient(userID, bkt, nil)

	seriesWithOutOfOrderChunks := labels.FromStrings("case", "out_of_order", labels.MetricName, metricName)
	seriesWithOverlappingChunks := labels.FromStrings("case", "overlapping", labels.MetricName, metricName)

	// Utility function originally used to generate a block with out of order chunks
	// used by this test. The block has been generated commenting out the checks done
	// by TSDB block Writer to prevent OOO chunks writing.
	_ = func() {
		specs := []*block.SeriesSpec{
			// Series with out of order chunks.
			{
				Labels: seriesWithOutOfOrderChunks,
				Chunks: []chunks.Meta{
					must(chunks.ChunkFromSamples([]chunks.Sample{sample{t: 20, v: 20}, sample{t: 21, v: 21}})),
					must(chunks.ChunkFromSamples([]chunks.Sample{sample{t: 10, v: 10}, sample{t: 11, v: 11}})),
				},
			},
			// Series with out of order and overlapping chunks.
			{
				Labels: seriesWithOverlappingChunks,
				Chunks: []chunks.Meta{
					must(chunks.ChunkFromSamples([]chunks.Sample{sample{t: 20, v: 20}, sample{t: 21, v: 21}})),
					must(chunks.ChunkFromSamples([]chunks.Sample{sample{t: 10, v: 10}, sample{t: 20, v: 20}})),
				},
			},
		}

		_, err := block.GenerateBlockFromSpec(fixtureDir, specs)
		require.NoError(t, err)
	}

	// Copy blocks from fixtures dir to the test bucket.
	entries, err := os.ReadDir(fixtureDir)
	require.NoError(t, err)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		blockID, err := ulid.Parse(entry.Name())
		require.NoErrorf(t, err, "parsing block ID from directory name %q", entry.Name())

		require.NoError(t, block.Upload(ctx, log.NewNopLogger(), userBkt, filepath.Join(fixtureDir, blockID.String()), nil))
	}

	createBucketIndex(t, bkt, userID)

	var allowedTenants *util.AllowedTenants
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, newNoShardingStrategy(), bkt, allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})

	tests := map[string]struct {
		minT                                int64
		maxT                                int64
		expectedSamplesForOutOfOrderChunks  []sample
		expectedSamplesForOverlappingChunks []sample
	}{
		"query all samples": {
			minT:                                math.MinInt64,
			maxT:                                math.MaxInt64,
			expectedSamplesForOutOfOrderChunks:  []sample{{t: 20, v: 20}, {t: 21, v: 21}, {t: 10, v: 10}, {t: 11, v: 11}},
			expectedSamplesForOverlappingChunks: []sample{{t: 20, v: 20}, {t: 21, v: 21}, {t: 10, v: 10}, {t: 20, v: 20}},
		},
		"query samples from 1st chunk only": {
			minT:                                21,
			maxT:                                22, // Not included.
			expectedSamplesForOutOfOrderChunks:  []sample{{t: 20, v: 20}, {t: 21, v: 21}},
			expectedSamplesForOverlappingChunks: []sample{{t: 20, v: 20}, {t: 21, v: 21}},
		},
		"query samples from 2nd (out of order) chunk only": {
			minT:                                10,
			maxT:                                11, // Not included.
			expectedSamplesForOutOfOrderChunks:  []sample{{t: 10, v: 10}, {t: 11, v: 11}},
			expectedSamplesForOverlappingChunks: []sample{{t: 10, v: 10}, {t: 20, v: 20}},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			seriesSet, warnings, err := querySeries(t, stores, userID, metricName, testData.minT, testData.maxT)
			require.NoError(t, err)
			assert.Empty(t, warnings)

			expectedSeries := 0
			if testData.expectedSamplesForOutOfOrderChunks != nil {
				expectedSeries++
			}
			if testData.expectedSamplesForOverlappingChunks != nil {
				expectedSeries++
			}
			require.Len(t, seriesSet, expectedSeries)

			// Check returned samples.
			nextSeriesIdx := 0

			if testData.expectedSamplesForOutOfOrderChunks != nil {
				assert.Equal(t, seriesWithOutOfOrderChunks, promLabels(seriesSet[nextSeriesIdx]))

				samples, err := readSamplesFromChunks(seriesSet[nextSeriesIdx].Chunks)
				require.NoError(t, err)
				assert.Equal(t, testData.expectedSamplesForOutOfOrderChunks, samples)

				nextSeriesIdx++
			}

			if testData.expectedSamplesForOverlappingChunks != nil {
				assert.Equal(t, seriesWithOverlappingChunks, promLabels(seriesSet[nextSeriesIdx]))

				samples, err := readSamplesFromChunks(seriesSet[nextSeriesIdx].Chunks)
				require.NoError(t, err)
				assert.Equal(t, testData.expectedSamplesForOverlappingChunks, samples)
			}
		})
	}
}

func promLabels(m *storepb.Series) labels.Labels {
	return mimirpb.FromLabelAdaptersToLabels(m.Labels)
}

func prepareStorageConfig(t *testing.T) mimir_tsdb.BlocksStorageConfig {
	tmpDir := t.TempDir()

	cfg := mimir_tsdb.BlocksStorageConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BucketStore.SyncDir = tmpDir

	return cfg
}

func generateStorageBlock(t *testing.T, storageDir, userID string, metricName string, minT, maxT int64, step int) {
	// Create a directory for the user (if doesn't already exist).
	userDir := filepath.Join(storageDir, userID)
	if _, err := os.Stat(userDir); err != nil {
		require.NoError(t, os.Mkdir(userDir, os.ModePerm))
	}

	// Create a temporary directory where the TSDB is opened,
	// then it will be snapshotted to the storage directory.
	tmpDir := t.TempDir()

	db, err := tsdb.Open(tmpDir, promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	series := labels.FromStrings(labels.MetricName, metricName)

	app := db.Appender(context.Background())
	for ts := minT; ts < maxT; ts += int64(step) {
		_, err = app.Append(0, series, ts, 1)
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	// Snapshot TSDB to the storage directory.
	require.NoError(t, db.Snapshot(userDir, true))
}

func querySeries(t *testing.T, stores *BucketStores, userID, metricName string, minT, maxT int64) ([]*storepb.Series, annotations.Annotations, error) {
	req := &storepb.SeriesRequest{
		MinTime: minT,
		MaxTime: maxT,
		Matchers: []storepb.LabelMatcher{{
			Type:  storepb.LabelMatcher_EQ,
			Name:  labels.MetricName,
			Value: metricName,
		}},
	}

	srv := newStoreGatewayTestServer(t, stores)
	seriesSet, warnings, _, _, err := srv.Series(setUserIDToGRPCContext(context.Background(), userID), req)

	return seriesSet, warnings, err
}

func setUserIDToGRPCContext(ctx context.Context, userID string) context.Context {
	return grpc_metadata.AppendToOutgoingContext(ctx, GrpcContextMetadataTenantID, userID)
}

func TestBucketStores_deleteLocalFilesForExcludedTenants(t *testing.T) {
	const (
		user1 = "user-1"
		user2 = "user-2"
	)

	userToMetric := map[string]string{
		user1: "series_1",
		user2: "series_2",
	}

	ctx := context.Background()
	cfg := prepareStorageConfig(t)

	storageDir := t.TempDir()

	for userID, metricName := range userToMetric {
		generateStorageBlock(t, storageDir, userID, metricName, 10, 100, 15)
	}

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	for userID := range userToMetric {
		createBucketIndex(t, bucket, userID)
	}

	sharding := userShardingStrategy{}

	var allowedTenants *util.AllowedTenants
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, &sharding, bucket, allowedTenants, defaultLimitsOverrides(t), log.NewNopLogger(), reg)
	require.NoError(t, err)

	// Perform sync.
	sharding.users = []string{user1, user2}
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})
	require.Equal(t, []string{user1, user2}, getUsersInDir(t, cfg.BucketStore.SyncDir))

	metricNames := []string{"cortex_bucket_store_block_drops_total", "cortex_bucket_store_block_loads_total", "cortex_bucket_store_blocks_loaded"}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
        	            	# HELP cortex_bucket_store_block_drops_total Total number of local blocks that were dropped.
        	            	# TYPE cortex_bucket_store_block_drops_total counter
        	            	cortex_bucket_store_block_drops_total 0
        	            	# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
        	            	# TYPE cortex_bucket_store_block_loads_total counter
        	            	cortex_bucket_store_block_loads_total 2
        	            	# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
        	            	# TYPE cortex_bucket_store_blocks_loaded gauge
        	            	cortex_bucket_store_blocks_loaded 2
	`), metricNames...))

	// Single user left in shard.
	sharding.users = []string{user1}
	require.NoError(t, stores.SyncBlocks(ctx))
	require.Equal(t, []string{user1}, getUsersInDir(t, cfg.BucketStore.SyncDir))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
        	            	# HELP cortex_bucket_store_block_drops_total Total number of local blocks that were dropped.
        	            	# TYPE cortex_bucket_store_block_drops_total counter
        	            	cortex_bucket_store_block_drops_total 1
        	            	# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
        	            	# TYPE cortex_bucket_store_block_loads_total counter
        	            	cortex_bucket_store_block_loads_total 2
        	            	# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
        	            	# TYPE cortex_bucket_store_blocks_loaded gauge
        	            	cortex_bucket_store_blocks_loaded 1
	`), metricNames...))

	// No users left in this shard.
	sharding.users = nil
	require.NoError(t, stores.SyncBlocks(ctx))
	require.Equal(t, []string(nil), getUsersInDir(t, cfg.BucketStore.SyncDir))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
        	            	# HELP cortex_bucket_store_block_drops_total Total number of local blocks that were dropped.
        	            	# TYPE cortex_bucket_store_block_drops_total counter
        	            	cortex_bucket_store_block_drops_total 2
        	            	# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
        	            	# TYPE cortex_bucket_store_block_loads_total counter
        	            	cortex_bucket_store_block_loads_total 2
        	            	# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
        	            	# TYPE cortex_bucket_store_blocks_loaded gauge
        	            	cortex_bucket_store_blocks_loaded 0
	`), metricNames...))

	// We can always get user back.
	sharding.users = []string{user1}
	require.NoError(t, stores.SyncBlocks(ctx))
	require.Equal(t, []string{user1}, getUsersInDir(t, cfg.BucketStore.SyncDir))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
        	            	# HELP cortex_bucket_store_block_drops_total Total number of local blocks that were dropped.
        	            	# TYPE cortex_bucket_store_block_drops_total counter
        	            	cortex_bucket_store_block_drops_total 2
        	            	# HELP cortex_bucket_store_block_loads_total Total number of remote block loading attempts.
        	            	# TYPE cortex_bucket_store_block_loads_total counter
        	            	cortex_bucket_store_block_loads_total 3
        	            	# HELP cortex_bucket_store_blocks_loaded Number of currently loaded blocks.
        	            	# TYPE cortex_bucket_store_blocks_loaded gauge
        	            	cortex_bucket_store_blocks_loaded 1
	`), metricNames...))
}

func getUsersInDir(t *testing.T, dir string) []string {
	fs, err := os.ReadDir(dir)
	require.NoError(t, err)

	var result []string
	for _, fi := range fs {
		if fi.IsDir() {
			result = append(result, fi.Name())
		}
	}
	slices.Sort(result)
	return result
}

type userShardingStrategy struct {
	users []string
}

func (u *userShardingStrategy) FilterUsers(context.Context, []string) ([]string, error) {
	return u.users, nil
}

func (u *userShardingStrategy) FilterBlocks(_ context.Context, userID string, metas map[ulid.ULID]*block.Meta, _ map[ulid.ULID]struct{}, _ block.GaugeVec) error {
	if util.StringsContain(u.users, userID) {
		return nil
	}

	for k := range metas {
		delete(metas, k)
	}
	return nil
}

// failFirstGetBucket is an objstore.Bucket wrapper which fails the first Get() request with a mocked error.
type failFirstGetBucket struct {
	objstore.Bucket

	firstGet atomic.Bool
}

func (f *failFirstGetBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if f.firstGet.CompareAndSwap(false, true) {
		return nil, errors.New("Get() request mocked error")
	}

	return f.Bucket.Get(ctx, name)
}

func BenchmarkBucketStoreLabelValues(tb *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := tb.TempDir()

	bkt, err := filesystemstore.NewBucket(filepath.Join(dir, "bkt"))
	assert.NoError(tb, err)
	defer func() { assert.NoError(tb, bkt.Close()) }()

	series := generateSeries([]int{1, 10, 100, 1000})
	highCardinalitySeries := prefixLabels("high_cardinality_", generateSeries([]int{1, 1_000_000}))
	series = append(series, highCardinalitySeries...)
	tb.Logf("Total %d series generated", len(series))

	prepareCfg := defaultPrepareStoreConfig(tb)
	prepareCfg.tempDir = dir
	prepareCfg.series = series
	prepareCfg.postingsStrategy = worstCaseFetchedDataStrategy{1.0}

	s := prepareStoreWithTestBlocks(tb, bkt, prepareCfg)
	mint, maxt := s.store.TimeRange()
	assert.Equal(tb, s.minTime, mint)
	assert.Equal(tb, s.maxTime, maxt)

	indexCache, err := indexcache.NewInMemoryIndexCacheWithConfig(s.logger, nil, indexcache.InMemoryIndexCacheConfig{
		MaxItemSize: 1e5,
		MaxSize:     2e5,
	})
	assert.NoError(tb, err)

	benchmarks := func(tb *testing.B) {
		tb.Run("10-series-matched-with-10-label-values", func(tb *testing.B) {
			ms, err := storepb.PromMatchersToMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "label_2", "0"),
				labels.MustNewMatcher(labels.MatchEqual, "label_3", "0"),
			)
			require.NoError(tb, err)

			req := &storepb.LabelValuesRequest{
				Label:    "label_1",
				Start:    timestamp.FromTime(minTime),
				End:      timestamp.FromTime(maxTime),
				Matchers: ms,
			}
			// warmup cache if any
			resp, err := s.store.LabelValues(ctx, req)
			require.NoError(tb, err)
			assert.Equal(tb, 10, len(resp.Values))

			tb.ResetTimer()
			for i := 0; i < tb.N; i++ {
				resp, err := s.store.LabelValues(ctx, req)
				require.NoError(tb, err)
				assert.Equal(tb, 10, len(resp.Values))
			}
		})

		tb.Run("1000-series-matched-with-1000-label-values", func(tb *testing.B) {
			ms, err := storepb.PromMatchersToMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "label_1", "0"),
				labels.MustNewMatcher(labels.MatchEqual, "label_2", "0"),
			)
			require.NoError(tb, err)

			req := &storepb.LabelValuesRequest{
				Label:    "label_3",
				Start:    timestamp.FromTime(minTime),
				End:      timestamp.FromTime(maxTime),
				Matchers: ms,
			}

			// warmup cache if any
			resp, err := s.store.LabelValues(ctx, req)
			require.NoError(tb, err)
			assert.Equal(tb, 1000, len(resp.Values))

			tb.ResetTimer()
			for i := 0; i < tb.N; i++ {
				resp, err := s.store.LabelValues(ctx, req)
				require.NoError(tb, err)
				assert.Equal(tb, 1000, len(resp.Values))
			}
		})

		tb.Run("1_000_000-series-matched-with-10-label-values", func(tb *testing.B) {
			ms, err := storepb.PromMatchersToMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "label_0", "0"), // matches all series
			)
			require.NoError(tb, err)

			req := &storepb.LabelValuesRequest{
				Label:    "label_1",
				Start:    timestamp.FromTime(minTime),
				End:      timestamp.FromTime(maxTime),
				Matchers: ms,
			}
			// warmup cache if any
			resp, err := s.store.LabelValues(ctx, req)
			require.NoError(tb, err)
			assert.Equal(tb, 10, len(resp.Values))

			tb.ResetTimer()
			for i := 0; i < tb.N; i++ {
				resp, err := s.store.LabelValues(ctx, req)
				require.NoError(tb, err)
				assert.Equal(tb, 10, len(resp.Values))
			}
		})

		tb.Run("1_000_000-series-matched-with-1-label-values", func(tb *testing.B) {
			ms, err := storepb.PromMatchersToMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "high_cardinality_label_1", "0"),   // matches a single series
				labels.MustNewMatcher(labels.MatchNotEqual, "high_cardinality_label_0", ""), // matches 1M series
			)
			require.NoError(tb, err)

			req := &storepb.LabelValuesRequest{
				Label:    "high_cardinality_label_0", // there is only 1 value for this label
				Start:    timestamp.FromTime(minTime),
				End:      timestamp.FromTime(maxTime),
				Matchers: ms,
			}
			// warmup cache if any
			resp, err := s.store.LabelValues(ctx, req)
			require.NoError(tb, err)
			assert.Equal(tb, 1, len(resp.Values))

			tb.ResetTimer()
			for i := 0; i < tb.N; i++ {
				resp, err := s.store.LabelValues(ctx, req)
				require.NoError(tb, err)
				assert.Equal(tb, 1, len(resp.Values))
			}
		})
	}

	tb.Run("no cache", func(tb *testing.B) {
		s.cache.SwapIndexCacheWith(noopCache{})
		benchmarks(tb)
	})

	tb.Run("inmemory cache (without label values cache)", func(tb *testing.B) {
		s.cache.SwapIndexCacheWith(indexCacheMissingLabelValues{indexCache})
		benchmarks(tb)
	})
}

func prefixLabels(prefix string, series []labels.Labels) []labels.Labels {
	prefixed := make([]labels.Labels, len(series))
	b := labels.NewScratchBuilder(2)
	for i := range series {
		b.Reset()
		series[i].Range(func(l labels.Label) {
			b.Add(prefix+l.Name, l.Value)
		})
		prefixed[i] = b.Labels()
	}
	return prefixed
}

// indexCacheMissingLabelValues wraps an IndexCache returning a miss on all FetchLabelValues calls,
// making it useful to benchmark the LabelValues calls (it still caches the underlying postings calls)
type indexCacheMissingLabelValues struct {
	indexcache.IndexCache
}

func (indexCacheMissingLabelValues) FetchLabelValues(context.Context, string, ulid.ULID, string, indexcache.LabelMatchersKey) ([]byte, bool) {
	return nil, false
}

// generateSeries generated series with len(card) labels, each one called label_n,
// with 0 <= n < len(card) and cardinality(label_n) = card[n]
func generateSeries(card []int) []labels.Labels {
	totalSeries := 1
	for _, c := range card {
		totalSeries *= c
	}
	series := make([]labels.Labels, 0, totalSeries)
	current := make([]labels.Label, len(card))
	for idx := range current {
		current[idx].Name = "label_" + strconv.Itoa(idx)
	}

	var rec func(idx int)
	rec = func(lvl int) {
		if lvl == len(card) {
			series = append(series, labels.New(current...))
			return
		}

		for i := 0; i < card[lvl]; i++ {
			current[lvl].Value = strconv.Itoa(i)
			rec(lvl + 1)
		}
	}
	rec(0)

	return series
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestTimeoutGate_CancellationRace(t *testing.T) {
	gate := timeoutGate{
		delegate: alwaysSuccessfulAfterDelayGate{time.Second},
		timeout:  time.Nanosecond,
	}

	err := gate.Start(context.Background())
	require.NoError(t, err, "must not return failure if delegated gate returns success even after timeout expires")
}

type alwaysSuccessfulAfterDelayGate struct {
	delay time.Duration
}

func (a alwaysSuccessfulAfterDelayGate) Start(_ context.Context) error {
	<-time.After(a.delay)
	return nil
}

func (a alwaysSuccessfulAfterDelayGate) Done() {}
