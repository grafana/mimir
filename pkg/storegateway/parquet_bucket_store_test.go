// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/gate"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/indexheader"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
)

func TestParquetBucketStore_InitialSync(t *testing.T) {
	userID := "test-user"
	metricName := "test_metric"
	ctx := context.Background()
	storageDir := t.TempDir()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	generateParquetStorageBlock(t, storageDir, bkt, userID, metricName, 1, 10, 100, 15)
	createBucketIndex(t, bkt, userID)

	loadIndexToDisk := []bool{true, false}
	for _, loadIndex := range loadIndexToDisk {

		t.Run("InitialSync"+fmt.Sprintf("/IndexToDisk=%t", loadIndexToDisk), func(t *testing.T) {
			store := createTestParquetBucketStore(t, userID, bkt, loadIndex)

			require.NoError(t, services.StartAndAwaitRunning(ctx, store))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), store))
			})

			err = store.InitialSync(ctx)
			require.NoError(t, err)

			stats := store.Stats()
			require.Greater(t, stats.BlocksLoadedTotal, 0)
		})
	}

}

func TestParquetBucketStore_Queries(t *testing.T) {
	userID := "test-user"
	metricName := "test_metric"
	ctx := context.Background()
	storageDir := t.TempDir()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	generateParquetStorageBlock(t, storageDir, bkt, userID, metricName, 1, 10, 100, 15)
	createBucketIndex(t, bkt, userID)

	loadIndexToDisk := []bool{true, false}
	for _, loadIndex := range loadIndexToDisk {
		store := createTestParquetBucketStore(t, userID, bkt, loadIndex)
		require.NoError(t, services.StartAndAwaitRunning(ctx, store))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), store))
		})

		err = store.SyncBlocks(ctx)
		require.NoError(t, err)

		t.Run("LabelNames"+fmt.Sprintf("/IndexToDisk=%t", loadIndexToDisk), func(t *testing.T) {
			req := &storepb.LabelNamesRequest{
				Start: 10,
				End:   100,
			}

			resp, err := store.LabelNames(ctx, req)
			require.NoError(t, err)
			require.Equal(t, []string{"__name__", "series_id"}, resp.Names)

			req = &storepb.LabelNamesRequest{
				Start: 10,
				End:   100,
				Matchers: []storepb.LabelMatcher{
					{
						Name:  "foo",
						Value: "bar",
						Type:  storepb.LabelMatcher_EQ,
					},
				},
			}

			resp, err = store.LabelNames(ctx, req)
			require.NoError(t, err)
			require.Empty(t, resp.Names)
		})

		t.Run("LabelValues"+fmt.Sprintf("/IndexToDisk=%t", loadIndexToDisk), func(t *testing.T) {
			req := &storepb.LabelValuesRequest{
				Label: labels.MetricName,
				Start: 10,
				End:   100,
			}

			resp, err := store.LabelValues(ctx, req)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Contains(t, resp.Values, metricName)
		})

		t.Run("Series_NonStreaming"+fmt.Sprintf("/IndexToDisk=%t", loadIndexToDisk), func(t *testing.T) {
			req := &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: 100,
				Matchers: []storepb.LabelMatcher{
					{
						Type:  storepb.LabelMatcher_EQ,
						Name:  labels.MetricName,
						Value: metricName,
					},
				},
			}
			seriesSet, warnings, err := grpcSeries(t, context.Background(), store, req)
			require.NoError(t, err)
			require.Empty(t, warnings)
			require.Len(t, seriesSet, 1)
		})

		t.Run("Series_Streaming"+fmt.Sprintf("/IndexToDisk=%t", loadIndexToDisk), func(t *testing.T) {
			req := &storepb.SeriesRequest{
				MinTime:                  0,
				MaxTime:                  100,
				StreamingChunksBatchSize: 10,
				Matchers: []storepb.LabelMatcher{
					{
						Type:  storepb.LabelMatcher_EQ,
						Name:  labels.MetricName,
						Value: metricName,
					},
				},
			}
			seriesSet, warnings, err := grpcSeries(t, context.Background(), store, req)
			require.NoError(t, err)
			require.Empty(t, warnings)
			require.Len(t, seriesSet, 1)
		})

		t.Run("Series_Sharding"+fmt.Sprintf("/IndexToDisk=%t", loadIndexToDisk), func(t *testing.T) {
			const shardCount = 3

			seriesResultCount := 0
			for shardID := range shardCount {
				shardLabelValue := sharding.FormatShardIDLabelValue(uint64(shardID), shardCount)
				req := &storepb.SeriesRequest{
					MinTime:                  0,
					MaxTime:                  100,
					StreamingChunksBatchSize: 10,
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  labels.MetricName,
							Value: metricName,
						},
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  sharding.ShardLabel,
							Value: shardLabelValue,
						},
					},
				}
				seriesSet, warnings, err := grpcSeries(t, context.Background(), store, req)
				require.NoError(t, err)
				require.Empty(t, warnings)
				require.LessOrEqual(t, len(seriesSet), 1, "Expected at most one series result per shard")
				seriesResultCount += len(seriesSet)
			}
			require.Equal(t, seriesResultCount, 1, "Expected only one series result across all shards")
		})

		t.Run("Series_SkipChunks"+fmt.Sprintf("/IndexToDisk=%t", loadIndexToDisk), func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				batch uint64
			}{
				{name: "NonStreaming", batch: 0},
				{name: "Streaming", batch: 10},
			} {
				t.Run(tc.name, func(t *testing.T) {
					req := &storepb.SeriesRequest{
						MinTime:                  0,
						MaxTime:                  100,
						SkipChunks:               true,
						StreamingChunksBatchSize: tc.batch,
						Matchers: []storepb.LabelMatcher{
							{
								Type:  storepb.LabelMatcher_EQ,
								Name:  labels.MetricName,
								Value: metricName,
							},
						},
					}
					seriesSet, warnings, err := grpcSeries(t, context.Background(), store, req)
					require.NoError(t, err)
					require.Empty(t, warnings)
					require.Len(t, seriesSet, 1)
					series := seriesSet[0]
					require.NotEmpty(t, series.Labels)
					require.Empty(t, series.Chunks)
					require.Equal(t, metricName, series.Labels[0].Value)
				})
			}
		})
	}

}

func createTestParquetBucketStore(
	t *testing.T,
	userID string,
	bkt objstore.Bucket,
	loadIndexToDisk bool,
) *ParquetBucketStore {
	localDir := t.TempDir()
	cfg := mimir_tsdb.BucketStoreConfig{
		StreamingBatchSize:   1000,
		BlockSyncConcurrency: 10,
		IndexHeader: indexheader.Config{
			LazyLoadingEnabled:     true,
			LazyLoadingIdleTimeout: time.Second,
		},
	}

	logger := log.NewLogfmtLogger(os.Stdout)
	metrics := NewParquetBucketStoreMetrics(prometheus.NewRegistry())
	userBkt := bucket.NewUserBucketClient(userID, objstore.WithNoopInstr(bkt), nil)

	fetcher := NewBucketIndexMetadataFetcher(
		userID,
		objstore.WithNoopInstr(bkt),
		nil,
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		nil,
	)

	store, err := NewParquetBucketStore(
		userID,
		localDir,
		userBkt,
		cfg,
		fetcher,
		gate.NewNoop(),
		gate.NewNoop(),
		loadIndexToDisk,
		true,
		nil,
		newStaticChunksLimiterFactory(0),
		newStaticSeriesLimiterFactory(0),
		metrics,
		logger,
	)
	require.NoError(t, err)
	return store
}
func TestParquetBucketStores_RowLimits(t *testing.T) {
	userID := "test-user"
	metricName := "test_metric"
	ctx := context.Background()
	storageDir := t.TempDir()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	generateParquetStorageBlock(t, storageDir, bkt, userID, metricName, 3, 10, 100, 15)
	createBucketIndex(t, bkt, userID)

	t.Run("RowCountLimitEnforcesLimit", func(t *testing.T) {
		cfg := prepareParquetStorageConfig(t)
		cfg.BucketStore.ParquetMaxRowCount = 1

		var allowedTenants *util.AllowList
		reg := prometheus.NewPedanticRegistry()
		stores, err := NewParquetBucketStores(cfg, defaultLimitsOverrides(t), allowedTenants, newNoShardingStrategy(), bkt, log.NewLogfmtLogger(os.Stdout), reg)
		require.NoError(t, err)

		require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
		})

		// Query should fail due to row limit (we have 3 series but limit is 1)
		seriesSet, warnings, err := queryParquetSeries(t, stores, userID, metricName, 10, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "would fetch too many rows")
		require.Empty(t, warnings)
		require.Empty(t, seriesSet)
	})

	t.Run("RowCountLimitAllowsNormalOperation", func(t *testing.T) {
		cfg := prepareParquetStorageConfig(t)
		cfg.BucketStore.ParquetMaxRowCount = 1000

		var allowedTenants *util.AllowList
		reg := prometheus.NewPedanticRegistry()
		stores, err := NewParquetBucketStores(cfg, defaultLimitsOverrides(t), allowedTenants, newNoShardingStrategy(), bkt, log.NewLogfmtLogger(os.Stdout), reg)
		require.NoError(t, err)

		require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
		})

		// Query should succeed
		seriesSet, warnings, err := queryParquetSeries(t, stores, userID, metricName, 10, 100)
		require.NoError(t, err)
		require.Empty(t, warnings)
		require.GreaterOrEqual(t, len(seriesSet), 0)
	})

	t.Run("RowCountLimitDisabledWhenZero", func(t *testing.T) {
		cfg := prepareParquetStorageConfig(t)
		// cfg.BucketStore.ParquetMaxRowCount = 0  already the default

		var allowedTenants *util.AllowList
		reg := prometheus.NewPedanticRegistry()
		stores, err := NewParquetBucketStores(cfg, defaultLimitsOverrides(t), allowedTenants, newNoShardingStrategy(), bkt, log.NewLogfmtLogger(os.Stdout), reg)
		require.NoError(t, err)

		require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
		})

		seriesSet, warnings, err := queryParquetSeries(t, stores, userID, metricName, 10, 100)
		require.NoError(t, err)
		require.Empty(t, warnings)
		require.GreaterOrEqual(t, len(seriesSet), 0)
	})
}

func generateStorageBlockWithMultipleSeries(t *testing.T, storageDir, userID string, metricName string, numSeries int, minT, maxT int64, step int) {
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

	app := db.Appender(context.Background())

	// Generate multiple series with the specified numSeries
	for seriesIdx := 0; seriesIdx < numSeries; seriesIdx++ {
		series := labels.FromStrings(labels.MetricName, metricName, "series_id", fmt.Sprintf("%d", seriesIdx))

		for ts := minT; ts < maxT; ts += int64(step) {
			_, err = app.Append(0, series, ts, 1)
			require.NoError(t, err)
		}
	}
	require.NoError(t, app.Commit())

	// Snapshot TSDB to the storage directory.
	require.NoError(t, db.Snapshot(userDir, true))
}
