// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/gate"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/indexheader"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func TestParquetBucketStore_InitialSync(t *testing.T) {
	userID := "test-user"
	metricName := "test_metric"
	ctx := context.Background()
	storageDir := t.TempDir()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	store := createTestParquetBucketStore(t, userID, bkt)

	generateParquetStorageBlock(t, storageDir, bkt, userID, metricName, 10, 100, 15)
	createBucketIndex(t, bkt, userID)

	require.NoError(t, services.StartAndAwaitRunning(ctx, store))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), store))
	})

	err = store.InitialSync(ctx)
	require.NoError(t, err)

	stats := store.Stats()
	require.Greater(t, stats.BlocksLoadedTotal, 0)
}

func TestParquetBucketStore_Queries(t *testing.T) {
	userID := "test-user"
	metricName := "test_metric"
	ctx := context.Background()
	storageDir := t.TempDir()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)
	store := createTestParquetBucketStore(t, userID, bkt)

	generateParquetStorageBlock(t, storageDir, bkt, userID, metricName, 10, 100, 15)
	createBucketIndex(t, bkt, userID)

	require.NoError(t, services.StartAndAwaitRunning(ctx, store))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), store))
	})

	err = store.SyncBlocks(ctx)
	require.NoError(t, err)

	t.Run("LabelNames", func(t *testing.T) {
		req := &storepb.LabelNamesRequest{
			Start: 10,
			End:   100,
		}

		resp, err := store.LabelNames(ctx, req)
		require.NoError(t, err)
		require.Equal(t, []string{"__name__"}, resp.Names)

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

	t.Run("LabelValues", func(t *testing.T) {
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

	t.Run("Series_NonStreaming", func(t *testing.T) {
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

	t.Run("Series_Streaming", func(t *testing.T) {
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

	t.Run("Series_Sharding", func(t *testing.T) {
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

}

func createTestParquetBucketStore(t *testing.T, userID string, bkt objstore.Bucket) *ParquetBucketStore {
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
		newStaticChunksLimiterFactory(0),
		newStaticSeriesLimiterFactory(0),
		metrics,
		logger,
	)
	require.NoError(t, err)
	return store
}
