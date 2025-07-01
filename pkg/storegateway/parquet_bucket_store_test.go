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
	"github.com/grafana/mimir/pkg/storage/tsdb"
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

}

func createTestParquetBucketStore(t *testing.T, userID string, bkt objstore.Bucket) *ParquetBucketStore {
	localDir := t.TempDir()
	cfg := tsdb.BucketStoreConfig{
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
