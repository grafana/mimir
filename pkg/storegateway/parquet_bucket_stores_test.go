// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/runutil"
	"github.com/grafana/dskit/services"
<<<<<<< HEAD
	"github.com/oklog/ulid/v2"
=======
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/mimirpb"
<<<<<<< HEAD
	"github.com/grafana/mimir/pkg/parquetconverter"
=======
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
)

func TestParquetBucketStores_InitialSync(t *testing.T) {
	userToMetric := map[string]string{
		"user-1": "series_1",
		"user-2": "series_2",
	}

	ctx := context.Background()
	cfg := prepareParquetStorageConfig(t)

	storageDir := t.TempDir()
	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	for userID, metricName := range userToMetric {
		generateParquetStorageBlock(t, storageDir, bucket, userID, metricName, 1, 10, 100, 15)
	}

	var allowedTenants *util.AllowList
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewParquetBucketStores(cfg, defaultLimitsOverrides(t), allowedTenants, newNoShardingStrategy(), bucket, log.NewLogfmtLogger(os.Stdout), reg)
	require.NoError(t, err)

	// Query series before the initial sync.
	for userID, metricName := range userToMetric {
		seriesSet, warnings, err := queryParquetSeries(t, stores, userID, metricName, 20, 40)
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
		seriesSet, warnings, err := queryParquetSeries(t, stores, userID, metricName, 20, 40)
		require.NoError(t, err)
		assert.Empty(t, warnings)
		require.Len(t, seriesSet, 1)
<<<<<<< HEAD
		expectedLabels := []mimirpb.LabelAdapter{
			{Name: labels.MetricName, Value: metricName},
			{Name: "series_id", Value: "0"},
		}
		assert.Equal(t, expectedLabels, seriesSet[0].Labels)
=======
		assert.Equal(t, []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: metricName}}, seriesSet[0].Labels)
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	}

	// Query series of another user.
	seriesSet, warnings, err := queryParquetSeries(t, stores, "user-1", "series_2", 20, 40)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Empty(t, seriesSet)

	// TODO: assert metrics
}

func TestParquetBucketStores_SyncBlocks(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "series_1"
	)

	ctx := context.Background()
	cfg := prepareParquetStorageConfig(t)

	storageDir := t.TempDir()

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	var allowedTenants *util.AllowList
	reg := prometheus.NewPedanticRegistry()
	stores, err := NewParquetBucketStores(cfg, defaultLimitsOverrides(t), allowedTenants, newNoShardingStrategy(), bucket, log.NewLogfmtLogger(os.Stdout), reg)
	require.NoError(t, err)

	// Run an initial sync to discover 1 block.
	generateParquetStorageBlock(t, storageDir, bucket, userID, metricName, 1, 10, 100, 15)
	createBucketIndex(t, bucket, userID)
	require.NoError(t, services.StartAndAwaitRunning(ctx, stores))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), stores))
	})

	// Query a range for which we have no samples.
	seriesSet, warnings, err := queryParquetSeries(t, stores, userID, metricName, 150, 180)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Empty(t, seriesSet)

	// Generate another block and sync blocks again.
	generateParquetStorageBlock(t, storageDir, bucket, userID, metricName, 1, 100, 200, 15)
	createBucketIndex(t, bucket, userID)
	require.NoError(t, stores.SyncBlocks(ctx))

	seriesSet, warnings, err = queryParquetSeries(t, stores, userID, metricName, 150, 180)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Len(t, seriesSet, 1)
<<<<<<< HEAD

	expectedLabels := []mimirpb.LabelAdapter{
		{Name: labels.MetricName, Value: metricName},
		{Name: "series_id", Value: "0"},
	}
	assert.Equal(t, expectedLabels, seriesSet[0].Labels)
=======
	assert.Equal(t, []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: metricName}}, seriesSet[0].Labels)
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))

	// TODO: assert metrics
}

func prepareParquetStorageConfig(t *testing.T) mimir_tsdb.BlocksStorageConfig {
	cfg := mimir_tsdb.BlocksStorageConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BucketStore.SyncDir = t.TempDir()
	return cfg
}

func generateParquetStorageBlock(t *testing.T, storageDir string, bkt objstore.Bucket, userID string, metricName string, numSeries int, minT, maxT int64, step int) {
	generateStorageBlockWithMultipleSeries(t, storageDir, userID, metricName, numSeries, minT, maxT, step)
	userDir := filepath.Join(storageDir, userID)
	bkt = bucket.NewPrefixedBucketClient(bkt, userID)
	entries, err := os.ReadDir(userDir)
	require.NoError(t, err)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		blockId := e.Name()
		path := filepath.Join(userDir, e.Name())
		tsdbBlock, err := tsdb.OpenBlock(nil, path, nil, tsdb.DefaultPostingsDecoderFactory)
		require.NoError(t, err)
		defer runutil.CloseWithErrCapture(&err, tsdbBlock, "close tsdb block")

		_, err = convert.ConvertTSDBBlock(
			context.Background(),
			bkt,
			minT,
			maxT,
			[]convert.Convertible{tsdbBlock},
			convert.WithName(blockId),
		)
		require.NoError(t, err)
<<<<<<< HEAD

		blockULID, err := ulid.Parse(blockId)
		require.NoError(t, err)
		instrumentedBkt := objstore.WithNoopInstr(bkt)
		err = parquetconverter.WriteConversionMark(context.Background(), blockULID, instrumentedBkt)
		require.NoError(t, err)
=======
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	}

}

func queryParquetSeries(t *testing.T, store storegatewaypb.StoreGatewayServer, userID, metricName string, minT, maxT int64) ([]*storepb.Series, annotations.Annotations, error) {
	req := &storepb.SeriesRequest{
		MinTime:                  minT,
		MaxTime:                  maxT,
		StreamingChunksBatchSize: 10,
	}

	if metricName != "" {
		req.Matchers = append(req.Matchers, storepb.LabelMatcher{
			Type:  storepb.LabelMatcher_EQ,
			Name:  "__name__",
			Value: metricName,
		})
	}
	return grpcSeries(t, setUserIDToGRPCContext(context.Background(), userID), store, req)
}

func grpcSeries(t *testing.T, ctx context.Context, store storegatewaypb.StoreGatewayServer, req *storepb.SeriesRequest) ([]*storepb.Series, annotations.Annotations, error) {
	srv := newStoreGatewayTestServer(t, store)
	seriesSet, warnings, _, _, err := srv.Series(ctx, req)
	return seriesSet, warnings, err
}
