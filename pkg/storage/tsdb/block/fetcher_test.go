// SPDX-License-Identifier: AGPL-3.0-only

package block

import (
	"context"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
)

func TestMetaFetcher_Fetch_ShouldReturnDiscoveredBlocksIncludingMarkedForDeletion(t *testing.T) {
	var (
		ctx    = context.Background()
		reg    = prometheus.NewPedanticRegistry()
		logger = log.NewNopLogger()
	)

	// Create a bucket client with global markers.
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)
	bkt = BucketWithGlobalMarkers(bkt)

	f, err := NewMetaFetcher(logger, 10, objstore.BucketWithMetrics("test", bkt, reg), t.TempDir(), reg, nil)
	require.NoError(t, err)

	t.Run("should return no metas and no partials on no block in the storage", func(t *testing.T) {
		actualMetas, actualPartials, actualErr := f.Fetch(ctx)
		require.NoError(t, actualErr)
		require.Empty(t, actualMetas)
		require.Empty(t, actualPartials)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP blocks_meta_sync_failures_total Total blocks metadata synchronization failures
			# TYPE blocks_meta_sync_failures_total counter
			blocks_meta_sync_failures_total 0
			
			# HELP blocks_meta_synced Number of block metadata synced
			# TYPE blocks_meta_synced gauge
			blocks_meta_synced{state="corrupted-meta-json"} 0
			blocks_meta_synced{state="duplicate"} 0
			blocks_meta_synced{state="failed"} 0
			blocks_meta_synced{state="label-excluded"} 0
			blocks_meta_synced{state="loaded"} 0
			blocks_meta_synced{state="marked-for-deletion"} 0
			blocks_meta_synced{state="marked-for-no-compact"} 0
			blocks_meta_synced{state="no-meta-json"} 0
			blocks_meta_synced{state="time-excluded"} 0

			# HELP blocks_meta_syncs_total Total blocks metadata synchronization attempts
			# TYPE blocks_meta_syncs_total counter
			blocks_meta_syncs_total 1
		`), "blocks_meta_syncs_total", "blocks_meta_sync_failures_total", "blocks_meta_synced"))
	})

	// Upload a block.
	block1ID, block1Dir := createTestBlock(t)
	require.NoError(t, Upload(ctx, logger, bkt, block1Dir, nil))

	// Upload a partial block.
	block2ID, block2Dir := createTestBlock(t)
	require.NoError(t, Upload(ctx, logger, bkt, block2Dir, nil))
	require.NoError(t, bkt.Delete(ctx, path.Join(block2ID.String(), MetaFilename)))

	t.Run("should return metas and partials on some blocks in the storage", func(t *testing.T) {
		actualMetas, actualPartials, actualErr := f.Fetch(ctx)
		require.NoError(t, actualErr)
		require.Len(t, actualMetas, 1)
		require.Contains(t, actualMetas, block1ID)
		require.Len(t, actualPartials, 1)
		require.Contains(t, actualPartials, block2ID)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP blocks_meta_sync_failures_total Total blocks metadata synchronization failures
			# TYPE blocks_meta_sync_failures_total counter
			blocks_meta_sync_failures_total 0
			
			# HELP blocks_meta_synced Number of block metadata synced
			# TYPE blocks_meta_synced gauge
			blocks_meta_synced{state="corrupted-meta-json"} 0
			blocks_meta_synced{state="duplicate"} 0
			blocks_meta_synced{state="failed"} 0
			blocks_meta_synced{state="label-excluded"} 0
			blocks_meta_synced{state="loaded"} 1
			blocks_meta_synced{state="marked-for-deletion"} 0
			blocks_meta_synced{state="marked-for-no-compact"} 0
			blocks_meta_synced{state="no-meta-json"} 1
			blocks_meta_synced{state="time-excluded"} 0

			# HELP blocks_meta_syncs_total Total blocks metadata synchronization attempts
			# TYPE blocks_meta_syncs_total counter
			blocks_meta_syncs_total 2
		`), "blocks_meta_syncs_total", "blocks_meta_sync_failures_total", "blocks_meta_synced"))
	})

	// Upload a block and mark it for deletion.
	block3ID, block3Dir := createTestBlock(t)
	require.NoError(t, Upload(ctx, logger, bkt, block3Dir, nil))
	require.NoError(t, MarkForDeletion(ctx, logger, bkt, block3ID, "", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))

	t.Run("should include blocks marked for deletion", func(t *testing.T) {
		actualMetas, actualPartials, actualErr := f.Fetch(ctx)
		require.NoError(t, actualErr)
		require.Len(t, actualMetas, 2)
		require.Contains(t, actualMetas, block1ID)
		require.Contains(t, actualMetas, block3ID)
		require.Len(t, actualPartials, 1)
		require.Contains(t, actualPartials, block2ID)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP blocks_meta_sync_failures_total Total blocks metadata synchronization failures
			# TYPE blocks_meta_sync_failures_total counter
			blocks_meta_sync_failures_total 0
			
			# HELP blocks_meta_synced Number of block metadata synced
			# TYPE blocks_meta_synced gauge
			blocks_meta_synced{state="corrupted-meta-json"} 0
			blocks_meta_synced{state="duplicate"} 0
			blocks_meta_synced{state="failed"} 0
			blocks_meta_synced{state="label-excluded"} 0
			blocks_meta_synced{state="loaded"} 2
			blocks_meta_synced{state="marked-for-deletion"} 0
			blocks_meta_synced{state="marked-for-no-compact"} 0
			blocks_meta_synced{state="no-meta-json"} 1
			blocks_meta_synced{state="time-excluded"} 0

			# HELP blocks_meta_syncs_total Total blocks metadata synchronization attempts
			# TYPE blocks_meta_syncs_total counter
			blocks_meta_syncs_total 3
		`), "blocks_meta_syncs_total", "blocks_meta_sync_failures_total", "blocks_meta_synced"))
	})
}

func TestMetaFetcher_FetchWithoutMarkedForDeletion_ShouldReturnDiscoveredBlocksExcludingMarkedForDeletion(t *testing.T) {
	var (
		ctx    = context.Background()
		reg    = prometheus.NewPedanticRegistry()
		logger = log.NewNopLogger()
	)

	// Create a bucket client with global markers.
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)
	bkt = BucketWithGlobalMarkers(bkt)

	f, err := NewMetaFetcher(logger, 10, objstore.BucketWithMetrics("test", bkt, reg), t.TempDir(), reg, nil)
	require.NoError(t, err)

	t.Run("should return no metas and no partials on no block in the storage", func(t *testing.T) {
		actualMetas, actualPartials, actualErr := f.FetchWithoutMarkedForDeletion(ctx)
		require.NoError(t, actualErr)
		require.Empty(t, actualMetas)
		require.Empty(t, actualPartials)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP blocks_meta_sync_failures_total Total blocks metadata synchronization failures
			# TYPE blocks_meta_sync_failures_total counter
			blocks_meta_sync_failures_total 0
			
			# HELP blocks_meta_synced Number of block metadata synced
			# TYPE blocks_meta_synced gauge
			blocks_meta_synced{state="corrupted-meta-json"} 0
			blocks_meta_synced{state="duplicate"} 0
			blocks_meta_synced{state="failed"} 0
			blocks_meta_synced{state="label-excluded"} 0
			blocks_meta_synced{state="loaded"} 0
			blocks_meta_synced{state="marked-for-deletion"} 0
			blocks_meta_synced{state="marked-for-no-compact"} 0
			blocks_meta_synced{state="no-meta-json"} 0
			blocks_meta_synced{state="time-excluded"} 0

			# HELP blocks_meta_syncs_total Total blocks metadata synchronization attempts
			# TYPE blocks_meta_syncs_total counter
			blocks_meta_syncs_total 1
		`), "blocks_meta_syncs_total", "blocks_meta_sync_failures_total", "blocks_meta_synced"))
	})

	// Upload a block.
	block1ID, block1Dir := createTestBlock(t)
	require.NoError(t, Upload(ctx, logger, bkt, block1Dir, nil))

	// Upload a partial block.
	block2ID, block2Dir := createTestBlock(t)
	require.NoError(t, Upload(ctx, logger, bkt, block2Dir, nil))
	require.NoError(t, bkt.Delete(ctx, path.Join(block2ID.String(), MetaFilename)))

	t.Run("should return metas and partials on some blocks in the storage", func(t *testing.T) {
		actualMetas, actualPartials, actualErr := f.FetchWithoutMarkedForDeletion(ctx)
		require.NoError(t, actualErr)
		require.Len(t, actualMetas, 1)
		require.Contains(t, actualMetas, block1ID)
		require.Len(t, actualPartials, 1)
		require.Contains(t, actualPartials, block2ID)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP blocks_meta_sync_failures_total Total blocks metadata synchronization failures
			# TYPE blocks_meta_sync_failures_total counter
			blocks_meta_sync_failures_total 0
			
			# HELP blocks_meta_synced Number of block metadata synced
			# TYPE blocks_meta_synced gauge
			blocks_meta_synced{state="corrupted-meta-json"} 0
			blocks_meta_synced{state="duplicate"} 0
			blocks_meta_synced{state="failed"} 0
			blocks_meta_synced{state="label-excluded"} 0
			blocks_meta_synced{state="loaded"} 1
			blocks_meta_synced{state="marked-for-deletion"} 0
			blocks_meta_synced{state="marked-for-no-compact"} 0
			blocks_meta_synced{state="no-meta-json"} 1
			blocks_meta_synced{state="time-excluded"} 0

			# HELP blocks_meta_syncs_total Total blocks metadata synchronization attempts
			# TYPE blocks_meta_syncs_total counter
			blocks_meta_syncs_total 2
		`), "blocks_meta_syncs_total", "blocks_meta_sync_failures_total", "blocks_meta_synced"))
	})

	// Upload a block and mark it for deletion.
	block3ID, block3Dir := createTestBlock(t)
	require.NoError(t, Upload(ctx, logger, bkt, block3Dir, nil))
	require.NoError(t, MarkForDeletion(ctx, logger, bkt, block3ID, "", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))

	t.Run("should include blocks marked for deletion", func(t *testing.T) {
		actualMetas, actualPartials, actualErr := f.FetchWithoutMarkedForDeletion(ctx)
		require.NoError(t, actualErr)
		require.Len(t, actualMetas, 1)
		require.Contains(t, actualMetas, block1ID)
		require.Len(t, actualPartials, 1)
		require.Contains(t, actualPartials, block2ID)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP blocks_meta_sync_failures_total Total blocks metadata synchronization failures
			# TYPE blocks_meta_sync_failures_total counter
			blocks_meta_sync_failures_total 0
			
			# HELP blocks_meta_synced Number of block metadata synced
			# TYPE blocks_meta_synced gauge
			blocks_meta_synced{state="corrupted-meta-json"} 0
			blocks_meta_synced{state="duplicate"} 0
			blocks_meta_synced{state="failed"} 0
			blocks_meta_synced{state="label-excluded"} 0
			blocks_meta_synced{state="loaded"} 1
			blocks_meta_synced{state="marked-for-deletion"} 1
			blocks_meta_synced{state="marked-for-no-compact"} 0
			blocks_meta_synced{state="no-meta-json"} 1
			blocks_meta_synced{state="time-excluded"} 0

			# HELP blocks_meta_syncs_total Total blocks metadata synchronization attempts
			# TYPE blocks_meta_syncs_total counter
			blocks_meta_syncs_total 3
		`), "blocks_meta_syncs_total", "blocks_meta_sync_failures_total", "blocks_meta_synced"))
	})
}

func TestMetaFetcher_ShouldNotIssueAnyAPICallToObjectStorageIfAllBlockMetasAreCached(t *testing.T) {
	var (
		ctx        = context.Background()
		logger     = log.NewNopLogger()
		fetcherDir = t.TempDir()
	)

	// Create a bucket client.
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	// Upload few blocks.
	block1ID, block1Dir := createTestBlock(t)
	require.NoError(t, Upload(ctx, logger, bkt, block1Dir, nil))
	block2ID, block2Dir := createTestBlock(t)
	require.NoError(t, Upload(ctx, logger, bkt, block2Dir, nil))

	// Create a fetcher and fetch block metas to populate the cache on disk.
	reg1 := prometheus.NewPedanticRegistry()
	fetcher1, err := NewMetaFetcher(logger, 10, objstore.BucketWithMetrics("test", bkt, reg1), fetcherDir, nil, nil)
	require.NoError(t, err)
	actualMetas, _, actualErr := fetcher1.Fetch(ctx)
	require.NoError(t, actualErr)
	require.Len(t, actualMetas, 2)
	require.Contains(t, actualMetas, block1ID)
	require.Contains(t, actualMetas, block2ID)

	assert.NoError(t, testutil.GatherAndCompare(reg1, strings.NewReader(`
		# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
		# TYPE thanos_objstore_bucket_operations_total counter
		thanos_objstore_bucket_operations_total{bucket="test",operation="attributes"} 0
		thanos_objstore_bucket_operations_total{bucket="test",operation="delete"} 0
		thanos_objstore_bucket_operations_total{bucket="test",operation="exists"} 0
		thanos_objstore_bucket_operations_total{bucket="test",operation="get"} 2
		thanos_objstore_bucket_operations_total{bucket="test",operation="get_range"} 0
		thanos_objstore_bucket_operations_total{bucket="test",operation="iter"} 1
		thanos_objstore_bucket_operations_total{bucket="test",operation="upload"} 0
	`), "thanos_objstore_bucket_operations_total"))

	// Create a new fetcher and fetch blocks again. This time we expect all meta.json to be loaded from cache.
	reg2 := prometheus.NewPedanticRegistry()
	fetcher2, err := NewMetaFetcher(logger, 10, objstore.BucketWithMetrics("test", bkt, reg2), fetcherDir, nil, nil)
	require.NoError(t, err)
	actualMetas, _, actualErr = fetcher2.Fetch(ctx)
	require.NoError(t, actualErr)
	require.Len(t, actualMetas, 2)
	require.Contains(t, actualMetas, block1ID)
	require.Contains(t, actualMetas, block2ID)

	assert.NoError(t, testutil.GatherAndCompare(reg2, strings.NewReader(`
		# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
		# TYPE thanos_objstore_bucket_operations_total counter
		thanos_objstore_bucket_operations_total{bucket="test",operation="attributes"} 0
		thanos_objstore_bucket_operations_total{bucket="test",operation="delete"} 0
		thanos_objstore_bucket_operations_total{bucket="test",operation="exists"} 0
		thanos_objstore_bucket_operations_total{bucket="test",operation="get"} 0
		thanos_objstore_bucket_operations_total{bucket="test",operation="get_range"} 0
		thanos_objstore_bucket_operations_total{bucket="test",operation="iter"} 1
		thanos_objstore_bucket_operations_total{bucket="test",operation="upload"} 0
	`), "thanos_objstore_bucket_operations_total"))
}

func createTestBlock(t *testing.T) (blockID ulid.ULID, blockDir string) {
	var err error

	parentDir := t.TempDir()
	series := []labels.Labels{
		labels.FromStrings(labels.MetricName, "series_1"),
		labels.FromStrings(labels.MetricName, "series_2"),
		labels.FromStrings(labels.MetricName, "series_3"),
	}

	blockID, err = CreateBlock(context.Background(), parentDir, series, 100, 0, 1000, labels.EmptyLabels())
	require.NoError(t, err)

	blockDir = filepath.Join(parentDir, blockID.String())
	return
}
