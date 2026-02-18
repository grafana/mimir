// SPDX-License-Identifier: AGPL-3.0-only

package block

import (
	"context"
	crypto_rand "crypto/rand"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
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

	f, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, reg, "test"), t.TempDir(), reg, nil, 0)
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
			blocks_meta_synced{state="lookback-excluded"} 0
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
	block1Meta, block1Dir := createTestBlock(t)
	_, err = Upload(ctx, logger, bkt, block1Dir, nil)
	require.NoError(t, err)

	// Upload a partial block.
	block2Meta, block2Dir := createTestBlock(t)
	_, err = Upload(ctx, logger, bkt, block2Dir, nil)
	require.NoError(t, err)
	require.NoError(t, bkt.Delete(ctx, path.Join(block2Meta.ULID.String(), MetaFilename)))

	t.Run("should return metas and partials on some blocks in the storage", func(t *testing.T) {
		actualMetas, actualPartials, actualErr := f.Fetch(ctx)
		require.NoError(t, actualErr)
		require.Len(t, actualMetas, 1)
		require.Contains(t, actualMetas, block1Meta.ULID)
		require.Len(t, actualPartials, 1)
		require.Contains(t, actualPartials, block2Meta.ULID)

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
			blocks_meta_synced{state="lookback-excluded"} 0
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
	block3Meta, block3Dir := createTestBlock(t)
	_, err = Upload(ctx, logger, bkt, block3Dir, nil)
	require.NoError(t, err)
	require.NoError(t, MarkForDeletion(ctx, logger, bkt, block3Meta.ULID, "", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))

	t.Run("should include blocks marked for deletion", func(t *testing.T) {
		actualMetas, actualPartials, actualErr := f.Fetch(ctx)
		require.NoError(t, actualErr)
		require.Len(t, actualMetas, 2)
		require.Contains(t, actualMetas, block1Meta.ULID)
		require.Contains(t, actualMetas, block3Meta.ULID)
		require.Len(t, actualPartials, 1)
		require.Contains(t, actualPartials, block2Meta.ULID)

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
			blocks_meta_synced{state="lookback-excluded"} 0
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

	f, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, reg, "test"), t.TempDir(), reg, nil, 0)
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
			blocks_meta_synced{state="lookback-excluded"} 0
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
	block1Meta, block1Dir := createTestBlock(t)
	_, err = Upload(ctx, logger, bkt, block1Dir, nil)
	require.NoError(t, err)

	// Upload a partial block.
	block2Meta, block2Dir := createTestBlock(t)
	_, err = Upload(ctx, logger, bkt, block2Dir, nil)
	require.NoError(t, err)
	require.NoError(t, bkt.Delete(ctx, path.Join(block2Meta.ULID.String(), MetaFilename)))

	t.Run("should return metas and partials on some blocks in the storage", func(t *testing.T) {
		actualMetas, actualPartials, actualErr := f.FetchWithoutMarkedForDeletion(ctx)
		require.NoError(t, actualErr)
		require.Len(t, actualMetas, 1)
		require.Contains(t, actualMetas, block1Meta.ULID)
		require.Len(t, actualPartials, 1)
		require.Contains(t, actualPartials, block2Meta.ULID)

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
			blocks_meta_synced{state="lookback-excluded"} 0
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
	block3Meta, block3Dir := createTestBlock(t)
	_, err = Upload(ctx, logger, bkt, block3Dir, nil)
	require.NoError(t, err)
	require.NoError(t, MarkForDeletion(ctx, logger, bkt, block3Meta.ULID, "", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))

	t.Run("should include blocks marked for deletion", func(t *testing.T) {
		actualMetas, actualPartials, actualErr := f.FetchWithoutMarkedForDeletion(ctx)
		require.NoError(t, actualErr)
		require.Len(t, actualMetas, 1)
		require.Contains(t, actualMetas, block1Meta.ULID)
		require.Len(t, actualPartials, 1)
		require.Contains(t, actualPartials, block2Meta.ULID)

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
			blocks_meta_synced{state="lookback-excluded"} 0
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
	block1Meta, block1Dir := createTestBlock(t)
	_, err = Upload(ctx, logger, bkt, block1Dir, nil)
	require.NoError(t, err)
	block2Meta, block2Dir := createTestBlock(t)
	_, err = Upload(ctx, logger, bkt, block2Dir, nil)
	require.NoError(t, err)

	// Create a fetcher and fetch block metas to populate the cache on disk.
	reg1 := prometheus.NewPedanticRegistry()
	fetcher1, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, prometheus.WrapRegistererWithPrefix("thanos_", reg1), "test"), fetcherDir, nil, nil, 0)
	require.NoError(t, err)
	actualMetas, _, actualErr := fetcher1.Fetch(ctx)
	require.NoError(t, actualErr)
	require.Len(t, actualMetas, 2)
	require.Contains(t, actualMetas, block1Meta.ULID)
	require.Contains(t, actualMetas, block2Meta.ULID)

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
	fetcher2, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, prometheus.WrapRegistererWithPrefix("thanos_", reg2), "test"), fetcherDir, nil, nil, 0)
	require.NoError(t, err)
	actualMetas, _, actualErr = fetcher2.Fetch(ctx)
	require.NoError(t, actualErr)
	require.Len(t, actualMetas, 2)
	require.Contains(t, actualMetas, block1Meta.ULID)
	require.Contains(t, actualMetas, block2Meta.ULID)

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

func TestMetaFetcher_Fetch_ShouldReturnDiscoveredBlocksWithinCompactorLookback(t *testing.T) {

	var (
		ctx        = context.Background()
		logger     = log.NewNopLogger()
		fetcherDir = t.TempDir()
	)

	// Create a bucket client.
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	// Upload recent blocks.
	block1Meta, block1Dir := createTestBlock(t)
	_, err = Upload(ctx, logger, bkt, block1Dir, nil)
	require.NoError(t, err)

	block2Meta, block2Dir := createTestBlock(t)
	_, err = Upload(ctx, logger, bkt, block2Dir, nil)
	require.NoError(t, err)

	block3Meta, block3Dir := createTestBlock(t)
	_, err = Upload(ctx, logger, bkt, block3Dir, nil)
	require.NoError(t, err)
	require.NoError(t, bkt.Delete(ctx, path.Join(block3Meta.ULID.String(), MetaFilename)))

	// Simulate a block uploaded before than MetaFetchers' maximum lookback period by creating a new block,
	// renaming the blockDir to an older ULID, and uploading meta.json w. the older ULID.
	now := time.Now()
	twoWeekOldBlockID := generateBlockUploadedAtTimestamp(t, bkt, logger, now.Add(-336*time.Hour))

	t.Run("should return all block metas when max lookback is unset", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		f, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, reg, "test"), fetcherDir, reg, nil, time.Duration(0))
		require.NoError(t, err)

		actualMetas, _, actualErr := f.Fetch(ctx)
		require.NoError(t, actualErr)
		require.Len(t, actualMetas, 3)
		require.Contains(t, actualMetas, block1Meta.ULID)
		require.Contains(t, actualMetas, block2Meta.ULID)
		require.Contains(t, actualMetas, twoWeekOldBlockID)

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
			blocks_meta_synced{state="loaded"} 3
			blocks_meta_synced{state="lookback-excluded"} 0
			blocks_meta_synced{state="marked-for-deletion"} 0
			blocks_meta_synced{state="marked-for-no-compact"} 0
			blocks_meta_synced{state="no-meta-json"} 1
			blocks_meta_synced{state="time-excluded"} 0

			# HELP blocks_meta_syncs_total Total blocks metadata synchronization attempts
			# TYPE blocks_meta_syncs_total counter
			blocks_meta_syncs_total 1
		`), "blocks_meta_syncs_total", "blocks_meta_sync_failures_total", "blocks_meta_synced"))
	})

	t.Run("should not return block metas exceeding lookback threshold", func(t *testing.T) {

		maxLookbackThreshold, err := time.ParseDuration("168h")
		require.NoError(t, err)

		reg := prometheus.NewPedanticRegistry()
		f, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, reg, "test"), fetcherDir, reg, nil, maxLookbackThreshold)
		require.NoError(t, err)

		actualMetas, actualPartials, actualErr := f.Fetch(ctx)
		require.NoError(t, actualErr)
		require.Len(t, actualMetas, 2)
		require.Contains(t, actualMetas, block1Meta.ULID)
		require.Contains(t, actualMetas, block2Meta.ULID)
		require.Len(t, actualPartials, 1)
		require.Contains(t, actualPartials, block3Meta.ULID)

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
			blocks_meta_synced{state="lookback-excluded"} 1
			blocks_meta_synced{state="marked-for-deletion"} 0
			blocks_meta_synced{state="marked-for-no-compact"} 0
			blocks_meta_synced{state="no-meta-json"} 1
			blocks_meta_synced{state="time-excluded"} 0

			# HELP blocks_meta_syncs_total Total blocks metadata synchronization attempts
			# TYPE blocks_meta_syncs_total counter
			blocks_meta_syncs_total 1
		`), "blocks_meta_syncs_total", "blocks_meta_sync_failures_total", "blocks_meta_synced"))
	})

	t.Run("should not return block metas just exceeding lookback threshold", func(t *testing.T) {
		// the timestamp embedded in ULID is precise to 1ms. We verify that truncating time to 6B won't cause
		// fetcher to accept blocks slightly older (~1s) than the lookback
		maxLookbackThreshold, err := time.ParseDuration("15s")
		require.NoError(t, err)

		reg := prometheus.NewPedanticRegistry()
		f, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, reg, "test"), fetcherDir, reg, nil, maxLookbackThreshold)
		require.NoError(t, err)

		now := time.Now()
		block1ID := generateBlockUploadedAtTimestamp(t, bkt, logger, now.Add(-15*time.Second))

		actualMetas, _, actualErr := f.Fetch(ctx)
		require.NoError(t, actualErr)
		require.NotContains(t, actualMetas, block1ID)
	})

	t.Run("should return all block metas when fetcher lookback is set long", func(t *testing.T) {

		maxLookbackThreshold, err := time.ParseDuration("1000h")
		require.NoError(t, err)
		reg := prometheus.NewPedanticRegistry()
		f, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, reg, "test"), fetcherDir, reg, nil, maxLookbackThreshold)
		require.NoError(t, err)

		actualMetas, _, actualErr := f.Fetch(ctx)
		require.NoError(t, actualErr)
		require.Contains(t, actualMetas, block1Meta.ULID)
		require.Contains(t, actualMetas, block2Meta.ULID)
		require.Contains(t, actualMetas, twoWeekOldBlockID)
	})

	t.Run("should return no block metas when fetcher lookback is set short", func(t *testing.T) {

		maxLookbackThreshold, err := time.ParseDuration("1ms")
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond) // sleep until uploaded blocks exceed block age threshold
		reg := prometheus.NewPedanticRegistry()
		f, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, reg, "test"), fetcherDir, reg, nil, maxLookbackThreshold)
		require.NoError(t, err)

		actualMetas, _, actualErr := f.Fetch(ctx)
		require.NoError(t, actualErr)
		require.Len(t, actualMetas, 0)
	})

	t.Run("should return block metas with ulid greater than current time ulid", func(t *testing.T) {
		maxLookbackThreshold, err := time.ParseDuration("0ms")
		require.NoError(t, err)

		reg := prometheus.NewPedanticRegistry()
		f, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, reg, "test"), fetcherDir, reg, nil, maxLookbackThreshold)
		require.NoError(t, err)

		now := time.Now()
		futureID := generateBlockUploadedAtTimestamp(t, bkt, logger, now.Add(30*time.Second))

		actualMetas, _, actualErr := f.Fetch(ctx)
		require.NoError(t, actualErr)
		require.Contains(t, actualMetas, futureID)
	})

}

func createTestBlock(t *testing.T) (meta *Meta, blockDir string) {
	var err error

	parentDir := t.TempDir()
	series := []labels.Labels{
		labels.FromStrings(model.MetricNameLabel, "series_1"),
		labels.FromStrings(model.MetricNameLabel, "series_2"),
		labels.FromStrings(model.MetricNameLabel, "series_3"),
	}

	meta, err = CreateBlock(context.Background(), parentDir, series, 100, 0, 1000, labels.EmptyLabels())
	require.NoError(t, err)

	blockDir = filepath.Join(parentDir, meta.ULID.String())
	return
}

func generateULIDs(count int) []ulid.ULID {
	result := make([]ulid.ULID, 0, count)
	for i := 0; i < count; i++ {
		result = append(result, ulid.MustNew(ulid.Now(), crypto_rand.Reader))
	}
	return result
}

func generateBlockUploadedAtTimestamp(t *testing.T, bkt objstore.Bucket, l log.Logger, ts time.Time) ulid.ULID {
	meta, blockDir := createTestBlock(t)
	fixedTimeULID, err := ulid.New(ulid.Timestamp(ts), nil)
	require.NoError(t, err)
	fixedTimeULIDDir := strings.ReplaceAll(blockDir, meta.ULID.String(), fixedTimeULID.String())
	err = os.Rename(blockDir, fixedTimeULIDDir)
	require.NoError(t, err)
	newMeta := &Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:       fixedTimeULID,
			Compaction: tsdb.BlockMetaCompaction{Level: 5, Sources: generateULIDs(10)},
			Version:    1,
		},
	}
	_, err = Upload(context.Background(), l, bkt, fixedTimeULIDDir, newMeta)
	require.NoError(t, err)
	return fixedTimeULID
}

func TestMetaFetcher_CacheMetrics(t *testing.T) {
	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()
	)

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	meta, blockDir := createTestBlock(t)
	_, err = Upload(ctx, logger, bkt, blockDir, nil)
	require.NoError(t, err)

	testCases := map[string]struct {
		setup               func(t *testing.T) (*prometheus.Registry, *MetaFetcher)
		expectedLoads       int
		expectedCachedLoads int
		expectedDiskLoads   int
	}{
		"first fetch should hit object storage": {
			setup: func(t *testing.T) (*prometheus.Registry, *MetaFetcher) {
				reg := prometheus.NewPedanticRegistry()
				fetcher, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, reg, "test"), t.TempDir(), reg, nil, 0)
				require.NoError(t, err)
				return reg, fetcher
			},
			expectedLoads: 1,
		},
		"second fetch with same fetcher should hit in-memory cache": {
			setup: func(t *testing.T) (*prometheus.Registry, *MetaFetcher) {
				reg := prometheus.NewPedanticRegistry()
				fetcher, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, reg, "test"), t.TempDir(), reg, nil, 0)
				require.NoError(t, err)
				_, _, err = fetcher.Fetch(ctx)
				require.NoError(t, err)
				return reg, fetcher
			},
			expectedLoads:       2,
			expectedCachedLoads: 1,
		},
		"new fetcher should hit disk cache": {
			setup: func(t *testing.T) (*prometheus.Registry, *MetaFetcher) {
				fetcherDir := t.TempDir()
				reg1 := prometheus.NewPedanticRegistry()
				fetcher1, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, reg1, "test"), fetcherDir, reg1, nil, 0)
				require.NoError(t, err)
				_, _, err = fetcher1.Fetch(ctx)
				require.NoError(t, err)

				reg2 := prometheus.NewPedanticRegistry()
				fetcher2, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, reg2, "test"), fetcherDir, reg2, nil, 0)
				require.NoError(t, err)
				return reg2, fetcher2
			},
			expectedLoads:     1,
			expectedDiskLoads: 1,
		},
		"no disk cache dir should not increment disk metrics": {
			setup: func(t *testing.T) (*prometheus.Registry, *MetaFetcher) {
				reg := prometheus.NewPedanticRegistry()
				fetcher, err := NewMetaFetcher(logger, 10, objstore.WrapWithMetrics(bkt, reg, "test"), "", reg, nil, 0)
				require.NoError(t, err)
				return reg, fetcher
			},
			expectedLoads: 1,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			reg, fetcher := tc.setup(t)

			metas, _, err := fetcher.Fetch(ctx)
			require.NoError(t, err)
			require.Len(t, metas, 1)
			require.Contains(t, metas, meta.ULID)

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
				# HELP blocks_meta_cached_loads Block metadata loads served from in-memory cache
				# TYPE blocks_meta_cached_loads counter
				blocks_meta_cached_loads `+fmt.Sprintf("%d", tc.expectedCachedLoads)+`

				# HELP blocks_meta_disk_loads Block metadata loads served from local disk
				# TYPE blocks_meta_disk_loads counter
				blocks_meta_disk_loads `+fmt.Sprintf("%d", tc.expectedDiskLoads)+`

				# HELP blocks_meta_loads_total Total number of block metadata load attempts
				# TYPE blocks_meta_loads_total counter
				blocks_meta_loads_total `+fmt.Sprintf("%d", tc.expectedLoads)+`
			`), "blocks_meta_loads_total", "blocks_meta_cached_loads", "blocks_meta_disk_loads"))
		})
	}
}
