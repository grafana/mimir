// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	mimir_testutil "github.com/grafana/mimir/pkg/storage/tsdb/testutil"
)

func TestMultiBucketBlocksFinder_GetBlocks(t *testing.T) {
	const userID = "user-1"

	var (
		now            = time.Now()
		recentDeletion = now.Unix()
		oldDeletion    = now.Add(-2 * time.Hour).Unix()

		onlyInNewBucket           = &bucketindex.Block{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20}
		inBothBucketsAsNew        = &bucketindex.Block{ID: ulid.MustNew(2, nil), MinTime: 10, MaxTime: 20, UploadedAt: 2}
		inBothBucketsAsOld        = &bucketindex.Block{ID: ulid.MustNew(2, nil), MinTime: 10, MaxTime: 20, UploadedAt: 1}
		onlyInOldBucket           = &bucketindex.Block{ID: ulid.MustNew(3, nil), MinTime: 10, MaxTime: 20}
		markedInOtherBucket       = &bucketindex.Block{ID: ulid.MustNew(4, nil), MinTime: 10, MaxTime: 20}
		markedInOwnBucket         = &bucketindex.Block{ID: ulid.MustNew(5, nil), MinTime: 10, MaxTime: 20}
		recentlyMarkedInOwnBucket = &bucketindex.Block{ID: ulid.MustNew(6, nil), MinTime: 10, MaxTime: 20}
	)

	newBucketIndex := &bucketindex.Index{
		Version: bucketindex.IndexVersion1,
		Blocks:  bucketindex.Blocks{onlyInNewBucket, inBothBucketsAsNew},
		BlockDeletionMarks: bucketindex.BlockDeletionMarks{
			{ID: markedInOtherBucket.ID, DeletionTime: oldDeletion},
		},
		UpdatedAt: now.Add(-time.Minute).Unix(),
	}
	oldBucketIndex := &bucketindex.Index{
		Version: bucketindex.IndexVersion1,
		Blocks:  bucketindex.Blocks{inBothBucketsAsOld, onlyInOldBucket, markedInOtherBucket, markedInOwnBucket, recentlyMarkedInOwnBucket},
		BlockDeletionMarks: bucketindex.BlockDeletionMarks{
			{ID: markedInOwnBucket.ID, DeletionTime: oldDeletion},
			{ID: recentlyMarkedInOwnBucket.ID, DeletionTime: recentDeletion},
		},
		UpdatedAt: now.Add(-10 * time.Minute).Unix(),
	}

	t.Run("returns the union of the blocks of all buckets, with the first bucket winning", func(t *testing.T) {
		newBucket, _ := mimir_testutil.PrepareFilesystemBucket(t)
		oldBucket, _ := mimir_testutil.PrepareFilesystemBucket(t)
		require.NoError(t, bucketindex.WriteIndex(t.Context(), newBucket, userID, nil, newBucketIndex))
		require.NoError(t, bucketindex.WriteIndex(t.Context(), oldBucket, userID, nil, oldBucketIndex))

		finder := prepareMultiBucketBlocksFinder(t, newBucket, oldBucket)

		blocks, metadata, err := finder.GetBlocks(t.Context(), userID, 0, 30)
		require.NoError(t, err)
		require.ElementsMatch(t, bucketindex.Blocks{onlyInNewBucket, inBothBucketsAsNew, onlyInOldBucket, markedInOtherBucket, recentlyMarkedInOwnBucket}, blocks)
		require.Equal(t, oldBucketIndex.Metadata(), metadata)
	})

	t.Run("returns the blocks of the buckets that have a bucket index", func(t *testing.T) {
		newBucket, _ := mimir_testutil.PrepareFilesystemBucket(t)
		oldBucket, _ := mimir_testutil.PrepareFilesystemBucket(t)
		require.NoError(t, bucketindex.WriteIndex(t.Context(), newBucket, userID, nil, newBucketIndex))

		finder := prepareMultiBucketBlocksFinder(t, newBucket, oldBucket)

		blocks, metadata, err := finder.GetBlocks(t.Context(), userID, 0, 30)
		require.NoError(t, err)
		require.ElementsMatch(t, bucketindex.Blocks{onlyInNewBucket, inBothBucketsAsNew}, blocks)
		require.Equal(t, newBucketIndex.Metadata(), metadata)
	})

	t.Run("returns no blocks and no metadata if no bucket has a bucket index", func(t *testing.T) {
		newBucket, _ := mimir_testutil.PrepareFilesystemBucket(t)
		oldBucket, _ := mimir_testutil.PrepareFilesystemBucket(t)

		finder := prepareMultiBucketBlocksFinder(t, newBucket, oldBucket)

		blocks, metadata, err := finder.GetBlocks(t.Context(), userID, 0, 30)
		require.NoError(t, err)
		require.Empty(t, blocks)
		require.Nil(t, metadata)
	})

	t.Run("fails if the bucket index of one bucket is corrupted", func(t *testing.T) {
		newBucket, _ := mimir_testutil.PrepareFilesystemBucket(t)
		oldBucket, _ := mimir_testutil.PrepareFilesystemBucket(t)
		require.NoError(t, bucketindex.WriteIndex(t.Context(), newBucket, userID, nil, newBucketIndex))
		require.NoError(t, oldBucket.Upload(t.Context(), path.Join(userID, bucketindex.IndexCompressedFilename), strings.NewReader("invalid}!")))

		finder := prepareMultiBucketBlocksFinder(t, newBucket, oldBucket)

		_, _, err := finder.GetBlocks(t.Context(), userID, 0, 30)
		require.ErrorIs(t, err, bucketindex.ErrIndexCorrupted)
	})

	t.Run("fails if the bucket index of one bucket is too old", func(t *testing.T) {
		newBucket, _ := mimir_testutil.PrepareFilesystemBucket(t)
		oldBucket, _ := mimir_testutil.PrepareFilesystemBucket(t)
		tooOldIndex := &bucketindex.Index{
			Version:   bucketindex.IndexVersion1,
			Blocks:    bucketindex.Blocks{onlyInOldBucket},
			UpdatedAt: now.Add(-2 * time.Hour).Unix(),
		}
		require.NoError(t, bucketindex.WriteIndex(t.Context(), newBucket, userID, nil, newBucketIndex))
		require.NoError(t, bucketindex.WriteIndex(t.Context(), oldBucket, userID, nil, tooOldIndex))

		finder := prepareMultiBucketBlocksFinder(t, newBucket, oldBucket)

		_, _, err := finder.GetBlocks(t.Context(), userID, 0, 30)
		require.EqualError(t, err, newBucketIndexTooOldError(tooOldIndex.GetUpdatedAt(), time.Hour).Error())
	})
}

func TestNewBlocksBucketSetFinder(t *testing.T) {
	const userID = "user-1"

	var storageCfg mimir_tsdb.BlocksStorageConfig
	flagext.DefaultValues(&storageCfg)

	newFilesystemConfig := func(dir string) bucket.Config {
		var cfg bucket.Config
		flagext.DefaultValues(&cfg)
		cfg.Backend = bucket.Filesystem
		cfg.Filesystem.Directory = dir
		return cfg
	}

	t.Run("returns a finder over all read buckets", func(t *testing.T) {
		finder, err := newBlocksBucketSetFinderFromConfigs(newFilesystemConfig(t.TempDir()), []bucket.Config{newFilesystemConfig(t.TempDir())}, cache.NewMockCache(), storageCfg, &blocksStoreLimitsMock{}, log.NewNopLogger(), prometheus.NewPedanticRegistry())
		require.NoError(t, err)
		require.IsType(t, &multiBucketBlocksFinder{}, finder)
		require.NoError(t, services.StartAndAwaitRunning(t.Context(), finder))
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), finder))
	})

	t.Run("rejects a duplicate bucket name", func(t *testing.T) {
		dir := t.TempDir()
		_, err := newBlocksBucketSetFinderFromConfigs(newFilesystemConfig(dir), []bucket.Config{newFilesystemConfig(dir)}, nil, storageCfg, &blocksStoreLimitsMock{}, log.NewNopLogger(), nil)
		require.ErrorContains(t, err, "is used more than once")
	})

	t.Run("with more than one read bucket it merges the blocks of all read buckets", func(t *testing.T) {
		newBucketDir, oldBucketDir := t.TempDir(), t.TempDir()

		// One registry and one metadata cache for all buckets, like NewBlocksStoreQueryableFromConfig does.
		reg := prometheus.NewPedanticRegistry()
		bucketSet, err := bucket.NewBlocksBucketSetFromConfigs(t.Context(), newFilesystemConfig(newBucketDir), []bucket.Config{newFilesystemConfig(oldBucketDir)}, "querier", log.NewNopLogger(), reg)
		require.NoError(t, err)

		finder, err := newBlocksBucketSetFinder(bucketSet, cache.NewMockCache(), storageCfg, &blocksStoreLimitsMock{}, log.NewNopLogger(), reg)
		require.NoError(t, err)
		require.IsType(t, &multiBucketBlocksFinder{}, finder)

		newBlock := &bucketindex.Block{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 20}
		oldBlock := &bucketindex.Block{ID: ulid.MustNew(2, nil), MinTime: 10, MaxTime: 20}
		for _, readBucket := range []struct {
			client objstore.Bucket
			block  *bucketindex.Block
		}{
			{client: bucketSet.ReadBuckets()[0].Client, block: newBlock},
			{client: bucketSet.ReadBuckets()[1].Client, block: oldBlock},
		} {
			require.NoError(t, bucketindex.WriteIndex(t.Context(), readBucket.client, userID, nil, &bucketindex.Index{
				Version:   bucketindex.IndexVersion1,
				Blocks:    bucketindex.Blocks{readBucket.block},
				UpdatedAt: time.Now().Unix(),
			}))
		}

		require.NoError(t, services.StartAndAwaitRunning(t.Context(), finder))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), finder))
		})

		blocks, _, err := finder.GetBlocks(t.Context(), userID, 0, 30)
		require.NoError(t, err)
		require.ElementsMatch(t, bucketindex.Blocks{newBlock, oldBlock}, blocks)
	})
}

func prepareMultiBucketBlocksFinder(t *testing.T, bucketClients ...objstore.Bucket) *multiBucketBlocksFinder {
	finders := make([]BlocksFinder, 0, len(bucketClients))
	for _, bucketClient := range bucketClients {
		finders = append(finders, newBucketIndexBlocksFinderForTest(bucketClient))
	}

	finder, err := newMultiBucketBlocksFinder(finders)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), finder))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), finder))
	})
	return finder
}
