// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/compactor/blocks_cleaner_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package compactor

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	prom_tsdb "github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	mimir_testutil "github.com/grafana/mimir/pkg/storage/tsdb/testutil"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/test"
)

type testBlocksCleanerOptions struct {
	concurrency         int
	tenantDeletionDelay time.Duration
	user4FilesExist     bool // User 4 has "FinishedTime" in tenant deletion marker set to "1h" ago.
}

func (o testBlocksCleanerOptions) String() string {
	return fmt.Sprintf("concurrency=%d, tenant deletion delay=%v",
		o.concurrency, o.tenantDeletionDelay)
}

func TestBlocksCleaner(t *testing.T) {
	for _, options := range []testBlocksCleanerOptions{
		{concurrency: 1, tenantDeletionDelay: 0, user4FilesExist: false},
		{concurrency: 1, tenantDeletionDelay: 2 * time.Hour, user4FilesExist: true},
		{concurrency: 2},
		{concurrency: 10},
	} {
		t.Run(options.String(), func(t *testing.T) {
			t.Parallel()
			testBlocksCleanerWithOptions(t, options)
		})
	}
}

func testBlocksCleanerWithOptions(t *testing.T, options testBlocksCleanerOptions) {
	bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bucketClient = block.BucketWithGlobalMarkers(bucketClient)

	// Create blocks.
	ctx := context.Background()
	now := time.Now()
	deletionDelay := 12 * time.Hour
	block1 := createTSDBBlock(t, bucketClient, "user-1", 10, 20, 2, nil)
	block2 := createTSDBBlock(t, bucketClient, "user-1", 20, 30, 2, nil)
	block3 := createTSDBBlock(t, bucketClient, "user-1", 30, 40, 2, nil)
	block4 := ulid.MustNew(4, rand.Reader)
	block5 := ulid.MustNew(5, rand.Reader)
	block6 := createTSDBBlock(t, bucketClient, "user-1", 40, 50, 2, nil)
	block7 := createTSDBBlock(t, bucketClient, "user-2", 10, 20, 2, nil)
	block8 := createTSDBBlock(t, bucketClient, "user-2", 40, 50, 2, nil)
	createDeletionMark(t, bucketClient, "user-1", block2, now.Add(-deletionDelay).Add(time.Hour))          // Block hasn't reached the deletion threshold yet.
	createDeletionMark(t, bucketClient, "user-1", block3, now.Add(-deletionDelay).Add(-time.Hour))         // Block reached the deletion threshold.
	createDeletionMark(t, bucketClient, "user-1", block4, now.Add(-deletionDelay).Add(time.Hour))          // Partial block hasn't reached the deletion threshold yet.
	createDeletionMark(t, bucketClient, "user-1", block5, now.Add(-deletionDelay).Add(-time.Hour))         // Partial block reached the deletion threshold.
	require.NoError(t, bucketClient.Delete(ctx, path.Join("user-1", block6.String(), block.MetaFilename))) // Partial block without deletion mark.
	createDeletionMark(t, bucketClient, "user-2", block7, now.Add(-deletionDelay).Add(-time.Hour))         // Block reached the deletion threshold.

	// Blocks for user-3, marked for deletion.
	require.NoError(t, tsdb.WriteTenantDeletionMark(context.Background(), bucketClient, "user-3", nil, tsdb.NewTenantDeletionMark(time.Now())))
	block9 := createTSDBBlock(t, bucketClient, "user-3", 10, 30, 2, nil)
	block10 := createTSDBBlock(t, bucketClient, "user-3", 30, 50, 2, nil)

	// User-4 with no more blocks, but couple of mark and debug files. Should be fully deleted.
	user4Mark := tsdb.NewTenantDeletionMark(time.Now())
	user4Mark.FinishedTime = util.UnixSecondsFromTime(time.Now().Add(-60 * time.Second)) // Set to check final user cleanup.
	require.NoError(t, tsdb.WriteTenantDeletionMark(context.Background(), bucketClient, "user-4", nil, user4Mark))
	user4DebugMetaFile := path.Join("user-4", block.DebugMetas, "meta.json")
	require.NoError(t, bucketClient.Upload(context.Background(), user4DebugMetaFile, strings.NewReader("some random content here")))

	cfg := BlocksCleanerConfig{
		DeletionDelay:                 deletionDelay,
		CleanupInterval:               time.Minute,
		CleanupConcurrency:            options.concurrency,
		TenantCleanupDelay:            options.tenantDeletionDelay,
		DeleteBlocksConcurrency:       1,
		GetDeletionMarkersConcurrency: 1,
	}

	reg := prometheus.NewPedanticRegistry()
	logger := log.NewNopLogger()
	cfgProvider := newMockConfigProvider()

	cleaner := NewBlocksCleaner(cfg, bucketClient, tsdb.AllUsers, cfgProvider, logger, reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, cleaner))
	defer services.StopAndAwaitTerminated(ctx, cleaner) //nolint:errcheck

	for _, tc := range []struct {
		path           string
		expectedExists bool
	}{
		// Check the storage to ensure only the block which has reached the deletion threshold
		// has been effectively deleted.
		{path: path.Join("user-1", block1.String(), block.MetaFilename), expectedExists: true},
		{path: path.Join("user-1", block3.String(), block.MetaFilename), expectedExists: false},
		{path: path.Join("user-2", block7.String(), block.MetaFilename), expectedExists: false},
		{path: path.Join("user-2", block8.String(), block.MetaFilename), expectedExists: true},
		// Should not delete a block with deletion mark which hasn't reached the deletion threshold yet.
		{path: path.Join("user-1", block2.String(), block.MetaFilename), expectedExists: true},
		{path: path.Join("user-1", block.DeletionMarkFilepath(block2)), expectedExists: true},
		// Should delete a partial block with deletion mark which hasn't reached the deletion threshold yet.
		{path: path.Join("user-1", block4.String(), block.DeletionMarkFilename), expectedExists: false},
		{path: path.Join("user-1", block.DeletionMarkFilepath(block4)), expectedExists: false},
		// Should delete a partial block with deletion mark which has reached the deletion threshold.
		{path: path.Join("user-1", block5.String(), block.DeletionMarkFilename), expectedExists: false},
		{path: path.Join("user-1", block.DeletionMarkFilepath(block5)), expectedExists: false},
		// Should not delete a partial block without deletion mark.
		{path: path.Join("user-1", block6.String(), "index"), expectedExists: true},
		// Should completely delete blocks for user-3, marked for deletion
		{path: path.Join("user-3", block9.String(), block.MetaFilename), expectedExists: false},
		{path: path.Join("user-3", block9.String(), "index"), expectedExists: false},
		{path: path.Join("user-3", block10.String(), block.MetaFilename), expectedExists: false},
		{path: path.Join("user-3", block10.String(), "index"), expectedExists: false},
		// Tenant deletion mark is not removed.
		{path: path.Join("user-3", tsdb.TenantDeletionMarkPath), expectedExists: true},
		// User-4 is removed fully.
		{path: path.Join("user-4", tsdb.TenantDeletionMarkPath), expectedExists: options.user4FilesExist},
		{path: path.Join("user-4", block.DebugMetas, "meta.json"), expectedExists: options.user4FilesExist},
	} {
		exists, err := bucketClient.Exists(ctx, tc.path)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedExists, exists, tc.path)
	}

	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsStarted))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsCompleted))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.runsFailed))
	assert.Equal(t, float64(6), testutil.ToFloat64(cleaner.blocksCleanedTotal))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.blocksFailedTotal))

	// Check the updated bucket index.
	for _, tc := range []struct {
		userID         string
		expectedIndex  bool
		expectedBlocks []ulid.ULID
		expectedMarks  []ulid.ULID
	}{
		{
			userID:         "user-1",
			expectedIndex:  true,
			expectedBlocks: []ulid.ULID{block1, block2 /* deleted: block3, block4, block5, partial: block6 */},
			expectedMarks:  []ulid.ULID{block2},
		}, {
			userID:         "user-2",
			expectedIndex:  true,
			expectedBlocks: []ulid.ULID{block8},
			expectedMarks:  []ulid.ULID{},
		}, {
			userID:        "user-3",
			expectedIndex: false,
		},
	} {
		idx, err := bucketindex.ReadIndex(ctx, bucketClient, tc.userID, nil, logger)
		if !tc.expectedIndex {
			assert.Equal(t, bucketindex.ErrIndexNotFound, err)
			continue
		}

		require.NoError(t, err)
		assert.ElementsMatch(t, tc.expectedBlocks, idx.Blocks.GetULIDs())
		assert.ElementsMatch(t, tc.expectedMarks, idx.BlockDeletionMarks.GetULIDs())
	}

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
		# TYPE cortex_bucket_blocks_count gauge
		cortex_bucket_blocks_count{user="user-1"} 2
		cortex_bucket_blocks_count{user="user-2"} 1
		# HELP cortex_bucket_blocks_marked_for_deletion_count Total number of blocks marked for deletion in the bucket.
		# TYPE cortex_bucket_blocks_marked_for_deletion_count gauge
		cortex_bucket_blocks_marked_for_deletion_count{user="user-1"} 1
		cortex_bucket_blocks_marked_for_deletion_count{user="user-2"} 0
		# HELP cortex_bucket_blocks_partials_count Total number of partial blocks.
		# TYPE cortex_bucket_blocks_partials_count gauge
		cortex_bucket_blocks_partials_count{user="user-1"} 2
		cortex_bucket_blocks_partials_count{user="user-2"} 0
		# HELP cortex_bucket_index_estimated_compaction_jobs Estimated number of compaction jobs based on latest version of bucket index.
		# TYPE cortex_bucket_index_estimated_compaction_jobs gauge
		cortex_bucket_index_estimated_compaction_jobs{type="merge",user="user-1"} 0
		cortex_bucket_index_estimated_compaction_jobs{type="split",user="user-1"} 0
		cortex_bucket_index_estimated_compaction_jobs{type="merge",user="user-2"} 0
		cortex_bucket_index_estimated_compaction_jobs{type="split",user="user-2"} 0
	`),
		"cortex_bucket_blocks_count",
		"cortex_bucket_blocks_marked_for_deletion_count",
		"cortex_bucket_blocks_partials_count",
		"cortex_bucket_index_estimated_compaction_jobs",
	))
}

func TestBlocksCleaner_ShouldContinueOnBlockDeletionFailure(t *testing.T) {
	const userID = "user-1"

	bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bucketClient = block.BucketWithGlobalMarkers(bucketClient)

	// Create blocks.
	ctx := context.Background()
	now := time.Now()
	deletionDelay := 12 * time.Hour
	block1 := createTSDBBlock(t, bucketClient, userID, 10, 20, 2, nil)
	block2 := createTSDBBlock(t, bucketClient, userID, 20, 30, 2, nil)
	block3 := createTSDBBlock(t, bucketClient, userID, 30, 40, 2, nil)
	block4 := createTSDBBlock(t, bucketClient, userID, 40, 50, 2, nil)
	createDeletionMark(t, bucketClient, userID, block2, now.Add(-deletionDelay).Add(-time.Hour))
	createDeletionMark(t, bucketClient, userID, block3, now.Add(-deletionDelay).Add(-time.Hour))
	createDeletionMark(t, bucketClient, userID, block4, now.Add(-deletionDelay).Add(-time.Hour))

	// To emulate a failure deleting a block, we wrap the bucket client in a mocked one.
	bucketClient = &mockBucketFailure{
		Bucket:         bucketClient,
		DeleteFailures: []string{path.Join(userID, block3.String(), block.MetaFilename)},
	}

	cfg := BlocksCleanerConfig{
		DeletionDelay:                 deletionDelay,
		CleanupInterval:               time.Minute,
		CleanupConcurrency:            1,
		DeleteBlocksConcurrency:       1,
		GetDeletionMarkersConcurrency: 1,
	}

	logger := log.NewNopLogger()
	cfgProvider := newMockConfigProvider()

	cleaner := NewBlocksCleaner(cfg, bucketClient, tsdb.AllUsers, cfgProvider, logger, nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, cleaner))
	defer services.StopAndAwaitTerminated(ctx, cleaner) //nolint:errcheck

	for _, tc := range []struct {
		path           string
		expectedExists bool
	}{
		{path: path.Join(userID, block1.String(), block.MetaFilename), expectedExists: true},
		{path: path.Join(userID, block2.String(), block.MetaFilename), expectedExists: false},
		{path: path.Join(userID, block3.String(), block.MetaFilename), expectedExists: true},
		{path: path.Join(userID, block4.String(), block.MetaFilename), expectedExists: false},
	} {
		exists, err := bucketClient.Exists(ctx, tc.path)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedExists, exists, tc.path)
	}

	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsStarted))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsCompleted))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.runsFailed))
	assert.Equal(t, float64(2), testutil.ToFloat64(cleaner.blocksCleanedTotal))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.blocksFailedTotal))

	// Check the updated bucket index.
	idx, err := bucketindex.ReadIndex(ctx, bucketClient, userID, nil, logger)
	require.NoError(t, err)
	assert.ElementsMatch(t, []ulid.ULID{block1, block3}, idx.Blocks.GetULIDs())
	assert.ElementsMatch(t, []ulid.ULID{block3}, idx.BlockDeletionMarks.GetULIDs())
}

func TestBlocksCleaner_ShouldRebuildBucketIndexOnCorruptedOne(t *testing.T) {
	const userID = "user-1"

	bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bucketClient = block.BucketWithGlobalMarkers(bucketClient)

	// Create blocks.
	ctx := context.Background()
	now := time.Now()
	deletionDelay := 12 * time.Hour
	block1 := createTSDBBlock(t, bucketClient, userID, 10, 20, 2, nil)
	block2 := createTSDBBlock(t, bucketClient, userID, 20, 30, 2, nil)
	block3 := createTSDBBlock(t, bucketClient, userID, 30, 40, 2, nil)
	createDeletionMark(t, bucketClient, userID, block2, now.Add(-deletionDelay).Add(-time.Hour))
	createDeletionMark(t, bucketClient, userID, block3, now.Add(-deletionDelay).Add(time.Hour))

	// Write a corrupted bucket index.
	require.NoError(t, bucketClient.Upload(ctx, path.Join(userID, bucketindex.IndexCompressedFilename), strings.NewReader("invalid!}")))

	cfg := BlocksCleanerConfig{
		DeletionDelay:                 deletionDelay,
		CleanupInterval:               time.Minute,
		CleanupConcurrency:            1,
		DeleteBlocksConcurrency:       1,
		GetDeletionMarkersConcurrency: 1,
	}

	logger := log.NewNopLogger()
	cfgProvider := newMockConfigProvider()

	cleaner := NewBlocksCleaner(cfg, bucketClient, tsdb.AllUsers, cfgProvider, logger, nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, cleaner))
	defer services.StopAndAwaitTerminated(ctx, cleaner) //nolint:errcheck

	for _, tc := range []struct {
		path           string
		expectedExists bool
	}{
		{path: path.Join(userID, block1.String(), block.MetaFilename), expectedExists: true},
		{path: path.Join(userID, block2.String(), block.MetaFilename), expectedExists: false},
		{path: path.Join(userID, block3.String(), block.MetaFilename), expectedExists: true},
	} {
		exists, err := bucketClient.Exists(ctx, tc.path)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedExists, exists, tc.path)
	}

	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsStarted))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsCompleted))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.runsFailed))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.blocksCleanedTotal))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.blocksFailedTotal))

	// Check the updated bucket index.
	idx, err := bucketindex.ReadIndex(ctx, bucketClient, userID, nil, logger)
	require.NoError(t, err)
	assert.ElementsMatch(t, []ulid.ULID{block1, block3}, idx.Blocks.GetULIDs())
	assert.ElementsMatch(t, []ulid.ULID{block3}, idx.BlockDeletionMarks.GetULIDs())
}

func TestBlocksCleaner_ShouldRemoveMetricsForTenantsNotBelongingAnymoreToTheShard(t *testing.T) {
	bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bucketClient = block.BucketWithGlobalMarkers(bucketClient)

	// Create blocks.
	createTSDBBlock(t, bucketClient, "user-1", 10, 20, 2, nil)
	createTSDBBlock(t, bucketClient, "user-1", 20, 30, 2, nil)
	createTSDBBlock(t, bucketClient, "user-2", 30, 40, 2, nil)

	cfg := BlocksCleanerConfig{
		DeletionDelay:                 time.Hour,
		CleanupInterval:               time.Minute,
		CleanupConcurrency:            1,
		DeleteBlocksConcurrency:       1,
		GetDeletionMarkersConcurrency: 1,
	}

	ctx := context.Background()
	logger := log.NewNopLogger()
	reg := prometheus.NewPedanticRegistry()
	cfgProvider := newMockConfigProvider()

	cleaner := NewBlocksCleaner(cfg, bucketClient, tsdb.AllUsers, cfgProvider, logger, reg)
	require.NoError(t, cleaner.runCleanupWithErr(ctx))

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
		# TYPE cortex_bucket_blocks_count gauge
		cortex_bucket_blocks_count{user="user-1"} 2
		cortex_bucket_blocks_count{user="user-2"} 1
		# HELP cortex_bucket_blocks_marked_for_deletion_count Total number of blocks marked for deletion in the bucket.
		# TYPE cortex_bucket_blocks_marked_for_deletion_count gauge
		cortex_bucket_blocks_marked_for_deletion_count{user="user-1"} 0
		cortex_bucket_blocks_marked_for_deletion_count{user="user-2"} 0
		# HELP cortex_bucket_blocks_partials_count Total number of partial blocks.
		# TYPE cortex_bucket_blocks_partials_count gauge
		cortex_bucket_blocks_partials_count{user="user-1"} 0
		cortex_bucket_blocks_partials_count{user="user-2"} 0
		# HELP cortex_bucket_index_estimated_compaction_jobs Estimated number of compaction jobs based on latest version of bucket index.
		# TYPE cortex_bucket_index_estimated_compaction_jobs gauge
		cortex_bucket_index_estimated_compaction_jobs{type="merge",user="user-1"} 0
		cortex_bucket_index_estimated_compaction_jobs{type="split",user="user-1"} 0
		cortex_bucket_index_estimated_compaction_jobs{type="merge",user="user-2"} 0
		cortex_bucket_index_estimated_compaction_jobs{type="split",user="user-2"} 0
	`),
		"cortex_bucket_blocks_count",
		"cortex_bucket_blocks_marked_for_deletion_count",
		"cortex_bucket_blocks_partials_count",
		"cortex_bucket_index_estimated_compaction_jobs",
	))

	// Override the users scanner to reconfigure it to only return a subset of users.
	cleaner.usersScanner = tsdb.NewUsersScanner(bucketClient, func(userID string) (bool, error) { return userID == "user-1", nil }, logger)

	// Create new blocks, to double check expected metrics have changed.
	createTSDBBlock(t, bucketClient, "user-1", 40, 50, 2, nil)
	createTSDBBlock(t, bucketClient, "user-2", 50, 60, 2, nil)

	require.NoError(t, cleaner.runCleanupWithErr(ctx))

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
		# TYPE cortex_bucket_blocks_count gauge
		cortex_bucket_blocks_count{user="user-1"} 3
		# HELP cortex_bucket_blocks_marked_for_deletion_count Total number of blocks marked for deletion in the bucket.
		# TYPE cortex_bucket_blocks_marked_for_deletion_count gauge
		cortex_bucket_blocks_marked_for_deletion_count{user="user-1"} 0
		# HELP cortex_bucket_blocks_partials_count Total number of partial blocks.
		# TYPE cortex_bucket_blocks_partials_count gauge
		cortex_bucket_blocks_partials_count{user="user-1"} 0
		# HELP cortex_bucket_index_estimated_compaction_jobs Estimated number of compaction jobs based on latest version of bucket index.
		# TYPE cortex_bucket_index_estimated_compaction_jobs gauge
		cortex_bucket_index_estimated_compaction_jobs{type="merge",user="user-1"} 0
		cortex_bucket_index_estimated_compaction_jobs{type="split",user="user-1"} 0
	`),
		"cortex_bucket_blocks_count",
		"cortex_bucket_blocks_marked_for_deletion_count",
		"cortex_bucket_blocks_partials_count",
		"cortex_bucket_index_estimated_compaction_jobs",
	))
}

func TestBlocksCleaner_ShouldNotCleanupUserThatDoesntBelongToShardAnymore(t *testing.T) {
	bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bucketClient = block.BucketWithGlobalMarkers(bucketClient)

	// Create blocks.
	createTSDBBlock(t, bucketClient, "user-1", 10, 20, 2, nil)
	createTSDBBlock(t, bucketClient, "user-2", 20, 30, 2, nil)

	cfg := BlocksCleanerConfig{
		DeletionDelay:           time.Hour,
		CleanupInterval:         time.Minute,
		CleanupConcurrency:      1,
		DeleteBlocksConcurrency: 1,
	}

	ctx := context.Background()
	logger := log.NewNopLogger()
	reg := prometheus.NewPedanticRegistry()
	cfgProvider := newMockConfigProvider()

	// We will simulate change of "ownUser" by counting number of replies per user. First reply will be "true",
	// all subsequent replies will be false.

	userSeen := map[string]bool{}
	ownUser := func(user string) (bool, error) {
		if userSeen[user] {
			return false, nil
		}
		userSeen[user] = true
		return true, nil
	}

	cleaner := NewBlocksCleaner(cfg, bucketClient, ownUser, cfgProvider, logger, reg)
	require.NoError(t, cleaner.runCleanupWithErr(ctx))

	// Verify that we have seen the users
	require.ElementsMatch(t, []string{"user-1", "user-2"}, cleaner.lastOwnedUsers)

	// But there are no metrics for any user, because we did not in fact clean them.
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
		# TYPE cortex_bucket_blocks_count gauge
	`),
		"cortex_bucket_blocks_count",
	))

	// Running cleanUsers again will see that users are no longer owned.
	require.NoError(t, cleaner.runCleanupWithErr(ctx))
	require.ElementsMatch(t, []string{}, cleaner.lastOwnedUsers)
}

func TestBlocksCleaner_ListBlocksOutsideRetentionPeriod(t *testing.T) {
	bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bucketClient = block.BucketWithGlobalMarkers(bucketClient)
	ctx := context.Background()
	logger := log.NewNopLogger()

	id1 := createTSDBBlock(t, bucketClient, "user-1", 5000, 6000, 2, nil)
	id2 := createTSDBBlock(t, bucketClient, "user-1", 6000, 7000, 2, nil)
	id3 := createTSDBBlock(t, bucketClient, "user-1", 7000, 8000, 2, nil)

	w := bucketindex.NewUpdater(bucketClient, "user-1", nil, 16, logger)
	idx, _, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)

	assert.ElementsMatch(t, []ulid.ULID{id1, id2, id3}, idx.Blocks.GetULIDs())

	// Excessive retention period (wrapping epoch)
	result := listBlocksOutsideRetentionPeriod(idx, time.Unix(10, 0).Add(-time.Hour))
	assert.ElementsMatch(t, []ulid.ULID{}, result.GetULIDs())

	// Normal operation - varying retention period.
	result = listBlocksOutsideRetentionPeriod(idx, time.Unix(6, 0))
	assert.ElementsMatch(t, []ulid.ULID{}, result.GetULIDs())

	result = listBlocksOutsideRetentionPeriod(idx, time.Unix(7, 0))
	assert.ElementsMatch(t, []ulid.ULID{id1}, result.GetULIDs())

	result = listBlocksOutsideRetentionPeriod(idx, time.Unix(8, 0))
	assert.ElementsMatch(t, []ulid.ULID{id1, id2}, result.GetULIDs())

	result = listBlocksOutsideRetentionPeriod(idx, time.Unix(9, 0))
	assert.ElementsMatch(t, []ulid.ULID{id1, id2, id3}, result.GetULIDs())

	// Avoiding redundant marking - blocks already marked for deletion.

	mark1 := &bucketindex.BlockDeletionMark{ID: id1}
	mark2 := &bucketindex.BlockDeletionMark{ID: id2}

	idx.BlockDeletionMarks = bucketindex.BlockDeletionMarks{mark1}

	result = listBlocksOutsideRetentionPeriod(idx, time.Unix(7, 0))
	assert.ElementsMatch(t, []ulid.ULID{}, result.GetULIDs())

	result = listBlocksOutsideRetentionPeriod(idx, time.Unix(8, 0))
	assert.ElementsMatch(t, []ulid.ULID{id2}, result.GetULIDs())

	idx.BlockDeletionMarks = bucketindex.BlockDeletionMarks{mark1, mark2}

	result = listBlocksOutsideRetentionPeriod(idx, time.Unix(7, 0))
	assert.ElementsMatch(t, []ulid.ULID{}, result.GetULIDs())

	result = listBlocksOutsideRetentionPeriod(idx, time.Unix(8, 0))
	assert.ElementsMatch(t, []ulid.ULID{}, result.GetULIDs())

	result = listBlocksOutsideRetentionPeriod(idx, time.Unix(9, 0))
	assert.ElementsMatch(t, []ulid.ULID{id3}, result.GetULIDs())
}

func TestBlocksCleaner_ShouldRemoveBlocksOutsideRetentionPeriod(t *testing.T) {
	bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bucketClient = block.BucketWithGlobalMarkers(bucketClient)

	ts := func(hours int) int64 {
		return time.Now().Add(time.Duration(hours)*time.Hour).Unix() * 1000
	}

	block1 := createTSDBBlock(t, bucketClient, "user-1", ts(-10), ts(-8), 2, nil)
	block2 := createTSDBBlock(t, bucketClient, "user-1", ts(-8), ts(-6), 2, nil)
	block3 := createTSDBBlock(t, bucketClient, "user-2", ts(-10), ts(-8), 2, nil)
	block4 := createTSDBBlock(t, bucketClient, "user-2", ts(-8), ts(-6), 2, nil)

	cfg := BlocksCleanerConfig{
		DeletionDelay:           time.Hour,
		CleanupInterval:         time.Minute,
		CleanupConcurrency:      1,
		DeleteBlocksConcurrency: 1,
	}

	ctx := context.Background()
	logger := test.NewTestingLogger(t)
	reg := prometheus.NewPedanticRegistry()
	cfgProvider := newMockConfigProvider()

	cleaner := NewBlocksCleaner(cfg, bucketClient, tsdb.AllUsers, cfgProvider, logger, reg)

	assertBlockExists := func(user string, blockID ulid.ULID, expectExists bool) {
		exists, err := bucketClient.Exists(ctx, path.Join(user, blockID.String(), block.MetaFilename))
		require.NoError(t, err)
		assert.Equal(t, expectExists, exists)
	}

	// Existing behaviour - retention period disabled.
	{
		cfgProvider.userRetentionPeriods["user-1"] = 0
		cfgProvider.userRetentionPeriods["user-2"] = 0

		require.NoError(t, cleaner.runCleanupWithErr(ctx))
		assertBlockExists("user-1", block1, true)
		assertBlockExists("user-1", block2, true)
		assertBlockExists("user-2", block3, true)
		assertBlockExists("user-2", block4, true)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
			# TYPE cortex_bucket_blocks_count gauge
			cortex_bucket_blocks_count{user="user-1"} 2
			cortex_bucket_blocks_count{user="user-2"} 2
			# HELP cortex_bucket_blocks_marked_for_deletion_count Total number of blocks marked for deletion in the bucket.
			# TYPE cortex_bucket_blocks_marked_for_deletion_count gauge
			cortex_bucket_blocks_marked_for_deletion_count{user="user-1"} 0
			cortex_bucket_blocks_marked_for_deletion_count{user="user-2"} 0
			# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
			# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
			cortex_compactor_blocks_marked_for_deletion_total{reason="partial"} 0
			cortex_compactor_blocks_marked_for_deletion_total{reason="retention"} 0
			`),
			"cortex_bucket_blocks_count",
			"cortex_bucket_blocks_marked_for_deletion_count",
			"cortex_compactor_blocks_marked_for_deletion_total",
		))
	}

	// Retention enabled only for a single user, but does nothing.
	{
		cfgProvider.userRetentionPeriods["user-1"] = 9 * time.Hour

		require.NoError(t, cleaner.runCleanupWithErr(ctx))
		assertBlockExists("user-1", block1, true)
		assertBlockExists("user-1", block2, true)
		assertBlockExists("user-2", block3, true)
		assertBlockExists("user-2", block4, true)
	}

	// Retention enabled only for a single user, marking a single block.
	// Note the block won't be deleted yet due to deletion delay.
	{
		cfgProvider.userRetentionPeriods["user-1"] = 7 * time.Hour

		require.NoError(t, cleaner.runCleanupWithErr(ctx))
		assertBlockExists("user-1", block1, true)
		assertBlockExists("user-1", block2, true)
		assertBlockExists("user-2", block3, true)
		assertBlockExists("user-2", block4, true)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
			# TYPE cortex_bucket_blocks_count gauge
			cortex_bucket_blocks_count{user="user-1"} 2
			cortex_bucket_blocks_count{user="user-2"} 2
			# HELP cortex_bucket_blocks_marked_for_deletion_count Total number of blocks marked for deletion in the bucket.
			# TYPE cortex_bucket_blocks_marked_for_deletion_count gauge
			cortex_bucket_blocks_marked_for_deletion_count{user="user-1"} 1
			cortex_bucket_blocks_marked_for_deletion_count{user="user-2"} 0
			# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
			# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
			cortex_compactor_blocks_marked_for_deletion_total{reason="partial"} 0
			cortex_compactor_blocks_marked_for_deletion_total{reason="retention"} 1
			`),
			"cortex_bucket_blocks_count",
			"cortex_bucket_blocks_marked_for_deletion_count",
			"cortex_compactor_blocks_marked_for_deletion_total",
		))
	}

	// Marking the block again, before the deletion occurs, should not cause an error.
	{
		require.NoError(t, cleaner.runCleanupWithErr(ctx))
		assertBlockExists("user-1", block1, true)
		assertBlockExists("user-1", block2, true)
		assertBlockExists("user-2", block3, true)
		assertBlockExists("user-2", block4, true)
	}

	// Reduce the deletion delay. Now the block will be deleted.
	{
		cleaner.cfg.DeletionDelay = 0

		require.NoError(t, cleaner.runCleanupWithErr(ctx))
		assertBlockExists("user-1", block1, false)
		assertBlockExists("user-1", block2, true)
		assertBlockExists("user-2", block3, true)
		assertBlockExists("user-2", block4, true)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
			# TYPE cortex_bucket_blocks_count gauge
			cortex_bucket_blocks_count{user="user-1"} 1
			cortex_bucket_blocks_count{user="user-2"} 2
			# HELP cortex_bucket_blocks_marked_for_deletion_count Total number of blocks marked for deletion in the bucket.
			# TYPE cortex_bucket_blocks_marked_for_deletion_count gauge
			cortex_bucket_blocks_marked_for_deletion_count{user="user-1"} 0
			cortex_bucket_blocks_marked_for_deletion_count{user="user-2"} 0
			# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
			# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
			cortex_compactor_blocks_marked_for_deletion_total{reason="partial"} 0
			cortex_compactor_blocks_marked_for_deletion_total{reason="retention"} 1
			`),
			"cortex_bucket_blocks_count",
			"cortex_bucket_blocks_marked_for_deletion_count",
			"cortex_compactor_blocks_marked_for_deletion_total",
		))
	}

	// Retention enabled for other user; test deleting multiple blocks.
	{
		cfgProvider.userRetentionPeriods["user-2"] = 5 * time.Hour

		require.NoError(t, cleaner.runCleanupWithErr(ctx))
		assertBlockExists("user-1", block1, false)
		assertBlockExists("user-1", block2, true)
		assertBlockExists("user-2", block3, false)
		assertBlockExists("user-2", block4, false)

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
			# TYPE cortex_bucket_blocks_count gauge
			cortex_bucket_blocks_count{user="user-1"} 1
			cortex_bucket_blocks_count{user="user-2"} 0
			# HELP cortex_bucket_blocks_marked_for_deletion_count Total number of blocks marked for deletion in the bucket.
			# TYPE cortex_bucket_blocks_marked_for_deletion_count gauge
			cortex_bucket_blocks_marked_for_deletion_count{user="user-1"} 0
			cortex_bucket_blocks_marked_for_deletion_count{user="user-2"} 0
			# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
			# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
			cortex_compactor_blocks_marked_for_deletion_total{reason="partial"} 0
			cortex_compactor_blocks_marked_for_deletion_total{reason="retention"} 3
			`),
			"cortex_bucket_blocks_count",
			"cortex_bucket_blocks_marked_for_deletion_count",
			"cortex_compactor_blocks_marked_for_deletion_total",
		))
	}
}

func checkBlock(t *testing.T, user string, bucketClient objstore.Bucket, blockID ulid.ULID, metaJSONExists bool, markedForDeletion bool) {
	exists, err := bucketClient.Exists(context.Background(), path.Join(user, blockID.String(), block.MetaFilename))
	require.NoError(t, err)
	require.Equal(t, metaJSONExists, exists)

	exists, err = bucketClient.Exists(context.Background(), path.Join(user, blockID.String(), block.DeletionMarkFilename))
	require.NoError(t, err)
	require.Equal(t, markedForDeletion, exists)
}

func TestBlocksCleaner_ShouldCleanUpFilesWhenNoMoreBlocksRemain(t *testing.T) {
	bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bucketClient = block.BucketWithGlobalMarkers(bucketClient)

	const userID = "user-1"
	ctx := context.Background()
	now := time.Now()
	deletionDelay := 12 * time.Hour

	// Create two blocks and mark them for deletion at a time before the deletionDelay
	block1 := createTSDBBlock(t, bucketClient, userID, 10, 20, 2, nil)
	block2 := createTSDBBlock(t, bucketClient, userID, 20, 30, 2, nil)

	createDeletionMark(t, bucketClient, userID, block1, now.Add(-deletionDelay).Add(-time.Hour))
	createDeletionMark(t, bucketClient, userID, block2, now.Add(-deletionDelay).Add(-time.Hour))

	checkBlock(t, "user-1", bucketClient, block1, true, true)
	checkBlock(t, "user-1", bucketClient, block2, true, true)

	// Create a deletion mark within the deletionDelay period that won't correspond to any block
	randomULID := ulid.MustNew(ulid.Now(), rand.Reader)
	createDeletionMark(t, bucketClient, userID, randomULID, now.Add(-deletionDelay).Add(time.Hour))
	blockDeletionMarkFile := path.Join(userID, block.DeletionMarkFilepath(randomULID))
	exists, err := bucketClient.Exists(ctx, blockDeletionMarkFile)
	require.NoError(t, err)
	assert.True(t, exists)

	// Create a debug file that wouldn't otherwise be deleted by the cleaner
	debugMetaFile := path.Join(userID, block.DebugMetas, "meta.json")
	require.NoError(t, bucketClient.Upload(context.Background(), debugMetaFile, strings.NewReader("random content")))

	cfg := BlocksCleanerConfig{
		DeletionDelay:              deletionDelay,
		CleanupInterval:            time.Minute,
		CleanupConcurrency:         1,
		DeleteBlocksConcurrency:    1,
		NoBlocksFileCleanupEnabled: true,
	}

	logger := test.NewTestingLogger(t)
	reg := prometheus.NewPedanticRegistry()
	cfgProvider := newMockConfigProvider()

	cleaner := NewBlocksCleaner(cfg, bucketClient, tsdb.AllUsers, cfgProvider, logger, reg)
	require.NoError(t, cleaner.runCleanupWithErr(ctx))

	// Check bucket index, markers and debug files have been deleted.
	exists, err = bucketClient.Exists(ctx, blockDeletionMarkFile)
	require.NoError(t, err)
	assert.False(t, exists)

	exists, err = bucketClient.Exists(ctx, debugMetaFile)
	require.NoError(t, err)
	assert.False(t, exists)

	_, err = bucketindex.ReadIndex(ctx, bucketClient, userID, nil, logger)
	require.ErrorIs(t, err, bucketindex.ErrIndexNotFound)
}

func TestBlocksCleaner_ShouldRemovePartialBlocksOutsideDelayPeriod(t *testing.T) {
	bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bucketClient = block.BucketWithGlobalMarkers(bucketClient)

	ts := func(hours int) int64 {
		return time.Now().Add(time.Duration(hours)*time.Hour).Unix() * 1000
	}

	block1 := createTSDBBlock(t, bucketClient, "user-1", ts(-10), ts(-8), 2, nil)
	block2 := createTSDBBlock(t, bucketClient, "user-1", ts(-8), ts(-6), 2, nil)

	cfg := BlocksCleanerConfig{
		DeletionDelay:                 time.Hour,
		CleanupInterval:               time.Minute,
		CleanupConcurrency:            1,
		DeleteBlocksConcurrency:       1,
		GetDeletionMarkersConcurrency: 1,
	}

	ctx := context.Background()
	logger := test.NewTestingLogger(t)
	reg := prometheus.NewPedanticRegistry()
	cfgProvider := newMockConfigProvider()

	cleaner := NewBlocksCleaner(cfg, bucketClient, tsdb.AllUsers, cfgProvider, logger, reg)

	makeBlockPartial := func(user string, blockID ulid.ULID) {
		err := bucketClient.Delete(ctx, path.Join(user, blockID.String(), block.MetaFilename))
		require.NoError(t, err)
	}

	checkBlock(t, "user-1", bucketClient, block1, true, false)
	checkBlock(t, "user-1", bucketClient, block2, true, false)
	makeBlockPartial("user-1", block1)
	checkBlock(t, "user-1", bucketClient, block1, false, false)
	checkBlock(t, "user-1", bucketClient, block2, true, false)

	require.NoError(t, cleaner.cleanUser(ctx, "user-1", logger))

	// check that no blocks were marked for deletion, because deletion delay is set to 0.
	checkBlock(t, "user-1", bucketClient, block1, false, false)
	checkBlock(t, "user-1", bucketClient, block2, true, false)

	// Test that partial block does get marked for deletion
	// The delay time must be very short since these temporary files were just created
	cfgProvider.userPartialBlockDelay["user-1"] = 1 * time.Nanosecond

	require.NoError(t, cleaner.cleanUser(ctx, "user-1", logger))

	// check that first block was marked for deletion (partial block updated far in the past), but not the second one, because it's not partial.
	checkBlock(t, "user-1", bucketClient, block1, false, true)
	checkBlock(t, "user-1", bucketClient, block2, true, false)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
			# TYPE cortex_bucket_blocks_count gauge
			cortex_bucket_blocks_count{user="user-1"} 1
			# HELP cortex_bucket_blocks_marked_for_deletion_count Total number of blocks marked for deletion in the bucket.
			# TYPE cortex_bucket_blocks_marked_for_deletion_count gauge
			cortex_bucket_blocks_marked_for_deletion_count{user="user-1"} 0
			# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
			# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
			cortex_compactor_blocks_marked_for_deletion_total{reason="partial"} 1
			cortex_compactor_blocks_marked_for_deletion_total{reason="retention"} 0
			`),
		"cortex_bucket_blocks_count",
		"cortex_bucket_blocks_marked_for_deletion_count",
		"cortex_compactor_blocks_marked_for_deletion_total",
	))
}

func TestBlocksCleaner_ShouldNotRemovePartialBlocksInsideDelayPeriod(t *testing.T) {
	bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bucketClient = block.BucketWithGlobalMarkers(bucketClient)

	ts := func(hours int) int64 {
		return time.Now().Add(time.Duration(hours)*time.Hour).Unix() * 1000
	}

	block1 := createTSDBBlock(t, bucketClient, "user-1", ts(-10), ts(-8), 2, nil)
	block2 := createTSDBBlock(t, bucketClient, "user-2", ts(-8), ts(-6), 2, nil)

	cfg := BlocksCleanerConfig{
		DeletionDelay:                 time.Hour,
		CleanupInterval:               time.Minute,
		CleanupConcurrency:            1,
		DeleteBlocksConcurrency:       1,
		GetDeletionMarkersConcurrency: 1,
	}

	ctx := context.Background()
	logger := test.NewTestingLogger(t)
	reg := prometheus.NewPedanticRegistry()
	cfgProvider := newMockConfigProvider()

	cleaner := NewBlocksCleaner(cfg, bucketClient, tsdb.AllUsers, cfgProvider, logger, reg)

	makeBlockPartial := func(user string, blockID ulid.ULID) {
		err := bucketClient.Delete(ctx, path.Join(user, blockID.String(), block.MetaFilename))
		require.NoError(t, err)
	}

	corruptMeta := func(user string, blockID ulid.ULID) {
		err := bucketClient.Upload(ctx, path.Join(user, blockID.String(), block.MetaFilename), strings.NewReader("corrupted file contents"))
		require.NoError(t, err)
	}

	checkBlock(t, "user-1", bucketClient, block1, true, false)
	checkBlock(t, "user-2", bucketClient, block2, true, false)

	makeBlockPartial("user-1", block1)
	corruptMeta("user-2", block2)

	checkBlock(t, "user-1", bucketClient, block1, false, false)
	checkBlock(t, "user-2", bucketClient, block2, true, false)

	// Set partial block delay such that block will not be marked for deletion
	// The comparison is based on inode modification time, so anything more than very recent (< 1 second) won't be
	// out of range
	cfgProvider.userPartialBlockDelay["user-1"] = 1 * time.Hour
	cfgProvider.userPartialBlockDelay["user-2"] = 1 * time.Nanosecond

	require.NoError(t, cleaner.cleanUser(ctx, "user-1", logger))
	checkBlock(t, "user-1", bucketClient, block1, false, false) // This block was updated too recently, so we don't mark it for deletion just yet.
	checkBlock(t, "user-2", bucketClient, block2, true, false)  // No change for user-2.

	require.NoError(t, cleaner.cleanUser(ctx, "user-2", logger))
	checkBlock(t, "user-1", bucketClient, block1, false, false) // No change for user-1
	checkBlock(t, "user-2", bucketClient, block2, true, false)  // Block with corrupted meta is NOT marked for deletion.

	// The cortex_compactor_blocks_marked_for_deletion_total{reason="partial"} counter should be zero since for user-1
	// the time since modification is shorter than the delay, and for user-2, the metadata is corrupted but the file
	// is still present in the bucket so the block is not partial
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
			# TYPE cortex_bucket_blocks_count gauge
			cortex_bucket_blocks_count{user="user-1"} 0
			cortex_bucket_blocks_count{user="user-2"} 0
			# HELP cortex_bucket_blocks_marked_for_deletion_count Total number of blocks marked for deletion in the bucket.
			# TYPE cortex_bucket_blocks_marked_for_deletion_count gauge
			cortex_bucket_blocks_marked_for_deletion_count{user="user-1"} 0
			cortex_bucket_blocks_marked_for_deletion_count{user="user-2"} 0
			# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
			# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
			cortex_compactor_blocks_marked_for_deletion_total{reason="partial"} 0
			cortex_compactor_blocks_marked_for_deletion_total{reason="retention"} 0
			`),
		"cortex_bucket_blocks_count",
		"cortex_bucket_blocks_marked_for_deletion_count",
		"cortex_compactor_blocks_marked_for_deletion_total",
	))
}

func TestBlocksCleaner_ShouldNotRemovePartialBlocksIfConfiguredDelayIsInvalid(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bucketClient = block.BucketWithGlobalMarkers(bucketClient)

	ts := func(hours int) int64 {
		return time.Now().Add(time.Duration(hours)*time.Hour).Unix() * 1000
	}

	// Create a partial block.
	block1 := createTSDBBlock(t, bucketClient, "user-1", ts(-10), ts(-8), 2, nil)
	err := bucketClient.Delete(ctx, path.Join("user-1", block1.String(), block.MetaFilename))
	require.NoError(t, err)

	cfg := BlocksCleanerConfig{
		DeletionDelay:                 time.Hour,
		CleanupInterval:               time.Minute,
		CleanupConcurrency:            1,
		DeleteBlocksConcurrency:       1,
		GetDeletionMarkersConcurrency: 1,
	}

	// Configure an invalid delay.
	cfgProvider := newMockConfigProvider()
	cfgProvider.userPartialBlockDelay["user-1"] = 0
	cfgProvider.userPartialBlockDelayInvalid["user-1"] = true

	// Pre-condition check: block should be partial and not being marked for deletion.
	checkBlock(t, "user-1", bucketClient, block1, false, false)

	// Run the cleanup.
	cleaner := NewBlocksCleaner(cfg, bucketClient, tsdb.AllUsers, cfgProvider, logger, reg)
	require.NoError(t, cleaner.cleanUser(ctx, "user-1", logger))

	// Ensure the block has NOT been marked for deletion.
	checkBlock(t, "user-1", bucketClient, block1, false, false)
	assert.Contains(t, logs.String(), "partial blocks deletion has been disabled for tenant because the delay has been set lower than the minimum value allowed")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
			# TYPE cortex_bucket_blocks_count gauge
			cortex_bucket_blocks_count{user="user-1"} 0
			# HELP cortex_bucket_blocks_marked_for_deletion_count Total number of blocks marked for deletion in the bucket.
			# TYPE cortex_bucket_blocks_marked_for_deletion_count gauge
			cortex_bucket_blocks_marked_for_deletion_count{user="user-1"} 0
			# HELP cortex_compactor_blocks_marked_for_deletion_total Total number of blocks marked for deletion in compactor.
			# TYPE cortex_compactor_blocks_marked_for_deletion_total counter
			cortex_compactor_blocks_marked_for_deletion_total{reason="partial"} 0
			cortex_compactor_blocks_marked_for_deletion_total{reason="retention"} 0
			`),
		"cortex_bucket_blocks_count",
		"cortex_bucket_blocks_marked_for_deletion_count",
		"cortex_compactor_blocks_marked_for_deletion_total",
	))
}

func TestStalePartialBlockLastModifiedTime(t *testing.T) {
	b, dir := mimir_testutil.PrepareFilesystemBucket(t)

	const tenant = "user"

	objectTime := time.Now().Add(-1 * time.Hour).Truncate(time.Second) // ignore milliseconds, as not all filesystems store them.
	blockID := createTSDBBlock(t, b, tenant, objectTime.UnixMilli(), time.Now().UnixMilli(), 2, nil)
	for _, f := range []string{"meta.json", "index", "chunks/000001", "tombstones"} {
		require.NoError(t, os.Chtimes(filepath.Join(dir, tenant, blockID.String(), filepath.FromSlash(f)), objectTime, objectTime))
	}

	userBucket := bucket.NewUserBucketClient(tenant, b, nil)

	emptyBlockID := ulid.ULID{}
	require.NotEqual(t, blockID, emptyBlockID)
	empty := true
	err := userBucket.Iter(context.Background(), emptyBlockID.String(), func(_ string) error {
		empty = false
		return nil
	})
	require.NoError(t, err)
	require.True(t, empty)

	testCases := []struct {
		name                 string
		blockID              ulid.ULID
		cutoff               time.Time
		expectedLastModified time.Time
	}{
		{name: "no objects", blockID: emptyBlockID, cutoff: objectTime, expectedLastModified: time.Time{}},
		{name: "objects newer than delay cutoff", blockID: blockID, cutoff: objectTime.Add(-1 * time.Second), expectedLastModified: time.Time{}},
		{name: "objects equal to delay cutoff", blockID: blockID, cutoff: objectTime, expectedLastModified: objectTime},
		{name: "objects older than delay cutoff", blockID: blockID, cutoff: objectTime.Add(1 * time.Second), expectedLastModified: objectTime},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lastModified, err := stalePartialBlockLastModifiedTime(context.Background(), tc.blockID, userBucket, tc.cutoff)
			require.NoError(t, err)
			require.Equal(t, tc.expectedLastModified, lastModified)
		})
	}
}

func TestComputeCompactionJobs(t *testing.T) {
	bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bucketClient = block.BucketWithGlobalMarkers(bucketClient)

	cfg := BlocksCleanerConfig{
		DeletionDelay:                 time.Hour,
		CleanupInterval:               time.Minute,
		CleanupConcurrency:            1,
		DeleteBlocksConcurrency:       1,
		GetDeletionMarkersConcurrency: 1,
		CompactionBlockRanges:         tsdb.DurationList{2 * time.Hour, 24 * time.Hour},
	}

	const user = "test"
	twoHoursMS := 2 * time.Hour.Milliseconds()
	dayMS := 24 * time.Hour.Milliseconds()

	userBucket := bucket.NewUserBucketClient(user, bucketClient, nil)

	// Mark block for no-compaction.
	blockMarkedForNoCompact := ulid.MustNew(ulid.Now(), rand.Reader)
	require.NoError(t, block.MarkForNoCompact(context.Background(), log.NewNopLogger(), userBucket, blockMarkedForNoCompact, block.CriticalNoCompactReason, "testing", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))

	cases := map[string]struct {
		blocks         bucketindex.Blocks
		expectedSplits int
		expectedMerges int
	}{
		"standard": {
			blocks: bucketindex.Blocks{
				// Some 2h blocks that should be compacted together and split.
				&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: 0, MaxTime: twoHoursMS},
				&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: 0, MaxTime: twoHoursMS},
				&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: 0, MaxTime: twoHoursMS},

				// Some merge jobs.
				&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: dayMS, MaxTime: 2 * dayMS, CompactorShardID: "1_of_3"},
				&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: dayMS, MaxTime: 2 * dayMS, CompactorShardID: "1_of_3"},

				&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: dayMS, MaxTime: 2 * dayMS, CompactorShardID: "2_of_3"},
				&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: dayMS, MaxTime: 2 * dayMS, CompactorShardID: "2_of_3"},

				// This merge job is skipped, as block is marked for no-compaction.
				&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: dayMS, MaxTime: 2 * dayMS, CompactorShardID: "3_of_3"},
				&bucketindex.Block{ID: blockMarkedForNoCompact, MinTime: dayMS, MaxTime: 2 * dayMS, CompactorShardID: "3_of_3"},
			},
			expectedSplits: 1,
			expectedMerges: 2,
		},
		"labels don't match": {
			blocks: bucketindex.Blocks{
				// Compactor wouldn't produce a job for this pair as their external labels differ:
				&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: 5 * dayMS, MaxTime: 6 * dayMS,
					Labels: map[string]string{
						tsdb.OutOfOrderExternalLabel: tsdb.OutOfOrderExternalLabelValue,
					},
				},
				&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: 5 * dayMS, MaxTime: 6 * dayMS,
					Labels: map[string]string{
						"another_label": "-1",
					},
				},
			},
			expectedSplits: 0,
			expectedMerges: 0,
		},
		"ignore deprecated labels": {
			blocks: bucketindex.Blocks{
				// Compactor will ignore deprecated labels when computing jobs. Estimation should do the same.
				&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: 5 * dayMS, MaxTime: 6 * dayMS,
					Labels: map[string]string{
						"honored_label":                        "12345",
						tsdb.DeprecatedTenantIDExternalLabel:   "tenant1",
						tsdb.DeprecatedIngesterIDExternalLabel: "ingester1",
					},
				},
				&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: 5 * dayMS, MaxTime: 6 * dayMS,
					Labels: map[string]string{
						"honored_label":                        "12345",
						tsdb.DeprecatedTenantIDExternalLabel:   "tenant2",
						tsdb.DeprecatedIngesterIDExternalLabel: "ingester2",
					},
				},
			},
			expectedSplits: 0,
			expectedMerges: 1,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			index := &bucketindex.Index{Blocks: c.blocks}
			jobs, err := estimateCompactionJobsFromBucketIndex(context.Background(), user, userBucket, index, cfg.CompactionBlockRanges, 3, 0)
			require.NoError(t, err)
			split, merge := computeSplitAndMergeJobs(jobs)
			require.Equal(t, c.expectedSplits, split)
			require.Equal(t, c.expectedMerges, merge)
		})
	}
}

func TestConvertBucketIndexToMetasForCompactionJobPlanning(t *testing.T) {
	twoHoursMS := 2 * time.Hour.Milliseconds()

	makeUlid := func(n byte) ulid.ULID {
		return ulid.ULID{n}
	}

	makeMeta := func(id ulid.ULID, labels map[string]string) *block.Meta {
		return &block.Meta{
			BlockMeta: prom_tsdb.BlockMeta{
				ULID:    id,
				MinTime: 0,
				MaxTime: twoHoursMS,
				Version: block.TSDBVersion1,
			},
			Thanos: block.ThanosMeta{
				Version: block.ThanosVersion1,
				Labels:  labels,
			},
		}
	}

	cases := map[string]struct {
		index         *bucketindex.Index
		expectedMetas map[ulid.ULID]*block.Meta
	}{
		"empty": {
			index:         &bucketindex.Index{Blocks: bucketindex.Blocks{}},
			expectedMetas: map[ulid.ULID]*block.Meta{},
		},
		"basic": {
			index: &bucketindex.Index{
				Blocks: bucketindex.Blocks{
					&bucketindex.Block{ID: makeUlid(1), MinTime: 0, MaxTime: twoHoursMS},
				},
			},
			expectedMetas: map[ulid.ULID]*block.Meta{
				makeUlid(1): makeMeta(makeUlid(1), map[string]string{}),
			},
		},
		"adopt shard ID": {
			index: &bucketindex.Index{
				Blocks: bucketindex.Blocks{
					&bucketindex.Block{ID: makeUlid(1), MinTime: 0, MaxTime: twoHoursMS, CompactorShardID: "78"},
				},
			},
			expectedMetas: map[ulid.ULID]*block.Meta{
				makeUlid(1): makeMeta(makeUlid(1), map[string]string{tsdb.CompactorShardIDExternalLabel: "78"}),
			},
		},
		"use labeled shard ID": {
			index: &bucketindex.Index{
				Blocks: bucketindex.Blocks{
					&bucketindex.Block{ID: makeUlid(1), MinTime: 0, MaxTime: twoHoursMS,
						Labels: map[string]string{tsdb.CompactorShardIDExternalLabel: "3"}},
				},
			},
			expectedMetas: map[ulid.ULID]*block.Meta{
				makeUlid(1): makeMeta(makeUlid(1), map[string]string{tsdb.CompactorShardIDExternalLabel: "3"}),
			},
		},
		"don't overwrite labeled shard ID": {
			index: &bucketindex.Index{
				Blocks: bucketindex.Blocks{
					&bucketindex.Block{ID: makeUlid(1), MinTime: 0, MaxTime: twoHoursMS, CompactorShardID: "78",
						Labels: map[string]string{tsdb.CompactorShardIDExternalLabel: "3"}},
				},
			},
			expectedMetas: map[ulid.ULID]*block.Meta{
				makeUlid(1): makeMeta(makeUlid(1), map[string]string{tsdb.CompactorShardIDExternalLabel: "3"}),
			},
		},
		"honor deletion marks": {
			index: &bucketindex.Index{
				BlockDeletionMarks: bucketindex.BlockDeletionMarks{
					&bucketindex.BlockDeletionMark{ID: makeUlid(14)},
				},
				Blocks: bucketindex.Blocks{
					&bucketindex.Block{ID: makeUlid(14), MinTime: 0, MaxTime: twoHoursMS},
				},
			},
			expectedMetas: map[ulid.ULID]*block.Meta{},
		},
		"excess deletes": {
			index: &bucketindex.Index{
				BlockDeletionMarks: bucketindex.BlockDeletionMarks{
					&bucketindex.BlockDeletionMark{ID: makeUlid(15)},
					&bucketindex.BlockDeletionMark{ID: makeUlid(16)},
				},
				Blocks: bucketindex.Blocks{
					&bucketindex.Block{ID: makeUlid(14), MinTime: 0, MaxTime: twoHoursMS},
				},
			},
			expectedMetas: map[ulid.ULID]*block.Meta{
				makeUlid(14): makeMeta(makeUlid(14), map[string]string{}),
			},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			m := ConvertBucketIndexToMetasForCompactionJobPlanning(c.index)
			require.Equal(t, c.expectedMetas, m)
		})
	}
}

// This test reproduces a race condition we've observed in production where there may be two
// compactor replicas running the blocks cleaner for the same tenant at the same time (e.g.
// during a scale up, or when a compactor is restarted and temporarily leaves the ring during
// the restart).
//
// See: https://github.com/grafana/mimir/issues/8687
func TestBlocksCleaner_RaceCondition_CleanerUpdatesBucketIndexWhileAnotherCleanerDeletesBlocks(t *testing.T) {
	const (
		tenantID      = "user-1"
		deletionDelay = time.Hour
	)

	var (
		ctx         = context.Background()
		logger      = log.NewNopLogger()
		cfgProvider = newMockConfigProvider()
		cfg         = BlocksCleanerConfig{
			DeletionDelay:                 deletionDelay,
			CleanupInterval:               time.Minute,
			CleanupConcurrency:            1,
			DeleteBlocksConcurrency:       1,
			GetDeletionMarkersConcurrency: 1,
			NoBlocksFileCleanupEnabled:    true,
		}
	)

	tests := map[string]struct {
		mockBucketClients func(bucket2 objstore.Bucket) (cleaner1BucketClient, cleaner2BucketClient objstore.Bucket)
	}{
		"the 2nd cleaner lists markers after the 1st cleaner has started deleting some blocks": {
			mockBucketClients: func(bucketClient objstore.Bucket) (cleaner1BucketClient, cleaner2BucketClient objstore.Bucket) {
				cleaner2ListMarkersStarted := make(chan struct{})
				cleaner2ListMarkersUnblocked := make(chan struct{})

				cleaner1BucketClient = &hookBucket{
					Bucket: bucketClient,
					preIterHook: func(_ context.Context, dir string, _ ...objstore.IterOption) {
						// When listing blocks, wait until the 2nd cleaner started to list markers.
						if path.Base(dir) == tenantID {
							<-cleaner2ListMarkersStarted
						}
					},
					postDeleteHook: func() func(context.Context, string) {
						once := sync.Once{}

						return func(_ context.Context, name string) {
							// After deleting the deletion mark of a block (which is expected to be the last object deleted of a block)
							// unblock the 2nd cleaner markers listing.
							if path.Base(name) == block.DeletionMarkFilename {
								once.Do(func() {
									close(cleaner2ListMarkersUnblocked)
								})
							}
						}
					}(),
				}

				cleaner2BucketClient = &hookBucket{
					Bucket: bucketClient,
					preIterHook: func() func(context.Context, string, ...objstore.IterOption) {
						once := sync.Once{}

						return func(_ context.Context, dir string, _ ...objstore.IterOption) {
							if path.Base(dir) == block.MarkersPathname {
								// Before listing markers, signal that the 2nd cleaner started to list markers and
								// then wait until it should proceed.
								once.Do(func() {
									close(cleaner2ListMarkersStarted)
									<-cleaner2ListMarkersUnblocked
								})
							}
						}
					}(),
				}

				return
			},
		},
		"the 2nd cleaner fetches new deletion marks after the 1st cleaner has started deleting blocks for the new deletion marks": {
			mockBucketClients: func(bucketClient objstore.Bucket) (cleaner1BucketClient, cleaner2BucketClient objstore.Bucket) {
				cleaner2ListMarkersStarted := make(chan struct{})
				cleaner2GetDeletionMarkUnblocked := make(chan struct{})

				// Mock the 1st cleaner to delete deletion marks when the 2nd cleaner is between the listing
				// of deletion marks and fetching their content (for new markers).
				cleaner1BucketClient = &hookBucket{
					Bucket: bucketClient,
					preDeleteHook: func(_ context.Context, name string) {
						if path.Base(name) == block.DeletionMarkFilename {
							// Before deleting the deletion mark of a block, wait until cleaner2 has listed the deletion marks,
							// because we want cleaner2 to discover the deletion mark before we delete it.
							<-cleaner2ListMarkersStarted
						}
					},
					postDeleteHook: func() func(context.Context, string) {
						once := sync.Once{}

						return func(_ context.Context, name string) {
							if path.Base(name) == block.DeletionMarkFilename {
								// After deleting the deletion mark of a block, signal it to the 2nd cleaner.
								once.Do(func() {
									go func() {
										close(cleaner2GetDeletionMarkUnblocked)
									}()
								})
							}
						}
					}(),
				}

				// Mock the 2nd cleaner to discover deletion marks *before* 1st cleaner deletes the blocks,
				// and to fetch the deletion markers after the 1st cleaner has deleted the blocks.
				cleaner2BucketClient = &hookBucket{
					Bucket: bucketClient,
					postIterHook: func() func(context.Context, string, ...objstore.IterOption) {
						once := sync.Once{}

						return func(_ context.Context, dir string, _ ...objstore.IterOption) {
							// Signal when cleaner1 has listed markers.
							if path.Base(dir) == block.MarkersPathname {
								once.Do(func() {
									go func() {
										close(cleaner2ListMarkersStarted)
									}()
								})
							}
						}
					}(),
					preGetHook: func(_ context.Context, name string) {
						// When fetching the deletion mark of a block, wait until cleaner2 has deleted it.
						if path.Base(name) == block.DeletionMarkFilename {
							<-cleaner2GetDeletionMarkUnblocked
						}
					},
				}

				return
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
			bucketClient = block.BucketWithGlobalMarkers(bucketClient)

			// Create two blocks and mark one of them for deletion at a time before the deletion delay.
			block1 := createTSDBBlock(t, bucketClient, tenantID, 10, 20, 1, nil)
			block2 := createTSDBBlock(t, bucketClient, tenantID, 20, 30, 1, nil)
			createDeletionMark(t, bucketClient, tenantID, block1, time.Now().Add(-deletionDelay).Add(-time.Minute))

			cleaner1BucketClient, cleaner2BucketClient := testData.mockBucketClients(bucketClient)
			cleaner1 := NewBlocksCleaner(cfg, cleaner1BucketClient, tsdb.AllUsers, cfgProvider, logger, nil)
			cleaner2 := NewBlocksCleaner(cfg, cleaner2BucketClient, tsdb.AllUsers, cfgProvider, logger, nil)

			// Run both cleaners concurrently.
			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				defer wg.Done()
				require.NoError(t, cleaner1.cleanUser(ctx, tenantID, logger))
			}()

			go func() {
				defer wg.Done()
				require.NoError(t, cleaner2.cleanUser(ctx, tenantID, logger))
			}()

			// Wait until both cleaners have done.
			wg.Wait()

			// Check the updated bucket index.
			idx, err := bucketindex.ReadIndex(ctx, bucketClient, tenantID, cfgProvider, logger)
			require.NoError(t, err)

			// The non-deleted block must be in the index.
			assert.Contains(t, idx.Blocks.GetULIDs(), block2)

			// The block marked for deletion should be deleted. Due to race, it could either be removed from the list of blocks
			// or it could still be listed among the list of blocks but, if so, the deletion marker should still be in the index.
			if slices.Contains(idx.Blocks.GetULIDs(), block1) {
				assert.Contains(t, idx.BlockDeletionMarks.GetULIDs(), block1)
			} else {
				assert.NotContains(t, idx.BlockDeletionMarks.GetULIDs(), block1)
			}
		})
	}
}

type hookBucket struct {
	objstore.Bucket

	preIterHook    func(ctx context.Context, dir string, options ...objstore.IterOption)
	postIterHook   func(ctx context.Context, dir string, options ...objstore.IterOption)
	preGetHook     func(ctx context.Context, name string)
	postGetHook    func(ctx context.Context, name string)
	preDeleteHook  func(ctx context.Context, name string)
	postDeleteHook func(ctx context.Context, name string)
}

func (b *hookBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if b.postIterHook != nil {
		defer b.postIterHook(ctx, dir, options...)
	}

	if b.preIterHook != nil {
		b.preIterHook(ctx, dir, options...)
	}

	return b.Bucket.Iter(ctx, dir, f, options...)
}

func (b *hookBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.postGetHook != nil {
		defer b.postGetHook(ctx, name)
	}

	if b.preGetHook != nil {
		b.preGetHook(ctx, name)
	}

	return b.Bucket.Get(ctx, name)
}

func (b *hookBucket) Delete(ctx context.Context, name string) error {
	if b.postDeleteHook != nil {
		defer b.postDeleteHook(ctx, name)
	}

	if b.preDeleteHook != nil {
		b.preDeleteHook(ctx, name)
	}

	return b.Bucket.Delete(ctx, name)
}

type mockBucketFailure struct {
	objstore.Bucket

	DeleteFailures []string
}

func (m *mockBucketFailure) Delete(ctx context.Context, name string) error {
	if util.StringsContain(m.DeleteFailures, name) {
		return errors.New("mocked delete failure")
	}
	return m.Bucket.Delete(ctx, name)
}

type mockConfigProvider struct {
	userRetentionPeriods         map[string]time.Duration
	splitAndMergeShards          map[string]int
	instancesShardSize           map[string]int
	splitGroups                  map[string]int
	blockUploadEnabled           map[string]bool
	blockUploadValidationEnabled map[string]bool
	blockUploadMaxBlockSizeBytes map[string]int64
	userPartialBlockDelay        map[string]time.Duration
	userPartialBlockDelayInvalid map[string]bool
	verifyChunks                 map[string]bool
	perTenantInMemoryCache       map[string]int
}

func newMockConfigProvider() *mockConfigProvider {
	return &mockConfigProvider{
		userRetentionPeriods:         make(map[string]time.Duration),
		splitAndMergeShards:          make(map[string]int),
		splitGroups:                  make(map[string]int),
		blockUploadEnabled:           make(map[string]bool),
		blockUploadValidationEnabled: make(map[string]bool),
		blockUploadMaxBlockSizeBytes: make(map[string]int64),
		userPartialBlockDelay:        make(map[string]time.Duration),
		userPartialBlockDelayInvalid: make(map[string]bool),
		verifyChunks:                 make(map[string]bool),
		perTenantInMemoryCache:       make(map[string]int),
	}
}

func (m *mockConfigProvider) CompactorBlocksRetentionPeriod(user string) time.Duration {
	if result, ok := m.userRetentionPeriods[user]; ok {
		return result
	}
	return 0
}

func (m *mockConfigProvider) CompactorSplitAndMergeShards(user string) int {
	if result, ok := m.splitAndMergeShards[user]; ok {
		return result
	}
	return 0
}

func (m *mockConfigProvider) CompactorSplitGroups(user string) int {
	if result, ok := m.splitGroups[user]; ok {
		return result
	}
	return 0
}

func (m *mockConfigProvider) CompactorTenantShardSize(user string) int {
	if result, ok := m.instancesShardSize[user]; ok {
		return result
	}
	return 0
}

func (m *mockConfigProvider) CompactorBlockUploadEnabled(tenantID string) bool {
	return m.blockUploadEnabled[tenantID]
}

func (m *mockConfigProvider) CompactorBlockUploadValidationEnabled(tenantID string) bool {
	return m.blockUploadValidationEnabled[tenantID]
}

func (m *mockConfigProvider) CompactorPartialBlockDeletionDelay(user string) (time.Duration, bool) {
	return m.userPartialBlockDelay[user], !m.userPartialBlockDelayInvalid[user]
}

func (m *mockConfigProvider) CompactorBlockUploadVerifyChunks(tenantID string) bool {
	return m.verifyChunks[tenantID]
}

func (m *mockConfigProvider) CompactorBlockUploadMaxBlockSizeBytes(user string) int64 {
	return m.blockUploadMaxBlockSizeBytes[user]
}

func (m *mockConfigProvider) CompactorInMemoryTenantMetaCacheSize(userID string) int {
	return m.perTenantInMemoryCache[userID]
}

func (m *mockConfigProvider) S3SSEType(string) string {
	return ""
}

func (m *mockConfigProvider) S3SSEKMSKeyID(string) string {
	return ""
}

func (m *mockConfigProvider) S3SSEKMSEncryptionContext(string) string {
	return ""
}

func (c *BlocksCleaner) runCleanupWithErr(ctx context.Context) error {
	allUsers, isDeleted, err := c.refreshOwnedUsers(ctx)
	if err != nil {
		return err
	}

	return c.cleanUsers(ctx, allUsers, isDeleted, log.NewNopLogger())
}
