// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"maps"
	"path"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	mimir_testutil "github.com/grafana/mimir/pkg/storage/tsdb/testutil"
)

func TestUnionOfBucketMetas(t *testing.T) {
	inBothBuckets, inFirstBucket, inSecondBucket := ulid.MustNew(1, nil), ulid.MustNew(2, nil), ulid.MustNew(3, nil)
	firstBucketMeta := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: inBothBuckets, MinTime: 1}}
	secondBucketMeta := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: inBothBuckets, MinTime: 2}}
	firstBucketOnlyMeta := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: inFirstBucket}}
	secondBucketOnlyMeta := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: inSecondBucket}}

	metas, bucketIndexes := unionOfBucketMetas([]map[ulid.ULID]*block.Meta{
		{inBothBuckets: firstBucketMeta, inFirstBucket: firstBucketOnlyMeta},
		{inBothBuckets: secondBucketMeta, inSecondBucket: secondBucketOnlyMeta},
	})

	require.Equal(t, map[ulid.ULID]*block.Meta{
		inBothBuckets:  firstBucketMeta,
		inFirstBucket:  firstBucketOnlyMeta,
		inSecondBucket: secondBucketOnlyMeta,
	}, metas)
	require.Equal(t, map[ulid.ULID]int{inSecondBucket: 1}, bucketIndexes)
}

func TestMultiBucketMetaFetcher_FetchWithoutMarkedForDeletion(t *testing.T) {
	const userID = "user-1"
	ctx := context.Background()

	writeBucket, _ := mimir_testutil.PrepareFilesystemBucket(t)
	writeBucket = block.BucketWithGlobalMarkers(writeBucket)
	readBucket, _ := mimir_testutil.PrepareFilesystemBucket(t)
	readBucket = block.BucketWithGlobalMarkers(readBucket)
	userWriteBucket := bucket.NewUserBucketClient(userID, writeBucket, nil)
	userReadBucket := bucket.NewUserBucketClient(userID, readBucket, nil)

	inWriteBucket := createTSDBBlock(t, writeBucket, userID, 10, 20, 2, nil)
	inReadBucket := createTSDBBlock(t, readBucket, userID, 20, 30, 2, nil)
	inBothBuckets := createTSDBBlock(t, writeBucket, userID, 30, 40, 2, nil)
	copyBlockMeta(t, userWriteBucket, userReadBucket, inBothBuckets)

	markedForDeletionInReadBucket := createTSDBBlock(t, readBucket, userID, 40, 50, 2, nil)
	createDeletionMark(t, readBucket, userID, markedForDeletionInReadBucket, time.Now())

	markedForNoCompactInReadBucket := createTSDBBlock(t, readBucket, userID, 50, 60, 2, nil)
	require.NoError(t, block.MarkForNoCompact(ctx, log.NewNopLogger(), userReadBucket, markedForNoCompactInReadBucket, block.ManualNoCompactReason, ""))

	// The no-compact mark of the write bucket doesn't apply to the block in the read bucket.
	require.NoError(t, block.MarkForNoCompact(ctx, log.NewNopLogger(), userWriteBucket, inReadBucket, block.ManualNoCompactReason, ""))

	buckets := newBlockBuckets(userWriteBucket, userReadBucket)
	fetcher, err := newMultiBucketMetaFetcher(log.NewNopLogger(), 1, buckets, []string{"write", "read"}, t.TempDir(), prometheus.NewPedanticRegistry(), NewShardAwareDeduplicateFilter(), 0)
	require.NoError(t, err)

	metas, partials, err := fetcher.FetchWithoutMarkedForDeletion(ctx)
	require.NoError(t, err)
	require.Empty(t, partials)
	require.ElementsMatch(t, []ulid.ULID{inWriteBucket, inReadBucket, inBothBuckets}, slices.Collect(maps.Keys(metas)))

	require.True(t, buckets.of(inWriteBucket) == userWriteBucket)
	require.True(t, buckets.of(inReadBucket) == userReadBucket)
	require.True(t, buckets.of(inBothBuckets) == userWriteBucket)
}

func copyBlockMeta(t *testing.T, from, to objstore.Bucket, blockID ulid.ULID) {
	metaPath := path.Join(blockID.String(), block.MetaFilename)
	r, err := from.Get(context.Background(), metaPath)
	require.NoError(t, err)
	defer r.Close()
	require.NoError(t, to.Upload(context.Background(), metaPath, r))
}
