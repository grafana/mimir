// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketcache"
)

func TestBatchCachingMetaFetcher_FetchCompactableMetasFromListing(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	tenant := "tenant"

	block1Meta := createBlockMeta(t, now.Add(-1*time.Hour), now)
	block2Meta := createBlockMeta(t, now.Add(-2*time.Hour), now.Add(-1*time.Hour))
	block3Meta := createBlockMeta(t, now.Add(-3*time.Hour), now.Add(-2*time.Hour))

	t.Run("all blocks found in cache", func(t *testing.T) {
		fetcher, bkt, c, metrics := newTestFetcher(t, tenant)

		uploadBlockMeta(t, bkt, block1Meta)
		uploadBlockMeta(t, bkt, block2Meta)
		uploadToCache(t, c, tenant, block1Meta)
		uploadToCache(t, c, tenant, block2Meta)

		metas, err := fetcher.fetchCompactableMetasFromListing(ctx, 0, nil, metrics)

		require.NoError(t, err)
		assert.Len(t, metas, 2)
		assert.Contains(t, metas, block1Meta.ULID)
		assert.Contains(t, metas, block2Meta.ULID)

		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Syncs))
		assert.Equal(t, float64(2), testutil.ToFloat64(metrics.Loads))
		assert.Equal(t, float64(2), testutil.ToFloat64(metrics.CachedLoads))
		assert.Equal(t, float64(2), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.LoadedMeta)))
	})

	t.Run("all blocks missed in cache", func(t *testing.T) {
		fetcher, bkt, c, metrics := newTestFetcher(t, tenant)

		uploadBlockMeta(t, bkt, block1Meta)
		uploadBlockMeta(t, bkt, block2Meta)

		metas, err := fetcher.fetchCompactableMetasFromListing(ctx, 0, nil, metrics)

		require.NoError(t, err)
		assert.Len(t, metas, 2)
		assert.Contains(t, metas, block1Meta.ULID)
		assert.Contains(t, metas, block2Meta.ULID)

		assert.Equal(t, float64(2), testutil.ToFloat64(metrics.Loads))
		assert.Equal(t, float64(0), testutil.ToFloat64(metrics.CachedLoads))
		assert.Equal(t, float64(2), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.LoadedMeta)))

		// Verify cache population
		key1 := tenantMetaCacheKey(tenant, block1Meta.ULID)
		key2 := tenantMetaCacheKey(tenant, block2Meta.ULID)
		cached := c.GetMulti(ctx, []string{key1, key2})
		require.Len(t, cached, 2)
	})

	t.Run("partial cache hits", func(t *testing.T) {
		fetcher, bkt, c, metrics := newTestFetcher(t, tenant)

		uploadBlockMeta(t, bkt, block1Meta)
		uploadToCache(t, c, tenant, block1Meta)
		uploadBlockMeta(t, bkt, block2Meta)

		metas, err := fetcher.fetchCompactableMetasFromListing(ctx, 0, nil, metrics)

		require.NoError(t, err)
		assert.Len(t, metas, 2)
		assert.Contains(t, metas, block1Meta.ULID)
		assert.Contains(t, metas, block2Meta.ULID)

		assert.Equal(t, float64(2), testutil.ToFloat64(metrics.Loads))
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.CachedLoads))
		assert.Equal(t, float64(2), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.LoadedMeta)))
	})

	t.Run("ULID time filtering skips old blocks", func(t *testing.T) {
		fetcher, bkt, _, metrics := newTestFetcher(t, tenant)

		uploadBlockMeta(t, bkt, block1Meta) // 1 hour old
		uploadBlockMeta(t, bkt, block3Meta) // 3 hours old

		metas, err := fetcher.fetchCompactableMetasFromListing(ctx, 2*time.Hour, nil, metrics)

		require.NoError(t, err)
		assert.Len(t, metas, 1)
		assert.Contains(t, metas, block1Meta.ULID)
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.LookbackExcludedMeta)))
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.LoadedMeta)))
	})

	t.Run("deletion markers filter out blocks", func(t *testing.T) {
		fetcher, bkt, _, metrics := newTestFetcher(t, tenant)

		uploadBlockMeta(t, bkt, block1Meta)
		uploadBlockMeta(t, bkt, block2Meta)
		uploadDeletionMark(t, bkt, block2Meta.ULID)

		metas, err := fetcher.fetchCompactableMetasFromListing(ctx, 0, nil, metrics)

		require.NoError(t, err)
		assert.Len(t, metas, 1)
		assert.Contains(t, metas, block1Meta.ULID)
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.MarkedForDeletionMeta)))
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.LoadedMeta)))
	})

	t.Run("no-compaction markers filter out blocks", func(t *testing.T) {
		fetcher, bkt, _, metrics := newTestFetcher(t, tenant)

		uploadBlockMeta(t, bkt, block1Meta)
		uploadBlockMeta(t, bkt, block2Meta)
		uploadNoCompactMark(t, bkt, block2Meta.ULID)

		metas, err := fetcher.fetchCompactableMetasFromListing(ctx, 0, nil, metrics)

		require.NoError(t, err)
		assert.Len(t, metas, 1)
		assert.Contains(t, metas, block1Meta.ULID)
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.MarkedForNoCompactionMeta)))
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.LoadedMeta)))
	})

	t.Run("corrupted cache entry triggers re-fetch", func(t *testing.T) {
		fetcher, bkt, c, metrics := newTestFetcher(t, tenant)

		uploadBlockMeta(t, bkt, block1Meta)

		// Load cache with corrupted data.
		key := tenantMetaCacheKey(tenant, block1Meta.ULID)
		c.SetAsync(key, []byte("not valid json"), 24*time.Hour)

		metas, err := fetcher.fetchCompactableMetasFromListing(ctx, 0, nil, metrics)

		require.NoError(t, err)
		assert.Len(t, metas, 1)
		assert.Contains(t, metas, block1Meta.ULID)
	})

	t.Run("ULID mismatch in cache triggers re-fetch", func(t *testing.T) {
		fetcher, bkt, c, metrics := newTestFetcher(t, tenant)

		uploadBlockMeta(t, bkt, block1Meta)

		// Store block2's metadata under block1's cache key to simulate a collision
		data, err := json.Marshal(block2Meta)
		require.NoError(t, err)
		key := tenantMetaCacheKey(tenant, block1Meta.ULID)
		require.NoError(t, c.Set(ctx, key, data, 24*time.Hour))

		metas, fetchErr := fetcher.fetchCompactableMetasFromListing(ctx, 0, nil, metrics)

		require.NoError(t, fetchErr)
		assert.Len(t, metas, 1)
		// The correctly fetched meta from storage should be returned, not the mismatched cached one.
		assert.Contains(t, metas, block1Meta.ULID)
		assert.NotContains(t, metas, block2Meta.ULID)
	})

	t.Run("incomplete block skipped", func(t *testing.T) {
		fetcher, bkt, _, metrics := newTestFetcher(t, tenant)

		// Upload block1 normally, but block2 has no meta.json
		uploadBlockMeta(t, bkt, block1Meta)
		require.NoError(t, bkt.Upload(ctx, path.Join(block2Meta.ULID.String(), "index"), bytes.NewReader([]byte{})))

		metas, err := fetcher.fetchCompactableMetasFromListing(ctx, 0, nil, metrics)

		require.NoError(t, err)
		assert.Len(t, metas, 1)
		assert.Contains(t, metas, block1Meta.ULID)
		assert.Equal(t, float64(0), testutil.ToFloat64(metrics.CachedLoads))
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.NoMeta))) // from block2
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.LoadedMeta)))
	})

	t.Run("corrupted meta.json in storage is counted and skipped", func(t *testing.T) {
		fetcher, bkt, _, metrics := newTestFetcher(t, tenant)

		uploadBlockMeta(t, bkt, block1Meta)
		// Upload invalid JSON as block2's meta.json directly to object storage.
		require.NoError(t, bkt.Upload(ctx, path.Join(block2Meta.ULID.String(), block.MetaFilename), bytes.NewReader([]byte("not valid json"))))

		metas, err := fetcher.fetchCompactableMetasFromListing(ctx, 0, nil, metrics)

		require.NoError(t, err)
		assert.Len(t, metas, 1)
		assert.Contains(t, metas, block1Meta.ULID)
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.CorruptedMeta)))
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.LoadedMeta)))
	})

	t.Run("iter error increments sync failures", func(t *testing.T) {
		inner := objstore.NewInMemBucket()
		errBkt := &bucket.ErrorInjectedBucketClient{
			Bucket:   inner,
			Injector: bucket.InjectErrorOn(bucket.OpIter, "", errors.New("injected iter error")),
		}
		c := cache.NewMockCache()
		metrics := block.NewFetcherMetrics(prometheus.NewPedanticRegistry(), nil)

		fetcher := newBatchCachingMetaFetcher(errBkt, c, log.NewNopLogger(), tenant, 2, 24*time.Hour)
		_, err := fetcher.fetchCompactableMetasFromListing(ctx, 0, nil, metrics)

		require.Error(t, err)
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Syncs))
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.SyncFailures))
	})

	t.Run("get error increments failed sync and meta", func(t *testing.T) {
		inner := objstore.NewInMemBucket()
		uploadBlockMeta(t, inner, block1Meta)
		errBkt := &bucket.ErrorInjectedBucketClient{
			Bucket:   inner,
			Injector: bucket.InjectErrorOn(bucket.OpGet, path.Join(block1Meta.ULID.String(), block.MetaFilename), errors.New("injected get error")),
		}
		metrics := block.NewFetcherMetrics(prometheus.NewPedanticRegistry(), nil)
		fetcher := newBatchCachingMetaFetcher(errBkt, nil, log.NewNopLogger(), tenant, 2, 24*time.Hour)

		_, err := fetcher.fetchCompactableMetasFromListing(ctx, 0, nil, metrics)
		require.Error(t, err)

		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Syncs))
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.SyncFailures))
		assert.Equal(t, float64(1), testutil.ToFloat64(metrics.Synced.WithLabelValues(block.FailedMeta)))
	})

	t.Run("empty block listing", func(t *testing.T) {
		fetcher, _, _, metrics := newTestFetcher(t, tenant)

		metas, err := fetcher.fetchCompactableMetasFromListing(ctx, 0, nil, metrics)

		require.NoError(t, err)
		assert.Empty(t, metas)
	})

	t.Run("nil cache falls back to storage", func(t *testing.T) {
		bkt := objstore.NewInMemBucket()
		metrics := block.NewFetcherMetrics(prometheus.NewPedanticRegistry(), nil)
		fetcher := newBatchCachingMetaFetcher(bkt, nil, log.NewNopLogger(), tenant, 2, 24*time.Hour)

		uploadBlockMeta(t, bkt, block1Meta)

		metas, err := fetcher.fetchCompactableMetasFromListing(ctx, 0, nil, metrics)

		require.NoError(t, err)
		assert.Len(t, metas, 1)
		assert.Contains(t, metas, block1Meta.ULID)
	})

	t.Run("metadata filter removes matching blocks", func(t *testing.T) {
		fetcher, bkt, _, metrics := newTestFetcher(t, tenant)

		uploadBlockMeta(t, bkt, block1Meta)
		uploadBlockMeta(t, bkt, block2Meta)

		// Filter out block2
		filter := &removeBlockFilter{id: block2Meta.ULID}
		metas, err := fetcher.fetchCompactableMetasFromListing(ctx, 0, []block.MetadataFilter{filter}, metrics)

		require.NoError(t, err)
		assert.Len(t, metas, 1)
		assert.Contains(t, metas, block1Meta.ULID)
		assert.NotContains(t, metas, block2Meta.ULID)
	})

}

func TestBatchCachingMetaFetcher_FetchMetasFromIDs(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	tenant := "tenant"

	block1Meta := createBlockMeta(t, now.Add(-1*time.Hour), now)
	block2Meta := createBlockMeta(t, now.Add(-2*time.Hour), now.Add(-1*time.Hour))
	block3Meta := createBlockMeta(t, now.Add(-3*time.Hour), now.Add(-2*time.Hour))

	t.Run("returns metas only for specified IDs", func(t *testing.T) {
		fetcher, bkt, _, _ := newTestFetcher(t, tenant)

		uploadBlockMeta(t, bkt, block1Meta)
		uploadBlockMeta(t, bkt, block2Meta)
		uploadBlockMeta(t, bkt, block3Meta)

		metas, err := fetcher.fetchMetasFromIDs(ctx, []ulid.ULID{block1Meta.ULID, block2Meta.ULID}, nil)

		require.NoError(t, err)
		assert.Len(t, metas, 2)
		assert.Contains(t, metas, block1Meta.ULID)
		assert.Contains(t, metas, block2Meta.ULID)
		assert.NotContains(t, metas, block3Meta.ULID)
	})

	t.Run("returns error when meta is not found in storage", func(t *testing.T) {
		fetcher, bkt, _, _ := newTestFetcher(t, tenant)

		uploadBlockMeta(t, bkt, block1Meta)
		// block2 prefix exists but has no meta.json.
		require.NoError(t, bkt.Upload(ctx, path.Join(block2Meta.ULID.String(), "index"), bytes.NewReader([]byte{})))

		_, err := fetcher.fetchMetasFromIDs(ctx, []ulid.ULID{block1Meta.ULID, block2Meta.ULID}, nil)

		require.Error(t, err)
		assert.ErrorIs(t, err, block.ErrorSyncMetaNotFound)
		assert.Contains(t, err.Error(), block2Meta.ULID.String())
	})

	t.Run("returns error when meta is corrupt in storage", func(t *testing.T) {
		fetcher, bkt, _, _ := newTestFetcher(t, tenant)

		uploadBlockMeta(t, bkt, block1Meta)
		require.NoError(t, bkt.Upload(ctx, path.Join(block2Meta.ULID.String(), block.MetaFilename), bytes.NewReader([]byte("not valid json"))))

		_, err := fetcher.fetchMetasFromIDs(ctx, []ulid.ULID{block1Meta.ULID, block2Meta.ULID}, nil)

		require.Error(t, err)
		require.ErrorIs(t, err, block.ErrorSyncMetaCorrupted)
	})

	t.Run("returns cached meta without storage fetch", func(t *testing.T) {
		fetcher, _, c, _ := newTestFetcher(t, tenant)

		// Block is only in cache
		uploadToCache(t, c, tenant, block1Meta)

		metas, err := fetcher.fetchMetasFromIDs(ctx, []ulid.ULID{block1Meta.ULID}, nil)

		require.NoError(t, err)
		assert.Len(t, metas, 1)
		assert.Contains(t, metas, block1Meta.ULID)
	})

	t.Run("nil cache falls back to storage", func(t *testing.T) {
		bkt := objstore.NewInMemBucket()
		fetcher := newBatchCachingMetaFetcher(bkt, nil, log.NewNopLogger(), tenant, 2, 24*time.Hour)

		uploadBlockMeta(t, bkt, block1Meta)

		metas, err := fetcher.fetchMetasFromIDs(ctx, []ulid.ULID{block1Meta.ULID}, nil)

		require.NoError(t, err)
		assert.Len(t, metas, 1)
		assert.Contains(t, metas, block1Meta.ULID)
	})
}

func newTestFetcher(t *testing.T, tenant string) (*batchCachingMetaFetcher, *objstore.InMemBucket, *cache.MockCache, *block.FetcherMetrics) {
	t.Helper()
	bkt := objstore.NewInMemBucket()
	c := cache.NewMockCache()
	metrics := block.NewFetcherMetrics(prometheus.NewPedanticRegistry(), nil)
	return newBatchCachingMetaFetcher(bkt, c, log.NewNopLogger(), tenant, 2, 24*time.Hour), bkt, c, metrics
}

// createBlockMeta creates a block.Meta with a ULID derived from minTime
func createBlockMeta(t *testing.T, minTime, maxTime time.Time) *block.Meta {
	t.Helper()
	id, err := ulid.New(ulid.Timestamp(minTime), nil)
	require.NoError(t, err)
	return &block.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id,
			MinTime: minTime.UnixMilli(),
			MaxTime: maxTime.UnixMilli(),
			Version: block.TSDBVersion1,
		},
	}
}

func uploadBlockMeta(t *testing.T, bkt objstore.Bucket, meta *block.Meta) {
	t.Helper()
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	require.NoError(t, bkt.Upload(context.Background(), path.Join(meta.ULID.String(), block.MetaFilename), bytes.NewReader(data)))
}

func uploadNoCompactMark(t *testing.T, bkt objstore.Bucket, id ulid.ULID) {
	t.Helper()
	mark := block.NoCompactMark{
		ID:            id,
		NoCompactTime: time.Now().Unix(),
		Version:       block.NoCompactMarkVersion1,
		Reason:        block.ManualNoCompactReason,
	}
	data, err := json.Marshal(mark)
	require.NoError(t, err)
	require.NoError(t, bkt.Upload(context.Background(), block.NoCompactMarkFilepath(id), bytes.NewReader(data)))
}

func uploadDeletionMark(t *testing.T, bkt objstore.Bucket, id ulid.ULID) {
	t.Helper()
	mark := block.DeletionMark{
		ID:           id,
		DeletionTime: time.Now().Unix(),
		Version:      block.DeletionMarkVersion1,
	}
	data, err := json.Marshal(mark)
	require.NoError(t, err)
	require.NoError(t, bkt.Upload(context.Background(), block.DeletionMarkFilepath(id), bytes.NewReader(data)))
}

// Duplicates metaCacheKey in batchCachingMetaFetcher intentionally to catch potential changes
func tenantMetaCacheKey(tenant string, id ulid.ULID) string {
	objectName := path.Join(tenant, id.String(), block.MetaFilename)
	return bucketcache.ContentKey("", objectName)
}

func uploadToCache(t *testing.T, c *cache.MockCache, tenant string, meta *block.Meta) {
	t.Helper()
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	key := tenantMetaCacheKey(tenant, meta.ULID)
	require.NoError(t, c.Set(context.Background(), key, data, 24*time.Hour))
}

// removeBlockFilter is a MetadataFilter that removes a specific block from metas
type removeBlockFilter struct {
	id ulid.ULID
}

func (f *removeBlockFilter) Filter(_ context.Context, metas map[ulid.ULID]*block.Meta, _ block.GaugeVec) error {
	delete(metas, f.id)
	return nil
}
