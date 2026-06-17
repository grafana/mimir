// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/caching_bucket_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketcache"
)

func TestIsTenantDir(t *testing.T) {
	assert.False(t, isTenantBlocksDir(""))
	assert.True(t, isTenantBlocksDir("test"))
	assert.True(t, isTenantBlocksDir("test/"))
	assert.False(t, isTenantBlocksDir("test/block"))
	assert.False(t, isTenantBlocksDir("test/block/chunks"))
}

func TestIsBucketIndexFile(t *testing.T) {
	assert.False(t, isBucketIndexFile(""))
	assert.False(t, isBucketIndexFile("test"))
	assert.False(t, isBucketIndexFile("test/block"))
	assert.False(t, isBucketIndexFile("test/block/chunks"))
	assert.True(t, isBucketIndexFile("test/bucket-index.json.gz"))
}

func TestIsBlockIndexFile(t *testing.T) {
	blockID := ulid.MustNew(1, nil)

	assert.False(t, isBlockIndexFile(""))
	assert.False(t, isBlockIndexFile("/index"))
	assert.False(t, isBlockIndexFile("test/index"))
	assert.False(t, isBlockIndexFile("/test/index"))
	assert.True(t, isBlockIndexFile(fmt.Sprintf("%s/index", blockID.String())))
	assert.True(t, isBlockIndexFile(fmt.Sprintf("/%s/index", blockID.String())))
}

func Test_NewStoreCachingBucket(t *testing.T) {
	type testCase struct {
		op         string
		configName string
	}
	metadataTestCases := []testCase{
		{op: objstore.OpExists, configName: "metafile"},
		{op: objstore.OpGet, configName: "metafile"},
		{op: objstore.OpAttributes, configName: "metafile"},
		{op: objstore.OpAttributes, configName: "block-index"},
		{op: objstore.OpGet, configName: "bucket-index"},
		{op: objstore.OpIter, configName: "tenants-iter"},
		{op: objstore.OpIter, configName: "tenant-blocks-iter"},
		{op: objstore.OpIter, configName: "chunks-iter"},
	}
	indexHeaderTestCases := []testCase{
		{op: objstore.OpGetRange, configName: "block-index-header"},
	}
	chunksTestCases := []testCase{
		{op: objstore.OpGetRange, configName: "chunks"},
	}

	makeValidateFunc := func(t *testing.T, expectCache, expectAttrsCache cache.Cache) func(opCache, attrsCache cache.Cache) error {
		return func(opCache, attrsCache cache.Cache) error {
			require.Equal(t, expectCache, opCache)
			require.Equal(t, expectAttrsCache, attrsCache)
			return nil
		}
	}

	validateMetadataCaching := func(
		t *testing.T, cfg *bucketcache.CachingBucketConfig, metaCache cache.Cache, expectErr error) {
		for _, tc := range metadataTestCases {
			validate := makeValidateFunc(t, metaCache, nil) // None use of attrs cache for metadata
			err := bucketcache.ValidateConfig(cfg, tc.op, tc.configName, validate)
			if expectErr != nil {
				require.ErrorContains(t, err, expectErr.Error())
			} else {
				require.NoError(t, err)
			}
		}
	}
	validateStoreCaching := func(
		t *testing.T,
		testCases []testCase,
		cfg *bucketcache.CachingBucketConfig,
		opCache, attrsCache cache.Cache,
		ll log.Logger,
		expectErr error,
	) {
		for _, tc := range testCases {
			if opCache != nil {
				if opCache == attrsCache {
					// If expected caches are the same, the op cache is re-used as attrs cache.
					// In order to pass the test assertions, it must be wrapped as the op cache will be.
					attrsCache = cache.NewSpanlessTracingCache(attrsCache, ll, tenant.NewMultiResolver())
				}
				// The op cache is always wrapped if enabled.
				opCache = cache.NewSpanlessTracingCache(opCache, ll, tenant.NewMultiResolver())
			}

			validate := makeValidateFunc(t, opCache, attrsCache)
			err := bucketcache.ValidateConfig(cfg, tc.op, tc.configName, validate)
			if expectErr != nil {
				require.ErrorContains(t, err, expectErr.Error())
			} else {
				require.NoError(t, err)
			}
		}
	}

	t.Run("no caches enabled", func(t *testing.T) {
		bkt, ll, reg := objstore.NewInMemBucket(), log.NewNopLogger(), prometheus.NewPedanticRegistry()
		bucketCacheCfg := bucketcache.NewCachingBucketConfig()

		cacheBkt, err := newStoreCachingBucket(
			bucketCacheCfg, BlocksStorageConfig{}, nil, nil, nil, bkt, ll, reg,
		)
		require.NoError(t, err)
		require.IsNotType(t, &bucketcache.CachingBucket{}, cacheBkt)
		// A configured bucket should have been a pointer type but just to make sure
		require.IsNotType(t, bucketcache.CachingBucket{}, cacheBkt)
	})

	t.Run("all caches enabled", func(t *testing.T) {
		bkt, ll, reg := objstore.NewInMemBucket(), log.NewNopLogger(), prometheus.NewPedanticRegistry()
		bucketCacheCfg := bucketcache.NewCachingBucketConfig()
		metadata, indexHeader, chunks := cache.NewMockCache(), cache.NewMockCache(), cache.NewMockCache()

		cacheBkt, err := newStoreCachingBucket(
			bucketCacheCfg, BlocksStorageConfig{}, metadata, indexHeader, chunks, bkt, ll, reg,
		)
		require.NoError(t, err)
		require.IsType(t, &bucketcache.CachingBucket{}, cacheBkt)

		validateMetadataCaching(t, bucketCacheCfg, metadata, nil)

		// Metadata cache enabled; used for index-header attrs.
		validateStoreCaching(t, indexHeaderTestCases, bucketCacheCfg, indexHeader, metadata, ll, nil)
		// Metadata cache enabled; used for chunks attrs.
		validateStoreCaching(t, chunksTestCases, bucketCacheCfg, chunks, metadata, ll, nil)
	})

	t.Run("no metadata cache enabled", func(t *testing.T) {
		bkt, ll, reg := objstore.NewInMemBucket(), log.NewNopLogger(), prometheus.NewPedanticRegistry()
		bucketCacheCfg := bucketcache.NewCachingBucketConfig()

		var metadata cache.Cache // nil to disable
		indexHeader, chunks := cache.NewMockCache(), cache.NewMockCache()

		cacheBkt, err := newStoreCachingBucket(
			bucketCacheCfg, BlocksStorageConfig{}, metadata, indexHeader, chunks, bkt, ll, reg,
		)
		require.NoError(t, err)
		require.IsType(t, &bucketcache.CachingBucket{}, cacheBkt)

		validateMetadataCaching(t, bucketCacheCfg, nil, errors.New("operation config not found"))

		// No metadata cache enabled; index-header uses itself for attrs.
		validateStoreCaching(t, indexHeaderTestCases, bucketCacheCfg, indexHeader, indexHeader, ll, nil)
		// No metadata cache enabled; chunks uses itself for attrs.
		validateStoreCaching(t, chunksTestCases, bucketCacheCfg, chunks, chunks, ll, nil)
	})

	t.Run("no index-header cache enabled", func(t *testing.T) {
		bkt, ll, reg := objstore.NewInMemBucket(), log.NewNopLogger(), prometheus.NewPedanticRegistry()
		bucketCacheCfg := bucketcache.NewCachingBucketConfig()

		var indexHeader cache.Cache // nil to disable
		metadata, chunks := cache.NewMockCache(), cache.NewMockCache()

		cacheBkt, err := newStoreCachingBucket(
			bucketCacheCfg, BlocksStorageConfig{}, metadata, indexHeader, chunks, bkt, ll, reg,
		)
		require.NoError(t, err)
		require.IsType(t, &bucketcache.CachingBucket{}, cacheBkt)

		validateMetadataCaching(t, bucketCacheCfg, metadata, nil)

		// No index-header cache enabled; no caching.
		validateStoreCaching(t, indexHeaderTestCases, bucketCacheCfg, nil, nil, ll, errors.New("operation config not found"))
		// Metadata cache enabled; used for chunks attrs.
		validateStoreCaching(t, chunksTestCases, bucketCacheCfg, chunks, metadata, ll, nil)
	})

	t.Run("no chunks cache enabled", func(t *testing.T) {
		bkt, ll, reg := objstore.NewInMemBucket(), log.NewNopLogger(), prometheus.NewPedanticRegistry()
		bucketCacheCfg := bucketcache.NewCachingBucketConfig()

		var chunks cache.Cache // nil to disable
		metadata, indexHeader := cache.NewMockCache(), cache.NewMockCache()

		cacheBkt, err := newStoreCachingBucket(
			bucketCacheCfg, BlocksStorageConfig{}, metadata, indexHeader, chunks, bkt, ll, reg,
		)
		require.NoError(t, err)
		require.IsType(t, &bucketcache.CachingBucket{}, cacheBkt)

		validateMetadataCaching(t, bucketCacheCfg, metadata, nil)

		// Metadata cache enabled; used for index-header attrs.
		validateStoreCaching(t, indexHeaderTestCases, bucketCacheCfg, indexHeader, metadata, ll, nil)
		// No chunks cache enabled; no caching.
		validateStoreCaching(t, chunksTestCases, bucketCacheCfg, nil, nil, ll, errors.New("operation config not found"))
	})
}
