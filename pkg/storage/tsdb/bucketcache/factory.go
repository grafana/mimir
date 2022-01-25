// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/caching_bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package bucketcache

import (
	"path/filepath"
	"regexp"
	"strings"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/grafana/mimir/pkg/cache"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

func CreateCachingBucket(chunksConfig tsdb.ChunksCacheConfig, metadataConfig tsdb.MetadataCacheConfig, bkt objstore.Bucket, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	cfg := NewCachingBucketConfig()
	cachingConfigured := false

	chunksCache, err := cache.CreateClient("chunks-cache", chunksConfig.BackendConfig, logger, reg)
	if err != nil {
		return nil, errors.Wrapf(err, "chunks-cache")
	}

	metadataCache, err := cache.CreateClient("metadata-cache", metadataConfig.BackendConfig, logger, reg)
	if err != nil {
		return nil, errors.Wrapf(err, "metadata-cache")
	}
	if metadataCache != nil {
		cachingConfigured = true
		metadataCache = cache.NewSpanlessTracingCache(metadataCache, logger)

		cfg.CacheExists("metafile", metadataCache, isMetaFile, metadataConfig.MetafileExistsTTL, metadataConfig.MetafileDoesntExistTTL)
		cfg.CacheGet("metafile", metadataCache, isMetaFile, metadataConfig.MetafileMaxSize, metadataConfig.MetafileContentTTL, metadataConfig.MetafileExistsTTL, metadataConfig.MetafileDoesntExistTTL)
		cfg.CacheAttributes("metafile", metadataCache, isMetaFile, metadataConfig.MetafileAttributesTTL)
		cfg.CacheAttributes("block-index", metadataCache, isBlockIndexFile, metadataConfig.BlockIndexAttributesTTL)
		cfg.CacheGet("bucket-index", metadataCache, isBucketIndexFile, metadataConfig.BucketIndexMaxSize, metadataConfig.BucketIndexContentTTL /* do not cache exist / not exist: */, 0, 0)

		codec := snappyIterCodec{JSONIterCodec{}}
		cfg.CacheIter("tenants-iter", metadataCache, isTenantsDir, metadataConfig.TenantsListTTL, codec)
		cfg.CacheIter("tenant-blocks-iter", metadataCache, isTenantBlocksDir, metadataConfig.TenantBlocksListTTL, codec)
		cfg.CacheIter("chunks-iter", metadataCache, isChunksDir, metadataConfig.ChunksListTTL, codec)
	}

	if chunksCache != nil {
		cachingConfigured = true
		chunksCache = cache.NewSpanlessTracingCache(chunksCache, logger)

		// Use the metadata cache for attributes if configured, otherwise fallback to chunks cache.
		// If in-memory cache is enabled, wrap the attributes cache with the in-memory LRU cache.
		attributesCache := chunksCache
		if metadataCache != nil {
			attributesCache = metadataCache
		}
		if chunksConfig.AttributesInMemoryMaxItems > 0 {
			var err error
			attributesCache, err = cache.WrapWithLRUCache(attributesCache, "chunks-attributes-cache", reg, chunksConfig.AttributesInMemoryMaxItems, chunksConfig.AttributesTTL)
			if err != nil {
				return nil, errors.Wrapf(err, "wrap metadata cache with in-memory cache")
			}
		}

		cfg.CacheGetRange("chunks", chunksCache, isTSDBChunkFile, chunksConfig.SubrangeSize, attributesCache, chunksConfig.AttributesTTL, chunksConfig.SubrangeTTL, chunksConfig.MaxGetRangeRequests)
	}

	if !cachingConfigured {
		// No caching is configured.
		return bkt, nil
	}

	return NewCachingBucket(bkt, cfg, logger, reg)
}

var chunksMatcher = regexp.MustCompile(`^.*/chunks/\d+$`)

func isTSDBChunkFile(name string) bool { return chunksMatcher.MatchString(name) }

func isMetaFile(name string) bool {
	return strings.HasSuffix(name, "/"+metadata.MetaFilename) || strings.HasSuffix(name, "/"+metadata.DeletionMarkFilename) || strings.HasSuffix(name, "/"+tsdb.TenantDeletionMarkPath)
}

func isBlockIndexFile(name string) bool {
	// Ensure the path ends with "<block id>/<index filename>".
	if !strings.HasSuffix(name, "/"+block.IndexFilename) {
		return false
	}

	_, err := ulid.Parse(filepath.Base(filepath.Dir(name)))
	return err == nil
}

func isBucketIndexFile(name string) bool {
	return strings.HasSuffix(name, "/"+bucketindex.IndexCompressedFilename)
}

func isTenantsDir(name string) bool {
	return name == ""
}

var tenantDirMatcher = regexp.MustCompile("^[^/]+/?$")

func isTenantBlocksDir(name string) bool {
	return tenantDirMatcher.MatchString(name)
}

func isChunksDir(name string) bool {
	return strings.HasSuffix(name, "/chunks")
}

type snappyIterCodec struct {
	IterCodec
}

func (i snappyIterCodec) Encode(files []string) ([]byte, error) {
	b, err := i.IterCodec.Encode(files)
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, b), nil
}

func (i snappyIterCodec) Decode(cachedData []byte) ([]string, error) {
	b, err := snappy.Decode(nil, cachedData)
	if err != nil {
		return nil, errors.Wrap(err, "snappyIterCodec")
	}
	return i.IterCodec.Decode(b)
}
