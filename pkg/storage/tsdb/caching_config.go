// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/caching_bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/regexp"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketcache"
)

// subrangeSize is the size of each subrange that bucket objects are split into for better caching
const subrangeSize int64 = 16000

var supportedCacheBackends = []string{cache.BackendMemcached, cache.BackendRedis}

type ChunksCacheConfig struct {
	cache.BackendConfig `yaml:",inline"`

	MaxGetRangeRequests        int           `yaml:"max_get_range_requests" category:"advanced"`
	AttributesTTL              time.Duration `yaml:"attributes_ttl" category:"advanced"`
	AttributesInMemoryMaxItems int           `yaml:"attributes_in_memory_max_items" category:"advanced"`
	SubrangeTTL                time.Duration `yaml:"subrange_ttl" category:"advanced"`
}

func (cfg *ChunksCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", "", fmt.Sprintf("Backend for chunks cache, if not empty. Supported values: %s.", strings.Join(supportedCacheBackends, ", ")))

	cfg.Memcached.RegisterFlagsWithPrefix(prefix+"memcached.", f)
	cfg.Redis.RegisterFlagsWithPrefix(prefix+"redis.", f)

	f.IntVar(&cfg.MaxGetRangeRequests, prefix+"max-get-range-requests", 3, "Maximum number of sub-GetRange requests that a single GetRange request can be split into when fetching chunks. Zero or negative value = unlimited number of sub-requests.")
	f.DurationVar(&cfg.AttributesTTL, prefix+"attributes-ttl", 168*time.Hour, "TTL for caching object attributes for chunks. If the metadata cache is configured, attributes will be stored under this cache backend, otherwise attributes are stored in the chunks cache backend.")
	f.IntVar(&cfg.AttributesInMemoryMaxItems, prefix+"attributes-in-memory-max-items", 50000, "Maximum number of object attribute items to keep in a first level in-memory LRU cache. Metadata will be stored and fetched in-memory before hitting the cache backend. 0 to disable the in-memory cache.")
	f.DurationVar(&cfg.SubrangeTTL, prefix+"subrange-ttl", 24*time.Hour, "TTL for caching individual chunks subranges.")
}

func (cfg *ChunksCacheConfig) Validate() error {
	return cfg.BackendConfig.Validate()
}

type MetadataCacheConfig struct {
	cache.BackendConfig `yaml:",inline"`

	TenantsListTTL          time.Duration `yaml:"tenants_list_ttl" category:"advanced"`
	TenantBlocksListTTL     time.Duration `yaml:"tenant_blocks_list_ttl" category:"advanced"`
	ChunksListTTL           time.Duration `yaml:"chunks_list_ttl" category:"advanced"`
	MetafileExistsTTL       time.Duration `yaml:"metafile_exists_ttl" category:"advanced"`
	MetafileDoesntExistTTL  time.Duration `yaml:"metafile_doesnt_exist_ttl" category:"advanced"`
	MetafileContentTTL      time.Duration `yaml:"metafile_content_ttl" category:"advanced"`
	MetafileMaxSize         int           `yaml:"metafile_max_size_bytes" category:"advanced"`
	MetafileAttributesTTL   time.Duration `yaml:"metafile_attributes_ttl" category:"advanced"`
	BlockIndexAttributesTTL time.Duration `yaml:"block_index_attributes_ttl" category:"advanced"`
	BucketIndexContentTTL   time.Duration `yaml:"bucket_index_content_ttl" category:"advanced"`
	BucketIndexMaxSize      int           `yaml:"bucket_index_max_size_bytes" category:"advanced"`
}

func (cfg *MetadataCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", "", fmt.Sprintf("Backend for metadata cache, if not empty. Supported values: %s.", strings.Join(supportedCacheBackends, ", ")))

	cfg.Memcached.RegisterFlagsWithPrefix(prefix+"memcached.", f)
	cfg.Redis.RegisterFlagsWithPrefix(prefix+"redis.", f)

	f.DurationVar(&cfg.TenantsListTTL, prefix+"tenants-list-ttl", 15*time.Minute, "How long to cache list of tenants in the bucket.")
	f.DurationVar(&cfg.TenantBlocksListTTL, prefix+"tenant-blocks-list-ttl", 5*time.Minute, "How long to cache list of blocks for each tenant.")
	f.DurationVar(&cfg.ChunksListTTL, prefix+"chunks-list-ttl", 24*time.Hour, "How long to cache list of chunks for a block.")
	f.DurationVar(&cfg.MetafileExistsTTL, prefix+"metafile-exists-ttl", 2*time.Hour, "How long to cache information that block metafile exists. Also used for tenant deletion mark file.")
	f.DurationVar(&cfg.MetafileDoesntExistTTL, prefix+"metafile-doesnt-exist-ttl", 5*time.Minute, "How long to cache information that block metafile doesn't exist. Also used for tenant deletion mark file.")
	f.DurationVar(&cfg.MetafileContentTTL, prefix+"metafile-content-ttl", 24*time.Hour, "How long to cache content of the metafile.")
	f.IntVar(&cfg.MetafileMaxSize, prefix+"metafile-max-size-bytes", 1*1024*1024, "Maximum size of metafile content to cache in bytes. Caching will be skipped if the content exceeds this size. This is useful to avoid network round trip for large content if the configured caching backend has an hard limit on cached items size (in this case, you should set this limit to the same limit in the caching backend).")
	f.DurationVar(&cfg.MetafileAttributesTTL, prefix+"metafile-attributes-ttl", 168*time.Hour, "How long to cache attributes of the block metafile.")
	f.DurationVar(&cfg.BlockIndexAttributesTTL, prefix+"block-index-attributes-ttl", 168*time.Hour, "How long to cache attributes of the block index.")
	f.DurationVar(&cfg.BucketIndexContentTTL, prefix+"bucket-index-content-ttl", 5*time.Minute, "How long to cache content of the bucket index.")
	f.IntVar(&cfg.BucketIndexMaxSize, prefix+"bucket-index-max-size-bytes", 1*1024*1024, "Maximum size of bucket index content to cache in bytes. Caching will be skipped if the content exceeds this size. This is useful to avoid network round trip for large content if the configured caching backend has an hard limit on cached items size (in this case, you should set this limit to the same limit in the caching backend).")
}

func (cfg *MetadataCacheConfig) Validate() error {
	return cfg.BackendConfig.Validate()
}

func CreateCachingBucket(chunksCache cache.Cache, chunksConfig ChunksCacheConfig, metadataConfig MetadataCacheConfig, bkt objstore.Bucket, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	cfg := bucketcache.NewCachingBucketConfig()
	cachingConfigured := false

	metadataCache, err := cache.CreateClient("metadata-cache", metadataConfig.BackendConfig, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg))
	if err != nil {
		return nil, errors.Wrapf(err, "metadata-cache")
	}
	if metadataCache != nil {
		cachingConfigured = true
		metadataCache = cache.NewSpanlessTracingCache(metadataCache, logger, tenant.NewMultiResolver())

		cfg.CacheExists("metafile", metadataCache, isMetaFile, metadataConfig.MetafileExistsTTL, metadataConfig.MetafileDoesntExistTTL)
		cfg.CacheGet("metafile", metadataCache, isMetaFile, metadataConfig.MetafileMaxSize, metadataConfig.MetafileContentTTL, metadataConfig.MetafileExistsTTL, metadataConfig.MetafileDoesntExistTTL, false)
		cfg.CacheAttributes("metafile", metadataCache, isMetaFile, metadataConfig.MetafileAttributesTTL, false)
		cfg.CacheAttributes("block-index", metadataCache, isBlockIndexFile, metadataConfig.BlockIndexAttributesTTL, false)
		cfg.CacheGet("bucket-index", metadataCache, isBucketIndexFile, metadataConfig.BucketIndexMaxSize, metadataConfig.BucketIndexContentTTL /* do not cache exist / not exist: */, 0, 0, false)

		codec := bucketcache.SnappyIterCodec{IterCodec: bucketcache.JSONIterCodec{}}
		cfg.CacheIter("tenants-iter", metadataCache, isTenantsDir, metadataConfig.TenantsListTTL, codec)
		cfg.CacheIter("tenant-blocks-iter", metadataCache, isTenantBlocksDir, metadataConfig.TenantBlocksListTTL, codec)
		cfg.CacheIter("chunks-iter", metadataCache, isChunksDir, metadataConfig.ChunksListTTL, codec)
	}

	if chunksCache != nil {
		// If the chunks cache is configured, we will use it for the attributes of chunk files instead of
		// the metadata cache.
		cachingConfigured = true
		chunksCache = cache.NewSpanlessTracingCache(chunksCache, logger, tenant.NewMultiResolver())

		// Use the metadata cache for attributes if configured, otherwise fallback to chunks cache.
		// If in-memory cache is enabled, wrap the attributes cache with the in-memory LRU cache.
		attributesCache := chunksCache
		if metadataCache != nil {
			attributesCache = metadataCache
		}
		if chunksConfig.AttributesInMemoryMaxItems > 0 {
			attributesCache, err = cache.WrapWithLRUCache(attributesCache, "chunks-attributes-cache", prometheus.WrapRegistererWithPrefix("cortex_", reg), chunksConfig.AttributesInMemoryMaxItems, chunksConfig.AttributesTTL)
			if err != nil {
				return nil, errors.Wrapf(err, "wrap metadata cache with in-memory cache")
			}
		}
		cfg.CacheGetRange("chunks", chunksCache, isTSDBChunkFile, subrangeSize, attributesCache, chunksConfig.AttributesTTL, chunksConfig.SubrangeTTL, chunksConfig.MaxGetRangeRequests)
	}

	if !cachingConfigured {
		// No caching is configured.
		return bkt, nil
	}

	// NOTE: the bucket ID should be "blocks" but we're passing an empty string to not cause
	// a massive cache invalidation when rolling out a new Mimir version introducing the bucket
	// ID. This is still fine, as far as all other caching bucket implementations specify their
	// own unique ID.
	return bucketcache.NewCachingBucket("", bkt, cfg, logger, reg)
}

var chunksMatcher = regexp.MustCompile(`^.*/chunks/\d+$`)

func isTSDBChunkFile(name string) bool { return chunksMatcher.MatchString(name) }

func isMetaFile(name string) bool {
	return strings.HasSuffix(name, "/"+block.MetaFilename) || strings.HasSuffix(name, "/"+block.DeletionMarkFilename) || strings.HasSuffix(name, "/"+TenantDeletionMarkPath)
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
	// TODO can't reference bucketindex because of a circular dependency. To be fixed.
	return strings.HasSuffix(name, "/bucket-index.json.gz")
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
