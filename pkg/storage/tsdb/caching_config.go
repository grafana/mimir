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
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketcache"
)

// subrangeSize is the size of each subrange that bucket objects are split into for better caching
const subrangeSize int64 = 16000

var supportedCacheBackends = []string{cache.BackendMemcached}

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

type IndexHeaderCacheConfig struct {
	Enabled       bool          `yaml:"enabled" category:"experimental"`
	AttributesTTL time.Duration `yaml:"attributes_ttl" category:"experimental"`

	SubrangeTTL              time.Duration `yaml:"subrange_ttl" category:"experimental"`
	SubRangeInMemoryMaxItems int           `yaml:"subrange_in_memory_max_items" category:"experimental"`
	MaxGetRangeRequests      int           `yaml:"max_get_range_requests" category:"experimental"`
}

func (cfg *IndexHeaderCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Enable caching of reads for TSDB index-header sections from object storage, utilizing the index-cache backend.")
	f.DurationVar(&cfg.AttributesTTL, prefix+"attributes-ttl", 168*time.Hour, "How long to cache attributes of the block index as utilized by the index-header reader.  If the metadata cache is configured, attributes will be stored in the metadata cache backend, otherwise attributes are stored in the index cache backend.")
	f.DurationVar(&cfg.SubrangeTTL, prefix+"subrange-ttl", 24*time.Hour, "TTL for caching individual index-header subranges.")
	f.IntVar(&cfg.SubRangeInMemoryMaxItems, prefix+"subrange-in-memory-max-items", 50000, "Maximum number of individual subrange items to keep in a first level in-memory LRU cache. Subranges will be stored and fetched in-memory before hitting the cache backend. 0 to disable the in-memory cache.")
	f.IntVar(&cfg.MaxGetRangeRequests, prefix+"max-get-range-requests", 3, "Maximum number of sub-GetRange requests that a single GetRange request can be split into when fetching index-header sections. Zero or negative value = unlimited number of sub-requests.")
}

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

	f.IntVar(&cfg.MaxGetRangeRequests, prefix+"max-get-range-requests", 3, "Maximum number of sub-GetRange requests that a single GetRange request can be split into when fetching chunks. Zero or negative value = unlimited number of sub-requests.")
	f.DurationVar(&cfg.AttributesTTL, prefix+"attributes-ttl", 168*time.Hour, "TTL for caching object attributes for chunks. If the metadata cache is configured, attributes will be stored under this cache backend, otherwise attributes are stored in the chunks cache backend.")
	f.IntVar(&cfg.AttributesInMemoryMaxItems, prefix+"attributes-in-memory-max-items", 50000, "Maximum number of object attribute items to keep in a first-level in-memory LRU cache. Metadata will be stored and fetched in-memory before hitting the cache backend. 0 to disable the in-memory cache.")
	f.DurationVar(&cfg.SubrangeTTL, prefix+"subrange-ttl", 24*time.Hour, "TTL for caching individual chunks subranges.")
}

func (cfg *ChunksCacheConfig) Validate() error {
	return cfg.BackendConfig.Validate()
}

func (cfg *MetadataCacheConfig) Validate() error {
	return cfg.BackendConfig.Validate()
}

// NewMetadataCacheClient returns a configured cache client, or nil if configured backend is in-memory.
func NewMetadataCacheClient(
	cfg cache.BackendConfig, logger log.Logger, reg prometheus.Registerer,
) (metadataCache cache.Cache, err error) {
	const name = "metadata-cache"
	metadataCache, err = cache.CreateClient(name, cfg, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}

	if metadataCache != nil {
		metadataCache = cache.NewSpanlessTracingCache(metadataCache, logger, tenant.NewMultiResolver())
	}
	return metadataCache, nil
}

func configureMetadataCaching(
	metadataCache cache.Cache,
	metadataCacheCfg MetadataCacheConfig,
	cachingBucketCfg *bucketcache.CachingBucketConfig,
) *bucketcache.CachingBucketConfig {
	if metadataCache == nil {
		// Metadata will not be cached
		return cachingBucketCfg
	}

	cachingBucketCfg.CacheExists("metafile", metadataCache, isMetaFile, metadataCacheCfg.MetafileExistsTTL, metadataCacheCfg.MetafileDoesntExistTTL)
	cachingBucketCfg.CacheGet("metafile", metadataCache, isMetaFile, metadataCacheCfg.MetafileMaxSize, metadataCacheCfg.MetafileContentTTL, metadataCacheCfg.MetafileExistsTTL, metadataCacheCfg.MetafileDoesntExistTTL)
	cachingBucketCfg.CacheAttributes("metafile", metadataCache, isMetaFile, metadataCacheCfg.MetafileAttributesTTL)
	cachingBucketCfg.CacheAttributes("block-index", metadataCache, isBlockIndexFile, metadataCacheCfg.BlockIndexAttributesTTL)
	cachingBucketCfg.CacheGet("bucket-index", metadataCache, isBucketIndexFile, metadataCacheCfg.BucketIndexMaxSize, metadataCacheCfg.BucketIndexContentTTL /* do not cache exist / not exist: */, 0, 0)

	codec := bucketcache.SnappyIterCodec{IterCodec: bucketcache.JSONIterCodec{}}
	cachingBucketCfg.CacheIter("tenants-iter", metadataCache, isTenantsDir, metadataCacheCfg.TenantsListTTL, codec)
	cachingBucketCfg.CacheIter("tenant-blocks-iter", metadataCache, isTenantBlocksDir, metadataCacheCfg.TenantBlocksListTTL, codec)
	cachingBucketCfg.CacheIter("chunks-iter", metadataCache, isChunksDir, metadataCacheCfg.ChunksListTTL, codec)

	return cachingBucketCfg
}

// NewMetadataCachingBucket creates a caching bucket for metadata and indexes of the bucket store and its tenant TSDBs.
func NewMetadataCachingBucket(
	metadataCfg MetadataCacheConfig,
	bkt objstore.Bucket,
	logger log.Logger,
	reg prometheus.Registerer,
	metrics *bucketcache.CachingBucketMetrics,
) (objstore.Bucket, error) {
	metadataCache, err := NewMetadataCacheClient(metadataCfg.BackendConfig, logger, reg)
	if err != nil {
		return nil, err
	}
	if metadataCache == nil {
		// No caching configured
		return bkt, nil
	}

	cachingBucketCfg := bucketcache.NewCachingBucketConfig()
	cachingBucketCfg = configureMetadataCaching(metadataCache, metadataCfg, cachingBucketCfg)

	// NOTE: the bucket ID should be "blocks" but we're passing an empty string to not cause
	// a massive cache invalidation when rolling out a new Mimir version introducing the bucket
	// ID. This is still fine, as far as all other caching bucket implementations specify their
	// own unique ID.
	return bucketcache.NewCachingBucket("", bkt, cachingBucketCfg, logger, metrics)
}

func NewChunksCacheClient(
	cfg cache.BackendConfig, logger log.Logger, reg prometheus.Registerer) (chunksCache cache.Cache, err error) {
	const name = "chunks-cache"
	chunksCache, err = cache.CreateClient(
		name, cfg, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "chunks-cache")
	}
	return chunksCache, nil
}

// NewStoreCachingBucket creates a single caching bucket that handles metadata, index-header, and chunks caching.
//   - Index-header config matches isBlockIndexFile to cache GetRange calls.
//   - Chunks config matches isTSDBChunkFile to cache GetRange calls.
//   - Metadata caching is shared with across index-header and chunks caching buckets if enabled,
//     otherwise each cache handles its own metadata storage.
func NewStoreCachingBucket(
	metadataCache cache.Cache,
	cfg BlocksStorageConfig,
	indexCacheClient cache.Cache,
	chunksCache cache.Cache,
	bkt objstore.Bucket,
	logger log.Logger,
	reg prometheus.Registerer,
	metrics *bucketcache.CachingBucketMetrics,
) (objstore.Bucket, error) {
	var (
		err               error
		cachingConfigured = false
		cachingBucketCfg  = bucketcache.NewCachingBucketConfig()
	)

	if metadataCache != nil {
		cachingConfigured = true
		cachingBucketCfg = configureMetadataCaching(metadataCache, cfg.BucketStore.MetadataCache, cachingBucketCfg)
	}

	if indexCacheClient != nil && cfg.BucketStore.IndexHeaderCache.Enabled {
		cachingConfigured = true
		indexCacheClient = cache.NewSpanlessTracingCache(indexCacheClient, logger, tenant.NewMultiResolver())

		// GetRange caching requires object attributes for calculating subranges.
		// Use the metadata cache for attributes if enabled, otherwise fallback to index cache.
		attributesCache := indexCacheClient
		if metadataCache != nil {
			attributesCache = metadataCache
		}
		if cfg.BucketStore.IndexHeaderCache.SubRangeInMemoryMaxItems > 0 {
			indexCacheClient, err = cache.WrapWithLRUCache(
				indexCacheClient,
				"block-index-header-cache",
				prometheus.WrapRegistererWithPrefix("cortex_", reg),
				cfg.BucketStore.IndexHeaderCache.SubRangeInMemoryMaxItems,
				cfg.BucketStore.IndexHeaderCache.SubrangeTTL,
				logger,
			)
			if err != nil {
				return nil, errors.Wrapf(err, "wrap index-header cache with in-memory cache")
			}
		}
		cachingBucketCfg.CacheGetRange(
			"block-index-header",
			indexCacheClient,
			isBlockIndexFile,
			subrangeSize,
			attributesCache,
			cfg.BucketStore.IndexHeaderCache.AttributesTTL,
			cfg.BucketStore.IndexHeaderCache.SubrangeTTL,
			cfg.BucketStore.IndexHeaderCache.MaxGetRangeRequests,
		)
	}

	if chunksCache != nil {
		cachingConfigured = true
		chunksCache = cache.NewSpanlessTracingCache(chunksCache, logger, tenant.NewMultiResolver())

		// GetRange caching requires object attributes for calculating subranges.
		// Use the metadata cache for attributes if configured, otherwise fallback to chunks cache.
		attributesCache := chunksCache
		if metadataCache != nil {
			attributesCache = metadataCache
		}
		if cfg.BucketStore.ChunksCache.AttributesInMemoryMaxItems > 0 {
			attributesCache, err = cache.WrapWithLRUCache(
				attributesCache, "chunks-attributes-cache",
				prometheus.WrapRegistererWithPrefix("cortex_", reg),
				cfg.BucketStore.ChunksCache.AttributesInMemoryMaxItems,
				cfg.BucketStore.ChunksCache.AttributesTTL,
				logger,
			)
			if err != nil {
				return nil, errors.Wrapf(err, "wrap metadata cache with in-memory cache")
			}
		}
		cachingBucketCfg.CacheGetRange(
			"chunks",
			chunksCache,
			isTSDBChunkFile,
			subrangeSize,
			attributesCache,
			cfg.BucketStore.ChunksCache.AttributesTTL,
			cfg.BucketStore.ChunksCache.SubrangeTTL,
			cfg.BucketStore.ChunksCache.MaxGetRangeRequests,
		)
	}

	if !cachingConfigured {
		return bkt, nil
	}

	// NOTE: the bucket ID should be "blocks" but we're passing an empty string to not cause
	// a massive cache invalidation when rolling out a new Mimir version introducing the bucket
	// ID. This is still fine, as far as all other caching bucket implementations specify their
	// own unique ID.
	return bucketcache.NewCachingBucket("", bkt, cachingBucketCfg, logger, metrics)
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
