// SPDX-License-Identifier: AGPL-3.0-only

package tsdb

import (
	"flag"
	"fmt"
	"time"
)

type ChunksCacheConfig struct {
	CacheBackend `yaml:",inline"`

	SubrangeSize        int64         `yaml:"subrange_size"`
	MaxGetRangeRequests int           `yaml:"max_get_range_requests"`
	AttributesTTL       time.Duration `yaml:"attributes_ttl"`
	SubrangeTTL         time.Duration `yaml:"subrange_ttl"`
}

type MetadataCacheConfig struct {
	CacheBackend `yaml:",inline"`

	TenantsListTTL          time.Duration `yaml:"tenants_list_ttl"`
	TenantBlocksListTTL     time.Duration `yaml:"tenant_blocks_list_ttl"`
	ChunksListTTL           time.Duration `yaml:"chunks_list_ttl"`
	MetafileExistsTTL       time.Duration `yaml:"metafile_exists_ttl"`
	MetafileDoesntExistTTL  time.Duration `yaml:"metafile_doesnt_exist_ttl"`
	MetafileContentTTL      time.Duration `yaml:"metafile_content_ttl"`
	MetafileMaxSize         int           `yaml:"metafile_max_size_bytes"`
	MetafileAttributesTTL   time.Duration `yaml:"metafile_attributes_ttl"`
	BlockIndexAttributesTTL time.Duration `yaml:"block_index_attributes_ttl"`
	BucketIndexContentTTL   time.Duration `yaml:"bucket_index_content_ttl"`
	BucketIndexMaxSize      int           `yaml:"bucket_index_max_size_bytes"`
}

const (
	CacheBackendMemcached = "memcached"
)

type CacheBackend struct {
	Backend   string                `yaml:"backend"`
	Memcached MemcachedClientConfig `yaml:"memcached"`
}

func (cfg *ChunksCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", "", fmt.Sprintf("Backend for chunks cache, if not empty. Supported values: %s.", CacheBackendMemcached))

	cfg.Memcached.RegisterFlagsWithPrefix(f, prefix+"memcached.")

	f.Int64Var(&cfg.SubrangeSize, prefix+"subrange-size", 16000, "Size of each subrange that bucket object is split into for better caching.")
	f.IntVar(&cfg.MaxGetRangeRequests, prefix+"max-get-range-requests", 3, "Maximum number of sub-GetRange requests that a single GetRange request can be split into when fetching chunks. Zero or negative value = unlimited number of sub-requests.")
	f.DurationVar(&cfg.AttributesTTL, prefix+"attributes-ttl", 168*time.Hour, "TTL for caching object attributes for chunks.")
	f.DurationVar(&cfg.SubrangeTTL, prefix+"subrange-ttl", 24*time.Hour, "TTL for caching individual chunks subranges.")
}
func (cfg *MetadataCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", "", fmt.Sprintf("Backend for metadata cache, if not empty. Supported values: %s.", CacheBackendMemcached))

	cfg.Memcached.RegisterFlagsWithPrefix(f, prefix+"memcached.")

	f.DurationVar(&cfg.TenantsListTTL, prefix+"tenants-list-ttl", 15*time.Minute, "How long to cache list of tenants in the bucket.")
	f.DurationVar(&cfg.TenantBlocksListTTL, prefix+"tenant-blocks-list-ttl", 5*time.Minute, "How long to cache list of blocks for each tenant.")
	f.DurationVar(&cfg.ChunksListTTL, prefix+"chunks-list-ttl", 24*time.Hour, "How long to cache list of chunks for a block.")
	f.DurationVar(&cfg.MetafileExistsTTL, prefix+"metafile-exists-ttl", 2*time.Hour, "How long to cache information that block metafile exists. Also used for user deletion mark file.")
	f.DurationVar(&cfg.MetafileDoesntExistTTL, prefix+"metafile-doesnt-exist-ttl", 5*time.Minute, "How long to cache information that block metafile doesn't exist. Also used for user deletion mark file.")
	f.DurationVar(&cfg.MetafileContentTTL, prefix+"metafile-content-ttl", 24*time.Hour, "How long to cache content of the metafile.")
	f.IntVar(&cfg.MetafileMaxSize, prefix+"metafile-max-size-bytes", 1*1024*1024, "Maximum size of metafile content to cache in bytes. Caching will be skipped if the content exceeds this size. This is useful to avoid network round trip for large content if the configured caching backend has an hard limit on cached items size (in this case, you should set this limit to the same limit in the caching backend).")
	f.DurationVar(&cfg.MetafileAttributesTTL, prefix+"metafile-attributes-ttl", 168*time.Hour, "How long to cache attributes of the block metafile.")
	f.DurationVar(&cfg.BlockIndexAttributesTTL, prefix+"block-index-attributes-ttl", 168*time.Hour, "How long to cache attributes of the block index.")
	f.DurationVar(&cfg.BucketIndexContentTTL, prefix+"bucket-index-content-ttl", 5*time.Minute, "How long to cache content of the bucket index.")
	f.IntVar(&cfg.BucketIndexMaxSize, prefix+"bucket-index-max-size-bytes", 1*1024*1024, "Maximum size of bucket index content to cache in bytes. Caching will be skipped if the content exceeds this size. This is useful to avoid network round trip for large content if the configured caching backend has an hard limit on cached items size (in this case, you should set this limit to the same limit in the caching backend).")
}
