// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/caching_bucket_factory.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.
package cache

import (
	"regexp"
	"time"

	"github.com/thanos-io/thanos/pkg/model"
)

// BucketCacheProvider is a type used to evaluate all bucket cache providers.
type BucketCacheProvider string

const (
	InMemoryBucketCacheProvider  BucketCacheProvider = "IN-MEMORY" // In-memory cache-provider for caching bucket.
	MemcachedBucketCacheProvider BucketCacheProvider = "MEMCACHED" // Memcached cache-provider for caching bucket.
)

// CachingWithBackendConfig is a configuration of caching bucket used by Store component.
type CachingWithBackendConfig struct {
	Type          BucketCacheProvider `yaml:"type"`
	BackendConfig interface{}         `yaml:"config"`

	// Basic unit used to cache chunks.
	ChunkSubrangeSize int64 `yaml:"chunk_subrange_size"`

	// Maximum number of GetRange requests issued by this bucket for single GetRange call. Zero or negative value = unlimited.
	MaxChunksGetRangeRequests int `yaml:"max_chunks_get_range_requests"`

	// TTLs for various cache items.
	ChunkObjectAttrsTTL time.Duration `yaml:"chunk_object_attrs_ttl"`
	ChunkSubrangeTTL    time.Duration `yaml:"chunk_subrange_ttl"`

	// How long to cache result of Iter call in root directory.
	BlocksIterTTL time.Duration `yaml:"blocks_iter_ttl"`

	// Config for Exists and Get operations for metadata files.
	MetafileExistsTTL      time.Duration `yaml:"metafile_exists_ttl"`
	MetafileDoesntExistTTL time.Duration `yaml:"metafile_doesnt_exist_ttl"`
	MetafileContentTTL     time.Duration `yaml:"metafile_content_ttl"`
	MetafileMaxSize        model.Bytes   `yaml:"metafile_max_size"`
}

func (cfg *CachingWithBackendConfig) Defaults() {
	cfg.ChunkSubrangeSize = 16000 // Equal to max chunk size.
	cfg.ChunkObjectAttrsTTL = 24 * time.Hour
	cfg.ChunkSubrangeTTL = 24 * time.Hour
	cfg.MaxChunksGetRangeRequests = 3
	cfg.BlocksIterTTL = 5 * time.Minute
	cfg.MetafileExistsTTL = 2 * time.Hour
	cfg.MetafileDoesntExistTTL = 15 * time.Minute
	cfg.MetafileContentTTL = 24 * time.Hour
	cfg.MetafileMaxSize = 1024 * 1024 // Equal to default MaxItemSize in memcached client.
}

var chunksMatcher = regexp.MustCompile(`^.*/chunks/\d+$`)

func isTSDBChunkFile(name string) bool { return chunksMatcher.MatchString(name) }
