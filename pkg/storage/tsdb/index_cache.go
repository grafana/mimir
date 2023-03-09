// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/index_cache.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"flag"
	"fmt"
	"strings"

	"github.com/alecthomas/units"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/util"
)

const (
	// IndexCacheBackendInMemory is the value for the in-memory index cache backend.
	IndexCacheBackendInMemory = "inmemory"

	// IndexCacheBackendMemcached is the value for the Memcached index cache backend.
	IndexCacheBackendMemcached = cache.BackendMemcached

	// IndexCacheBackendRedis is the value for the Redis index cache backend.
	IndexCacheBackendRedis = cache.BackendRedis

	// IndexCacheBackendDefault is the value for the default index cache backend.
	IndexCacheBackendDefault = IndexCacheBackendInMemory

	defaultMaxItemSize = flagext.Bytes(128 * units.MiB)
)

var (
	supportedIndexCacheBackends = []string{IndexCacheBackendInMemory, IndexCacheBackendMemcached, IndexCacheBackendRedis}

	errUnsupportedIndexCacheBackend = errors.New("unsupported index cache backend")
)

type IndexCacheConfig struct {
	cache.BackendConfig `yaml:",inline"`
	InMemory            InMemoryIndexCacheConfig `yaml:"inmemory"`
}

func (cfg *IndexCacheConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(f, "blocks-storage.bucket-store.index-cache.")
}

func (cfg *IndexCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", IndexCacheBackendDefault, fmt.Sprintf("The index cache backend type. Supported values: %s.", strings.Join(supportedIndexCacheBackends, ", ")))

	cfg.InMemory.RegisterFlagsWithPrefix(prefix+"inmemory.", f)
	cfg.Memcached.RegisterFlagsWithPrefix(prefix+"memcached.", f)
	cfg.Redis.RegisterFlagsWithPrefix(prefix+"redis.", f)
}

// Validate the config.
func (cfg *IndexCacheConfig) Validate() error {
	if !util.StringsContain(supportedIndexCacheBackends, cfg.Backend) {
		return errUnsupportedIndexCacheBackend
	}

	// Validate backend config only when not using the in-memory cache.
	if cfg.Backend == IndexCacheBackendMemcached || cfg.Backend == IndexCacheBackendRedis {
		if err := cfg.BackendConfig.Validate(); err != nil {
			return err
		}
	}

	return nil
}

type InMemoryIndexCacheConfig struct {
	MaxSizeBytes uint64 `yaml:"max_size_bytes"`
}

func (cfg *InMemoryIndexCacheConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Uint64Var(&cfg.MaxSizeBytes, prefix+"max-size-bytes", uint64(1*units.Gibibyte), "Maximum size in bytes of in-memory index cache used to speed up blocks index lookups (shared between all tenants).")
}

// NewIndexCache creates a new index cache based on the input configuration.
func NewIndexCache(cfg IndexCacheConfig, logger log.Logger, registerer prometheus.Registerer) (indexcache.IndexCache, error) {
	switch cfg.Backend {
	case IndexCacheBackendInMemory:
		return newInMemoryIndexCache(cfg.InMemory, logger, registerer)
	case IndexCacheBackendMemcached:
		return newMemcachedIndexCache(cfg.Memcached, logger, registerer)
	case IndexCacheBackendRedis:
		return newRedisIndexCache(cfg.Redis, logger, registerer)
	default:
		return nil, errUnsupportedIndexCacheBackend
	}
}

func newInMemoryIndexCache(cfg InMemoryIndexCacheConfig, logger log.Logger, registerer prometheus.Registerer) (indexcache.IndexCache, error) {
	maxCacheSize := flagext.Bytes(cfg.MaxSizeBytes)

	// Calculate the max item size.
	maxItemSize := defaultMaxItemSize
	if maxItemSize > maxCacheSize {
		maxItemSize = maxCacheSize
	}

	return indexcache.NewInMemoryIndexCacheWithConfig(logger, registerer, indexcache.InMemoryIndexCacheConfig{
		MaxSize:     maxCacheSize,
		MaxItemSize: maxItemSize,
	})
}

func newMemcachedIndexCache(cfg cache.MemcachedClientConfig, logger log.Logger, registerer prometheus.Registerer) (indexcache.IndexCache, error) {
	client, err := cache.NewMemcachedClientWithConfig(logger, "index-cache", cfg, prometheus.WrapRegistererWithPrefix("thanos_", registerer))
	if err != nil {
		return nil, errors.Wrap(err, "create index cache memcached client")
	}

	c, err := indexcache.NewRemoteIndexCache(logger, client, registerer)
	if err != nil {
		return nil, errors.Wrap(err, "create memcached-based index cache")
	}

	return indexcache.NewTracingIndexCache(c, logger), nil
}

func newRedisIndexCache(cfg cache.RedisClientConfig, logger log.Logger, registerer prometheus.Registerer) (indexcache.IndexCache, error) {
	client, err := cache.NewRedisClient(logger, "index-cache", cfg, prometheus.WrapRegistererWithPrefix("thanos_", registerer))
	if err != nil {
		return nil, errors.Wrap(err, "create index cache redis client")
	}

	c, err := indexcache.NewRemoteIndexCache(logger, client, registerer)
	if err != nil {
		return nil, errors.Wrap(err, "create redis-based index cache")
	}

	return indexcache.NewTracingIndexCache(c, logger), nil
}
