// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/index_cache.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package indexcache

import (
	"flag"
	"fmt"
	"slices"
	"strings"

	"github.com/alecthomas/units"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// BackendInMemory is the value for the in-memory index cache backend.
	BackendInMemory = "inmemory"

	// BackendMemcached is the value for the Memcached index cache backend.
	BackendMemcached = cache.BackendMemcached

	// BackendDefault is the value for the default index cache backend.
	BackendDefault = BackendInMemory

	defaultMaxItemSize = 128 * 1024 * 1024 // 128 MiB
)

var (
	supportedIndexCacheBackends = []string{BackendInMemory, BackendMemcached}

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
	f.StringVar(&cfg.Backend, prefix+"backend", BackendDefault, fmt.Sprintf("The index cache backend type. Supported values: %s.", strings.Join(supportedIndexCacheBackends, ", ")))

	cfg.InMemory.RegisterFlagsWithPrefix(prefix+"inmemory.", f)
	cfg.Memcached.RegisterFlagsWithPrefix(prefix+"memcached.", f)
}

// Validate the config.
func (cfg *IndexCacheConfig) Validate() error {
	if !slices.Contains(supportedIndexCacheBackends, cfg.Backend) {
		return errUnsupportedIndexCacheBackend
	}

	switch cfg.Backend {
	case BackendMemcached:
		// Validate backend config only when not using the in-memory cache.
		if err := cfg.BackendConfig.Validate(); err != nil {
			return err
		}
	case BackendInMemory:
		// Validate sets defaults not exposed in config
		if err := cfg.InMemory.Validate(); err != nil {
			return err
		}
	}

	return nil
}

type InMemoryIndexCacheConfig struct {
	MaxCacheSizeBytes uint64 `yaml:"max_size_bytes"`
	MaxItemSizeBytes  uint64
}

func (cfg *InMemoryIndexCacheConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Uint64Var(&cfg.MaxCacheSizeBytes, prefix+"max-size-bytes", uint64(1*units.Gibibyte), "Maximum size in bytes of in-memory index cache used to speed up blocks index lookups (shared between all tenants).")
}
func (cfg *InMemoryIndexCacheConfig) Validate() error {
	if cfg.MaxItemSizeBytes == 0 {
		cfg.MaxItemSizeBytes = defaultMaxItemSize
	}
	if cfg.MaxItemSizeBytes > cfg.MaxCacheSizeBytes {
		cfg.MaxItemSizeBytes = cfg.MaxCacheSizeBytes
	}
	return nil
}

// NewIndexCache creates a new index cache based on the input configuration.
func NewIndexCache(cfg IndexCacheConfig, logger log.Logger, registerer prometheus.Registerer) (IndexCache, error) {
	switch cfg.Backend {
	case BackendInMemory:
		return NewInMemoryIndexCacheWithConfig(cfg.InMemory, registerer, logger)
	case BackendMemcached:
		return newMemcachedIndexCache(cfg.Memcached, logger, registerer)
	default:
		return nil, errUnsupportedIndexCacheBackend
	}
}

func newMemcachedIndexCache(cfg cache.MemcachedClientConfig, logger log.Logger, registerer prometheus.Registerer) (IndexCache, error) {
	client, err := cache.NewMemcachedClientWithConfig(logger, "index-cache", cfg, prometheus.WrapRegistererWithPrefix("thanos_", registerer))
	if err != nil {
		return nil, errors.Wrap(err, "create index cache memcached client")
	}

	c, err := NewRemoteIndexCache(logger, client, registerer)
	if err != nil {
		return nil, errors.Wrap(err, "create memcached-based index cache")
	}

	return NewTracingIndexCache(c, logger), nil
}
