// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/index_cache.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package indexcache

import (
	"github.com/alecthomas/units"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/model"

	cache2 "github.com/grafana/mimir/pkg/cache"
	"github.com/grafana/mimir/pkg/storage/tsdb"
)

const defaultMaxItemSize = model.Bytes(128 * units.MiB)

// NewIndexCache creates a new index cache based on the input configuration.
func NewIndexCache(cfg tsdb.IndexCacheConfig, logger log.Logger, registerer prometheus.Registerer) (IndexCache, error) {
	switch cfg.Backend {
	case tsdb.IndexCacheBackendInMemory:
		return newInMemoryIndexCache(cfg.InMemory, logger, registerer)
	case tsdb.IndexCacheBackendMemcached:
		return newMemcachedIndexCache(cfg.Memcached, logger, registerer)
	default:
		return nil, tsdb.ErrUnsupportedIndexCacheBackend
	}
}

func newInMemoryIndexCache(cfg tsdb.InMemoryIndexCacheConfig, logger log.Logger, registerer prometheus.Registerer) (IndexCache, error) {
	maxCacheSize := model.Bytes(cfg.MaxSizeBytes)

	// Calculate the max item size.
	maxItemSize := defaultMaxItemSize
	if maxItemSize > maxCacheSize {
		maxItemSize = maxCacheSize
	}

	return NewInMemoryIndexCacheWithConfig(logger, registerer, InMemoryIndexCacheConfig{
		MaxSize:     maxCacheSize,
		MaxItemSize: maxItemSize,
	})
}

func newMemcachedIndexCache(cfg cache2.MemcachedConfig, logger log.Logger, registerer prometheus.Registerer) (IndexCache, error) {
	client, err := cacheutil.NewMemcachedClientWithConfig(logger, "index-cache", cfg.ToMemcachedClientConfig(), registerer)
	if err != nil {
		return nil, errors.Wrap(err, "create index cache memcached client")
	}

	cache, err := NewMemcachedIndexCache(logger, client, registerer)
	if err != nil {
		return nil, errors.Wrap(err, "create memcached-based index cache")
	}

	return NewTracingIndexCache(cache, logger), nil
}
