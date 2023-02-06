// SPDX-License-Identifier: AGPL-3.0-only

package chunkscache

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	memcachedDefaultTTL = 7 * 24 * time.Hour
)

// MemcachedChunksCache is a memcached-based index cache.
type MemcachedChunksCache struct {
	logger    log.Logger
	memcached cache.MemcachedClient

	// Metrics.
	requests prometheus.Counter
	hits     prometheus.Counter
}

// NewMemcachedChunksCache makes a new MemcachedChunksCache.
func NewMemcachedChunksCache(logger log.Logger, memcached cache.MemcachedClient, reg prometheus.Registerer) (*MemcachedChunksCache, error) {
	c := &MemcachedChunksCache{
		logger:    logger,
		memcached: memcached,
	}

	c.requests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_chunks_cache_requests_total",
		Help: "Total number of items requested from the cache.",
	})

	c.hits = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_chunks_cache_hits_total",
		Help: "Total number of items retrieved from the cache.",
	})

	level.Info(logger).Log("msg", "created memcached chunks cache")

	return c, nil
}

func (c *MemcachedChunksCache) FetchMultiChunks(ctx context.Context, userID string, ranges []Range) (hits map[Range][]byte) {
	keysMap := make(map[string]Range, len(ranges))
	keys := make([]string, len(ranges))
	for i, r := range ranges {
		k := chunksKey(userID, r)
		keysMap[k] = ranges[i]
		keys[i] = k
	}

	hitBytes := c.memcached.GetMulti(ctx, keys)
	if len(hitBytes) > 0 {
		hits = make(map[Range][]byte, len(hitBytes))
	}

	for key, b := range hitBytes {
		hits[keysMap[key]] = b
	}
	c.requests.Add(float64(len(ranges)))
	c.hits.Add(float64(len(hits)))
	return
}

func chunksKey(userID string, r Range) string {
	return fmt.Sprintf("C:%s:%s:%d:%d", userID, r.BlockID, r.Start, r.NumChunks)
}

func (c *MemcachedChunksCache) StoreChunks(ctx context.Context, userID string, r Range, v []byte) {
	err := c.memcached.SetAsync(ctx, chunksKey(userID, r), v, memcachedDefaultTTL)
	if err != nil {
		level.Warn(c.logger).Log("msg", "storing chunks", "err", err)
	}
}
