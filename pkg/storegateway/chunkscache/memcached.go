// SPDX-License-Identifier: AGPL-3.0-only

package chunkscache

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/util/pool"
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
		Help: "Total number of items requests to the cache.",
	})

	c.hits = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_chunks_cache_hits_total",
		Help: "Total number of items requests to the cache that were a hit.",
	})

	level.Info(logger).Log("msg", "created memcached index cache")

	return c, nil
}

func (c *MemcachedChunksCache) FetchMultiChunks(ctx context.Context, userID string, blockID ulid.ULID, refs []chunks.ChunkRef, bytesPool *pool.SafeSlabPool[byte]) (hits map[chunks.ChunkRef][]byte) {
	c.requests.Add(float64(len(refs)))

	keysMap := make(map[string]chunks.ChunkRef, len(refs))
	keys := make([]string, 0, len(refs))
	for idx, ref := range refs {
		key := chunksKey(userID, blockID, ref)
		keysMap[key] = refs[idx]
		keys = append(keys, key)
	}

	hitBytes := c.memcached.GetMulti(ctx, keys, cache.WithAllocator(slabPoolAllocator{bytesPool}))
	if len(hitBytes) > 0 {
		hits = make(map[chunks.ChunkRef][]byte, len(hitBytes))
	}

	for key, b := range hitBytes {
		hits[keysMap[key]] = b
	}
	c.hits.Add(float64(len(hits)))
	return
}

func (c *MemcachedChunksCache) StoreChunk(ctx context.Context, userID string, blockID ulid.ULID, ref chunks.ChunkRef, data []byte) {
	err := c.memcached.SetAsync(ctx, chunksKey(userID, blockID, ref), data, memcachedDefaultTTL)
	if err != nil {
		level.Warn(c.logger).Log("msg", "storing chunks", "err", err)
	}
}

func chunksKey(userID string, blockID ulid.ULID, ref chunks.ChunkRef) string {
	return fmt.Sprintf("C:%s:%s:%d", userID, blockID, ref)
}

type slabPoolAllocator struct {
	p *pool.SafeSlabPool[byte]
}

func (s slabPoolAllocator) Get(sz int) *[]byte {
	b := s.p.Get(sz)
	return &b
}

func (s slabPoolAllocator) Put(*[]byte) {}
