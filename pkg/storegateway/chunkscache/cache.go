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
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// Range is a contiguous range of chunks. Start is the ref of the first chunk and NumChunks is how many chunks
// there are in the range.
type Range struct {
	BlockID   ulid.ULID
	Start     chunks.ChunkRef
	NumChunks int
}

type Cache interface {
	FetchMultiChunks(ctx context.Context, userID string, ranges []Range, chunksPool *pool.SafeSlabPool[byte]) (hits map[Range][]byte)
	StoreChunks(userID string, ranges map[Range][]byte)
}

type TracingCache struct {
	c Cache
	l log.Logger
}

func NewTracingCache(c Cache, l log.Logger) TracingCache {
	return TracingCache{
		c: c,
		l: l,
	}
}

func (c TracingCache) FetchMultiChunks(ctx context.Context, userID string, ranges []Range, chunksPool *pool.SafeSlabPool[byte]) (hits map[Range][]byte) {
	hits = c.c.FetchMultiChunks(ctx, userID, ranges, chunksPool)

	l := spanlogger.FromContext(ctx, c.l)
	level.Debug(l).Log(
		"name", "ChunksCache.FetchMultiChunks",
		"ranges_requested", len(ranges),
		"ranges_hit", len(hits),
		"ranges_hit_bytes", hitsSize(hits),
		"ranges_misses", len(ranges)-len(hits),
	)
	return
}

func hitsSize(hits map[Range][]byte) (size int) {
	for _, b := range hits {
		size += len(b)
	}
	return
}

func (c TracingCache) StoreChunks(userID string, ranges map[Range][]byte) {
	c.c.StoreChunks(userID, ranges)
}

type ChunksCache struct {
	logger log.Logger
	cache  cache.Cache

	// TODO these two will soon be tracked by the dskit, we can remove them once https://github.com/grafana/mimir/pull/4078 is merged
	requests prometheus.Counter
	hits     prometheus.Counter
}

type NoopCache struct{}

func (NoopCache) FetchMultiChunks(_ context.Context, _ string, _ []Range, _ *pool.SafeSlabPool[byte]) (hits map[Range][]byte) {
	return nil
}

func (NoopCache) StoreChunks(_ string, _ map[Range][]byte) {
}

func NewChunksCache(logger log.Logger, client cache.Cache, reg prometheus.Registerer) (*ChunksCache, error) {
	c := &ChunksCache{
		logger: logger,
		cache:  client,
	}

	c.requests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_chunks_cache_requests_total",
		Help: "Total number of items requested from the cache.",
	})

	c.hits = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_chunks_cache_hits_total",
		Help: "Total number of items retrieved from the cache.",
	})

	level.Info(logger).Log("msg", "created chunks cache")

	return c, nil
}

func (c *ChunksCache) FetchMultiChunks(ctx context.Context, userID string, ranges []Range, chunksPool *pool.SafeSlabPool[byte]) (hits map[Range][]byte) {
	keysMap := make(map[string]Range, len(ranges))
	keys := make([]string, len(ranges))
	for i, r := range ranges {
		k := chunksKey(userID, r)
		keysMap[k] = ranges[i]
		keys[i] = k
	}

	hitBytes := c.cache.Fetch(ctx, keys, cache.WithAllocator(pool.NewSafeSlabPoolAllocator(chunksPool)))
	if len(hitBytes) > 0 {
		hits = make(map[Range][]byte, len(hitBytes))
		for key, b := range hitBytes {
			hits[keysMap[key]] = b
		}
	}

	c.requests.Add(float64(len(ranges)))
	c.hits.Add(float64(len(hits)))
	return
}

func chunksKey(userID string, r Range) string {
	return fmt.Sprintf("C:%s:%s:%d:%d", userID, r.BlockID, r.Start, r.NumChunks)
}

const (
	defaultTTL = 7 * 24 * time.Hour
)

func (c *ChunksCache) StoreChunks(userID string, ranges map[Range][]byte) {
	rangesWithTenant := make(map[string][]byte, len(ranges))
	for r, v := range ranges {
		rangesWithTenant[chunksKey(userID, r)] = v
	}
	c.cache.StoreAsync(rangesWithTenant, defaultTTL)
}
