// SPDX-License-Identifier: AGPL-3.0-only

package chunkscache

import (
	"context"
	"fmt"
	"time"

	gokit_log "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/oklog/ulid"
	"github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type Range struct {
	BlockID   ulid.ULID
	Start     chunks.ChunkRef
	NumChunks int
}

type ChunksCache interface {
	FetchMultiChunks(ctx context.Context, userID string, ranges []Range) (hits map[Range][]byte)
	StoreChunks(ctx context.Context, userID string, r Range, v []byte)
}

type TracingCache struct {
	C ChunksCache
}

func (c TracingCache) FetchMultiChunks(ctx context.Context, userID string, ranges []Range) (hits map[Range][]byte) {
	l := spanlogger.FromContext(ctx, gokit_log.NewNopLogger())
	hits = c.C.FetchMultiChunks(ctx, userID, ranges)

	l.LogFields(
		log.String("name", "ChunksCache.FetchMultiChunks"),
		log.Int("requested", len(ranges)),
		log.Int("hits", len(hits)),
		log.Int("hits_bytes", hitsSize(hits)),
		log.Int("misses", len(ranges)-len(hits)),
	)
	return
}

func hitsSize(hits map[Range][]byte) (size int) {
	for _, b := range hits {
		size += len(b)
	}
	return
}

func (c TracingCache) StoreChunks(ctx context.Context, userID string, r Range, v []byte) {
	c.C.StoreChunks(ctx, userID, r, v)
}

const (
	defaultTTL = 7 * 24 * time.Hour
)

// DskitChunksCache is a cache-based index cache.
type DskitChunksCache struct {
	logger gokit_log.Logger
	cache  cache.Cache

	// TODO these two will soon be tracked by the dskit, we can remove them once https://github.com/grafana/mimir/pull/4078 is marged
	requests prometheus.Counter
	hits     prometheus.Counter
}

// NewDskitCache makes a new DskitChunksCache.
func NewDskitCache(logger gokit_log.Logger, client cache.Cache, reg prometheus.Registerer) (*DskitChunksCache, error) {
	c := &DskitChunksCache{
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

func (c *DskitChunksCache) FetchMultiChunks(ctx context.Context, userID string, ranges []Range) (hits map[Range][]byte) {
	keysMap := make(map[string]Range, len(ranges))
	keys := make([]string, len(ranges))
	for i, r := range ranges {
		k := chunksKey(userID, r)
		keysMap[k] = ranges[i]
		keys[i] = k
	}

	hitBytes := c.cache.Fetch(ctx, keys)
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

func (c *DskitChunksCache) StoreChunks(ctx context.Context, userID string, r Range, v []byte) {
	c.cache.Store(ctx, map[string][]byte{chunksKey(userID, r): v}, defaultTTL)
}
