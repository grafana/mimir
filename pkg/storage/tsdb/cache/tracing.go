// SPDX-License-Identifier: AGPL-3.0-only
package cache

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/cache"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// TracingCache logs Fetch operation in the parent spans.
type TracingCache struct {
	c      cache.Cache
	logger log.Logger
}

func NewTracingCache(cache cache.Cache, logger log.Logger) cache.Cache {
	return TracingCache{c: cache, logger: logger}
}

func (t TracingCache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	t.c.Store(ctx, data, ttl)
}

func (t TracingCache) Fetch(ctx context.Context, keys []string) (result map[string][]byte) {
	var (
		bytes  int
		logger = spanlogger.FromContext(ctx, t.logger)
	)
	result = t.c.Fetch(ctx, keys)

	for _, v := range result {
		bytes += len(v)
	}
	level.Debug(logger).Log("msg", "cache_fetch", "name", t.Name(), "requested keys", len(keys), "returned keys", len(result), "returned bytes", bytes)

	return
}

func (t TracingCache) Name() string {
	return t.c.Name()
}

type TracingIndexCache struct {
	c      IndexCache
	logger log.Logger
}

func NewTracingIndexCache(cache IndexCache, logger log.Logger) IndexCache {
	return &TracingIndexCache{
		c:      cache,
		logger: logger,
	}
}

func (t *TracingIndexCache) StorePostings(ctx context.Context, blockID ulid.ULID, l labels.Label, v []byte) {
	t.c.StorePostings(ctx, blockID, l, v)
}

func (t *TracingIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label) {
	hits, misses = t.c.FetchMultiPostings(ctx, blockID, keys)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	level.Debug(spanLogger).Log("msg", "cache_fetch_postings", "requested keys", len(keys), "cache hits", len(hits), "cache misses", len(misses))

	return hits, misses
}

func (t *TracingIndexCache) StoreSeries(ctx context.Context, blockID ulid.ULID, id storage.SeriesRef, v []byte) {
	t.c.StoreSeries(ctx, blockID, id, v)
}

func (t *TracingIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	hits, misses = t.c.FetchMultiSeries(ctx, blockID, ids)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	level.Debug(spanLogger).Log("msg", "cache_fetch_series", "requested series", len(ids), "cache hits", len(hits), "cache misses", len(misses))

	return hits, misses
}

func (t *TracingIndexCache) StoreExpandedPostings(ctx context.Context, blockID ulid.ULID, key LabelMatchersKey, v []byte) {
	t.c.StoreExpandedPostings(ctx, blockID, key, v)
}

func (t *TracingIndexCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, key LabelMatchersKey) ([]byte, bool) {
	data, found := t.c.FetchExpandedPostings(ctx, blockID, key)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	level.Debug(spanLogger).Log("msg", "cache_fetch_expanded_postings", "requested key", key, "found", found, "returned bytes", len(data))

	return data, found
}
