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

	"github.com/grafana/mimir/pkg/storage/sharding"
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

func (t *TracingIndexCache) StoreSeriesForRef(ctx context.Context, blockID ulid.ULID, id storage.SeriesRef, v []byte) {
	t.c.StoreSeriesForRef(ctx, blockID, id, v)
}

func (t *TracingIndexCache) FetchMultiSeriesForRefs(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	hits, misses = t.c.FetchMultiSeriesForRefs(ctx, blockID, ids)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	level.Debug(spanLogger).Log("msg", "cache_fetch_series_for_ref", "requested series", len(ids), "cache hits", len(hits), "cache misses", len(misses))

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

func (t *TracingIndexCache) StoreSeries(ctx context.Context, blockID ulid.ULID, matchersKey LabelMatchersKey, shard *sharding.ShardSelector, v []byte) {
	t.c.StoreSeries(ctx, blockID, matchersKey, shard, v)
}

func (t *TracingIndexCache) FetchSeries(ctx context.Context, blockID ulid.ULID, matchersKey LabelMatchersKey, shard *sharding.ShardSelector) ([]byte, bool) {
	data, found := t.c.FetchSeries(ctx, blockID, matchersKey, shard)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	level.Debug(spanLogger).Log("msg", "cache_fetch_series", "requested key", matchersKey, "shard", shardKey(shard), "found", found, "returned bytes", len(data))

	return data, found
}

func (t *TracingIndexCache) StoreLabelNames(ctx context.Context, blockID ulid.ULID, matchersKey LabelMatchersKey, v []byte) {
	t.c.StoreLabelNames(ctx, blockID, matchersKey, v)
}

func (t *TracingIndexCache) FetchLabelNames(ctx context.Context, blockID ulid.ULID, matchersKey LabelMatchersKey) ([]byte, bool) {
	data, found := t.c.FetchLabelNames(ctx, blockID, matchersKey)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	level.Debug(spanLogger).Log("msg", "cache_fetch_label_names", "requested key", matchersKey, "found", found, "returned bytes", len(data))

	return data, found
}

func (t *TracingIndexCache) StoreLabelValues(ctx context.Context, blockID ulid.ULID, labelName string, matchersKey LabelMatchersKey, v []byte) {
	t.c.StoreLabelValues(ctx, blockID, labelName, matchersKey, v)
}

func (t *TracingIndexCache) FetchLabelValues(ctx context.Context, blockID ulid.ULID, labelName string, matchersKey LabelMatchersKey) ([]byte, bool) {
	data, found := t.c.FetchLabelValues(ctx, blockID, labelName, matchersKey)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	level.Debug(spanLogger).Log("msg", "cache_fetch_label_values", "label name", labelName, "requested key", matchersKey, "found", found, "returned bytes", len(data))

	return data, found
}
