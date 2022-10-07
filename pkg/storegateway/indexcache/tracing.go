// SPDX-License-Identifier: AGPL-3.0-only

package indexcache

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

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

func (t *TracingIndexCache) StorePostings(ctx context.Context, userID string, blockID ulid.ULID, l labels.Label, v []byte) {
	t.c.StorePostings(ctx, userID, blockID, l, v)
}

func (t *TracingIndexCache) FetchMultiPostings(ctx context.Context, userID string, blockID ulid.ULID, keys []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label) {
	t0 := time.Now()
	hits, misses = t.c.FetchMultiPostings(ctx, userID, blockID, keys)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	level.Debug(spanLogger).Log(
		"msg", "IndexCache.FetchMultiPostings",
		"requested keys", len(keys),
		"cache hits", len(hits),
		"cache misses", len(misses),
		"time elapsed", time.Since(t0),
		"returned bytes", sumBytes(hits),
		"user_id", userID,
	)
	return hits, misses
}

func (t *TracingIndexCache) StoreSeriesForRef(ctx context.Context, userID string, blockID ulid.ULID, id storage.SeriesRef, v []byte) {
	t.c.StoreSeriesForRef(ctx, userID, blockID, id, v)
}

func (t *TracingIndexCache) FetchMultiSeriesForRefs(ctx context.Context, userID string, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	t0 := time.Now()
	hits, misses = t.c.FetchMultiSeriesForRefs(ctx, userID, blockID, ids)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	level.Debug(spanLogger).Log("msg", "IndexCache.FetchMultiSeriesForRefs",
		"requested series", len(ids),
		"cache hits", len(hits),
		"cache misses", len(misses),
		"time elapsed", time.Since(t0),
		"returned bytes", sumBytes(hits),
		"user_id", userID,
	)

	return hits, misses
}

func (t *TracingIndexCache) StoreExpandedPostings(ctx context.Context, userID string, blockID ulid.ULID, key LabelMatchersKey, v []byte) {
	t.c.StoreExpandedPostings(ctx, userID, blockID, key, v)
}

func (t *TracingIndexCache) FetchExpandedPostings(ctx context.Context, userID string, blockID ulid.ULID, key LabelMatchersKey) ([]byte, bool) {
	t0 := time.Now()
	data, found := t.c.FetchExpandedPostings(ctx, userID, blockID, key)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	level.Debug(spanLogger).Log(
		"msg", "IndexCache.FetchExpandedPostings",
		"requested key", key,
		"found", found,
		"time elapsed", time.Since(t0),
		"returned bytes", len(data),
		"user_id", userID,
	)

	return data, found
}

func (t *TracingIndexCache) StoreSeries(ctx context.Context, userID string, blockID ulid.ULID, matchersKey LabelMatchersKey, shard *sharding.ShardSelector, v []byte) {
	t.c.StoreSeries(ctx, userID, blockID, matchersKey, shard, v)
}

func (t *TracingIndexCache) FetchSeries(ctx context.Context, userID string, blockID ulid.ULID, matchersKey LabelMatchersKey, shard *sharding.ShardSelector) ([]byte, bool) {
	t0 := time.Now()
	data, found := t.c.FetchSeries(ctx, userID, blockID, matchersKey, shard)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	level.Debug(spanLogger).Log(
		"msg", "IndexCache.FetchSeries",
		"requested key", matchersKey,
		"shard", shardKey(shard),
		"found", found,
		"time elapsed", time.Since(t0),
		"returned bytes", len(data),
		"user_id", userID,
	)

	return data, found
}

func (t *TracingIndexCache) StoreLabelNames(ctx context.Context, userID string, blockID ulid.ULID, matchersKey LabelMatchersKey, v []byte) {
	t.c.StoreLabelNames(ctx, userID, blockID, matchersKey, v)
}

func (t *TracingIndexCache) FetchLabelNames(ctx context.Context, userID string, blockID ulid.ULID, matchersKey LabelMatchersKey) ([]byte, bool) {
	t0 := time.Now()
	data, found := t.c.FetchLabelNames(ctx, userID, blockID, matchersKey)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	level.Debug(spanLogger).Log(
		"msg", "IndexCache.FetchLabelNames",
		"requested key", matchersKey,
		"found", found,
		"time elapsed", time.Since(t0),
		"returned bytes", len(data),
		"user_id", userID,
	)

	return data, found
}

func (t *TracingIndexCache) StoreLabelValues(ctx context.Context, userID string, blockID ulid.ULID, labelName string, matchersKey LabelMatchersKey, v []byte) {
	t.c.StoreLabelValues(ctx, userID, blockID, labelName, matchersKey, v)
}

func (t *TracingIndexCache) FetchLabelValues(ctx context.Context, userID string, blockID ulid.ULID, labelName string, matchersKey LabelMatchersKey) ([]byte, bool) {
	t0 := time.Now()
	data, found := t.c.FetchLabelValues(ctx, userID, blockID, labelName, matchersKey)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	level.Debug(spanLogger).Log(
		"msg", "IndexCache.FetchLabelValues",
		"label name", labelName,
		"requested key", matchersKey,
		"found", found,
		"time elapsed", time.Since(t0),
		"returned bytes", len(data),
		"user_id", userID,
	)

	return data, found
}

func sumBytes[T comparable](res map[T][]byte) int {
	sum := 0
	for _, v := range res {
		sum += len(v)
	}
	return sum
}
