// SPDX-License-Identifier: AGPL-3.0-only

package indexcache

import (
	"context"
	"time"

	"github.com/go-kit/log"
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

func (t *TracingIndexCache) StorePostings(userID string, blockID ulid.ULID, l labels.Label, v []byte, ttl time.Duration) {
	t.c.StorePostings(userID, blockID, l, v, ttl)
}

func (t *TracingIndexCache) FetchMultiPostings(ctx context.Context, userID string, blockID ulid.ULID, keys []labels.Label) (hits BytesResult) {
	t0 := time.Now()
	hits = t.c.FetchMultiPostings(ctx, userID, blockID, keys)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	spanLogger.DebugLog(
		"msg", "IndexCache.FetchMultiPostings",
		"block", blockID,
		"requested keys", len(keys),
		"cache hits", hits.Remaining(),
		"cache misses", len(keys)-hits.Remaining(),
		"time elapsed", time.Since(t0),
		"returned bytes", hits.Size(),
		"user_id", userID,
	)
	return hits
}

func (t *TracingIndexCache) StoreSeriesForRef(userID string, blockID ulid.ULID, id storage.SeriesRef, v []byte, ttl time.Duration) {
	t.c.StoreSeriesForRef(userID, blockID, id, v, ttl)
}

func (t *TracingIndexCache) FetchMultiSeriesForRefs(ctx context.Context, userID string, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	t0 := time.Now()
	hits, misses = t.c.FetchMultiSeriesForRefs(ctx, userID, blockID, ids)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	spanLogger.DebugLog("msg", "IndexCache.FetchMultiSeriesForRefs",
		"requested series", len(ids),
		"block", blockID,
		"cache hits", len(hits),
		"cache misses", len(misses),
		"time elapsed", time.Since(t0),
		"returned bytes", sumBytes(hits),
		"user_id", userID,
	)

	return hits, misses
}

func (t *TracingIndexCache) StoreExpandedPostings(userID string, blockID ulid.ULID, key LabelMatchersKey, postingsSelectionStrategy string, v []byte) {
	t.c.StoreExpandedPostings(userID, blockID, key, postingsSelectionStrategy, v)
}

func (t *TracingIndexCache) FetchExpandedPostings(ctx context.Context, userID string, blockID ulid.ULID, key LabelMatchersKey, postingsSelectionStrategy string) ([]byte, bool) {
	t0 := time.Now()
	data, found := t.c.FetchExpandedPostings(ctx, userID, blockID, key, postingsSelectionStrategy)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	spanLogger.DebugLog(
		"msg", "IndexCache.FetchExpandedPostings",
		"block", blockID,
		"requested key", key,
		"postings selection strategy", postingsSelectionStrategy,
		"found", found,
		"time elapsed", time.Since(t0),
		"returned bytes", len(data),
		"user_id", userID,
	)

	return data, found
}

func (t *TracingIndexCache) StoreSeriesForPostings(userID string, blockID ulid.ULID, shard *sharding.ShardSelector, postingsKey PostingsKey, v []byte) {
	t.c.StoreSeriesForPostings(userID, blockID, shard, postingsKey, v)
}

func (t *TracingIndexCache) FetchSeriesForPostings(ctx context.Context, userID string, blockID ulid.ULID, shard *sharding.ShardSelector, postingsKey PostingsKey) ([]byte, bool) {
	t0 := time.Now()
	data, found := t.c.FetchSeriesForPostings(ctx, userID, blockID, shard, postingsKey)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	spanLogger.DebugLog(
		"msg", "IndexCache.FetchSeriesForPostings",
		"block", blockID,
		"shard", shardKey(shard),
		"found", found,
		"time_elapsed", time.Since(t0),
		"returned_bytes", len(data),
		"user_id", userID,
		"postings_key", postingsKey,
	)

	return data, found
}

func (t *TracingIndexCache) StoreLabelNames(userID string, blockID ulid.ULID, matchersKey LabelMatchersKey, v []byte) {
	t.c.StoreLabelNames(userID, blockID, matchersKey, v)
}

func (t *TracingIndexCache) FetchLabelNames(ctx context.Context, userID string, blockID ulid.ULID, matchersKey LabelMatchersKey) ([]byte, bool) {
	t0 := time.Now()
	data, found := t.c.FetchLabelNames(ctx, userID, blockID, matchersKey)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	spanLogger.DebugLog(
		"msg", "IndexCache.FetchLabelNames",
		"block", blockID,
		"requested key", matchersKey,
		"found", found,
		"time elapsed", time.Since(t0),
		"returned bytes", len(data),
		"user_id", userID,
	)

	return data, found
}

func (t *TracingIndexCache) StoreLabelValues(userID string, blockID ulid.ULID, labelName string, matchersKey LabelMatchersKey, v []byte) {
	t.c.StoreLabelValues(userID, blockID, labelName, matchersKey, v)
}

func (t *TracingIndexCache) FetchLabelValues(ctx context.Context, userID string, blockID ulid.ULID, labelName string, matchersKey LabelMatchersKey) ([]byte, bool) {
	t0 := time.Now()
	data, found := t.c.FetchLabelValues(ctx, userID, blockID, labelName, matchersKey)

	spanLogger := spanlogger.FromContext(ctx, t.logger)
	spanLogger.DebugLog(
		"msg", "IndexCache.FetchLabelValues",
		"block", blockID,
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
