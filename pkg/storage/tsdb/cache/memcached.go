// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/memcached.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package cache

import (
	"context"
	"encoding/base64"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"golang.org/x/crypto/blake2b"

	"github.com/grafana/mimir/pkg/storage/sharding"
)

const (
	memcachedDefaultTTL = 24 * time.Hour
)

// MemcachedIndexCache is a memcached-based index cache.
type MemcachedIndexCache struct {
	logger    log.Logger
	memcached cacheutil.MemcachedClient

	// Metrics.
	requests *prometheus.CounterVec
	hits     *prometheus.CounterVec
}

// NewMemcachedIndexCache makes a new MemcachedIndexCache.
func NewMemcachedIndexCache(logger log.Logger, memcached cacheutil.MemcachedClient, reg prometheus.Registerer) (*MemcachedIndexCache, error) {
	c := &MemcachedIndexCache{
		logger:    logger,
		memcached: memcached,
	}

	c.requests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_requests_total",
		Help: "Total number of items requests to the cache.",
	}, []string{"item_type"})
	initLabelValuesForAllCacheTypes(c.requests.MetricVec)

	c.hits = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_hits_total",
		Help: "Total number of items requests to the cache that were a hit.",
	}, []string{"item_type"})
	initLabelValuesForAllCacheTypes(c.hits.MetricVec)

	level.Info(logger).Log("msg", "created memcached index cache")

	return c, nil
}

// set stores a value for the given key in memcached.
func (c *MemcachedIndexCache) set(ctx context.Context, typ string, key string, val []byte) {
	if err := c.memcached.SetAsync(ctx, key, val, memcachedDefaultTTL); err != nil {
		level.Error(c.logger).Log("msg", "failed to cache in memcached", "type", typ, "err", err)
	}
}

// get retrieves a single value from memcached, returned bool value indicates whether the value was found or not.
func (c *MemcachedIndexCache) get(ctx context.Context, typ string, key string) ([]byte, bool) {
	c.requests.WithLabelValues(typ).Inc()
	results := c.memcached.GetMulti(ctx, []string{key})
	data, ok := results[key]
	if ok {
		c.hits.WithLabelValues(typ).Inc()
	}
	return data, ok
}

// StorePostings sets the postings identified by the ulid and label to the value v.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *MemcachedIndexCache) StorePostings(ctx context.Context, userID string, blockID ulid.ULID, l labels.Label, v []byte) {
	c.set(ctx, cacheTypePostings, postingsCacheKey(userID, blockID, l), v)
}

// FetchMultiPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
// In case of error, it logs and return an empty cache hits map.
func (c *MemcachedIndexCache) FetchMultiPostings(ctx context.Context, userID string, blockID ulid.ULID, lbls []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label) {
	// Build the cache keys, while keeping a map between input matchers and the cache key
	// so that we can easily reverse it back after the GetMulti().
	keys := make([]string, 0, len(lbls))
	keysMapping := map[labels.Label]string{}

	for _, lbl := range lbls {
		key := postingsCacheKey(userID, blockID, lbl)

		keys = append(keys, key)
		keysMapping[lbl] = key
	}

	// Fetch the keys from memcached in a single request.
	c.requests.WithLabelValues(cacheTypePostings).Add(float64(len(keys)))
	results := c.memcached.GetMulti(ctx, keys)
	if len(results) == 0 {
		return nil, lbls
	}

	// Construct the resulting hits map and list of missing keys. We iterate on the input
	// list of labels to be able to easily create the list of ones in a single iteration.
	hits = map[labels.Label][]byte{}

	for _, lbl := range lbls {
		key, ok := keysMapping[lbl]
		if !ok {
			level.Error(c.logger).Log("msg", "keys mapping inconsistency found in memcached index cache client", "type", "postings", "label", lbl.Name+":"+lbl.Value)
			continue
		}

		// Check if the key has been found in memcached. If not, we add it to the list
		// of missing keys.
		value, ok := results[key]
		if !ok {
			misses = append(misses, lbl)
			continue
		}

		hits[lbl] = value
	}

	c.hits.WithLabelValues(cacheTypePostings).Add(float64(len(hits)))
	return hits, misses
}

func postingsCacheKey(userID string, blockID ulid.ULID, l labels.Label) string {
	// Use cryptographically hash functions to avoid hash collisions
	// which would end up in wrong query results.
	lblHash := blake2b.Sum256([]byte(l.Name + ":" + l.Value))
	return "P:" + userID + ":" + blockID.String() + ":" + base64.RawURLEncoding.EncodeToString(lblHash[0:])
}

// StoreSeriesForRef sets the series identified by the ulid and id to the value v.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *MemcachedIndexCache) StoreSeriesForRef(ctx context.Context, userID string, blockID ulid.ULID, id storage.SeriesRef, v []byte) {
	c.set(ctx, cacheTypeSeriesForRef, seriesForRefCacheKey(userID, blockID, id), v)
}

// FetchMultiSeriesForRefs fetches multiple series - each identified by ID - from the cache
// and returns a map containing cache hits, along with a list of missing IDs.
// In case of error, it logs and return an empty cache hits map.
func (c *MemcachedIndexCache) FetchMultiSeriesForRefs(ctx context.Context, userID string, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	// Build the cache keys, while keeping a map between input id and the cache key
	// so that we can easily reverse it back after the GetMulti().
	keys := make([]string, 0, len(ids))
	keysMapping := map[storage.SeriesRef]string{}

	for _, id := range ids {
		key := seriesForRefCacheKey(userID, blockID, id)

		keys = append(keys, key)
		keysMapping[id] = key
	}

	// Fetch the keys from memcached in a single request.
	c.requests.WithLabelValues(cacheTypeSeriesForRef).Add(float64(len(ids)))
	results := c.memcached.GetMulti(ctx, keys)
	if len(results) == 0 {
		return nil, ids
	}

	// Construct the resulting hits map and list of missing keys. We iterate on the input
	// list of ids to be able to easily create the list of ones in a single iteration.
	hits = map[storage.SeriesRef][]byte{}

	for _, id := range ids {
		key, ok := keysMapping[id]
		if !ok {
			_ = level.Error(c.logger).Log("msg", "keys mapping inconsistency found in memcached index cache client", "type", "series", "id", id)
			continue
		}

		// Check if the key has been found in memcached. If not, we add it to the list
		// of missing keys.
		value, ok := results[key]
		if !ok {
			misses = append(misses, id)
			continue
		}

		hits[id] = value
	}

	c.hits.WithLabelValues(cacheTypeSeriesForRef).Add(float64(len(hits)))
	return hits, misses
}

func seriesForRefCacheKey(userID string, blockID ulid.ULID, id storage.SeriesRef) string {
	return "S:" + userID + ":" + blockID.String() + ":" + strconv.FormatUint(uint64(id), 10)
}

// StoreExpandedPostings stores the encoded result of ExpandedPostings for specified matchers identified by the provided LabelMatchersKey.
func (c *MemcachedIndexCache) StoreExpandedPostings(ctx context.Context, userID string, blockID ulid.ULID, lmKey LabelMatchersKey, v []byte) {
	c.set(ctx, cacheTypeExpandedPostings, expandedPostingsCacheKey(userID, blockID, lmKey), v)
}

// FetchExpandedPostings fetches the encoded result of ExpandedPostings for specified matchers identified by the provided LabelMatchersKey.
func (c *MemcachedIndexCache) FetchExpandedPostings(ctx context.Context, userID string, blockID ulid.ULID, lmKey LabelMatchersKey) ([]byte, bool) {
	return c.get(ctx, cacheTypeExpandedPostings, expandedPostingsCacheKey(userID, blockID, lmKey))
}

func expandedPostingsCacheKey(userID string, blockID ulid.ULID, lmKey LabelMatchersKey) string {
	hash := blake2b.Sum256([]byte(lmKey))
	return "E:" + userID + ":" + blockID.String() + ":" + base64.RawURLEncoding.EncodeToString(hash[0:])
}

// StoreSeries stores the result of a Series() call.
func (c *MemcachedIndexCache) StoreSeries(ctx context.Context, userID string, blockID ulid.ULID, matchersKey LabelMatchersKey, shard *sharding.ShardSelector, v []byte) {
	c.set(ctx, cacheTypeSeries, seriesCacheKey(userID, blockID, matchersKey, shard), v)
}

// FetchSeries fetches the result of a Series() call.
func (c *MemcachedIndexCache) FetchSeries(ctx context.Context, userID string, blockID ulid.ULID, matchersKey LabelMatchersKey, shard *sharding.ShardSelector) ([]byte, bool) {
	return c.get(ctx, cacheTypeSeries, seriesCacheKey(userID, blockID, matchersKey, shard))
}

func seriesCacheKey(userID string, blockID ulid.ULID, matchersKey LabelMatchersKey, shard *sharding.ShardSelector) string {
	hash := blake2b.Sum256([]byte(matchersKey))
	// We use SS: as S: is already used for SeriesForRef
	return "SS:" + userID + ":" + blockID.String() + ":" + shardKey(shard) + ":" + base64.RawURLEncoding.EncodeToString(hash[0:])
}

// StoreLabelNames stores the result of a LabelNames() call.
func (c *MemcachedIndexCache) StoreLabelNames(ctx context.Context, userID string, blockID ulid.ULID, matchersKey LabelMatchersKey, v []byte) {
	c.set(ctx, cacheTypeLabelNames, labelNamesCacheKey(userID, blockID, matchersKey), v)
}

// FetchLabelNames fetches the result of a LabelNames() call.
func (c *MemcachedIndexCache) FetchLabelNames(ctx context.Context, userID string, blockID ulid.ULID, matchersKey LabelMatchersKey) ([]byte, bool) {
	return c.get(ctx, cacheTypeLabelNames, labelNamesCacheKey(userID, blockID, matchersKey))
}

func labelNamesCacheKey(userID string, blockID ulid.ULID, matchersKey LabelMatchersKey) string {
	hash := blake2b.Sum256([]byte(matchersKey))
	return "LN:" + userID + ":" + blockID.String() + ":" + base64.RawURLEncoding.EncodeToString(hash[0:])
}

// StoreLabelValues stores the result of a LabelValues() call.
func (c *MemcachedIndexCache) StoreLabelValues(ctx context.Context, userID string, blockID ulid.ULID, labelName string, matchersKey LabelMatchersKey, v []byte) {
	c.set(ctx, cacheTypeLabelValues, labelValuesCacheKey(userID, blockID, labelName, matchersKey), v)
}

// FetchLabelValues fetches the result of a LabelValues() call.
func (c *MemcachedIndexCache) FetchLabelValues(ctx context.Context, userID string, blockID ulid.ULID, labelName string, matchersKey LabelMatchersKey) ([]byte, bool) {
	return c.get(ctx, cacheTypeLabelValues, labelValuesCacheKey(userID, blockID, labelName, matchersKey))
}

func labelValuesCacheKey(userID string, blockID ulid.ULID, labelName string, matchersKey LabelMatchersKey) string {
	hash := blake2b.Sum256([]byte(matchersKey))
	return "LV:" + userID + ":" + blockID.String() + ":" + labelName + ":" + base64.RawURLEncoding.EncodeToString(hash[0:])
}
