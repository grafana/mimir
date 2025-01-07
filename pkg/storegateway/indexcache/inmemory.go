// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/inmemory.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.
package indexcache

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	lru "github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/storage/sharding"
)

var DefaultInMemoryIndexCacheConfig = InMemoryIndexCacheConfig{
	MaxSize:     250 * 1024 * 1024,
	MaxItemSize: 125 * 1024 * 1024,
}

const maxInt = int(^uint(0) >> 1)

const (
	stringHeaderSize = 8
	sliceHeaderSize  = 16
)

var ulidSize = uint64(len(ulid.ULID{}))

type InMemoryIndexCache struct {
	mtx sync.Mutex

	logger           log.Logger
	lru              *lru.LRU[cacheKey, []byte]
	maxSizeBytes     uint64
	maxItemSizeBytes uint64

	curSize uint64

	evicted          *prometheus.CounterVec
	requests         *prometheus.CounterVec
	hits             *prometheus.CounterVec
	added            *prometheus.CounterVec
	current          *prometheus.GaugeVec
	currentSize      *prometheus.GaugeVec
	totalCurrentSize *prometheus.GaugeVec
	overflow         *prometheus.CounterVec
}

// InMemoryIndexCacheConfig holds the in-memory index cache config.
type InMemoryIndexCacheConfig struct {
	// MaxSize represents overall maximum number of bytes cache can contain.
	MaxSize flagext.Bytes `yaml:"max_size"`
	// MaxItemSize represents maximum size of single item.
	MaxItemSize flagext.Bytes `yaml:"max_item_size"`
}

// parseInMemoryIndexCacheConfig unmarshals a buffer into a InMemoryIndexCacheConfig with default values.
func parseInMemoryIndexCacheConfig(conf []byte) (InMemoryIndexCacheConfig, error) {
	config := DefaultInMemoryIndexCacheConfig
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return InMemoryIndexCacheConfig{}, err
	}

	return config, nil
}

// NewInMemoryIndexCache creates a new thread-safe LRU cache for index entries and ensures the total cache
// size approximately does not exceed maxBytes.
func NewInMemoryIndexCache(logger log.Logger, reg prometheus.Registerer, conf []byte) (*InMemoryIndexCache, error) {
	config, err := parseInMemoryIndexCacheConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewInMemoryIndexCacheWithConfig(logger, reg, config)
}

// NewInMemoryIndexCacheWithConfig creates a new thread-safe LRU cache for index entries and ensures the total cache
// size approximately does not exceed maxBytes.
func NewInMemoryIndexCacheWithConfig(logger log.Logger, reg prometheus.Registerer, config InMemoryIndexCacheConfig) (*InMemoryIndexCache, error) {
	if config.MaxItemSize > config.MaxSize {
		return nil, errors.Errorf("max item size (%v) cannot be bigger than overall cache size (%v)", config.MaxItemSize, config.MaxSize)
	}

	c := &InMemoryIndexCache{
		logger:           logger,
		maxSizeBytes:     uint64(config.MaxSize),
		maxItemSizeBytes: uint64(config.MaxItemSize),
	}

	c.evicted = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_evicted_total",
		Help: "Total number of items that were evicted from the index cache.",
	}, []string{"item_type"})
	initLabelValuesForAllCacheTypes(c.evicted.MetricVec)

	c.added = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_added_total",
		Help: "Total number of items that were added to the index cache.",
	}, []string{"item_type"})
	initLabelValuesForAllCacheTypes(c.added.MetricVec)

	c.requests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_requests_total",
		Help: "Total number of requests to the cache.",
	}, []string{"item_type"})
	initLabelValuesForAllCacheTypes(c.requests.MetricVec)

	c.overflow = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_overflowed_total",
		Help: "Total number of items that could not be added to the cache due to being too big.",
	}, []string{"item_type"})
	initLabelValuesForAllCacheTypes(c.overflow.MetricVec)

	c.hits = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_hits_total",
		Help: "Total number of requests to the cache that were a hit.",
	}, []string{"item_type"})
	initLabelValuesForAllCacheTypes(c.hits.MetricVec)

	c.current = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_items",
		Help: "Current number of items in the index cache.",
	}, []string{"item_type"})
	initLabelValuesForAllCacheTypes(c.current.MetricVec)

	c.currentSize = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_items_size_bytes",
		Help: "Current byte size of items in the index cache.",
	}, []string{"item_type"})
	initLabelValuesForAllCacheTypes(c.currentSize.MetricVec)

	c.totalCurrentSize = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_total_size_bytes",
		Help: "Current byte size of items (both value and key) in the index cache.",
	}, []string{"item_type"})
	initLabelValuesForAllCacheTypes(c.totalCurrentSize.MetricVec)

	_ = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_max_size_bytes",
		Help: "Maximum number of bytes to be held in the index cache.",
	}, func() float64 {
		return float64(c.maxSizeBytes)
	})
	_ = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_store_index_cache_max_item_size_bytes",
		Help: "Maximum number of bytes for single entry to be held in the index cache.",
	}, func() float64 {
		return float64(c.maxItemSizeBytes)
	})

	// Initialize LRU cache with a high size limit since we will manage evictions ourselves
	// based on stored size using `RemoveOldest` method.
	l, err := lru.NewLRU(maxInt, c.onEvict)
	if err != nil {
		return nil, err
	}
	c.lru = l

	level.Info(logger).Log(
		"msg", "created in-memory index cache",
		"maxItemSizeBytes", c.maxItemSizeBytes,
		"maxSizeBytes", c.maxSizeBytes,
		"maxItems", "maxInt",
	)
	return c, nil
}

func (c *InMemoryIndexCache) onEvict(key cacheKey, val []byte) {
	typ := key.typ()
	entrySize := sliceSize(val)

	c.evicted.WithLabelValues(typ).Inc()
	c.current.WithLabelValues(typ).Dec()
	c.currentSize.WithLabelValues(typ).Sub(float64(entrySize))
	c.totalCurrentSize.WithLabelValues(typ).Sub(float64(entrySize + key.size()))

	c.curSize -= entrySize
}

func (c *InMemoryIndexCache) get(key cacheKey) ([]byte, bool) {
	typ := key.typ()
	c.requests.WithLabelValues(typ).Inc()

	c.mtx.Lock()
	defer c.mtx.Unlock()

	v, ok := c.lru.Get(key)
	if !ok {
		return nil, false
	}
	c.hits.WithLabelValues(typ).Inc()
	return v, true
}

func (c *InMemoryIndexCache) set(key cacheKey, val []byte) {
	typ := key.typ()
	size := sliceSize(val)

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, ok := c.lru.Get(key); ok {
		return
	}

	if !c.ensureFits(size, typ) {
		c.overflow.WithLabelValues(typ).Inc()
		return
	}

	// The caller may be passing in a sub-slice of a huge array. Copy the data
	// to ensure we don't waste huge amounts of space for something small.
	v := make([]byte, len(val))
	copy(v, val)
	c.lru.Add(key, v)

	c.added.WithLabelValues(typ).Inc()
	c.currentSize.WithLabelValues(typ).Add(float64(size))
	c.totalCurrentSize.WithLabelValues(typ).Add(float64(size + key.size()))
	c.current.WithLabelValues(typ).Inc()
	c.curSize += size
}

// ensureFits tries to make sure that the passed slice will fit into the LRU cache.
// Returns true if it will fit.
func (c *InMemoryIndexCache) ensureFits(size uint64, typ string) bool {
	if size > c.maxItemSizeBytes {
		level.Debug(c.logger).Log(
			"msg", "item bigger than maxItemSizeBytes. Ignoring..",
			"maxItemSizeBytes", c.maxItemSizeBytes,
			"maxSizeBytes", c.maxSizeBytes,
			"curSize", c.curSize,
			"itemSize", size,
			"cacheType", typ,
		)
		return false
	}

	for c.curSize+size > c.maxSizeBytes {
		if _, _, ok := c.lru.RemoveOldest(); !ok {
			level.Error(c.logger).Log(
				"msg", "LRU has nothing more to evict, but we still cannot allocate the item. Resetting cache.",
				"maxItemSizeBytes", c.maxItemSizeBytes,
				"maxSizeBytes", c.maxSizeBytes,
				"curSize", c.curSize,
				"itemSize", size,
				"cacheType", typ,
			)
			c.reset()
		}
	}
	return true
}

func (c *InMemoryIndexCache) reset() {
	c.lru.Purge()
	c.current.Reset()
	c.currentSize.Reset()
	c.totalCurrentSize.Reset()
	c.curSize = 0
}

// copyLabel is required as underlying strings might be mmaped.
func copyLabel(l labels.Label) labels.Label {
	return labels.Label{Value: strings.Clone(l.Value), Name: strings.Clone(l.Name)}
}

// StorePostings sets the postings identified by the ulid and label to the value v,
// if the postings already exists in the cache it is not mutated.
func (c *InMemoryIndexCache) StorePostings(userID string, blockID ulid.ULID, l labels.Label, v []byte, _ time.Duration) {
	c.set(cacheKeyPostings{userID, blockID, copyLabel(l)}, v)
}

// FetchMultiPostings fetches multiple postings - each identified by a label.
func (c *InMemoryIndexCache) FetchMultiPostings(_ context.Context, userID string, blockID ulid.ULID, keys []labels.Label) BytesResult {
	hits := map[labels.Label][]byte{}

	for _, key := range keys {
		if b, ok := c.get(cacheKeyPostings{userID, blockID, key}); ok {
			hits[key] = b
			continue
		}
	}

	return &MapIterator[labels.Label]{
		Keys: keys,
		M:    hits,
	}
}

// StoreSeriesForRef sets the series identified by the ulid and id to the value v,
// if the series already exists in the cache it is not mutated.
func (c *InMemoryIndexCache) StoreSeriesForRef(userID string, blockID ulid.ULID, id storage.SeriesRef, v []byte, _ time.Duration) {
	c.set(cacheKeySeriesForRef{userID, blockID, id}, v)
}

// FetchMultiSeriesForRefs fetches multiple series - each identified by ID - from the cache
// and returns a map containing cache hits, along with a list of missing IDs.
func (c *InMemoryIndexCache) FetchMultiSeriesForRefs(_ context.Context, userID string, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	hits = map[storage.SeriesRef][]byte{}

	for _, id := range ids {
		if b, ok := c.get(cacheKeySeriesForRef{userID, blockID, id}); ok {
			hits[id] = b
			continue
		}

		misses = append(misses, id)
	}

	return hits, misses
}

// StoreExpandedPostings stores the encoded result of ExpandedPostings for specified matchers identified by the provided LabelMatchersKey.
func (c *InMemoryIndexCache) StoreExpandedPostings(userID string, blockID ulid.ULID, key LabelMatchersKey, postingsSelectionStrategy string, v []byte) {
	c.set(cacheKeyExpandedPostings{userID, blockID, key, postingsSelectionStrategy}, v)
}

// FetchExpandedPostings fetches the encoded result of ExpandedPostings for specified matchers identified by the provided LabelMatchersKey.
func (c *InMemoryIndexCache) FetchExpandedPostings(_ context.Context, userID string, blockID ulid.ULID, key LabelMatchersKey, postingsSelectionStrategy string) ([]byte, bool) {
	return c.get(cacheKeyExpandedPostings{userID, blockID, key, postingsSelectionStrategy})
}

// StoreSeriesForPostings stores a series set for the provided postings.
func (c *InMemoryIndexCache) StoreSeriesForPostings(userID string, blockID ulid.ULID, shard *sharding.ShardSelector, postingsKey PostingsKey, v []byte) {
	c.set(cacheKeySeriesForPostings{userID, blockID, shardKey(shard), postingsKey}, v)
}

// FetchSeriesForPostings fetches a series set for the provided postings.
func (c *InMemoryIndexCache) FetchSeriesForPostings(_ context.Context, userID string, blockID ulid.ULID, shard *sharding.ShardSelector, postingsKey PostingsKey) ([]byte, bool) {
	return c.get(cacheKeySeriesForPostings{userID, blockID, shardKey(shard), postingsKey})
}

// StoreLabelNames stores the result of a LabelNames() call.
func (c *InMemoryIndexCache) StoreLabelNames(userID string, blockID ulid.ULID, matchersKey LabelMatchersKey, v []byte) {
	c.set(cacheKeyLabelNames{userID, blockID, matchersKey}, v)
}

// FetchLabelNames fetches the result of a LabelNames() call.
func (c *InMemoryIndexCache) FetchLabelNames(_ context.Context, userID string, blockID ulid.ULID, matchersKey LabelMatchersKey) ([]byte, bool) {
	return c.get(cacheKeyLabelNames{userID, blockID, matchersKey})
}

// StoreLabelValues stores the result of a LabelValues() call.
func (c *InMemoryIndexCache) StoreLabelValues(userID string, blockID ulid.ULID, labelName string, matchersKey LabelMatchersKey, v []byte) {
	c.set(cacheKeyLabelValues{userID, blockID, labelName, matchersKey}, v)
}

// FetchLabelValues fetches the result of a LabelValues() call.
func (c *InMemoryIndexCache) FetchLabelValues(_ context.Context, userID string, blockID ulid.ULID, labelName string, matchersKey LabelMatchersKey) ([]byte, bool) {
	return c.get(cacheKeyLabelValues{userID, blockID, labelName, matchersKey})
}

// cacheKey is used by in-memory representation to store cached data.
// The implementations of cacheKey should be hashable, as they will be used as keys for *lru.LRU cache
type cacheKey interface {
	// typ is used as label for metrics.
	typ() string
	// size is used to keep track of the cache size, it represents the footprint of the cache key in memory.
	size() uint64
}

// cacheKeyPostings implements cacheKey and is used to reference a postings cache entry in the inmemory cache.
type cacheKeyPostings struct {
	userID string
	block  ulid.ULID
	label  labels.Label
}

func (c cacheKeyPostings) typ() string { return cacheTypePostings }

func (c cacheKeyPostings) size() uint64 {
	return stringSize(c.userID) + ulidSize + stringSize(c.label.Name) + stringSize(c.label.Value)
}

// cacheKeyPostings implements cacheKey and is used to reference a seriesRef cache entry in the inmemory cache.
type cacheKeySeriesForRef struct {
	userID string
	block  ulid.ULID
	ref    storage.SeriesRef
}

func (c cacheKeySeriesForRef) typ() string { return cacheTypeSeriesForRef }

func (c cacheKeySeriesForRef) size() uint64 {
	return stringSize(c.userID) + ulidSize + 8
}

// cacheKeyPostings implements cacheKey and is used to reference an expanded postings cache entry in the inmemory cache.
type cacheKeyExpandedPostings struct {
	userID                    string
	block                     ulid.ULID
	matchersKey               LabelMatchersKey
	postingsSelectionStrategy string
}

func (c cacheKeyExpandedPostings) typ() string { return cacheTypeExpandedPostings }

func (c cacheKeyExpandedPostings) size() uint64 {
	return stringSize(c.userID) + ulidSize + stringSize(string(c.matchersKey)) + stringSize(c.postingsSelectionStrategy)
}

type cacheKeySeriesForPostings struct {
	userID      string
	block       ulid.ULID
	shard       string
	postingsKey PostingsKey
}

func (c cacheKeySeriesForPostings) typ() string {
	return cacheTypeSeriesForPostings
}

func (c cacheKeySeriesForPostings) size() uint64 {
	return stringSize(c.userID) + ulidSize + stringSize(c.shard) + stringSize(string(c.postingsKey))
}

type cacheKeyLabelNames struct {
	userID      string
	block       ulid.ULID
	matchersKey LabelMatchersKey
}

func (c cacheKeyLabelNames) typ() string {
	return cacheTypeLabelNames
}

func (c cacheKeyLabelNames) size() uint64 {
	return stringSize(c.userID) + ulidSize + stringSize(string(c.matchersKey))
}

type cacheKeyLabelValues struct {
	userID      string
	block       ulid.ULID
	labelName   string
	matchersKey LabelMatchersKey
}

func (c cacheKeyLabelValues) typ() string {
	return cacheTypeLabelValues
}

func (c cacheKeyLabelValues) size() uint64 {
	return stringSize(c.userID) + ulidSize + stringSize(c.labelName) + stringSize(string(c.matchersKey))
}

func stringSize(s string) uint64 {
	return stringHeaderSize + uint64(len(s))
}

func sliceSize(b []byte) uint64 {
	return sliceHeaderSize + uint64(len(b))
}
