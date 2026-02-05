// SPDX-License-Identifier: AGPL-3.0-only

package index

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	lru "github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/tsdb/indexcache"
)

const maxInt = int(^uint(0) >> 1)
const (
	stringHeaderSize = 8
	sliceHeaderSize  = 16
)

var ulidSize = uint64(len(ulid.ULID{}))

// InMemoryCacheKey defines behavior required for in-memory cache implementations.
// InMemoryCacheKey implementations do not need to pre-hash keys.
// Hashing & potential collision is handled internally by lru.LRU caches or other map-based types.
type InMemoryCacheKey interface {
	// Size represents the footprint of the cache key in memory
	// to track cache size and check whether eviction is required to add a new entry.
	Size() uint64
}

type InMemoryPostingsOffsetCacheKey struct {
	tenantID string
	blockID  ulid.ULID
	lbl      labels.Label
}

// Key implements CacheKey, specific to InMemoryCacheKey where it does not need to be pre-hashed.
func (k InMemoryPostingsOffsetCacheKey) Key() InMemoryPostingsOffsetCacheKey {
	return k
}

// Size implements InMemoryCacheKey
func (k InMemoryPostingsOffsetCacheKey) Size() uint64 {
	return stringSize(k.tenantID) + ulidSize + stringSize(k.lbl.Name) + stringSize(k.lbl.Value)
}

type InMemoryPostingsOffsetTableCache struct {
	maxCacheSizeBytes uint64
	maxItemSizeBytes  uint64

	valCodec PostingsOffsetCacheCodec

	mtx     sync.Mutex
	lru     *lru.LRU[InMemoryCacheKey, []byte]
	curSize uint64

	logger log.Logger
}

func NewInMemoryPostingsOffsetTableCacheWithConfig(
	config indexcache.InMemoryIndexCacheConfig,
	logger log.Logger,
) (*InMemoryPostingsOffsetTableCache, error) {
	if config.MaxItemSizeBytes > config.MaxCacheSizeBytes {
		return nil, errors.Errorf("max item size (%v) cannot be bigger than overall cache size (%v)", config.MaxItemSizeBytes, config.MaxCacheSizeBytes)
	}

	c := &InMemoryPostingsOffsetTableCache{
		maxCacheSizeBytes: config.MaxCacheSizeBytes,
		maxItemSizeBytes:  config.MaxItemSizeBytes,
		valCodec:          BigEndianPostingsOffsetCodec{},
		logger:            logger,
	}

	l, err := lru.NewLRU[InMemoryCacheKey, []byte](maxInt, c.onEvict)
	if err != nil {
		return nil, err
	}
	c.lru = l

	level.Info(logger).Log(
		"msg", "created in-memory index cache",
		"maxItemSizeBytes", c.maxItemSizeBytes,
		"maxSizeBytes", c.maxCacheSizeBytes,
		"maxItems", "maxInt",
	)
	return c, nil
}

func (c *InMemoryPostingsOffsetTableCache) StorePostingsOffset(tenantID string, blockID ulid.ULID, lbl labels.Label, rng index.Range, _ time.Duration) {
	key := InMemoryPostingsOffsetCacheKey{tenantID, blockID, lbl}
	c.set(key, rng)
}

func (c *InMemoryPostingsOffsetTableCache) FetchPostingsOffset(_ context.Context, tenantID string, blockID ulid.ULID, lbl labels.Label) (index.Range, bool) {
	key := InMemoryPostingsOffsetCacheKey{tenantID, blockID, lbl}
	return c.get(key)
}

func (c *InMemoryPostingsOffsetTableCache) get(key InMemoryCacheKey) (index.Range, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	val, ok := c.lru.Get(key)
	if !ok {
		return index.Range{}, false
	}

	rng, err := c.valCodec.Decode(val)
	if err != nil {
		level.Error(c.logger).Log(
			"msg", "error decoding cache value to index.Range",
			"key", key,
			"value", val,
		)
		c.lru.Remove(key)
		return index.Range{}, false
	}

	return rng, true
}

func (c *InMemoryPostingsOffsetTableCache) set(key InMemoryCacheKey, rng index.Range) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, ok := c.lru.Get(key); ok {
		return
	}
	val := c.valCodec.Encode(rng)
	valSize := sliceSize(val)

	if valSize > c.maxItemSizeBytes {
		level.Debug(c.logger).Log(
			"msg", "item bigger than maxItemSizeBytes. Ignoring...",
			"maxItemSizeBytes", c.maxItemSizeBytes,
			"maxSizeBytes", c.maxCacheSizeBytes,
			"curSize", c.curSize,
			"itemSize", valSize,
		)
		return
	}

	for c.curSize+valSize > c.maxCacheSizeBytes {
		// Evict to make room for new value;
		// onEvict callback will subtract size of evicted item from curSize.
		if _, _, ok := c.lru.RemoveOldest(); !ok {
			// Tracked curSize does not have room for a new value, but there is nothing to evict.
			// Accounting of curSize or configuration is broken; reset underlying cache and start over.
			level.Error(c.logger).Log(
				"msg", "LRU has nothing more to evict, but we still cannot allocate the item. Resetting cache.",
				"maxItemSizeBytes", c.maxItemSizeBytes,
				"maxSizeBytes", c.maxCacheSizeBytes,
				"curSize", c.curSize,
				"itemSize", valSize,
			)
			c.reset()
		}
	}

	c.lru.Add(key, val)
	c.curSize += valSize
	return
}

// ensureFits tries to make sure that the passed slice will fit into the LRU cache.
// Returns true if it will fit.
func (c *InMemoryPostingsOffsetTableCache) ensureFits(size uint64) bool {

	for c.curSize > c.maxCacheSizeBytes {
		if _, _, ok := c.lru.RemoveOldest(); !ok {
			level.Error(c.logger).Log(
				"msg", "LRU has nothing more to evict, but we still cannot allocate the item. Resetting cache.",
				"maxItemSizeBytes", c.maxItemSizeBytes,
				"maxSizeBytes", c.maxCacheSizeBytes,
				"curSize", c.curSize,
				"itemSize", size,
			)
			c.reset()
		}
	}
	return true
}

func (c *InMemoryPostingsOffsetTableCache) reset() {
	c.lru.Purge()
	c.curSize = 0
}

func (c *InMemoryPostingsOffsetTableCache) onEvict(_ InMemoryCacheKey, v []byte) {
	entrySize := sliceSize(v)
	c.curSize -= entrySize
}

func stringSize(s string) uint64 {
	return stringHeaderSize + uint64(len(s))
}

func sliceSize(b []byte) uint64 {
	return sliceHeaderSize + uint64(len(b))
}
