// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"encoding/binary"
	"sync"

	xxhash "github.com/cespare/xxhash/v2"
	lru "github.com/hashicorp/golang-lru/v2/simplelru"
	ulid "github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// decodedSeriesCacheShards is the number of shards for the decoded series cache. Sharding
// reduces lock contention on the lookup path: each Get / Set acquires only its shard's
// mutex, so under uniform key hashing N goroutines collide on the same shard with
// probability 1/N. Must be a power of two so the shard index is a bitmask.
const decodedSeriesCacheShards = 64

// decodedSeriesEntry is an immutable snapshot of one series' decoded form. Once stored
// in the cache, callers must treat the slices as read-only and copy out into their own
// buffers (see (*decodedSeriesCache).populate). The encodedByteSize field carries the
// size of the original encoded form (NOT the in-memory footprint of this struct) so
// query stats stay consistent across cache hits and misses; the cache itself is
// bounded by item count, so this field is informational only and a future change to
// byte-budgeted accounting must compute decoded memory separately.
//
// Invariant: every cached entry has at least one chunk. The Set call site only inserts
// after a successful full decode that returned chunks, so a "no chunks" series never
// reaches the cache. Callers don't need to check.
type decodedSeriesEntry struct {
	labels          []symbolizedLabel
	chunks          []chunks.Meta
	encodedByteSize int
}

type decodedSeriesShard struct {
	mu  sync.Mutex
	lru *lru.LRU[decodedSeriesKey, decodedSeriesEntry]
}

type decodedSeriesKey struct {
	block ulid.ULID
	ref   storage.SeriesRef
}

// decodedSeriesCache holds decoded SeriesForRef entries (symbolized labels + chunk metas)
// in-pod. It sits alongside the index cache: the index cache stores the raw encoded
// series bytes, this cache stores the decoded form so cache hits skip the varint decode
// loop entirely. It is intentionally small and bounded — it is not a substitute for the
// memcached index cache, only an opt-in CPU optimization for hot keys whose decoded form
// fits in pod memory.
//
// Concurrency: shards are independently locked. Entries are immutable after Set; callers
// must copy out into their own slab-allocated buffers via populate.
type decodedSeriesCache struct {
	shards [decodedSeriesCacheShards]*decodedSeriesShard
	mask   uint64

	hits   prometheus.Counter
	misses prometheus.Counter
}

// newDecodedSeriesCache returns a cache bounded to roughly maxItems total entries
// (rounded up so each shard has at least ceil(maxItems/shards)). If maxItems is non-
// positive, returns nil — callers must nil-check before use.
func newDecodedSeriesCache(maxItems int, reg prometheus.Registerer) (*decodedSeriesCache, error) {
	if maxItems <= 0 {
		return nil, nil
	}

	perShard := (maxItems + decodedSeriesCacheShards - 1) / decodedSeriesCacheShards
	c := &decodedSeriesCache{
		mask: uint64(decodedSeriesCacheShards - 1),
		hits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_decoded_series_cache_hits_total",
			Help: "Total number of decoded SeriesForRef cache hits (decode skipped).",
		}),
		misses: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_decoded_series_cache_misses_total",
			Help: "Total number of decoded SeriesForRef cache misses (full decode performed).",
		}),
	}
	for i := range c.shards {
		l, err := lru.NewLRU[decodedSeriesKey, decodedSeriesEntry](perShard, nil)
		if err != nil {
			return nil, err
		}
		c.shards[i] = &decodedSeriesShard{lru: l}
	}
	return c, nil
}

// shardFor picks the shard for a given (block, ref) by hashing the 16-byte ULID
// concatenated with the 8-byte ref.
func (c *decodedSeriesCache) shardFor(k decodedSeriesKey) *decodedSeriesShard {
	var buf [24]byte
	copy(buf[:16], k.block[:])
	binary.LittleEndian.PutUint64(buf[16:], uint64(k.ref))
	return c.shards[xxhash.Sum64(buf[:])&c.mask]
}

// Get returns the decoded entry for (block, ref) if present.
func (c *decodedSeriesCache) Get(block ulid.ULID, ref storage.SeriesRef) (decodedSeriesEntry, bool) {
	if c == nil {
		return decodedSeriesEntry{}, false
	}
	k := decodedSeriesKey{block: block, ref: ref}
	s := c.shardFor(k)
	s.mu.Lock()
	entry, ok := s.lru.Get(k)
	s.mu.Unlock()
	if ok {
		c.hits.Inc()
	} else {
		c.misses.Inc()
	}
	return entry, ok
}

// Set stores a decoded entry. The caller must pass slices that are safe to retain — Set
// does not copy. The current single call site (in series_refs.go) allocates fresh slices
// for the cache entry; if other call sites are added, they must do the same.
func (c *decodedSeriesCache) Set(block ulid.ULID, ref storage.SeriesRef, entry decodedSeriesEntry) {
	if c == nil {
		return
	}
	k := decodedSeriesKey{block: block, ref: ref}
	s := c.shardFor(k)
	s.mu.Lock()
	s.lru.Add(k, entry)
	s.mu.Unlock()
}
