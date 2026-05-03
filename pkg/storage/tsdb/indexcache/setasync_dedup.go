// SPDX-License-Identifier: AGPL-3.0-only

package indexcache

import (
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/grafana/dskit/cache"
	lru "github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// dedupShards is the number of shards for the dedup LRU. Sharding reduces lock
// contention on hot-write paths: SetAsync acquires only its shard's mutex, so under
// uniform key hashing N goroutines collide on the same shard with probability 1/N.
// Must be a power of two so the shard index can be computed with a bitmask.
//
// The configured lruSize is divided across shards, so the per-shard capacity is
// ceil(lruSize / dedupShards). Under skewed key distribution a single shard may
// evict its oldest entry before the global capacity is reached, which can cause
// dedup drops to be lower than expected. If you observe a low value of
// cache_setasync_duplicates_dropped_total{name="index-cache"} relative to the
// expected concurrent SetAsync rate, raise -*-memcached-setasync-dedup-max-items
// above the naive estimate of unique keys per dedup window.
const dedupShards = 64

// dedupShard is one shard of the dedup state: an LRU of recently-seen keys with its
// own mutex.
type dedupShard struct {
	mu   sync.Mutex
	seen *lru.LRU[string, time.Time]
}

// dedupSetAsyncCache wraps a cache.Cache and drops SetAsync calls whose key was last
// seen within `window`. It does not change Get-side semantics.
//
// The dedup is intentionally *soft*: it coalesces writes within a fixed time window
// instead of trying to track real underlying-write completion, which is invisible
// from outside dskit's MemcachedClient. For deterministic cache writes — same key
// implies same value, true for the index cache where keys are derived from immutable
// block IDs and series refs — this is safe and removes a large fraction of redundant
// memcached SETs during overlapping query bursts.
//
// The dedup state is sharded across dedupShards independent LRUs to reduce lock
// contention under high write rates. LRU recency is per-shard (i.e., not strictly
// global LRU), which is acceptable because the LRU is only a memory bound — it does
// not affect correctness of the dedup window.
//
// Interaction with an L1 LRU layered above this wrapper:
//
// The factory in cache.go composes the stack as
// RemoteIndexCache → TypedLRUCache (or a single global LRU) → dedupSetAsyncCache → memcached.
// The L1 wrapper populates its own LRU after forwarding the call into this dedup
// wrapper, so a SetAsync that this wrapper drops still leaves an entry in the L1
// LRU above. Practical consequence: under heavy same-key concurrency *plus*
// memcached eviction within the dedup window, the writing pod's L1 can briefly
// hold an entry that memcached no longer has — peer pods will then miss memcached
// while the writer hits L1. This is an effectiveness divergence, not a correctness
// bug; it cannot serve stale data because the keys are derived from immutable block
// IDs and series refs (same key implies same value).
//
// The window must therefore stay short relative to all per-type TTLs the L1
// honours, so that a write suppressed by dedup cannot outlive its memcached entry.
// IndexCacheConfig.Validate() enforces window < min(TTL_*) when dedup is enabled.
//
// Observability note for operators: this wrapper sits *above* dskit's
// MemcachedClient, whose SetAsync also drops calls silently when its internal
// async queue is full (configured via memcached `-max-async-buffer-size`,
// default 25000). Those drops increment the dskit
// `cache_skipped_total{reason="asyncBufferFull"}` metric and emit a
// debug-level log (invisible at the default Mimir log level). When
// investigating "low memcached write rate during a burst", check both
// `cache_setasync_duplicates_dropped_total` (dropped here) and
// `cache_skipped_total{reason="asyncBufferFull"}` (dropped at dskit) — a
// dedup-suppressed write was reported "successful" from this wrapper's POV
// even when the eventual queue submission would have dropped.
type dedupSetAsyncCache struct {
	cache.Cache // embed → delegates GetMulti/GetMultiWithError/Set/Add/Delete/Stop/Name

	shards [dedupShards]*dedupShard
	mask   uint64
	window time.Duration

	duplicates prometheus.Counter
}

// newDedupSetAsyncCache wraps inner with a soft SetAsync deduplicator. lruSize bounds
// memory usage across all shards combined; window controls how long a key is treated
// as "recently written". If lruSize <= 0 *or* window <= 0, the dedup is disabled and
// inner is returned unchanged. The window guard matters because a non-positive window
// would make every Get-then-compare miss (the LRU's last+window timestamp would never
// be after now), so every SetAsync would still pay the per-shard mutex + LRU touch
// without ever actually dropping a duplicate. Mimir's CLI is shielded by Validate(),
// but the constructor is package-exported and callers shouldn't have to know this.
func newDedupSetAsyncCache(inner cache.Cache, name string, lruSize int, window time.Duration, reg prometheus.Registerer) (cache.Cache, error) {
	if lruSize <= 0 || window <= 0 {
		return inner, nil
	}

	// Round per-shard size up so the combined capacity is at least lruSize.
	perShard := (lruSize + dedupShards - 1) / dedupShards

	d := &dedupSetAsyncCache{
		Cache:  inner,
		mask:   uint64(dedupShards - 1),
		window: window,
		duplicates: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cache_setasync_duplicates_dropped_total",
			Help:        "Total number of SetAsync calls dropped because a SetAsync for the same key was made within the dedup window.",
			ConstLabels: prometheus.Labels{"name": name},
		}),
	}
	for i := range d.shards {
		l, err := lru.NewLRU[string, time.Time](perShard, nil)
		if err != nil {
			return nil, err
		}
		d.shards[i] = &dedupShard{seen: l}
	}
	return d, nil
}

// shardFor picks the shard for a given key by hashing it and masking to the shard count.
func (d *dedupSetAsyncCache) shardFor(key string) *dedupShard {
	return d.shards[xxhash.Sum64String(key)&d.mask]
}

// SetAsync drops the call if a SetAsync for the same key happened within `window`;
// otherwise records the timestamp and forwards to the inner cache. Only this key's
// shard is locked, so unrelated keys do not contend.
func (d *dedupSetAsyncCache) SetAsync(key string, value []byte, ttl time.Duration) {
	now := time.Now()
	s := d.shardFor(key)
	s.mu.Lock()
	if last, ok := s.seen.Get(key); ok && last.Add(d.window).After(now) {
		s.mu.Unlock()
		d.duplicates.Inc()
		return
	}
	s.seen.Add(key, now)
	s.mu.Unlock()
	d.Cache.SetAsync(key, value, ttl)
}

// SetMultiAsync filters out keys recently seen and forwards only the rest. Keys are
// grouped by shard so each shard is locked at most once per call, regardless of how
// many keys hash into it.
func (d *dedupSetAsyncCache) SetMultiAsync(data map[string][]byte, ttl time.Duration) {
	if len(data) == 0 {
		return
	}
	now := time.Now()

	// Bucket keys by shard.
	type shardEntry struct {
		s    *dedupShard
		keys []string
	}
	perShard := make(map[*dedupShard]*shardEntry, dedupShards)
	for k := range data {
		s := d.shardFor(k)
		e, ok := perShard[s]
		if !ok {
			e = &shardEntry{s: s}
			perShard[s] = e
		}
		e.keys = append(e.keys, k)
	}

	filtered := make(map[string][]byte, len(data))
	for _, e := range perShard {
		e.s.mu.Lock()
		for _, k := range e.keys {
			if last, ok := e.s.seen.Get(k); ok && last.Add(d.window).After(now) {
				d.duplicates.Inc()
				continue
			}
			e.s.seen.Add(k, now)
			filtered[k] = data[k]
		}
		e.s.mu.Unlock()
	}
	if len(filtered) > 0 {
		d.Cache.SetMultiAsync(filtered, ttl)
	}
}
