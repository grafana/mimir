// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/index_cache.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package indexcache

import (
	"flag"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/alecthomas/units"
	"github.com/grafana/dskit/cache"
	"github.com/pkg/errors"
)

const (
	// BackendInMemory is the value for the in-memory index cache backend.
	BackendInMemory = "inmemory"

	// BackendMemcached is the value for the Memcached index cache backend.
	BackendMemcached = cache.BackendMemcached

	// BackendDefault is the value for the default index cache backend.
	BackendDefault = BackendInMemory

	defaultMaxItemSize = 128 * 1024 * 1024 // 128 MiB
)

var (
	supportedIndexCacheBackends = []string{BackendInMemory, BackendMemcached}

	errUnsupportedIndexCacheBackend          = errors.New("unsupported index cache backend")
	errInvalidMemcachedInMemoryMaxItems      = errors.New("memcached-inmemory-max-items must be >= 0")
	errInvalidMemcachedInMemoryTTL           = errors.New("memcached-inmemory-ttl must be > 0 when memcached-inmemory-max-items > 0")
	errInvalidMemcachedSetAsyncDedupMaxItems = errors.New("memcached-setasync-dedup-max-items must be >= 0")
	errInvalidMemcachedSetAsyncDedupWindow   = errors.New("memcached-setasync-dedup-window must be > 0 when memcached-setasync-dedup-max-items > 0")
)

type IndexCacheConfig struct {
	cache.BackendConfig `yaml:",inline"`
	InMemory            InMemoryIndexCacheConfig `yaml:"inmemory"`

	// MemcachedInMemoryMaxItems and MemcachedInMemoryTTL configure an optional in-memory
	// LRU cache fronting the memcached backend. Items are stored and fetched from the
	// LRU before hitting memcached, reducing read load on memcached for hot keys.
	// 0 disables the L1 cache. Only applicable when Backend == BackendMemcached.
	MemcachedInMemoryMaxItems int           `yaml:"memcached_inmemory_max_items" category:"advanced"`
	MemcachedInMemoryTTL      time.Duration `yaml:"memcached_inmemory_ttl"       category:"advanced"`

	// MemcachedSetAsyncDedupMaxItems and MemcachedSetAsyncDedupWindow configure an
	// optional soft deduplicator for SetAsync calls that wraps the memcached backend.
	// When two or more SetAsync calls happen for the same key within the window, only
	// the first reaches memcached; the rest are dropped. Bounded memory via the LRU
	// of recent keys. 0 disables. Only applicable when Backend == BackendMemcached.
	MemcachedSetAsyncDedupMaxItems int           `yaml:"memcached_setasync_dedup_max_items" category:"advanced"`
	MemcachedSetAsyncDedupWindow   time.Duration `yaml:"memcached_setasync_dedup_window"    category:"advanced"`
}

func (cfg *IndexCacheConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(f, "blocks-storage.bucket-store.index-cache.")
}

func (cfg *IndexCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", BackendDefault, fmt.Sprintf("The index cache backend type. Supported values: %s.", strings.Join(supportedIndexCacheBackends, ", ")))

	cfg.InMemory.RegisterFlagsWithPrefix(prefix+"inmemory.", f)
	cfg.Memcached.RegisterFlagsWithPrefix(prefix+"memcached.", f)

	f.IntVar(&cfg.MemcachedInMemoryMaxItems, prefix+"memcached-inmemory-max-items", 0,
		"Maximum number of items to keep in a first-level in-memory LRU cache fronting the memcached index cache. "+
			"Items are stored and fetched in-memory before hitting memcached. 0 to disable. "+
			"Note: index cache items vary widely in size (a few hundred bytes to several KB depending on the cache "+
			"type), so resident memory of the LRU is roughly this value multiplied by the average item size. Size "+
			"accordingly. Note also: the underlying LRU implementation serializes all reads on a single per-pod "+
			"mutex while it forwards misses to memcached, so during cold-cache scenarios (after restart, or large "+
			"working-set shifts) reads can become bottlenecked on this mutex; the L1 is most beneficial for steady-"+
			"state hit-heavy workloads. If cold-cache contention dominates, set this to 0. "+
			"Only applicable when "+prefix+"backend=memcached.")
	f.DurationVar(&cfg.MemcachedInMemoryTTL, prefix+"memcached-inmemory-ttl", 24*time.Hour,
		"TTL for items stored in the L1 in-memory LRU cache fronting memcached. The L1 TTL is uniform across all "+
			"item kinds and overrides the per-entry TTL of the underlying memcached cache for L1 hits. Index cache "+
			"content is keyed on immutable block IDs, so serving from L1 past the original memcached TTL is correct. "+
			"Only applicable when the L1 cache is enabled.")

	f.IntVar(&cfg.MemcachedSetAsyncDedupMaxItems, prefix+"memcached-setasync-dedup-max-items", 0,
		"Maximum number of recently-stored cache keys to track for SetAsync deduplication. Concurrent or near-simultaneous "+
			"SetAsync calls for the same key are coalesced for up to "+prefix+"memcached-setasync-dedup-window. "+
			"0 to disable. Note: the dedup state is sharded across a fixed number of shards (currently 64) to reduce "+
			"lock contention; under skewed key distribution a single shard may evict before the global capacity is "+
			"reached, so size this above the naive estimate of unique keys per dedup window if dedup drops appear lower "+
			"than expected. Only applicable when "+prefix+"backend=memcached.")
	f.DurationVar(&cfg.MemcachedSetAsyncDedupWindow, prefix+"memcached-setasync-dedup-window", 5*time.Second,
		"Window during which a previous SetAsync for the same key suppresses duplicate SetAsync calls. "+
			"Only applicable when the SetAsync deduplicator is enabled.")
}

// Validate the config.
func (cfg *IndexCacheConfig) Validate() error {
	if !slices.Contains(supportedIndexCacheBackends, cfg.Backend) {
		return errUnsupportedIndexCacheBackend
	}

	switch cfg.Backend {
	case BackendMemcached:
		// Validate backend config only when not using the in-memory cache.
		if err := cfg.BackendConfig.Validate(); err != nil {
			return err
		}
		if cfg.MemcachedInMemoryMaxItems < 0 {
			return errInvalidMemcachedInMemoryMaxItems
		}
		if cfg.MemcachedInMemoryMaxItems > 0 && cfg.MemcachedInMemoryTTL <= 0 {
			return errInvalidMemcachedInMemoryTTL
		}
		if cfg.MemcachedSetAsyncDedupMaxItems < 0 {
			return errInvalidMemcachedSetAsyncDedupMaxItems
		}
		if cfg.MemcachedSetAsyncDedupMaxItems > 0 && cfg.MemcachedSetAsyncDedupWindow <= 0 {
			return errInvalidMemcachedSetAsyncDedupWindow
		}
	case BackendInMemory:
		// Validate sets defaults not exposed in config
		if err := cfg.InMemory.Validate(); err != nil {
			return err
		}
	}

	return nil
}

type InMemoryIndexCacheConfig struct {
	MaxCacheSizeBytes uint64 `yaml:"max_size_bytes"`
	MaxItemSizeBytes  uint64 `yaml:"-"`
}

func (cfg *InMemoryIndexCacheConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Uint64Var(&cfg.MaxCacheSizeBytes, prefix+"max-size-bytes", uint64(1*units.Gibibyte), "Maximum size in bytes of in-memory index cache used to speed up blocks index lookups (shared between all tenants).")
}
func (cfg *InMemoryIndexCacheConfig) Validate() error {
	if cfg.MaxItemSizeBytes == 0 {
		cfg.MaxItemSizeBytes = defaultMaxItemSize
	}
	if cfg.MaxItemSizeBytes > cfg.MaxCacheSizeBytes {
		cfg.MaxItemSizeBytes = cfg.MaxCacheSizeBytes
	}
	return nil
}
