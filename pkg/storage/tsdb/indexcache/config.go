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
	// errInvalidMemcachedSetAsyncDedupWindowExceedsTTL guards a foot-gun: when the
	// dedup window is >= some per-type TTL, the wrapper can suppress a re-write
	// whose memcached entry has already expired, leaving a real cache miss until
	// the dedup state forgets the key. A short dedup window relative to TTLs is
	// a load-bearing assumption in the wrapper's docstring; this validator turns
	// it into a hard error at config-load time.
	errInvalidMemcachedSetAsyncDedupWindowExceedsTTL = errors.New("memcached-setasync-dedup-window must be less than every per-type ttl-* value")
	errInvalidTTL                                    = errors.New("per-item-type TTL must be > 0")
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

	// Per-item-type max-item budgets for the L1 in-memory LRU. When any of these is
	// set to a positive value, the L1 switches from a single global LRU to a per-item-
	// type LRU so a hot working set in one item type cannot evict entries from the
	// others. Item types whose flag is left at 0 fall through directly to memcached
	// (no L1 caching for that type). Only applicable when Backend == BackendMemcached
	// and at least one of these flags is positive; otherwise the behavior is the
	// single global LRU controlled by MemcachedInMemoryMaxItems.
	MemcachedInMemoryMaxItemsPostings          int `yaml:"memcached_inmemory_max_items_postings"            category:"experimental"`
	MemcachedInMemoryMaxItemsSeriesForRef      int `yaml:"memcached_inmemory_max_items_series_for_ref"      category:"experimental"`
	MemcachedInMemoryMaxItemsExpandedPostings  int `yaml:"memcached_inmemory_max_items_expanded_postings"   category:"experimental"`
	MemcachedInMemoryMaxItemsSeriesForPostings int `yaml:"memcached_inmemory_max_items_series_for_postings" category:"experimental"`
	MemcachedInMemoryMaxItemsLabelNames        int `yaml:"memcached_inmemory_max_items_label_names"         category:"experimental"`
	MemcachedInMemoryMaxItemsLabelValues       int `yaml:"memcached_inmemory_max_items_label_values"        category:"experimental"`

	// MemcachedSetAsyncDedupMaxItems and MemcachedSetAsyncDedupWindow configure an
	// optional soft deduplicator for SetAsync calls that wraps the memcached backend.
	// When two or more SetAsync calls happen for the same key within the window, only
	// the first reaches memcached; the rest are dropped. Bounded memory via the LRU
	// of recent keys. 0 disables. Only applicable when Backend == BackendMemcached.
	MemcachedSetAsyncDedupMaxItems int           `yaml:"memcached_setasync_dedup_max_items" category:"advanced"`
	MemcachedSetAsyncDedupWindow   time.Duration `yaml:"memcached_setasync_dedup_window"    category:"advanced"`

	// Per-item-type TTLs for memcached-stored entries whose Store* methods do not take
	// a caller-supplied TTL. (StorePostings and StoreSeriesForRef accept a TTL that the
	// store-gateway derives from the block via BlockTTL, so they are not configured
	// here.) The defaults match the historical hardcoded value (7 days). Operators
	// running a cell where matcher-keyed items churn frequently can lower
	// TTLExpandedPostings / TTLLabelValues independently of the others to bias
	// memcached eviction toward retaining the more durable item types.
	TTLExpandedPostings  time.Duration `yaml:"ttl_expanded_postings"   category:"advanced"`
	TTLSeriesForPostings time.Duration `yaml:"ttl_series_for_postings" category:"advanced"`
	TTLLabelNames        time.Duration `yaml:"ttl_label_names"         category:"advanced"`
	TTLLabelValues       time.Duration `yaml:"ttl_label_values"        category:"advanced"`
}

// PerTypeMaxItems returns the configured per-item-type L1 LRU max-items budget,
// reading directly from the per-type flags. AnyEnabled() reports whether at least
// one per-type budget is positive.
//
// Most callers want effectivePerTypeMaxItems instead, which also fills in budgets
// from the legacy MemcachedInMemoryMaxItems global flag when no per-type flag is
// set explicitly.
func (cfg *IndexCacheConfig) PerTypeMaxItems() PerTypeLRUSizes {
	return PerTypeLRUSizes{
		Postings:          cfg.MemcachedInMemoryMaxItemsPostings,
		SeriesForRef:      cfg.MemcachedInMemoryMaxItemsSeriesForRef,
		ExpandedPostings:  cfg.MemcachedInMemoryMaxItemsExpandedPostings,
		SeriesForPostings: cfg.MemcachedInMemoryMaxItemsSeriesForPostings,
		LabelNames:        cfg.MemcachedInMemoryMaxItemsLabelNames,
		LabelValues:       cfg.MemcachedInMemoryMaxItemsLabelValues,
	}
}

// effectivePerTypeMaxItems returns the per-type L1 LRU budgets that the L1 wrapper
// should be sized with. Precedence:
//   - If at least one per-type flag is positive, the per-type values are used
//     verbatim. Item types whose per-type flag is zero are not cached in L1.
//   - Otherwise, if MemcachedInMemoryMaxItems is positive, it is divided evenly
//     across all six item types (one global "total cardinality" knob, distributed).
//   - Otherwise, all six budgets are zero and the L1 is disabled.
func (cfg *IndexCacheConfig) effectivePerTypeMaxItems() PerTypeLRUSizes {
	if sizes := cfg.PerTypeMaxItems(); sizes.AnyEnabled() {
		return sizes
	}
	if cfg.MemcachedInMemoryMaxItems <= 0 {
		return PerTypeLRUSizes{}
	}
	// Divide the global budget evenly across all item types. Round up so a small
	// global value still produces at least 1 entry per type rather than disabling
	// the L1 by accident; the over-allocation is bounded by numItemTypes-1 entries
	// across the cache.
	perType := (cfg.MemcachedInMemoryMaxItems + numItemTypes - 1) / numItemTypes
	return PerTypeLRUSizes{
		Postings:          perType,
		SeriesForRef:      perType,
		ExpandedPostings:  perType,
		SeriesForPostings: perType,
		LabelNames:        perType,
		LabelValues:       perType,
	}
}

func (cfg *IndexCacheConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(f, "blocks-storage.bucket-store.index-cache.")
}

func (cfg *IndexCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", BackendDefault, fmt.Sprintf("The index cache backend type. Supported values: %s.", strings.Join(supportedIndexCacheBackends, ", ")))

	cfg.InMemory.RegisterFlagsWithPrefix(prefix+"inmemory.", f)
	cfg.Memcached.RegisterFlagsWithPrefix(prefix+"memcached.", f)

	f.IntVar(&cfg.MemcachedInMemoryMaxItems, prefix+"memcached-inmemory-max-items", 0,
		"Total max-items budget for the L1 in-memory LRU cache fronting the memcached index cache. The L1 is "+
			"per-item-type internally; this flag is divided evenly across the six item types (postings, series-for-ref, "+
			"expanded-postings, series-for-postings, label-names, label-values). For finer control, set the "+
			"per-item-type "+prefix+"memcached-inmemory-max-items-* flags directly — any positive per-type flag "+
			"overrides this global distribution. 0 to disable the L1 entirely. "+
			"Note: index cache items vary widely in size (a few hundred bytes to several KB depending on the cache "+
			"type), so resident memory of the LRU is roughly this value multiplied by the average item size. Size "+
			"accordingly. "+
			"Only applicable when "+prefix+"backend=memcached.")
	f.DurationVar(&cfg.MemcachedInMemoryTTL, prefix+"memcached-inmemory-ttl", 24*time.Hour,
		"TTL for items stored in the L1 in-memory LRU cache fronting memcached. The L1 TTL is uniform across all "+
			"item kinds and overrides the per-entry TTL of the underlying memcached cache for L1 hits. Index cache "+
			"content is keyed on immutable block IDs, so serving from L1 past the original memcached TTL is correct. "+
			"Only applicable when the L1 cache is enabled.")

	perTypeFlagDoc := "Per-item-type budget for the L1 LRU. When any per-item-type flag is positive the L1 is " +
		"sized from these flags directly and the global " + prefix + "memcached-inmemory-max-items knob is " +
		"ignored. Item types whose per-type flag is left at 0 are not cached in the L1 and fall through to memcached."
	f.IntVar(&cfg.MemcachedInMemoryMaxItemsPostings, prefix+"memcached-inmemory-max-items-postings", 0, perTypeFlagDoc)
	f.IntVar(&cfg.MemcachedInMemoryMaxItemsSeriesForRef, prefix+"memcached-inmemory-max-items-series-for-ref", 0, perTypeFlagDoc)
	f.IntVar(&cfg.MemcachedInMemoryMaxItemsExpandedPostings, prefix+"memcached-inmemory-max-items-expanded-postings", 0, perTypeFlagDoc)
	f.IntVar(&cfg.MemcachedInMemoryMaxItemsSeriesForPostings, prefix+"memcached-inmemory-max-items-series-for-postings", 0, perTypeFlagDoc)
	f.IntVar(&cfg.MemcachedInMemoryMaxItemsLabelNames, prefix+"memcached-inmemory-max-items-label-names", 0, perTypeFlagDoc)
	f.IntVar(&cfg.MemcachedInMemoryMaxItemsLabelValues, prefix+"memcached-inmemory-max-items-label-values", 0, perTypeFlagDoc)

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

	f.DurationVar(&cfg.TTLExpandedPostings, prefix+"ttl-expanded-postings", defaultTTL,
		"TTL for cached ExpandedPostings entries (matcher-keyed). Lower this if matchers churn quickly to bias eviction toward more durable item types.")
	f.DurationVar(&cfg.TTLSeriesForPostings, prefix+"ttl-series-for-postings", defaultTTL,
		"TTL for cached SeriesForPostings entries (postings-key-keyed).")
	f.DurationVar(&cfg.TTLLabelNames, prefix+"ttl-label-names", defaultTTL,
		"TTL for cached LabelNames entries (matcher-keyed).")
	f.DurationVar(&cfg.TTLLabelValues, prefix+"ttl-label-values", defaultTTL,
		"TTL for cached LabelValues entries (label name + matcher-keyed).")
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
		// L1 is enabled when either the global flag or any per-type flag is positive.
		// The per-type path was added after the original validator was written, so
		// we must check both routes here — otherwise a TTL=0 configuration gated
		// only on per-type flags constructs a TypedLRUCache whose entries expire
		// immediately on Get, doing wasteful work with no operator-visible signal.
		if (cfg.MemcachedInMemoryMaxItems > 0 || cfg.PerTypeMaxItems().AnyEnabled()) && cfg.MemcachedInMemoryTTL <= 0 {
			return errInvalidMemcachedInMemoryTTL
		}
		if cfg.MemcachedSetAsyncDedupMaxItems < 0 {
			return errInvalidMemcachedSetAsyncDedupMaxItems
		}
		if cfg.MemcachedSetAsyncDedupMaxItems > 0 && cfg.MemcachedSetAsyncDedupWindow <= 0 {
			return errInvalidMemcachedSetAsyncDedupWindow
		}
		if cfg.TTLExpandedPostings <= 0 || cfg.TTLSeriesForPostings <= 0 || cfg.TTLLabelNames <= 0 || cfg.TTLLabelValues <= 0 {
			return errInvalidTTL
		}
		if cfg.MemcachedSetAsyncDedupMaxItems > 0 {
			// We enforce window < min TTL only as a hard bound. In practice operators
			// should pick a window orders of magnitude smaller than any TTL: the dedup
			// is meant to coalesce a single fan-out's overlapping writes (sub-second to
			// a few seconds), not to bound steady-state refresh cadence. A window in
			// minutes or hours, even with multi-day TTLs, is almost always a misconfig
			// (e.g. value entered in the wrong unit). We do not enforce that stricter
			// bound here so a one-off operational override remains possible, but be
			// suspicious of any window above ~1 minute.
			minTTL := min(cfg.TTLExpandedPostings, cfg.TTLSeriesForPostings, cfg.TTLLabelNames, cfg.TTLLabelValues)
			if cfg.MemcachedSetAsyncDedupWindow >= minTTL {
				return errInvalidMemcachedSetAsyncDedupWindowExceedsTTL
			}
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
