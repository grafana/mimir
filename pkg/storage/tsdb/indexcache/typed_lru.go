// SPDX-License-Identifier: AGPL-3.0-only

package indexcache

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	lru "github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// PerTypeLRUSizes is a per-item-type max-items budget for the L1 LRU. Zero for any
// type means "do not cache that type in the L1" — those keys fall through directly to
// the upstream cache without any in-pod caching.
type PerTypeLRUSizes struct {
	Postings          int
	SeriesForRef      int
	ExpandedPostings  int
	SeriesForPostings int
	LabelNames        int
	LabelValues       int
}

// AnyEnabled reports whether any per-type budget is positive.
func (s PerTypeLRUSizes) AnyEnabled() bool {
	return s.Postings > 0 || s.SeriesForRef > 0 || s.ExpandedPostings > 0 ||
		s.SeriesForPostings > 0 || s.LabelNames > 0 || s.LabelValues > 0
}

// Per-item-type slot indices for the parallel arrays in TypedLRUCache. These mirror
// the cache key prefixes assigned in remote.go and the cacheType* string constants.
const (
	itemTypeIdxPostings = iota
	itemTypeIdxSeriesForRef
	itemTypeIdxExpandedPostings
	itemTypeIdxSeriesForPostings
	itemTypeIdxLabelNames
	itemTypeIdxLabelValues
	numItemTypes
)

// itemTypePrefixes holds the cache-key prefix that identifies each item type. Order
// must match itemTypeIdx* above. The prefixes themselves come from remote.go's
// *CacheKey functions and are not subject to change without a memcached schema bump.
var itemTypePrefixes = [numItemTypes]string{
	// Prefixes mirror remote.go's *CacheKey functions; any change here must be made
	// in lockstep there (and add a typed_lru_test.go coverage case for the new value).
	itemTypeIdxPostings:          "P2:",  // postingsCacheKey
	itemTypeIdxSeriesForRef:      "S:",   // seriesForRefCacheKey
	itemTypeIdxExpandedPostings:  "E2:",  // expandedPostingsCacheKey
	itemTypeIdxSeriesForPostings: "SP2:", // seriesForPostingsCacheKey
	itemTypeIdxLabelNames:        "LN:",  // labelNamesCacheKey
	itemTypeIdxLabelValues:       "LV2:", // labelValuesCacheKey
}

var itemTypeNames = [numItemTypes]string{
	itemTypeIdxPostings:          cacheTypePostings,
	itemTypeIdxSeriesForRef:      cacheTypeSeriesForRef,
	itemTypeIdxExpandedPostings:  cacheTypeExpandedPostings,
	itemTypeIdxSeriesForPostings: cacheTypeSeriesForPostings,
	itemTypeIdxLabelNames:        cacheTypeLabelNames,
	itemTypeIdxLabelValues:       cacheTypeLabelValues,
}

// keyTypeIdx classifies a cache key by its prefix. Returns -1 for unrecognized keys
// so the caller can fall through to the upstream without caching.
//
// We check the longer prefixes (E2:, SP2:) before the single-letter ones to avoid
// accidental matches; the table is iterated in slot order which already places them
// correctly relative to ambiguous shorter prefixes.
func keyTypeIdx(key string) int {
	// Hot path: most index-cache keys are S: or P: which are checked first via the
	// fixed iteration order below. The total prefix set is small (6) and the keys are
	// short, so a linear scan beats a map for both throughput and allocation.
	for i, p := range itemTypePrefixes {
		if strings.HasPrefix(key, p) {
			return i
		}
	}
	return -1
}

// TypedLRUCache wraps an upstream cache.Cache with a per-item-type LRU layer. Hits in
// the per-type LRU short-circuit the upstream call. Misses delegate to the upstream
// and populate the corresponding LRU with the returned bytes. An item type whose
// configured size is 0 has no LRU slot — its keys pass straight through.
//
// Compared to the single-LRU dskit WrapWithLRUCache, this wrapper prevents a hot
// working set in one item type (notably SeriesForRef during cold-block fan-out) from
// evicting unrelated entries. Per-type metrics let operators see hit-ratio drift
// independently per item type.
type TypedLRUCache struct {
	c          cache.Cache
	logger     log.Logger
	defaultTTL time.Duration
	name       string

	mtxs [numItemTypes]sync.Mutex
	// lrus[i] is nil when the corresponding item type has no L1 budget; keys for that
	// type bypass the L1 entirely.
	lrus [numItemTypes]*lru.LRU[string, *cache.Item]

	requests *prometheus.CounterVec
	hits     *prometheus.CounterVec
	items    *prometheus.GaugeVec
}

// NewTypedLRUCache wraps c with per-item-type LRUs sized by sizes. defaultTTL is used
// for entries populated from upstream GetMulti results (those have no caller-supplied
// TTL). Returns the wrapper or an error if any non-zero size is invalid.
func NewTypedLRUCache(c cache.Cache, name string, reg prometheus.Registerer, sizes PerTypeLRUSizes, defaultTTL time.Duration, logger log.Logger) (*TypedLRUCache, error) {
	t := &TypedLRUCache{
		c:          c,
		logger:     logger,
		defaultTTL: defaultTTL,
		name:       name,
	}

	perTypeSizes := [numItemTypes]int{
		itemTypeIdxPostings:          sizes.Postings,
		itemTypeIdxSeriesForRef:      sizes.SeriesForRef,
		itemTypeIdxExpandedPostings:  sizes.ExpandedPostings,
		itemTypeIdxSeriesForPostings: sizes.SeriesForPostings,
		itemTypeIdxLabelNames:        sizes.LabelNames,
		itemTypeIdxLabelValues:       sizes.LabelValues,
	}

	for i, sz := range perTypeSizes {
		if sz <= 0 {
			continue
		}
		l, err := lru.NewLRU[string, *cache.Item](sz, nil)
		if err != nil {
			return nil, err
		}
		t.lrus[i] = l
	}

	t.requests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:        "cache_memory_requests_total",
		Help:        "Total number of requests to the in-memory cache, per item type.",
		ConstLabels: map[string]string{"name": name},
	}, []string{"item_type"})
	t.hits = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:        "cache_memory_hits_total",
		Help:        "Total number of in-memory cache requests that were a hit, per item type.",
		ConstLabels: map[string]string{"name": name},
	}, []string{"item_type"})
	t.items = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name:        "cache_memory_items_count",
		Help:        "Total number of items currently in the in-memory cache, per item type.",
		ConstLabels: map[string]string{"name": name},
	}, []string{"item_type"})

	for i, name := range itemTypeNames {
		t.requests.WithLabelValues(name)
		t.hits.WithLabelValues(name)
		idx := i
		t.items.WithLabelValues(name).Set(0)
		_ = idx // reserved if we add a per-type sampler
	}

	return t, nil
}

func (t *TypedLRUCache) Name() string { return "in-memory-typed-" + t.name }

func (t *TypedLRUCache) Stop() { t.c.Stop() }

// SetAsync routes the local-LRU populate by item-type prefix, then forwards to
// upstream. Keys for item types with no LRU slot are forwarded without local caching.
//
// Order: forward-then-populate. The local LRU is populated *after* the upstream
// SetAsync returns, so an upstream that drops the call (e.g. dedupSetAsyncCache
// suppressing a write within its window) still leaves an entry in this LRU. This
// is intentional — the L1 is single-pod and is allowed to absorb the duplicate,
// while the dedup wrapper protects memcached from the redundant SET. The L1 entry
// is bounded by ttl; the dedup wrapper requires window < min TTL (validated in
// IndexCacheConfig) so that a dedup-suppressed write cannot outlive the memcached
// entry it would have refreshed.
func (t *TypedLRUCache) SetAsync(key string, value []byte, ttl time.Duration) {
	t.c.SetAsync(key, value, ttl)
	if idx := keyTypeIdx(key); idx >= 0 && t.lrus[idx] != nil {
		t.localAdd(idx, key, value, time.Now().Add(ttl))
	}
}

// SetMultiAsync forwards in bulk to upstream and then splits the local-LRU populate
// per item type. The bulk forward preserves the dedup decorator's batching path; the
// per-type bucketing only governs what we keep in-pod.
//
// Order: forward-then-populate, same as SetAsync. See SetAsync for the full
// discussion of the dedup-vs-L1 interaction; the multi variant inherits the same
// rule that L1 entries can briefly outlive memcached siblings dropped by dedup.
func (t *TypedLRUCache) SetMultiAsync(data map[string][]byte, ttl time.Duration) {
	t.c.SetMultiAsync(data, ttl)
	if len(data) == 0 {
		return
	}
	expires := time.Now().Add(ttl)
	for key, val := range data {
		idx := keyTypeIdx(key)
		if idx < 0 || t.lrus[idx] == nil {
			continue
		}
		t.localAdd(idx, key, val, expires)
	}
}

func (t *TypedLRUCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	err := t.c.Set(ctx, key, value, ttl)
	if idx := keyTypeIdx(key); idx >= 0 && t.lrus[idx] != nil {
		t.localAdd(idx, key, value, time.Now().Add(ttl))
	}
	return err
}

// Add only populates the LRU on a successful upstream Add — the same semantics as
// dskit's LRUCache, since callers of Add depend on absence/presence to mean
// something.
func (t *TypedLRUCache) Add(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	err := t.c.Add(ctx, key, value, ttl)
	if err != nil {
		return err
	}
	if idx := keyTypeIdx(key); idx >= 0 && t.lrus[idx] != nil {
		t.localAdd(idx, key, value, time.Now().Add(ttl))
	}
	return err
}

func (t *TypedLRUCache) Delete(ctx context.Context, key string) error {
	if idx := keyTypeIdx(key); idx >= 0 && t.lrus[idx] != nil {
		t.mtxs[idx].Lock()
		t.lrus[idx].Remove(key)
		t.mtxs[idx].Unlock()
	}
	return t.c.Delete(ctx, key)
}

func (t *TypedLRUCache) GetMulti(ctx context.Context, keys []string, opts ...cache.Option) map[string][]byte {
	result, err := t.GetMultiWithError(ctx, keys, opts...)
	if err != nil {
		level.Warn(t.logger).Log("msg", "failed to get items from cache", "err", err)
	}
	return result
}

// GetMultiWithError serves L1 hits per-type then forwards remaining misses to
// upstream. Keys for item types with no LRU slot are sent to upstream as-is.
//
// Each per-type LRU is locked in turn rather than holding a single global lock; under
// mixed traffic this lets one item type proceed while another is being mutated.
func (t *TypedLRUCache) GetMultiWithError(ctx context.Context, keys []string, opts ...cache.Option) (map[string][]byte, error) {
	if len(keys) == 0 {
		return map[string][]byte{}, nil
	}

	found := make(map[string][]byte, len(keys))
	// Per-type bucketing of the missing keys minimises lock acquisitions.
	var miss []string
	now := time.Now()

	// Group keys by item type so we hold each per-type mutex once.
	var perTypeKeys [numItemTypes][]string
	var passthrough []string
	for _, k := range keys {
		idx := keyTypeIdx(k)
		if idx < 0 || t.lrus[idx] == nil {
			passthrough = append(passthrough, k)
			continue
		}
		perTypeKeys[idx] = append(perTypeKeys[idx], k)
	}

	for idx, keysOfType := range perTypeKeys {
		if len(keysOfType) == 0 {
			continue
		}
		typeName := itemTypeNames[idx]
		t.requests.WithLabelValues(typeName).Add(float64(len(keysOfType)))

		t.mtxs[idx].Lock()
		hits := 0
		for _, k := range keysOfType {
			item, ok := t.lrus[idx].Get(k)
			if !ok {
				miss = append(miss, k)
				continue
			}
			if item.ExpiresAt.After(now) {
				found[k] = item.Data
				hits++
				continue
			}
			t.lrus[idx].Remove(k)
			miss = append(miss, k)
		}
		t.mtxs[idx].Unlock()
		t.hits.WithLabelValues(typeName).Add(float64(hits))
	}

	miss = append(miss, passthrough...)
	if len(miss) == 0 {
		return found, nil
	}

	upstream, err := t.c.GetMultiWithError(ctx, miss, opts...)
	for k, v := range upstream {
		idx := keyTypeIdx(k)
		if idx >= 0 && t.lrus[idx] != nil {
			t.localAdd(idx, k, v, now.Add(t.defaultTTL))
		}
		found[k] = v
	}
	return found, err
}

func (t *TypedLRUCache) localAdd(idx int, key string, value []byte, expiresAt time.Time) {
	t.mtxs[idx].Lock()
	t.lrus[idx].Add(key, &cache.Item{
		Data:      value,
		ExpiresAt: expiresAt,
	})
	t.mtxs[idx].Unlock()
}
