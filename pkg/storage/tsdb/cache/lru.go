package cache

import (
	"context"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/simplelru"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/cache"
)

type LRUCache struct {
	c          cache.Cache
	defaultTTL time.Duration

	mtx sync.Mutex
	lru *lru.LRU

	requests prometheus.Counter
	hits     prometheus.Counter
}

type cacheItem struct {
	data      []byte
	expiresAt time.Time
}

// WrapWithLRUCache wraps a given `cache.Cache` c with a LRU cache. The LRU cache will always store items in both caches.
// However it will only fetch items from the underlying cache if the LRU cache doesn't have the item.
// Items fetched from the underlying cache will be stored in the LRU cache with a default TTL.
// The LRU cache will also remove items from the underlying cache if they are expired.
// The LRU cache is limited in number of items using `lruSize`. This means this cache is not tailored for large items or items that have a big
// variation in size.
func WrapWithLRUCache(c cache.Cache, reg prometheus.Registerer, lruSize int, defaultTTL time.Duration) (*LRUCache, error) {
	cache := &LRUCache{
		c:          c,
		defaultTTL: defaultTTL,
	}
	lru, err := lru.NewLRU(lruSize, nil)
	if err != nil {
		return nil, err
	}
	cache.lru = lru
	cache.requests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "store_" + cache.Name() + "_cache_requests_total",
		Help:      "Total number of requests to the cache.",
	})

	cache.hits = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "store_" + cache.Name() + "_cache_hits_total",
		Help:      "Total number of requests to the cache that were a hit.",
	})
	return cache, nil
}

func (l *LRUCache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	// store the data in the shared cache.
	l.c.Store(ctx, data, ttl)

	l.mtx.Lock()
	defer l.mtx.Unlock()

	for k, v := range data {
		l.lru.Add(k, &cacheItem{
			data:      v,
			expiresAt: time.Now().Add(ttl),
		})
	}
}

func (l *LRUCache) Fetch(ctx context.Context, keys []string) (result map[string][]byte) {
	l.requests.Add(float64(len(keys)))
	l.mtx.Lock()
	defer l.mtx.Unlock()
	var (
		found = make(map[string][]byte, len(keys))
		miss  = make([]string, 0, len(keys))
		now   = time.Now()
	)

	for _, k := range keys {
		val, ok := l.lru.Get(k)
		if !ok {
			miss = append(miss, k)
			continue
		}
		item := val.(*cacheItem)
		if item.expiresAt.After(now) {
			found[k] = item.data
			continue
		}
		l.lru.Remove(k)
		miss = append(miss, k)

	}
	l.hits.Add(float64(len(found)))

	if len(miss) > 0 {
		result = l.c.Fetch(ctx, miss)
		for k, v := range result {
			// we don't know the ttl of the result, so we use the default one.
			l.lru.Add(k, &cacheItem{
				data:      v,
				expiresAt: now.Add(l.defaultTTL),
			})
			found[k] = v
		}
	}

	return found
}

func (l *LRUCache) Name() string {
	return "lru_" + l.c.Name()
}
