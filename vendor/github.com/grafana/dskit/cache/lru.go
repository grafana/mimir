package cache

import (
	"context"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/simplelru"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type LRUCache struct {
	c          Cache
	defaultTTL time.Duration
	name       string

	mtx sync.Mutex
	lru *lru.LRU

	requests prometheus.Counter
	hits     prometheus.Counter
	items    prometheus.GaugeFunc
}

type Item struct {
	Data      []byte
	ExpiresAt time.Time
}

// WrapWithLRUCache wraps a given `Cache` c with a LRU cache. The LRU cache will always store items in both caches.
// However it will only fetch items from the underlying cache if the LRU cache doesn't have the item.
// Items fetched from the underlying cache will be stored in the LRU cache with a default TTL.
// The LRU cache will also remove items from the underlying cache if they are expired.
// The LRU cache is limited in number of items using `lruSize`. This means this cache is not tailored for large items or items that have a big
// variation in size.
func WrapWithLRUCache(c Cache, name string, reg prometheus.Registerer, lruSize int, defaultTTL time.Duration) (*LRUCache, error) {
	lru, err := lru.NewLRU(lruSize, nil)
	if err != nil {
		return nil, err
	}

	cache := &LRUCache{
		c:          c,
		lru:        lru,
		name:       name,
		defaultTTL: defaultTTL,

		requests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cache_memory_requests_total",
			Help:        "Total number of requests to the in-memory cache.",
			ConstLabels: map[string]string{"name": name},
		}),
		hits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cache_memory_hits_total",
			Help:        "Total number of requests to the in-memory cache that were a hit.",
			ConstLabels: map[string]string{"name": name},
		}),
	}

	cache.items = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cache_memory_items_count",
		Help:        "Total number of items currently in the in-memory cache.",
		ConstLabels: map[string]string{"name": name},
	}, func() float64 {
		cache.mtx.Lock()
		defer cache.mtx.Unlock()

		return float64(cache.lru.Len())
	})

	return cache, nil
}

func (l *LRUCache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	// store the data in the shared cache.
	l.c.Store(ctx, data, ttl)

	l.mtx.Lock()
	defer l.mtx.Unlock()

	for k, v := range data {
		l.lru.Add(k, &Item{
			Data:      v,
			ExpiresAt: time.Now().Add(ttl),
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
		item := val.(*Item)
		if item.ExpiresAt.After(now) {
			found[k] = item.Data
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
			l.lru.Add(k, &Item{
				Data:      v,
				ExpiresAt: now.Add(l.defaultTTL),
			})
			found[k] = v
		}
	}

	return found
}

func (l *LRUCache) Name() string {
	return "in-memory-" + l.name
}
