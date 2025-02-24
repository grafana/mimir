package cache

import (
	"context"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var _ Cache = (*LRUCache)(nil)

type LRUCache struct {
	c          Cache
	defaultTTL time.Duration
	name       string

	mtx sync.Mutex
	lru *lru.LRU[string, *Item]

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
	l, err := lru.NewLRU[string, *Item](lruSize, nil)
	if err != nil {
		return nil, err
	}

	cache := &LRUCache{
		c:          c,
		lru:        l,
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

func (l *LRUCache) SetAsync(key string, value []byte, ttl time.Duration) {
	l.c.SetAsync(key, value, ttl)

	l.mtx.Lock()
	defer l.mtx.Unlock()

	expires := time.Now().Add(ttl)
	l.lru.Add(key, &Item{
		Data:      value,
		ExpiresAt: expires,
	})
}

func (l *LRUCache) SetMultiAsync(data map[string][]byte, ttl time.Duration) {
	// store the data in the shared cache.
	l.c.SetMultiAsync(data, ttl)

	l.mtx.Lock()
	defer l.mtx.Unlock()

	expires := time.Now().Add(ttl)
	for k, v := range data {
		l.lru.Add(k, &Item{
			Data:      v,
			ExpiresAt: expires,
		})
	}
}

func (l *LRUCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	err := l.c.Set(ctx, key, value, ttl)

	l.mtx.Lock()
	defer l.mtx.Unlock()

	expires := time.Now().Add(ttl)
	l.lru.Add(key, &Item{
		Data:      value,
		ExpiresAt: expires,
	})

	return err
}

func (l *LRUCache) Add(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	err := l.c.Add(ctx, key, value, ttl)

	// When a caller uses the Add method, the presence of absence of an entry in the cache
	// has significance. In order to maintain the semantics of that, we only add an entry to
	// the LRU when it was able to be successfully added to the shared cache.
	if err == nil {
		l.mtx.Lock()
		defer l.mtx.Unlock()

		expires := time.Now().Add(ttl)
		l.lru.Add(key, &Item{
			Data:      value,
			ExpiresAt: expires,
		})
	}

	return err
}

func (l *LRUCache) GetMulti(ctx context.Context, keys []string, opts ...Option) (result map[string][]byte) {
	l.requests.Add(float64(len(keys)))
	l.mtx.Lock()
	defer l.mtx.Unlock()
	var (
		found = make(map[string][]byte, len(keys))
		miss  = make([]string, 0, len(keys))
		now   = time.Now()
	)

	for _, k := range keys {
		item, ok := l.lru.Get(k)
		if !ok {
			miss = append(miss, k)
			continue
		}
		if item.ExpiresAt.After(now) {
			found[k] = item.Data
			continue
		}
		l.lru.Remove(k)
		miss = append(miss, k)

	}
	l.hits.Add(float64(len(found)))

	if len(miss) > 0 {
		result = l.c.GetMulti(ctx, miss, opts...)
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

func (l *LRUCache) Stop() {
	l.c.Stop()
}

func (l *LRUCache) Delete(ctx context.Context, key string) error {
	l.mtx.Lock()
	l.lru.Remove(key)
	l.mtx.Unlock()
	return l.c.Delete(ctx, key)
}
