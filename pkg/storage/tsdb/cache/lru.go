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

	requests *prometheus.CounterVec
	hits     *prometheus.CounterVec
}

type cacheItem struct {
	data      []byte
	expiresAt time.Time
}

func WrapWithLRUCache(name string, c cache.Cache, reg prometheus.Registerer, lruSize int, defaultTTL time.Duration) (cache.Cache, error) {
	cache := &LRUCache{
		c:          c,
		defaultTTL: defaultTTL,
	}
	lru, err := lru.NewLRU(lruSize, nil)
	if err != nil {
		return nil, err
	}
	cache.lru = lru
	cache.requests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_requests_total",
		Help: "Total number of requests to the cache.",
	}, []string{"item_type"})

	cache.hits = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_hits_total",
		Help: "Total number of requests to the cache that were a hit.",
	}, []string{"item_type"})
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
