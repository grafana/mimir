package cache

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	_ Cache = (*MemcachedCache)(nil)
	_ Cache = (*RedisCache)(nil)
)

const (
	labelName        = "name"
	labelBackend     = "backend"
	backendRedis     = "redis"
	backendMemcached = "memcached"
	cachePrefix      = "cache_"
	getMultiPrefix   = "getMulti_"
)

// MemcachedCache is a memcached-based cache.
type MemcachedCache struct {
	*remoteCache
}

// NewMemcachedCache makes a new MemcachedCache.
func NewMemcachedCache(name string, logger log.Logger, memcachedClient RemoteCacheClient, reg prometheus.Registerer) *MemcachedCache {
	return &MemcachedCache{
		remoteCache: newRemoteCache(
			name,
			logger,
			memcachedClient,
			prometheus.WrapRegistererWith(
				prometheus.Labels{labelBackend: backendMemcached},
				prometheus.WrapRegistererWithPrefix(cachePrefix, reg)),
		),
	}
}

// RedisCache is a Redis-based cache.
type RedisCache struct {
	*remoteCache
}

// NewRedisCache makes a new RedisCache.
func NewRedisCache(name string, logger log.Logger, redisClient RemoteCacheClient, reg prometheus.Registerer) *RedisCache {
	return &RedisCache{
		remoteCache: newRemoteCache(
			name,
			logger,
			redisClient,
			prometheus.WrapRegistererWith(
				prometheus.Labels{labelBackend: backendRedis},
				prometheus.WrapRegistererWithPrefix(cachePrefix, reg)),
		),
	}
}

type remoteCache struct {
	logger       log.Logger
	remoteClient RemoteCacheClient
	name         string

	// Metrics.
	requests prometheus.Counter
	hits     prometheus.Counter
}

func newRemoteCache(name string, logger log.Logger, remoteClient RemoteCacheClient, reg prometheus.Registerer) *remoteCache {
	c := &remoteCache{
		logger:       logger,
		remoteClient: remoteClient,
		name:         name,
	}

	c.requests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name:        "requests_total",
		Help:        "Total number of items requests to cache.",
		ConstLabels: prometheus.Labels{labelName: name},
	})

	c.hits = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name:        "hits_total",
		Help:        "Total number of items requests to the cache that were a hit.",
		ConstLabels: prometheus.Labels{labelName: name},
	})

	level.Info(logger).Log("msg", "created remote cache")

	return c
}

// Store data identified by keys.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *remoteCache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	var (
		firstErr error
		failed   int
	)

	for key, val := range data {
		if err := c.remoteClient.SetAsync(ctx, key, val, ttl); err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if firstErr != nil {
		level.Warn(c.logger).Log("msg", "failed to store one or more items into remote cache", "failed", failed, "firstErr", firstErr)
	}
}

// Fetch fetches multiple keys and returns a map containing cache hits, along with a list of missing keys.
// In case of error, it logs and return an empty cache hits map.
func (c *remoteCache) Fetch(ctx context.Context, keys []string, opts ...Option) map[string][]byte {
	// Fetch the keys from remote cache in a single request.
	c.requests.Add(float64(len(keys)))
	results := c.remoteClient.GetMulti(ctx, keys, opts...)
	c.hits.Add(float64(len(results)))
	return results
}

// Delete data with the given key from cache.
func (c *remoteCache) Delete(ctx context.Context, key string) error {
	return c.remoteClient.Delete(ctx, key)
}

func (c *remoteCache) Name() string {
	return c.name
}
