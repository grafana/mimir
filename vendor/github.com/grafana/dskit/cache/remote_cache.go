package cache

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

var (
	_ Cache = (*MemcachedCache)(nil)
	_ Cache = (*RedisCache)(nil)
)

const (
	labelCacheName           = "name"
	labelCacheBackend        = "backend"
	backendValueRedis        = "redis"
	backendValueMemcached    = "memcached"
	cacheMetricNamePrefix    = "cache_"
	getMultiMetricNamePrefix = "getmulti_"
	clientInfoMetricName     = "client_info"
)

// MemcachedCache is a memcached-based cache.
type MemcachedCache struct {
	*remoteCache
}

// NewMemcachedCache makes a new MemcachedCache.
func NewMemcachedCache(name string, logger log.Logger, memcachedClient RemoteCacheClient) *MemcachedCache {
	return &MemcachedCache{
		remoteCache: newRemoteCache(
			name,
			logger,
			memcachedClient,
		),
	}
}

// RedisCache is a Redis-based cache.
type RedisCache struct {
	*remoteCache
}

// NewRedisCache makes a new RedisCache.
func NewRedisCache(name string, logger log.Logger, redisClient RemoteCacheClient) *RedisCache {
	return &RedisCache{
		remoteCache: newRemoteCache(
			name,
			logger,
			redisClient,
		),
	}
}

type remoteCache struct {
	logger       log.Logger
	remoteClient RemoteCacheClient
	name         string
}

func newRemoteCache(name string, logger log.Logger, remoteClient RemoteCacheClient) *remoteCache {
	c := &remoteCache{
		logger:       logger,
		remoteClient: remoteClient,
		name:         name,
	}

	level.Info(logger).Log("msg", "created remote cache")

	return c
}

// StoreAsync data identified by keys asynchronously using a job queue.
// The function enqueues the request and returns immediately: the entry will be
// asynchronously stored in the cache.
func (c *remoteCache) StoreAsync(data map[string][]byte, ttl time.Duration) {
	var (
		firstErr error
		failed   int
	)

	for key, val := range data {
		if err := c.remoteClient.SetAsync(key, val, ttl); err != nil {
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
	return c.remoteClient.GetMulti(ctx, keys, opts...)
}

// Delete data with the given key from cache.
func (c *remoteCache) Delete(ctx context.Context, key string) error {
	return c.remoteClient.Delete(ctx, key)
}

func (c *remoteCache) Name() string {
	return c.name
}
