package caching

import (
	"context"
	"encoding/hex"
	"hash/fnv"
	"time"

	"github.com/grafana/dskit/cache"
)

type Backend interface {
	GetMulti(ctx context.Context, keys []string, opts ...cache.Option) map[string][]byte
	SetAsync(key string, value []byte, ttl time.Duration)
}

// HashCacheKey returns a hashed version of key that is small enough to fit within the Memcached key limit
// and contains no unprintable characters.
func HashCacheKey(key []byte) string {
	hasher := fnv.New64a()
	_, _ = hasher.Write(key)
	return hex.EncodeToString(hasher.Sum(nil))
}
