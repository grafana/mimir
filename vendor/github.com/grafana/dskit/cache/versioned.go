package cache

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

var _ Cache = (*Versioned)(nil)

// Versioned cache adds a version prefix to the keys.
// This allows cache keys to be changed in a newer version of the code (after a bugfix or a cached data format change).
type Versioned struct {
	cache         Cache
	logger        log.Logger
	versionPrefix string
}

// NewVersioned creates a new Versioned cache.
func NewVersioned(c Cache, version uint, logger log.Logger) *Versioned {
	return &Versioned{
		cache:         c,
		logger:        logger,
		versionPrefix: fmt.Sprintf("%d@", version),
	}
}

func (c *Versioned) SetAsync(key string, value []byte, ttl time.Duration) {
	c.cache.SetAsync(c.addVersion(key), value, ttl)
}

func (c *Versioned) SetMultiAsync(data map[string][]byte, ttl time.Duration) {
	versioned := make(map[string][]byte, len(data))
	for k, v := range data {
		versioned[c.addVersion(k)] = v
	}
	c.cache.SetMultiAsync(versioned, ttl)
}

func (c *Versioned) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.cache.Set(ctx, c.addVersion(key), value, ttl)
}

func (c *Versioned) Add(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.cache.Add(ctx, c.addVersion(key), value, ttl)
}

func (c *Versioned) GetMulti(ctx context.Context, keys []string, opts ...Option) map[string][]byte {
	result, err := c.GetMultiWithError(ctx, keys, opts...)
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to get items from cache", "err", err)
	}
	return result
}

func (c *Versioned) GetMultiWithError(ctx context.Context, keys []string, opts ...Option) (map[string][]byte, error) {
	versionedKeys := make([]string, len(keys))
	for i, k := range keys {
		versionedKeys[i] = c.addVersion(k)
	}
	versionedRes, err := c.cache.GetMultiWithError(ctx, versionedKeys, opts...)
	res := make(map[string][]byte, len(versionedRes))
	for k, v := range versionedRes {
		res[c.removeVersion(k)] = v
	}
	return res, err
}

func (c *Versioned) Name() string {
	return c.cache.Name()
}

func (c *Versioned) Stop() {
	c.cache.Stop()
}

func (c *Versioned) Delete(ctx context.Context, key string) error {
	return c.cache.Delete(ctx, c.addVersion(key))
}

func (c *Versioned) addVersion(k string) string {
	return c.versionPrefix + k
}
func (c *Versioned) removeVersion(k string) string {
	return strings.TrimPrefix(k, c.versionPrefix)
}
