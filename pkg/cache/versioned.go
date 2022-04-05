package cache

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Versioned cache adds a version prefix to the keys.
// This allows cache keys to be changed in a newer version of the code (after a bugfix or a cached data format change).
type Versioned struct {
	cache         Cache
	versionPrefix string
}

// NewVersioned creates a new Versioned cache.
func NewVersioned(c Cache, version uint) Versioned {
	return Versioned{
		cache:         c,
		versionPrefix: fmt.Sprintf("%d@", version),
	}
}

func (c Versioned) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	versioned := make(map[string][]byte, len(data))
	for k, v := range data {
		versioned[c.addVersion(k)] = v
	}
	c.cache.Store(ctx, versioned, ttl)
}

func (c Versioned) Fetch(ctx context.Context, keys []string) map[string][]byte {
	versionedKeys := make([]string, len(keys))
	for i, k := range keys {
		versionedKeys[i] = c.addVersion(k)
	}
	versionedRes := c.cache.Fetch(ctx, versionedKeys)
	res := make(map[string][]byte, len(versionedRes))
	for k, v := range versionedRes {
		res[c.removeVersion(k)] = v
	}
	return res
}

func (c Versioned) Name() string {
	return c.cache.Name()
}

func (c Versioned) addVersion(k string) string {
	return c.versionPrefix + k
}
func (c Versioned) removeVersion(k string) string {
	return strings.TrimPrefix(k, c.versionPrefix)
}
