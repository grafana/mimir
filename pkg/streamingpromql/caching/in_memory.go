// SPDX-License-Identifier: AGPL-3.0-only

package caching

import (
	"context"
	"slices"
	"time"

	"github.com/grafana/dskit/cache"
)

type InMemoryCache struct {
	Entries map[string]InMemoryCacheEntry

	GetCount int
	HitCount int
	SetCount int
}

// NewInMemoryCache creates a new in-memory cache.
// This is intended for testing purposes only.
func NewInMemoryCache() *InMemoryCache {
	return &InMemoryCache{
		Entries: make(map[string]InMemoryCacheEntry),
	}
}

type InMemoryCacheEntry struct {
	Value []byte
	TTL   time.Duration
}

func (c *InMemoryCache) GetMulti(ctx context.Context, keys []string, opts ...cache.Option) (map[string][]byte, error) {
	c.GetCount++
	results := make(map[string][]byte, len(keys))

	for _, key := range keys {
		entry, ok := c.Entries[key]

		if ok && len(entry.Value) > 0 {
			// Clone bytes to simulate network serialization
			results[key] = slices.Clone(entry.Value)
			c.HitCount++
		}
	}

	return results, nil
}

func (c *InMemoryCache) SetAsync(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	c.SetCount++
	c.Entries[key] = InMemoryCacheEntry{Value: value, TTL: ttl}

	return nil
}

func (c *InMemoryCache) Reset() {
	c.Entries = make(map[string]InMemoryCacheEntry)
	c.SetCount = 0
	c.GetCount = 0
	c.HitCount = 0
}
