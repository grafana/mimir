// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestLRUCache_StoreFetch(t *testing.T) {
	var (
		mock = NewMockCache()
		ctx  = context.Background()
	)
	// This entry is only known by our underlying cache.
	mock.Store(ctx, map[string][]byte{"buzz": []byte("buzz")}, time.Hour)

	reg := prometheus.NewPedanticRegistry()
	lru, err := WrapWithLRUCache(mock, "test", reg, 10000, 2*time.Hour)
	require.NoError(t, err)

	lru.Store(ctx, map[string][]byte{
		"foo": []byte("bar"),
		"bar": []byte("baz"),
	}, time.Minute)

	lru.Store(ctx, map[string][]byte{
		"expired": []byte("expired"),
	}, -time.Minute)

	result := lru.Fetch(ctx, []string{"buzz", "foo", "bar", "expired"})
	require.Equal(t, map[string][]byte{
		"buzz": []byte("buzz"),
		"foo":  []byte("bar"),
		"bar":  []byte("baz"),
	}, result)

	// Ensure we cache back entries from the underlying cache.
	item, ok := lru.lru.Get("buzz")
	require.True(t, ok)
	require.Equal(t, []byte("buzz"), item.(*cacheItem).data)
	require.True(t, time.Until(item.(*cacheItem).expiresAt) > 1*time.Hour)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_cache_memory_items_count Total number of items currently in the in-memory cache.
		# TYPE cortex_cache_memory_items_count gauge
		cortex_cache_memory_items_count{name="test"} 3
		# HELP cortex_cache_memory_hits_total Total number of requests to the in-memory cache that were a hit.
		# TYPE cortex_cache_memory_hits_total counter
		cortex_cache_memory_hits_total{name="test"} 2
		# HELP cortex_cache_memory_requests_total Total number of requests to the in-memory cache.
		# TYPE cortex_cache_memory_requests_total counter
		cortex_cache_memory_requests_total{name="test"} 4
	`)))
}

func TestLRUCache_Evictions(t *testing.T) {
	const maxItems = 2

	reg := prometheus.NewPedanticRegistry()
	lru, err := WrapWithLRUCache(NewMockCache(), "test", reg, maxItems, 2*time.Hour)
	require.NoError(t, err)

	lru.Store(context.Background(), map[string][]byte{
		"key_1": []byte("value_1"),
		"key_2": []byte("value_2"),
		"key_3": []byte("value_3"),
	}, time.Minute)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_cache_memory_items_count Total number of items currently in the in-memory cache.
		# TYPE cortex_cache_memory_items_count gauge
		cortex_cache_memory_items_count{name="test"} 2
	`), "cortex_cache_memory_items_count"))
}
