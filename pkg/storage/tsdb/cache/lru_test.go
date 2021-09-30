// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLRUCache_StoreFetch(t *testing.T) {
	var (
		mock = newMockCache()
		ctx  = context.Background()
	)
	// This entry is only known by our underlying cache.
	mock.Store(ctx, map[string][]byte{"buzz": []byte("buzz")}, time.Hour)

	lru, err := WrapWithLRUCache(mock, nil, 10000, 2*time.Hour)
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
}
