// SPDX-License-Identifier: AGPL-3.0-only

package caching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPrefixingCache(t *testing.T) {
	prefix := "initial-prefix-"

	backend := NewInMemoryCache()
	prefixingCache := NewPrefixingCache(backend, func(ctx context.Context) (string, error) {
		return prefix, nil
	})

	ctx := context.Background()
	ttl := time.Hour
	require.NoError(t, prefixingCache.SetAsync(ctx, "key", []byte("value-1"), ttl))

	require.Equal(t, map[string]InMemoryCacheEntry{
		"initial-prefix-key": {Value: []byte("value-1"), TTL: ttl},
	}, backend.Entries)

	prefix = "new-prefix-"
	require.NoError(t, prefixingCache.SetAsync(ctx, "key", []byte("value-2"), ttl))
	require.NoError(t, prefixingCache.SetAsync(ctx, "other-key", []byte("value-3"), ttl))

	require.Equal(t, map[string]InMemoryCacheEntry{
		"initial-prefix-key":   {Value: []byte("value-1"), TTL: ttl},
		"new-prefix-key":       {Value: []byte("value-2"), TTL: ttl},
		"new-prefix-other-key": {Value: []byte("value-3"), TTL: ttl},
	}, backend.Entries)

	results, err := prefixingCache.GetMulti(ctx, []string{"key", "other-key"})
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{
		"key":       []byte("value-2"),
		"other-key": []byte("value-3"),
	}, results)
}
