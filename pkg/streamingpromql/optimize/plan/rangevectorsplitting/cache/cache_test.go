// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/streamingpromql/caching"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type testTTLProvider struct{}

func (testTTLProvider) GetMinResultsCacheTTL(context.Context) (time.Duration, error) {
	return time.Hour, nil
}

type testCodec struct{}

func (testCodec) Marshal([]int) ([]byte, error)   { return []byte{}, nil }
func (testCodec) Unmarshal([]byte) ([]int, error) { return nil, nil }

const (
	testFunction = functions.FUNCTION_SUM_OVER_TIME
)

// TestCacheKeys verifies that the cache prefix is folded into both the full cache key (which is
// stored in the cache entry for collision verification) and the hashed backend key.
func TestCacheKeys(t *testing.T) {
	function := testFunction
	inner := []byte("inner")
	var start, end int64 = 0, 100
	rawKey, err := generateFullCacheKey(context.Background(), func(context.Context) (string, error) { return "", nil }, function, inner, start, end)
	require.NoError(t, err)

	// With an empty prefix generator the full key is just the raw key.
	noPrefix := &Cache[int]{prefixGenerator: func(context.Context) (string, error) { return "", nil }}
	fullKey, hashedKey, err := noPrefix.cacheKeys(context.Background(), function, inner, start, end)
	require.NoError(t, err)
	require.Equal(t, rawKey, fullKey)
	require.Equal(t, caching.HashCacheKey(rawKey), hashedKey)

	// With a prefix generator the prefix is prepended to the full key and folded into the hash.
	withPrefix := &Cache[int]{prefixGenerator: func(context.Context) (string, error) { return "tenant-a:", nil }}
	fullKey, hashedKey, err = withPrefix.cacheKeys(context.Background(), function, inner, start, end)
	require.NoError(t, err)
	require.Equal(t, append([]byte("tenant-a:"), rawKey...), fullKey)
	require.Equal(t, caching.HashCacheKey(fullKey), hashedKey)
}

// TestCacheKeysIsolatesPrefixes verifies that the same inner query under two different prefixes
// produces different full keys and different hashed keys. Because the prefix is part of the full
// key stored in the cache entry, even a hash collision across tenants or label policies would be
// rejected by the stored-key comparison rather than silently serving another tenant's data.
func TestCacheKeysIsolatesPrefixes(t *testing.T) {
	function := testFunction
	inner := []byte("inner")
	var start, end int64 = 0, 100

	tenantA := &Cache[int]{prefixGenerator: func(context.Context) (string, error) { return "tenant-a:", nil }}
	tenantB := &Cache[int]{prefixGenerator: func(context.Context) (string, error) { return "tenant-b:", nil }}

	keyA, hashedA, err := tenantA.cacheKeys(context.Background(), function, inner, start, end)
	require.NoError(t, err)
	keyB, hashedB, err := tenantB.cacheKeys(context.Background(), function, inner, start, end)
	require.NoError(t, err)

	require.NotEqual(t, keyA, keyB)
	require.NotEqual(t, hashedA, hashedB)
}

// TestCacheIsolatesTenants verifies end-to-end that an entry written under one prefix is not
// returned when a different prefix is active.
func TestCacheIsolatesTenants(t *testing.T) {
	backend := caching.NewInMemoryCache()

	prefix := "tenant-a:"
	factory := NewCacheFactoryWithBackend(backend, testTTLProvider{}, func(context.Context) (string, error) { return prefix, nil }, prometheus.NewRegistry(), log.NewNopLogger())
	c := NewCache[int](factory, testCodec{})

	// Get extracts the org ID for logging, so an org ID must be present in the context.
	ctx := user.InjectOrgID(context.Background(), "tenant-a")
	function := testFunction
	inner := []byte("inner")
	var start, end int64 = 0, 100

	require.NoError(t, c.Set(ctx, function, inner, start, end, nil, querierpb.Annotations{}, []int{1}, types.EncodedOperatorEvaluationStats{}, 1, &CacheStats{}))

	_, _, _, _, found, err := c.Get(ctx, function, inner, start, end, &CacheStats{})
	require.NoError(t, err)
	require.True(t, found, "entry should be found when the same prefix is active")

	prefix = "tenant-b:"
	_, _, _, _, found, err = c.Get(ctx, function, inner, start, end, &CacheStats{})
	require.NoError(t, err)
	require.False(t, found, "entry written under one prefix must not be visible under another prefix")
}
