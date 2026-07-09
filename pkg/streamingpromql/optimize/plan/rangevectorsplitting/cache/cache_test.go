// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"bytes"
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

// TestCacheGetMulti verifies that a single GetMulti call handles a mix of hits, misses, undecodable entries and
// hashed-key collisions, degrading each problematic entry to a miss without failing the whole batch.
func TestCacheGetMulti(t *testing.T) {
	backend := caching.NewInMemoryCache()
	keyGenerator := caching.NewCacheKeyGenerator(nil, caching.StaticPrefixGenerator("tenant-a:"))
	factory := NewCacheFactoryWithBackend(backend, testTTLProvider{}, keyGenerator, prometheus.NewRegistry(), log.NewNopLogger())
	c := NewCache[int](factory, testCodec{})

	ctx := user.InjectOrgID(context.Background(), "tenant-a")
	inner := []byte("inner")

	hit1 := GetRange{Start: 0, End: 100}
	miss := GetRange{Start: 100, End: 200}
	hit2 := GetRange{Start: 200, End: 300}
	undecodable := GetRange{Start: 300, End: 400}
	collision := GetRange{Start: 400, End: 500}

	require.NoError(t, c.Set(ctx, testFunction, inner, hit1.Start, hit1.End, nil, querierpb.Annotations{}, []int{1, 2}, types.EncodedOperatorEvaluationStats{}, 2, &CacheStats{}))
	require.NoError(t, c.Set(ctx, testFunction, inner, hit2.Start, hit2.End, nil, querierpb.Annotations{}, []int{3}, types.EncodedOperatorEvaluationStats{}, 1, &CacheStats{}))

	// Store bytes that don't decode as a CachedSeries
	undecodableKey, err := TestGenerateHashedCacheKey(ctx, keyGenerator, testFunction, inner, undecodable.Start, undecodable.End)
	require.NoError(t, err)
	require.NoError(t, backend.SetAsync(ctx, undecodableKey, []byte{0xff, 0xff, 0xff}, time.Hour))

	collided := &CachedSeries{CacheKey: []byte("some-other-key")}
	collidedData, err := collided.Marshal()
	require.NoError(t, err)
	collisionKey, err := TestGenerateHashedCacheKey(ctx, keyGenerator, testFunction, inner, collision.Start, collision.End)
	require.NoError(t, err)
	require.NoError(t, backend.SetAsync(ctx, collisionKey, collidedData, time.Hour))

	stats := &CacheStats{}
	results, err := c.GetMulti(ctx, testFunction, inner, []GetRange{hit1, miss, hit2, undecodable, collision}, stats)
	require.NoError(t, err)
	require.Len(t, results, 5)

	require.True(t, results[0].Found)
	require.False(t, results[1].Found, "range without a cache entry should be a miss")
	require.True(t, results[2].Found)
	require.False(t, results[3].Found, "range with an undecodable cache entry should be a miss")
	require.False(t, results[4].Found, "range with a colliding cache entry should be a miss")

	require.Equal(t, 1, backend.GetCount, "all ranges should be fetched with a single backend request")
	require.Equal(t, 5, backend.KeysCount)
}

// TestCacheKeysIsolatesPrefixes verifies that the same inner query under two different prefixes
// produces different full keys and different hashed keys. Because the prefix is part of the full
// key stored in the cache entry, even a hash collision across tenants or label policies would be
// rejected by the stored-key comparison rather than silently serving another tenant's data.
func TestCacheKeysIsolatesPrefixes(t *testing.T) {
	function := testFunction
	inner := []byte("inner")
	var start, end int64 = 0, 100

	tenantA := &Cache[int]{keyGenerator: caching.NewCacheKeyGenerator(nil, caching.StaticPrefixGenerator("tenant-a:"))}
	tenantB := &Cache[int]{keyGenerator: caching.NewCacheKeyGenerator(nil, caching.StaticPrefixGenerator("tenant-b:"))}

	keyA, hashedA, err := tenantA.cacheKeys(context.Background(), function, inner, start, end)
	require.NoError(t, err)
	keyB, hashedB, err := tenantB.cacheKeys(context.Background(), function, inner, start, end)
	require.NoError(t, err)

	require.True(t, bytes.HasPrefix(keyA, []byte("tenant-a:")), "full key must start with the tenant's prefix")
	require.True(t, bytes.HasPrefix(keyB, []byte("tenant-b:")), "full key must start with the tenant's prefix")

	require.NotEqual(t, keyA, keyB)
	require.NotEqual(t, hashedA, hashedB)
}

// TestCacheIsolatesTenants verifies end-to-end that an entry written under one prefix is not
// returned when a different prefix is active.
func TestCacheIsolatesTenants(t *testing.T) {
	backend := caching.NewInMemoryCache()

	prefix := "tenant-a:"
	prefixGeneratorFunc := func(ctx context.Context) (string, error) { return prefix, nil }
	factory := NewCacheFactoryWithBackend(backend, testTTLProvider{}, caching.NewCacheKeyGenerator(nil, prefixGeneratorFunc), prometheus.NewRegistry(), log.NewNopLogger())
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
