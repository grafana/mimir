// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/cache_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexcache

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestMain(m *testing.M) {
	test.VerifyNoLeakTestMain(m)
}

func TestCanonicalLabelMatchersKey(t *testing.T) {
	m1 := labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")
	m2 := labels.MustNewMatcher(labels.MatchEqual, "bar", "foo")
	m3 := labels.MustNewMatcher(labels.MatchNotEqual, "foo", "bar")
	m4 := labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar")
	m5 := labels.MustNewMatcher(labels.MatchNotRegexp, "foo", "bar")

	// No matter what order we combine these matchers in, they should always generate the same key.
	const expected = LabelMatchersKey("bar=foo\x00foo=bar\x00foo!=bar\x00foo=~bar\x00foo!~bar\x00")
	allMatchers := []*labels.Matcher{m1, m2, m3, m4, m5}

	for range 10 {
		rand.Shuffle(len(allMatchers), func(i, j int) {
			allMatchers[i], allMatchers[j] = allMatchers[j], allMatchers[i]
		})

		t.Run(fmt.Sprintf("input=%s", allMatchers), func(t *testing.T) {
			assert.Equal(t, expected, CanonicalLabelMatchersKey(allMatchers), "did not get expected key %s from input %s", expected, allMatchers)
		})
	}
}

func BenchmarkCanonicalLabelMatchersKey(b *testing.B) {
	ms := make([]*labels.Matcher, 20)
	for i := range ms {
		ms[i] = labels.MustNewMatcher(labels.MatchType(i%4), fmt.Sprintf("%04x", i%3), fmt.Sprintf("%04x", i%2))
	}
	for _, l := range []int{1, 5, 10, 20} {
		b.Run(fmt.Sprintf("%d matchers", l), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = CanonicalLabelMatchersKey(ms[:l])
			}
		})
	}
}

func BenchmarkCanonicalPostingsKey(b *testing.B) {
	ms := make([]storage.SeriesRef, 1_000_000)
	for i := range ms {
		ms[i] = storage.SeriesRef(i)
	}
	for numPostings := 10; numPostings <= len(ms); numPostings *= 10 {
		b.Run(fmt.Sprintf("%d postings", numPostings), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = CanonicalPostingsKey(ms[:numPostings])
			}
		})
	}
}

func TestUnsafeCastPostingsToBytes(t *testing.T) {
	slowPostingsToBytes := func(postings []storage.SeriesRef) []byte {
		byteSlice := make([]byte, len(postings)*8)
		for i, posting := range postings {
			for octet := 0; octet < 8; octet++ {
				byteSlice[i*8+octet] = byte(posting >> (octet * 8))
			}
		}
		return byteSlice
	}
	t.Run("base case", func(t *testing.T) {
		postings := []storage.SeriesRef{1, 2}
		assert.Equal(t, slowPostingsToBytes(postings), unsafeCastPostingsToBytes(postings))
	})
	t.Run("zero-length postings", func(t *testing.T) {
		postings := make([]storage.SeriesRef, 0)
		assert.Equal(t, slowPostingsToBytes(postings), unsafeCastPostingsToBytes(postings))
	})
	t.Run("nil postings", func(t *testing.T) {
		assert.Equal(t, []byte(nil), unsafeCastPostingsToBytes(nil))
	})
	t.Run("more than 256 postings", func(t *testing.T) {
		// Only casting a slice pointer truncates all postings to only their last byte.
		postings := make([]storage.SeriesRef, 300)
		for i := range postings {
			postings[i] = storage.SeriesRef(i + 1)
		}
		assert.Equal(t, slowPostingsToBytes(postings), unsafeCastPostingsToBytes(postings))
	})
}

func TestCanonicalPostingsKey(t *testing.T) {
	t.Run("same length postings have different hashes", func(t *testing.T) {
		postings1 := []storage.SeriesRef{1, 2, 3, 4}
		postings2 := []storage.SeriesRef{5, 6, 7, 8}

		assert.NotEqual(t, CanonicalPostingsKey(postings1), CanonicalPostingsKey(postings2))
	})

	t.Run("when postings are a subset of each other, they still have different hashes", func(t *testing.T) {
		postings1 := []storage.SeriesRef{1, 2, 3, 4}
		postings2 := []storage.SeriesRef{1, 2, 3, 4, 5}

		assert.NotEqual(t, CanonicalPostingsKey(postings1), CanonicalPostingsKey(postings2))
	})

	t.Run("same postings with different slice capacities have same hashes", func(t *testing.T) {
		postings1 := []storage.SeriesRef{1, 2, 3, 4}
		postings2 := make([]storage.SeriesRef, 4, 8)
		copy(postings2, postings1)

		assert.Equal(t, CanonicalPostingsKey(postings1), CanonicalPostingsKey(postings2))
	})

	t.Run("postings key is a base64-encoded string (i.e. is printable)", func(t *testing.T) {
		key := CanonicalPostingsKey([]storage.SeriesRef{1, 2, 3, 4})
		_, err := base64.RawURLEncoding.DecodeString(string(key))
		assert.NoError(t, err)
	})
}

// TestRemoteIndexCacheWithL1LRU exercises the LRU wrapping path that newMemcachedIndexCache
// applies when MemcachedInMemoryMaxItems > 0. The wiring inside newMemcachedIndexCache itself
// requires a real memcached connection to construct, so this test mirrors the wrap order
// (mock cache -> WrapWithLRUCache -> RemoteIndexCache) and asserts that fetches are served
// from L1 without touching the inner cache after the L1 has been populated by Store.
func TestRemoteIndexCacheWithL1LRU(t *testing.T) {
	innerMock := newMockedRemoteCacheClient(nil)
	l1Reg := prometheus.NewPedanticRegistry()

	wrapped, err := cache.WrapWithLRUCache(innerMock, "index-cache", l1Reg, 100, time.Hour, log.NewNopLogger())
	require.NoError(t, err)

	rc, err := NewRemoteIndexCache(log.NewNopLogger(), wrapped, prometheus.NewPedanticRegistry())
	require.NoError(t, err)

	user := "tenant1"
	blockID := ulid.MustNew(1, nil)
	id := storage.SeriesRef(123)
	value := []byte("series-bytes")

	// Storing populates both L1 and the inner cache (the LRU wrapper writes through).
	rc.StoreSeriesForRef(user, blockID, id, value, time.Hour)

	ctx := context.Background()

	// Two fetches for the same key — both should hit L1 and return the stored value.
	hits1, misses1 := rc.FetchMultiSeriesForRefs(ctx, user, blockID, []storage.SeriesRef{id})
	require.Empty(t, misses1)
	require.Equal(t, value, hits1[id])

	hits2, misses2 := rc.FetchMultiSeriesForRefs(ctx, user, blockID, []storage.SeriesRef{id})
	require.Empty(t, misses2)
	require.Equal(t, value, hits2[id])

	// L1 metrics: 2 requests, 2 hits.
	assert.Equal(t, 2.0, gatherCounter(t, l1Reg, "cache_memory_requests_total"))
	assert.Equal(t, 2.0, gatherCounter(t, l1Reg, "cache_memory_hits_total"))
}

// TestRemoteIndexCacheWithoutL1 confirms behavior is unchanged when L1 is disabled —
// the inner cache is used directly.
func TestRemoteIndexCacheWithoutL1(t *testing.T) {
	innerMock := newMockedRemoteCacheClient(nil)
	rc, err := NewRemoteIndexCache(log.NewNopLogger(), innerMock, prometheus.NewPedanticRegistry())
	require.NoError(t, err)

	user := "tenant1"
	blockID := ulid.MustNew(1, nil)
	id := storage.SeriesRef(123)
	value := []byte("series-bytes")

	rc.StoreSeriesForRef(user, blockID, id, value, time.Hour)

	hits, misses := rc.FetchMultiSeriesForRefs(context.Background(), user, blockID, []storage.SeriesRef{id})
	require.Empty(t, misses)
	require.Equal(t, value, hits[id])
}

// gatherCounter sums all sample values for a counter metric across all label sets.
func gatherCounter(t *testing.T, g prometheus.Gatherer, name string) float64 {
	t.Helper()
	mfs, err := g.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var sum float64
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				sum += c.GetValue()
			}
		}
		return sum
	}
	return 0
}

func TestBlockTTL(t *testing.T) {
	for _, tt := range []struct {
		name string
		minT int64
		maxT int64
		ttl  time.Duration
	}{
		{
			name: "30m block",
			minT: 1700000000000,
			maxT: 1700001800000,
			ttl:  1 * time.Hour,
		},
		{
			name: "1h block",
			minT: 1700000000000,
			maxT: 1700003600000,
			ttl:  1 * time.Hour,
		},
		{
			name: "65m block",
			minT: 1700000000000,
			maxT: 1700003900000,
			ttl:  2 * time.Hour,
		},
		{
			name: "2h block",
			minT: 1700000000000,
			maxT: 1700007200000,
			ttl:  2 * time.Hour,
		},
		{
			name: "3h block",
			minT: 1700000000000,
			maxT: 1700010800000,
			ttl:  3 * time.Hour,
		},
		{
			name: "185m block",
			minT: 1700000000000,
			maxT: 1700011100000,
			ttl:  4 * time.Hour,
		},
		{
			name: "10h block",
			minT: 1700000000000,
			maxT: 1700036000000,
			ttl:  10 * time.Hour,
		},
		{
			name: "12h block",
			minT: 1700000000000,
			maxT: 1700043200000,
			ttl:  12 * time.Hour,
		},
		{
			name: "20h block",
			minT: 1700000000000,
			maxT: 1700072000000,
			ttl:  20 * time.Hour,
		},
		{
			name: "24h block",
			minT: 1700000000000,
			maxT: 1700086400000,
			ttl:  168 * time.Hour,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			meta := &block.Meta{
				BlockMeta: tsdb.BlockMeta{
					MinTime: tt.minT,
					MaxTime: tt.maxT,
				},
			}

			ttl := BlockTTL(meta)
			assert.Equal(t, tt.ttl, ttl)
		})
	}
}

// slowCache wraps a cache.Cache and adds a fixed sleep to GetMultiWithError, used
// only by BenchmarkL1IndexCache_HitPath to simulate the per-call cost of a real
// memcached round-trip without needing a real backend.
type slowCache struct {
	cache.Cache
	latency time.Duration
}

func (s *slowCache) GetMultiWithError(ctx context.Context, keys []string, opts ...cache.Option) (map[string][]byte, error) {
	if s.latency > 0 {
		time.Sleep(s.latency)
	}
	return s.Cache.GetMultiWithError(ctx, keys, opts...)
}

// BenchmarkL1IndexCache_HitPath compares serving a hot key from the L1 LRU against
// paying a simulated memcached round-trip on every read. The inner cache is wrapped
// with a fixed-latency sleep so the benchmark surfaces the avoided round-trip cost
// without needing an actual memcached server.
//
// The benchmark also exposes the per-pod LRU mutex contention noted in the
// "*-memcached-inmemory-max-items" help text: under high parallelism, even L1 hits
// serialize on the LRU mutex, so the with-l1 numbers will not scale linearly with
// GOMAXPROCS — that's expected and is the documented limitation.
func BenchmarkL1IndexCache_HitPath(b *testing.B) {
	const fetchLatency = 100 * time.Microsecond
	const hotKey = "hot-key"
	hotVal := []byte("v")

	b.Run("with-l1", func(b *testing.B) {
		inner := &slowCache{Cache: newMockedRemoteCacheClient(nil), latency: fetchLatency}
		l1, err := cache.WrapWithLRUCache(inner, "test", prometheus.NewPedanticRegistry(), 100, time.Hour, log.NewNopLogger())
		require.NoError(b, err)

		// Pre-populate L1 so every benchmark iteration is a hit.
		l1.SetAsync(hotKey, hotVal, time.Hour)

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			keys := []string{hotKey}
			for pb.Next() {
				_, _ = l1.GetMultiWithError(context.Background(), keys)
			}
		})
	})

	b.Run("without-l1", func(b *testing.B) {
		inner := &slowCache{Cache: newMockedRemoteCacheClient(nil), latency: fetchLatency}
		inner.SetAsync(hotKey, hotVal, time.Hour)

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			keys := []string{hotKey}
			for pb.Next() {
				_, _ = inner.GetMultiWithError(context.Background(), keys)
			}
		})
	})
}
