// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/inmemory_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

// Tests out the index cache implementation.
package indexcache

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/go-kit/log"
	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/storage/sharding"
)

func TestNewInMemoryIndexCache(t *testing.T) {
	// Should return error on invalid YAML config.
	conf := []byte("invalid")
	cache, err := NewInMemoryIndexCache(log.NewNopLogger(), nil, conf)
	assert.Error(t, err)
	assert.Equal(t, (*InMemoryIndexCache)(nil), cache)

	// Should instance an in-memory index cache with default config
	// on empty YAML config.
	conf = []byte{}
	cache, err = NewInMemoryIndexCache(log.NewNopLogger(), nil, conf)
	assert.NoError(t, err)
	assert.Equal(t, uint64(DefaultInMemoryIndexCacheConfig.MaxSize), cache.maxSizeBytes)
	assert.Equal(t, uint64(DefaultInMemoryIndexCacheConfig.MaxItemSize), cache.maxItemSizeBytes)

	// Should instance an in-memory index cache with specified YAML config.s with units.
	conf = []byte(`
max_size: 1MB
max_item_size: 2KB
`)
	cache, err = NewInMemoryIndexCache(log.NewNopLogger(), nil, conf)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1024*1024), cache.maxSizeBytes)
	assert.Equal(t, uint64(2*1024), cache.maxItemSizeBytes)

	// Should instance an in-memory index cache with specified YAML config.s with units.
	conf = []byte(`
max_size: 2KB
max_item_size: 1MB
`)
	cache, err = NewInMemoryIndexCache(log.NewNopLogger(), nil, conf)
	assert.Error(t, err)
	assert.Equal(t, (*InMemoryIndexCache)(nil), cache)
	// assert.Equal(t, uint64(1024*1024), cache.maxSizeBytes)
	// assert.Equal(t, uint64(2*1024), cache.maxItemSizeBytes)

	// assert.Equal(t, uint64(1024*1024), cache.maxItemSizeBytes)
	// assert.Equal(t, uint64(2*1024), cache.maxSizeBytes)
}

func TestInMemoryIndexCache_AvoidsDeadlock(t *testing.T) {
	user := "tenant"
	metrics := prometheus.NewRegistry()
	cache, err := NewInMemoryIndexCacheWithConfig(log.NewNopLogger(), metrics, InMemoryIndexCacheConfig{
		MaxItemSize: sliceHeaderSize + 5,
		MaxSize:     sliceHeaderSize + 5,
	})
	assert.NoError(t, err)

	l, err := simplelru.NewLRU(math.MaxInt64, func(key, val interface{}) {
		// Hack LRU to simulate broken accounting: evictions do not reduce current size.
		size := cache.curSize
		cache.onEvict(key, val)
		cache.curSize = size
	})
	assert.NoError(t, err)
	cache.lru = l

	ctx := context.Background()
	cache.StorePostings(ctx, user, ulid.MustNew(0, nil), labels.Label{Name: "test2", Value: "1"}, []byte{42, 33, 14, 67, 11})

	assert.Equal(t, uint64(sliceHeaderSize+5), cache.curSize)
	assert.Equal(t, float64(cache.curSize), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))

	// This triggers deadlock logic.
	cache.StorePostings(ctx, user, ulid.MustNew(0, nil), labels.Label{Name: "test1", Value: "1"}, []byte{42})

	assert.Equal(t, uint64(sliceHeaderSize+1), cache.curSize)
	assert.Equal(t, float64(cache.curSize), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
}

func TestInMemoryIndexCache_UpdateItem(t *testing.T) {
	const maxSize = 2 * (sliceHeaderSize + 1)

	var errorLogs []string
	errorLogger := log.LoggerFunc(func(kvs ...interface{}) error {
		var lvl string
		for i := 0; i < len(kvs); i += 2 {
			if kvs[i] == "level" {
				lvl = fmt.Sprint(kvs[i+1])
				break
			}
		}
		if lvl != "error" {
			return nil
		}
		var buf bytes.Buffer
		defer func() { errorLogs = append(errorLogs, buf.String()) }()
		return log.NewLogfmtLogger(&buf).Log(kvs...)
	})

	metrics := prometheus.NewRegistry()
	cache, err := NewInMemoryIndexCacheWithConfig(log.NewSyncLogger(errorLogger), metrics, InMemoryIndexCacheConfig{
		MaxItemSize: maxSize,
		MaxSize:     maxSize,
	})
	assert.NoError(t, err)

	user := "tenant"
	uid := func(id uint64) ulid.ULID { return ulid.MustNew(uint64(id), nil) }
	lbl := labels.Label{Name: "foo", Value: "bar"}
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"), labels.MustNewMatcher(labels.MatchNotRegexp, "baz", ".*")}
	shard := &sharding.ShardSelector{ShardIndex: 1, ShardCount: 16}
	ctx := context.Background()

	for _, tt := range []struct {
		typ string
		set func(uint64, []byte)
		get func(uint64) ([]byte, bool)
	}{
		{
			typ: cacheTypePostings,
			set: func(id uint64, b []byte) { cache.StorePostings(ctx, user, uid(id), lbl, b) },
			get: func(id uint64) ([]byte, bool) {
				hits, _ := cache.FetchMultiPostings(ctx, user, uid(id), []labels.Label{lbl})
				b, ok := hits[lbl]

				return b, ok
			},
		},
		{
			typ: cacheTypeSeriesForRef,
			set: func(id uint64, b []byte) {
				cache.StoreSeriesForRef(ctx, user, uid(id), storage.SeriesRef(id), b)
			},
			get: func(id uint64) ([]byte, bool) {
				seriesRef := storage.SeriesRef(id)
				hits, _ := cache.FetchMultiSeriesForRefs(ctx, user, uid(id), []storage.SeriesRef{seriesRef})
				b, ok := hits[seriesRef]

				return b, ok
			},
		},
		{
			typ: cacheTypeExpandedPostings,
			set: func(id uint64, b []byte) {
				cache.StoreExpandedPostings(ctx, user, uid(id), CanonicalLabelMatchersKey(matchers), b)
			},
			get: func(id uint64) ([]byte, bool) {
				return cache.FetchExpandedPostings(ctx, user, uid(id), CanonicalLabelMatchersKey(matchers))
			},
		},
		{
			typ: cacheTypeSeries,
			set: func(id uint64, b []byte) {
				cache.StoreSeries(ctx, user, uid(id), CanonicalLabelMatchersKey(matchers), shard, b)
			},
			get: func(id uint64) ([]byte, bool) {
				return cache.FetchSeries(ctx, user, uid(id), CanonicalLabelMatchersKey(matchers), shard)
			},
		},
		{
			typ: cacheTypeSeriesForPostings,
			set: func(id uint64, b []byte) {
				cache.StoreSeriesForPostings(ctx, user, uid(id), shard, CanonicalPostingsKey([]storage.SeriesRef{1}), b)
			},
			get: func(id uint64) ([]byte, bool) {
				return cache.FetchSeriesForPostings(ctx, user, uid(id), shard, CanonicalPostingsKey([]storage.SeriesRef{1}))
			},
		},
		{
			typ: cacheTypeLabelNames,
			set: func(id uint64, b []byte) {
				cache.StoreLabelNames(ctx, user, uid(id), CanonicalLabelMatchersKey(matchers), b)
			},
			get: func(id uint64) ([]byte, bool) {
				return cache.FetchLabelNames(ctx, user, uid(id), CanonicalLabelMatchersKey(matchers))
			},
		},
		{
			typ: cacheTypeLabelValues,
			set: func(id uint64, b []byte) {
				cache.StoreLabelValues(ctx, user, uid(id), fmt.Sprintf("lbl_%d", id), CanonicalLabelMatchersKey(matchers), b)
			},
			get: func(id uint64) ([]byte, bool) {
				return cache.FetchLabelValues(ctx, user, uid(id), fmt.Sprintf("lbl_%d", id), CanonicalLabelMatchersKey(matchers))
			},
		},
	} {
		t.Run(tt.typ, func(t *testing.T) {
			defer func() { errorLogs = nil }()

			// Set value.
			tt.set(0, []byte{0})
			buf, ok := tt.get(0)
			assert.Equal(t, true, ok)
			assert.Equal(t, []byte{0}, buf)
			assert.Equal(t, float64(sliceHeaderSize+1), promtest.ToFloat64(cache.currentSize.WithLabelValues(tt.typ)))
			assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(tt.typ)))
			assert.Equal(t, []string(nil), errorLogs)

			// Set the same value again.
			// NB: This used to over-count the value.
			tt.set(0, []byte{0})
			buf, ok = tt.get(0)
			assert.Equal(t, true, ok)
			assert.Equal(t, []byte{0}, buf)
			assert.Equal(t, float64(sliceHeaderSize+1), promtest.ToFloat64(cache.currentSize.WithLabelValues(tt.typ)))
			assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(tt.typ)))
			assert.Equal(t, []string(nil), errorLogs)

			// Set a larger value.
			// NB: This used to deadlock when enough values were over-counted and it
			// couldn't clear enough space -- repeatedly removing oldest after empty.
			tt.set(1, []byte{0, 1})
			buf, ok = tt.get(1)
			assert.Equal(t, true, ok)
			assert.Equal(t, []byte{0, 1}, buf)
			assert.Equal(t, float64(sliceHeaderSize+2), promtest.ToFloat64(cache.currentSize.WithLabelValues(tt.typ)))
			assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(tt.typ)))
			assert.Equal(t, []string(nil), errorLogs)

			// Mutations to existing values will be ignored.
			tt.set(1, []byte{1, 2})
			buf, ok = tt.get(1)
			assert.Equal(t, true, ok)
			assert.Equal(t, []byte{0, 1}, buf)
			assert.Equal(t, float64(sliceHeaderSize+2), promtest.ToFloat64(cache.currentSize.WithLabelValues(tt.typ)))
			assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(tt.typ)))
			assert.Equal(t, []string(nil), errorLogs)
		})
	}
}

// This should not happen as we hardcode math.MaxInt, but we still add test to check this out.
func TestInMemoryIndexCache_MaxNumberOfItemsHit(t *testing.T) {
	user := "tenant"
	metrics := prometheus.NewRegistry()
	cache, err := NewInMemoryIndexCacheWithConfig(log.NewNopLogger(), metrics, InMemoryIndexCacheConfig{
		MaxItemSize: 2*sliceHeaderSize + 10,
		MaxSize:     2*sliceHeaderSize + 10,
	})
	assert.NoError(t, err)

	l, err := simplelru.NewLRU(2, cache.onEvict)
	assert.NoError(t, err)
	cache.lru = l

	id := ulid.MustNew(0, nil)
	ctx := context.Background()

	cache.StorePostings(ctx, user, id, labels.Label{Name: "test", Value: "123"}, []byte{42, 33})
	cache.StorePostings(ctx, user, id, labels.Label{Name: "test", Value: "124"}, []byte{42, 33})
	cache.StorePostings(ctx, user, id, labels.Label{Name: "test", Value: "125"}, []byte{42, 33})

	assert.Equal(t, uint64(2*sliceHeaderSize+4), cache.curSize)
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(3), promtest.ToFloat64(cache.added.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.requests.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.hits.WithLabelValues(cacheTypePostings)))
	for _, typ := range remove(allCacheTypes, cacheTypePostings) {
		assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(typ)))
		assert.Equal(t, float64(0), promtest.ToFloat64(cache.evicted.WithLabelValues(typ)))
		assert.Equal(t, float64(0), promtest.ToFloat64(cache.added.WithLabelValues(typ)))
		assert.Equal(t, float64(0), promtest.ToFloat64(cache.requests.WithLabelValues(typ)))
		assert.Equal(t, float64(0), promtest.ToFloat64(cache.hits.WithLabelValues(typ)))
	}
}

func TestInMemoryIndexCache_Eviction_WithMetrics(t *testing.T) {
	user := "tenant"
	metrics := prometheus.NewRegistry()
	cache, err := NewInMemoryIndexCacheWithConfig(log.NewNopLogger(), metrics, InMemoryIndexCacheConfig{
		MaxItemSize: 2*sliceHeaderSize + 5,
		MaxSize:     2*sliceHeaderSize + 5,
	})
	assert.NoError(t, err)

	id := ulid.MustNew(0, nil)
	lbls := labels.Label{Name: "test", Value: "123"}
	ctx := context.Background()
	emptyPostingsHits := map[labels.Label][]byte{}
	emptyPostingsMisses := []labels.Label(nil)
	emptySeriesHits := map[storage.SeriesRef][]byte{}
	emptySeriesMisses := []storage.SeriesRef(nil)

	pHits, pMisses := cache.FetchMultiPostings(ctx, user, id, []labels.Label{lbls})
	assert.Equal(t, emptyPostingsHits, pHits, "no such key")
	assert.Equal(t, []labels.Label{lbls}, pMisses)

	// Add sliceHeaderSize + 2 bytes.
	cache.StorePostings(ctx, user, id, lbls, []byte{42, 33})
	assert.Equal(t, uint64(sliceHeaderSize+2), cache.curSize)
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(sliceHeaderSize+2), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(sliceHeaderSize+2+cacheKeyPostings{user, id, lbls}.size()), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeriesForRef)))

	pHits, pMisses = cache.FetchMultiPostings(ctx, user, id, []labels.Label{lbls})
	assert.Equal(t, map[labels.Label][]byte{lbls: {42, 33}}, pHits, "key exists")
	assert.Equal(t, emptyPostingsMisses, pMisses)

	pHits, pMisses = cache.FetchMultiPostings(ctx, user, ulid.MustNew(1, nil), []labels.Label{lbls})
	assert.Equal(t, emptyPostingsHits, pHits, "no such key")
	assert.Equal(t, []labels.Label{lbls}, pMisses)

	pHits, pMisses = cache.FetchMultiPostings(ctx, user, id, []labels.Label{{Name: "test", Value: "124"}})
	assert.Equal(t, emptyPostingsHits, pHits, "no such key")
	assert.Equal(t, []labels.Label{{Name: "test", Value: "124"}}, pMisses)

	// Add sliceHeaderSize + 3 more bytes.
	cache.StoreSeriesForRef(ctx, user, id, 1234, []byte{222, 223, 224})
	assert.Equal(t, uint64(2*sliceHeaderSize+5), cache.curSize)
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(sliceHeaderSize+2), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(sliceHeaderSize+2+cacheKeyPostings{user, id, lbls}.size()), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(sliceHeaderSize+3), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(sliceHeaderSize+3+cacheKeySeriesForRef{user, id, 1234}.size()), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeriesForRef)))

	sHits, sMisses := cache.FetchMultiSeriesForRefs(ctx, user, id, []storage.SeriesRef{1234})
	assert.Equal(t, map[storage.SeriesRef][]byte{1234: {222, 223, 224}}, sHits, "key exists")
	assert.Equal(t, emptySeriesMisses, sMisses)

	lbls2 := labels.Label{Name: "test", Value: "124"}

	// Add sliceHeaderSize + 5 + 16 bytes, should fully evict 2 last items.
	v := []byte{42, 33, 14, 67, 11}
	for i := 0; i < sliceHeaderSize; i++ {
		v = append(v, 3)
	}
	cache.StorePostings(ctx, user, id, lbls2, v)

	assert.Equal(t, uint64(2*sliceHeaderSize+5), cache.curSize)
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(2*sliceHeaderSize+5), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(2*sliceHeaderSize+5+cacheKeyPostings{user, id, lbls}.size()), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))     // Eviction.
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeriesForRef))) // Eviction.

	// Evicted.
	pHits, pMisses = cache.FetchMultiPostings(ctx, user, id, []labels.Label{lbls})
	assert.Equal(t, emptyPostingsHits, pHits, "no such key")
	assert.Equal(t, []labels.Label{lbls}, pMisses)

	sHits, sMisses = cache.FetchMultiSeriesForRefs(ctx, user, id, []storage.SeriesRef{1234})
	assert.Equal(t, emptySeriesHits, sHits, "no such key")
	assert.Equal(t, []storage.SeriesRef{1234}, sMisses)

	pHits, pMisses = cache.FetchMultiPostings(ctx, user, id, []labels.Label{lbls2})
	assert.Equal(t, map[labels.Label][]byte{lbls2: v}, pHits)
	assert.Equal(t, emptyPostingsMisses, pMisses)

	// Add same item again.
	cache.StorePostings(ctx, user, id, lbls2, v)

	assert.Equal(t, uint64(2*sliceHeaderSize+5), cache.curSize)
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(2*sliceHeaderSize+5), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(2*sliceHeaderSize+5+cacheKeyPostings{user, id, lbls}.size()), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeriesForRef)))

	pHits, pMisses = cache.FetchMultiPostings(ctx, user, id, []labels.Label{lbls2})
	assert.Equal(t, map[labels.Label][]byte{lbls2: v}, pHits)
	assert.Equal(t, emptyPostingsMisses, pMisses)

	// Add too big item.
	cache.StorePostings(ctx, user, id, labels.Label{Name: "test", Value: "toobig"}, append(v, 5))
	assert.Equal(t, uint64(2*sliceHeaderSize+5), cache.curSize)
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(2*sliceHeaderSize+5), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(2*sliceHeaderSize+5+cacheKeyPostings{user, id, lbls}.size()), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings))) // Overflow.
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeriesForRef)))

	_, _, ok := cache.lru.RemoveOldest()
	assert.True(t, ok, "something to remove")

	assert.Equal(t, uint64(0), cache.curSize)
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(2), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeriesForRef)))

	_, _, ok = cache.lru.RemoveOldest()
	assert.True(t, !ok, "nothing to remove")

	lbls3 := labels.Label{Name: "test", Value: "124"}

	cache.StorePostings(ctx, user, id, lbls3, []byte{})

	assert.Equal(t, uint64(sliceHeaderSize), cache.curSize)
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(sliceHeaderSize), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(sliceHeaderSize+cacheKeyPostings{user, id, lbls3}.size()), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(2), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeriesForRef)))

	pHits, pMisses = cache.FetchMultiPostings(ctx, user, id, []labels.Label{lbls3})
	assert.Equal(t, map[labels.Label][]byte{lbls3: {}}, pHits, "key exists")
	assert.Equal(t, emptyPostingsMisses, pMisses)

	// nil works and still allocates empty slice.
	lbls4 := labels.Label{Name: "test", Value: "125"}
	cache.StorePostings(ctx, user, id, lbls4, []byte(nil))

	assert.Equal(t, 2*uint64(sliceHeaderSize), cache.curSize)
	assert.Equal(t, float64(2), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, 2*float64(sliceHeaderSize), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, 2*float64(sliceHeaderSize+cacheKeyPostings{user, id, lbls4}.size()), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.totalCurrentSize.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(0), promtest.ToFloat64(cache.overflow.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(2), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.evicted.WithLabelValues(cacheTypeSeriesForRef)))

	pHits, pMisses = cache.FetchMultiPostings(ctx, user, id, []labels.Label{lbls4})
	assert.Equal(t, map[labels.Label][]byte{lbls4: {}}, pHits, "key exists")
	assert.Equal(t, emptyPostingsMisses, pMisses)

	// Other metrics.
	assert.Equal(t, float64(4), promtest.ToFloat64(cache.added.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.added.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(9), promtest.ToFloat64(cache.requests.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(2), promtest.ToFloat64(cache.requests.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(5), promtest.ToFloat64(cache.hits.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.hits.WithLabelValues(cacheTypeSeriesForRef)))
}
