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
	"time"

	"github.com/go-kit/log"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"

	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/sharding"
)

func TestInMemoryIndexCache_AvoidsDeadlock(t *testing.T) {
	user := "tenant"
	metrics := prometheus.NewRegistry()
	cfg := IndexCacheConfig{
		InMemory: InMemoryIndexCacheConfig{
			MaxItemSizeBytes:  sliceHeaderSize + 5,
			MaxCacheSizeBytes: sliceHeaderSize + 5,
		},
	}
	cache, err := NewInMemoryIndexCacheWithConfig(cfg, metrics, log.NewNopLogger())
	assert.NoError(t, err)

	l, err := simplelru.NewLRU(math.MaxInt64, func(key cacheKey, val []byte) {
		// Hack LRU to simulate broken accounting: evictions do not reduce current size.
		size := cache.curSize
		cache.onEvict(key, val)
		cache.curSize = size
	})
	assert.NoError(t, err)
	cache.lru = l

	cache.StorePostings(user, ulid.MustNew(0, nil), labels.Label{Name: "test2", Value: "1"}, []byte{42, 33, 14, 67, 11}, time.Hour)

	assert.Equal(t, uint64(sliceHeaderSize+5), cache.curSize)
	assert.Equal(t, float64(cache.curSize), promtest.ToFloat64(cache.currentSize.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(cacheTypePostings)))

	// This triggers deadlock logic.
	cache.StorePostings(user, ulid.MustNew(0, nil), labels.Label{Name: "test1", Value: "1"}, []byte{42}, time.Hour)

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
	cfg := IndexCacheConfig{
		CachePostingsOffsets: true,
		InMemory: InMemoryIndexCacheConfig{
			MaxItemSizeBytes:  maxSize,
			MaxCacheSizeBytes: maxSize,
		},
	}
	cache, err := NewInMemoryIndexCacheWithConfig(cfg, metrics, log.NewSyncLogger(errorLogger))
	assert.NoError(t, err)

	user := "tenant"
	uid := func(id uint64) ulid.ULID { return ulid.MustNew(uint64(id), nil) }
	lbl := labels.Label{Name: "foo", Value: "bar"}
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"), labels.MustNewMatcher(labels.MatchNotRegexp, "baz", ".*")}
	shard := &sharding.ShardSelector{ShardIndex: 1, ShardCount: 16}
	ctx := context.Background()

	// Cache interfaces for PostingsOffset and PostingsOffSetsForMatcher
	// use structs rather than bytes, wrapping encode/decode within the method.
	// Stub out the encoded versions here; other interfaces just take arbitrary byte blobs.
	rangeSmall := encodeSingleRange(index.Range{Start: 1, End: 2})
	rangeLarge := encodeSingleRange(index.Range{Start: 1 << 14, End: 1 << 20})
	rangeMutate := encodeSingleRange(index.Range{Start: 99, End: 100})

	offsetsSmall := encodePostingsOffsets([]streamindex.PostingListOffset{
		{LabelValue: "a", Off: index.Range{Start: 1, End: 2}},
	})
	offsetsLarge := encodePostingsOffsets([]streamindex.PostingListOffset{
		{LabelValue: "aa", Off: index.Range{Start: 3, End: 4}},
		{LabelValue: "bb", Off: index.Range{Start: 5, End: 6}},
	})
	offsetsMutate := encodePostingsOffsets([]streamindex.PostingListOffset{
		{LabelValue: "z", Off: index.Range{Start: 5, End: 6}},
	})

	for _, tt := range []struct {
		typ                           string
		smallVal, largeVal, mutateVal []byte
		set                           func(uint64, []byte)
		get                           func(uint64) ([]byte, bool)
	}{
		{
			typ:       cacheTypePostings,
			smallVal:  []byte{0},
			largeVal:  []byte{0, 1},
			mutateVal: []byte{1, 2},
			set:       func(id uint64, b []byte) { cache.StorePostings(user, uid(id), lbl, b, time.Hour) },
			get: func(id uint64) ([]byte, bool) {
				hits := cache.FetchMultiPostings(ctx, user, uid(id), []labels.Label{lbl})
				b, _ := hits.Next()
				return b, b != nil
			},
		},
		{
			typ:       cacheTypeSeriesForRef,
			smallVal:  []byte{0},
			largeVal:  []byte{0, 1},
			mutateVal: []byte{1, 2},
			set: func(id uint64, b []byte) {
				cache.StoreSeriesForRef(user, uid(id), storage.SeriesRef(id), b, time.Hour)
			},
			get: func(id uint64) ([]byte, bool) {
				seriesRef := storage.SeriesRef(id)
				hits, _ := cache.FetchMultiSeriesForRefs(ctx, user, uid(id), []storage.SeriesRef{seriesRef})
				b, ok := hits[seriesRef]

				return b, ok
			},
		},
		{
			typ:       cacheTypeExpandedPostings,
			smallVal:  []byte{0},
			largeVal:  []byte{0, 1},
			mutateVal: []byte{1, 2},
			set: func(id uint64, b []byte) {
				cache.StoreExpandedPostings(user, uid(id), CanonicalLabelMatchersKey(matchers), "strategy", b)
			},
			get: func(id uint64) ([]byte, bool) {
				return cache.FetchExpandedPostings(ctx, user, uid(id), CanonicalLabelMatchersKey(matchers), "strategy")
			},
		},
		{
			typ:       cacheTypeSeriesForPostings,
			smallVal:  []byte{0},
			largeVal:  []byte{0, 1},
			mutateVal: []byte{1, 2},
			set: func(id uint64, b []byte) {
				cache.StoreSeriesForPostings(user, uid(id), shard, CanonicalPostingsKey([]storage.SeriesRef{1}), b)
			},
			get: func(id uint64) ([]byte, bool) {
				return cache.FetchSeriesForPostings(ctx, user, uid(id), shard, CanonicalPostingsKey([]storage.SeriesRef{1}))
			},
		},
		{
			typ:       cacheTypeLabelNames,
			smallVal:  []byte{0},
			largeVal:  []byte{0, 1},
			mutateVal: []byte{1, 2},
			set: func(id uint64, b []byte) {
				cache.StoreLabelNames(user, uid(id), CanonicalLabelMatchersKey(matchers), b)
			},
			get: func(id uint64) ([]byte, bool) {
				return cache.FetchLabelNames(ctx, user, uid(id), CanonicalLabelMatchersKey(matchers))
			},
		},
		{
			typ:       cacheTypeLabelValues,
			smallVal:  []byte{0},
			largeVal:  []byte{0, 1},
			mutateVal: []byte{1, 2},
			set: func(id uint64, b []byte) {
				cache.StoreLabelValues(user, uid(id), fmt.Sprintf("lbl_%d", id), CanonicalLabelMatchersKey(matchers), b)
			},
			get: func(id uint64) ([]byte, bool) {
				return cache.FetchLabelValues(ctx, user, uid(id), fmt.Sprintf("lbl_%d", id), CanonicalLabelMatchersKey(matchers))
			},
		},
		{
			typ:       cacheTypePostingsOffset,
			smallVal:  rangeSmall,
			largeVal:  rangeLarge,
			mutateVal: rangeMutate,
			// StorePostingsOffset & FetchPostingsOffset use index.Range as value type.
			// Set & get ops use extra decode step for compat with []byte type in test.
			set: func(id uint64, b []byte) {
				rng, err := decodeSingleRange(b)
				assert.NoError(t, err)
				cache.StorePostingsOffset(user, uid(id), lbl, rng, time.Hour)
			},
			get: func(id uint64) ([]byte, bool) {
				rng, ok := cache.FetchPostingsOffset(ctx, user, uid(id), lbl)
				if !ok {
					return nil, false
				}
				return encodeSingleRange(rng), true
			},
		},
		{
			typ:       cacheTypePostingsOffsetsForMatcher,
			smallVal:  offsetsSmall,
			largeVal:  offsetsLarge,
			mutateVal: offsetsMutate,
			// StorePostingsOffsetsForMatcher & FetchPostingsOffsetsForMatcher use streamindex.PostingListOffset as value type.
			// Set & get ops use extra decode step for compat with []byte type in test.
			set: func(id uint64, b []byte) {
				offs, err := decodePostingsOffsets(b)
				assert.NoError(t, err)
				cache.StorePostingsOffsetsForMatcher(user, uid(id), lbl.Name, matchers[0], false, offs, time.Hour)
			},
			get: func(id uint64) ([]byte, bool) {
				offs, ok := cache.FetchPostingsOffsetsForMatcher(ctx, user, uid(id), lbl.Name, matchers[0], false)
				if !ok {
					return nil, false
				}
				return encodePostingsOffsets(offs), true
			},
		},
	} {
		t.Run(tt.typ, func(t *testing.T) {
			defer func() { errorLogs = nil }()

			smallSize := float64(sliceHeaderSize + len(tt.smallVal))
			largeSize := float64(sliceHeaderSize + len(tt.largeVal))

			// Set value.
			tt.set(0, tt.smallVal)
			buf, ok := tt.get(0)
			assert.Equal(t, true, ok)
			assert.Equal(t, tt.smallVal, buf)
			assert.Equal(t, smallSize, promtest.ToFloat64(cache.currentSize.WithLabelValues(tt.typ)))
			assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(tt.typ)))
			assert.Equal(t, []string(nil), errorLogs)

			// Set the same value again.
			// NB: This used to over-count the value.
			tt.set(0, tt.smallVal)
			buf, ok = tt.get(0)
			assert.Equal(t, true, ok)
			assert.Equal(t, tt.smallVal, buf)
			assert.Equal(t, smallSize, promtest.ToFloat64(cache.currentSize.WithLabelValues(tt.typ)))
			assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(tt.typ)))
			assert.Equal(t, []string(nil), errorLogs)

			// Set a larger value.
			// NB: This used to deadlock when enough values were over-counted and it
			// couldn't clear enough space -- repeatedly removing oldest after empty.
			tt.set(1, tt.largeVal)
			buf, ok = tt.get(1)
			assert.Equal(t, true, ok)
			assert.Equal(t, tt.largeVal, buf)
			assert.Equal(t, largeSize, promtest.ToFloat64(cache.currentSize.WithLabelValues(tt.typ)))
			assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(tt.typ)))
			assert.Equal(t, []string(nil), errorLogs)

			// Mutations to existing values will be ignored.
			tt.set(1, tt.mutateVal)
			buf, ok = tt.get(1)
			assert.Equal(t, true, ok)
			assert.Equal(t, tt.largeVal, buf)
			assert.Equal(t, largeSize, promtest.ToFloat64(cache.currentSize.WithLabelValues(tt.typ)))
			assert.Equal(t, float64(1), promtest.ToFloat64(cache.current.WithLabelValues(tt.typ)))
			assert.Equal(t, []string(nil), errorLogs)
		})
	}
}

func TestInMemoryIndexCache_PostingsOffsetsDisabled(t *testing.T) {
	tenant := "tenant-0"
	block := ulid.MustNew(1, nil)
	lbl := labels.Label{Name: "instance", Value: "a"}
	matcher := labels.MustNewMatcher(labels.MatchEqual, "instance", "a")
	offsets := []streamindex.PostingListOffset{
		{LabelValue: "a", Off: index.Range{Start: 1, End: 2}},
	}

	cfg := IndexCacheConfig{
		InMemory: InMemoryIndexCacheConfig{
			MaxItemSizeBytes:  1024,
			MaxCacheSizeBytes: 1024,
		},
	}
	cache, err := NewInMemoryIndexCacheWithConfig(cfg, prometheus.NewRegistry(), log.NewNopLogger())
	assert.NoError(t, err)

	ctx := context.Background()

	// Stores must no-op: nothing should land in the underlying LRU.
	cache.StorePostingsOffset(tenant, block, lbl, index.Range{Start: 4, End: 16}, time.Hour)
	cache.StorePostingsOffsetsForMatcher(tenant, block, lbl.Name, matcher, false, offsets, time.Hour)
	assert.Equal(t, 0, cache.lru.Len())

	// Fetches must return zero values without consulting the LRU.
	rng, ok := cache.FetchPostingsOffset(ctx, tenant, block, lbl)
	assert.False(t, ok)
	assert.Equal(t, index.Range{}, rng)

	gotOffsets, ok := cache.FetchPostingsOffsetsForMatcher(ctx, tenant, block, lbl.Name, matcher, false)
	assert.False(t, ok)
	assert.Nil(t, gotOffsets)
}

// This should not happen as we hardcode math.MaxInt, but we still add test to check this out.
func TestInMemoryIndexCache_MaxNumberOfItemsHit(t *testing.T) {
	user := "tenant"
	metrics := prometheus.NewRegistry()
	cfg := IndexCacheConfig{
		InMemory: InMemoryIndexCacheConfig{
			MaxItemSizeBytes:  2*sliceHeaderSize + 10,
			MaxCacheSizeBytes: 2*sliceHeaderSize + 10,
		},
	}
	cache, err := NewInMemoryIndexCacheWithConfig(cfg, metrics, log.NewNopLogger())
	assert.NoError(t, err)

	l, err := simplelru.NewLRU(2, cache.onEvict)
	assert.NoError(t, err)
	cache.lru = l

	id := ulid.MustNew(0, nil)

	cache.StorePostings(user, id, labels.Label{Name: "test", Value: "123"}, []byte{42, 33}, time.Hour)
	cache.StorePostings(user, id, labels.Label{Name: "test", Value: "124"}, []byte{42, 33}, time.Hour)
	cache.StorePostings(user, id, labels.Label{Name: "test", Value: "125"}, []byte{42, 33}, time.Hour)

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
	cfg := IndexCacheConfig{
		InMemory: InMemoryIndexCacheConfig{
			MaxItemSizeBytes:  2*sliceHeaderSize + 5,
			MaxCacheSizeBytes: 2*sliceHeaderSize + 5,
		},
	}
	cache, err := NewInMemoryIndexCacheWithConfig(cfg, metrics, log.NewNopLogger())
	assert.NoError(t, err)

	id := ulid.MustNew(0, nil)
	lbls := labels.Label{Name: "test", Value: "123"}
	ctx := context.Background()
	emptySeriesHits := map[storage.SeriesRef][]byte{}
	emptySeriesMisses := []storage.SeriesRef(nil)

	testFetchMultiPostings(ctx, t, cache, user, id, []labels.Label{lbls}, nil)

	// Add sliceHeaderSize + 2 bytes.
	cache.StorePostings(user, id, lbls, []byte{42, 33}, time.Hour)
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

	testFetchMultiPostings(ctx, t, cache, user, id, []labels.Label{lbls}, map[labels.Label][]byte{lbls: {42, 33}})

	testFetchMultiPostings(ctx, t, cache, user, ulid.MustNew(1, nil), []labels.Label{lbls}, nil)

	testFetchMultiPostings(ctx, t, cache, user, id, []labels.Label{{Name: "test", Value: "124"}}, nil)

	// Add sliceHeaderSize + 3 more bytes.
	cache.StoreSeriesForRef(user, id, 1234, []byte{222, 223, 224}, time.Hour)
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
	cache.StorePostings(user, id, lbls2, v, time.Hour)

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
	testFetchMultiPostings(ctx, t, cache, user, id, []labels.Label{lbls}, nil)

	sHits, sMisses = cache.FetchMultiSeriesForRefs(ctx, user, id, []storage.SeriesRef{1234})
	assert.Equal(t, emptySeriesHits, sHits, "no such key")
	assert.Equal(t, []storage.SeriesRef{1234}, sMisses)

	testFetchMultiPostings(ctx, t, cache, user, id, []labels.Label{lbls2}, map[labels.Label][]byte{lbls2: v})

	// Add same item again.
	cache.StorePostings(user, id, lbls2, v, time.Hour)

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

	testFetchMultiPostings(ctx, t, cache, user, id, []labels.Label{lbls2}, map[labels.Label][]byte{lbls2: v})

	// Add too big item.
	cache.StorePostings(user, id, labels.Label{Name: "test", Value: "toobig"}, append(v, 5), time.Hour)
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

	cache.StorePostings(user, id, lbls3, []byte{}, time.Hour)

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

	testFetchMultiPostings(ctx, t, cache, user, id, []labels.Label{lbls3}, map[labels.Label][]byte{lbls3: {}})

	// nil works and still allocates empty slice.
	lbls4 := labels.Label{Name: "test", Value: "125"}
	cache.StorePostings(user, id, lbls4, []byte(nil), time.Hour)

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

	testFetchMultiPostings(ctx, t, cache, user, id, []labels.Label{lbls4}, map[labels.Label][]byte{lbls4: {}})

	// Other metrics.
	assert.Equal(t, float64(4), promtest.ToFloat64(cache.added.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.added.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(9), promtest.ToFloat64(cache.requests.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(2), promtest.ToFloat64(cache.requests.WithLabelValues(cacheTypeSeriesForRef)))
	assert.Equal(t, float64(5), promtest.ToFloat64(cache.hits.WithLabelValues(cacheTypePostings)))
	assert.Equal(t, float64(1), promtest.ToFloat64(cache.hits.WithLabelValues(cacheTypeSeriesForRef)))
}

func testFetchPostingsOffsets(
	ctx context.Context,
	t *testing.T,
	cache IndexCache,
	tenantID string,
	blockID ulid.ULID,
	keys []labels.Label,
	expectedHits map[labels.Label]index.Range,
) {
	t.Helper()

	hits := make(map[labels.Label]index.Range)
	for _, key := range keys {
		if rng, ok := cache.FetchPostingsOffset(ctx, tenantID, blockID, key); ok {
			hits[key] = rng
		}
	}

	if len(expectedHits) == 0 {
		assert.Equal(t, len(hits), 0)
	} else {
		assert.Equal(t, expectedHits, hits)
	}
}

func testFetchPostingsOffsetsForMatcher(
	ctx context.Context,
	t *testing.T,
	cache IndexCache,
	keys []mockedPostingsOffsetsForMatcher,
	expectedHits map[mockedPostingsOffsetsForMatcher][]streamindex.PostingListOffset,
	expectedMisses []mockedPostingsOffsetsForMatcher,
) {
	t.Helper()

	hits := make(map[mockedPostingsOffsetsForMatcher][]streamindex.PostingListOffset)
	misses := make([]mockedPostingsOffsetsForMatcher, 0, len(keys))
	for _, key := range keys {
		offsets, ok := cache.FetchPostingsOffsetsForMatcher(
			ctx, key.tenantID, key.blockID, key.labelName, key.m, key.isSubtract,
		)
		if ok {
			hits[key] = offsets
		} else {
			misses = append(misses, key)
		}
	}

	assert.Equal(t, len(expectedHits), len(hits))
	assert.Equal(t, len(expectedMisses), len(misses))
	assert.Equal(t, len(keys)-len(expectedHits), len(expectedMisses))

	if len(expectedHits) != 0 {
		assert.Equal(t, len(expectedHits), len(hits))
		for k, expectedV := range expectedHits {
			assert.EqualValues(t, expectedV, hits[k])
		}
	}
}

func testFetchMultiPostings(ctx context.Context, t *testing.T, cache IndexCache, user string, id ulid.ULID, keys []labels.Label, expectedHits map[labels.Label][]byte) {
	t.Helper()
	pHits := cache.FetchMultiPostings(ctx, user, id, keys)
	expectedResult := &MapIterator[labels.Label]{M: expectedHits, Keys: keys}

	assert.Equal(t, expectedResult.Remaining(), pHits.Remaining())
	for exp, hasNext := expectedResult.Next(); hasNext; exp, hasNext = expectedResult.Next() {
		actual, ok := pHits.Next()
		assert.True(t, ok)
		assert.Equal(t, exp, actual)
	}
	_, ok := pHits.Next()
	assert.False(t, ok)
}
