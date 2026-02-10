package indexcache

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
)

func TestInMemoryCache(t *testing.T) {
	//inMemCache := NewInMemoryPostingsOffsetTableCacheWithConfig[]
}

func TestInMemoryIndexCache_AvoidsSizeAccountingDeadlock(t *testing.T) {
	cfg := InMemoryIndexCacheConfig{
		MaxItemSizeBytes:  sliceHeaderSize + 2, // Exact size of values the test inserts
		MaxCacheSizeBytes: (sliceHeaderSize + 2) * 2,
	}

	tenant := "tenant-0"

	cache, err := NewInMemoryPostingsOffsetTableCacheWithConfig(cfg, log.NewNopLogger())
	assert.NoError(t, err)

	l, err := simplelru.NewLRU(math.MaxInt64, func(key InMemoryCacheKey, val []byte) {
		// Hack LRU to simulate broken accounting: evictions do not reduce current size.
		size := cache.curSize
		cache.onEvict(key, val)
		cache.curSize = size
	})
	assert.NoError(t, err)
	cache.lru = l

	blockID := ulid.MustNew(0, nil)
	lbl0 := labels.Label{Name: "test0", Value: "0"}
	rng0 := index.Range{Start: 4, End: 8} // Big-endian encoding will be a byte slice of length 2

	// Insert first value, size is equal to item max size and half of cache max size.
	cache.StorePostingsOffset(
		tenant, blockID, lbl0, rng0, time.Hour,
	)
	expectedValueSize0 := sliceSize(cache.valCodec.EncodeRange(rng0))
	assert.Equal(t, expectedValueSize0, cache.curSize)

	// Insert second value, same size as first, then cache will be exactly full.
	lbl1 := labels.Label{Name: "test0", Value: "1"}
	rng1 := index.Range{Start: 8, End: 12}

	cache.StorePostingsOffset(
		tenant, blockID, lbl1, rng1, time.Hour,
	)
	expectedValueSize1 := sliceSize(cache.valCodec.EncodeRange(rng1))
	assert.Equal(t, expectedValueSize0+expectedValueSize1, cache.curSize)
	assert.Equal(t, cache.curSize, cache.maxCacheSizeBytes)

	// Now insert a new value into the cache which requires eviction of previous value to fit.
	// Size accounting is broken via the bad onEvict callback;
	// Here we ensure that it resets the cache and inserts the new item anyway.
	lbl2 := labels.Label{Name: "test0", Value: "2"}
	rng2 := index.Range{Start: 12, End: 16}

	cache.StorePostingsOffset(
		tenant, blockID, lbl2, rng2, time.Hour,
	)
	expectedValueSize2 := sliceSize(cache.valCodec.EncodeRange(rng2))
	assert.Equal(t, expectedValueSize2, cache.curSize)

	ctx := context.Background()
	_, key0InCache := cache.FetchPostingsOffset(ctx, tenant, blockID, lbl0)
	assert.False(t, key0InCache)
	_, key1InCache := cache.FetchPostingsOffset(ctx, tenant, blockID, lbl1)
	assert.False(t, key1InCache)
	_, key2InCache := cache.FetchPostingsOffset(ctx, tenant, blockID, lbl2)
	assert.True(t, key2InCache)
}
