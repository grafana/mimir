// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"sync"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDecodedSeriesCacheForTest(t *testing.T, maxItems int) (*decodedSeriesCache, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	c, err := newDecodedSeriesCache(maxItems, reg)
	require.NoError(t, err)
	return c, reg
}

func makeEntry(labels []symbolizedLabel, chunkCount int, encodedByteSize int) decodedSeriesEntry {
	chs := make([]chunks.Meta, chunkCount)
	for i := range chs {
		chs[i] = chunks.Meta{Ref: chunks.ChunkRef(i + 1), MinTime: int64(i), MaxTime: int64(i + 1)}
	}
	return decodedSeriesEntry{
		labels:          labels,
		chunks:          chs,
		encodedByteSize: encodedByteSize,
	}
}

func TestNewDecodedSeriesCache_DisabledWhenMaxItemsZero(t *testing.T) {
	c, err := newDecodedSeriesCache(0, prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	assert.Nil(t, c, "newDecodedSeriesCache must return nil for non-positive maxItems")

	c, err = newDecodedSeriesCache(-1, prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	assert.Nil(t, c, "newDecodedSeriesCache must return nil for negative maxItems")
}

func TestDecodedSeriesCache_NilReceiverIsSafe(t *testing.T) {
	// The cache type's contract is that callers may invoke Get/Set on a nil pointer
	// (see the nil-check inside both methods); this lets the bucketBlock initialise an
	// "off" cache as a typed nil and skip a branch on every call site.
	var c *decodedSeriesCache
	block := ulid.MustNew(1, nil)

	// Get on nil returns a zero entry and false; Set is a no-op.
	got, ok := c.Get(block, 1)
	assert.False(t, ok)
	assert.Equal(t, decodedSeriesEntry{}, got)

	c.Set(block, 1, makeEntry([]symbolizedLabel{{name: 1, value: 2}}, 1, 100))
}

func TestDecodedSeriesCache_SetGetRoundTrip(t *testing.T) {
	c, reg := newDecodedSeriesCacheForTest(t, 16)
	block := ulid.MustNew(1, nil)
	entry := makeEntry([]symbolizedLabel{{name: 1, value: 2}, {name: 3, value: 4}}, 5, 200)

	c.Set(block, 42, entry)

	got, ok := c.Get(block, 42)
	require.True(t, ok)
	assert.Equal(t, entry, got)
	assert.Equal(t, 1.0, counterValue(t, reg, "cortex_bucket_store_decoded_series_cache_hits_total"))
	assert.Equal(t, 0.0, counterValue(t, reg, "cortex_bucket_store_decoded_series_cache_misses_total"))
}

func TestDecodedSeriesCache_GetMissIncrementsMisses(t *testing.T) {
	c, reg := newDecodedSeriesCacheForTest(t, 16)
	block := ulid.MustNew(1, nil)

	_, ok := c.Get(block, 99)
	assert.False(t, ok)
	assert.Equal(t, 0.0, counterValue(t, reg, "cortex_bucket_store_decoded_series_cache_hits_total"))
	assert.Equal(t, 1.0, counterValue(t, reg, "cortex_bucket_store_decoded_series_cache_misses_total"))
}

func TestDecodedSeriesCache_DistinctBlocksAreIndependent(t *testing.T) {
	// Cache keys are (block ULID, ref); two different blocks with the same ref must
	// not return each other's data.
	c, _ := newDecodedSeriesCacheForTest(t, 16)
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	entry1 := makeEntry([]symbolizedLabel{{name: 1, value: 1}}, 1, 100)
	entry2 := makeEntry([]symbolizedLabel{{name: 2, value: 2}}, 2, 200)

	c.Set(block1, 7, entry1)
	c.Set(block2, 7, entry2)

	got1, ok1 := c.Get(block1, 7)
	require.True(t, ok1)
	got2, ok2 := c.Get(block2, 7)
	require.True(t, ok2)

	assert.Equal(t, entry1, got1)
	assert.Equal(t, entry2, got2)
	assert.NotEqual(t, got1, got2, "different blocks must not collide on the same ref")
}

func TestDecodedSeriesCache_LRUEvictsAtBudget(t *testing.T) {
	// Sized so each shard holds exactly 2 entries: with shards=64, ceil(maxItems/64)=2
	// when maxItems=2*shards. A shard hammered with 3 distinct refs should evict the
	// oldest after the third Set; the two more recent must still be hits.
	c, _ := newDecodedSeriesCacheForTest(t, 2*decodedSeriesCacheShards)

	// Find three refs that hash to the same shard so the eviction is observable.
	block := ulid.MustNew(1, nil)
	var refsInSameShard []storage.SeriesRef
	target := c.shardFor(decodedSeriesKey{block: block, ref: 0})
	for ref := storage.SeriesRef(1); ref < 5000 && len(refsInSameShard) < 3; ref++ {
		if c.shardFor(decodedSeriesKey{block: block, ref: ref}) == target {
			refsInSameShard = append(refsInSameShard, ref)
		}
	}
	require.Lenf(t, refsInSameShard, 3, "could not find 3 refs that hash to the same shard within the search range")

	for i, ref := range refsInSameShard {
		c.Set(block, ref, makeEntry([]symbolizedLabel{{name: uint32(i), value: uint32(i)}}, 1, 100))
	}

	// Oldest evicted; two more recent still resident.
	_, ok := c.Get(block, refsInSameShard[0])
	assert.False(t, ok, "oldest entry in the shard must be evicted under per-shard budget=2")
	_, ok = c.Get(block, refsInSameShard[1])
	assert.True(t, ok)
	_, ok = c.Get(block, refsInSameShard[2])
	assert.True(t, ok)
}

func TestDecodedSeriesCache_ConcurrentGetSet(t *testing.T) {
	// Run with -race; verifies that per-shard mutexes serialise correctly under
	// contention. The test interleaves Set, Get, and re-Set calls across many
	// goroutines on a shared key space.
	c, _ := newDecodedSeriesCacheForTest(t, 1024)
	block := ulid.MustNew(1, nil)
	const goroutines, ops = 16, 200

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				ref := storage.SeriesRef((seed*ops + i) % 64) // small key space → shard reuse
				if i%3 == 0 {
					c.Set(block, ref, makeEntry([]symbolizedLabel{{name: uint32(seed), value: uint32(i)}}, 1, 100))
				} else {
					_, _ = c.Get(block, ref)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestDecodedSeriesCache_ShardDistributionIsNotPathological(t *testing.T) {
	// Sanity check that the xxhash-based shardFor does not collapse a sequential ref
	// range onto a single shard. We allow some imbalance but reject the case where
	// >50% of refs land on one shard.
	c, _ := newDecodedSeriesCacheForTest(t, 1024)
	block := ulid.MustNew(1, nil)

	const n = 10_000
	hits := make(map[*decodedSeriesShard]int, decodedSeriesCacheShards)
	for ref := storage.SeriesRef(0); ref < n; ref++ {
		hits[c.shardFor(decodedSeriesKey{block: block, ref: ref})]++
	}

	require.Equal(t, decodedSeriesCacheShards, len(hits), "every shard should receive at least one ref over %d sequential refs", n)
	for s, count := range hits {
		assert.LessOrEqualf(t, count, n/2, "shard %p received %d/%d refs — distribution is pathological", s, count, n)
	}
}

func TestDecodedSeriesCache_StoresSlicesByReference(t *testing.T) {
	// Documents (and locks down) the cache's Set contract: stored slices are NOT
	// deep-copied, so callers must pass slices they no longer plan to mutate. A
	// caller-side bug that overwrites a slice it handed off here would corrupt
	// future Gets. The production call site that satisfies this contract is in
	// loadingSeriesChunkRefsSetIterator.loadSeries (see series_refs.go around
	// the cache.Set call: it allocates fresh `entryLabels` and `entryChunks`
	// slices and copies into them before storing). If a future change there
	// stops allocating fresh slices (e.g. reuses a slab pool), this test must
	// also be updated — the cache's by-reference behaviour is the load-bearing
	// contract on the Set side.
	c, _ := newDecodedSeriesCacheForTest(t, 16)
	block := ulid.MustNew(1, nil)

	labels := []symbolizedLabel{{name: 1, value: 2}}
	entry := decodedSeriesEntry{labels: labels, chunks: nil, encodedByteSize: 0}
	c.Set(block, 1, entry)

	// Mutate the original slice — the cache's stored slice header points at the same
	// backing array, so the mutation is visible on Get. This is the documented
	// contract; if it ever stops being true (i.e. someone adds a copy in Set) this
	// test will fail and force a deliberate decision.
	labels[0].value = 999

	got, ok := c.Get(block, 1)
	require.True(t, ok)
	require.Equal(t, uint32(999), got.labels[0].value, "Set is documented to retain slices without copying; if this fails Set silently started copying")
}
