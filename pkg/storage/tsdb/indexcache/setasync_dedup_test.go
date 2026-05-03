// SPDX-License-Identifier: AGPL-3.0-only

package indexcache

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/grafana/dskit/cache"
	lru "github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDedupSetAsyncCache_DropsWithinWindow(t *testing.T) {
	fake := newFakeCache()
	reg := prometheus.NewPedanticRegistry()
	wrapped, err := newDedupSetAsyncCache(fake, "test", 100, 5*time.Second, reg)
	require.NoError(t, err)

	wrapped.SetAsync("k", []byte("v1"), time.Hour)
	wrapped.SetAsync("k", []byte("v2"), time.Hour)
	wrapped.SetAsync("k", []byte("v3"), time.Hour)

	assert.Equal(t, int32(1), fake.setAsyncCalls.Load(), "only first SetAsync should reach inner")
	assert.Equal(t, []byte("v1"), fake.lastValue("k"), "inner should have stored the first value")
	assert.Equal(t, 2.0, gatherCounter(t, reg, "cache_setasync_duplicates_dropped_total"))
}

func TestDedupSetAsyncCache_AllowsAfterWindow(t *testing.T) {
	fake := newFakeCache()
	reg := prometheus.NewPedanticRegistry()
	wrapped, err := newDedupSetAsyncCache(fake, "test", 100, 10*time.Millisecond, reg)
	require.NoError(t, err)

	wrapped.SetAsync("k", []byte("v1"), time.Hour)
	time.Sleep(20 * time.Millisecond)
	wrapped.SetAsync("k", []byte("v2"), time.Hour)

	assert.Equal(t, int32(2), fake.setAsyncCalls.Load(), "both SetAsyncs should reach inner once window expires")
	assert.Equal(t, []byte("v2"), fake.lastValue("k"), "inner should have the second value as latest")
	assert.Equal(t, 0.0, gatherCounter(t, reg, "cache_setasync_duplicates_dropped_total"))
}

func TestDedupSetAsyncCache_LRUEviction(t *testing.T) {
	fake := newFakeCache()
	reg := prometheus.NewPedanticRegistry()
	// lruSize=2 distributed across dedupShards yields perShard=1 entry per shard.
	wrapped, err := newDedupSetAsyncCache(fake, "test", 2, time.Hour, reg)
	require.NoError(t, err)

	// Eviction is per-shard, so we need three keys that all hash to the same shard
	// to demonstrate that re-adding the oldest key proceeds (it was evicted).
	keys := findKeysInSameShard(t, 3)

	wrapped.SetAsync(keys[0], []byte("v0"), time.Hour)
	wrapped.SetAsync(keys[1], []byte("v1"), time.Hour)  // evicts keys[0] from this shard
	wrapped.SetAsync(keys[2], []byte("v2"), time.Hour)  // evicts keys[1] from this shard
	wrapped.SetAsync(keys[0], []byte("v0b"), time.Hour) // keys[0] was evicted, so this proceeds

	assert.Equal(t, int32(4), fake.setAsyncCalls.Load(), "all four SetAsyncs should reach inner (per-shard eviction)")
	assert.Equal(t, 0.0, gatherCounter(t, reg, "cache_setasync_duplicates_dropped_total"))
}

// findKeysInSameShard returns n keys that all map to the same dedup shard. It panics
// (via t.Fatal) if it cannot find enough; in practice the search converges quickly
// because each random key has a 1/dedupShards chance of landing in any given shard.
func findKeysInSameShard(t *testing.T, n int) []string {
	t.Helper()
	shardOf := func(s string) uint64 { return xxhash.Sum64String(s) & uint64(dedupShards-1) }

	first := "shardkey-0"
	target := shardOf(first)
	out := []string{first}
	for i := 1; len(out) < n && i < 100000; i++ {
		k := fmt.Sprintf("shardkey-%d", i)
		if shardOf(k) == target {
			out = append(out, k)
		}
	}
	require.Len(t, out, n, "could not find %d keys in the same shard within search budget", n)
	return out
}

func TestDedupSetAsyncCache_SetMultiAsyncFiltersOnly(t *testing.T) {
	fake := newFakeCache()
	reg := prometheus.NewPedanticRegistry()
	wrapped, err := newDedupSetAsyncCache(fake, "test", 100, 5*time.Second, reg)
	require.NoError(t, err)

	wrapped.SetMultiAsync(map[string][]byte{"k1": []byte("v1"), "k2": []byte("v2")}, time.Hour)
	// k1 and k2 are now in the dedup window. Only k3 should pass through.
	wrapped.SetMultiAsync(map[string][]byte{"k1": []byte("v1b"), "k3": []byte("v3")}, time.Hour)

	assert.Equal(t, int32(2), fake.setMultiAsyncCalls.Load(), "two SetMultiAsync calls observed by inner")
	assert.ElementsMatch(t, []string{"k1", "k2"}, fake.lastMultiKeys(0), "first call should pass all keys")
	assert.ElementsMatch(t, []string{"k3"}, fake.lastMultiKeys(1), "second call should pass only the new key")
	assert.Equal(t, 1.0, gatherCounter(t, reg, "cache_setasync_duplicates_dropped_total"))
}

func TestDedupSetAsyncCache_OtherMethodsDelegate(t *testing.T) {
	fake := newFakeCache()
	reg := prometheus.NewPedanticRegistry()
	wrapped, err := newDedupSetAsyncCache(fake, "test", 100, 5*time.Second, reg)
	require.NoError(t, err)

	ctx := context.Background()
	_ = wrapped.GetMulti(ctx, []string{"k1"})
	_, _ = wrapped.GetMultiWithError(ctx, []string{"k1"})
	_ = wrapped.Set(ctx, "k1", []byte("v"), time.Hour)
	_ = wrapped.Add(ctx, "k1", []byte("v"), time.Hour)
	_ = wrapped.Delete(ctx, "k1")
	wrapped.Stop()
	_ = wrapped.Name()

	assert.Equal(t, int32(1), fake.getMultiCalls.Load())
	assert.Equal(t, int32(1), fake.getMultiWithErrorCalls.Load())
	assert.Equal(t, int32(1), fake.setCalls.Load())
	assert.Equal(t, int32(1), fake.addCalls.Load())
	assert.Equal(t, int32(1), fake.deleteCalls.Load())
	assert.Equal(t, int32(1), fake.stopCalls.Load())
}

func TestDedupSetAsyncCache_DisabledWhenLruSizeZero(t *testing.T) {
	fake := newFakeCache()
	reg := prometheus.NewPedanticRegistry()
	wrapped, err := newDedupSetAsyncCache(fake, "test", 0, 5*time.Second, reg)
	require.NoError(t, err)

	// When disabled the helper should return the inner unchanged.
	assert.Same(t, fake, wrapped)
}

func TestDedupSetAsyncCache_ConcurrentSetAsyncCoalesces(t *testing.T) {
	fake := newFakeCache()
	reg := prometheus.NewPedanticRegistry()
	wrapped, err := newDedupSetAsyncCache(fake, "test", 100, 5*time.Second, reg)
	require.NoError(t, err)

	var wg sync.WaitGroup
	const goroutines = 100
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			wrapped.SetAsync("hot-key", []byte("v"), time.Hour)
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(1), fake.setAsyncCalls.Load(), "exactly one SetAsync should reach inner under contention")
	assert.Equal(t, float64(goroutines-1), gatherCounter(t, reg, "cache_setasync_duplicates_dropped_total"))
}

// --- fake cache.Cache for tests ---

type fakeCache struct {
	mu sync.Mutex

	store          map[string][]byte
	multiCallsKeys [][]string

	setAsyncCalls          atomic.Int32
	setMultiAsyncCalls     atomic.Int32
	setCalls               atomic.Int32
	addCalls               atomic.Int32
	getMultiCalls          atomic.Int32
	getMultiWithErrorCalls atomic.Int32
	deleteCalls            atomic.Int32
	stopCalls              atomic.Int32
}

func newFakeCache() *fakeCache {
	return &fakeCache{store: map[string][]byte{}}
}

var _ cache.Cache = (*fakeCache)(nil)

func (f *fakeCache) lastValue(k string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.store[k]
}

func (f *fakeCache) lastMultiKeys(idx int) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if idx >= len(f.multiCallsKeys) {
		return nil
	}
	return f.multiCallsKeys[idx]
}

func (f *fakeCache) GetMulti(_ context.Context, keys []string, _ ...cache.Option) map[string][]byte {
	f.getMultiCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string][]byte{}
	for _, k := range keys {
		if v, ok := f.store[k]; ok {
			out[k] = v
		}
	}
	return out
}

func (f *fakeCache) GetMultiWithError(_ context.Context, keys []string, _ ...cache.Option) (map[string][]byte, error) {
	f.getMultiWithErrorCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string][]byte{}
	for _, k := range keys {
		if v, ok := f.store[k]; ok {
			out[k] = v
		}
	}
	return out, nil
}

func (f *fakeCache) SetAsync(key string, value []byte, _ time.Duration) {
	f.setAsyncCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.store[key] = value
}

func (f *fakeCache) SetMultiAsync(data map[string][]byte, _ time.Duration) {
	f.setMultiAsyncCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(data))
	for k, v := range data {
		f.store[k] = v
		keys = append(keys, k)
	}
	f.multiCallsKeys = append(f.multiCallsKeys, keys)
}

func (f *fakeCache) Set(_ context.Context, key string, value []byte, _ time.Duration) error {
	f.setCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.store[key] = value
	return nil
}

func (f *fakeCache) Add(_ context.Context, key string, value []byte, _ time.Duration) error {
	f.addCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.store[key]; ok {
		return errors.New("already stored")
	}
	f.store[key] = value
	return nil
}

func (f *fakeCache) Delete(_ context.Context, key string) error {
	f.deleteCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.store, key)
	return nil
}

func (f *fakeCache) Stop() {
	f.stopCalls.Add(1)
}

func (f *fakeCache) Name() string {
	return "fake"
}

// noopCache implements cache.Cache with no-op methods. Used as the inner cache in
// BenchmarkDedupSetAsync so the benchmark measures only the dedup layer's
// contention; the fakeCache used in the unit tests holds its own mutex on SetAsync,
// which would dominate and mask the dedup-level difference.
type noopCache struct{}

var _ cache.Cache = noopCache{}

func (noopCache) GetMulti(_ context.Context, _ []string, _ ...cache.Option) map[string][]byte {
	return nil
}
func (noopCache) GetMultiWithError(_ context.Context, _ []string, _ ...cache.Option) (map[string][]byte, error) {
	return nil, nil
}
func (noopCache) SetAsync(_ string, _ []byte, _ time.Duration)                     {}
func (noopCache) SetMultiAsync(_ map[string][]byte, _ time.Duration)               {}
func (noopCache) Set(_ context.Context, _ string, _ []byte, _ time.Duration) error { return nil }
func (noopCache) Add(_ context.Context, _ string, _ []byte, _ time.Duration) error { return nil }
func (noopCache) Delete(_ context.Context, _ string) error                         { return nil }
func (noopCache) Stop()                                                            {}
func (noopCache) Name() string                                                     { return "noop" }

// singleMutexDedup is a single-mutex baseline used only by BenchmarkDedupSetAsync to
// quantify the contention reduction from sharding. It deliberately mirrors the
// pre-sharded design: one mutex protects one LRU. Not exposed outside this file.
type singleMutexDedup struct {
	cache.Cache
	mu     sync.Mutex
	seen   *lru.LRU[string, time.Time]
	window time.Duration
}

func newSingleMutexDedup(inner cache.Cache, lruSize int, window time.Duration) *singleMutexDedup {
	seen, err := lru.NewLRU[string, time.Time](lruSize, nil)
	if err != nil {
		panic(err)
	}
	return &singleMutexDedup{Cache: inner, seen: seen, window: window}
}

func (d *singleMutexDedup) SetAsync(key string, value []byte, ttl time.Duration) {
	now := time.Now()
	d.mu.Lock()
	if last, ok := d.seen.Get(key); ok && last.Add(d.window).After(now) {
		d.mu.Unlock()
		return
	}
	d.seen.Add(key, now)
	d.mu.Unlock()
	d.Cache.SetAsync(key, value, ttl)
}

// BenchmarkDedupSetAsync compares the sharded dedup implementation against a
// single-mutex baseline under concurrent random-key writes. Random keys are
// chosen so the LRU doesn't drop anything (so we measure the lock + LRU cost
// itself, not the dedup decision). With sharding, parallel goroutines spread
// across shards; with a single mutex they all serialize.
func BenchmarkDedupSetAsync(b *testing.B) {
	const (
		lruSize = 64 * 1024
		window  = 5 * time.Second
	)
	val := []byte("v")

	var seedCounter atomic.Int64
	run := func(b *testing.B, setAsync func(key string, value []byte, ttl time.Duration)) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(seedCounter.Add(1)))
			for pb.Next() {
				key := strconv.FormatInt(rng.Int63(), 36)
				setAsync(key, val, time.Hour)
			}
		})
	}

	b.Run("sharded", func(b *testing.B) {
		d, err := newDedupSetAsyncCache(noopCache{}, "test", lruSize, window, prometheus.NewPedanticRegistry())
		require.NoError(b, err)
		run(b, d.SetAsync)
	})

	b.Run("single-mutex", func(b *testing.B) {
		d := newSingleMutexDedup(noopCache{}, lruSize, window)
		run(b, d.SetAsync)
	})
}
