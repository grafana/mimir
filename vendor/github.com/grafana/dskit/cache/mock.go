// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package cache

import (
	"context"
	"sync"
	"time"

	"go.uber.org/atomic"
)

var (
	_ Cache = (*MockCache)(nil)
	_ Cache = (*InstrumentedMockCache)(nil)
)

type MockCache struct {
	mu    sync.Mutex
	cache map[string]Item
}

func NewMockCache() *MockCache {
	c := &MockCache{}
	c.Flush()
	return c
}

func (m *MockCache) SetAsync(key string, value []byte, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache[key] = Item{Data: value, ExpiresAt: time.Now().Add(ttl)}
}

func (m *MockCache) SetMultiAsync(data map[string][]byte, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	exp := time.Now().Add(ttl)
	for key, val := range data {
		m.cache[key] = Item{Data: val, ExpiresAt: exp}
	}
}

func (m *MockCache) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache[key] = Item{Data: value, ExpiresAt: time.Now().Add(ttl)}
	return nil
}

func (m *MockCache) Add(_ context.Context, key string, value []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.cache[key]; ok {
		return ErrNotStored
	}

	m.cache[key] = Item{Data: value, ExpiresAt: time.Now().Add(ttl)}
	return nil
}

func (m *MockCache) GetMulti(_ context.Context, keys []string, _ ...Option) map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	found := make(map[string][]byte, len(keys))

	now := time.Now()
	for _, k := range keys {
		v, ok := m.cache[k]
		if ok && now.Before(v.ExpiresAt) {
			found[k] = v.Data
		}
	}

	return found
}

func (m *MockCache) GetItems() map[string]Item {
	m.mu.Lock()
	defer m.mu.Unlock()

	res := make(map[string]Item, len(m.cache))
	for k, v := range m.cache {
		res[k] = v
	}

	return res
}

func (m *MockCache) Name() string {
	return "mock"
}

func (m *MockCache) Stop() {
	// no-op
}

func (m *MockCache) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.cache, key)
	return nil
}

func (m *MockCache) Flush() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cache = map[string]Item{}
}

// InstrumentedMockCache is a mocked cache implementation which also tracks the number
// of times its functions are called.
type InstrumentedMockCache struct {
	cache       *MockCache
	storeCount  atomic.Int32
	fetchCount  atomic.Int32
	deleteCount atomic.Int32
}

// NewInstrumentedMockCache makes a new InstrumentedMockCache.
func NewInstrumentedMockCache() *InstrumentedMockCache {
	return &InstrumentedMockCache{
		cache: NewMockCache(),
	}
}

func (m *InstrumentedMockCache) SetAsync(key string, value []byte, ttl time.Duration) {
	m.storeCount.Inc()
	m.cache.SetAsync(key, value, ttl)
}

func (m *InstrumentedMockCache) SetMultiAsync(data map[string][]byte, ttl time.Duration) {
	m.storeCount.Inc()
	m.cache.SetMultiAsync(data, ttl)
}

func (m *InstrumentedMockCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	m.storeCount.Inc()
	return m.cache.Set(ctx, key, value, ttl)
}

func (m *InstrumentedMockCache) Add(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	m.storeCount.Inc()
	return m.cache.Add(ctx, key, value, ttl)
}

func (m *InstrumentedMockCache) GetMulti(ctx context.Context, keys []string, opts ...Option) map[string][]byte {
	m.fetchCount.Inc()
	return m.cache.GetMulti(ctx, keys, opts...)
}

func (m *InstrumentedMockCache) Name() string {
	return m.cache.Name()
}

func (m *InstrumentedMockCache) Stop() {
	m.cache.Stop()
}

func (m *InstrumentedMockCache) Delete(ctx context.Context, key string) error {
	m.deleteCount.Inc()
	return m.cache.Delete(ctx, key)
}

func (m *InstrumentedMockCache) GetItems() map[string]Item {
	return m.cache.GetItems()
}

func (m *InstrumentedMockCache) Flush() {
	m.cache.Flush()
}

func (m *InstrumentedMockCache) CountStoreCalls() int {
	return int(m.storeCount.Load())
}

func (m *InstrumentedMockCache) CountFetchCalls() int {
	return int(m.fetchCount.Load())
}

func (m *InstrumentedMockCache) CountDeleteCalls() int {
	return int(m.deleteCount.Load())
}
