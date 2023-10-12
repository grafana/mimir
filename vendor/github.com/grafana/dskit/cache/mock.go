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

func (m *MockCache) StoreAsync(data map[string][]byte, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	exp := time.Now().Add(ttl)
	for key, val := range data {
		m.cache[key] = Item{Data: val, ExpiresAt: exp}
	}
}

func (m *MockCache) Fetch(_ context.Context, keys []string, _ ...Option) map[string][]byte {
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

func (m *InstrumentedMockCache) StoreAsync(data map[string][]byte, ttl time.Duration) {
	m.storeCount.Inc()
	m.cache.StoreAsync(data, ttl)
}

func (m *InstrumentedMockCache) Fetch(ctx context.Context, keys []string, opts ...Option) map[string][]byte {
	m.fetchCount.Inc()
	return m.cache.Fetch(ctx, keys, opts...)
}

func (m *InstrumentedMockCache) Name() string {
	return m.cache.Name()
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
