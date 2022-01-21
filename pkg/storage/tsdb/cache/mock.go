// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/cache/mock.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package cache

import (
	"context"
	"sync"
	"time"

	"go.uber.org/atomic"
)

type MockCache struct {
	sync.Mutex
	cache map[string][]byte
}

// NewMockCache makes a new MockCache.
func NewMockCache() *MockCache {
	return &MockCache{
		cache: map[string][]byte{},
	}
}

func (m *MockCache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	m.Lock()
	defer m.Unlock()

	for key, value := range data {
		m.cache[key] = value
	}
}

func (m *MockCache) Fetch(ctx context.Context, keys []string) map[string][]byte {
	found := make(map[string][]byte)

	m.Lock()
	defer m.Unlock()

	for _, key := range keys {
		buf, ok := m.cache[key]
		if ok {
			found[key] = buf
		}
	}

	return found
}

func (m *MockCache) Name() string {
	return "mock"
}

// InstrumentedMockCache is a mocked cache implementation which also tracks the number
// of times its functions are called.
type InstrumentedMockCache struct {
	cache      *MockCache
	storeCount atomic.Int32
	fetchCount atomic.Int32
}

// NewInstrumentedMockCache makes a new InstrumentedMockCache.
func NewInstrumentedMockCache() *InstrumentedMockCache {
	return &InstrumentedMockCache{
		cache: NewMockCache(),
	}
}

func (m *InstrumentedMockCache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	m.storeCount.Inc()
	m.cache.Store(ctx, data, ttl)
}

func (m *InstrumentedMockCache) Fetch(ctx context.Context, keys []string) map[string][]byte {
	m.fetchCount.Inc()
	return m.cache.Fetch(ctx, keys)
}

func (m *InstrumentedMockCache) Name() string {
	return m.cache.Name()
}

func (m *InstrumentedMockCache) CountStoreCalls() int {
	return int(m.storeCount.Load())
}

func (m *InstrumentedMockCache) CountFetchCalls() int {
	return int(m.fetchCount.Load())
}
