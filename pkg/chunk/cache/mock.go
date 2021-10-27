// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/cache/mock.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package cache

import (
	"context"
	"sync"

	"go.uber.org/atomic"
)

type mockCache struct {
	sync.Mutex
	cache map[string][]byte
}

func (m *mockCache) Store(_ context.Context, keys []string, bufs [][]byte) {
	m.Lock()
	defer m.Unlock()
	for i := range keys {
		m.cache[keys[i]] = bufs[i]
	}
}

func (m *mockCache) Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missing []string) {
	m.Lock()
	defer m.Unlock()
	for _, key := range keys {
		buf, ok := m.cache[key]
		if ok {
			found = append(found, key)
			bufs = append(bufs, buf)
		} else {
			missing = append(missing, key)
		}
	}
	return
}

func (m *mockCache) Stop() {
}

// NewMockCache makes a new MockCache.
func NewMockCache() Cache {
	return &mockCache{
		cache: map[string][]byte{},
	}
}

// NewNoopCache returns a no-op cache.
func NewNoopCache() Cache {
	return NewTiered(nil)
}

// InstrumentedMockCache is a mocked cache implementation which also tracks the number
// of times its functions are called.
type InstrumentedMockCache struct {
	cache      Cache
	storeCount atomic.Int32
	fetchCount atomic.Int32
}

// NewInstrumentedMockCache makes a new InstrumentedMockCache.
func NewInstrumentedMockCache() *InstrumentedMockCache {
	return &InstrumentedMockCache{
		cache: NewMockCache(),
	}
}

func (m *InstrumentedMockCache) Store(ctx context.Context, keys []string, bufs [][]byte) {
	m.storeCount.Inc()
	m.cache.Store(ctx, keys, bufs)
}

func (m *InstrumentedMockCache) Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missing []string) {
	m.fetchCount.Inc()
	return m.cache.Fetch(ctx, keys)
}

func (m *InstrumentedMockCache) Stop() {
	m.cache.Stop()
}

func (m *InstrumentedMockCache) CountStoreCalls() int {
	return int(m.storeCount.Load())
}
func (m *InstrumentedMockCache) CountFetchCalls() int {
	return int(m.fetchCount.Load())
}
