// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/cache/memcached_client_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package cache_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/go-kit/log"
	"github.com/grafana/mimir/pkg/chunk/cache"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/bradfitz/gomemcache/memcache"
)

type mockMemcachedBasicClient struct {
	sync.RWMutex
	contents map[string][]byte
}

func newMockMemcachedBasicClient() *mockMemcachedBasicClient {
	return &mockMemcachedBasicClient{
		contents: map[string][]byte{},
	}
}

func (m *mockMemcachedBasicClient) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	m.RLock()
	defer m.RUnlock()
	result := map[string]*memcache.Item{}
	for _, k := range keys {
		if c, ok := m.contents[k]; ok {
			result[k] = &memcache.Item{
				Value: c,
			}
		}
	}
	return result, nil
}

func (m *mockMemcachedBasicClient) Set(item *memcache.Item) error {
	m.Lock()
	defer m.Unlock()
	m.contents[item.Key] = item.Value
	return nil
}

func TestMemcachedClient_Get_Error(t *testing.T) {
	m := cache.NewMemcachedClient(
		cache.MemcachedClientConfig{
			UpdateInterval: time.Second,
		},
		"test",
		prometheus.NewRegistry(),
		log.NewNopLogger(),
	)
	t.Cleanup(m.Stop)
	item, err := m.Get("foo")
	assert.Error(t, err)
	assert.Nil(t, item)
}
