// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/cache/background_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package cache_test

import (
	"testing"

	"github.com/grafana/mimir/pkg/chunk/cache"
)

func TestBackground(t *testing.T) {
	c := cache.NewBackground("mock", cache.BackgroundConfig{
		WriteBackGoroutines: 1,
		WriteBackBuffer:     100,
	}, cache.NewMockCache(), nil)

	keys, chunks := fillCache(t, c)
	cache.Flush(c)

	testCacheSingle(t, c, keys, chunks)
	testCacheMultiple(t, c, keys, chunks)
	testCacheMiss(t, c)
}
