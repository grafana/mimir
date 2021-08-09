// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/cache/stop_once.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package cache

import "sync"

type stopOnce struct {
	once sync.Once
	Cache
}

// StopOnce wraps a Cache and ensures its only stopped once.
func StopOnce(cache Cache) Cache {
	return &stopOnce{
		Cache: cache,
	}
}

func (s *stopOnce) Stop() {
	s.once.Do(func() {
		s.Cache.Stop()
	})
}
