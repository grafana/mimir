// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/index_cache_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/cache"
)

func TestIndexCacheConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		cfg      IndexCacheConfig
		expected error
	}{
		"default config should pass": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				return cfg
			}(),
		},
		"unsupported backend should fail": {
			cfg: IndexCacheConfig{
				BackendConfig: cache.BackendConfig{
					Backend: "xxx",
				},
			},
			expected: errUnsupportedIndexCacheBackend,
		},
		"no memcached addresses should fail": {
			cfg: IndexCacheConfig{
				BackendConfig: cache.BackendConfig{
					Backend: IndexCacheBackendMemcached,
				},
			},
			expected: cache.ErrNoMemcachedAddresses,
		},
		"one memcached address should pass": {
			cfg: IndexCacheConfig{
				BackendConfig: cache.BackendConfig{Backend: IndexCacheBackendMemcached,
					Memcached: cache.MemcachedConfig{
						Addresses: "dns+localhost:11211",
					},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.cfg.Validate())
		})
	}
}
