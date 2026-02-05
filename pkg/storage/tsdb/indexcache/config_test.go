// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/index_cache_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package indexcache

import (
	"testing"

	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
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
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)

				cfg.Backend = "xxx"

				return cfg
			}(),
			expected: errUnsupportedIndexCacheBackend,
		},
		"no memcached addresses should fail": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)

				cfg.Backend = BackendMemcached

				return cfg
			}(),
			expected: cache.ErrNoMemcachedAddresses,
		},
		"one memcached address should pass": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)

				cfg.Backend = BackendMemcached
				cfg.Memcached.Addresses = []string{"dns+localhost:11211"}

				return cfg
			}(),
		},
		"inmemory should pass": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)

				cfg.Backend = BackendInMemory

				return cfg
			}(),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.cfg.Validate())
		})
	}
}
